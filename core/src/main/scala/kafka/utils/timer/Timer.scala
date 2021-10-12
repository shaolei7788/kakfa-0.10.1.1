/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Utils
import kafka.utils.SystemTime

//Timer 接口定义了管理延迟操作的方法，而 SystemTimer 是实现延迟操作的关键代码
trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  // 将给定的定时任务插入到时间轮上，等待后续延迟执行
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  // 向前推进时钟，执行已达过期时间的延迟任务
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  // 获取时间轮上总的定时任务数
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  // 关闭定时器
  def shutdown(): Unit
}

// System.nanoTime() 纳秒
// SystemTime.hiResClockMs 毫秒
// System.currentTimeMillis()返回的毫秒，这个毫秒其实就是自1970年1月1日0时起的毫秒数
@threadsafe
class SystemTimer(executorName: String,// Purgatory 的名字
                  tickMs: Long = 1,// 默认时间格时间为 1 毫秒
                  wheelSize: Int = 20,// 默认时间格大小为 20
                  //该SystemTimer 定时器启动时间，单位是毫秒
                  startMs: Long = SystemTime.hiResClockMs) extends Timer {

  // 单线程的线程池用于异步执行定时任务
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      Utils.newThread("executor-"+executorName, runnable, false)
  })

  //todo 延迟队列保存所有Bucket，即所有TimerTaskList对象，TimerTaskList 是一个双向链表
  // 它保存了该定时器下管理的所有 Bucket 对象。因为是DelayQueue，
  // 所以只有在 Bucket 过期后，才能从该队列中获取到。SystemTimer 类
  // 的advanceClock 方法正是依靠了这个特性向前驱动时钟
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  //所有的时间轮公用一个计数器  总定时任务数
  private[this] val taskCounter = new AtomicInteger(0)
  //时间轮  startMs 时间轮被创建的起始时间
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // 维护线程安全的读写锁
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  //延迟操作是一个定时任务
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      //将timerTask封装成TimerTaskEntry 定时任务条目  delayMs 延迟ms    SystemTime.hiResClockMs 返回毫秒数
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + SystemTime.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }
  //将定时任务条目加入到时间轮，如果失败，会立即执行定时任务条目
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    //判断是否能加入时间轮
    if (!timingWheel.add(timerTaskEntry)) {
      //过期的任务会立即执行
      // Already expired or cancelled
      if (!timerTaskEntry.cancelled) {
        //提交给线程池执行
        taskExecutor.submit(timerTaskEntry.timerTask)
      }
    }
  }

  //这个将高层时间轮上的任务插入到低层时间轮的过程
  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  //弹出超时的定时任务列表，将定时器的时钟往前移动，并将定时器任务重新加入到定时器中
  // timeoutMs 轮询队列的最长等待时间
  def advanceClock(timeoutMs: Long): Boolean = {
    //只会弹出超时的定时任务列表，队列中的每个元素按照时间排序
    //从延迟队列中取出最近的一个槽，如果槽的expireTime没到，此操作会阻塞timeoutMs
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      // 获取写锁
      // 一旦有线程持有写锁，其他任何线程执行add或advanceClock方法时会阻塞
      writeLock.lock()
      try {
        while (bucket != null) {
          // 推动时间轮向前"滚动"到Bucket的过期时间点
          timingWheel.advanceClock(bucket.getExpiration())
          //将该Bucket下的所有定时任务重写回到时间轮
          bucket.flush(reinsert)
          // 读取下一个Bucket对象
          //立即再轮询一次，如果没有超时，返回的桶为空
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  //计算的是给定 Purgatory 下的总延迟请求数
  def size: Int = taskCounter.get

  override def shutdown() {
    taskExecutor.shutdown()
  }

}

