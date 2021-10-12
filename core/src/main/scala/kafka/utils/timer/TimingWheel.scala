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

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/*
 * Hierarchical Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 *
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 */
// tickMs 表示一个槽所代表的时间范围，默认1ms
// wheelSize 表示该时间轮有多少个槽，默认值是20  不会变 其它几个属性都会变
// startMs：表示该时间轮的开始时间
// taskCounter：表示该时间轮的任务总数
// queue：是一个TimerTaskList的延迟队列。每个槽都有它一个对应的TimerTaskList，
//  TimerTaskList是一个双向链表，有一个expireTime的值，这些TimerTaskList都被加到这个延迟队列中，
//  expireTime最小的槽会排在队列的最前面
// interval：时间轮所能表示的时间跨度，也就是tickMs*wheelSize
// currentTime：表示当前时间，也就是时间轮指针指向的时间
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {
  //时间轮的范围
  private[this] val interval = tickMs * wheelSize
  //表示TimerTaskList的数组，即各个槽
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }

  //currentTime：当前时间戳，将它设置成小于当前时间的最大滴答时长的整数倍。
  // 举个例子，假设滴答时长是 20 毫秒，当前时间戳是 123 毫秒，那么，currentTime 会被调整为 120 毫秒。
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs

  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
  @volatile private[this] var overflowWheel: TimingWheel = null

  //创建更高层的时间轮，低层时间轮的interval作为高层时间轮的tickMs
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      // 只有之前没有创建上层时间轮方法才会继续
      if (overflowWheel == null) {
        // 创建新的TimingWheel实例
        // 滴答时长tickMs等于下层时间轮总时长
        // 第一轮 1*20 = 20ms
        // 第二轮 1*20*20 = 400ms
        // 第三轮 1*20*20*20 = 8s
        // 第四轮 1*20*20*20*20 = 160  即四轮就能容纳160s以内的延迟请求
        // 每层的轮子数都是相同的
        overflowWheel = new TimingWheel(
          tickMs = interval,
          wheelSize = wheelSize,
          startMs = currentTime,
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }

  //将定时任务条目加入时间轮，如果超过当前时间轮的范围，加入更高层的时间轮
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    // 获取定时任务的过期时间戳
    val expiration = timerTaskEntry.expirationMs
    if (timerTaskEntry.cancelled) {
      // 被其它线程取消了，不再需要添加到定时器中
      false
    } else if (expiration < currentTime + tickMs) {
      // 定时任务已经超时了，不需要添加
      false
      //判断当前时间轮所能表示的时间范围是否可以容纳该任务
      // 对于当前时间轮是否可以容纳目标任务，是通过expiration < currentTime + interval来计算的
    } else if (expiration < currentTime + interval) {
      // 还没超时可以添加
      val virtualId = expiration / tickMs
      //根据任务的失效时间，将任务添加到指定的桶
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      // 添加到Bucket中
      bucket.add(timerTaskEntry)
      // 设置Bucket过期时间
      // 如果该时间变更过，说明Bucket是新建或被重用，将其加回到DelayQueue
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket)
      }
      true
    } else {
      //如果时间超出当前所能表示的最大范围，则创建新的时间轮,并把任务添加到那个时间轮上面
      if (overflowWheel == null) {
        addOverflowWheel()
      }
      // 加入到上层时间轮中
      overflowWheel.add(timerTaskEntry)
    }
  }


  //参数 timeMs 表示要把时钟向前推动到这个时点。向前驱动到的时点必须要超过 Bucket 的时间范围，才是有意义的推进，
  // 否则什么都不做，毕竟它还在 Bucket 时间范围内。
  //往前移动时间轮，主要是更新了当前时间轮的当前时间，下一步是重新加入定时任务条目
  //对于新的当前时间，更高层时间相同的桶的定时任务条目会降级加入到低层时间轮不同的桶
  def advanceClock(timeMs: Long): Unit = {
    if (timeMs >= currentTime + tickMs) {
      // 更新当前时间currentTime到下一个Bucket的起始时点
      currentTime = timeMs - (timeMs % tickMs)
      // Try to advance the clock of the overflow wheel if present
      // 同时尝试为上一层时间轮做向前推进动作
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
