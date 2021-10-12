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

import java.util.concurrent.{TimeUnit, Delayed}
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import kafka.utils.{SystemTime, threadsafe}

import scala.math._
//TimerTaskList 是一个双向队列
// TimerTaskEntry TimerTask 是一对一关系
// TimerTaskList 包含多个 TimerTaskEntry 是一对多关系
// taskCounter  用于标识当前这个链表中的总定时任务数；
//只有当前时间超越过bucket的起始时间，这个bucket才算是过期，而这里的起始时间就是expiration
@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  //根 定时任务条目
  private[this] val root = new TimerTaskEntry(null, -1)
  root.next = root
  root.prev = root
  //expiration，表示这个链表所在 Bucket 的过期时间戳。
  private[this] val expiration = new AtomicLong(-1L)

  // 设置 expiration
  //这里为什么要比较新旧值是否不同呢？这是因为，目前 Kafka 使用一个 DelayQueue 统一管理所有的 Bucket，
  // 也就是 TimerTaskList 对象。随着时钟不断向前推进，原有 Bucket 会不断地过期，然后失效。
  // 当这些 Bucket 失效后，源码会重用这些 Bucket。重用的方式就是重新设置 Bucket 的过期时间，
  // 并把它们加回到 DelayQueue 中。这里进行比较的目的，就是用来判断这个 Bucket 是否要被插入到 DelayQueue。
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // 获取 expiration
  def getExpiration(): Long = {
    expiration.get()
  }

  // Apply the supplied function to each of tasks in this list
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next

        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    while (!done) {
      // 在添加之前尝试移除该定时任务，保证该任务没有在其他链表中
      timerTaskEntry.remove()
      synchronized {
        timerTaskEntry.synchronized {
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            val tail = root.prev
            timerTaskEntry.next = root
            timerTaskEntry.prev = tail
            timerTaskEntry.list = this
            // 把timerTaskEntry添加到链表末尾
            tail.next = timerTaskEntry
            root.prev = timerTaskEntry
            taskCounter.incrementAndGet()
            done = true
          }
        }
      }
    }
  }

  // Remove the specified timer task entry from this list
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        if (timerTaskEntry.list eq this) {
          timerTaskEntry.next.prev = timerTaskEntry.prev
          timerTaskEntry.prev.next = timerTaskEntry.next
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          timerTaskEntry.list = null
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // 基本上，flush 方法是清空链表中的所有元素，并对每个元素执行指定的逻辑。
  // 该方法用于将高层次时间轮 Bucket 上的定时任务重新插入回低层次的 Bucket 中。
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      //找到链表第一个元素
      var head = root.next
      //开始遍历
      while (head ne root) {
        //移除遍历到的链表元素
        remove(head)
        //执行f函数
        f(head)
        head = root.next
      }
      //清空过期时间设置
      expiration.set(-1L)
    }
  }

  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - SystemTime.hiResClockMs, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {

    val other = d.asInstanceOf[TimerTaskList]

    if(getExpiration < other.getExpiration) -1
    else if(getExpiration > other.getExpiration) 1
    else 0
  }

}

private[timer] class TimerTaskEntry(val timerTask: TimerTask, val expirationMs: Long) extends Ordered[TimerTaskEntry] {

  // 绑定的Bucket链表实例
  @volatile
  var list: TimerTaskList = null
  //next指针
  var next: TimerTaskEntry = null
  //prev指针
  var prev: TimerTaskEntry = null

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  // 关联给定的定时任务
  if (timerTask != null) timerTask.setTimerTaskEntry(this)

  // 关联定时任务是否已经被取消了
  def cancelled: Boolean = {
    timerTask.getTimerTaskEntry != this
  }
  // 从Bucket链表中移除自己
  def remove(): Unit = {
    var currentList = list
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
    // remove方法可能会被其它线程同时调用，所以才有while循环确保TimerTaskEntry的list自动为空
    while (currentList != null) {
      currentList.remove(this)
      currentList = list
    }
  }

  override def compare(that: TimerTaskEntry): Int = {
    this.expirationMs compare that.expirationMs
  }
}

