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

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.utils.timer._

import scala.collection._

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 */
//todo
// 延迟操作不仅存在于延迟缓存中，还会被定时器监控
// 将延迟操作加入延迟缓存中，目的是让外部事件有机会尝试完成延迟的操作
// 将延迟操作加入定时器中，目的是在延迟超时后，服务端可以强制返回响应结果给客户端
abstract class DelayedOperation(override val delayMs: Long) extends TimerTask with Logging {

  // 标识该延迟操作是否已经完成
  private val completed = new AtomicBoolean(false)

  //强制完成延迟操作，不管它是否满足完成条件。每当操作满足完成条件或已经过期了，就需要调用该方法完成该操作
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      //取消定时器
      cancel()
      //完成的回调方法
      onComplete()
      true
    } else {
      //有其它线程完成了这个延迟的请求
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   */
  //检查延迟的操作是否已经完成
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   */
  //当延迟操作超时后会执行该回调，如果有多个线程，只有一个线程会执行一次
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   */
  //完成延迟操作所需的处理逻辑。这个方法只会在 forceComplete 方法中被调用
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   */
  //尝试完成被延迟的任务，在forceComplete中只会调用一次
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete(). This can be overridden if the operation provides its
   * own synchronization.
   */
  def safeTryComplete(): Boolean = {
    synchronized {
      tryComplete()
    }
  }

  //调用延迟操作超时后的过期逻辑
  //任务在超时时才会调用一次run方法
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

//保存延迟请求的缓冲区也就是说 它保存的是因为不满足条件而无法完成，但是又没有超时的请求
object DelayedOperationPurgatory {

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    // purgeInterval = 1000
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                       timeoutTimer: Timer,
                                                       brokerId: Int = 0,
                                                       purgeInterval: Int = 1000,
                                                       //表示是否启动删除线程
                                                       reaperEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

  private val removeWatchersLock = new ReentrantReadWriteLock()

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value = watched()
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value = delayed()
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  //尝试完成延迟操作，如果不能完成就以指定的键监控这个延迟操作
  // operation = DelayedJoin    watchKeys = GroupKey
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")
    //第一次尝试完成延迟操作
    var isCompletedByMe = operation.safeTryComplete()
    // 如果该延迟请求是由本线程完成的，直接返回true即可
    if (isCompletedByMe)
      return true
    //判断这个操作是否被监视过
    var watchCreated = false
    // 遍历所有要监控的Key
    for(key <- watchKeys) {
      //在加入过程中，延迟操作已经完成，那么之后的键不要再监视了
      if (operation.isCompleted)
        return false
      //延迟操作没有完成，才将操作加入到每个键的监视列表中
      // 将该operation加入到Key所在的WatcherList
      watchForOperation(key, operation)
      // 设置watchCreated标记，表明该任务已经被加入到WatcherList
      if (!watchCreated) {
        //一旦为true，其它键就没有机会再执行了
        watchCreated = true
        //一个操作即使有多个键，只会增加一次
        // 更新Purgatory中总请求数
        estimatedTotalOperations.incrementAndGet()
      }
    }
    //第二次尝试完成延迟操作
    isCompletedByMe = operation.safeTryComplete()
    //能完成立即返回
    if (isCompletedByMe)
      return true
    // 经过两轮safeTryComplete，但还没完成，并且也被监视了，才会加入到定时器中
    // 如果依然不能完成此请求，将其加入到过期队列
    if (!operation.isCompleted) {
      //todo 加入失效队列 operation extends TimerTask
      // SystemTimer#add
      timeoutTimer.add(operation)
      //添加前没完成，但添加后完成了
      if (operation.isCompleted) {
        // 取消这个延迟操作的定时线程
        operation.cancel()
      }
    }
    false
  }

  /**
   * Check if some some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  //检查并尝试完成指定键的延迟操作，在tryCompleteElseWatch，如果延迟操作没有完成，会被加入到延迟缓存中
  def checkAndComplete(key: Any): Int = {
    // 获取WatcherList中Key对应的Watchers对象实例
    val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
    if(watchers == null)
      0
    else {
      // 尝试完成满足完成条件的延迟请求并返回成功完成的请求数
      watchers.tryCompleteWatched()
    }
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched() = allWatchers.map(_.countWatched).sum

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def delayed() = timeoutTimer.size

  /*
   * Return all the current watcher lists,
   * note that the returned watchers may be removed from the list by other threads
   */
  private def allWatchers = inReadLock(removeWatchersLock) { watchersForKey.values }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  //根据给定的key，监视指定的延迟操作
  private def watchForOperation(key: Any, operation: T) {
    inReadLock(removeWatchersLock) {
      val watcher = watchersForKey.getAndMaybePut(key)
      //将延迟操作加入到键的监视器列表中
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
    inWriteLock(removeWatchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (watchersForKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        watchersForKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  //延迟缓存的每个键对应一个监视器，它管理了这个键的所有延迟操作
  //管理了链表结构的延迟操作
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()
    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    //将延迟操作加到键的监视列表中
    def watch(t: T) {
      operations.add(t)
    }

    // 尝试完成监视器中所有的延迟操作，它被外部事件的checkAndComplete调用
    def tryCompleteWatched(): Int = {
      var completed = 0
      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          iter.remove()
        } else if (curr.safeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    // 遍历列表，并移除已经完成的延迟
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long) {
    //定时器时钟的滑动间隔，每隔200ms前进一次
    timeoutTimer.advanceClock(timeoutMs)
    // purge 净化 清理
    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    // purgeInterval = 1000
    if (estimatedTotalOperations.get - delayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(delayed)
      debug("Begin purging watch lists")
      //清理监视器中已经完成的延迟操作
      val purged = allWatchers.map(_.purgeCompleted()).sum
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  //后台清理失效的延迟操作的线程   Reaper收割者
  //用于将已过期的延迟请求从数据结构中移除掉
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d".format(brokerId),
    false) {

    override def doWork() {
      //todo 用于将已过期的延迟请求从数据结构中移除掉
      advanceClock(200L)
    }
  }
}
