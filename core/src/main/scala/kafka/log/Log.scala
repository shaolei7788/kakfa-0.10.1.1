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

package kafka.log

import kafka.api.KAFKA_0_10_0_IV0
import kafka.utils._
import kafka.message._
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogOffsetMetadata}
import java.io.{File, IOException}
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic._
import java.text.NumberFormat

import org.apache.kafka.common.errors.{UnsupportedForMessageFormatException, CorruptRecordException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.ListOffsetRequest

import scala.collection.{Seq, JavaConversions}
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.Utils

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(-1, -1, Message.NoTimestamp, -1L, Message.NoTimestamp, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset The first offset in the message set
 * @param lastOffset The last offset in the message set
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 */
case class LogAppendInfo(var firstOffset: Long,
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean)


/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock
 *
 */
@threadsafe
//Log 代表一个分区的目录
class Log(val dir: File,//Log对应的磁盘文件 dir = /opt/module/logs/first-0
          @volatile var config: LogConfig,//Log相关的配置信息
          //指定恢复操作的起始offset,recoveryPoint之前的Message都已刷到磁盘上持久存储
          //而其之后的消息则不一定，出现宕机可能会丢失，所以只需要恢复recoveryPoint之后的消息即可
          @volatile var recoveryPoint: Long = 0L,
          scheduler: Scheduler,
          time: Time = SystemTime) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  /* A lock that guards all modifications to the log */
  //可能存在多个Handler线程并发向同一个Log追加消息
  private val lock = new Object

  /* last time it was flushed */
  private val lastflushedTime = new AtomicLong(time.milliseconds)

  def initFileSize() : Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }
  val t = time.milliseconds
  /* the actual segments of the log */
  //管理LogSegement集合的跳表 key= 分段的基础偏移量 value= LogSegment 对象
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]
  //todo 加载所有的日志分段 通常发生在broker节点重启时
  loadSegments()

  // 给消息分配offset,nextOffsetMetadata 下一个偏移量，专门针对写入操作
  @volatile var nextOffsetMetadata = new LogOffsetMetadata(
            //下一个偏移量
            activeSegment.nextOffset(),
            //activeSegment基准偏移量
            activeSegment.baseOffset,
            //activeSegment 大小
            activeSegment.size.toInt)

  val topicAndPartition: TopicAndPartition = Log.parseTopicPartitionName(dir)

  info("Completed load of log %s with %d log segments and log end offset %d in %d ms"
      .format(name, segments.size(), logEndOffset, time.milliseconds - t))

  val tags = Map("topic" -> topicAndPartition.topic, "partition" -> topicAndPartition.partition.toString)

  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  /** The name of this log */
  def name  = dir.getName()

  /* Load the log segments from the log files on disk */
  private def loadSegments() {
    // create the log directory if it doesn't exist
    dir.mkdirs()
    var swapFiles = Set[File]()

    //将非正常的文件删除
    for(file <- dir.listFiles if file.isFile) {
      if(!file.canRead)
        throw new IOException("Could not read file " + file)
      val filename = file.getName
      if(filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
        // 如果文件以.deleted 或 .cleaned 结尾 直接删除
        file.delete()
      } else if(filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the .index file, complete the swap operation later
        // if an index just delete it, it will be rebuilt
        val baseName = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        if(baseName.getPath.endsWith(IndexFileSuffix)) {
          file.delete()
        } else if(baseName.getPath.endsWith(LogFileSuffix)){
          // delete the index
          val index = new File(CoreUtils.replaceSuffix(baseName.getPath, LogFileSuffix, IndexFileSuffix))
          index.delete()
          swapFiles += file
        }
      }
    }

    // now do a second pass and load all the .log and all index files
    for(file <- dir.listFiles if file.isFile) {
      val filename = file.getName
      //索引文件
      if(filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix)) {
        // if it is an index file, make sure it has a corresponding .log file
        val logFile =
          if (filename.endsWith(TimeIndexFileSuffix))
            new File(file.getAbsolutePath.replace(TimeIndexFileSuffix, LogFileSuffix))
          else
            new File(file.getAbsolutePath.replace(IndexFileSuffix, LogFileSuffix))

        if(!logFile.exists) {
          warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
          file.delete()
        }
      } else if(filename.endsWith(LogFileSuffix)) {
        //日志文件
        // if its a log file, load the corresponding log segment
        //todo 基准偏移量
        val start = filename.substring(0, filename.length - LogFileSuffix.length).toLong
        val indexFile = Log.indexFilename(dir, start)
        val timeIndexFile = Log.timeIndexFilename(dir, start)

        val indexFileExists = indexFile.exists()
        //创建LogSegment对象
        val segment = new LogSegment(dir = dir,
                                     startOffset = start,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = true)

        if (indexFileExists) {
          try {
            segment.index.sanityCheck()
            segment.timeIndex.sanityCheck()
          } catch {
            case e: java.lang.IllegalArgumentException =>
              warn(s"Found a corrupted index file due to ${e.getMessage}}. deleting ${timeIndexFile.getAbsolutePath}, " +
                s"${indexFile.getAbsolutePath} and rebuilding index...")
              indexFile.delete()
              timeIndexFile.delete()
              segment.recover(config.maxMessageSize)
          }
        } else {
          error("Could not find index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
          segment.recover(config.maxMessageSize)
        }
        segments.put(start, segment)
      }
    }

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val fileName = logFile.getName
      val startOffset = fileName.substring(0, fileName.length - LogFileSuffix.length).toLong
      val indexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, IndexFileSuffix) + SwapFileSuffix)
      val index =  new OffsetIndex(indexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      val timeIndexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, TimeIndexFileSuffix) + SwapFileSuffix)
      val timeIndex = new TimeIndex(timeIndexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      val swapSegment = new LogSegment(new FileMessageSet(file = swapFile),
                                       index = index,
                                       timeIndex = timeIndex,
                                       baseOffset = startOffset,
                                       indexIntervalBytes = config.indexInterval,
                                       rollJitterMs = config.randomSegmentJitter,
                                       time = time)
      info("Found log file %s from interrupted swap operation, repairing.".format(swapFile.getPath))
      swapSegment.recover(config.maxMessageSize)
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.nextOffset)
      replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
    }

    if(logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at offset 0
      segments.put(0L, new LogSegment(dir = dir,
                                     startOffset = 0,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = false,
                                     initFileSize = this.initFileSize(),
                                     preallocate = config.preallocate))
    } else {
      recoverLog()
      // reset the index size of the currently active log segment to allow more entries
      activeSegment.index.resize(config.maxIndexSize)
      activeSegment.timeIndex.resize(config.maxIndexSize)
    }

  }

  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size.toInt)
  }

  private def recoverLog() {
    // if we have the clean shutdown marker, skip recovery
    if(hasCleanShutdownFile) {
      this.recoveryPoint = activeSegment.nextOffset
      return
    }

    // okay we need to actually recovery this log
    val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
    while(unflushed.hasNext) {
      val curr = unflushed.next
      info("Recovering unflushed segment %d in log %s.".format(curr.baseOffset, name))
      val truncatedBytes =
        try {
          curr.recover(config.maxMessageSize)
        } catch {
          case e: InvalidOffsetException =>
            val startOffset = curr.baseOffset
            warn("Found invalid offset during recovery for log " + dir.getName +". Deleting the corrupt segment and " +
                 "creating an empty one with starting offset " + startOffset)
            curr.truncateTo(startOffset)
        }
      if(truncatedBytes > 0) {
        // we had an invalid message, delete all remaining log
        warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(curr.baseOffset, name, curr.nextOffset))
        unflushed.foreach(deleteSegment)
      }
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile() = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log
   */
  def close() {
    debug("Closing log " + name)
    lock synchronized {
      logSegments.foreach(_.close())
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param messages The message set to append
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  //todo 会将生产者发送过来请求解析成ByteBufferMessageSet  assignOffsets 是否需要给消息添加offset
  def append(messages: ByteBufferMessageSet, assignOffsets: Boolean = true): LogAppendInfo = {
    //todo 进行数据校验 LogAppendInfo 日志追加信息 代表这批消息的概要信息 但不包含消息内容
    // 该对象的内容包括消息集第一条和最后一条消息的偏移量，消息集的总字节大小，偏移量是否单调递增
    val appendInfo: LogAppendInfo = analyzeAndValidateMessageSet(messages)

    // if we have any valid messages, append them to the log
    //todo 没有有效消息直接返回
    if (appendInfo.shallowCount == 0)
      return appendInfo

    // trim any invalid bytes or partial messages before appending it to the on-disk log
    //todo 清除未通过验证的message
    var validMessages: ByteBufferMessageSet = trimInvalidBytes(messages, appendInfo)

    try {
      // they are valid, insert them in the log
      lock synchronized {
        //todo assignOffsets 是否需要给消息添加offset
        if (assignOffsets) {
          //todo nextOffsetMetadata.messageOffset 作为下一个消息的绝对偏移量
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          //使用appendInfo.firstOffset 记录第一条消息的offset,不受压缩影响
          //todo 获取下一个消息的绝对偏移量(不是消息自带的相对偏移量)作为第一条消息的绝对偏移量  从此值开始向后分配offset
          appendInfo.firstOffset = offset.value
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            //todo 基于起始偏移量，为有效消息集的每条消息重新分配绝对偏移量
            // offset的返回值是最后一条消息的偏移量再加1
            validMessages.validateMessagesAndAssignOffsets(offset,
                                                           now,
                                                           appendInfo.sourceCodec,
                                                           appendInfo.targetCodec,
                                                           config.compact,
                                                           config.messageFormatVersion.messageFormatVersion,
                                                           config.messageTimestampType,
                                                           config.messageTimestampDifferenceMaxMs)
          } catch {
            case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
          }
          //校验后的消息
          validMessages = validateAndOffsetAssignResult.validatedMessages
          //获取最大的时间戳
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          //获取时间戳的偏移量
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.offsetOfMaxTimestamp
          //记录最后一条消息的offset
          appendInfo.lastOffset = offset.value - 1
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME) {
            //为日志添加时间戳
            appendInfo.logAppendTime = now
          }

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          //确保消息大小不超限
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (messageAndOffset <- validMessages.shallowIterator) {
              if (MessageSet.entrySize(messageAndOffset.message) > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
                BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
                throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
                  .format(MessageSet.entrySize(messageAndOffset.message), config.maxMessageSize))
              }
            }
          }

        } else {
          // we are taking the offsets we are given
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException("Out of order offsets found in " + messages)
        }

        // check messages set size may be exceed config.segmentSize
        if (validMessages.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException("Message set size is %d bytes which exceeds the maximum configured segment size of %d."
            .format(validMessages.sizeInBytes, config.segmentSize))
        }

        // maybe roll the log if this segment is full
        //todo 获取activeSegement,此过程可能会分配新的activeSegement
        val segment: LogSegment = maybeRoll(messagesSize = validMessages.sizeInBytes, maxTimestampInMessages = appendInfo.maxTimestamp)

        //todo 添加消息
        segment.append(firstOffset = appendInfo.firstOffset, largestTimestamp = appendInfo.maxTimestamp,
          offsetOfLargestTimestamp = appendInfo.offsetOfMaxTimestamp, messages = validMessages)

        //todo 修改LEO  appendInfo.lastOffset 这批消息最后一个偏移量
        updateLogEndOffset(appendInfo.lastOffset + 1)

        trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
          .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validMessages))
        //unflushedMessages 未刷新消息的数量 检测未刷到磁盘的数据是否达到一定的阈值，如果是调用flush方法刷新
        // unflushedMessages = 最新偏移量 - 检查点位置
        if (unflushedMessages >= config.flushInterval) {
          //todo flushInterval 可配置 如果不配置不会执行此操作 由操作系统刷到磁盘
          flush()
        }
        appendInfo
      }
    } catch {
      case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
    }
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */

  private def analyzeAndValidateMessageSet(messages: ByteBufferMessageSet): LogAppendInfo = {
    //记录外层消息的数量
    var shallowMessageCount = 0
    //记录通过验证的message的字节数之和
    var validBytesCount = 0
    //记录第一条消息 最后一条消息
    var firstOffset, lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true //单调递增
    var maxTimestamp = Message.NoTimestamp
    var offsetOfMaxTimestamp = -1L
    //shallowIterator 浅层迭代器 即未使用压缩 获取每条消息的内容即对应的偏移量
    //deepIterator    深层迭代器 即使用了压缩
    for(messageAndOffset <- messages.shallowIterator) {
      // update the first offset if on the first message
      if(firstOffset < 0)
        // messageAndOffset.offset 相对偏移量 消息集中的偏移量都是从0开始
        firstOffset = messageAndOffset.offset
      // check that offsets are monotonically increasing
      //主要是判断偏移量是否单调递增
      if(lastOffset >= messageAndOffset.offset) {
        monotonic = false
      }
      // 每循环一条消息就更新lastOffset
      lastOffset = messageAndOffset.offset
      val m : Message = messageAndOffset.message
      // Check if the message sizes are valid.
      //消息的大小
      val messageSize = MessageSet.entrySize(m)
      if(messageSize > config.maxMessageSize) {
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
        throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
          .format(messageSize, config.maxMessageSize))
      }

      // 校验消息大小
      m.ensureValid()
      if (m.timestamp > maxTimestamp) {
        maxTimestamp = m.timestamp
        offsetOfMaxTimestamp = lastOffset
      }
      shallowMessageCount += 1
      validBytesCount += messageSize

      val messageCodec = m.compressionCodec
      if(messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)

    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, Message.NoTimestamp, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param messages The message set to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(messages: ByteBufferMessageSet, info: LogAppendInfo): ByteBufferMessageSet = {
    val messageSetValidBytes = info.validBytes
    if(messageSetValidBytes < 0)
      throw new CorruptRecordException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    if(messageSetValidBytes == messages.sizeInBytes) {
      messages
    } else {
      // trim invalid bytes
      val validByteBuffer = messages.buffer.duplicate()
      validByteBuffer.limit(messageSetValidBytes)
      new ByteBufferMessageSet(validByteBuffer)
    }
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param maxOffset The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set)
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  //todo offset = 起始偏移量 adjustedFetchSize = 拉取的字节长度   maxOffsetOpt = 读取消息的上限即最大偏移量
  // maxLength 默认是1m   maxOffset 备份副本拉取数据不会有这个值
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None, minOneMessage: Boolean = false): FetchDataInfo = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))
    //将nextOffsetMetadata 保存成局部变量 记录了Log中最后一个offset值
    val currentNextOffsetMetadata = nextOffsetMetadata
    //LEO
    val next = currentNextOffsetMetadata.messageOffset
    if(startOffset == next)
      return FetchDataInfo(currentNextOffsetMetadata, MessageSet.Empty)

    //todo 从跳表 查询baseOffset小于startOffset 且 baseOffset 最大的segment
    var entry = segments.floorEntry(startOffset)
    // attempt to read beyond the log end offset is an error
    if(startOffset > next || entry == null)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))

    // Do the read on the segment with a base offset less than the target offset
    // but if that segment doesn't contain any messages with an offset greater than that
    // continue to read from successive segments until we get some messages or we reach the end of the log
    while(entry != null) {
      // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
      // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
      // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
      // end of the active segment.
      //todo 最大物理位置
      val maxPosition = {
        if (entry == segments.lastEntry) {
          //读取activeSegment的情况
          val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
          // Check the segment again in case a new segment has just rolled out.
          if (entry != segments.lastEntry) {
            // 写线程并发进行roll操作，变成了读取非activeSegment的情况
            entry.getValue.size
          } else {
            exposedPos
          }
        } else {
          //读取的是非activeSegment的情况，直接可以读取到LogSegment的结尾
          entry.getValue.size
        }
      }
      //todo 读取消息 entry.getValue = LogSegment  LogSegment#read
      // startOffset 指定读取起始消息的偏移量
      // maxOffset 指定读取结束消息的偏移量
      // maxLength 最大字节数
      // maxPosition 最大物理位置
      val fetchInfo = entry.getValue.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)
      if(fetchInfo == null) {
        //在此Logsegment中没读到数据，则继续读取下更高的Logsegment
        entry = segments.higherEntry(entry.getKey)
      } else {
        return fetchInfo
      }
    }

    // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
    // this can happen when all messages with offset larger than start offsets have been deleted.
    // In this case, we will return the empty set with log end offset metadata
    //查找不到startOffset之后的消息
    FetchDataInfo(nextOffsetMetadata, MessageSet.Empty)
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   */
  def fetchOffsetsByTimestamp(targetTimestamp: Long): Option[TimestampOffset] = {
    debug(s"Searching offset for timestamp $targetTimestamp")

    if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
      throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicAndPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val segmentsCopy = logSegments.toBuffer
    // For the earliest and latest, we do not need to return the timestamp.
    if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
        return Some(TimestampOffset(Message.NoTimestamp, segmentsCopy.head.baseOffset))
    else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP)
        return Some(TimestampOffset(Message.NoTimestamp, logEndOffset))

    val targetSeg = {
      // Get all the segments whose largest timestamp is smaller than target timestamp
      val earlierSegs = segmentsCopy.takeWhile(_.largestTimestamp < targetTimestamp)
      // We need to search the first segment whose largest timestamp is greater than the target timestamp if there is one.
      if (earlierSegs.length < segmentsCopy.length)
        Some(segmentsCopy(earlierSegs.length))
      else
        None
    }

    targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp))
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return unknown offset metadata
   */
  def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      val fetchDataInfo = read(offset, 1)
      fetchDataInfo.fetchOffsetMetadata
    } catch {
      case e: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * @param predicate A function that takes in a single log segment and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
    lock synchronized {
      val deletable = deletableSegments(predicate)
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if (segments.size == numToDelete) {
          //全部LogSegment都符合删除条件 至少保留一个LogSegment，删除前先创建一个新的activeSegment
          roll()
        }
        // 删除LogSegment
        deletable.foreach(deleteSegment(_))
      }
      numToDelete
    }
  }

  /**
    * Find segments starting from the oldest until the the user-supplied predicate is false.
    * A final segment that is empty will never be returned (since we would just end up re-creating it).
    * @param predicate A function that takes in a single log segment and returns true iff it is deletable
    * @return the segments ready to be deleted
    */
  private def deletableSegments(predicate: LogSegment => Boolean) = {
    //获取activeSegment
    val lastEntry = segments.lastEntry
    if (lastEntry == null) Seq.empty
    else logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastEntry.getValue.baseOffset || s.size > 0))
  }

  /**
    * Delete any log segments that have either expired due to time based retention
    * or because the log size is > retentionSize
    */
  def deleteOldSegments(): Int = {
    if (!config.delete) return 0
    //todo deleteRetenionMsBreachedSegments 会根据存活时长判断是否要删除logsegment
    //todo deleteRetentionSizeBreachedSegments 会根据文件大小判断是否要删除logsegment
    deleteRetenionMsBreachedSegments() + deleteRetentionSizeBreachedSegments()
  }

  private def deleteRetenionMsBreachedSegments() : Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds
    //删除条件是 Logsegment 的日志文件在最近一段时间内没有被修改  7 * 24 h
    deleteOldSegments(startMs - _.largestTimestamp > config.retentionMs)
  }

  private def deleteRetentionSizeBreachedSegments() : Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }
    deleteOldSegments(shouldDelete)
  }

  /**
   * The size of the log in bytes
   */
  def size: Long = logSegments.map(_.size).sum

   /**
   * The earliest message offset in the log
   */
  def logStartOffset: Long = logSegments.head.baseOffset

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  //读取时的结束偏移量，专门针对读取操作
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   *  The offset of the next message that will be appended to the log
   */
  //下一条消息的偏移量
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes
   * @param maxTimestampInMessages The maximum timestamp in the messages.
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int, maxTimestampInMessages: Long): LogSegment = {
    val segment = activeSegment
    val now = time.milliseconds
    val reachedRollMs = segment.timeWaitedForRoll(now, maxTimestampInMessages) > config.segmentMs - segment.rollJitterMs
    //如果一个segment大小超过了1g,就需要新建一个segment
    //距离上次创建日志分段的时间达到了一定的阈值 默认7天，并且数据文件有数据
    //索引文件满了 默认10m
    //时间索引文件满了 默认10m
    if (segment.size > config.segmentSize - messagesSize ||
        (segment.size > 0 && reachedRollMs) ||
        segment.index.isFull ||
        segment.timeIndex.isFull) {
      debug(s"Rolling new log segment in $name (log_size = ${segment.size}/${config.segmentSize}}, " +
          s"index_size = ${segment.index.entries}/${segment.index.maxEntries}, " +
          s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
          s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")
      //todo 创建新的activeSegment
      roll()
    } else {
      //直接返回可用的segment
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   *
   * @return The newly rolled segment
   */
  def roll(): LogSegment = {
    val start = time.nanoseconds
    lock synchronized {
      //LEO
      val newOffset = logEndOffset
      //基于LEO生成新日志文件名 LEO.log
      val logFile = logFilename(dir, newOffset)
      //基于LEO生成新索引文件名 LEO.index
      val indexFile = indexFilename(dir, newOffset)
      //基于LEO生成时间索引文件名 LEO.timeindex
      val timeIndexFile = timeIndexFilename(dir, newOffset)
      for(file <- List(logFile, indexFile, timeIndexFile); if file.exists) {
        warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
        //todo 文件存在就删除
        file.delete()
      }

      //segments最后的一个 也就是旧的activeSegment
      segments.lastEntry() match {
        case null =>
        case entry => {
          val seg: LogSegment = entry.getValue
          seg.onBecomeInactiveSegment()
          //todo 对日志文件索引文件进行截断，保证文件中只保存了有效字节，这对预分区的文件尤其重要
          seg.index.trimToValidSize()
          seg.timeIndex.trimToValidSize()
          seg.log.trim()
        }
      }
      //新创建的segment
      val segment = new LogSegment(dir,
                                   startOffset = newOffset,
                                   // 4 k
                                   indexIntervalBytes = config.indexInterval,
                                   // 10 * 1024 * 1024 = 10m
                                   maxIndexSize = config.maxIndexSize,
                                   rollJitterMs = config.randomSegmentJitter,
                                   time = time,
                                   fileAlreadyExists = false,
                                   initFileSize = initFileSize,
                                   preallocate = config.preallocate)
      //todo 将新创建的segment加到segments跳表中
      val prev = addSegment(segment)
      if(prev != null)
        //之前就存在对应的baseOffse的segment 抛异常
        throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))
      // We need to update the segment base offset and append position data of the metadata when log rolls.
      // The next offset should not change.
      //更新nextOffsetMetadata，更新其中记录的activeSegment的baseoffset 和activeSegment.size,    但LEO不会变
      updateLogEndOffset(nextOffsetMetadata.messageOffset)
      // schedule an asynchronous flush of the old segment
      //执行flush操作
      scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

      info("Rolled new log segment for '" + name + "' in %.0f ms.".format((System.nanoTime - start) / (1000.0*1000.0)))
      //返回新的segment
      segment
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  //最新偏移量 - 检查点位置
  def unflushedMessages() = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  //todo 执行刷新操作
  // 消息追加到日志中，有两种场景会发送刷新日志的动作
  // 新创建一个日志分段，立即刷新旧的日志分段
  // 日志中未刷新的消息数量超过log.flush.interval.message的配置项
  def flush(offset: Long) : Unit = {
    if (offset <= this.recoveryPoint)
      return
    debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " +
          time.milliseconds + " unflushed = " + unflushedMessages)
    //找到recoveryPoint 和 offset之间的logsegment对象
    for(segment <- logSegments(this.recoveryPoint, offset)) {
      //todo LogSegment#flush 会调用日志文件索引文件的flush方法，最终调用操作系统的fsync命令刷新磁盘，保证数据持久化
      segment.flush()
    }
    lock synchronized {
      if(offset > this.recoveryPoint) {
        //后移recoveryPoint
        this.recoveryPoint = offset
        //更新最近的刷新时间
        lastflushedTime.set(time.milliseconds)
      }
    }
  }

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete() {
    lock synchronized {
      removeLogMetrics()
      logSegments.foreach(_.delete())
      segments.clear()
      Utils.delete(dir)
    }
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   */
  private[log] def truncateTo(targetOffset: Long) {
    info("Truncating log %s to offset %d.".format(name, targetOffset))
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    if(targetOffset > logEndOffset) {
      info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset-1))
      return
    }
    lock synchronized {
      if(segments.firstEntry.getValue.baseOffset > targetOffset) {
        truncateFullyAndStartAt(targetOffset)
      } else {
        val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
        deletable.foreach(deleteSegment(_))
        activeSegment.truncateTo(targetOffset)
        updateLogEndOffset(targetOffset)
        this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    debug("Truncate and start log '" + name + "' to " + newOffset)
    lock synchronized {
      val segmentsToDelete = logSegments.toList
      segmentsToDelete.foreach(deleteSegment(_))
      addSegment(new LogSegment(dir,
                                newOffset,
                                indexIntervalBytes = config.indexInterval,
                                maxIndexSize = config.maxIndexSize,
                                rollJitterMs = config.randomSegmentJitter,
                                time = time,
                                fileAlreadyExists = false,
                                initFileSize = initFileSize,
                                preallocate = config.preallocate))
      updateLogEndOffset(newOffset)
      this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime(): Long = lastflushedTime.get

  /**
   * The active segment that is currently taking appends
   */
  //最新获得的 Segment
  def activeSegment : LogSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = {
    import JavaConversions._
    segments.values
  }

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset)
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    import JavaConversions._
    lock synchronized {
      val floor = segments.floorKey(from)
      if(floor eq null)
        segments.headMap(to).values
      else
        segments.subMap(floor, true, to, false).values
    }
  }

  override def toString = "Log(" + dir + ")"

  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   *
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, name))
    lock synchronized {
      //从segments中移除该segment
      segments.remove(segment.baseOffset)
      //异步删除日志分段
      asyncDeleteSegment(segment)
    }
  }

  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   *
   * @throws KafkaStorageException if the file can't be renamed and still exists
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    //将日志分段的文件名 添加上 .deleted后缀
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    def deleteSeg() {
      info("Deleting segment %d from log %s.".format(segment.baseOffset, name))
      //执行删除操作
      segment.delete()
    }
    scheduler.schedule("delete-file", deleteSeg, delay = config.fileDeleteDelayMs)
  }

  /**
   * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
   * be asynchronously deleted.
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates new segment with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned file is deleted on recovery in loadSegments().
   *   <li> New segment is renamed .swap. If the broker crashes after this point before the whole
   *        operation is completed, the swap operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment is renamed to replace the existing segment, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegment The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile : Boolean = false) {
    lock synchronized {
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
      addSegment(newSegment)

      // delete the old files
      for(seg <- oldSegments) {
        // remove the index entry
        if(seg.baseOffset != newSegment.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment
        asyncDeleteSegment(seg)
      }
      // okay we are safe now, remove the swap suffix
      newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }
  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   *
   * @param segment The segment to add
   */
  def addSegment(segment: LogSegment) = this.segments.put(segment.baseOffset, segment)

}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /** a time index file */
  val TimeIndexFileSuffix = ".timeindex"

  /** a file that is scheduled to be deleted */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
   * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
  /** TODO: Get rid of CleanShutdownFile in 0.8.2 */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   *
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def logFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)

  /**
   * Construct an index file name in the given dir using the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def indexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)

  /**
   * Construct a time index file name in the given dir using the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def timeIndexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix)

  /**
   * Parse the topic and partition out of the directory name of a log
   */
  def parseTopicPartitionName(dir: File): TopicAndPartition = {
    val name: String = dir.getName
    if (name == null || name.isEmpty || !name.contains('-')) {
      throwException(dir)
    }
    val index = name.lastIndexOf('-')
    val topic: String = name.substring(0, index)
    val partition: String = name.substring(index + 1)
    if (topic.length < 1 || partition.length < 1) {
      throwException(dir)
    }
    TopicAndPartition(topic, partition.toInt)
  }

  def throwException(dir: File) {
    throw new KafkaException("Found directory " + dir.getCanonicalPath + ", " +
      "'" + dir.getName + "' is not in the form of topic-partition\n" +
      "If a directory does not contain Kafka topic data it should not exist in Kafka's log " +
      "directory")
  }
}

