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

import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._

import kafka.utils._
import kafka.message._
import kafka.common.KafkaException
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.network.TransportLayer
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * An on-disk message set. An optional start and end position can be applied to the message set
 * which will allow slicing a subset of the file.
 * @param file The file name for the underlying log data
 * @param channel the underlying file channel used
 * @param start A lower bound on the absolute position in the file from which the message set begins
 * @param end The upper bound on the absolute position in the file at which the message set ends
 * @param isSlice Should the start and end parameters be used for slicing?
 */
//对应磁盘上一个真正的日志文件
//todo 存储到日志文件中的消息必须是分区级别的绝对偏移量
@nonthreadsafe
class FileMessageSet private[kafka](@volatile var file: File,//指向磁盘上对应的日志文件
                                    private[log] val channel: FileChannel,//用于读写对应的日志文件
                                    private[log] val start: Int,//分片的起始位置
                                    private[log] val end: Int,//分片的结束位置
                                    isSlice: Boolean) extends MessageSet with Logging {

  /* the size of the message set in bytes */
  //文件大小
  private val _size =
    if(isSlice)
      new AtomicInteger(end - start) // don't check the file size if this is just a slice view
    else
      new AtomicInteger(math.min(channel.size.toInt, end) - start)

  /* if this is not a slice, update the file pointer to the end of the file */
  if (!isSlice)
    /* set the file position to the last byte in the file */
    channel.position(math.min(channel.size.toInt, end))

  /**
   * Create a file message set with no slicing.
   */
  def this(file: File, channel: FileChannel) =
    this(file, channel, start = 0, end = Int.MaxValue, isSlice = false)

  /**
   * Create a file message set with no slicing
   */
  def this(file: File) =
    this(file, FileMessageSet.openChannel(file, mutable = true))

  /**
   * Create a file message set with no slicing, and with initFileSize and preallocate.
   * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
   * with one value (for example 512 * 1024 *1024 ) can improve the kafka produce performance.
   * If it's new file and preallocate is true, end will be set to 0.  Otherwise set to Int.MaxValue.
   */
  def this(file: File, fileAlreadyExists: Boolean, initFileSize: Int, preallocate: Boolean) =
      this(file,
        //todo ============顺序写磁盘的原理就在这里 ============ 通过RandomAccessFile对应获得channel
        //todo ============顺序写磁盘的原理就在这里 ============ 通过RandomAccessFile对应获得channel
        //todo ============顺序写磁盘的原理就在这里 ============ 通过RandomAccessFile对应获得channel
        channel = FileMessageSet.openChannel(file, mutable = true, fileAlreadyExists, initFileSize, preallocate),
        start = 0,
        end = if (!fileAlreadyExists && preallocate) 0 else Int.MaxValue,
        isSlice = false)

  /**
   * Create a file message set with mutable option
   */
  def this(file: File, mutable: Boolean) = this(file, FileMessageSet.openChannel(file, mutable))

  /**
   * Create a slice view of the file message set that begins and ends at the given byte offsets
   */
  def this(file: File, channel: FileChannel, start: Int, end: Int) =
    this(file, channel, start, end, isSlice = true)

  /**
   * Search forward for the file position of the last offset that is greater than or equal to the target offset
   * and return its physical position and the size of the message (including log overhead) at the returned offset. If
   * no such offsets are found, return null.
   * @param targetOffset The offset to search for.
   * @param startingPosition The starting position in the file to begin searching from.
   */
  //todo 从指定的 startingPosition 开始逐条遍历FileMessageSet中的消息，并将每个消费的offset与targetOffset进行比较
  // 直到offset大于等于targetOffset,最后返回查找到的offset
  def searchForOffsetWithSize(targetOffset: Long, startingPosition: Int): (OffsetPosition, Int) = {
    //起始位置
    var position = startingPosition
    //用于读取LogOverhead 即 offset + size = 12  buffer = HeapByteBuffer
    val buffer = ByteBuffer.allocate(MessageSet.LogOverhead)
    //当前FileMessageSet大小，单位字节
    val size = sizeInBytes()
    while(position + MessageSet.LogOverhead < size) {
      //rewind会将指针移到缓冲区的开始位置，准备写入数据
      buffer.rewind()
      //从文件通道读取数据到缓冲区，缓冲区被填满后，缓冲区的指针到了末尾
      channel.read(buffer, position)
      if(buffer.hasRemaining)
        throw new IllegalStateException("Failed to read complete buffer for targetOffset %d startPosition %d in %s"
          .format(targetOffset, startingPosition, file.getAbsolutePath))
      //rewind会将指针移到缓冲区的开始位置，准备读取数据
      buffer.rewind()
      //绝对偏移量
      val offset = buffer.getLong()
      //消息大小
      val messageSize = buffer.getInt()
      if (messageSize < Message.MinMessageOverhead)
        throw new IllegalStateException("Invalid message size: " + messageSize)
      if (offset >= targetOffset) {
        //返回符合的
        return (OffsetPosition(offset, position), messageSize + MessageSet.LogOverhead)
      }
      //移动position 准备读取下一个消息  这样不用读取消息内容
      position += MessageSet.LogOverhead + messageSize
    }
    null
  }

  /**
   * Return a message set which is a view into this set starting from the given position and with the given size limit.
   *
   * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
   *
   * If this message set is already sliced, the position will be taken relative to that slicing.
   *
   * @param position The start position to begin the read from
   * @param size The number of bytes after the start position to include
   *
   * @return A sliced wrapper on this message set limited based on the given position and size
   */
  //读取文件消息集，给定起始位置和读取大小，创建一个新的文件消息集视图
  // 它的大小等于结束位置-开始位置，因为是一个视图，所以一旦创建后，文件消息集就固定下来了
  def read(position: Int, size: Int): FileMessageSet = {
    if(position < 0)
      throw new IllegalArgumentException("Invalid position: " + position)
    if(size < 0)
      throw new IllegalArgumentException("Invalid size: " + size)
    new FileMessageSet(file,
                       channel,
                       start = this.start + position,
                       end = {
                         // Handle the integer overflow
                         if (this.start + position + size < 0)
                           sizeInBytes()
                         else
                           math.min(this.start + position + size, sizeInBytes())
                       })
  }

  /**
   * Search forward for the message whose timestamp is greater than or equals to the target timestamp.
   *
   * @param targetTimestamp The timestamp to search for.
   * @param startingPosition The starting position to search.
   * @return The timestamp and offset of the message found. None, if no message is found.
   */
  def searchForTimestamp(targetTimestamp: Long, startingPosition: Int): Option[TimestampOffset] = {
    val messagesToSearch = read(startingPosition, sizeInBytes)
    for (messageAndOffset <- messagesToSearch) {
      val message = messageAndOffset.message
      if (message.timestamp >= targetTimestamp) {
        // We found a message
        message.compressionCodec match {
          case NoCompressionCodec =>
            return Some(TimestampOffset(messageAndOffset.message.timestamp, messageAndOffset.offset))
          case _ =>
            // Iterate over the inner messages to get the exact offset.
            for (innerMessageAndOffset <- ByteBufferMessageSet.deepIterator(messageAndOffset)) {
              val timestamp = innerMessageAndOffset.message.timestamp
              if (timestamp >= targetTimestamp)
                return Some(TimestampOffset(innerMessageAndOffset.message.timestamp, innerMessageAndOffset.offset))
            }
            throw new IllegalStateException(s"The message set (max timestamp = ${message.timestamp}, max offset = ${messageAndOffset.offset}" +
                s" should contain target timestamp $targetTimestamp but it does not.")
        }
      }
    }
    None
  }

  /**
   * Return the largest timestamp of the messages after a given position in this file message set.
   * @param startingPosition The starting position.
   * @return The largest timestamp of the messages after the given position.
   */
  def largestTimestampAfter(startingPosition: Int): TimestampOffset = {
    var maxTimestamp = Message.NoTimestamp
    var offsetOfMaxTimestamp = -1L
    val messagesToSearch = read(startingPosition, Int.MaxValue)
    for (messageAndOffset <- messagesToSearch) {
      if (messageAndOffset.message.timestamp > maxTimestamp) {
        maxTimestamp = messageAndOffset.message.timestamp
        offsetOfMaxTimestamp = messageAndOffset.offset
      }
    }
    TimestampOffset(maxTimestamp, offsetOfMaxTimestamp)
  }

  /**
   * Write some of this set to the given channel.
   * @param destChannel The channel to write to.
   * @param writePosition The position in the message set to begin writing from.
   * @param size The maximum number of bytes to write
   * @return The number of bytes actually written.
   */
  //将文件消息集的数据传输到目标通道中，通常用于文件读取
  def writeTo(destChannel: GatheringByteChannel, writePosition: Long, size: Int): Int = {
    // Ensure that the underlying size has not changed.
    val newSize = math.min(channel.size.toInt, end) - start
    if (newSize < _size.get()) {
      throw new KafkaException("Size of FileMessageSet %s has been truncated during write: old size %d, new size %d"
        .format(file.getAbsolutePath, _size.get(), newSize))
    }
    val position = start + writePosition
    val count = math.min(size, sizeInBytes)
    val bytesTransferred = (destChannel match {
      //todo 会走这里 transferFrom 也是调用 transferTo方法 将当前文件通道的字节直接传输到网络通道  文件通道的零拷贝传输
      case tl: TransportLayer => tl.transferFrom(channel, position, count)
      //todo 将当前文件通道的字节直接传输到可写通道     文件通道的零拷贝传输
      case dc => channel.transferTo(position, count, dc)
    }).toInt
    trace("FileMessageSet " + file.getAbsolutePath + " : bytes transferred : " + bytesTransferred
      + " bytes requested for transfer : " + math.min(size, sizeInBytes))
    bytesTransferred
  }

  /**
    * This method is called before we write messages to the socket using zero-copy transfer. We need to
    * make sure all the messages in the message set have the expected magic value.
    *
    * @param expectedMagicValue the magic value expected
    * @return true if all messages have expected magic value, false otherwise
    */
  override def isMagicValueInAllWrapperMessages(expectedMagicValue: Byte): Boolean = {
    var location = start
    val offsetAndSizeBuffer = ByteBuffer.allocate(MessageSet.LogOverhead)
    val crcAndMagicByteBuffer = ByteBuffer.allocate(Message.CrcLength + Message.MagicLength)
    while (location < end) {
      offsetAndSizeBuffer.rewind()
      channel.read(offsetAndSizeBuffer, location)
      if (offsetAndSizeBuffer.hasRemaining)
        return true
      offsetAndSizeBuffer.rewind()
      offsetAndSizeBuffer.getLong // skip offset field
      val messageSize = offsetAndSizeBuffer.getInt
      if (messageSize < Message.MinMessageOverhead)
        throw new IllegalStateException("Invalid message size: " + messageSize)
      crcAndMagicByteBuffer.rewind()
      channel.read(crcAndMagicByteBuffer, location + MessageSet.LogOverhead)
      if (crcAndMagicByteBuffer.get(Message.MagicOffset) != expectedMagicValue)
        return false
      location += (MessageSet.LogOverhead + messageSize)
    }
    true
  }

  /**
   * Convert this message set to use the specified message format.
   */
  def toMessageFormat(toMagicValue: Byte): MessageSet = {
    val offsets = new ArrayBuffer[Long]
    val newMessages = new ArrayBuffer[Message]
    this.foreach { messageAndOffset =>
      val message = messageAndOffset.message
      if (message.compressionCodec == NoCompressionCodec) {
        newMessages += message.toFormatVersion(toMagicValue)
        offsets += messageAndOffset.offset
      } else {
        // File message set only has shallow iterator. We need to do deep iteration here if needed.
        val deepIter = ByteBufferMessageSet.deepIterator(messageAndOffset)
        for (innerMessageAndOffset <- deepIter) {
          newMessages += innerMessageAndOffset.message.toFormatVersion(toMagicValue)
          offsets += innerMessageAndOffset.offset
        }
      }
    }

    if (sizeInBytes > 0 && newMessages.isEmpty) {
      // This indicates that the message is too large. We just return all the bytes in the file message set.
      this
    } else {
      // We use the offset seq to assign offsets so the offset of the messages does not change.
      new ByteBufferMessageSet(
        compressionCodec = this.headOption.map(_.message.compressionCodec).getOrElse(NoCompressionCodec),
        offsetSeq = offsets,
        newMessages: _*)
    }
  }

  /**
   * Get a shallow iterator over the messages in the set.
   */
  override def iterator: Iterator[MessageAndOffset] = iterator(Int.MaxValue)

  /**
   * Get an iterator over the messages in the set. We only do shallow iteration here.
   * @param maxMessageSize A limit on allowable message size to avoid allocating unbounded memory.
   * If we encounter a message larger than this we throw an InvalidMessageException.
   * @return The iterator.
   */
  //提供迭代器，顺序读取MessageSet中的消息
  def iterator(maxMessageSize: Int): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var location = start
      val sizeOffsetLength = 12
      val sizeOffsetBuffer = ByteBuffer.allocate(sizeOffsetLength)

      override def makeNext(): MessageAndOffset = {
        if(location + sizeOffsetLength >= end)
          return allDone()

        // read the size of the item
        sizeOffsetBuffer.rewind()
        channel.read(sizeOffsetBuffer, location)
        if(sizeOffsetBuffer.hasRemaining)
          return allDone()

        sizeOffsetBuffer.rewind()
        val offset = sizeOffsetBuffer.getLong()
        val size = sizeOffsetBuffer.getInt()
        if(size < Message.MinMessageOverhead || location + sizeOffsetLength + size > end)
          return allDone()
        if(size > maxMessageSize)
          throw new CorruptRecordException("Message size exceeds the largest allowable message size (%d).".format(maxMessageSize))

        // read the item itself
        val buffer = ByteBuffer.allocate(size)
        channel.read(buffer, location + sizeOffsetLength)
        if(buffer.hasRemaining)
          return allDone()
        buffer.rewind()

        // increment the location and return the item
        location += size + sizeOffsetLength
        new MessageAndOffset(new Message(buffer), offset)
      }
    }
  }

  /**
   * The number of bytes taken up by this file set
   */
  def sizeInBytes(): Int = _size.get()

  /**
   * Append these messages to the message set
   */
  def append(messages: ByteBufferMessageSet) {
    //todo 写文件
    val written: Int = messages.writeFullyTo(channel)
    //修改FileMessageSet的大小
    _size.getAndAdd(written)
  }

  /**
   * Commit all written data to the physical disk
   */
  def flush() = {
    channel.force(true)
  }

  /**
   * Close this message set
   */
  def close() {
    flush()
    trim()
    channel.close()
  }

  /**
   * Trim file when close or roll to next file
   */
  def trim() {
    truncateTo(sizeInBytes())
  }

  /**
   * Delete this message set from the filesystem
   * @return True iff this message set was deleted.
   */
  def delete(): Boolean = {
    CoreUtils.swallow(channel.close())
    file.delete()
  }

  /**
   * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
   * given size falls on a valid message boundary.
   * In some versions of the JDK truncating to the same size as the file message set will cause an
   * update of the files mtime, so truncate is only performed if the targetSize is smaller than the
   * size of the underlying FileChannel.
   * It is expected that no other threads will do writes to the log when this function is called.
   * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes.
   * @return The number of bytes truncated off
   */
  def truncateTo(targetSize: Int): Int = {
    val originalSize = sizeInBytes
    if(targetSize > originalSize || targetSize < 0)
      throw new KafkaException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                               " size of this log segment is " + originalSize + " bytes.")
    if (targetSize < channel.size.toInt) {
      channel.truncate(targetSize)
      channel.position(targetSize)
      _size.set(targetSize)
    }
    originalSize - targetSize
  }

  /**
   * Read from the underlying file into the buffer starting at the given position
   */
  def readInto(buffer: ByteBuffer, relativePosition: Int): ByteBuffer = {
    channel.read(buffer, relativePosition + this.start)
    buffer.flip()
    buffer
  }

  /**
   * Rename the file that backs this message set
   * @throws IOException if rename fails.
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally this.file = f
  }

}

object FileMessageSet
{
  /**
   * Open a channel for the given file
   * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
   * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
   * @param file File path
   * @param mutable mutable
   * @param fileAlreadyExists File already exists or not
   * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
   * @param preallocate Pre allocate file or not, gotten from configuration.
   */
  def openChannel(file: File, mutable: Boolean, fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false): FileChannel = {
    if (mutable) {
      //创建
      if (fileAlreadyExists)
        //todo 返回对象是FileChannelImpl
        new RandomAccessFile(file, "rw").getChannel()
      else {
        //preallocate 决定是否开启预分配的功能
        if (preallocate) {
          //todo 对应新创建的的且进行预分配空间的日志文件，其end会初始化为0
          val randomAccessFile = new RandomAccessFile(file, "rw")
          randomAccessFile.setLength(initFileSize)
          randomAccessFile.getChannel()
        } else {
          new RandomAccessFile(file, "rw").getChannel()
        }
      }
    }
    else {
      //创建只读的FileChannel
      new FileInputStream(file).getChannel()
    }
  }
}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
