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

package kafka.message

import kafka.utils.{CoreUtils, IteratorTemplate, Logging}
import kafka.common.{KafkaException, LongRef}
import java.nio.ByteBuffer
import java.nio.channels._
import java.io._
import java.util.ArrayDeque

import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable

object ByteBufferMessageSet {

  private def create(offsetAssigner: OffsetAssigner,
                     compressionCodec: CompressionCodec,
                     wrapperMessageTimestamp: Option[Long],
                     timestampType: TimestampType,
                     messages: Message*): ByteBuffer = {
    if (messages.isEmpty)
      MessageSet.Empty.buffer
    else if (compressionCodec == NoCompressionCodec) {
      //没有使用压缩
      val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      for (message <- messages) {
        //为每个消息分配offset，并写入buffer
        writeMessage(buffer, message, offsetAssigner.nextAbsoluteOffset())
      }
      buffer.rewind()
      buffer
    } else {
      //使用了压缩
      val magicAndTimestamp = wrapperMessageTimestamp match {
        case Some(ts) => MagicAndTimestamp(messages.head.magic, ts)
        case None => MessageSet.magicAndLargestTimestamp(messages)
      }
      var offset = -1L
      val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      messageWriter.write(codec = compressionCodec, timestamp = magicAndTimestamp.timestamp, timestampType = timestampType, magicValue = magicAndTimestamp.magic) { outputStream =>
        val output = new DataOutputStream(CompressionFactory(compressionCodec, magicAndTimestamp.magic, outputStream))
        try {
          for (message <- messages) {
            offset = offsetAssigner.nextAbsoluteOffset()
            if (message.magic != magicAndTimestamp.magic)
              throw new IllegalArgumentException("Messages in the message set must have same magic value")
            // Use inner offset if magic value is greater than 0
            if (magicAndTimestamp.magic > Message.MagicValue_V0)
              output.writeLong(offsetAssigner.toInnerOffset(offset))
            else
              output.writeLong(offset)
            output.writeInt(message.size)
            output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          }
        } finally {
          output.close()
        }
      }
      val buffer = ByteBuffer.allocate(messageWriter.size + MessageSet.LogOverhead)
      writeMessage(buffer, messageWriter, offset)
      buffer.rewind()
      buffer
    }
  }

  /** Deep iterator that decompresses the message sets and adjusts timestamp and offset if needed. */
  def deepIterator(wrapperMessageAndOffset: MessageAndOffset, ensureMatchingMagic: Boolean = false): Iterator[MessageAndOffset] = {

    import Message._

    new IteratorTemplate[MessageAndOffset] {

      val MessageAndOffset(wrapperMessage, wrapperMessageOffset) = wrapperMessageAndOffset

      if (wrapperMessage.payload == null)
        throw new KafkaException(s"Message payload is null: $wrapperMessage")

      val wrapperMessageTimestampOpt: Option[Long] =
        if (wrapperMessage.magic > MagicValue_V0) Some(wrapperMessage.timestamp) else None
      val wrapperMessageTimestampTypeOpt: Option[TimestampType] =
        if (wrapperMessage.magic > MagicValue_V0) Some(wrapperMessage.timestampType) else None

      var lastInnerOffset = -1L

      val messageAndOffsets = {
        val inputStream = new ByteBufferBackedInputStream(wrapperMessage.payload)
        val compressed = try {
          new DataInputStream(CompressionFactory(wrapperMessage.compressionCodec, wrapperMessage.magic, inputStream))
        } catch {
          case ioe: IOException =>
            throw new InvalidMessageException(s"Failed to instantiate input stream compressed with ${wrapperMessage.compressionCodec}", ioe)
        }

        val innerMessageAndOffsets = new ArrayDeque[MessageAndOffset]()
        try {
          while (true)
            innerMessageAndOffsets.add(readMessageFromStream(compressed))
        } catch {
          case eofe: EOFException =>
            // we don't do anything at all here, because the finally
            // will close the compressed input stream, and we simply
            // want to return the innerMessageAndOffsets
          case ioe: IOException =>
            throw new InvalidMessageException(s"Error while reading message from stream compressed with ${wrapperMessage.compressionCodec}", ioe)
        } finally {
          CoreUtils.swallow(compressed.close())
        }

        innerMessageAndOffsets
      }

      private def readMessageFromStream(compressed: DataInputStream): MessageAndOffset = {
        val innerOffset = compressed.readLong()
        val recordSize = compressed.readInt()

        if (recordSize < MinMessageOverhead)
          throw new InvalidMessageException(s"Message found with corrupt size `$recordSize` in deep iterator")

        // read the record into an intermediate record buffer (i.e. extra copy needed)
        val bufferArray = new Array[Byte](recordSize)
        compressed.readFully(bufferArray, 0, recordSize)
        val buffer = ByteBuffer.wrap(bufferArray)

        // Override the timestamp if necessary
        val newMessage = new Message(buffer, wrapperMessageTimestampOpt, wrapperMessageTimestampTypeOpt)

        // Due to KAFKA-4298, it is possible for the inner and outer magic values to differ. We ignore
        // this and depend on the outer message in order to decide how to compute the respective offsets
        // for the inner messages
        if (ensureMatchingMagic && newMessage.magic != wrapperMessage.magic)
          throw new InvalidMessageException(s"Compressed message has magic value ${wrapperMessage.magic} " +
            s"but inner message has magic value ${newMessage.magic}")

        lastInnerOffset = innerOffset
        MessageAndOffset(newMessage, innerOffset)
      }

      override def makeNext(): MessageAndOffset = {
        messageAndOffsets.pollFirst() match {
          case null => allDone()
          case nextMessage@ MessageAndOffset(message, offset) =>
            if (wrapperMessage.magic > MagicValue_V0) {
              val relativeOffset = offset - lastInnerOffset
              val absoluteOffset = wrapperMessageOffset + relativeOffset
              MessageAndOffset(message, absoluteOffset)
            } else {
              nextMessage
            }
        }
      }
    }
  }

  private[kafka] def writeMessage(buffer: ByteBuffer, message: Message, offset: Long) {
    buffer.putLong(offset)
    buffer.putInt(message.size)
    buffer.put(message.buffer)
    message.buffer.rewind()
  }

  private[kafka] def writeMessage(buffer: ByteBuffer, messageWriter: MessageWriter, offset: Long) {
    buffer.putLong(offset)
    buffer.putInt(messageWriter.size)
    messageWriter.writeTo(buffer)
  }
}

private object OffsetAssigner {

  def apply(offsetCounter: LongRef, size: Int): OffsetAssigner =
    new OffsetAssigner(offsetCounter.value to offsetCounter.addAndGet(size))

}

private class OffsetAssigner(offsets: Seq[Long]) {
  private var index = 0

  def nextAbsoluteOffset(): Long = {
    val result = offsets(index)
    index += 1
    result
  }

  def toInnerOffset(offset: Long): Long = offset - offsets.head

}

/**
 * A sequence of messages stored in a byte buffer
 *
 * There are two ways to create a ByteBufferMessageSet
 *
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 *
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 *
 *
 * Message format v1 has the following changes:
 * - For non-compressed messages, timestamp and timestamp type attributes have been added. The offsets of
 *   the messages remain absolute offsets.
 * - For compressed messages, timestamp and timestamp type attributes have been added and inner offsets (IO) are used
 *   for inner messages of compressed messages (see offset calculation details below). The timestamp type
 *   attribute is only set in wrapper messages. Inner messages always have CreateTime as the timestamp type in attributes.
 *
 * We set the timestamp in the following way:
 * For non-compressed messages: the timestamp and timestamp type message attributes are set and used.
 * For compressed messages:
 * 1. Wrapper messages' timestamp type attribute is set to the proper value
 * 2. Wrapper messages' timestamp is set to:
 *    - the max timestamp of inner messages if CreateTime is used
 *    - the current server time if wrapper message's timestamp = LogAppendTime.
 *      In this case the wrapper message timestamp is used and all the timestamps of inner messages are ignored.
 * 3. Inner messages' timestamp will be:
 *    - used when wrapper message's timestamp type is CreateTime
 *    - ignored when wrapper message's timestamp type is LogAppendTime
 * 4. Inner messages' timestamp type will always be ignored with one exception: producers must set the inner message
 *    timestamp type to CreateTime, otherwise the messages will be rejected by broker.
 *
 * Absolute offsets are calculated in the following way:
 * Ideally the conversion from relative offset(RO) to absolute offset(AO) should be:
 *
 * AO = AO_Of_Last_Inner_Message + RO
 *
 * However, note that the message sets sent by producers are compressed in a streaming way.
 * And the relative offset of an inner message compared with the last inner message is not known until
 * the last inner message is written.
 * Unfortunately we are not able to change the previously written messages after the last message is written to
 * the message set when stream compression is used.
 *
 * To solve this issue, we use the following solution:
 *
 * 1. When the producer creates a message set, it simply writes all the messages into a compressed message set with
 *    offset 0, 1, ... (inner offset).
 * 2. The broker will set the offset of the wrapper message to the absolute offset of the last message in the
 *    message set.
 * 3. When a consumer sees the message set, it first decompresses the entire message set to find out the inner
 *    offset (IO) of the last inner message. Then it computes RO and AO of previous messages:
 *
 *    RO = IO_of_a_message - IO_of_the_last_message
 *    AO = AO_Of_Last_Inner_Message + RO
 *
 * 4. This solution works for compacted message sets as well.
 *
 */
class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet with Logging {
  private var shallowValidByteCount = -1

  private[kafka] def this(compressionCodec: CompressionCodec,
                          offsetCounter: LongRef,
                          wrapperMessageTimestamp: Option[Long],
                          timestampType: TimestampType,
                          messages: Message*) {
    this(ByteBufferMessageSet.create(OffsetAssigner(offsetCounter, messages.size), compressionCodec,
      wrapperMessageTimestamp, timestampType, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, offsetCounter: LongRef, messages: Message*) {
    this(compressionCodec, offsetCounter, None, TimestampType.CREATE_TIME, messages:_*)
  }

  def this(compressionCodec: CompressionCodec, offsetSeq: Seq[Long], messages: Message*) {
    this(ByteBufferMessageSet.create(new OffsetAssigner(offsetSeq), compressionCodec,
      None, TimestampType.CREATE_TIME, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(compressionCodec, new LongRef(0L), messages: _*)
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, messages: _*)
  }

  def getBuffer = buffer

  private def shallowValidBytes: Int = {
    if (shallowValidByteCount < 0) {
      this.shallowValidByteCount = this.internalIterator(isShallow = true).map { messageAndOffset =>
        MessageSet.entrySize(messageAndOffset.message)
      }.sum
    }
    shallowValidByteCount
  }

  /** Write the messages in this set to the given channel */
  def writeFullyTo(channel: GatheringByteChannel): Int = {
    // FileChannel implements GatheringByteChannel
    //标记缓冲区
    buffer.mark()
    //写入字节数
    var written = 0
    while (written < sizeInBytes) {
      //将底层包含的消息内容的字节缓冲区写到文件通道,消息就持久化到日志分段对应的数据文件了
      written += channel.write(buffer)
    }
    //重置缓存区
    buffer.reset()
    written
  }

  /** Write the messages in this set to the given channel starting at the given offset byte.
    * Less than the complete amount may be written, but no more than maxSize can be. The number
    * of bytes written is returned */
  //todo 将当前MessageSet中的消息写入到Channel
  def writeTo(channel: GatheringByteChannel, offset: Long, maxSize: Int): Int = {
    if (offset > Int.MaxValue)
      throw new IllegalArgumentException(s"offset should not be larger than Int.MaxValue: $offset")
    val dup = buffer.duplicate()
    val position = offset.toInt
    dup.position(position)
    dup.limit(math.min(buffer.limit, position + maxSize))
    channel.write(dup)
  }

  override def isMagicValueInAllWrapperMessages(expectedMagicValue: Byte): Boolean = {
    for (messageAndOffset <- shallowIterator) {
      if (messageAndOffset.message.magic != expectedMagicValue)
        return false
    }
    true
  }

  /** default iterator that iterates over decompressed messages */
    //提供迭代器，顺序读取MessageSet中的消息
  override def iterator: Iterator[MessageAndOffset] = internalIterator()

  /** iterator over compressed messages without decompressing */
  //迭代压缩消息 没有解压
  def shallowIterator: Iterator[MessageAndOffset] = internalIterator(isShallow = true)

  /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. **/
  private def internalIterator(isShallow: Boolean = false, ensureMatchingMagic: Boolean = false): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var topIter = buffer.slice()
      var innerIter: Iterator[MessageAndOffset] = null

      def innerDone(): Boolean = (innerIter == null || !innerIter.hasNext)

      def makeNextOuter: MessageAndOffset = {
        // if there isn't at least an offset and size, we are done
        if (topIter.remaining < 12)
          return allDone()
        val offset = topIter.getLong()
        val size = topIter.getInt()
        if(size < Message.MinMessageOverhead)
          throw new InvalidMessageException("Message found with corrupt size (" + size + ") in shallow iterator")

        // we have an incomplete message
        if(topIter.remaining < size)
          return allDone()

        // read the current message and check correctness
        val message = topIter.slice()
        message.limit(size)
        topIter.position(topIter.position + size)
        val newMessage = new Message(message)
        if(isShallow) {
          MessageAndOffset(newMessage, offset)
        } else {
          newMessage.compressionCodec match {
            case NoCompressionCodec =>
              innerIter = null
              MessageAndOffset(newMessage, offset)
            case _ =>
              innerIter = ByteBufferMessageSet.deepIterator(MessageAndOffset(newMessage, offset), ensureMatchingMagic)
              if(!innerIter.hasNext)
                innerIter = null
              makeNext()
          }
        }
      }

      override def makeNext(): MessageAndOffset = {
        if(isShallow){
          makeNextOuter
        } else {
          if(innerDone())
            makeNextOuter
          else
            innerIter.next()
        }
      }

    }
  }

  /**
   * Update the offsets for this message set and do further validation on messages including:
   * 1. Messages for compacted topics must have keys
   * 2. When magic value = 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0.
   * 3. When magic value = 1, validate and maybe overwrite timestamps of messages.
   *
   * This method will convert the messages in the following scenarios:
   * A. Magic value of a message = 0 and messageFormatVersion is 1
   * B. Magic value of a message = 1 and messageFormatVersion is 0
   *
   * If no format conversion or value overwriting is required for messages, this method will perform in-place
   * operations and avoid re-compression.
   *
   * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp, the offset
   * of the shallow message with the max timestamp and a boolean indicating whether the message sizes may have changed.
   */
  private[kafka] def validateMessagesAndAssignOffsets(offsetCounter: LongRef,
                                                      now: Long,
                                                      sourceCodec: CompressionCodec,
                                                      targetCodec: CompressionCodec,
                                                      compactedTopic: Boolean = false,
                                                      messageFormatVersion: Byte = Message.CurrentMagicValue,
                                                      messageTimestampType: TimestampType,
                                                      messageTimestampDiffMaxMs: Long): ValidationAndOffsetAssignResult = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      //未使用压缩
      // check the magic value
      if (!isMagicValueInAllWrapperMessages(messageFormatVersion))
        convertNonCompressedMessages(offsetCounter, compactedTopic, now, messageTimestampType, messageTimestampDiffMaxMs, messageFormatVersion)
      else {
        // todo
        validateNonCompressedMessagesAndAssignOffsetInPlace(offsetCounter, now, compactedTopic, messageTimestampType, messageTimestampDiffMaxMs)
      }
    } else {
      // Deal with compressed messages
      // We cannot do in place assignment in one of the following situations:
      // 1. Source and target compression codec are different
      // 2. When magic value to use is 0 because offsets need to be overwritten
      // 3. When magic value to use is above 0, but some fields of inner messages need to be overwritten.
      // 4. Message format conversion is needed.

      // No in place assignment situation 1 and 2
      var inPlaceAssignment = sourceCodec == targetCodec && messageFormatVersion > Message.MagicValue_V0

      var maxTimestamp = Message.NoTimestamp
      var offsetOfMaxTimestamp = -1L
      val expectedInnerOffset = new LongRef(0)
      val validatedMessages = new mutable.ArrayBuffer[Message]

      this.internalIterator(isShallow = false, ensureMatchingMagic = true).foreach { messageAndOffset =>
        val message = messageAndOffset.message
        validateMessageKey(message, compactedTopic)

        if (message.magic > Message.MagicValue_V0 && messageFormatVersion > Message.MagicValue_V0) {
          // No in place assignment situation 3
          // Validate the timestamp
          validateTimestamp(message, now, messageTimestampType, messageTimestampDiffMaxMs)
          // Check if we need to overwrite offset
          if (messageAndOffset.offset != expectedInnerOffset.getAndIncrement())
            inPlaceAssignment = false
          if (message.timestamp > maxTimestamp) {
            maxTimestamp = message.timestamp
            offsetOfMaxTimestamp = offsetCounter.value + expectedInnerOffset.value - 1
          }
        }

        if (sourceCodec != NoCompressionCodec && message.compressionCodec != NoCompressionCodec)
          throw new InvalidMessageException("Compressed outer message should not have an inner message with a " +
            s"compression attribute set: $message")

        // No in place assignment situation 4
        if (message.magic != messageFormatVersion)
          inPlaceAssignment = false

        validatedMessages += message.toFormatVersion(messageFormatVersion)
      }

      if (!inPlaceAssignment) {
        // Cannot do in place assignment.
        val (largestTimestampOfMessageSet, offsetOfMaxTimestampInMessageSet) = {
          if (messageFormatVersion == Message.MagicValue_V0)
            (Some(Message.NoTimestamp), -1L)
          else if (messageTimestampType == TimestampType.CREATE_TIME)
            (Some(maxTimestamp), {if (targetCodec == NoCompressionCodec) offsetOfMaxTimestamp else offsetCounter.value + validatedMessages.length - 1})
          else // Log append time
            (Some(now), {if (targetCodec == NoCompressionCodec) offsetCounter.value else offsetCounter.value + validatedMessages.length - 1})
        }

        ValidationAndOffsetAssignResult(validatedMessages = new ByteBufferMessageSet(compressionCodec = targetCodec,
                                                                                     offsetCounter = offsetCounter,
                                                                                     wrapperMessageTimestamp = largestTimestampOfMessageSet,
                                                                                     timestampType = messageTimestampType,
                                                                                     messages = validatedMessages: _*),
                                        maxTimestamp = largestTimestampOfMessageSet.get,
                                        offsetOfMaxTimestamp = offsetOfMaxTimestampInMessageSet,
                                        messageSizeMaybeChanged = true)
      } else {
        // Do not do re-compression but simply update the offset, timestamp and attributes field of the wrapper message.
        buffer.putLong(0, offsetCounter.addAndGet(validatedMessages.size) - 1)
        // validate the messages
        validatedMessages.foreach(_.ensureValid())

        var crcUpdateNeeded = true
        val timestampOffset = MessageSet.LogOverhead + Message.TimestampOffset
        val attributeOffset = MessageSet.LogOverhead + Message.AttributesOffset
        val timestamp = buffer.getLong(timestampOffset)
        val attributes = buffer.get(attributeOffset)
        buffer.putLong(timestampOffset, maxTimestamp)
        if (messageTimestampType == TimestampType.CREATE_TIME && timestamp == maxTimestamp)
          // We don't need to recompute crc if the timestamp is not updated.
          crcUpdateNeeded = false
        else if (messageTimestampType == TimestampType.LOG_APPEND_TIME) {
          // Set timestamp type and timestamp
          buffer.putLong(timestampOffset, now)
          buffer.put(attributeOffset, messageTimestampType.updateAttributes(attributes))
        }

        if (crcUpdateNeeded) {
          // need to recompute the crc value
          buffer.position(MessageSet.LogOverhead)
          val wrapperMessage = new Message(buffer.slice())
          Utils.writeUnsignedInt(buffer, MessageSet.LogOverhead + Message.CrcOffset, wrapperMessage.computeChecksum)
        }
        buffer.rewind()
        // For compressed messages,
        ValidationAndOffsetAssignResult(validatedMessages = this,
                                        maxTimestamp = buffer.getLong(timestampOffset),
                                        offsetOfMaxTimestamp = buffer.getLong(0),
                                        messageSizeMaybeChanged = false)
      }
    }
  }

  // We create this method to avoid a memory copy. It reads from the original message set and directly
  // writes the converted messages into new message set buffer. Hence we don't need to allocate memory for each
  // individual message during message format conversion.
  private def convertNonCompressedMessages(offsetCounter: LongRef,
                                           compactedTopic: Boolean,
                                           now: Long,
                                           timestampType: TimestampType,
                                           messageTimestampDiffMaxMs: Long,
                                           toMagicValue: Byte): ValidationAndOffsetAssignResult = {
    val sizeInBytesAfterConversion = shallowValidBytes + this.internalIterator(isShallow = true).map { messageAndOffset =>
      Message.headerSizeDiff(messageAndOffset.message.magic, toMagicValue)
    }.sum
    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    var newMessagePosition = 0
    var maxTimestamp = Message.NoTimestamp
    var offsetOfMaxTimestamp = -1L
    this.internalIterator(isShallow = true).foreach { case MessageAndOffset(message, _) =>
      validateMessageKey(message, compactedTopic)
      validateTimestamp(message, now, timestampType, messageTimestampDiffMaxMs)
      newBuffer.position(newMessagePosition)
      newBuffer.putLong(offsetCounter.getAndIncrement())
      val newMessageSize = message.size + Message.headerSizeDiff(message.magic, toMagicValue)
      newBuffer.putInt(newMessageSize)
      val newMessageBuffer = newBuffer.slice()
      newMessageBuffer.limit(newMessageSize)
      message.convertToBuffer(toMagicValue, newMessageBuffer, now, timestampType)
      if (toMagicValue > Message.MagicValue_V0) {
        val timestamp = newMessageBuffer.getLong(Message.TimestampOffset)
        if (maxTimestamp < timestamp) {
          maxTimestamp = timestamp
          offsetOfMaxTimestamp = offsetCounter.value - 1
        }
      }
      newMessagePosition += MessageSet.LogOverhead + newMessageSize
    }
    newBuffer.rewind()
    new ValidationAndOffsetAssignResult(validatedMessages = new ByteBufferMessageSet(newBuffer),
                                        maxTimestamp = maxTimestamp,
                                        offsetOfMaxTimestamp = offsetOfMaxTimestamp,
                                        messageSizeMaybeChanged = true)
  }

  //更新每条消息的偏移量数据 将相对偏移量改为绝对偏移量
  private def validateNonCompressedMessagesAndAssignOffsetInPlace(offsetCounter: LongRef,
                                                                  now: Long,
                                                                  compactedTopic: Boolean,
                                                                  timestampType: TimestampType,
                                                                  timestampDiffMaxMs: Long): ValidationAndOffsetAssignResult = {
    // 偏移量  消息长度(字节)  消息内容(不固定) 消息占用大小  起始位置
    // 0        3             abc          8+4+3 =15   0
    // 1        5             bcdef        8+4+5 =17   15
    // 0        3             cdef         8+4+4 =16   15+17=32
    //todo 消息集中的第一条消息的偏移量一定是从字节缓冲区的0开始的，每条消息的长度 = 8 + 4 +消息大小
    var messagePosition = 0
    var maxTimestamp = Message.NoTimestamp
    var offsetOfMaxTimestamp = -1L
    buffer.mark()
    while (messagePosition < sizeInBytes - MessageSet.LogOverhead) {
      //定位每条消息的起始位置 刚进来就是0的位置  直接定位到指定的位置
      buffer.position(messagePosition)
      //todo 修改偏移量 以最新的偏移量计数器为基础，每条消息都的偏移量都在此基数上加1    putLong是修改操作不是添加操作
      buffer.putLong(offsetCounter.getAndIncrement())
      //消息的大小
      val messageSize = buffer.getInt()
      val messageBuffer = buffer.slice()
      messageBuffer.limit(messageSize)
      val message = new Message(messageBuffer)
      validateMessageKey(message, compactedTopic)
      if (message.magic > Message.MagicValue_V0) {
        validateTimestamp(message, now, timestampType, timestampDiffMaxMs)
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
          message.buffer.putLong(Message.TimestampOffset, now)
          message.buffer.put(Message.AttributesOffset, timestampType.updateAttributes(message.attributes))
          Utils.writeUnsignedInt(message.buffer, Message.CrcOffset, message.computeChecksum)
        }
        if (message.timestamp > maxTimestamp) {
          maxTimestamp = message.timestamp
          offsetOfMaxTimestamp = offsetCounter.value - 1
        }
      }
      //下一条消息的起始位置    12 + 消息大小 表示一条完整的消息
      messagePosition += MessageSet.LogOverhead + messageSize
    }
    //回到最开始标记的地方
    buffer.reset()
    ValidationAndOffsetAssignResult(validatedMessages = this,
                                    maxTimestamp = maxTimestamp,
                                    offsetOfMaxTimestamp = offsetOfMaxTimestamp,
                                    messageSizeMaybeChanged = false)
  }

  private def validateMessageKey(message: Message, compactedTopic: Boolean) {
    if (compactedTopic && !message.hasKey)
      throw new InvalidMessageException("Compacted topic cannot accept message without key.")
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is within acceptable range.
   */
  private def validateTimestamp(message: Message,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long) {
    if (timestampType == TimestampType.CREATE_TIME && math.abs(message.timestamp - now) > timestampDiffMaxMs)
      throw new InvalidTimestampException(s"Timestamp ${message.timestamp} of message is out of range. " +
        s"The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}")
    if (message.timestampType == TimestampType.LOG_APPEND_TIME)
      throw new InvalidTimestampException(s"Invalid timestamp type in message $message. Producer should not set " +
        s"timestamp type to LogAppendTime.")
  }

  /**
   * The total number of bytes in this message set, including any partial trailing messages
   */
  //字节缓冲区的大小
  def sizeInBytes: Int = buffer.limit

  /**
   * The total number of bytes in this message set not including any partial, trailing messages
   */
  def validBytes: Int = shallowValidBytes

  /**
   * Two message sets are equal if their respective byte buffers are equal
   */
  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode: Int = buffer.hashCode

}

case class ValidationAndOffsetAssignResult(validatedMessages: ByteBufferMessageSet,
                                           maxTimestamp: Long,
                                           offsetOfMaxTimestamp: Long,
                                           messageSizeMaybeChanged: Boolean)
