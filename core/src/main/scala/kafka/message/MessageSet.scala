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

import java.nio._
import java.nio.channels._

/**
 * Message set helper functions
 */
//todo 消息集中的每条消息由3部分组成： 偏移量 + 消息大小 + 消息内容
// 服务端在存储消息时可以直接修改字节缓冲区中每条消息的偏移量值，其它数据内容不变，字节缓冲区大小也不会变
// 如果客户端填充消息到缓冲区没有写入相对偏移量，服务端在存储消息时由于要保存消息的偏移量，就需要在字节缓冲区每条消息前添加相对偏移量才行
// 这种方式会修改字节缓冲区的大小，原来的字节缓冲区就不能使用了
object MessageSet {
  //标识消息长度是多少
  val MessageSizeLength = 4
  //标识偏移量长度是多少
  val OffsetLength = 8

  val LogOverhead = MessageSizeLength + OffsetLength
  val Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0))
  
  /**
   * The size of a message set containing the given messages
   */
  def messageSetSize(messages: Iterable[Message]): Int =
    messages.foldLeft(0)(_ + entrySize(_))

  /**
   * The size of a size-delimited entry in a message set
   */
  def entrySize(message: Message): Int = LogOverhead + message.size

  /**
   * Validate that all "magic" values in `messages` are the same and return their magic value and max timestamp
   */
  def magicAndLargestTimestamp(messages: Seq[Message]): MagicAndTimestamp = {
    val firstMagicValue = messages.head.magic
    var largestTimestamp = Message.NoTimestamp
    for (message <- messages) {
      if (message.magic != firstMagicValue)
        throw new IllegalStateException("Messages in the same message set must have same magic value")
      if (firstMagicValue > Message.MagicValue_V0)
        largestTimestamp = math.max(largestTimestamp, message.timestamp)
    }
    MagicAndTimestamp(firstMagicValue, largestTimestamp)
  }

}

case class MagicAndTimestamp(magic: Byte, timestamp: Long)

/**
 * A set of messages with offsets. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. The format of each message is
 * as follows:
 * 8 byte message offset number
 * 4 byte size containing an integer N
 * N message bytes as described in the Message class
 */
abstract class MessageSet extends Iterable[MessageAndOffset] {

  /** Write the messages in this set to the given channel starting at the given offset byte. 
    * Less than the complete amount may be written, but no more than maxSize can be. The number
    * of bytes written is returned */
  //将当前MessageSet写入当前channel
  def writeTo(channel: GatheringByteChannel, offset: Long, maxSize: Int): Int

  /**
   * Check if all the wrapper messages in the message set have the expected magic value
   */
  def isMagicValueInAllWrapperMessages(expectedMagicValue: Byte): Boolean

  /**
   * Provides an iterator over the message/offset pairs in this set
   */
  //提供迭代器，顺序读取MessageSet中的消息
  def iterator: Iterator[MessageAndOffset]
  
  /**
   * Gives the total size of this message set in bytes
   */
  def sizeInBytes: Int

  /**
   * Print this message set's contents. If the message set has more than 100 messages, just
   * print the first 100.
   */
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append(getClass.getSimpleName + "(")
    val iter = this.iterator
    var i = 0
    while(iter.hasNext && i < 100) {
      val message = iter.next
      builder.append(message)
      if(iter.hasNext)
        builder.append(", ")
      i += 1
    }
    if(iter.hasNext)
      builder.append("...")
    builder.append(")")
    builder.toString
  }

}
