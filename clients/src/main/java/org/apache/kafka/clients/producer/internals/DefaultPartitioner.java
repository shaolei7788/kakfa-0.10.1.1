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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The default partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose a partition in a round-robin fashion
 */
public class DefaultPartitioner implements Partitioner {
    //原子类,初始化的时候,给的一个随机数,是线程安全的
    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());
    //用于获取配置信息和初始化数据
    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name 主题
     * @param key The key to partition on (or null if no key) 键
     * @param keyBytes serialized key to partition on (or null if no key) 序列化之后的键
     * @param value The value to partition on or null 值
     * @param valueBytes serialized value to partition on or null 序列化之后的值
     * @param cluster The current cluster metadata 元数据信息
     */
    //该方法用来计算分区号,返回值为int类型
    //如果key不为null,那么计算得到的分区号会是所有分区号中的某一个
    //如果分区为null并且有可用的分区,那么计算得到的分区号仅为可用分区中的任意一个
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //首先获取到我们要发送消息的对应的topic的分区信息
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        //计算出来分区的总的个数
        int numPartitions = partitions.size();
        //策略一: 如果发送消息的时候,没有指定的key
        if (keyBytes == null) {
            //这里有一个计数器,每次执行都会加1
            int nextValue = counter.getAndIncrement();
            //获取主题中可用的分区
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                //计算消息发送到哪个分区
                //实现一个轮询的效果,达到消息的负载均衡
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                //根据取模的值分配分区
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            //策略二: 这个地方指定了key
            // hash the keyBytes to choose a partition
            //直接对key的hash值 % 分区数量
            //如果是同一个key,计算出来的分区肯定是同一个
            //如果我们想让消息发送到同一个分区上,那么我们必须指定同一个key,这点非常重要
            //murmur2算法具有高运算性能和低碰撞率
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    public void close() {}

}
