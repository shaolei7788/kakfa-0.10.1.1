/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common;

import org.apache.kafka.common.utils.Utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
 * 该对象包含Kafka集群中节点、主题和分区的子集
 */
public final class Cluster {

    //是否已经引导配置
    private final boolean isBootstrapConfigured;
    //我们一个kafka集群是有多个节点,这个参数代表的就是kafka的服务器信息
    private final List<Node> nodes;
    //没有授权的topic
    private final Set<String> unauthorizedTopics;
    //内部主题
    private final Set<String> internalTopics;
    /**
     * 这里有很多数据结构,这些数据结构里面有些数据可能是冗余的
     * 这里面用到的思想就是用空间换时间,设置适当的冗余的数据结构,可以提升性能
     */
    //一个partition和partition的相关信息之间的对应关系 因为partition有副本
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    //topic对应哪些分区
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    //topic对应哪些可用的分区
    private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
    //一台服务器上和这台服务器上有哪些partition,key用的是服务器的编号
    //我们搭建kafka集群的时候设置的broker.id,一般从0开始 每个broker都有一个唯一的id值用来区分彼此
    //默认情况下broker.id值为-1。Kafka broker的id值必须大于等于0时才有可能正常启动
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    //服务器编号和服务器对应的关系
    private final Map<Integer, Node> nodesById;
    //kafka集群的id信息
    private final ClusterResource clusterResource;

    /**
     * Create a new cluster with the given nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     * @deprecated Use the Cluster constructor with 5 parameters
     */
    @Deprecated
    public Cluster(Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics) {
        this(null, false, nodes, partitions, unauthorizedTopics, Collections.<String>emptySet());
    }


    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, internalTopics);
    }

    private Cluster(String clusterId,
                    boolean isBootstrapConfigured,
                    Collection<Node> nodes,
                    Collection<PartitionInfo> partitions,
                    Set<String> unauthorizedTopics,
                    Set<String> internalTopics) {
        this.isBootstrapConfigured = isBootstrapConfigured;
        this.clusterResource = new ClusterResource(clusterId);
        // make a randomized, unmodifiable copy of the nodes
        List<Node> copy = new ArrayList<>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);
        this.nodesById = new HashMap<>();
        for (Node node : nodes)
            this.nodesById.put(node.id(), node);

        // index the partitions by topic/partition for quick lookup
        this.partitionsByTopicPartition = new HashMap<>(partitions.size());
        for (PartitionInfo p : partitions)
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);

        // index the partitions by topic and node respectively, and make the lists
        // unmodifiable so we can hand them out in user-facing apis without risk
        // of the client modifying the contents
        HashMap<String, List<PartitionInfo>> partsForTopic = new HashMap<>();
        HashMap<Integer, List<PartitionInfo>> partsForNode = new HashMap<>();
        for (Node n : this.nodes) {
            partsForNode.put(n.id(), new ArrayList<PartitionInfo>());
        }
        for (PartitionInfo p : partitions) {
            if (!partsForTopic.containsKey(p.topic()))
                partsForTopic.put(p.topic(), new ArrayList<PartitionInfo>());
            List<PartitionInfo> psTopic = partsForTopic.get(p.topic());
            psTopic.add(p);

            if (p.leader() != null) {
                List<PartitionInfo> psNode = Utils.notNull(partsForNode.get(p.leader().id()));
                psNode.add(p);
            }
        }
        this.partitionsByTopic = new HashMap<>(partsForTopic.size());
        this.availablePartitionsByTopic = new HashMap<>(partsForTopic.size());
        for (Map.Entry<String, List<PartitionInfo>> entry : partsForTopic.entrySet()) {
            String topic = entry.getKey();
            List<PartitionInfo> partitionList = entry.getValue();
            this.partitionsByTopic.put(topic, Collections.unmodifiableList(partitionList));
            List<PartitionInfo> availablePartitions = new ArrayList<>();
            for (PartitionInfo part : partitionList) {
                if (part.leader() != null)
                    availablePartitions.add(part);
            }
            this.availablePartitionsByTopic.put(topic, Collections.unmodifiableList(availablePartitions));
        }
        this.partitionsByNode = new HashMap<>(partsForNode.size());
        for (Map.Entry<Integer, List<PartitionInfo>> entry : partsForNode.entrySet())
            this.partitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));

        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
        this.internalTopics = Collections.unmodifiableSet(internalTopics);
    }

    /**
     * Create an empty cluster instance with no nodes and no topic-partitions.
     */
    public static Cluster empty() {
        return new Cluster(null, new ArrayList<Node>(0), new ArrayList<PartitionInfo>(0), Collections.<String>emptySet(),
                Collections.<String>emptySet());
    }

    /**
     * Create a "bootstrap" cluster using the given list of host/ports
     * @param addresses The addresses
     * @return A cluster for these hosts/ports
     */
    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        return new Cluster(null, true, nodes, new ArrayList<PartitionInfo>(0), Collections.<String>emptySet(), Collections.<String>emptySet());
    }

    /**
     * Return a copy of this cluster combined with `partitions`.
     */
    public Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions) {
        Map<TopicPartition, PartitionInfo> combinedPartitions = new HashMap<>(this.partitionsByTopicPartition);
        combinedPartitions.putAll(partitions);
        return new Cluster(clusterResource.clusterId(), this.nodes, combinedPartitions.values(),
                new HashSet<>(this.unauthorizedTopics), new HashSet<>(this.internalTopics));
    }

    /**
     * @return The known set of nodes
     */
    public List<Node> nodes() {
        return this.nodes;
    }
    
    /**
     * Get the node by the node id (or null if no such node exists)
     * @param id The id of the node
     * @return The node, or null if no such node exists
     */
    public Node nodeById(int id) {
        return this.nodesById.get(id);
    }

    /**
     * Get the current leader for the given topic-partition
     * @param topicPartition The topic and partition we want to know the leader for
     * @return The node that is the leader for this topic-partition, or null if there is currently no leader
     */
    public Node leaderFor(TopicPartition topicPartition) {
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null)
            return null;
        else
            return info.leader();
    }

    /**
     * Get the metadata for the specified partition
     * @param topicPartition The topic and partition to fetch info for
     * @return The metadata about the given topic and partition
     */
    public PartitionInfo partition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.get(topicPartition);
    }

    /**
     * Get the list of partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForTopic(String topic) {
        return this.partitionsByTopic.get(topic);
    }

    /**
     * Get the list of available partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> availablePartitionsForTopic(String topic) {
        return this.availablePartitionsByTopic.get(topic);
    }

    /**
     * Get the list of partitions whose leader is this node
     * @param nodeId The node id
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return this.partitionsByNode.get(nodeId);
    }

    /**
     * Get the number of partitions for the given topic
     * 得到给定主题的分区数量
     * @param topic The topic to get the number of partitions for
     *              要获取其分区数的主题
     * @return The number of partitions or null if there is no corresponding metadata
     */
    public Integer partitionCountForTopic(String topic) {
        List<PartitionInfo> partitionInfos = this.partitionsByTopic.get(topic);
        return partitionInfos == null ? null : partitionInfos.size();
    }

    /**
     * Get all topics.
     * @return a set of all topics
     */
    public Set<String> topics() {
        return this.partitionsByTopic.keySet();
    }

    public Set<String> unauthorizedTopics() {
        return unauthorizedTopics;
    }

    public Set<String> internalTopics() {
        return internalTopics;
    }

    public boolean isBootstrapConfigured() {
        return isBootstrapConfigured;
    }

    public ClusterResource clusterResource() {
        return clusterResource;
    }

    @Override
    public String toString() {
        return "Cluster(id = " + clusterResource.clusterId() + ", nodes = " + this.nodes + ", partitions = " + this.partitionsByTopicPartition.values() + ")";
    }

}
