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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, long)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * This class also maintains a cache of the latest commit position for each of the assigned
 * partitions. This is updated through {@link #committed(TopicPartition, OffsetAndMetadata)} and can be used
 * to set the initial fetch position (e.g. {@link Fetcher resetOffset(TopicPartition)}.
 */
//消费者订阅状态 保存了分配给消费者的所有分区及其状态
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE = "Subscription to topics, partitions and pattern are mutually exclusive";

    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
        //NONE 初始值
        //AUTO_TOPICS 自动分配分区
        //AUTO_PATTERN 按指定正则表达式进行自动分配分区
        //USER_ASSIGNED 手动指定消费的topic
    }

    /* the type of subscription */
    //内部枚举类 订阅类型
    private SubscriptionType subscriptionType;

    //正则表达式订阅的主题
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    //消费者订阅的主题
    private Set<String> subscription;

    //用户分配的分区
    private Set<TopicPartition> userAssignment;

    /* the list of topics the group has subscribed to (set only for the leader on join group completion) */
    //todo 整个消费组订阅的主题
    private final Set<String> groupSubscription;

    //todo 存储消费者当前分区的分区及状态
    // SubscriptionState保存了分配给消费者的所有分区及其状态
    private final PartitionStates<TopicPartitionState> assignment;

    //需要拉取偏移量标识
    private boolean needsFetchCommittedOffsets;

    //默认分区策略
    private final OffsetResetStrategy defaultResetStrategy;

    //消费再平衡监听器
    private ConsumerRebalanceListener listener;

    public SubscriptionState(OffsetResetStrategy defaultResetStrategy) {
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = Collections.emptySet();
        this.userAssignment = Collections.emptySet();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    public void subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        //todo 设置订阅类型 自动分配分区
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);
        this.listener = listener;
        changeSubscription(topics);
    }

    public void subscribeFromPattern(Set<String> topics) {
        if (subscriptionType != SubscriptionType.AUTO_PATTERN)
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);

        changeSubscription(topics);
    }

    private void changeSubscription(Set<String> topicsToSubscribe) {
        //判断 subscription 跟 topicsToSubscribe 是否完全相同
        if (!this.subscription.equals(topicsToSubscribe)) {
            this.subscription = topicsToSubscribe;
            this.groupSubscription.addAll(topicsToSubscribe);
        }
    }

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     * @param topics The topics to add to the group subscription
     */
    public void groupSubscribe(Collection<String> topics) {
        if (this.subscriptionType == SubscriptionType.USER_ASSIGNED)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        this.groupSubscription.addAll(topics);
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    public void resetGroupSubscription() {
        //retain 保留，持有的意思
        this.groupSubscription.retainAll(subscription);
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public void assignFromUser(Set<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        if (!this.assignment.partitionSet().equals(partitions)) {
            this.userAssignment = partitions;

            Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
            for (TopicPartition partition : partitions) {
                TopicPartitionState state = assignment.stateValue(partition);
                if (state == null)
                    state = new TopicPartitionState();
                partitionToState.put(partition, state);
            }
            this.assignment.set(partitionToState);
            this.needsFetchCommittedOffsets = true;
        }
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator,
     * note this is different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs
     */
    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.partitionsAutoAssigned())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        for (TopicPartition tp : assignments)
            if (!this.subscription.contains(tp.topic()))
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");

        // after rebalancing, we always reinitialize the assignment value
        this.assignment.set(partitionToStateMap(assignments));
        this.needsFetchCommittedOffsets = true;
    }

    private Map<TopicPartition, TopicPartitionState> partitionToStateMap(Collection<TopicPartition> assignments) {
        Map<TopicPartition, TopicPartitionState> map = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments)
            map.put(tp, new TopicPartitionState());
        return map;
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        setSubscriptionType(SubscriptionType.AUTO_PATTERN);

        this.listener = listener;
        this.subscribedPattern = pattern;
    }

    public boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    public void unsubscribe() {
        this.subscription = Collections.emptySet();
        this.userAssignment = Collections.emptySet();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    public Pattern getSubscribedPattern() {
        return this.subscribedPattern;
    }

    public Set<String> subscription() {
        return this.subscription;
    }

    public Set<TopicPartition> pausedPartitions() {
        HashSet<TopicPartition> paused = new HashSet<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (state.value().paused) {
                paused.add(state.topicPartition());
            }
        }
        return paused;
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    public Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    //获取指定分区的状态对象
    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    //更新分区的消费偏移量
    public void committed(TopicPartition tp, OffsetAndMetadata offset) {
        assignedState(tp).committed(offset);
    }

    //获取分区最新提交的偏移量
    public OffsetAndMetadata committed(TopicPartition tp) {
        return assignedState(tp).committed;
    }

    public void needRefreshCommits() {
        this.needsFetchCommittedOffsets = true;
    }

    public boolean refreshCommitsNeeded() {
        return this.needsFetchCommittedOffsets;
    }

    public void commitsRefreshed() {
        this.needsFetchCommittedOffsets = false;
    }

    //定位到分区的指定位置，更新拉取偏移量
    public void seek(TopicPartition tp, long offset) {
        assignedState(tp).seek(offset);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignment.partitionSet();
    }

    //获取允许拉取，即存在拉取偏移量的分区，用来构建拉取请求
    public List<TopicPartition> fetchablePartitions() {
        List<TopicPartition> fetchable = new ArrayList<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (state.value().isFetchable())
                fetchable.add(state.topicPartition());
        }
        return fetchable;
    }

    public boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    //设置分区的拉取偏移量
    public void position(TopicPartition tp, long offset) {
        assignedState(tp).position(offset);
    }

    //获取分区拉取的偏移量
    public Long position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    //所有已经消费的分区偏移量
    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (state.value().hasValidPosition())
                // <主题分区，偏移量元数据>
                allConsumed.put(state.topicPartition(), new OffsetAndMetadata(state.value().position));
        }
        return allConsumed;
    }

    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        //获取分区状态
        TopicPartitionState topicPartitionState = assignedState(partition);
        //重置拉取偏移量策略
        topicPartitionState.awaitReset(offsetResetStrategy);
    }

    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, defaultResetStrategy);
    }

    public boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    //todo 判断是否所有的分区都存在有效的拉取偏移量
    public boolean hasAllFetchPositions() {
        for (TopicPartitionState state : assignment.partitionStateValues())
            if (!state.hasValidPosition())
                return false;
        return true;
    }

    //todo 没有拉取偏移量的分区
    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> missing = new HashSet<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (!state.value().hasValidPosition())
                missing.add(state.topicPartition());
        }
        return missing;
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    //暂停拉取分区
    public void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    //恢复拉取分区
    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    public void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public ConsumerRebalanceListener listener() {
        return listener;
    }

    //todo 主题分区状态
    // 消费者拉取消息时要更新拉取偏移量
    // 处理消息时要更新提交偏移量
    private static class TopicPartitionState {
        //拉取偏移量 ，上次消费的位置，下次拉取就是从这个位置开始拉取 表示分区的拉取进度，它的值不为空，消费者才可以拉取这分区  默认值为null
        private Long position; // last consumed position
        //提交偏移量，上次提交的位置
        private OffsetAndMetadata committed;  // last committed position
        //分区是否被暂停拉取
        private boolean paused;
        //重置策略
        private OffsetResetStrategy resetStrategy;

        public TopicPartitionState() {
            this.paused = false;
            this.position = null;
            this.committed = null;
            this.resetStrategy = null;
        }

        //重置拉取偏移量策略
        private void awaitReset(OffsetResetStrategy strategy) {
            this.resetStrategy = strategy;
            this.position = null;
        }

        public boolean awaitingReset() {
            return resetStrategy != null;
        }

        public boolean hasValidPosition() {
            return position != null;
        }

        //从指定的位置拉取数据
        private void seek(long offset) {
            this.position = offset;
            this.resetStrategy = null;
        }

        //更新偏移量 更新拉取状态
        private void position(long offset) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = offset;
        }

        //更新提交偏移量 定时提交任务用 更新消费状态
        private void committed(OffsetAndMetadata offset) {
            this.committed = offset;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        //判断是否可以拉取  条件是 没有终止 并且拉取偏移量不为空
        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

    }

}
