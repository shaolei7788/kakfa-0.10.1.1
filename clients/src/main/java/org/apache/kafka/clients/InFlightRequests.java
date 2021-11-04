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
package org.apache.kafka.clients;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 */
final class InFlightRequests {

    //5
    private final int maxInFlightRequestsPerConnection;
    private final Map<String, Deque<ClientRequest>> requests = new HashMap<>();

    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * Add the given request to the queue for the connection it was directed to
     */
    public void add(ClientRequest request) {
        // request.request().destination() 节点id
        Deque<ClientRequest> reqs = this.requests.get(request.request().destination());
        if (reqs == null) {
            reqs = new ArrayDeque<>();
            this.requests.put(request.request().destination(), reqs);
        }
        reqs.addFirst(request);
    }

    /**
     * Get the request queue for the given node
     * 获取给定节点的请求队列
     */
    private Deque<ClientRequest> requestQueue(String node) {
        Deque<ClientRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty())
            throw new IllegalStateException("Response from server for which there are no in-flight requests.");
        return reqs;
    }

    /**
     * Get the oldest request (the one that that will be completed next) for the given node
     * 从给定的节点中获得已经发送的请求列表,检索并删除请求列表的最后一个元素
     */
    //获取给定节点node的时间最久执行中请求
    // 根据给定节点node获取客户端请求双端队列reqs，并从poll出队尾元素
    // add时是通过addFirst()方法添加到队首的，所以队尾的元素是时间最久的，也是应该先处理的
    public ClientRequest completeNext(String node) {
        return requestQueue(node).pollLast();
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     * @param node The node id
     */
    public ClientRequest lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    /**
     * Complete the last request that was sent to a particular node.
     * @param node The node the request was sent to
     * @return The request
     */
    public ClientRequest completeLastSent(String node) {
        return requestQueue(node).pollFirst();
    }

    /**
     * Can we send more requests to this node?
     * 
     * @param node Node in question
     * @return true iff we have no requests still being sent to the given node
     */
    public boolean canSendMore(String node) {
        Deque<ClientRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty() ||
                // maxInFlightRequestsPerConnection = 5
               (queue.peekFirst().request().completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    /**
     * Return the number of inflight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    public int inFlightRequestCount(String node) {
        Deque<ClientRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Count all in-flight requests for all nodes
     */
    public int inFlightRequestCount() {
        int total = 0;
        for (Deque<ClientRequest> deque : this.requests.values())
            total += deque.size();
        return total;
    }

    /**
     * Clear out all the in-flight requests for the given node and return them
     * 
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    public Iterable<ClientRequest> clearAll(String node) {
        Deque<ClientRequest> reqs = requests.get(node);
        if (reqs == null) {
            return Collections.emptyList();
        } else {
            return requests.remove(node);
        }
    }

    /**
     * Returns a list of nodes with pending inflight request, that need to be timed out
     *
     * @param now current time in milliseconds
     * @param requestTimeout max time to wait for the request to be completed
     * @return list of nodes
     */
    //获取到请求超时的broker集合
    public List<String> getNodesWithTimedOutRequests(long now, int requestTimeout) {
        List<String> nodeIds = new LinkedList<>();
        for (Map.Entry<String, Deque<ClientRequest>> requestEntry : requests.entrySet()) {
            String nodeId = requestEntry.getKey();
            Deque<ClientRequest> deque = requestEntry.getValue();

            if (!deque.isEmpty()) {
                ClientRequest request = deque.peekLast();
                long timeSinceSend = now - request.sendTimeMs();
                if (timeSinceSend > requestTimeout)
                    nodeIds.add(nodeId);
            }
        }

        return nodeIds;
    }
}
