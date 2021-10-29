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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    /* the selector used to perform network i/o */
    private final Selectable selector;

    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response */
    //缓存了还没有收到服务端响应的客户端请求
    // 当有新请求需要处理时，会在队首入列，而实际被处理的请求，则是从队尾出列，保证入列早的请求先得到处理
    private final InFlightRequests inFlightRequests;

    /* the socket send buffer size in bytes */
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* max time in ms for the producer to wait for acknowledgement from server*/
    private final int requestTimeoutMs;

    private final Time time;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time) {
        this(null, metadata, selector, clientId, maxInFlightRequestsPerConnection,
                reconnectBackoffMs, socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time) {
        this(metadataUpdater, null, selector, clientId, maxInFlightRequestsPerConnection, reconnectBackoffMs,
                socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time);
    }

    private NetworkClient(MetadataUpdater metadataUpdater,
                          Metadata metadata,
                          Selectable selector,
                          String clientId,
                          int maxInFlightRequestsPerConnection,
                          long reconnectBackoffMs,
                          int socketSendBuffer,
                          int socketReceiveBuffer,
                          int requestTimeoutMs,
                          Time time) {

        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.requestTimeoutMs = requestTimeoutMs;
        this.time = time;
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    public boolean ready(Node node, long now) {
        //如果当前的节点为null,就报异常
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);
        //检查给定的节点是否准备好发送请求。
        if (isReady(node, now))
            return true;
        //判断是否可以尝试去建立连接
        if (connectionStates.canConnect(node.idString(), now))
            //todo 允许连接 初始化连接 绑定连接事件
            initiateConnect(node, now);
        return false;
    }

    /**
     * Closes the connection to a particular node (if there is one).
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        selector.close(nodeId);
        for (ClientRequest request : inFlightRequests.clearAll(nodeId))
            metadataUpdater.maybeHandleDisconnection(request);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.connectionState(node.idString()).equals(ConnectionState.DISCONNECTED);
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     * 检查具有给定id的节点是否准备好发送更多请求。
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        //!metadataUpdater.isUpdateDue(now) 我们要发送写数据请求的时候,不能是正在更新元数据的时候
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString());
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     */

    /**
     * connectionStates.isConnected(node): 判断缓存里面是否已经把连接建立好了
     *
     * selector.isChannelReady(node):
     *      java NIO :selector
     *      selector--> 绑定了多个kafkaChannel(java socketChannel )
     *      一个kafkaChannel就代表一个连接
     *
     *
     * inFlightRequests.canSendMore(node):每个往broker主机上面发送请求的连接,最多能容忍5个没有响应
     *
     * @param node
     * @return
     */
    private boolean canSendRequest(String node) {
        return connectionStates.isConnected(node)
                && selector.isChannelReady(node)
                // 会判断请求个数
                && inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     *
     * @param request The request
     * @param now The current timestamp
     */
    //为每个节点创建一个客户端请求，将请求暂存到节点对应的通道里
    @Override
    public void send(ClientRequest request, long now) {
        String nodeId = request.request().destination();
        if (!canSendRequest(nodeId))
            throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        // TODO 关键代码
        doSend(request, now);
    }


    private void doSend(ClientRequest request, long now) {
        //设置发送时间
        request.setSendTimeMs(now);
        //往inFlightRequests组件里存储request请求
        //其实就是存储还没有收到响应的请求
        //默认最多存储5个请求
        //如果inFlightRequests里面的请求成功发送出去了并且成功接收到了响应,那么这个请求会被移除
        //todo inFlightRequests缓存了还没有收到服务端响应的客户端请求
        // 包含一个节点到双端队列的Map类型  inFlightRequests.requests = Map<String, Deque<ClientRequest>>
        // 在准备发送请求时，请求将添加到指定节点对应的队列中，收到响应后，才会将该请求从队列中移除
        this.inFlightRequests.add(request);
        // request.request() = RequestSend
        selector.send(request.request());
    }

    /**
     * Do actual reads and writes to sockets.
     * 执行读写操作
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        //步骤一: 封装一个要拉取元数据的请求  DefaultMetadataUpdater#maybeUpdate
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            //步骤二: 不仅有发送请求,也有接收响应 会进行网络读写
            //TODO 执行网络IO的操作
            this.selector.poll(Utils.min(timeout, metadataTimeout, requestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        //不需要响应的流程。 开始发送请求 -> 添加到客户端请求队列(InFlightRequests) -> 发送请求 -> 请求发送成功 -> 从队列中删除发送请求 -> 构造客户端响应 ->
        //需要响应的流程。   开始发送请求 -> 添加到客户端请求队列(InFlightRequests) -> 发送请求 -> 请求发送成功 -> 等待接收响应 -> 接收响应 -> 接收到完整的响应
        // -> 从队列中删除发送请求 -> 构造客户端响应 ->

        // process completed actions
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        //处理完成发送的请求
        handleCompletedSends(responses, updatedNow);
        //在上一步this.selector.poll 已经发送出去了请求,那么在这里就来处理响应
        //步骤三: 处理响应,响应里面就会有我们需要的元数据
        /**
         * kafka获取元数据的流程跟发送消息的流程是一模一样的
         * 获取元数据--> 判断网络连接是否建立好--> 建立网络连接--> 发送请求(获取元数据的请求)--> 服务端发送回来响应
         */
        //处理完成接收
        handleCompletedReceives(responses, updatedNow);
        //处理断开的连接
        handleDisconnections(responses, updatedNow);
        handleConnections();
        //TODO 处理长时间没有接收到响应的请求
        handleTimedOutRequests(responses, updatedNow);

        // 上面几个处理操作都会往 responses 中添加响应，有了响应后开始调用请求的回调函数
        // invoke callbacks
        for (ClientResponse response : responses) {
            if (response.request().hasCallback()) {
                try {
                    //todo 获取响应对应的客户端请求
                    ClientRequest clientRequest = response.request();
                    //响应对象中封装有最初的请求对象,
                    //可以直接调用请求对象中的回调函数
                    //生产者回调对象是RequestCompletionHandler
                    //消费者回调对象是RequestFutureCompletionHandler
                    //RequestFutureCompletionHandler implements RequestCompletionHandler
                    RequestCompletionHandler callback = clientRequest.callback();
                    //todo 这个callback 即可能是生产者也可能是消费者的回调对象
                    // 此处调用的是网络请求的回调函数,不是业务代码里面我们自己的回调函数
                    // 将response响应放入pendingCompletion
                    callback.onComplete(response);
                } catch (Exception e) {
                    log.error("Uncaught error in request completion:", e);
                }
            }
        }
        return responses;
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.inFlightRequestCount();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.inFlightRequestCount(node);
    }

    /**
     * Generate a request header for the given API key
     *
     * @param key The api key
     * @return A request header with the appropriate client id and correlation id
     */
    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        // correlation 相关性，自增
        return new RequestHeader(key.id, clientId, correlation++);
    }

    /**
     * Generate a request header for the given API key and version
     *
     * @param key The api key
     * @param version The api version
     * @return A request header with the appropriate client id and correlation id
     */
    @Override
    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return new RequestHeader(key.id, version, clientId, correlation++);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        this.selector.close();
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period.
     *
     * @return The node with the fewest in-flight requests.
     */
    //todo 查找集群负载最低的一个node 通过查找inFlightRequests 中未确认请求最少的的节点
    @Override
    public Node leastLoadedNode(long now) {
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        int inflight = Integer.MAX_VALUE;
        Node found = null;

        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            int currInflight = this.inFlightRequests.inFlightRequestCount(node.idString());
            if (currInflight == 0 && this.connectionStates.isConnected(node.idString())) {
                // if we find an established connection with no in-flight requests we can stop right away
                return node;
            } else if (!this.connectionStates.isBlackedOut(node.idString(), now) && currInflight < inflight) {
                // otherwise if this is the best we have found so far, record that
                inflight = currInflight;
                found = node;
            }
        }

        return found;
    }

    //todo 解析服务端发送回来的请求(里面有响应的结果数据)  receive.payload() = 响应体  req.request().header() 请求头
    public static Struct parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        //todo 解析响应buffer,获取响应头
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);
        // Always expect the response version id to be the same as the request version id
        short apiKey = requestHeader.apiKey();
        short apiVer = requestHeader.apiVersion();
        //todo 解析响应buffer,获取响应体  ProtoUtils.responseSchema(apiKey, apiVer) 获取指定apikey对应的Schema
        Struct responseBody = ProtoUtils.responseSchema(apiKey, apiVer).read(responseBuffer);
        correlate(requestHeader, responseHeader);
        return responseBody;
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     */
    private void processDisconnection(List<ClientResponse> responses, String nodeId, long now) {
        //修改连接的状态
        connectionStates.disconnected(nodeId, now);
        for (ClientRequest request : this.inFlightRequests.clearAll(nodeId)) {
            log.trace("Cancelled request {} due to node {} being disconnected", request, nodeId);
            if (!metadataUpdater.maybeHandleDisconnection(request))
                //对这些主机进行处理
                //自己封装一个响应对象,对象中没有响应体消息,失去连接的状态标识为true
                responses.add(new ClientResponse(request, now, true, null));
        }
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        //获取到请求超时的主机
        List<String> nodeIds = this.inFlightRequests.getNodesWithTimedOutRequests(now, this.requestTimeoutMs);
        for (String nodeId : nodeIds) {
            // close connection to the node
            //关闭请求超时的主机连接
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            //修改连接的状态
            processDisconnection(responses, nodeId, now);
        }

        // we disconnected, so we should probably refresh our metadata
        if (nodeIds.size() > 0)
            metadataUpdater.requestUpdate();
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    //处理已经完成的发送请求，如果不期望得到响应，就认为整个请求全部完成
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (Send send : this.selector.completedSends()) {
            ClientRequest request = this.inFlightRequests.lastSent(send.destination());
            if (!request.expectResponse()) {
                //todo 请求不期待响应   completeLastSent 最后一个完成发送请求 从队列中移除
                this.inFlightRequests.completeLastSent(send.destination());
                responses.add(new ClientResponse(request, now, false, null));
            }
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     * 处理任何已完成的接收，并用接收到的响应更新响应列表
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {
            //获取发送方的 broker id
            String source = receive.source();
            /**
             * kafka有这样一个机制: 每个连接可以容忍5个没有接收到响应,但是已经发出去了的请求
             */
            //todo 从inFlightRequests中移除已经接收到响应的请求，把之前存入的请求也获取到了
            ClientRequest req = inFlightRequests.completeNext(source);
            //todo 解析服务端发送回来的请求(里面有响应的结果数据)  receive.payload() = 响应体  req.request().header() 请求头
            Struct body = parseResponse(receive.payload(), req.request().header());
            //todo 判断是否是关于元数据信息响应
            if (!metadataUpdater.maybeHandleCompletedReceive(req, now, body)) {
                //不是关于元数据信息响应的，需要添加到responses

                //将解析的结果封装到ClientResponse对象中
                //ClientResponse对象中的四个参数依次是
                // 1.发出去的请求
                // 2.接收到响应的时间戳
                // 3.客户端是否在完全读取响应之前断开连接
                // 4.响应的内容
                responses.add(new ClientResponse(req, now, false, body));
            }
        }
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        for (String node : this.selector.disconnected()) {
            log.debug("Node {} disconnected.", node);
            processDisconnection(responses, node, now);
        }
        // we got a disconnect so we should probably refresh our metadata and see if that broker is dead
        if (this.selector.disconnected().size() > 0)
            metadataUpdater.requestUpdate();
    }

    /**
     * Record any newly completed connections
     */
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            log.debug("Completed connection to node {}", node);
            this.connectionStates.connected(node);
        }
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     */
    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + "), request header: " + requestHeader);
    }

    /**
     * Initiate a connection to the given node
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            log.debug("Initiating connection to node {} at {}:{}.", node.id(), node.host(), node.port());
            this.connectionStates.connecting(nodeConnectionId, now);
            //尝试进行连接
            selector.connect(nodeConnectionId,
                             new InetSocketAddress(node.host(), node.port()),
                             this.socketSendBuffer,
                             this.socketReceiveBuffer);
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            connectionStates.disconnected(nodeConnectionId, now);
            /* maybe the problem is our metadata, update it */
            metadataUpdater.requestUpdate();
            log.debug("Error connecting to node {} at {}:{}:", node.id(), node.host(), node.port(), e);
        }
    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        private final Metadata metadata;

        /* true iff there is a metadata request that has been sent and for which we have not yet received a response */
        private boolean metadataFetchInProgress;

        /* the last timestamp when no broker node is available to connect */
        private long lastNoNodeAvailableMs;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.metadataFetchInProgress = false;
            this.lastNoNodeAvailableMs = 0;
        }

        @Override
        public List<Node> fetchNodes() {
            //todo metadata.fetch() = Cluster
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
        }

        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?  调用doSend()调用waitOnMetadata方法 awaitUpdate 会 将 needUpdate改成true
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            // metadata.refreshBackoff() = 100
            long timeToNextReconnectAttempt = Math.max(this.lastNoNodeAvailableMs + metadata.refreshBackoff() - now, 0);
            long waitForMetadataFetch = this.metadataFetchInProgress ? Integer.MAX_VALUE : 0;
            // if there is no node available to connect, back off refreshing metadata
            long metadataTimeout = Math.max(Math.max(timeToNextMetadataUpdate, timeToNextReconnectAttempt), waitForMetadataFetch);
            if (metadataTimeout == 0) {
                // Beware that the behavior of this method and the computation of timeouts for poll() are
                // highly dependent on the behavior of leastLoadedNode.
                Node node = leastLoadedNode(now);
                //TODO 这个方法里面会封装请求
                maybeUpdate(now, node);
            }

            return metadataTimeout;
        }

        @Override
        public boolean maybeHandleDisconnection(ClientRequest request) {
            ApiKeys requestKey = ApiKeys.forId(request.request().header().apiKey());

            if (requestKey == ApiKeys.METADATA && request.isInitiatedByNetworkClient()) {
                Cluster cluster = metadata.fetch();
                if (cluster.isBootstrapConfigured()) {
                    int nodeId = Integer.parseInt(request.request().destination());
                    Node node = cluster.nodeById(nodeId);
                    if (node != null)
                        log.warn("Bootstrap broker {}:{} disconnected", node.host(), node.port());
                }

                metadataFetchInProgress = false;
                return true;
            }

            return false;
        }

        @Override
        public boolean maybeHandleCompletedReceive(ClientRequest req, long now, Struct body) {
            short apiKey = req.request().header().apiKey();
            //两个判断: 1.请求头中是否有元数据的API的id 2. isInitiatedByNetworkClient 判断请求是否是网络客户端发起的
            if (apiKey == ApiKeys.METADATA.id && req.isInitiatedByNetworkClient()) {
                //TODO 处理响应
                handleResponse(req.request().header(), body, now);
                return true;
            }
            return false;
        }

        @Override
        public void requestUpdate() {
            this.metadata.requestUpdate();
        }

        private void handleResponse(RequestHeader header, Struct body, long now) {
            this.metadataFetchInProgress = false;
            //因为服务端返回的响应是一个二进制的数据结构
            //所以生产者要在这里对其进行解析
            //将解析的结果封装成MetadataResponse对象
            MetadataResponse response = new MetadataResponse(body);
            //获取到了从服务端拉取到的元数据信息
            Cluster cluster = response.cluster();
            // check if any topics metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", header.correlationId(), errors);

            // don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
            // created which means we will get errors and no nodes until it exists
            //如果没有有效节点，请不要更新群集…我们需要的主题可能仍在创建过程中，这意味着在它存在之前，我们将得到错误且没有节点
            //如果正常获取到了元数据信息,就会更新元数据
            if (cluster.nodes().size() > 0) {
                //更新元数据信息
                this.metadata.update(cluster, now);
            } else {
                log.trace("Ignoring empty metadata response with correlation id {}.", header.correlationId());
                this.metadata.failedUpdate(now);
            }
        }

        /**
         * Create a metadata request for the given topics
         * 为给定主题创建元数据请求
         */
        private ClientRequest request(long now, String node, MetadataRequest metadata) {
            // 三个参数依次为  String destination, RequestHeader header, Struct body
            RequestSend send = new RequestSend(node, nextRequestHeader(ApiKeys.METADATA), metadata.toStruct());
            return new ClientRequest(now, true, send, null, true);
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         */
        private void maybeUpdate(long now, Node node) {
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                // mark the timestamp for no node available to connect
                this.lastNoNodeAvailableMs = now;
                return;
            }
            String nodeConnectionId = node.idString();
            //判断网络连接是否建立好
            if (canSendRequest(nodeConnectionId)) {
                this.metadataFetchInProgress = true;
                MetadataRequest metadataRequest;
                //判断是否需要获取所有主题的元数据
                if (metadata.needMetadataForAllTopics()) {
                    //封装请求,获取所有topics
                    //但是我们获取元数据的时候,只获取自己要发送消息的对应的topic的元数据的信息
                    metadataRequest = MetadataRequest.allTopics();
                }else {
                    //只拉取接收我们发送的消息的topics
                    metadataRequest = new MetadataRequest(new ArrayList<>(metadata.topics()));
                }
                //todo 创建获取主题元数据请求
                ClientRequest clientRequest = request(now, nodeConnectionId, metadataRequest);
                log.debug("Sending metadata request {} to node {}", metadataRequest, node.id());
                System.out.println("发送更新元数据请求=========");
                //在这个方法里面,会将请求存储起来
                doSend(clientRequest, now);
            } else if (connectionStates.canConnect(nodeConnectionId, now)) {
                // we don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node.id());
                initiateConnect(node, now);
                // If initiateConnect failed immediately, this node will be put into blackout and we
                // should allow immediately retrying in case there is another candidate node. If it
                // is still connecting, the worst case is that we end up setting a longer timeout
                // on the next round and then wait for the response.
            } else { // connected, but can't send more OR connecting
                // In either case, we just need to wait for a network event to let us know the selected
                // connection might be usable again.
                this.lastNoNodeAvailableMs = now;
            }
        }

    }

}
