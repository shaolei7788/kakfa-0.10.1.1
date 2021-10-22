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

package org.apache.kafka.common.network;


import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.security.Principal;
//TODO 我们认为KafkaChannel就是对javaNIO中的socketChannel进行了封装
public class KafkaChannel {
    //broker的id
    //一个brokerId对应一个kafkaChannel
    private final String id;
    private final TransportLayer transportLayer;
    private final Authenticator authenticator;
    private final int maxReceiveSize;
    //接收到的响应
    private NetworkReceive receive;
    //发出去的请求
    private Send send;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    public void close() throws IOException {
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isMute() {
        return transportLayer.isMute();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    //是为了保证服务端的处理性能，客户端网络连接对象有一个限制条件，针对同一个服务端，如果上一个客户端请求还没有发送完成，则不允许发送新的客户端请求
    //KafkaChannel 一次只能发送一个send请求
    public void setSend(Send send) {
        if (this.send != null) {
            //之前的send还没有发送完成，新的请求不能进来  调用这个方法的方法有捕获这个请求
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        }
        //往KafkaChannel里面绑定一个发送请求
        this.send = send;
        //这里绑定了一个OP_WRITE事件
        //绑定了OP_WRITE事件之后,就可以往服务端发送请求了
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    //读取操作如果一次read没有完成，也要调用多次read
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;
        if (receive == null) {
            //id 会作为NetworkReceive的source
            receive = new NetworkReceive(maxReceiveSize, id);
        }
        //一直在读取数据
        receive(receive);
        //是否读完一个完整的响应消息
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        return result;
    }

    private long receive(NetworkReceive receive) throws IOException {
        //todo 真正的读取
        return receive.readFrom(transportLayer);
    }


    public Send write() throws IOException {
        Send result = null;
        //todo send方法就是发送网络请求的方法 如果send(send)返回值为false,表示请求还没有发送成功
        if (send != null && send(send)) {
            result = send;
            //请求发送完成，才可以发送下一个请求
            send = null;
        }
        return result;
    }

    private boolean send(Send send) throws IOException {
        //最终执行发送请求的代码就在这里
        send.writeTo(transportLayer);
        if (send.completed()) {
            //todo send请求全部写出去，移除OP_WRITE
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        }
        //send请求没有全部写出去，会继续监听写事件
        return send.completed();
    }



}
