/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for(int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting
 * we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code>
 * as 0 it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple string or byte types.
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";

    private String clientId;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Metrics metrics;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    //用于生产者到块缓冲时间 60 * 1000 = 60s
    private final long maxBlockTimeMs;
    //控制客户端等待请求响应的最长时间
    private final int requestTimeoutMs;
    //一个拦截器，对我们发送的数据进行拦截,
    //这个功能其实没啥用，我们即使真的要过滤，拦截一些消息，也不考虑使用它，我们直接发送数据之前自己用代码过滤即可
    private final ProducerInterceptors<K, V> interceptors;

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
                keySerializer, valueSerializer);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
                keySerializer, valueSerializer);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    //初始化过程
    private KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        try {
            log.trace("Starting the Kafka producer");
            //获取用户自定义的配置
            Map<String, Object> userProvidedConfigs = config.originals();
            this.producerConfig = config;
            this.time = new SystemTime();
            //todo 获取client id，如果没有就自动生成一个
            clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0) {
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            }
            //跟监控信息有关
            Map<String, String> metricTags = new LinkedHashMap<String, String>();
            metricTags.put("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .tags(metricTags);
            List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,MetricsReporter.class);
            reporters.add(new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time);
            //设置分区器,初始化流程中,默认会有一个分区器,目的是计算消息发送到服务端哪个地方
            //kafka生产者要把一条消息发送到服务端的哪一个地方,需要借助分区器进行计算
            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
            /**
             * Producer发送消息的时候,我们的代码里一般会设置重试机制(指定时间或者次数)
             * 为什么会有重试机制: 因为我们是分布式的系统,并且需要网络传输,而网络传输一般是不稳定的,
             * 所以我们在写代码的时候,我们需要Producer指定重试机制
             */
            // 重试间隔时间 retry.backoff.ms 默认是100ms
            // 重新发送请求之前设置等待的间隔时间,目的可以避免在某些故障情况下重复发送请求
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            //设置序列化器,kafka发送数据需要进行网络传输,所以需要将数据序列化为二进制
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            // load interceptors and make sure they get clientId
            // 加载拦截器并确保它们获得clientId
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            //设置拦截器
            //类似一个过滤器,本质上很少用到这个功能,因为数据在此之前我们已经做了过滤
            List<ProducerInterceptor<K, V>> interceptorList = (List) (new ProducerConfig(userProvidedConfigs)).getConfiguredInstances(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            this.interceptors = interceptorList.isEmpty() ? null : new ProducerInterceptors<>(interceptorList);

            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keySerializer, valueSerializer, interceptorList, reporters);

            //生产者从服务端拉取过来的kafka的元数据
            //初始化一个元数据对象
            //retryBackoffMs 100ms
            //metadata.max.age.ms 默认5分钟
            //生产者每隔一段时间都要去更新一下集群的元数据
            //拿到元数据信息的很重要的一个目的就是计算消息将会发到哪个分区
            //生产者不管理元数据,只是拿过来用,真正管理元数据的是服务端
            this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG), true, clusterResourceListeners);
            //max.request.size 生产者往服务端发送消息的时候,规定一条消息最大有多大. 1*1024*1024
            //如果消息的大小超过了规定值,那么这个消息就发送不出去
            //规定默认大小是1M,不过这个值偏小,我们需要修改这个值
            //经验值是10M,根据公司的具体情况而定
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            //设置缓存大小.默认是32M
            //缓存大小一般够用,但是特殊情况我们还是需要去修改,调大或者调小
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            //kafka是可以将消息进行压缩的,提高系统的吞吐量,我们可以在这个地方设置压缩的格式,默认值是 none,也就是没有设置
            //好处: 提高系统的吞吐量,一次性发送的消息更多;但是生产者在这里会消耗更多的cpu
            //如果压缩之后的大小超过了maxRequestSize规定值,就会报错
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            /* check for user defined settings.
             * If the BLOCK_ON_BUFFER_FULL is set to true,we do not honor METADATA_FETCH_TIMEOUT_CONFIG.
             * This should be removed with release 0.9 when the deprecated configs are removed.
             */
            if (userProvidedConfigs.containsKey(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG)) {
                log.warn(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG + " config is deprecated and will be removed soon. " +
                        "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                boolean blockOnBufferFull = config.getBoolean(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG);
                if (blockOnBufferFull) {
                    this.maxBlockTimeMs = Long.MAX_VALUE;
                } else if (userProvidedConfigs.containsKey(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG)) {
                    log.warn(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG + " config is deprecated and will be removed soon. " +
                            "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                    this.maxBlockTimeMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
                } else {
                    this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
                }
            } else if (userProvidedConfigs.containsKey(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG)) {
                log.warn(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG + " config is deprecated and will be removed soon. " +
                        "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                this.maxBlockTimeMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
            } else {
                this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            }

            /* check for user defined settings.
             * If the TIME_OUT config is set use that for request timeout.
             * This should be removed with release 0.9
             */
            if (userProvidedConfigs.containsKey(ProducerConfig.TIMEOUT_CONFIG)) {
                log.warn(ProducerConfig.TIMEOUT_CONFIG + " config is deprecated and will be removed soon. Please use " +
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
                this.requestTimeoutMs = config.getInt(ProducerConfig.TIMEOUT_CONFIG);
            } else {
                this.requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            }

            //TODO 创建了一个核心组件,也就是缓存对象  BATCH_SIZE_CONFIG 16384 = 16k
            this.accumulator = new RecordAccumulator(config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                    this.totalMemorySize,//32m
                    this.compressionType,
                    //等待批量数据的时间，超过此时间，未达到批量发送基本单位也发送
                    config.getLong(ProducerConfig.LINGER_MS_CONFIG),//0
                    retryBackoffMs,//100ms
                    metrics,
                    time);

            //通过参数获取 bootstrap.servers
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            this.metadata.update(Cluster.bootstrap(addresses), time.milliseconds());
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
            //TODO 初始化一个重要的管理网络的组件,整个kafka的网络都是由它来负责的
            /**
             * (1). connections.max.idle.ms:默认值是9分钟
             *      一个网络连接的最大空闲时间,超过这个空闲时间,就会关闭这个网络连接
             * (2). max.in.flight.requests.per.connection: 默认值是5
             *      Producer向broker发送数据的时候,其实是有多个网络连接,每个网络连接可以忍受Producer端发送给broker消息然后消息没有响应的个数
             *      因为kafka有重试机制,当有个消息响应的结果失败请求重新发送,那么就会造成数据乱序,如果想要保证数据有序,这个值要设置成1
             * (3). send.buffer.bytes:socket发送数据的缓冲区的大小,默认值是128K
             * (4). receive.buffer.bytes:socket接收数据的缓冲区的大小,默认值是32K
             */
            NetworkClient client = new NetworkClient(
                    // connections.max.idle.ms = 9 * 60 * 1000
                    new Selector(config.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    this.metrics, time, "producer", channelBuilder),
                    this.metadata,
                    clientId,
                    // 5
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                    // backoff 补偿的意思
                    config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    // send.buffer.bytes = 128 * 1024 socket的发送缓冲区
                    config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                    // receive.buffer.bytes = 32 * 1024 socket的接收缓冲区
                    config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                    this.requestTimeoutMs, time);
            /**
             * retries:重试的次数,默认是0
             *
             * acks: producer的消息发送确认机制,其本质是producer决定是否发送消息的不同判断方式,设置的值不一样,生产者发送消息的方式也不一样
             *
             *  0: producer不等待分区中副本同步完成的反馈,不管消息是否发送成功，继续发送下一条(批)消息
             *      提供了最低的延迟。但是最弱的持久性，当服务器发生故障时，就很可能发生数据丢失。
             *          例如leader已经死亡，producer不知情，还会继续发送消息,broker接收不到数据就会数据丢失
             *  1: producer要等待分区中的leader成功收到数据并得到确认，才发送下一条消息。
             *      提供了较好的持久性较低的延迟性。
             *     Partition的Leader死亡，follower尚未复制，数据就会丢失
             *  -1:producer只有收到分区内所有副本的成功写入的通知才认为推送消息成功了。
             *      持久性最好，延时性最差。
             *
             *  我们分析一下ack=1的情况下，为什么消息也会丢失？
             * ack=1的情况下，producer只要收到分区leader成功写入的通知就会认为消息发送成功了。
             * 如果leader成功写入后，还没来得及把数据同步到follower节点就挂了，这时候消息就丢失了。
             *
             * 注意，ack的默认值就是1。这个默认值其实就是吞吐量与可靠性的一个折中方案。
             * 生产上我们可以根据实际情况进行调整，比如如果你要追求高吞吐量，那么就要放弃可靠性。
             * 还需要注意一点,ack是String类型的,不是int类型,因为 ack=-1和 ack=all是一样的
             */
            //初始化一个线程
            this.sender = new Sender(client,
                    this.metadata,
                    this.accumulator,
                    // max.in.flight.requests.per.connection = 5 即 guaranteeMessageOrder 默认为false
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION) == 1,
                    config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                    (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG)),
                    //重试次数,默认为0次
                    config.getInt(ProducerConfig.RETRIES_CONFIG),
                    this.metrics,
                    new SystemTime(),
                    clientId,
                    this.requestTimeoutMs);
            String ioThreadName = "kafka-producer-network-thread" + (clientId.length() > 0 ? " | " + clientId : "");
            //创建一个线程,然后里面传进去了一个sender对象,其本质是包装sender对象
            //把业务的代码和线程相关的代码隔离开了: 业务代码在sender中,线程的设置在KafkaThread中进行,比如设置成后台线程,给线程命名
            //关于线程的这种代码设计方式,其实很值得学习和积累
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            //启动线程
            this.ioThread.start();

            this.errors = this.metrics.sensor("errors");
            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId);
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed
            // this is to prevent resource leak. see KAFKA-2121
            close(0, TimeUnit.MILLISECONDS, true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If
     * {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record. If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null) {
     *                          e.printStackTrace();
     *                       } else {
     *                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                       }
     *                   }
     *               });
     * }
     * </pre>
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     *
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws TimeoutException If the time taken for fetching metadata or allocating memory for the record has surpassed <code>max.block.ms</code>.
     * @throws KafkaException If a Kafka related error occurs that does not belong to the public API exceptions.
     *
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        //拦截发送的记录，该记录可能被修改；该方法不会抛出异常
        ProducerRecord<K, V> interceptedRecord = this.interceptors == null ? record : this.interceptors.onSend(record);
        //TODO 核心代码
        return doSend(interceptedRecord, callback);
    }

    /**
     * Implementation of asynchronously send a record to a topic.
     * 实现将异步消息发送到主题上,服务器的元数据会记录服务器已经接收到了消息
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            // first make sure the metadata for the topic is available
            // 首先确保主题的元数据可用
            /**
             * 步骤一:
             *      同步等待拉取元数据
             *      maxBlockTimeMs : 最大的等待时间 60s
             */
            ClusterAndWaitTime clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            //clusterAndWaitTime.waitedOnMetadataMs 拉取元数据所用的时间
            //remainingWaitMs: 剩下可用的等待时间 = maxBlockTimeMs : 最大的等待时间 - 拉取元数据用掉的时间
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            //得到最新的集群元数据
            Cluster cluster = clusterAndWaitTime.cluster;
            /**
             * 步骤二:
             *      对消息的key和value进行序列化
             */
            byte[] serializedKey;//序列化key
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() + " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() + " specified in key.serializer");
            }
            byte[] serializedValue;//序列化value
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() + " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() + " specified in value.serializer");
            }
            /**
             * 步骤三:
             *      根据分区器选择消息应该发送的分区
             *      因为在此处已经获取到了元数据的信息,我们就可以进行计算
             */
            //计算发送的消息应该发到哪个分区 如果记录有分区，则返回值，否则调用配置的分区器类来计算分区。    todo 讲解
            int partition = partition(record, serializedKey, serializedValue, cluster);
            //计算发送消息的大小
            int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(serializedKey, serializedValue);
            /**
             *  步骤四:
             *      确认一下发送的消息是否超过规定的最大值
             *      在kafkaProducer初始化的时候,指定了一个最大值
             *      默认大小是1M,不过这个值偏小,我们需要修改这个值
             *      经验值是10M,根据公司的具体情况而定
             */
            //验证记录大小是否过大
            ensureValidRecordSize(serializedSize);
            /**
             * 步骤五:
             *      获取到了元数据,也计算出来了分区号,就可以创建一个封装分区的对象
             */
            tp = new TopicPartition(record.topic(), partition);
            //每条消息都有时间戳 当前时间
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
            // producer callback will make sure to call both 'callback' and interceptor callback
            // producer callback将确保同时调用'callback'和interceptor callback
            /**
             *  步骤六:
             *      给每一条消息都绑定它的回调函数,因为我们使用的是异步的方式发送消息
             *      如果发送出去的消息有返回响应,就会调用回调函数
             *      此处是给消息绑定回调函数
             */
            Callback interceptCallback = this.interceptors == null ? callback : new InterceptorCallback<>(callback, this.interceptors, tp);
            /**
             * 步骤七:
             *      把消息放进累加器accumulator(32M的一个内存),
             *      然后accumulator把消息封装成一个批次一个批次的去发送
             */
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey, serializedValue, interceptCallback, remainingWaitMs);
            //判断批次满了,或者新创建出来一个批次
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
                /**
                 * 步骤八:
                 *      如果批次满了,或者新创建出来一个批次,那么就唤醒sender线程,它才是真正发送数据的线程
                 *      生产者的主线程获取元数据,将消息缓存,然后唤醒sender线程,发送数据的工作由sender完成
                 */
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors; 处理异常并记录错误；
            // for API exceptions return them in the future, 对于API异常，返回到future对象,以后处理
            // for other exceptions throw directly 对于其他异常，直接抛出
            /**
             * 我们没有大型项目的架构经验,所以在设计异常体系的时候没有思路,
             * 所以我们可以好好研究一下优秀的kafka(世界级优秀的开源代码)
             * kafka源码一个很优秀的特点,核心流程捕捉各种异常
             *      1.核心流程捕捉各种异常
             *      2.底层代码的异常往上抛: 为了能让用户很直观的知道程序报错的时候有什么异常,
             *          我们发现kafka自定义了很多异常,这种方式很值得学习借鉴
             */
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null) {
                callback.onCompletion(null, e);
            }
            this.errors.record();
            if (this.interceptors != null) {
                this.interceptors.onSendError(record, tp, e);
            }
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            if (this.interceptors != null) {
                this.interceptors.onSendError(record, tp, e);
            }
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            if (this.interceptors != null) {
                this.interceptors.onSendError(record, tp, e);
            }
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            if (this.interceptors != null) {
                this.interceptors.onSendError(record, tp, e);
            }
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            if (this.interceptors != null) {
                this.interceptors.onSendError(record, tp, e);
            }
            throw e;
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * 返回一个包含主题元数据的Cluster集群对象和等待时间
     * @param topic The topic we want metadata for
     *              主题名称,我们需要等到该主题的元数据
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     *                  消息应该发往的分区的编号,如果没有就是null
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     *                  最大的等待时间
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     *          返回一个包含主题元数据的集群和一个等待的时间(拉取元数据所用的时间)
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {
        // add topic to metadata topic list if it is not there already and reset expiry
        //将主题添加到元数据主题列表（如果尚未存在）并重置过期
        metadata.add(topic);
        //利用场景驱动的方式,代码运行到此处,Producer的初始化已经完成
        //第一次运行到此处的时候,这个cluster里面是没有元数据的,只有我们在代码中设置的address
        Cluster cluster = metadata.fetch(); //获取当前未被阻塞的集群元数据
        //根据当前的topic从集群的元数据中获取分区信息
        //如果是第一次运行到此处,所以这里是肯定没有对应的分区信息
        Integer partitionsCount = cluster.partitionCountForTopic(topic);//得到给定主题中的分区数量
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        //当我们第一次运行到这里的时候,partitionsCount是null,是不会进入下面的判断,而是向下继续执行,递归调用自己
        //如果有缓存的元数据，并且记录的分区未定义或在已知分区范围内，则返回该元数据
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            //直接返回cluster元数据信息,拉取数据花费的时间
        {
            return new ClusterAndWaitTime(cluster, 0);
        }
        //如果代码运行到了这里,说明是第一次运行到这里,那么就真的需要去服务端拉取元数据
        //记录当前时间
        long begin = time.milliseconds();
        //剩余多长时间,默认是最大的等待时间
        long remainingWaitMs = maxWaitMs;
        //花费的时间
        long elapsed;
        // Issue metadata requests until we have metadata for the topic or maxWaitTimeMs is exceeded.
        // In case we already have cached metadata for the topic, but the requested partition is greater
        // than expected, issue an update request only once. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        do {
            log.trace("Requesting metadata update for topic {}.", topic);
            //1).获取当前元数据的版本
            //在Producer管理元数据的时候,对于他来说元数据是有版本号的
            //每次成功更新版本,版本号就会递增
            //2).把needUpdate标识符设置为true
            int version = metadata.requestUpdate();//得到的是更新元数据之前的版本号
            //todo 唤醒sender线程去获取元数据  这个地方很巧妙
            // 其他线程如果因为调用了selector.select()或者selector.select(long)这两个方法而阻塞，
            // 调用了selector.wakeup()之后，就会立即返回结果，并且返回的值!=0
            sender.wakeup();
            try {
                //TODO 同步等待元数据
                //等待上面的sender.wakeup()线程,直到获取到元数据
                //拿到元数据信息(集群里有那几个节点,节点里面有那几个主题和分区,),就可以计算消息将会发送到哪个分区
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            //尝试获取一下集群的元数据信息
            cluster = metadata.fetch();
            //计算一下,拉取元数据已经花了多少时间
            elapsed = time.milliseconds() - begin;
            //花费的时间超过了最大时间,那么就会报超时异常
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            //如果已经获取到了元数据,但是发现主题没有被授权
            if (cluster.unauthorizedTopics().contains(topic)) {
                throw new TopicAuthorizationException(topic);
            }
            //计算出来,还可以用的时间
            remainingWaitMs = maxWaitMs - elapsed;
            //尝试获取一下,我们要发送消息的这个主题对应的分区信息
            //如果不为null,说明send线程已经获取到了元数据信息,可以跳出循环,如果没有获取到,则继续循环等待
            partitionsCount = cluster.partitionCountForTopic(topic);
            //如果获取到了元数据,代码运行到这里就会退出
            //也有一种情况,即使获取到了元数据,如果元数据发生变化,partitionsCount也可能是null,为了确保万无一失,所以就再判断一次
        } while (partitionsCount == null);//代替了if判断的做法,do -while循环是先执行一次 再进行判断,很严谨很巧妙

        if (partition != null && partition >= partitionsCount) {
            throw new KafkaException(String.format("Invalid partition given with record: %d is not in the range [0...%d).", partition, partitionsCount));
        }

        //递归调用自己
        //返回了一个对象,里面有两个属性
        //cluster 集群的元数据,包含Kafka集群中节点、主题和分区的子集
        //elapsed 拉取元数据花费了多长时间
        return new ClusterAndWaitTime(cluster, elapsed);
    }

    /**
     * Validate that the record size isn't too large
     * 验证记录大小是否过大
     */
    private void ensureValidRecordSize(int size) {
        //如果一条消息大小超过maxRequestSize,那么就会报错
        //maxRequestSize的默认大小是1M,偏小,根据具体的业务场景设置大小,经验值是10M
        if (size > this.maxRequestSize) {
            //自定义了异常
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the maximum request size you have configured with the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                    " configuration.");
        }
        //如果一条消息大小超过了32M也会报错
        //totalMemorySize是内存池默认的大小
        if (size > this.totalMemorySize) {
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
        }
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the give topic. This can be used for custom partitioning.
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        try {
            return waitOnMetadata(topic, null, maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    //close()方法会阻塞等待之前所有的发送请求完成之后再关闭
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(0, TimeUnit.MILLISECONDS)</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @param timeUnit The time unit for the <code>timeout</code>
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     */
    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        if (timeout < 0) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }

        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeUnit.toMillis(timeout));
        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.", timeout);
            } else {
                // Try to close gracefully.
                if (this.sender != null) {
                    this.sender.initiateClose();
                }
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeUnit.toMillis(timeout));
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, t);
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeout);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, e);
                }
            }
        }

        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId);
        log.debug("The Kafka producer has closed.");
        if (firstException.get() != null && !swallowException) {
            throw new KafkaException("Failed to close kafka producer", firstException.get());
        }
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer, Serializer<V> valueSerializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList : candidateLists) {
            clusterResourceListeners.maybeAddAll(candidateList);
        }

        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

    /**
     * computes partition for given record.
     * 分区器:返回一个分区的编号,目的是计算发送的消息应该发到哪个分区
     * if the record has partition returns the value otherwise
     * calls configured partitioner class to compute the partition.
     * 如果记录有分区，则返回值，否则调用配置的分区器类来计算分区。
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        //如果你的消息已经分配了分区号,直接用就行了
        //但是正常情况下,第一次进来的消息是没有分区号的
        //因为有重试机制,虽然第一次进来没有分区号,但是分区器会给一个,当重试的时候,就可以获取到了
        Integer partition = record.partition();
        return partition != null ?
                partition :
                //没有分区号的时候,使用分区器进行选择合适的分区
                partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;

        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
     * notifies producer interceptors about the request completion.
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        public InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors,
                                   TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (this.interceptors != null) {
                if (metadata == null) {
                    this.interceptors.onAcknowledgement(new RecordMetadata(tp, -1, -1, Record.NO_TIMESTAMP, -1, -1, -1),
                            exception);
                } else {
                    this.interceptors.onAcknowledgement(metadata, exception);
                }
            }
            if (this.userCallback != null) {
                this.userCallback.onCompletion(metadata, exception);
            }
        }
    }
}
