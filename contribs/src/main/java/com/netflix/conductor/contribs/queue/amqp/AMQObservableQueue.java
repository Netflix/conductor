package com.netflix.conductor.contribs.queue.amqp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import com.rabbitmq.client.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Created at 19/03/2019 16:38
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
public class AMQObservableQueue implements ObservableQueue {

    private static Logger logger = LoggerFactory.getLogger(AMQObservableQueue.class);

    private static final String QUEUE_TYPE = "amqp";

    private static final String PROPERTY_KEY_TEMPLATE = "workflow.event.queues.amqp.%s";
    private static final int DEFAULT_BATCH_SIZE = 1;
    private static final int DEFAULT_POLL_TIME_MS = 100;
    private static final String DEFAULT_CONTENT_TYPE = "application/json";
    private static final String DEFAULT_CONTENT_ENCODING = "UTF-8";

    private final String queueName;
    private boolean isDurable;
    private boolean isExclusive;
    private final Map<String, Object> queueArgs;

    private final int batchSize, pollTimeInMS;

    private boolean isConnOpened = false, isChanOpened = false;

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private Address[] addresses;

    AMQObservableQueue(final ConnectionFactory factory, final Address[] addresses,
                       final String queueName, final boolean isDurable, final boolean isExclusive,
                       final Map<String, Object> queueArgs,
                       final int batchSize, final int pollTimeInMS) {
        this.factory = factory;
        this.addresses = addresses;
        this.queueName = queueName;
        this.isDurable = isDurable;
        this.isExclusive = isExclusive;
        this.queueArgs = queueArgs;
        this.batchSize = batchSize;
        this.pollTimeInMS = pollTimeInMS;
    }

    private void connect() {
        if (isConnOpened)
            return;
        try {
            connection = factory.newConnection(addresses);
            isConnOpened = connection.isOpen();
        }
        catch (final IOException e) {
            isConnOpened = false;
            final String error = "IO error while connecting to "+ Arrays.stream(addresses)
                    .map(address -> address.toString()).collect(Collectors.joining(","));
            logger.error(error, e);
            throw new RuntimeException(error, e);
        }
        catch (final TimeoutException e) {
            isConnOpened = false;
            final String error = "Timeout while connecting to "+ Arrays.stream(addresses)
                    .map(address -> address.toString()).collect(Collectors.joining(","));
            logger.error(error, e);
            throw new RuntimeException(error, e);
        }
    }

    public boolean isClosed() {
        return !isConnOpened && !isChanOpened;
    }

    public void open() {
        connect();
    }

    @Override
    public Observable<Message> observe() {
        Observable.OnSubscribe<Message> subscriber = getOnSubscribe();
        return Observable.create(subscriber);
    }

    @Override
    public String getType() {
        return QUEUE_TYPE;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public String getURI() {
        return queueName;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        final List<String> processedDeliveryTags = new ArrayList<>();
        for (final Message message : messages) {
            try {
                if (logger.isInfoEnabled()) {
                    logger.info("ACK message with delivery tag {}", message.getReceipt());
                }
                getOrCreateChannel().basicAck(Long.valueOf(message.getReceipt()), false);
                // Message ACKed
                processedDeliveryTags.add(message.getReceipt());
            }
            catch (final IOException e) {
                logger.error("Cannot ACK message with delivery tag {}", message.getReceipt(), e);
            }
        }
        return processedDeliveryTags;
    }

    @Override
    public void publish(List<Message> messages) {
        try {
            final AMQP.Queue.DeclareOk declareOk = getOrCreateQueue();
            messages.forEach(message -> {
                try {
                    final String payload = message.getPayload();
                    getOrCreateChannel().basicPublish(StringUtils.EMPTY, declareOk.getQueue(),
                            new AMQP.BasicProperties.Builder()
                                    .messageId(StringUtils.isEmpty(message.getId()) ?
                                            UUID.randomUUID().toString() :
                                            message.getId())
                                    .correlationId(StringUtils.isEmpty(message.getReceipt()) ?
                                            UUID.randomUUID().toString() :
                                            message.getReceipt())
                                    .contentType(DEFAULT_CONTENT_TYPE)
                                    .contentEncoding(DEFAULT_CONTENT_ENCODING)
                                    .deliveryMode(2) /* Persist messages */
                                    .build(), payload.getBytes(DEFAULT_CONTENT_ENCODING));
                    logger.info(String.format("Published message to %s: %s", declareOk.getQueue(), payload));
                } catch (Exception ex) {
                    logger.error("Failed to publish message {} to {}", message.getPayload(), declareOk.getQueue(), ex);
                    throw new RuntimeException(ex);
                }
            });
        } catch (final Exception ex) {
            logger.error("Failed to get queue {}", queueName, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void setUnackTimeout(Message message, long l) {
    }

    @Override
    public long size() {
        if (!isClosed()) {
            try {
                return getOrCreateChannel().messageCount(queueName);
            }
            catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
        return 0;
    }

    @Override
    public void close() {
        closeChannel();
        closeConnection();
    }

    public static class Builder {

        private Address[] addresses;

        private String username, password, virtualHost;

        private int port, connectionTimeout;

        private boolean useNio = false, isExclusive = false, isDurable = true;

        private String queueName;

        private Map<String, Object> queueArguments;

        private int batchSize = 1;

        private int pollTimeInMS = 100;

        public Builder() {}

        public Builder(Configuration config) {
            final int maxPriority = config.getIntProperty("workflow.event.queues.amqp.maxPriority", -1);
            final Map<String, Object> queueArgs = Maps.newHashMap();
            if (maxPriority > 0) {
                queueArgs.put("x-max-priority", maxPriority);
            }
            init(config, queueArgs);
        }

        private void init(Configuration config, Map<String, Object> queueArgs) {
            withHosts(config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, "hosts"),
                    ConnectionFactory.DEFAULT_HOST)).
            withUsername(config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, "username"),
                    ConnectionFactory.DEFAULT_USER)).
            withPassword(config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, "password"),
                    ConnectionFactory.DEFAULT_PASS)).
            withVirtualHost(config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, "virtualHost"),
                    ConnectionFactory.DEFAULT_VHOST)).
            withPort(config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, "port"),  AMQP.PROTOCOL.PORT)).
            withConnectionTimeout(config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, "connectionTimeout"),
                    ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT)).
            useNio(config.getBoolProperty(String.format(PROPERTY_KEY_TEMPLATE, "useNio"), false)).
            /* messages polling settings */
            withBatchSize(config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, "batchSize"), DEFAULT_BATCH_SIZE)).
            withPollTimeInMS(config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, "pollTimeInMs"), DEFAULT_POLL_TIME_MS)).
            /* properties for queues */
            withQueueArgs(queueArgs).
            isDurable(config.getBooleanProperty(String.format(PROPERTY_KEY_TEMPLATE, "durable"), true)).
            isExclusive(config.getBooleanProperty(String.format(PROPERTY_KEY_TEMPLATE, "exclusive"), false));
        }

        public Builder withHosts(String hosts) {
            this.addresses = Address.parseAddresses(hosts);
            return this;
        }

        public Builder withUsername(String username) {
            if (StringUtils.isEmpty(username)) {
                throw new IllegalArgumentException("Username is null or empty");
            }
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            if (StringUtils.isEmpty(password)) {
                throw new IllegalArgumentException("Password is null or empty");
            }
            this.password = password;
            return this;
        }

        public Builder withVirtualHost(String virtualHost) {
            if (StringUtils.isEmpty(virtualHost)) {
                throw new IllegalArgumentException("Virtual host is null or empty");
            }
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder withQueueName(String name) {
            if (StringUtils.isEmpty(name)) {
                throw new IllegalArgumentException("Queue name is null or empty");
            }
            this.queueName = name;
            return this;
        }

        public Builder withQueueArgs(Map<String, Object> arguments) {
            this.queueArguments = arguments;
            return this;
        }

        public Builder withPort(int port) {
            if (port <= 0) {
                throw new IllegalArgumentException("Port must be greater than 0");
            }
            this.port = port;
            return this;
        }

        public Builder withConnectionTimeout(int connectionTimeout) {
            if (connectionTimeout <= 0) {
                throw new IllegalArgumentException("Connection timeout must be greater than 0");
            }
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be greater than 0");
            }
            this.batchSize = batchSize;
            return this;
        }

        public Builder withPollTimeInMS(int pollTimeInMS) {
            if (pollTimeInMS <= 0) {
                throw new IllegalArgumentException("Poll time must be greater than 0 ms");
            }
            this.pollTimeInMS = pollTimeInMS;
            return this;
        }

        public Builder isDurable(boolean isDurable) {
            this.isDurable = isDurable;
            return this;
        }

        public Builder isExclusive(boolean isExclusive) {
            this.isExclusive = isExclusive;
            return this;
        }

        public Builder useNio(boolean useNio) {
            this.useNio = useNio;
            return this;
        }

        private ConnectionFactory buildConnectionFactory() {
            final ConnectionFactory factory = new ConnectionFactory();
            if (!Strings.isNullOrEmpty(username)) {
                factory.setUsername(username);
            }
            if (!Strings.isNullOrEmpty(password)) {
                factory.setPassword(password);
            }
            if (port > 0) {
                factory.setPort(port);
            }
            if (connectionTimeout >= 0) {
                factory.setConnectionTimeout(connectionTimeout);
            }
            if (!Strings.isNullOrEmpty(virtualHost)) {
                factory.setVirtualHost(virtualHost);
            }
            if (useNio) {
                factory.useNio();
            }
            return factory;
        }

        public AMQObservableQueue build() {
            return new AMQObservableQueue(buildConnectionFactory(), addresses,
                    queueName, isDurable, isExclusive, queueArguments,
                    batchSize, pollTimeInMS);
        }
    }

    private Channel getOrCreateChannel() {
        if (!isConnOpened) {
            open();
        }
        // Return the existing channel if it's still opened
        if (channel != null && isChanOpened) {
            return channel;
        }
        // Channel creation is required
        try {
            channel = null;
            channel = connection.createChannel();
            channel.addShutdownListener(cause -> {
                isChanOpened = false;
                logger.error("Channel has been shutdown: {}", cause.getMessage(), cause);
            });
        }
        catch (final IOException e) {
            throw new RuntimeException("Cannot open channel on "+ Arrays.stream(addresses)
                    .map(address -> address.toString())
                    .collect(Collectors.joining(",")), e);
        }
        isChanOpened = channel.isOpen();
        if (!isChanOpened) {
            throw new RuntimeException("Channel is not opened");
        }
        return channel;
    }

    private AMQP.Queue.DeclareOk getOrCreateQueue() throws IOException {
        if (!isConnOpened) {
            open();
        }
        if (Strings.isNullOrEmpty(queueName)) {
            throw new RuntimeException("Queue name is undefined");
        }
        AMQP.Queue.DeclareOk declareOk;
        try {
            declareOk = getOrCreateChannel().queueDeclarePassive(queueName);
        }
        catch (final IOException e) {
            logger.warn("Queue {} might not exists", queueName, e);
            declareOk = getOrCreateChannel().queueDeclare(queueName, isDurable, isExclusive,
                    false, queueArgs);
        }
        return declareOk;
    }

    private void closeConnection() {
        if (connection == null) {
            logger.warn("Connection is null. Do not close it");
        }
        else {
            try {
                if (connection.isOpen()) {
                    try {
                        if (logger.isInfoEnabled()) {
                            logger.info("Close AMQP connection");
                        }
                        connection.close();
                    } catch (final IOException e) {
                        logger.warn("Fail to close connection: {}", e.getMessage(), e);
                    }
                }
            }
            finally {
                isConnOpened = connection.isOpen();
                connection = null;
            }
        }
    }

    private void closeChannel() {
        if (channel == null) {
            logger.warn("Channel is null. Do not close it");
        }
        else {
            try {
                if (channel.isOpen()) {
                    try {
                        if (logger.isInfoEnabled()) {
                            logger.info("Close AMQP channel");
                        }
                        channel.close();
                    } catch (final TimeoutException e) {
                        logger.warn("Timeout while closing channel: {}", e.getMessage(), e);
                    } catch (final IOException e) {
                        logger.warn("Fail to close channel: {}", e.getMessage(), e);
                    }
                }
            }
            finally {
                channel = null;
            }
        }
    }

    private static Message asMessage(GetResponse response) throws Exception {
        if (response == null) {
            return null;
        }
        final Message message = new Message();
        message.setId(response.getProps().getMessageId());
        message.setPayload(new String(response.getBody(), "UTF-8"));
        message.setReceipt(String.valueOf(response.getEnvelope().getDeliveryTag()));
        return message;
    }

    @VisibleForTesting
    List<Message> receiveMessages() {
        try {
            final AMQP.Queue.DeclareOk declareOk = getOrCreateQueue();
            final List<Message> messages = new LinkedList<>();
            getOrCreateChannel().basicQos(batchSize);
            Message message;
            int nb = 0;
            do {
                message = asMessage(getOrCreateChannel().basicGet(declareOk.getQueue(), false));
                if (message != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Got message {}", message);
                    }
                    messages.add(message);
                }
            }
            while (++nb < batchSize && message != null);
            Monitors.recordEventQueueMessagesProcessed(QUEUE_TYPE, declareOk.getQueue(), messages.size());
            return messages;
        }
        catch (Exception exception) {
            logger.error("Exception while getting messages from RabbitMQ", exception);
            Monitors.recordObservableQMessageReceivedErrors(QUEUE_TYPE);
        }
        return new ArrayList<>();
    }

    @VisibleForTesting
    Observable.OnSubscribe<Message> getOnSubscribe() {
        return subscriber -> {
            Observable<Long> interval = Observable.interval(pollTimeInMS, TimeUnit.MILLISECONDS);
            interval.flatMap((Long x)->{
                List<Message> msgs = receiveMessages();
                return Observable.from(msgs);
            }).subscribe(subscriber::onNext, subscriber::onError);
        };
    }
}
