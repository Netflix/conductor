package com.netflix.conductor.contribs.queue.amqp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.netflix.conductor.contribs.queue.AbstractObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.metrics.Monitors;
import com.rabbitmq.client.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.netflix.conductor.contribs.queue.amqp.AMQProperties.*;

/**
 * Created at 19/03/2019 16:38
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
public class AMQObservableQueue extends AbstractObservableQueue {

    private static Logger logger = LoggerFactory.getLogger(AMQObservableQueue.class);

    private static final String QUEUE_TYPE = "amqp";

    private static final String PROPERTY_KEY_TEMPLATE = "workflow.event.queues.amqp.%s";

    public static final int DEFAULT_BATCH_SIZE = 1;
    public static final int DEFAULT_POLL_TIME_MS = 100;

    private final AMQConsumeSettings subSettings;
    private final AMQPublishSettings pubSettings;

    private final int batchSize;

    private boolean isConnOpened = false, isChanOpened = false;

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private Address[] addresses;

    AMQObservableQueue(final ConnectionFactory factory, final Address[] addresses,
                       final AMQConsumeSettings subSettings, final AMQPublishSettings pubSettings,
                       final int batchSize, final int pollTimeInMS) {
        if (factory == null) {
            throw new IllegalArgumentException("Connection factory is undefined");
        }
        if (addresses == null || addresses.length == 0) {
            throw new IllegalArgumentException("Addresses are undefined");
        }
        if (subSettings == null) {
            throw new IllegalArgumentException("Consume settings are undefined");
        }
        if (pubSettings == null) {
            throw new IllegalArgumentException("Publish settings are undefined");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be greater than 0");
        }
        if (pollTimeInMS <= 0) {
            throw new IllegalArgumentException("Poll time must be greater than 0 ms");
        }
        this.factory = factory;
        this.addresses = addresses;
        this.subSettings = subSettings;
        this.pubSettings = pubSettings;
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

    private boolean isClosed() {
        return !isConnOpened && !isChanOpened;
    }

    private void open() {
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
        return subSettings.getQueueName();
    }

    @Override
    public String getURI() {
        return subSettings.getQueueName();
    }

    public ConnectionFactory getConnectionFactory() {
        return factory;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public AMQConsumeSettings getConsumeSettings() {
        return subSettings;
    }

    public AMQPublishSettings getPublishSettings() {
        return pubSettings;
    }

    public Address[] getAddresses() {
        return addresses;
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

    private static AMQP.BasicProperties buildBasicProperties(final Message message,
                                                             final AMQPublishSettings settings) {
        return new AMQP.BasicProperties.Builder()
                .messageId(StringUtils.isEmpty(message.getId()) ?
                        UUID.randomUUID().toString() :
                        message.getId())
                .correlationId(StringUtils.isEmpty(message.getReceipt()) ?
                        UUID.randomUUID().toString() :
                        message.getReceipt())
                .contentType(settings.getContentType())
                .contentEncoding(settings.getContentEncoding())
                .deliveryMode(settings.getDeliveryMode())
                .build();
    }

    private void publishMessage(Message message, String exchange, String routingKey) {
        try {
            final String payload = message.getPayload();
            getOrCreateChannel().basicPublish(exchange,
                    routingKey,
                    buildBasicProperties(message, pubSettings),
                    payload.getBytes(pubSettings.getContentEncoding()));
            logger.info(String.format("Published message to %s: %s", exchange, payload));
        } catch (Exception ex) {
            logger.error("Failed to publish message {} to {}", message.getPayload(), exchange, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void publish(List<Message> messages) {
        try {
            final String exchange, routingKey;
            if (pubSettings.useExchange()) {
                // Use exchange + routing key for publishing
                getOrCreateExchange(pubSettings.getQueueOrExchangeName(), pubSettings.getExchangeType(),
                        pubSettings.isDurable(), pubSettings.autoDelete(), pubSettings.getArguments());
                exchange = pubSettings.getQueueOrExchangeName();
                routingKey = pubSettings.getRoutingKey();
            }
            else {
                // Use queue for publishing
                final AMQP.Queue.DeclareOk declareOk = getOrCreateQueue(pubSettings.getQueueOrExchangeName(),
                        pubSettings.isDurable(), pubSettings.isExclusive(), pubSettings.autoDelete(),
                        pubSettings.getArguments());
                exchange = StringUtils.EMPTY; // Empty exchange name for queue
                routingKey = declareOk.getQueue(); // Routing name is the name of queue
            }
            messages.forEach(message -> publishMessage(message, exchange, routingKey));
        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final Exception ex) {
            logger.error("Failed to publish messages: {}", ex.getMessage(), ex);
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
                return getOrCreateChannel().messageCount(subSettings.getQueueName());
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

        private int batchSize;
        private int pollTimeInMS;

        private ConnectionFactory factory;
        private AMQConsumeSettings consumeSettings;
        private AMQPublishSettings pubSettings;

        public Builder(Configuration config) {
            this.addresses = buildAddressesFromHosts(config);
            this.factory = buildConnectionFactory(config);
            /* messages polling settings */
            batchSize = config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, BATCH_SIZE),
                    DEFAULT_BATCH_SIZE);
            pollTimeInMS = config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, POLL_TIME_IN_MS),
                    DEFAULT_POLL_TIME_MS);
        }

        private static Address[] buildAddressesFromHosts(final Configuration config) {
            // Read hosts from config
            final String hosts = config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, HOSTS),
                    ConnectionFactory.DEFAULT_HOST);
            if (StringUtils.isEmpty(hosts)) {
                throw new IllegalArgumentException("Hosts are undefined");
            }
            return Address.parseAddresses(hosts);
        }

        private static ConnectionFactory buildConnectionFactory(final Configuration config) {
            final ConnectionFactory factory = new ConnectionFactory();
            // Get rabbitmq username from config
            final String username = config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, USERNAME),
                    ConnectionFactory.DEFAULT_USER);
            if (StringUtils.isEmpty(username)) {
                throw new IllegalArgumentException("Username is null or empty");
            }
            else {
                factory.setUsername(username);
            }
            // Get rabbitmq password from config
            final String password = config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, PASSWORD),
                    ConnectionFactory.DEFAULT_PASS);
            if (StringUtils.isEmpty(password)) {
                throw new IllegalArgumentException("Password is null or empty");
            }
            else {
                factory.setPassword(password);
            }
            // Get vHost from config
            final String virtualHost = config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, VIRTUAL_HOST),
                    ConnectionFactory.DEFAULT_VHOST);
            if (StringUtils.isEmpty(virtualHost)) {
                throw new IllegalArgumentException("Virtual host is null or empty");
            }
            else {
                factory.setVirtualHost(virtualHost);
            }
            // Get server port from config
            final int port = config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, PORT),
                    AMQP.PROTOCOL.PORT);
            if (port <= 0) {
                throw new IllegalArgumentException("Port must be greater than 0");
            }
            else {
                factory.setPort(port);
            }
            // Get connection timeout from config
            final int connectionTimeout = config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, CONNECTION_TIMEOUT),
                    ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT);
            if (connectionTimeout <= 0) {
                throw new IllegalArgumentException("Connection timeout must be greater than 0");
            }
            else {
                factory.setConnectionTimeout(connectionTimeout);
            }
            final boolean useNio = config.getBoolProperty(String.format(PROPERTY_KEY_TEMPLATE, USE_NIO), false);
            if (useNio) {
                factory.useNio();
            }
            return factory;
        }

        public Builder withConsumeSettings(final AMQConsumeSettings consumeSettings) {
            this.consumeSettings = consumeSettings;
            return this;
        }

        public Builder withPublishSettings(final AMQPublishSettings pubSettings) {
            this.pubSettings = pubSettings;
            return this;
        }

        public AMQObservableQueue build() {
            return new AMQObservableQueue(factory, addresses,
                    consumeSettings, pubSettings,
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

    private AMQP.Exchange.DeclareOk getOrCreateExchange(final String name, final String type,
                                                        final boolean isDurable, final boolean autoDelete,
                                                        final Map<String, Object> arguments) throws IOException {
        if (StringUtils.isEmpty(name)) {
            throw new RuntimeException("Exchange name is undefined");
        }
        if (StringUtils.isEmpty(type)) {
            throw new RuntimeException("Exchange type is undefined");
        }
        if (!isClosed()) {
            open();
        }
        AMQP.Exchange.DeclareOk declareOk;
        try {
            declareOk = getOrCreateChannel().exchangeDeclarePassive(name);
        }
        catch (final IOException e) {
            logger.warn("Exchange {} of type {} might not exists", name, type, e);
            declareOk = getOrCreateChannel().exchangeDeclare(name, type, isDurable,
                    autoDelete, arguments);
        }
        return declareOk;
    }

    private AMQP.Queue.DeclareOk getOrCreateQueue() throws IOException {
        return getOrCreateQueue(subSettings.getQueueName(), subSettings.isDurable(), subSettings.isExclusive(),
                subSettings.autoDelete(), subSettings.getArguments());
    }

    private AMQP.Queue.DeclareOk getOrCreateQueue(final String name, final boolean isDurable, final boolean isExclusive,
                                                  final boolean autoDelete, final Map<String, Object> arguments)
            throws IOException {
        if (Strings.isNullOrEmpty(name)) {
            throw new RuntimeException("Queue name is undefined");
        }
        if (!isClosed()) {
            open();
        }
        AMQP.Queue.DeclareOk declareOk;
        try {
            declareOk = getOrCreateChannel().queueDeclarePassive(name);
        }
        catch (final IOException e) {
            logger.warn("Queue {} might not exists", name, e);
            declareOk = getOrCreateChannel().queueDeclare(name, isDurable, isExclusive,
                    autoDelete, arguments);
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

    private static Message asMessage(AMQConsumeSettings settings, GetResponse response) throws Exception {
        if (response == null) {
            return null;
        }
        final Message message = new Message();
        message.setId(response.getProps().getMessageId());
        message.setPayload(new String(response.getBody(), settings.getContentEncoding()));
        message.setReceipt(String.valueOf(response.getEnvelope().getDeliveryTag()));
        return message;
    }

    @VisibleForTesting
    protected List<Message> receiveMessages() {
        try {
            final AMQP.Queue.DeclareOk declareOk = getOrCreateQueue();
            final List<Message> messages = new LinkedList<>();
            getOrCreateChannel().basicQos(batchSize);
            Message message;
            int nb = 0;
            do {
                message = asMessage(getConsumeSettings(),
                        getOrCreateChannel().basicGet(declareOk.getQueue(), false));
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
}
