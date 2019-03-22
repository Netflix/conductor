package com.netflix.conductor.contribs.queue.amqp;

import com.netflix.conductor.core.config.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;

import static com.netflix.conductor.contribs.queue.amqp.AMQProperties.*;
import static com.netflix.conductor.contribs.queue.amqp.AMQPublishParameters.*;

/**
 * Settings for publishing messages to AMQP queue or exchange \w routing key
 * Created at 22/03/2019 11:35
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
public class AMQPublishSettings extends AMQSettings {

    private static final String PROPERTY_KEY_TEMPLATE = "workflow.event.queues.amqp.publish.%s";

    public static final String DEFAULT_CONTENT_TYPE = "application/json";
    public static final String DEFAULT_EXCHANGE_TYPE = "topic";
    public static final int DEFAULT_DELIVERY_MODE = 2; // Persistent messages

    private String queueOrExchangeName, exchangeType, routingKey, contentType;
    private boolean useExchange;
    private int deliveryMode;

    public AMQPublishSettings(final Configuration config) {
        super(config.getBooleanProperty(String.format(PROPERTY_KEY_TEMPLATE, IS_DURABLE), true),
                config.getBooleanProperty(String.format(PROPERTY_KEY_TEMPLATE, IS_EXCLUSIVE), false),
                config.getBooleanProperty(String.format(PROPERTY_KEY_TEMPLATE, AUTO_DELETE), false),
                config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, CONTENT_ENCODING), DEFAULT_CONTENT_ENCODING));
        // Initialize exchangeType with a default value
        exchangeType = config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, EXCHANGE_TYPE), DEFAULT_EXCHANGE_TYPE);
        // Set common settings for publishing and consuming
        setDeliveryMode(config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, DELIVERY_MODE), DEFAULT_DELIVERY_MODE))
                .setContentType(DEFAULT_CONTENT_TYPE);
    }

    /**
     * Use queue for publishing
     * @param queueName the name of queue
     */
    public void setQueue(String queueName) {
        if (StringUtils.isEmpty(queueName)) {
            throw new IllegalArgumentException("Queue name for publishing is undefined");
        }
        this.useExchange = false;
        this.queueOrExchangeName = queueName;
    }

    /**
     * Use exchange + routing key for publishing
     * @param exchangeName the name of exchange
     * @param exchangeType the type of exchange
     * @param routingKey the routing key
     */
    public void setExchange(String exchangeName, String exchangeType, String routingKey) {
        if (StringUtils.isEmpty(exchangeName)) {
            throw new IllegalArgumentException("Exchange name for publishing is undefined");
        }
        if (StringUtils.isEmpty(exchangeType)) {
            throw new IllegalArgumentException("Exchange type for publishing is undefined");
        }
        if (StringUtils.isEmpty(routingKey)) {
            throw new IllegalArgumentException("Routing key for publishing is undefined");
        }
        this.useExchange = true;
        this.queueOrExchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.routingKey = routingKey;
    }

    public String getQueueOrExchangeName() {
        return queueOrExchangeName;
    }

    public String getExchangeType() {
        return exchangeType;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public boolean useExchange() {
        return useExchange;
    }

    public int getDeliveryMode() {
        return deliveryMode;
    }

    public AMQPublishSettings setDeliveryMode(int deliveryMode) {
        if (deliveryMode < 1 && deliveryMode > 2) {
            throw new IllegalArgumentException("Delivery mode must be 1 or 2");
        }
        this.deliveryMode = deliveryMode;
        return this;
    }

    public String getContentType() {
        return contentType;
    }

    public AMQPublishSettings setContentType(String contentType) {
        if (StringUtils.isEmpty(contentType)) {
            throw new IllegalArgumentException("Content type is null or empty");
        }
        this.contentType = contentType;
        return this;
    }

    public final AMQPublishSettings fromURI(final String queueURI) {
        final Matcher matcher = QUEUE_URI_PATTERN.matcher(queueURI);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Queue URI doesn't matches the expected regexp");
        }
        if (matcher.groupCount() == 1) {
            // No parameter. Publish messages to the same queue
            setQueue(matcher.group("queueName"));
        }
        else if (matcher.groupCount() > 1) {
            final String queryParams = matcher.group("params");
            if (StringUtils.isNotEmpty(queryParams)) {
                // Handle parameters
                final Map<String, String> params = new HashMap<>();
                Arrays.stream(queryParams.split("\\s*\\&\\s*")).forEach(
                        param -> {
                            final String[] kv = param.split("\\s*=\\s*");
                            if (kv.length == 2) {
                                params.put(kv[0], kv[1]);
                            }
                        }
                );
                if (!params.isEmpty()) {
                    // Set the exchange to use from parameters
                    setExchange(params.get(String.valueOf(PUBLISH_NAME)),
                            params.getOrDefault(String.valueOf(PUBLISH_TARGET_TYPE), exchangeType),
                            params.get(String.valueOf(PUBLISH_ROUTING_KEY)));
                    if (params.containsKey(String.valueOf(PUBLISH_DELIVERY_MODE))) {
                        setDeliveryMode(Integer.parseInt(params.get(String.valueOf(PUBLISH_DELIVERY_MODE))));
                    }
                }
            }
        }
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AMQPublishSettings)) return false;
        AMQPublishSettings that = (AMQPublishSettings) o;
        return useExchange == that.useExchange &&
                getDeliveryMode() == that.getDeliveryMode() &&
                getQueueOrExchangeName().equals(that.getQueueOrExchangeName()) &&
                Objects.equals(getExchangeType(), that.getExchangeType()) &&
                Objects.equals(getRoutingKey(), that.getRoutingKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getQueueOrExchangeName(), useExchange, getDeliveryMode(),
                getExchangeType(), getRoutingKey());
    }

    @Override
    public String toString() {
        return "AMQPublishSettings{" +
                "queueOrExchangeName='" + queueOrExchangeName + '\'' +
                ", useExchange=" + useExchange +
                ", deliveryMode=" + deliveryMode +
                ", exchangeType='" + exchangeType + '\'' +
                ", routingKey='" + routingKey + '\'' +
                ", isDurable=" + isDurable() +
                ", isExclusive=" + isExclusive() +
                ", autoDelete=" + autoDelete() +
                ", arguments=" + getArguments() +
                "} " + super.toString();
    }
}
