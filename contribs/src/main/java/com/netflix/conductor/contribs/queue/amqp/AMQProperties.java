package com.netflix.conductor.contribs.queue.amqp;

/**
 * Last properties name for AMQP queues
 * Created at 22/03/2019 17:19
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
public enum AMQProperties {

    CONTENT_TYPE("contentType"),
    CONTENT_ENCODING("contentEncoding"),
    IS_DURABLE("isDurable"),
    IS_EXCLUSIVE("isExclusive"),
    AUTO_DELETE("autoDelete"),
    DELIVERY_MODE("deliveryMode"),
    EXCHANGE_TYPE("exchangeType"),
    MAX_PRIORITY("maxPriority"),
    BATCH_SIZE("batchSize"),
    POLL_TIME_IN_MS("pollTimeInMs"),
    HOSTS("hosts"),
    USERNAME("username"),
    PASSWORD("password"),
    VIRTUAL_HOST("virtualHost"),
    PORT("port"),
    CONNECTION_TIMEOUT("connectionTimeout"),
    USE_NIO("useNio");

    String propertyName;

    AMQProperties(String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public String toString() {
        return propertyName;
    }
}

