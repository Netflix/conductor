package com.netflix.conductor.contribs.queue.amqp;

/**
 * Last properties name for AMQP queues
 * Created at 22/03/2019 17:19
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
public enum AMQPublishParameters {

    PUBLISH_TARGET_TYPE("pubTargetType"),
    PUBLISH_NAME("pubName"),
    PUBLISH_ROUTING_KEY("pubRoutingKey"),
    PUBLISH_DELIVERY_MODE("pubDeliveryMode");

    String propertyName;

    AMQPublishParameters(String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public String toString() {
        return propertyName;
    }
}

