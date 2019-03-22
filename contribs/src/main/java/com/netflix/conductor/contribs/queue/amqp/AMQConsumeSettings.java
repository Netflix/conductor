package com.netflix.conductor.contribs.queue.amqp;

import com.netflix.conductor.core.config.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.regex.Matcher;

import static com.netflix.conductor.contribs.queue.amqp.AMQProperties.*;

/**
 * Settings for consuming messages from AMQP queue
 * Created at 22/03/2019 13:27
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
public class AMQConsumeSettings extends AMQSettings {

    private static final String PROPERTY_KEY_TEMPLATE = "workflow.event.queues.amqp.consume.%s";

    private String queueName;

    public AMQConsumeSettings(final Configuration config) {
        super(config.getBooleanProperty(String.format(PROPERTY_KEY_TEMPLATE, IS_DURABLE), true),
                config.getBooleanProperty(String.format(PROPERTY_KEY_TEMPLATE, IS_EXCLUSIVE), false),
                config.getBooleanProperty(String.format(PROPERTY_KEY_TEMPLATE, AUTO_DELETE), false),
                config.getProperty(String.format(PROPERTY_KEY_TEMPLATE, CONTENT_ENCODING), DEFAULT_CONTENT_ENCODING));
        setMaxPriority(config.getIntProperty(String.format(PROPERTY_KEY_TEMPLATE, MAX_PRIORITY), -1));
    }

    public AMQConsumeSettings setMaxPriority(final int maxPriority) {
        if (maxPriority > 0) {
            getArguments().put("x-max-priority", maxPriority);
        }
        return this;
    }

    public String getQueueName() {
        return queueName;
    }

    public AMQConsumeSettings setQueueName(String queueName) {
        if (StringUtils.isEmpty(queueName)) {
            throw new IllegalArgumentException("Queue name must be a non empty string");
        }
        this.queueName = queueName;
        return this;
    }

    public final AMQConsumeSettings fromURI(final String queueUri) {
        final Matcher matcher = QUEUE_URI_PATTERN.matcher(queueUri);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Queue URI doesn't matches the expected regexp");
        }
        return setQueueName(matcher.group("queueName"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AMQConsumeSettings)) return false;
        if (!super.equals(o)) return false;
        AMQConsumeSettings that = (AMQConsumeSettings) o;
        return Objects.equals(getQueueName(), that.getQueueName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getQueueName());
    }

    @Override
    public String toString() {
        return "AMQConsumeSettings{" +
                "queueName='" + queueName + '\'' +
                ", durable=" + isDurable() +
                ", exclusive=" + isExclusive() +
                ", autoDelete=" + autoDelete() +
                ", arguments=" + getArguments() +
                ", contentEncoding='" + getContentEncoding() + '\'' +
                "} " + super.toString();
    }
}
