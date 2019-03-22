package com.netflix.conductor.contribs.queue.amqp;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Created at 22/03/2019 13:28
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
abstract class AMQSettings {

    public static final String QUEUE_TYPE = "amqp";

    public static final String DEFAULT_CONTENT_ENCODING = "UTF-8";

    static final Pattern QUEUE_URI_PATTERN = Pattern.compile("^(?:"+ QUEUE_TYPE +")?\\:?(?<queueName>[^\\?]+)\\??(?<params>.*)$",
            Pattern.CASE_INSENSITIVE);

    private final boolean isDurable;
    private final boolean isExclusive;
    private final boolean autoDelete;

    private String contentEncoding;

    private final Map<String, Object> arguments = new HashMap<>();

    AMQSettings(final boolean isDurable, final boolean isExclusive, final boolean autoDelete,
                final String contentEncoding) {
        this.isDurable = isDurable;
        this.isExclusive = isExclusive;
        this.autoDelete = autoDelete;
        this.contentEncoding = contentEncoding;
    }

    public final boolean isDurable() {
        return isDurable;
    }

    public final boolean isExclusive() {
        return isExclusive;
    }

    public final boolean autoDelete() {
        return autoDelete;
    }

    public final Map<String, Object> getArguments() {
        return arguments;
    }

    public final String getContentEncoding() {
        return contentEncoding;
    }

    public abstract <T> T fromURI(String queueURI);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AMQSettings)) return false;
        AMQSettings that = (AMQSettings) o;
        return isDurable() == that.isDurable() &&
                isExclusive() == that.isExclusive() &&
                autoDelete() == that.autoDelete() &&
                Objects.equals(getContentEncoding(), that.getContentEncoding()) &&
                Objects.equals(getArguments(), that.getArguments());
    }

    @Override
    public int hashCode() {
        return Objects.hash(isDurable(), isExclusive(), autoDelete(), getContentEncoding(),
                getArguments());
    }
}
