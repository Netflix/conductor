package com.netflix.conductor.core.events.amqp;

import com.netflix.conductor.contribs.queue.amqp.AMQConsumeSettings;
import com.netflix.conductor.contribs.queue.amqp.AMQObservableQueue;
import com.netflix.conductor.contribs.queue.amqp.AMQPublishSettings;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.netflix.conductor.contribs.queue.amqp.AMQObservableQueue.Builder;

/**
 * Created at 19/03/2019 16:29
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
@Singleton
public class AMQEventQueueProvider implements EventQueueProvider {

    private static Logger logger = LoggerFactory.getLogger(AMQEventQueueProvider.class);

    protected Map<String, AMQObservableQueue> queues = new ConcurrentHashMap<>();

    private Configuration config;

    @Inject
    public AMQEventQueueProvider(Configuration config) {
        this.config = config;
    }

    @Override
    public ObservableQueue getQueue(String queueURI) {
        if (logger.isInfoEnabled()) {
            logger.info("Retrieve queue with URI {}", queueURI);
        }
        // Build the queue with the inner Builder class of AMQObservableQueue
        final AMQObservableQueue queue = queues.computeIfAbsent(queueURI,
                q -> new Builder(config)
                        .withConsumeSettings(new AMQConsumeSettings(config).fromURI(q))
                        .withPublishSettings(new AMQPublishSettings(config).fromURI(q))
                        .build());
        return queue;
    }
}
