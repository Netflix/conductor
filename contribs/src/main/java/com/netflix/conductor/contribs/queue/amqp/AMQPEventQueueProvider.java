package com.netflix.conductor.contribs.queue.amqp;

import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.netflix.conductor.contribs.queue.amqp.AMQPObservableQueue.Builder;

/**
 * Created at 19/03/2019 16:29
 *
 * @author Ritu Parathody
 * @version $Id$ This code is based on the PR
 *          https://github.com/Netflix/conductor/pull/1063 which did not get
 *          merged to the master
 */
@Singleton
public class AMQPEventQueueProvider implements EventQueueProvider {

	private static Logger logger = LoggerFactory.getLogger(AMQPEventQueueProvider.class);

	protected Map<String, AMQPObservableQueue> queues = new ConcurrentHashMap<>();

	private final boolean useExchange;

	private final Configuration config;

	@Inject
	public AMQPEventQueueProvider(Configuration config, boolean useExchange) {
		this.config = config;
		this.useExchange = useExchange;
	}

	@Override
	public ObservableQueue getQueue(String queueURI) {
		if (logger.isInfoEnabled()) {
			logger.info("Retrieve queue with URI {}", queueURI);
		}
		// Build the queue with the inner Builder class of AMQPObservableQueue
		final AMQPObservableQueue queue = queues.computeIfAbsent(queueURI,
				q -> new Builder(config).build(useExchange, q));
		return queue;
	}
}