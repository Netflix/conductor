package com.netflix.conductor.core.events.amqp;

import com.netflix.conductor.contribs.queue.amqp.AMQObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created at 21/03/2019 16:23
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
public class TestAMQEventQueueProvider {

    private Configuration configuration;

    @Before
    public void setup() {
        configuration = mock(Configuration.class);
    }

    @Test
    public void testGetQueueWithDefaultConfiguration() {
        when(configuration.getProperty(anyString(), anyString())).thenAnswer(invocation -> invocation.getArguments()[1]);
        when(configuration.getBooleanProperty(anyString(), anyBoolean())).thenAnswer(invocation -> invocation.getArguments()[1]);
        when(configuration.getIntProperty(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArguments()[1]);

        final String queueUri = "test_queue_1";
        AMQEventQueueProvider amqEventQueueProvider = new AMQEventQueueProvider(configuration);
        AMQObservableQueue amqObservableQueue = (AMQObservableQueue) amqEventQueueProvider.getQueue(queueUri);

        assertNotNull(amqObservableQueue);

        assertNotNull(amqObservableQueue.getConnectionFactory());
        assertEquals(ConnectionFactory.DEFAULT_HOST, amqObservableQueue.getConnectionFactory().getHost());
        assertEquals(ConnectionFactory.DEFAULT_AMQP_PORT, amqObservableQueue.getConnectionFactory().getPort());
        assertEquals(ConnectionFactory.DEFAULT_USER, amqObservableQueue.getConnectionFactory().getUsername());
        assertEquals(ConnectionFactory.DEFAULT_PASS, amqObservableQueue.getConnectionFactory().getPassword());
        assertEquals(ConnectionFactory.DEFAULT_VHOST, amqObservableQueue.getConnectionFactory().getVirtualHost());

        assertTrue(amqObservableQueue.isDurable());
        assertFalse(amqObservableQueue.isExclusive());

        assertNotNull(amqObservableQueue.getAddresses());

        assertEquals("amqp", amqObservableQueue.getType());
        assertEquals(queueUri, amqObservableQueue.getQueueName());
        assertEquals(queueUri, amqObservableQueue.getURI());

        assertEquals(AMQObservableQueue.DEFAULT_BATCH_SIZE, amqObservableQueue.getBatchSize());
        assertEquals(AMQObservableQueue.DEFAULT_POLL_TIME_MS, amqObservableQueue.getPollTimeInMS());
        assertTrue(amqObservableQueue.getQueueArgs().isEmpty());
    }

    private static int getRandomInt(int multiplier) {
        return Double.valueOf(Math.random()*multiplier).intValue();
    }

    @Test
    public void testGetQueueWithSpecificConfiguration() {
        // Hosts
        final Address[] addresses = new Address[]{
                new Address("rabbit-1", getRandomInt(10000)),
                new Address("rabbit-2", getRandomInt(10000)),
        };
        when(configuration.getProperty(eq("workflow.event.queues.amqp.hosts"), anyString())).
                thenReturn(Arrays.stream(addresses).map(a -> a.toString()).collect(Collectors.joining(",")));

        // Port
        final int port = getRandomInt(10000);
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.port"), anyInt())).thenReturn(port);

        // Credentials
        final String username = RandomStringUtils.randomAscii(10),
                password =  RandomStringUtils.randomAlphanumeric(20),
                vhost =  RandomStringUtils.randomAlphabetic(20);
        when(configuration.getProperty(eq("workflow.event.queues.amqp.username"), anyString())).thenReturn(username);
        when(configuration.getProperty(eq("workflow.event.queues.amqp.password"), anyString())).thenReturn(password);
        when(configuration.getProperty(eq("workflow.event.queues.amqp.virtualHost"), anyString())).thenReturn(vhost);
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.connectionTimeout"), anyInt()))
                .thenReturn(20);

        // Add priority
        int maxPriority = getRandomInt(10) + 1;
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.maxPriority"), anyInt()))
                .thenReturn(maxPriority);

        // Add polling settings
        int batchSize = getRandomInt(100000), pollTimeInMs = getRandomInt(100000);
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.batchSize"), anyInt()))
                .thenReturn(batchSize);
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.pollTimeInMs"), anyInt()))
                .thenReturn(pollTimeInMs);

        boolean isDurable = getRandomInt(10) > 5, isExclusive = getRandomInt(10) > 5;
        when(configuration.getBooleanProperty(eq("workflow.event.queues.amqp.durable"), anyBoolean()))
                .thenReturn(isDurable);
        when(configuration.getBooleanProperty(eq("workflow.event.queues.amqp.exclusive"), anyBoolean()))
                .thenReturn(isExclusive);

        final String queueUri = RandomStringUtils.randomAlphabetic(20);
        AMQEventQueueProvider amqEventQueueProvider = new AMQEventQueueProvider(configuration);
        AMQObservableQueue amqObservableQueue = (AMQObservableQueue) amqEventQueueProvider.getQueue(queueUri);

        assertNotNull(amqObservableQueue);
        assertNotNull(amqObservableQueue.getConnectionFactory());
        assertEquals(port, amqObservableQueue.getConnectionFactory().getPort());
        assertEquals(username, amqObservableQueue.getConnectionFactory().getUsername());
        assertEquals(password, amqObservableQueue.getConnectionFactory().getPassword());
        assertEquals(vhost, amqObservableQueue.getConnectionFactory().getVirtualHost());
        assertEquals(20, amqObservableQueue.getConnectionFactory().getConnectionTimeout());

        assertEquals(isDurable, amqObservableQueue.isDurable());
        assertEquals(isExclusive, amqObservableQueue.isExclusive());

        assertNotNull(amqObservableQueue.getAddresses());
        assertEquals(addresses.length, amqObservableQueue.getAddresses().length);
        assertArrayEquals(addresses, amqObservableQueue.getAddresses());

        assertEquals("amqp", amqObservableQueue.getType());
        assertEquals(queueUri, amqObservableQueue.getQueueName());
        assertEquals(queueUri, amqObservableQueue.getURI());

        assertEquals(batchSize, amqObservableQueue.getBatchSize());
        assertEquals(pollTimeInMs, amqObservableQueue.getPollTimeInMS());
        assertFalse(amqObservableQueue.getQueueArgs().isEmpty());
        assertTrue(amqObservableQueue.getQueueArgs().containsKey("x-max-priority"));
        assertEquals(maxPriority, amqObservableQueue.getQueueArgs().get("x-max-priority"));
    }

    private void testGetQueueWithEmptyValue(String prop) {
        when(configuration.getProperty(eq("workflow.event.queues.amqp."+ prop), anyString())).
                thenReturn(StringUtils.EMPTY);
        AMQEventQueueProvider amqEventQueueProvider = new AMQEventQueueProvider(configuration);
        amqEventQueueProvider.getQueue(RandomStringUtils.randomAlphabetic(20));
    }

    private void testGetQueueWithNegativeValue(String prop) {
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp."+ prop), anyInt())).
                thenReturn(-1);
        AMQEventQueueProvider amqEventQueueProvider = new AMQEventQueueProvider(configuration);
        amqEventQueueProvider.getQueue(RandomStringUtils.randomAlphabetic(20));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueueWithEmptyHosts() {
        testGetQueueWithEmptyValue("hosts");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueueWithEmptyUsername() {
        when(configuration.getProperty(eq("workflow.event.queues.amqp.hosts"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        testGetQueueWithEmptyValue("username");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueueWithEmptyPassword() {
        when(configuration.getProperty(eq("workflow.event.queues.amqp.hosts"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.username"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        testGetQueueWithEmptyValue("password");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueueWithEmptyVhost() {
        when(configuration.getProperty(eq("workflow.event.queues.amqp.hosts"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.username"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.password"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        testGetQueueWithEmptyValue("virtualHost");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueueWithNegativePort() {
        when(configuration.getProperty(eq("workflow.event.queues.amqp.hosts"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.username"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.password"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.virtualHost"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        testGetQueueWithNegativeValue("port");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueueWithNegativeConnectionTimeout() {
        when(configuration.getProperty(eq("workflow.event.queues.amqp.hosts"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.username"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.password"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.virtualHost"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.port"), anyInt())).
                thenReturn(getRandomInt(100));
        testGetQueueWithNegativeValue("connectionTimeout");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueueWithNegativeBatchSize() {
        when(configuration.getProperty(eq("workflow.event.queues.amqp.hosts"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.username"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.password"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.virtualHost"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.port"), anyInt())).
                thenReturn(getRandomInt(100));
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.connectionTimeout"), anyInt())).
                thenReturn(getRandomInt(100));
        testGetQueueWithNegativeValue("batchSize");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueueWithNegativePollTimeInMs() {
        when(configuration.getProperty(eq("workflow.event.queues.amqp.hosts"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.username"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.password"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getProperty(eq("workflow.event.queues.amqp.virtualHost"), anyString())).
                thenReturn(RandomStringUtils.randomAlphabetic(10));
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.port"), anyInt())).
                thenReturn(getRandomInt(100));
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.connectionTimeout"), anyInt())).
                thenReturn(getRandomInt(100));
        when(configuration.getIntProperty(eq("workflow.event.queues.amqp.batchSize"), anyInt())).
                thenReturn(getRandomInt(100));
        testGetQueueWithNegativeValue("pollTimeInMs");
    }

}
