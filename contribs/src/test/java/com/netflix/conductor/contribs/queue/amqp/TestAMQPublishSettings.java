package com.netflix.conductor.contribs.queue.amqp;

import com.netflix.conductor.core.config.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created at 22/03/2019 15:38
 *
 * @author Mickaël GREGORI <mickael.gregori@alchimie.com>
 * @version $Id$
 */
public class TestAMQPublishSettings {

    private Configuration config;

    @Before
    public void setUp() {
        config = mock(Configuration.class);
    }

    @Test
    public void testQueueURIWithDefaultConfig() {
        when(config.getProperty(anyString(), anyString())).thenAnswer(invocation -> invocation.getArguments()[1]);
        when(config.getBooleanProperty(anyString(), anyBoolean())).thenAnswer(invocation -> invocation.getArguments()[1]);
        when(config.getIntProperty(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArguments()[1]);
        final String queueURI = "amqp:myQueueName?pubTargetType=exchange&pubName=myPublishExchange&pubRoutingKey=myRoutingKey&pubDeliveryMode=2";
        AMQPublishSettings settings = new AMQPublishSettings(config);
        settings.fromURI(queueURI);
        assertEquals(AMQPublishSettings.DEFAULT_CONTENT_TYPE, settings.getContentType());
        assertEquals("myRoutingKey", settings.getRoutingKey());
        assertEquals("myPublishExchange", settings.getQueueOrExchangeName());
        assertEquals(2, settings.getDeliveryMode());
    }
}
