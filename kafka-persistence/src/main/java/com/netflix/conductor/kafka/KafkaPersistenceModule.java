package com.netflix.conductor.kafka;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.KafkaConsumerDAO;
import com.netflix.conductor.dao.kafka.index.KafkaConsumer;
import com.netflix.conductor.dao.kafka.index.KafkaProducer;

/**
 * @author Manan
 * KafkaPersistenceModule.
 */
public class KafkaPersistenceModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(KafkaConsumer.class).asEagerSingleton();
    }
}
