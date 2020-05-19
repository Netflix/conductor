package com.netflix.conductor.kafka.index.configuration;
        
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.kafka.index.producer.KafkaProducer;
import com.swiggy.kafka.clients.configs.*;
import com.swiggy.kafka.clients.configs.enums.ProducerAcks;
import com.swiggy.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class KafkaConfiguration {

    @Inject
    Configuration configuration;

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    public ProducerConfig producerConfig() {
        String primaryProducerBootStrapServer = configuration.getProperty("PRIMARY.BOOTSTRAP.SERVERS", "localhost:9092");
        String primaryProducerUsername = configuration.getProperty("KAFKA.TXN.PRIMARY.API.KEY", "");
        String primaryProducerPassword = configuration.getProperty("KAFKA.TXN.PRIMARY.API.SECRET", "");
        String primaryProducerAuthMechanism = configuration.getProperty("PRIMARY.AUTH.MECHANISM", "NONE");

        CommonConfig.Cluster primaryCluster =  new CommonConfig.Cluster();
        primaryCluster.setBootstrapServers(primaryProducerBootStrapServer);
        primaryCluster.setAuthMechanism(AuthMechanism.valueOf(primaryProducerAuthMechanism));
        primaryCluster.setUsername(primaryProducerUsername);
        primaryCluster.setPassword(primaryProducerPassword);

        ProducerConfig producerConfig = ProducerConfig.builder().primary(primaryCluster).clientId("ff-flo").enableCompression(true).acks(ProducerAcks.ALL).build();
        logger.info("Created producer config " + producerConfig + " successfully");
        return producerConfig;
    }

    public Topic floEventLogsTopic() {
        String name = configuration.getProperty("FLO.INDEXER.TOPIC.NAME", "ff-flo-event-logs");
        String keyId = configuration.getProperty("ENCRYPTION.KEY.ID", "flo_indexer_encryption_key_id");
        boolean enableEncryption = Boolean.valueOf(configuration.getProperty("FLO.INDEXER.TOPIC.ENABLE.ENCRYPTION", "false"));
        return Topic.builder().name(name).keyId(keyId).enableEncryption(enableEncryption).faultStrategy(Topic.FaultStrategy.NONE).build();
    }

    @Singleton
    public Producer getTier1Producer() {
        Map<String, Topic> topics = new HashMap<>();
        Topic topic = floEventLogsTopic();
        topics.put(topic.getName(), topic);
        return new Producer(producerConfig().toBuilder().topics(topics).build());
    }

}
