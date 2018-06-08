package org.sample;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.StringDocument;
import com.playtika.test.common.utils.ContainerUtils;
import com.playtika.test.couchbase.CouchbaseProperties;
import com.playtika.test.kafka.KafkaTopicsConfigurer;
import com.playtika.test.kafka.properties.KafkaConfigurationProperties;
import com.playtika.test.kafka.properties.ZookeeperConfigurationProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.sample.config.KafkaConnectConfigurationProperties;
import org.sample.config.SchemaRegistryConfigurationProperties;
import org.sample.container.*;
import org.sample.steps.CouchbaseSteps;
import org.sample.steps.KafkaConnectSteps;
import org.sample.steps.KafkaSteps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class SampleTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final KafkaConnectSteps kafkaConnectSteps = new KafkaConnectSteps();
    private final CouchbaseSteps couchbaseSteps = new CouchbaseSteps();
    GenericContainer couchbase;
    GenericContainer zookeeper;
    GenericContainer kafka;
    GenericContainer schemaRegistry;
    GenericContainer kafkaConnect;
    private KafkaSteps kafkaSteps;

    @After
    public void tearDown() throws Exception {
        couchbaseSteps.shutdownConnection();
        if (kafkaSteps != null) {
            kafkaSteps.close();
        }
        stop(kafkaConnect);
        stop(schemaRegistry);
        stop(kafka);
        stop(zookeeper);
        stop(couchbase);
    }

    @Test
    public void kafkaConnectorShouldPostMessageToKafka() throws Exception {
        Network network = Network.newNetwork();

        CouchbaseProperties couchbaseProperties = new CouchbaseProperties();
        couchbase = CouchbaseContainerFactory.create(couchbaseProperties, logger, network);
        couchbase.start();

        ZookeeperConfigurationProperties zookeeperProperties = new ZookeeperConfigurationProperties();
        zookeeperProperties.setZookeeperPort(ContainerUtils.getAvailableMappingPort());
        zookeeper = ZookeeperContainerFactory.create(zookeeperProperties, logger, network);
        zookeeper.start();

        KafkaConfigurationProperties kafkaProperties = new KafkaConfigurationProperties();
        kafkaProperties.setBrokerPort(ContainerUtils.getAvailableMappingPort());
        kafka = KafkaContainerFactory.create(kafkaProperties, zookeeperProperties, logger, network);
        kafka.start();

        CouchbaseSourceConnectorConfigurationProperties couchbaseSourceConnectorProperties =
                new CouchbaseSourceConnectorConfigurationProperties();
        KafkaConnectConfigurationProperties kafkaConnectProperties = new KafkaConnectConfigurationProperties();

        KafkaTopicsConfigurer topicsConfigurer =
                new KafkaTopicsConfigurer(kafka,
                        ZookeeperContainerFactory.ZOOKEEPER_NOST_NAME + ":" + zookeeperProperties.getZookeeperPort(), kafkaProperties);
        topicsConfigurer.createTopics(Arrays.asList(couchbaseSourceConnectorProperties.getTopicName(),
                kafkaConnectProperties.getConfigStorageTopic(),
                kafkaConnectProperties.getOffsetStorageTopic(),
                kafkaConnectProperties.getStatusStorageTopic()
                ));

        SchemaRegistryConfigurationProperties schemaRegistryProperties = new SchemaRegistryConfigurationProperties();
        schemaRegistry = SchemaRegistryContainerFactory.create(schemaRegistryProperties, kafkaProperties, logger, network);
        schemaRegistry.start();


        kafkaConnect = KafkaConnectContainerFactory.create(kafkaConnectProperties, schemaRegistryProperties,
                zookeeperProperties, kafkaProperties, logger, network);
        kafkaConnect.start();

        kafkaConnectSteps.connectorsAreEmpty();

        kafkaConnectSteps.registerSourceConnector(couchbaseSourceConnectorProperties);

        //fuuuu remove me (but somehow add artificial delay, cause without it next response status will be 409
        Thread.sleep(5_000);

        kafkaConnectSteps.connectorIsRegistered(couchbaseSourceConnectorProperties.getName());

        kafkaConnectSteps.connectorHasRunningState(couchbaseSourceConnectorProperties.getName());

        //TODO: use external kafka port
        String brokers = "localhost:" + (kafkaProperties.getBrokerPort() + 1);
        kafkaSteps = new KafkaSteps(couchbaseSourceConnectorProperties.getTopicName(), brokers);

        Bucket bucket = couchbaseSteps.createConnection(couchbaseProperties);

        bucket.insert(StringDocument.create("first", "nastya"));

        assertThat(bucket.get("first", StringDocument.class).content()).isEqualTo("nastya");

        List<ConsumerRecord<String, String>> consumed = kafkaSteps.pollForRecords(30_000);

        assertThat(consumed).hasSize(1);

        System.out.println("nastya has finally setup kafka-connect!");
    }


    private void stop(GenericContainer container) {
        if (container != null) {
            try {
                container.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
