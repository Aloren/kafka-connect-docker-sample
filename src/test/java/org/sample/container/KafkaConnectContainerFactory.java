package org.sample.container;

import com.playtika.test.kafka.properties.KafkaConfigurationProperties;
import com.playtika.test.kafka.properties.ZookeeperConfigurationProperties;
import org.sample.config.KafkaConnectConfigurationProperties;
import org.sample.config.SchemaRegistryConfigurationProperties;
import org.slf4j.Logger;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.Wait;

import static com.playtika.test.common.utils.ContainerUtils.containerLogsConsumer;

public class KafkaConnectContainerFactory {
    private static final String KAFKA_CONNECT_NOST_NAME = "connect";

    public static GenericContainer create(KafkaConnectConfigurationProperties properties,
                                          SchemaRegistryConfigurationProperties schemaRegistryProperties,
                                          ZookeeperConfigurationProperties zookeeperProperties,
                                          KafkaConfigurationProperties kafkaProperties,
                                          Logger logger, Network network) {
        return new FixedHostPortGenericContainer<>(properties.getDockerImage())
                .withClasspathResourceMapping("/jars", "/usr/share/java/kafka-connect-couchbase", BindMode.READ_ONLY)
                .withLogConsumer(containerLogsConsumer(logger))
                .withEnv("CONNECT_PLUGIN_PATH", properties.getPluginPath())
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", KafkaContainerFactory.KAFKA_HOST_NAME + ":" + kafkaProperties.getBrokerPort())
                .withEnv("CONNECT_ZOOKEEPER_CONNECT", ZookeeperContainerFactory.ZOOKEEPER_NOST_NAME + ":" + zookeeperProperties.getZookeeperPort())
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", KAFKA_CONNECT_NOST_NAME)
                .withEnv("CONNECT_REST_PORT", String.valueOf(properties.getPort()))

                .withEnv("CONNECT_GROUP_ID", properties.getGroupId())
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", properties.getConfigStorageTopic())
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", properties.getStatusStorageTopic())
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", properties.getOffsetStorageTopic())

                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", String.valueOf(properties.getConfigStorageReplicationFactor()))
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", String.valueOf(properties.getStatusStorageReplicationFactor()))
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", String.valueOf(properties.getOffsetStorageReplicationFactor()))
                .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", String.valueOf(properties.getOffsetFlushIntervalMs()))

                .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")

                .withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", "http://" + SchemaRegistryContainerFactory.SCHEMA_REGISTRY_HOST_NAME + ":" + schemaRegistryProperties.getPort())
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", "http://" + SchemaRegistryContainerFactory.SCHEMA_REGISTRY_HOST_NAME + ":" + schemaRegistryProperties.getPort())

//                .withEnv("CONNECT_LOG4J_LOGGERS", "org.apache.kafka.connect.runtime.WorkerSourceTask=TRACE")
                .withExposedPorts(properties.getPort())
                .withFixedExposedPort(properties.getPort(), properties.getPort())
                .withNetworkAliases(KAFKA_CONNECT_NOST_NAME)
                .withNetwork(network)
                .waitingFor(Wait.forListeningPort());
    }

}
