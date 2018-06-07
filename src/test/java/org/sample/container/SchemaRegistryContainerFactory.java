package org.sample.container;

import com.playtika.test.kafka.properties.KafkaConfigurationProperties;
import org.sample.config.SchemaRegistryConfigurationProperties;
import org.slf4j.Logger;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.Wait;

import static com.playtika.test.common.utils.ContainerUtils.DEFAULT_CONTAINER_WAIT_DURATION;
import static com.playtika.test.common.utils.ContainerUtils.containerLogsConsumer;

public class SchemaRegistryContainerFactory {

    public static final String SCHEMA_REGISTRY_HOST_NAME = "schema-registry";

    public static GenericContainer create(SchemaRegistryConfigurationProperties properties,
                                          KafkaConfigurationProperties kafkaProperties,
                                          Logger logger, Network network) {
        return new FixedHostPortGenericContainer<>(properties.getDockerImage())
                .withLogConsumer(containerLogsConsumer(logger))
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KafkaContainerFactory.KAFKA_HOST_NAME + ":" + kafkaProperties.getBrokerPort())
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", SCHEMA_REGISTRY_HOST_NAME)
//                TODO: add SCHEMA_REGISTRY_KAFKASTORE_TOPIC
                .withExposedPorts(properties.getPort())
                .withFixedExposedPort(properties.getPort(), properties.getPort())
                .withNetwork(network)
                .withNetworkAliases(SCHEMA_REGISTRY_HOST_NAME)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(DEFAULT_CONTAINER_WAIT_DURATION))
                ;
    }
}
