package org.sample.container;

import com.playtika.test.kafka.properties.ZookeeperConfigurationProperties;
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
                                          ZookeeperConfigurationProperties zookeeperProperties,
                                          Logger logger, Network network) {
        return new FixedHostPortGenericContainer<>(properties.getDockerImage())
                .withLogConsumer(containerLogsConsumer(logger))
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", ZookeeperContainerFactory.ZOOKEEPER_NOST_NAME + ":" + zookeeperProperties.getZookeeperPort())
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", SCHEMA_REGISTRY_HOST_NAME)
                .withExposedPorts(properties.getPort())
                .withFixedExposedPort(properties.getPort(), properties.getPort())
                .withNetwork(network)
                .withNetworkAliases(SCHEMA_REGISTRY_HOST_NAME)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(DEFAULT_CONTAINER_WAIT_DURATION))
                ;
    }
}
