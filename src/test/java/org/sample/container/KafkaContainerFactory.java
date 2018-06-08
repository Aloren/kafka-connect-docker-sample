package org.sample.container;

import com.playtika.test.common.utils.ContainerUtils;
import com.playtika.test.kafka.checks.KafkaStatusCheck;
import com.playtika.test.kafka.properties.KafkaConfigurationProperties;
import com.playtika.test.kafka.properties.ZookeeperConfigurationProperties;
import org.slf4j.Logger;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.WaitStrategy;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.playtika.test.common.utils.ContainerUtils.containerLogsConsumer;
import static org.sample.container.ZookeeperContainerFactory.ZOOKEEPER_NOST_NAME;

public class KafkaContainerFactory {

    public static final String KAFKA_HOST_NAME = "broker";

    public static GenericContainer create(KafkaConfigurationProperties kafkaProperties,
                                          ZookeeperConfigurationProperties zookeeperProperties,
                                          Logger logger, Network network) {
        int kafkaInternalPort = kafkaProperties.getBrokerPort();
        //TODO: move to config
        int kafkaExternalPort = ContainerUtils.getAvailableMappingPort();

        String currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH-mm-ss-nnnnnnnnn"));
        String kafkaData = Paths.get(kafkaProperties.getDataFileSystemBind(), currentTimestamp).toAbsolutePath().toString();

        WaitStrategy waitStrategy = new KafkaStatusCheck(kafkaProperties);

        return new FixedHostPortGenericContainer<>(kafkaProperties.getDockerImage())
                .withLogConsumer(containerLogsConsumer(logger))
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(KAFKA_HOST_NAME))
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", ZOOKEEPER_NOST_NAME + ":" + zookeeperProperties.getZookeeperPort())
                .withEnv("KAFKA_BROKER_ID", "-1")
                //see: https://stackoverflow.com/questions/41868161/kafka-in-kubernetes-cluster-how-to-publish-consume-messages-from-outside-of-kub
                //see: https://github.com/wurstmeister/kafka-docker/blob/master/README.md
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL_PLAINTEXT:PLAINTEXT,EXTERNAL_PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL_PLAINTEXT://" + KAFKA_HOST_NAME + ":" + kafkaInternalPort + ",EXTERNAL_PLAINTEXT://localhost:" + kafkaExternalPort)
                .withEnv("KAFKA_LISTENERS", "INTERNAL_PLAINTEXT://0.0.0.0:" + kafkaInternalPort + ",EXTERNAL_PLAINTEXT://0.0.0.0:" + kafkaExternalPort)
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL_PLAINTEXT")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", String.valueOf(kafkaProperties.getReplicationFactor()))
                .withEnv("KAFKA_LOG_FLUSH_INTERVAL_MS", String.valueOf(kafkaProperties.getLogFlushIntervalMs()))
                .withEnv("KAFKA_REPLICA_SOCKET_TIMEOUT_MS", String.valueOf(kafkaProperties.getReplicaSocketTimeoutMs()))
                .withEnv("KAFKA_CONTROLLER_SOCKET_TIMEOUT_MS", String.valueOf(kafkaProperties.getControllerSocketTimeoutMs()))
                .withFileSystemBind(kafkaData, "/var/lib/kafka/data", BindMode.READ_WRITE)
                .withExposedPorts(kafkaInternalPort, kafkaExternalPort)
                .withFixedExposedPort(kafkaInternalPort, kafkaInternalPort)
                .withFixedExposedPort(kafkaExternalPort, kafkaExternalPort)
                .withNetwork(network)
                .withNetworkAliases(KAFKA_HOST_NAME)
                .withExtraHost(KAFKA_HOST_NAME, "127.0.0.1")
                .waitingFor(waitStrategy);
    }
}
