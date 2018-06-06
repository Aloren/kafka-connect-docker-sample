package org.sample.container;

import com.playtika.test.kafka.checks.ZookeeperStatusCheck;
import com.playtika.test.kafka.properties.ZookeeperConfigurationProperties;
import org.slf4j.Logger;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.playtika.test.common.utils.ContainerUtils.containerLogsConsumer;

public class ZookeeperContainerFactory {

    public static final String ZOOKEEPER_NOST_NAME = "zookeeper";

    public static GenericContainer create(ZookeeperConfigurationProperties zookeeperProperties,
                                          Logger containerLogger, Network network) {
        String currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH-mm-ss-nnnnnnnnn"));
        String zkData = Paths.get(zookeeperProperties.getDataFileSystemBind(), currentTimestamp).toAbsolutePath().toString();
        String zkTransactionLogs = Paths.get(zookeeperProperties.getTxnLogsFileSystemBind(), currentTimestamp).toAbsolutePath().toString();
        int mappingPort = zookeeperProperties.getZookeeperPort();

        return new FixedHostPortGenericContainer<>(zookeeperProperties.getDockerImage())
                .withLogConsumer(containerLogsConsumer(containerLogger))
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(mappingPort))
                .withEnv("ZOOKEEPER_TICK_TIME", String.valueOf(2_000))
                .withFileSystemBind(zkData, "/var/lib/zookeeper/data", BindMode.READ_WRITE)
                .withFileSystemBind(zkTransactionLogs, "/var/lib/zookeeper/log", BindMode.READ_WRITE)
                .withExposedPorts(mappingPort)
                .withFixedExposedPort(mappingPort, mappingPort)
                .withNetwork(network)
                .withNetworkAliases(ZOOKEEPER_NOST_NAME)
                .waitingFor(new ZookeeperStatusCheck(zookeeperProperties));
    }
}
