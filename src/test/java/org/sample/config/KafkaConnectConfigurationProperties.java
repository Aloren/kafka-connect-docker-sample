package org.sample.config;

import lombok.Data;

@Data
public class KafkaConnectConfigurationProperties {

    String dockerImage = "confluentinc/cp-kafka-connect-base:4.1.0";
    int port = 8083;
    String pluginPath = "/usr/share/java";
    String groupId = "compose-connect-group";
    String configStorageTopic = "docker-connect-configs";
    String statusStorageTopic = "docker-connect-status";
    String offsetStorageTopic = "docker-connect-offsets";
    int offsetFlushIntervalMs = 10_000;
    int configStorageReplicationFactor = 1;
    int statusStorageReplicationFactor = 1;
    int offsetStorageReplicationFactor = 1;
}
