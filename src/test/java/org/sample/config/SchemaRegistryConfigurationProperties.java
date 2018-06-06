package org.sample.config;

import lombok.Data;

@Data
public class SchemaRegistryConfigurationProperties {
    private String dockerImage = "confluentinc/cp-schema-registry:4.1.0";
    private int port = 8081;

}
