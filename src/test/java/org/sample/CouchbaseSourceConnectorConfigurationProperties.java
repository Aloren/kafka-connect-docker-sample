package org.sample;

import lombok.Data;

@Data
public class CouchbaseSourceConnectorConfigurationProperties {

    String name = "test-couchbase-source";
    String topicName = "couchbase-changes";
    String bucket = "test";
    String bucketPassword = "password";
}
