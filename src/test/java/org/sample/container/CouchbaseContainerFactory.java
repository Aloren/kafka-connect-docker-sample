package org.sample.container;

import com.playtika.test.couchbase.CouchbaseProperties;
import com.playtika.test.couchbase.rest.*;
import org.slf4j.Logger;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.WaitAllStrategy;
import org.testcontainers.containers.wait.WaitStrategy;

import java.time.Duration;

import static com.playtika.test.common.utils.ContainerUtils.containerLogsConsumer;

public class CouchbaseContainerFactory {

    public static final String COUCHBASE_NOST_NAME = "couchbase";

    public static GenericContainer create(CouchbaseProperties properties, Logger containerLogger, Network network) {
        return new FixedHostPortGenericContainer<>(properties.getDockerImage())
                .withFixedExposedPort(properties.getCarrierDirectPort(), properties.getCarrierDirectPort())
                .withFixedExposedPort(properties.getHttpDirectPort(), properties.getHttpDirectPort())
                .withFixedExposedPort(properties.getQueryServicePort(), properties.getQueryServicePort())
                .withFixedExposedPort(properties.getQueryRestTrafficPort(), properties.getQueryRestTrafficPort())
                .withFixedExposedPort(properties.getSearchServicePort(), properties.getSearchServicePort())
                .withFixedExposedPort(properties.getAnalyticsServicePort(), properties.getAnalyticsServicePort())
                .withFixedExposedPort(properties.getMemcachedSslPort(), properties.getMemcachedSslPort())
                .withFixedExposedPort(properties.getMemcachedPort(), properties.getMemcachedPort())
                .withFixedExposedPort(properties.getQueryRestTrafficSslPort(), properties.getQueryRestTrafficSslPort())
                .withFixedExposedPort(properties.getQueryServiceSslPort(), properties.getQueryServiceSslPort())
                .withNetworkAliases(COUCHBASE_NOST_NAME)
                .withNetwork(network)
                .withLogConsumer(containerLogsConsumer(containerLogger))
                .waitingFor(getCompositeWaitStrategy(properties));
    }

    /**
     * https://developer.couchbase.com/documentation/server/current/rest-api/rest-node-provisioning.html
     */
    private static WaitStrategy getCompositeWaitStrategy(CouchbaseProperties properties) {
        return new WaitAllStrategy()
                .withStrategy(new SetupNodeStorage(properties))
                .withStrategy(new SetupRamQuotas(properties))
                .withStrategy(new SetupServices(properties))
                .withStrategy(new SetupIndexesType(properties))
                .withStrategy(new SetupAdminUserAndPassword(properties))
                .withStrategy(new CreateBucket(properties))
                .withStrategy(new CreatePrimaryIndex(properties))
                .withStrategy(new CreateBucketUser(properties))
                .withStartupTimeout(Duration.ofMinutes(3));
    }
}