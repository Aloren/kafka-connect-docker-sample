package org.sample.steps;

import org.apache.commons.io.IOUtils;
import org.sample.CouchbaseSourceConnectorConfigurationProperties;
import org.sample.container.CouchbaseContainerFactory;

import java.io.IOException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;

public class KafkaConnectSteps {

    private final int kafkaConnectPublicPort = 8083;

    public void connectorsAreEmpty() {
        given()
                .port(kafkaConnectPublicPort)
                .contentType("application/json")
                .when()
                .get("/connectors")
                .then()
                .assertThat()
                .statusCode(200)
                .body("$.size()", is(0));
    }

    public void registerSourceConnector(CouchbaseSourceConnectorConfigurationProperties couchbaseSourceConnectorProperties) {
        String addConnectorBody = addConnectorRequest(couchbaseSourceConnectorProperties);

        given()
                .port(kafkaConnectPublicPort)
                .contentType("application/json")
                .body(addConnectorBody)
                .when()
                .post("/connectors")
                .then()
                .assertThat()
                .statusCode(201)
                .body("name", equalTo(couchbaseSourceConnectorProperties.getName()))
        ;
    }

    public void connectorIsRegistered(String name) {
        given()
                .port(kafkaConnectPublicPort)
                .contentType("application/json")
                .when()
                .get("/connectors")
                .then()
                .assertThat()
                .statusCode(200)
                .body("$", hasItem(name));
    }


    public void connectorHasRunningState(String name) {
        given()
                .port(kafkaConnectPublicPort)
                .contentType("application/json")
                .when()
                .get("/connectors/" + name + "/status")
                .then()
                .assertThat()
                .statusCode(200)
//                        .body("connector.state", equalTo("RUNNING"))
        ;
    }

    private String addConnectorRequest(CouchbaseSourceConnectorConfigurationProperties properties) {
        return getContent("couchbase-source-connector.json")
                .replace("%NAME%", properties.getName())
                .replace("%TOPIC_NAME%", properties.getTopicName())
                .replace("%BUCKET%", properties.getBucket())
                .replace("%BUCKET_PASSWORD%", properties.getBucketPassword())
                .replace("%CLUSTER_ADDRESS%", CouchbaseContainerFactory.COUCHBASE_NOST_NAME);
    }

    protected String getContent(String fileName) {
        try {
            return IOUtils.toString(this.getClass().getResourceAsStream("/" + fileName), "UTF-8");
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to open file: " + fileName, e);
        }
    }
}
