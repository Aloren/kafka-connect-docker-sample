package org.sample;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;

import static org.junit.Assert.assertEquals;

public class KafkaConnectDockerSample {

    public static final File composeFile = new File("src/test/resources/docker-compose.yml");

    @ClassRule
    public static DockerComposeContainer stageEnvironment = new DockerComposeContainer(composeFile)
            .withPull(true)
            .withExposedService("connect", 8083,
                    // https://docs.confluent.io/current/connect/references/restapi.html
                    Wait.forHttp("/connectors")
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(2)));

    @Test
    public void shouldSetupKafkaConnect() throws Exception {
        Response response = RestAssured.get("http://localhost:8083/connectors");

        assertEquals(200, response.statusCode());
        assertEquals("[]", response.getBody().asString());
    }

}
