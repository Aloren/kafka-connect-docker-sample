package org.sample.steps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.awaitility.Awaitility.await;

public class KafkaSteps {

    private final KafkaConsumer<String, String> consumer;

    public KafkaSteps(String topic, String brokers) {
        this.consumer = createKafkaConsumer(topic, brokers);
    }

    public void createTopic() {

    }

    public List<String> consumeMessages(int messages) {
        return pollUntilRecordsReceived(messages)
                .stream()
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());
    }

    public void close() {
        consumer.close();
    }

    private List<ConsumerRecord<String, String>> pollUntilRecordsReceived(int maxMessages)  {
        List<ConsumerRecord<String, String>> consumedValues = new ArrayList<>();
        await().until (() ->{
            consumedValues.addAll(pollForRecords(1));
            return consumedValues.size() >= maxMessages;
        });
        return consumedValues;
    }

    private List<ConsumerRecord<String, String>> pollForRecords(int timeout) {
        ConsumerRecords<String, String> received = consumer.poll((timeout * 1000));
        if (received == null) {
            return emptyList();
        }
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        received.iterator().forEachRemaining(list::add);
        return list;
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String topic, String brokers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group[" + topic + "]");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(config);
        kafkaConsumer.subscribe(singletonList(topic));
        return kafkaConsumer;
    }
}
