package org.uni.potsdam.p1.execution;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaTopicManager {
    public static void ensureTopicExists(List<String> requiredTopic, String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {

            ListTopicsResult listTopics = admin.listTopics();
            Set<String> existingTopics = listTopics.names().get();

            // Get existing topics
            List<NewTopic> topicsToCreate = requiredTopic.stream()
                .filter(topic -> !existingTopics.contains(topic))
                .map(topic -> new NewTopic(topic, 1, (short) 1))
                .collect(Collectors.toList());
            
            // Create missing topics
            if (!topicsToCreate.isEmpty()) {
                CreateTopicsResult createTopicsResult = admin.createTopics(topicsToCreate);
                createTopicsResult.all().get(); // Wait for completion
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Kafka topics", e);
        }
    }
}
