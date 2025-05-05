package com.solace.samples.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.resources.Topic;

import java.io.*;
import java.util.*;

public class EventRouter {

    public static void main(String[] args) throws IOException, InterruptedException {

        // Solace connection details
        String host = "**";
        String vpn = "**";
        String username = "**";
        String password = "**";

        Properties solaceProps = new Properties();
        solaceProps.setProperty(SolaceProperties.TransportLayerProperties.HOST, host);
        solaceProps.setProperty(SolaceProperties.ServiceProperties.VPN_NAME, vpn);
        solaceProps.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_USER_NAME, username);
        solaceProps.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_PASSWORD, password);

        MessagingService messagingService = MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(solaceProps)
                .build()
                .connect();

        DirectMessagePublisher publisher = messagingService.createDirectMessagePublisherBuilder()
                .onBackPressureWait(1)
                .build()
                .start();

        List<JsonNode> events = getEventsFromFile(); // No customerId filtering here
        Map<String, Integer> messageCounts = new HashMap<>();

        System.out.println("\nProcessing events:\n");
        for (JsonNode event : events) {
            String eventType = event.get("eventType").asText();
            String topicName = buildTopicName(event);
            String messageContent = event.toString();
            String messageId = UUID.randomUUID().toString();

            OutboundMessage outboundMessage = messagingService.messageBuilder()
                    .withProperty("messageId", messageId)
                    .build(messageContent);

            publisher.publish(outboundMessage, Topic.of(topicName));

            messageCounts.put(eventType, messageCounts.getOrDefault(eventType, 0) + 1);
            System.out.printf("Published to Topic: %s | Message: %s | Message ID: %s%n",
                    topicName, messageContent, messageId);

            Thread.sleep(500);
        }

        System.out.println("\nSummary:");
        for (Map.Entry<String, Integer> entry : messageCounts.entrySet()) {
            System.out.printf("%s events published: %d%n", capitalize(entry.getKey()), entry.getValue());
        }

        publisher.terminate(1000);
        messagingService.disconnect();

        System.out.println("✅ Application terminated.");
    }

    private static String buildTopicName(JsonNode event) {
        return String.format("bank/%s/%s/%s/%s/%s/%s",
                event.get("lineOfBusiness").asText(),
                event.get("function").asText(),
                event.get("region").asText(),
                event.get("operation").asText(),
                event.get("resourceType").asText(),
                event.get("eventType").asText());
    }

    private static List<JsonNode> getEventsFromFile() {
        List<JsonNode> events = new ArrayList<>();
        try (InputStream inputStream = getPayloadInputStream()) {
            if (inputStream == null) {
                System.out.println("payloads.json not found in either classpath or current directory");
                return events;
            }

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(inputStream);
            JsonNode eventsNode = rootNode.get("actions");

            if (eventsNode == null || !eventsNode.isArray()) {
                System.err.println("'actions' array not found in payloads.json");
                return events;
            }

            for (JsonNode eventNode : eventsNode) {
                events.add(eventNode); // No filtering by customerId
            }
        } catch (IOException e) {
            System.err.println("Error loading payloads.json: " + e.getMessage());
        }
        return events;
    }

    private static InputStream getPayloadInputStream() throws FileNotFoundException {
        InputStream inputStream = EventRouter.class.getClassLoader().getResourceAsStream("payloads.json");
        if (inputStream == null) {
            File file = new File("payloads.json");
            if (file.exists()) {
                return new FileInputStream(file);
            }
        }
        return inputStream;
    }

    private static String capitalize(String s) {
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
}
