package com.solace.samples.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import com.solace.messaging.resources.Topic;

import java.io.*;
import java.util.*;

public class HelloWorld {

    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String accountId = "";
        while (accountId.isEmpty()) {
            System.out.print("Enter the account ID (e.g., 12345): ");
            accountId = reader.readLine().trim();
        }

        final String targetMessageId = getTargetMessageId(reader);

        // 🔒 Hardcoded Solace connection details
        String host = "tcps://mr-connection-6ngc89ky4et.messaging.solace.cloud:55443";
        String vpn = "solacesample";
        String username = "solace-cloud-client";
        String password = "n97dj1rfu968ss09ovkte7qmo4";

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

        List<JsonNode> actions = getActionsFromFile(accountId);
        Map<String, Integer> messageCounts = new HashMap<>();

        System.out.println("\nPublishing messages to topics:\n");
        for (JsonNode action : actions) {
            String topic = String.format(action.get("topic").asText(), accountId);
            String messageContent = String.format(action.get("message").asText(), accountId);

            OutboundMessage outboundMessage = messagingService.messageBuilder().build(messageContent);
            publisher.publish(outboundMessage, Topic.of(topic));

            messageCounts.put(topic, messageCounts.getOrDefault(topic, 0) + 1);
            System.out.printf("Published to Topic: %s | Message: %s%n", topic, messageContent);

            Thread.sleep(500);
        }

        System.out.println("\nSummary:");
        for (Map.Entry<String, Integer> entry : messageCounts.entrySet()) {
            System.out.printf("Topic: %s | Messages Published: %d%n", entry.getKey(), entry.getValue());
        }

        System.out.println("\nReplaying specific message from queue_debit...\n");
        PersistentMessageReceiver receiver = messagingService
                .createPersistentMessageReceiverBuilder()
                .build(Queue.durableExclusiveQueue("queue_debit"));

        receiver.start();

        final int[] messageCount = {0};

        receiver.receiveAsync((InboundMessage inboundMessage) -> {
            try {
                String payload = new String(inboundMessage.getPayloadAsBytes());
                String topicString = inboundMessage.getDestinationName();
                Map<String, String> properties = inboundMessage.getProperties();
                String receivedMessageId = properties.get("messageId");

                if (receivedMessageId != null && receivedMessageId.equals(targetMessageId)) {
                    OutboundMessage outboundMessage = messagingService.messageBuilder().build(payload);
                    publisher.publish(outboundMessage, Topic.of(topicString));
                    messageCount[0]++;
                    System.out.printf("Replaying message from queue_debit: %s | Message ID: %s%n", payload, receivedMessageId);
                }

            } catch (Exception e) {
                System.err.println("Error while replaying message: " + e.getMessage());
            }
        });

        Thread.sleep(5000);

        if (messageCount[0] > 0) {
            System.out.println("✅ Specific message replayed successfully.");
        } else {
            System.out.println("❌ No matching messages found to replay.");
        }

        receiver.terminate(1000);
        publisher.terminate(1000);
        messagingService.disconnect();

        System.out.println("✅ Application terminated.");
    }

    private static List<JsonNode> getActionsFromFile(String accountId) {
        List<JsonNode> actions = new ArrayList<>();
        try (InputStream inputStream = HelloWorld.class.getClassLoader().getResourceAsStream("payloads.json")) {
            if (inputStream == null) {
                System.out.println("payloads.json file not found!");
                return actions;
            }
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(inputStream);
            JsonNode actionsNode = rootNode.get("actions");

            for (JsonNode actionNode : actionsNode) {
                actions.add(actionNode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return actions;
    }

    private static String getTargetMessageId(BufferedReader reader) throws IOException {
        String targetMessageId = "";
        while (targetMessageId.isEmpty()) {
            System.out.print("Enter the message ID to replay (e.g., abc123): ");
            targetMessageId = reader.readLine().trim();
        }
        return targetMessageId;
    }
}
