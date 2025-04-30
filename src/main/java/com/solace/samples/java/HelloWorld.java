package com.solace.samples.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties.AuthenticationProperties;
import com.solace.messaging.config.SolaceProperties.ServiceProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.MessageReceiver.MessageHandler;
import com.solace.messaging.resources.Topic;
import com.solace.messaging.resources.TopicSubscription;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class HelloWorld {

    private static volatile boolean isShutdown = false;

    // AtomicReference to store the last received message's ID (for replay)
    private static AtomicReference<String> lastReceivedMessageId = new AtomicReference<>("");

    public static void main(String... args) throws IOException, InterruptedException {

        // Load configuration from external properties file
        Properties properties = loadConfig("application.properties");

        // Extract Solace connection details from properties file
        String host = properties.getProperty("solace.host");
        String vpn = properties.getProperty("solace.vpn");
        String username = properties.getProperty("solace.username");
        String password = properties.getProperty("solace.password");

        // Ask for account ID
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String accountId = "";
        while (accountId.isEmpty()) {
            System.out.printf("Enter the account ID (e.g., 12345): ");
            accountId = reader.readLine().trim();
        }

        System.out.println("Initializing...");

        // Set up properties for Solace connection
        Properties solaceProperties = new Properties();
        solaceProperties.setProperty(TransportLayerProperties.HOST, host);
        solaceProperties.setProperty(ServiceProperties.VPN_NAME, vpn);
        solaceProperties.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME, username);
        solaceProperties.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD, password);
        solaceProperties.setProperty(ServiceProperties.RECEIVER_DIRECT_SUBSCRIPTION_REAPPLY, "true");

        // Connect to Solace broker
        final MessagingService messagingService = MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(solaceProperties).build().connect();

        // Start the publisher
        DirectMessagePublisher publisher = messagingService.createDirectMessagePublisherBuilder()
                .onBackPressureWait(1)
                .build()
                .start();

        // === Direct message receiver with wildcard subscription banking/> ===
        final DirectMessageReceiver receiver = messagingService.createDirectMessageReceiverBuilder()
                .withSubscriptions(TopicSubscription.of("banking/>"))
                .build()
                .start();

        final MessageHandler messageHandler = (inboundMessage) -> {
            // Save the Message ID for potential replay (fetching it from message properties)
            String messageId = getMessageId(inboundMessage);
            lastReceivedMessageId.set(messageId);  // Store Message ID for replay
            System.out.printf("=== RECEIVED A MESSAGE ===%nMessage ID: %s%nPayload: %s%n===%n", messageId, inboundMessage.dump());
        };
        receiver.receiveAsync(messageHandler);

        // Ask user if they want to replay the last message
        Thread replayThread = new Thread(() -> {
            try {
                while (!isShutdown) {
                    System.out.printf("Enter 'r' to replay the last received message or 'q' to quit: ");
                    String input = reader.readLine().trim();
                    if ("r".equalsIgnoreCase(input)) {
                        replayLastMessage(publisher, messagingService);  // Replay the last message
                    } else if ("q".equalsIgnoreCase(input)) {
                        isShutdown = true;
                    }
                }
            } catch (IOException e) {
                System.out.println("Error while reading input.");
            }
        });
        replayThread.start();

        System.out.printf("%nConnected and subscribed to [banking/>]. Press [ENTER] to quit.%n");

        // Get the messages from the JSON file
        List<String> messages = getActionsFromFile(accountId);

        // Publish each action
        for (String messageContent : messages) {
            try {
                // Construct the outbound message
                OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();
                OutboundMessage message = messageBuilder.build(messageContent);

                // Publish to the dynamic topic directly from the JSON
                String topic = messageContent.split(":")[0].trim();  // Extract topic from the formatted message
                publisher.publish(message, Topic.of(topic));

                // Sleep for a short duration before publishing the next message
                Thread.sleep(1000);

            } catch (RuntimeException e) {
                System.out.printf("### Exception caught during send(): %s%n", e);
                isShutdown = true;
            } catch (InterruptedException e) {
                // Do nothing
            }
        }

        // Shutdown
        isShutdown = true;
        publisher.terminate(500);
        receiver.terminate(500);
        messagingService.disconnect();
        System.out.println("Main thread quitting.");
    }

    // Load configuration from a properties file
    public static Properties loadConfig(String fileName) {
        Properties properties = new Properties();
        try (InputStream input = HelloWorld.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                System.out.println("Unable to find the file: " + fileName);
                return properties;  // Return empty properties if file is not found
            }
            // Load properties from the resource file
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return properties;
    }

    // Method to read JSON file and extract actions
    public static List<String> getActionsFromFile(String accountId) {
        List<String> messages = new ArrayList<>();

        try {
            // Get the input stream for the resource file
            InputStream inputStream = HelloWorld.class.getClassLoader().getResourceAsStream("payloads.json");

            // Check if the input stream is null (file not found)
            if (inputStream == null) {
                System.out.println("File not found!");
                return messages;
            }

            // Create an ObjectMapper instance for reading JSON
            ObjectMapper objectMapper = new ObjectMapper();

            // Read the JSON file into a JsonNode
            JsonNode rootNode = objectMapper.readTree(inputStream);
            JsonNode actionsNode = rootNode.get("actions");

            // Iterate through the actions and generate messages
            for (JsonNode actionNode : actionsNode) {
                String topic = actionNode.get("topic").asText();
                String messageTemplate = actionNode.get("message").asText();

                // Format the message with the provided accountId
                String formattedMessage = String.format(messageTemplate, accountId);
                messages.add(topic + ":" + formattedMessage);  // Adding the full topic with message
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return messages;
    }

    // Method to replay the last received message
    private static void replayLastMessage(DirectMessagePublisher publisher, MessagingService messagingService) {
        String messageId = lastReceivedMessageId.get();
        if (messageId.isEmpty()) {
            System.out.println("No message has been received yet to replay.");
            return;
        }

        // Construct a message based on the last received message's ID
        String messageContent = String.format("Replaying message with ID: %s", messageId);
        OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();
        OutboundMessage message = messageBuilder.build(messageContent);

        // Publish the replayed message to a static topic (could be customized)
        String replayTopic = "banking/>";
        publisher.publish(message, Topic.of(replayTopic));

        System.out.printf("Message with ID %s has been replayed on topic: %s%n", messageId, replayTopic);
    }

    // Helper method to get the message ID from the inbound message properties
    private static String getMessageId(InboundMessage inboundMessage) {
        // Print the entire message dump for debugging purposes
        System.out.println("Message Dump: " + inboundMessage.dump());

        // Solace internal message ID is in the message properties (solace.message.id)
        String messageId = inboundMessage.getProperties().get("solace.message.id");

        if (messageId == null) {
            // If no Solace message ID found, return "Unknown"
            System.out.println("No Solace internal message ID found.");
            messageId = "Unknown";
        } else {
            // Print the internal message ID for debugging
            System.out.println("Solace Internal Message ID: " + messageId);
        }

        return messageId;
    }
}
