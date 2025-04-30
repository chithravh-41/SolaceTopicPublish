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

public class HelloWorld {

    private static volatile boolean isShutdown = false;

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

        // Get the actions (including topics) from the JSON file
        List<JsonNode> actions = getActionsFromFile(accountId);

        // Subscribe to each topic dynamically
        for (JsonNode action : actions) {
            String topic = String.format(action.get("topic").asText(), accountId);
            final DirectMessageReceiver receiver = messagingService.createDirectMessageReceiverBuilder()
                    .withSubscriptions(TopicSubscription.of(topic))
                    .build()
                    .start();

            final MessageHandler messageHandler = (inboundMessage) -> {
                // Handle the incoming message
                System.out.printf("=== RECEIVED A MESSAGE ===%nPayload: %s%n===%n", inboundMessage.dump());
            };

            receiver.receiveAsync(messageHandler);
        }

        System.out.printf("%nConnected and subscribed to individual topics for account %s. Press [ENTER] to quit.%n", accountId);

        // Publish each action to its specific topic
        for (JsonNode action : actions) {
            String topic = String.format(action.get("topic").asText(), accountId);
            String messageContent = String.format(action.get("message").asText(), accountId);

            try {
                // Construct the outbound message
                OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();
                OutboundMessage message = messageBuilder.build(messageContent);

                // Publish to the specific topic directly from the JSON
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
    public static List<JsonNode> getActionsFromFile(String accountId) {
        List<JsonNode> actions = new ArrayList<>();

        try {
            // Get the input stream for the resource file
            InputStream inputStream = HelloWorld.class.getClassLoader().getResourceAsStream("payloads.json");

            // Check if the input stream is null (file not found)
            if (inputStream == null) {
                System.out.println("File not found!");
                return actions;
            }

            // Create an ObjectMapper instance for reading JSON
            ObjectMapper objectMapper = new ObjectMapper();

            // Read the JSON file into a JsonNode
            JsonNode rootNode = objectMapper.readTree(inputStream);
            JsonNode actionsNode = rootNode.get("actions");

            // Iterate through the actions and add them to the list
            for (JsonNode actionNode : actionsNode) {
                actions.add(actionNode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return actions;
    }
}
