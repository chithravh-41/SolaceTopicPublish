package com.solace.samples.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

public class HelloWorld {

    private static final String SAMPLE_NAME = HelloWorld.class.getSimpleName();
    private static final String TOPIC_PREFIX = "banking/accounts/";
    private static final String API = "Java";
    private static volatile boolean isShutdown = false;

    // AtomicReference to store the last received message's ID (for replay)
    private static AtomicReference<String> lastReceivedMessageId = new AtomicReference<>("");

    public static void main(String... args) throws IOException {

        // Hardcoded Solace connection details
        String host = "tcps://mr-connection-6ngc89ky4et.messaging.solace.cloud:55443";
        String vpn = "solacesample";
        String username = "solace-cloud-client";
        String password = "n97dj1rfu968ss09ovkte7qmo4";

        // Ask for account ID
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String accountId = "";
        while (accountId.isEmpty()) {
            System.out.printf("Enter the account ID (e.g., 12345): ");
            accountId = reader.readLine().trim();
        }

        System.out.println(API + " " + SAMPLE_NAME + " initializing...");
        final Properties properties = new Properties();
        properties.setProperty(TransportLayerProperties.HOST, host);
        properties.setProperty(ServiceProperties.VPN_NAME, vpn);
        properties.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME, username);
        properties.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD, password);
        properties.setProperty(ServiceProperties.RECEIVER_DIRECT_SUBSCRIPTION_REAPPLY, "true");

        // Connect to Solace broker
        final MessagingService messagingService = MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(properties)
                .build()
                .connect();

        // Start the publisher
        final DirectMessagePublisher publisher = messagingService.createDirectMessagePublisherBuilder()
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

        OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();

        String[] actions = {
            "create",
            "update",
            "balance/credit",
            "balance/debit",
            "status/active",
            "status/closed"
        };

        int index = 0;
        while (System.in.available() == 0 && !isShutdown) {
            try {
                Thread.sleep(3000);

                String action = actions[index % actions.length];
                String messageContent = String.format("Account ID %s - Action: %s", accountId, action);
                String dynamicTopic = TOPIC_PREFIX + accountId + "/" + action;

                System.out.printf(">> Publishing to topic: %s%n", dynamicTopic);
                OutboundMessage message = messageBuilder.build(messageContent);
                publisher.publish(message, Topic.of(dynamicTopic));

                index++;

            } catch (RuntimeException e) {
                System.out.printf("### Exception caught during send(): %s%n", e);
                isShutdown = true;
            } catch (InterruptedException e) {
                // Do nothing
            }
        }

        isShutdown = true;
        publisher.terminate(500);
        receiver.terminate(500);
        messagingService.disconnect();
        System.out.println("Main thread quitting.");
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
        // Get message properties as a Map
        Map<String, String> properties = inboundMessage.getProperties();
        
        // Return the Message ID from the properties map
        return properties.getOrDefault("solace.message.id", "Unknown");
    }
}
