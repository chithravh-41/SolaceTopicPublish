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
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String customerId = "";
        while (customerId.isEmpty()) {
            System.out.print("Enter the customer ID (e.g., CUST1001): ");
            customerId = reader.readLine().trim();
        }
 
        // Solace connection details
        String host = "*";
        String vpn = "*";
        String username = "*";
        String password = "*";
 
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
 
        List<JsonNode> events = getEventsFromFile(customerId);
        Map<String, Integer> messageCounts = new HashMap<>();
        messageCounts.put("success", 0);
        messageCounts.put("failure", 0);
 
        System.out.println("\nProcessing events:\n");
        for (JsonNode event : events) {
            if (!event.get("customerId").asText().equals(customerId)) {
                continue;
            }
 
            String eventType = event.get("eventType").asText();
            String region = event.get("region").asText();
            String topicName = buildTopicName(event);
            String messageContent = event.toString();
            String messageId = UUID.randomUUID().toString();
 
            OutboundMessage outboundMessage = messagingService.messageBuilder()
                    .withProperty("messageId", messageId)
                    .build(messageContent);
 
            publisher.publish(outboundMessage, Topic.of(topicName));
 
            messageCounts.put(eventType, messageCounts.get(eventType) + 1);
            System.out.printf("Published to Topic: %s | Message: %s | Message ID: %s%n",
                    topicName, messageContent, messageId);
 
            Thread.sleep(500);
        }
 
        System.out.println("\nSummary:");
        System.out.printf("Success events published: %d%n", messageCounts.get("success"));
        System.out.printf("Failure events published: %d%n", messageCounts.get("failure"));
 
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
 
    private static List<JsonNode> getEventsFromFile(String customerId) {
        List<JsonNode> events = new ArrayList<>();
        try {
            // Try loading from resources first
            InputStream inputStream = EventRouter.class.getClassLoader().getResourceAsStream("payloads.json");
            
            // If not found in resources, try file system
            if (inputStream == null) {
                File file = new File("payloads.json");
                if (file.exists()) {
                    inputStream = new FileInputStream(file);
                } else {
                    System.out.println("payloads.json not found in either classpath or current directory");
                    return events;
                }
            }
 
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(inputStream);
            JsonNode eventsNode = rootNode.get("actions");
 
            for (JsonNode eventNode : eventsNode) {
                if (eventNode.get("customerId").asText().equals(customerId)) {
                    events.add(eventNode);
                }
            }
            inputStream.close();
        } catch (IOException e) {
            System.err.println("Error loading payloads.json: " + e.getMessage());
        }
        return events;
    }
}