package com.solace.samples.java.snippets;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.ReplayStrategy;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.InboundMessage.ReplicationGroupMessageId;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import java.time.ZonedDateTime;

public class HowToTriggerReplayAndConsumePersistentMessage {

  /**
   * Trigger replay of all available messages
   */
  public static void requestReplayOfAllAvailableMessages(MessagingService service, Queue queueToConsumeFrom) {
    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageReplay(ReplayStrategy.allMessages())
        .build(queueToConsumeFrom)
        .start();

    final InboundMessage message = receiver.receiveMessage();
    if (message != null) {
      // Process the received message here
      System.out.printf("Replay received message: %s%n", message.dump());
    } else {
      System.out.println("No messages received during replay.");
    }

    receiver.terminate(500);  // Close receiver after consuming
  }

  /**
   * Trigger replay from a specific date
   */
  public static void requestReplayFromDate(MessagingService service, Queue queueToConsumeFrom, ZonedDateTime dateReplayFrom) {
    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageReplay(ReplayStrategy.timeBased(dateReplayFrom))
        .build(queueToConsumeFrom)
        .start();

    final InboundMessage message = receiver.receiveMessage();
    if (message != null) {
      // Process the received message here
      System.out.printf("Replay received message: %s%n", message.dump());
    } else {
      System.out.println("No messages received during replay.");
    }

    receiver.terminate(500);  // Close receiver after consuming
  }

  /**
   * Extract Replication Group Message Id string from a received message
   */
  public static String getReplicationGroupMessageIdStringFromInboundMessage(InboundMessage previouslyReceivedMessage) {
    final ReplicationGroupMessageId originalReplicationGroupMessageId = previouslyReceivedMessage.getReplicationGroupMessageId();
    return originalReplicationGroupMessageId.toString();
  }

  /**
   * Trigger replay from a Replication Group Message Id string
   */
  public static void requestReplayFromReplicationGroupMessageIdAsString(MessagingService service, Queue queueToConsumeFrom, String replicationGroupMessageIdToString) {
    final ReplicationGroupMessageId restoredReplicationGroupMessageId = ReplicationGroupMessageId.of(replicationGroupMessageIdToString);

    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageReplay(ReplayStrategy.replicationGroupMessageIdBased(restoredReplicationGroupMessageId))
        .build(queueToConsumeFrom)
        .start();

    final InboundMessage message = receiver.receiveMessage();
    if (message != null) {
      // Process the received message here
      System.out.printf("Replay received message: %s%n", message.dump());
    } else {
      System.out.println("No messages received during replay.");
    }

    receiver.terminate(500);  // Close receiver after consuming
  }
}
