package upic.server.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.MessageProperties;
import upic.server.config.RabbitMQConfig;
import upic.server.config.ServerConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Publisher for sending messages to RabbitMQ.
 * Supports both individual and batch message publishing with async confirmation.
 */
public class RabbitMQPublisher {
    private static final Logger LOGGER = Logger.getLogger(RabbitMQPublisher.class.getName());

    private final RabbitMQChannelPool channelPool;
    private final String queueName;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutor;

    // Message batching
    private final ConcurrentLinkedQueue<BatchItem> messageBatch = new ConcurrentLinkedQueue<>();
    private static final int BATCH_SIZE = ServerConfig.BATCH_SIZE;
    private static final long BATCH_FLUSH_INTERVAL_MS = ServerConfig.BATCH_FLUSH_INTERVAL_MS;

    // Performance metrics
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder publishErrors = new LongAdder();

    // Async confirmation tracking
    private final ConcurrentMap<Long, BatchItem> unconfirmedMessages = new ConcurrentHashMap<>();
    private final LongAdder confirmedMessages = new LongAdder();
    private final LongAdder nackMessages = new LongAdder();

    /**
     * Creates a new RabbitMQ publisher.
     * @param channelPoolSize The size of the channel pool
     * @throws Exception If initialization fails
     */
    public RabbitMQPublisher(int channelPoolSize) throws Exception {
        this.queueName = RabbitMQConfig.getQueueName();
        this.channelPool = new RabbitMQChannelPool(channelPoolSize);

        // Create thread pools for async operations
        this.executorService = Executors.newFixedThreadPool(
                ServerConfig.getOptimalThreadCount() / 4 // Use a fraction of server threads
        );
        this.scheduledExecutor = Executors.newScheduledThreadPool(2);

        // Schedule batch message processor
        scheduledExecutor.scheduleAtFixedRate(
                this::flushMessageBatch,
                BATCH_FLUSH_INTERVAL_MS,
                BATCH_FLUSH_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        // Schedule confirmation status reporter
        scheduledExecutor.scheduleAtFixedRate(
                this::reportConfirmationStatus,
                10000,
                10000,
                TimeUnit.MILLISECONDS
        );

        LOGGER.info("RabbitMQ publisher initialized with queue: " + queueName +
                ", channel pool size: " + channelPoolSize + ", async confirmation enabled");
    }

    /**
     * Publishes a message to the queue using the batching mechanism.
     * @param messageId A unique ID for the message
     * @param messageBody The message body as a string
     * @return true if the message was successfully added to the batch, false otherwise
     */
    public boolean publishMessage(String messageId, String messageBody) {
        try {
            // Add message to batch
            BatchItem batchItem = new BatchItem(messageId, messageBody);
            boolean added = messageBatch.add(batchItem);

            // If batch size reached, trigger flush in separate thread
            if (messageBatch.size() >= BATCH_SIZE) {
                triggerBatchFlush();
            }

            return added; // Return whether the message was successfully added to the batch
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error adding message to batch", e);
            publishErrors.increment();
            return false;
        }
    }

    /**
     * Publishes a message to the queue immediately (bypassing the batch).
     * @param messageBody The message body as a string
     * @return True if successful, false otherwise
     */
    public boolean publishMessageImmediate(String messageBody) {
        Channel channel = null;
        try {
            channel = channelPool.getChannel();

            // Setup async confirmation callback
            setupConfirmCallback(channel);

            long seqNo = channel.getNextPublishSeqNo();
            channel.basicPublish(
                    "", // Default exchange
                    queueName,
                    MessageProperties.PERSISTENT_TEXT_PLAIN, // Make message persistent
                    messageBody.getBytes()
            );

            // Store message in unconfirmed map for tracking
            unconfirmedMessages.put(seqNo, new BatchItem("immediate-" + seqNo, messageBody));
            messagesSent.increment();
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error publishing immediate message", e);
            publishErrors.increment();
            return false;
        } finally {
            if (channel != null) {
                channelPool.returnChannel(channel);
            }
        }
    }

    /**
     * Triggers an immediate batch flush in a separate thread.
     */
    private void triggerBatchFlush() {
        executorService.submit(this::flushMessageBatch);
    }

    /**
     * Sets up an async confirmation callback on the channel.
     * This ensures messages are properly tracked and confirmed.
     */
    private void setupConfirmCallback(Channel channel) throws IOException {
        channel.confirmSelect(); // Enable publisher confirms

        // Add confirmation listener, only set it once per channel
        if (!channel.getClass().getName().contains("ConfirmListenerProxy")) {
            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) {
                    // If multiple is true, confirm all messages up to deliveryTag
                    if (multiple) {
                        Set<Long> confirmed = unconfirmedMessages.keySet().stream()
                                .filter(tag -> tag <= deliveryTag)
                                .collect(Collectors.toSet());

                        confirmed.forEach(tag -> {
                            unconfirmedMessages.remove(tag);
                            confirmedMessages.increment();
                        });
                    } else {
                        // Just confirm this one message
                        unconfirmedMessages.remove(deliveryTag);
                        confirmedMessages.increment();
                    }
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) {
                    // Handle rejected messages, we can re-publish them
                    if (multiple) {
                        Set<Long> nacked = unconfirmedMessages.keySet().stream()
                                .filter(tag -> tag <= deliveryTag)
                                .collect(Collectors.toSet());

                        nacked.forEach(tag -> {
                            BatchItem item = unconfirmedMessages.remove(tag);
                            if (item != null) {
                                // Add message back to batch queue for retry
                                messageBatch.add(item);
                                nackMessages.increment();
                            }
                        });
                    } else {
                        BatchItem item = unconfirmedMessages.remove(deliveryTag);
                        if (item != null) {
                            messageBatch.add(item);
                            nackMessages.increment();
                        }
                    }
                }
            });
        }
    }

    /**
     * Reports confirmation statistics periodically.
     * Useful for monitoring the publisher's health.
     */
    private void reportConfirmationStatus() {
        long confirmed = confirmedMessages.sum();
        long nacked = nackMessages.sum();
        long pending = unconfirmedMessages.size();

        LOGGER.info("Publisher confirmation status: confirmed=" + confirmed +
                ", nacked=" + nacked + ", pending=" + pending);
    }

    /**
     * Flushes the current batch of messages to RabbitMQ.
     */
    private synchronized void flushMessageBatch() {
        if (messageBatch.isEmpty()) {
            return;
        }

        List<BatchItem> batchToSend = new ArrayList<>(BATCH_SIZE);
        int count = 0;

        // Take up to BATCH_SIZE messages
        while (!messageBatch.isEmpty() && count < BATCH_SIZE) {
            BatchItem item = messageBatch.poll();
            if (item != null) {
                batchToSend.add(item);
                count++;
            }
        }

        if (!batchToSend.isEmpty()) {
            Channel channel = null;
            try {
                channel = channelPool.getChannel();

                // Setup async confirmation for reliable delivery
                setupConfirmCallback(channel);

                for (BatchItem item : batchToSend) {
                    // Get sequence number for tracking this message
                    long nextSeqNo = channel.getNextPublishSeqNo();

                    channel.basicPublish(
                            "", // Default exchange
                            queueName,
                            MessageProperties.PERSISTENT_TEXT_PLAIN, // Make message persistent
                            item.messageBody.getBytes()
                    );

                    // Store message for confirmation tracking
                    unconfirmedMessages.put(nextSeqNo, item);
                }

                // Update metrics
                messagesSent.add(batchToSend.size());

                if (Math.random() < 0.01) { // Log 1% of batch sends
                    LOGGER.info("Sent batch of " + batchToSend.size() + " messages to RabbitMQ");
                }
            } catch (Exception e) {
                // Log error and handle failure
                LOGGER.log(Level.WARNING, "Error sending batch to RabbitMQ", e);
                publishErrors.increment();

                // Put messages back to batch queue for retry
                messageBatch.addAll(batchToSend);
            } finally {
                if (channel != null) {
                    channelPool.returnChannel(channel);
                }
            }
        }
    }

    /**
     * Get the total number of messages sent.
     * @return The count of messages sent
     */
    public long getMessagesSent() {
        return messagesSent.sum();
    }

    /**
     * Get the number of publish errors encountered.
     * @return The count of publish errors
     */
    public long getPublishErrors() {
        return publishErrors.sum();
    }

    /**
     * Get the number of confirmed messages.
     * @return The count of successfully confirmed messages
     */
    public long getConfirmedMessages() {
        return confirmedMessages.sum();
    }

    /**
     * Get the number of nacked messages.
     * @return The count of messages that were rejected by the broker
     */
    public long getNackMessages() {
        return nackMessages.sum();
    }

    /**
     * Get the current count of messages awaiting confirmation.
     * @return The number of pending messages
     */
    public long getPendingConfirmationCount() {
        return unconfirmedMessages.size();
    }

    /**
     * Shutdown the publisher, flushing any pending messages.
     */
    public void shutdown() {
        // Final flush of any messages
        flushMessageBatch();

        // Wait for confirmations to complete (up to 5 seconds)
        long waitStart = System.currentTimeMillis();
        while (!unconfirmedMessages.isEmpty() &&
                System.currentTimeMillis() - waitStart < 5000) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Shutdown thread pools
        scheduledExecutor.shutdown();
        executorService.shutdown();

        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Close the channel pool
        channelPool.close();

        LOGGER.info("RabbitMQ publisher shutdown complete, sent " + messagesSent.sum() +
                " messages, confirmed " + confirmedMessages.sum() +
                " messages, " + unconfirmedMessages.size() + " messages still pending");
    }

    /**
     * Class to hold batch message items
     */
    private static class BatchItem {
        final String id;
        final String messageBody;

        BatchItem(String id, String messageBody) {
            this.id = id;
            this.messageBody = messageBody;
        }
    }
}