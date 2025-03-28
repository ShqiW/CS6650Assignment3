package upic.consumer.messaging;

import com.google.gson.Gson;
import com.rabbitmq.client.*;
// import upic.consumer.CircuitBreaker;
import upic.consumer.config.CircuitBreakerConfig;
import upic.consumer.config.RabbitMQConfig;
import upic.consumer.repository.ResortRepository;
import upic.consumer.repository.SkierRepository;
import upic.consumer.repository.impl.RedisResortRepository;
import upic.consumer.repository.impl.RedisSkierRepository;
import upic.consumer.model.LiftRideEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * RabbitMQ consumer implementation with dynamic thread scaling, performance
 * monitoring,
 * circuit breaker pattern for resilience, and Redis persistence.
 */
public class RabbitMQConsumer {
    // Configuration parameters
    private final int initialThreads;
    private final int maxThreads;
    private final String host;
    private final int port;
    private final String virtualHost;
    private final String username;
    private final String password;
    private final String queueName;
    private final int prefetchCount;
    private final boolean autoAck;

    // Services and utilities
    private final ExecutorService consumerExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private final Connection connection;
    private final Gson gson;

    // Circuit breakers
    // private final CircuitBreaker consumeMessageCircuitBreaker;
    // private final CircuitBreaker processMessageCircuitBreaker;

    // Data storage for skier events - thread-safe hash map
    private final ConcurrentHashMap<Integer, List<LiftRideEvent>> skierData = new ConcurrentHashMap<>();

    // Performance tracking
    // private final LongAdder messagesProcessed = new LongAdder();
    // private final LongAdder messagesPerSecond = new LongAdder();
    // private final LongAdder processingErrors = new LongAdder();
    // private final LongAdder circuitBreakerTrips = new LongAdder();
    // private final LongAdder circuitBreakerSuccessfulRetries = new LongAdder();
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger activeConsumers = new AtomicInteger(0);

    // Control flags
    private volatile boolean running = true;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean queueDeclared = new AtomicBoolean(false);

    // List of created channels for cleanup
    private final List<Channel> channels = Collections.synchronizedList(new ArrayList<>());

    // Database repository
    private final SkierRepository skierRepository;
    private final ResortRepository resortRepository;
    private final LongAdder databaseOperations = new LongAdder();
    private final LongAdder databaseErrors = new LongAdder();

    // Queue to store delivery tags and channel references for acknowledgment
    private final BlockingQueue<AckInfo> ackQueue = new LinkedBlockingQueue<>();

    /**
     * Constructor using default configuration.
     */
    public RabbitMQConsumer() throws Exception {
        this(
                RabbitMQConfig.CONSUMER_THREAD_COUNT,
                RabbitMQConfig.MAX_CONSUMER_THREAD_COUNT,
                RabbitMQConfig.HOST,
                RabbitMQConfig.PORT,
                RabbitMQConfig.VIRTUAL_HOST,
                RabbitMQConfig.USERNAME,
                RabbitMQConfig.PASSWORD,
                RabbitMQConfig.QUEUE_NAME,
                RabbitMQConfig.PREFETCH_COUNT,
                RabbitMQConfig.AUTO_ACK);
    }

    /**
     * Full constructor with all parameters.
     */
    public RabbitMQConsumer(int initialThreads, int maxThreads, String host, int port,
            String virtualHost, String username, String password,
            String queueName, int prefetchCount, boolean autoAck) throws Exception {
        this.initialThreads = initialThreads;
        this.maxThreads = maxThreads;
        this.host = host;
        this.port = port;
        this.virtualHost = virtualHost;
        this.username = username;
        this.password = password;
        this.queueName = queueName;
        this.prefetchCount = prefetchCount;
        this.autoAck = autoAck;
        this.skierRepository = new RedisSkierRepository();
        this.resortRepository = new RedisResortRepository();

        // Create thread pools with custom thread factory for better thread naming
        this.consumerExecutor = Executors.newFixedThreadPool(maxThreads, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("consumer-" + counter.incrementAndGet());
                return thread;
            }
        });

        this.scheduledExecutor = Executors.newScheduledThreadPool(2);

        // Setup RabbitMQ connection with improved error handling
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setVirtualHost(virtualHost);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setConnectionTimeout(RabbitMQConfig.CONNECTION_TIMEOUT);
            factory.setRequestedHeartbeat(RabbitMQConfig.HEARTBEAT_TIMEOUT);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setNetworkRecoveryInterval(RabbitMQConfig.NETWORK_RECOVERY_INTERVAL);

            System.out.println("Attempting to connect to RabbitMQ: " + host + ":" + port +
                    " username: " + username + " vhost: " + virtualHost);

            this.connection = factory.newConnection();

            System.out.println("Successfully connected to RabbitMQ! Connection ID: " + connection.getId());
        } catch (Exception e) {
            System.err.println("!!!CRITICAL ERROR!!! Failed to connect to RabbitMQ server: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // Setup Gson for JSON parsing
        this.gson = new Gson();

        // Initialize circuit breakers
        // CircuitBreakerConfig consumeConfig = new CircuitBreakerConfig(
        // "RabbitMQ-Consume",
        // 5, // Failure threshold
        // 10000 // Reset timeout in ms
        // );
        // this.consumeMessageCircuitBreaker = new CircuitBreaker(consumeConfig,
        // circuitBreakerTrips,
        // circuitBreakerSuccessfulRetries);

        // CircuitBreakerConfig processConfig = new CircuitBreakerConfig(
        // "Message-Processing",
        // 50, // Failure threshold - increased from 5 to 50
        // 5000 // Reset timeout in ms - decreased from 10000 to 5000
        // );
        // this.processMessageCircuitBreaker = new CircuitBreaker(processConfig,
        // circuitBreakerTrips,
        // circuitBreakerSuccessfulRetries);
    }

    /**
     * Declare the queue if it doesn't exist.
     */
    private void declareQueue(Channel channel) throws IOException {
        if (!queueDeclared.getAndSet(true)) {
            System.out.println("Attempting to declare queue: " + queueName);
            System.out.println("Queue parameters - durable: " + RabbitMQConfig.QUEUE_DURABLE +
                    ", exclusive: " + RabbitMQConfig.QUEUE_EXCLUSIVE +
                    ", autoDelete: " + RabbitMQConfig.QUEUE_AUTO_DELETE);

            AMQP.Queue.DeclareOk declareOk = channel.queueDeclare(
                    queueName,
                    RabbitMQConfig.QUEUE_DURABLE,
                    RabbitMQConfig.QUEUE_EXCLUSIVE,
                    RabbitMQConfig.QUEUE_AUTO_DELETE,
                    null);

            System.out.println("Queue declared successfully: " + queueName);
            System.out.println("Queue details - message count: " + declareOk.getMessageCount() +
                    ", consumer count: " + declareOk.getConsumerCount());
        }
    }

    /**
     * Start the consumer with initial threads.
     */
    public void start() {
        System.out.println("====================================================");
        System.out.println("Starting RabbitMQ Consumer with " + initialThreads + " initial threads");
        System.out.println("Configuration:");
        System.out.println(" - Queue Name: " + queueName);
        System.out.println(" - Host: " + host + ":" + port);
        System.out.println(" - Virtual Host: " + virtualHost);
        System.out.println(" - Initial Threads: " + initialThreads);
        System.out.println(" - Max Threads: " + maxThreads);
        System.out.println(" - Prefetch Count: " + prefetchCount);
        System.out.println(" - Auto Ack: " + autoAck);
        System.out.println("====================================================");

        // Start background monitoring tasks
        startStatsReporting();
        startQueueMonitoring();
        // startCircuitBreakerMonitoring();

        // Start the acknowledgment thread
        startAckThread();

        // Start consumer threads
        for (int i = 0; i < initialThreads; i++) {
            startConsumerThread(i);
        }
    }

    /**
     * Start a single consumer thread.
     */
    private void startConsumerThread(int threadId) {
        consumerExecutor.submit(() -> {
            System.out.println("Consumer thread #" + threadId + " started");
            activeThreads.incrementAndGet();

            try {
                // Create a channel for this thread
                Channel channel = connection.createChannel();
                channels.add(channel);

                // Declare queue and set QoS
                declareQueue(channel);
                channel.basicQos(prefetchCount);

                // Start consuming
                startConsumer(channel, threadId);
            } catch (Exception e) {
                System.err.println(
                        "!!!CRITICAL ERROR!!! Consumer thread #" + threadId + " terminated: " + e.getMessage());
                e.printStackTrace();
            } finally {
                activeThreads.decrementAndGet();
                System.out.println("Consumer thread #" + threadId + " is exiting!");
            }
        });
    }

    /**
     * Start a thread to acknowledge processed messages.
     */
    private void startAckThread() {
        Thread ackThread = new Thread(() -> {
            while (running) {
                try {
                    // Take an acknowledgment info object from the queue
                    AckInfo ackInfo = ackQueue.take();

                    // Acknowledge the message
                    ackInfo.getChannel().basicAck(ackInfo.getDeliveryTag(), false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Ack thread interrupted: " + e.getMessage());
                } catch (IOException e) {
                    System.err.println("Error acknowledging message: " + e.getMessage());
                }
            }
        });

        ackThread.setName("ack-thread");
        ackThread.setDaemon(true);
        ackThread.start();
    }

    /**
     * Start a consumer on the given channel.
     */
    private void startConsumer(Channel channel, int threadId) {
        try {
            // Create a consumer that processes messages
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                    // // Process message with circuit breaker protection
                    // boolean processed = processMessageCircuitBreaker.execute(() -> {
                    // // Parse JSON message body
                    // LiftRideEvent liftRideEvent = new LiftRideEvent(message);

                    // // Process message and update skier data
                    // processMessage(liftRideEvent);

                    // // Update statistics
                    // messagesProcessed.increment();
                    // messagesPerSecond.increment();

                    // return true;
                    // });
                    Boolean processed = true;
                    LiftRideEvent liftRideEvent = new LiftRideEvent(message);

                    // Process message and update skier data
                    processMessage(liftRideEvent);

                    // Add the delivery tag and channel to the acknowledgment queue
                    ackQueue.put(new AckInfo(channel, delivery.getEnvelope().getDeliveryTag()));

                    // Only acknowledge if processing was successful
                    // if (processed && !autoAck) {
                    // channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // } else if (!processed && !autoAck) {
                    // // Negative acknowledgment, requeue the message
                    // channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    // }
                } catch (Exception e) {
                    // processingErrors.increment();
                    System.err.println("Thread #" + threadId + " error processing message: " + e.getMessage());

                    if (!autoAck) {
                        // Negative acknowledgment with requeue on exception
                        try {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                        } catch (IOException ackError) {
                            System.err.println("Failed to send NACK after error: " + ackError.getMessage());
                        }
                    }
                }
            };

            // Cancel callback
            CancelCallback cancelCallback = consumerTag -> System.out.println("Consumer " + consumerTag + " cancelled");

            // Start consuming with additional logging
            System.out.println("Thread #" + threadId + " declaring consuming from queue: " + queueName);
            System.out.println("Thread #" + threadId + " prefetch count: " + prefetchCount);
            System.out.println("Thread #" + threadId + " autoAck: " + autoAck);

            String consumerTag = "consumer-" + threadId + "-" + System.currentTimeMillis();

            // Start consuming and get actual tag
            String actualTag = channel.basicConsume(queueName, autoAck, consumerTag, deliverCallback, cancelCallback);
            activeConsumers.incrementAndGet();

            System.out.println("Consumer registered successfully with tag: " + actualTag);

            // Keep thread alive until shutdown
            while (running && !Thread.currentThread().isInterrupted()) {
                Thread.sleep(1000);
            }

            // Cancel the consumer on shutdown
            channel.basicCancel(actualTag);
            activeConsumers.decrementAndGet();

        } catch (Exception e) {
            System.err.println("Thread #" + threadId + " error in consumer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void startQueueMonitoring() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                // Create temporary channel to get queue information
                Channel monitorChannel = connection.createChannel();

                // Get queue status and depth
                AMQP.Queue.DeclareOk queueInfo = monitorChannel.queueDeclarePassive(queueName);
                int messagesInQueue = queueInfo.getMessageCount();

                System.out.println("\n=== Queue Status - " +
                        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME) + " ===");
                System.out.println("Current queue depth: " + messagesInQueue + " messages");
                System.out.println("Active consumer threads: " + activeThreads.get());
                System.out.println("Active consumers: " + activeConsumers.get());

                // Adjust thread count based on queue depth
                adjustThreadCount(messagesInQueue);

                // Close monitoring channel
                monitorChannel.close();

            } catch (Exception e) {
                System.err.println("Error monitoring queue: " + e.getMessage());
            }
        }, 5000, 5000, TimeUnit.MILLISECONDS);
    }

    private void startCircuitBreakerMonitoring() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                System.out.println("\n=== Circuit Breaker Status - " +
                        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME) + " ===");

                // // Report message consumption circuit breaker status
                // System.out.println("Consume Message Circuit: " +
                // consumeMessageCircuitBreaker.getState() +
                // " (Failures: " + consumeMessageCircuitBreaker.getFailureCount() +
                // ", Successes: " + consumeMessageCircuitBreaker.getSuccessCount() + ")");

                // // Report message processing circuit breaker status
                // System.out.println("Process Message Circuit: " +
                // processMessageCircuitBreaker.getState() +
                // " (Failures: " + processMessageCircuitBreaker.getFailureCount() +
                // ", Successes: " + processMessageCircuitBreaker.getSuccessCount() + ")");

                // // Report overall statistics
                // System.out.println("Total circuit breaker trips: " +
                // circuitBreakerTrips.sum());
                // System.out.println("Successful retries: " +
                // circuitBreakerSuccessfulRetries.sum());

            } catch (Exception e) {
                System.err.println("Error monitoring circuit breakers: " + e.getMessage());
            }
        }, 10000, 10000, TimeUnit.MILLISECONDS);
    }

    /**
     * Dynamically adjust thread count based on queue depth.
     */
    private void adjustThreadCount(int messagesInQueue) {
        int currentThreads = activeThreads.get();

        // More aggressive thread scaling logic
        if (messagesInQueue > 100 && currentThreads < maxThreads) {
            // Calculate threads to add based on queue depth
            int threadsToAdd = Math.min(
                    Math.max(5, messagesInQueue / 1000), // Based on queue depth
                    maxThreads - currentThreads);
            System.out.println(
                    "Adding " + threadsToAdd + " consumer threads due to high queue depth: " + messagesInQueue);

            for (int i = 0; i < threadsToAdd; i++) {
                startConsumerThread(-1); // -1 indicates a dynamically added thread
            }
        } else if (messagesInQueue < RabbitMQConfig.LOW_QUEUE_THRESHOLD && currentThreads > initialThreads) {
            // Queue is nearly empty, we can reduce threads
            // This is handled passively - we don't forcibly stop threads, but just note
            // that we could
            // use fewer threads. This avoids interrupting work in progress.
            System.out.println("Queue depth is low. Could reduce by " +
                    Math.min(RabbitMQConfig.THREADS_TO_REMOVE, currentThreads - initialThreads) +
                    " threads when current work completes.");
        }
    }

    /**
     * Process a message with circuit breaker protection.
     */
    // private boolean processMessageWithCircuitBreaker(String messageBody) {
    // try {
    // processMessage(new LiftRideEvent(messageBody));
    // // return true;
    // // return processMessageCircuitBreaker.execute(() -> {

    // // // Process message and update skier data
    // // processMessage(new LiftRideEvent(messageBody));

    // // // Update statistics
    // // messagesProcessed.increment();
    // // messagesPerSecond.increment();

    // // return true;
    // // });
    // } catch (Exception e) {
    // // processingErrors.increment();
    // System.err.println("Error processing message: " + e.getMessage());
    // return false;
    // }
    // }

    /**
     * Add data to database
     */
    private void processMessage(LiftRideEvent event) {
        int skierId = event.getSkierId();

        try {
            skierRepository.recordLiftRide(
                    event.getSkierId(),
                    event.getResortId(),
                    event.getLiftId(),
                    event.getSeasonId(),
                    event.getDayId(),
                    event.getTime());

            resortRepository.recordSkierVisit(
                    event.getResortId(),
                    event.getSkierId(),
                    event.getSeasonId(),
                    event.getDayId());

            databaseOperations.increment();

        } catch (Exception e) {
            databaseErrors.increment();
            System.err.println("Error storing data in Redis: " + e.getMessage());
        }

        skierData.computeIfAbsent(skierId, k -> Collections.synchronizedList(new ArrayList<>())).add(event);

        // Add logging for 1% of messages
        if (Math.random() < 0.01) {
            System.out.println("Processing message for skierId: " + skierId);
        }
    }

    /**
     * Start periodic statistics reporting.
     */
    private void startStatsReporting() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                // long processed = messagesProcessed.sum();
                // long processedPerSecond = messagesPerSecond.sumThenReset();
                // long errors = processingErrors.sum();
                int numSkiers = skierData.size();
                int active = activeThreads.get();
                int consumers = activeConsumers.get();
                long dbOps = databaseOperations.sum();
                long dbErrors = databaseErrors.sum();

                System.out.println("\n=== Performance Statistics - " +
                        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME) + " ===");
                System.out.println("Active consumer threads: " + active + " (consumers: " + consumers + ")");
                // System.out.println("Messages processed per second: " + processedPerSecond);
                // System.out.println("Total messages processed: " + processed);
                // System.out.println("Processing errors: " + errors);
                System.out.println("Distinct skiers tracked: " + numSkiers);
                System.out.println("Database operations: " + dbOps);
                System.out.println("Database errors: " + dbErrors);

                // Memory usage
                Runtime runtime = Runtime.getRuntime();
                long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
                long totalMemory = runtime.totalMemory() / (1024 * 1024);
                long maxMemory = runtime.maxMemory() / (1024 * 1024);

                System.out.println("Memory usage: " + usedMemory + "MB / " + totalMemory +
                        "MB (Max: " + maxMemory + "MB)");

                System.out.println("=====================================");
            } catch (Exception e) {
                System.err.println("Error generating statistics: " + e.getMessage());
            }
        }, RabbitMQConfig.STATS_INTERVAL_MS, RabbitMQConfig.STATS_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown the consumer gracefully.
     */
    public void shutdown() {
        System.out.println("Shutting down RabbitMQ Consumer...");
        running = false;

        // Close all channels
        for (Channel channel : channels) {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (Exception e) {
                System.err.println("Error closing channel: " + e.getMessage());
            }
        }

        // Close connection
        try {
            if (connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }

        // Shutdown executors
        scheduledExecutor.shutdown();
        consumerExecutor.shutdown();

        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
            if (!consumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                consumerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            consumerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // System.out.println("RabbitMQ Consumer shutdown complete. Processed " +
        // messagesProcessed.sum() + " messages.");
        shutdownLatch.countDown();
    }

    /**
     * Wait for shutdown to complete.
     */
    public void waitForShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    // /**
    // * Get the number of messages processed.
    // */
    // public long getMessagesProcessed() {
    // return messagesProcessed.sum();
    // }

    // /**
    // * Get the number of processing errors.
    // */
    // public long getProcessingErrors() {
    // return processingErrors.sum();
    // }

    /**
     * Get the current active thread count.
     */
    public int getActiveThreads() {
        return activeThreads.get();
    }

    /**
     * Main method for running the consumer as a standalone application.
     */
    public static void main(String[] args) {
        try {
            System.out.println("RabbitMQ Consumer application starting...");

            // Create and start the consumer
            RabbitMQConsumer consumer = new RabbitMQConsumer();
            consumer.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

            // Wait for shutdown signal
            consumer.waitForShutdown();
        } catch (Exception e) {
            System.err.println("Error running consumer: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Helper class to store acknowledgment information
    private static class AckInfo {
        private final Channel channel;
        private final long deliveryTag;

        public AckInfo(Channel channel, long deliveryTag) {
            this.channel = channel;
            this.deliveryTag = deliveryTag;
        }

        public Channel getChannel() {
            return channel;
        }

        public long getDeliveryTag() {
            return deliveryTag;
        }
    }
}