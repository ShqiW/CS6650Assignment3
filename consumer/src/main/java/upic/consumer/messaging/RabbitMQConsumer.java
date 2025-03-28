package upic.consumer.messaging;

import com.google.gson.Gson;
import com.rabbitmq.client.*;
import upic.consumer.CircuitBreaker;
import upic.consumer.config.CircuitBreakerConfig;
import upic.consumer.config.RabbitMQConfig;
import upic.consumer.model.LiftRideEvent;
import upic.consumer.repository.ResortRepository;
import upic.consumer.repository.SkierRepository;
import upic.consumer.repository.impl.RedisResortRepository;
import upic.consumer.repository.impl.RedisSkierRepository;

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
 * RabbitMQ consumer implementation with dynamic thread scaling, performance monitoring,
 * circuit breaker pattern for resilience, and Redis persistence.
 */
public class RabbitMQConsumer {
    // Existing member variables
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

    private final ExecutorService consumerExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private final Connection connection;
    private final Gson gson;

    private final CircuitBreaker consumeMessageCircuitBreaker;
    private final CircuitBreaker processMessageCircuitBreaker;

    private final ConcurrentHashMap<Integer, List<LiftRideEvent>> skierData = new ConcurrentHashMap<>();

    private final LongAdder messagesProcessed = new LongAdder();
    private final LongAdder messagesPerSecond = new LongAdder();
    private final LongAdder processingErrors = new LongAdder();
    private final LongAdder circuitBreakerTrips = new LongAdder();
    private final LongAdder circuitBreakerSuccessfulRetries = new LongAdder();
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger activeConsumers = new AtomicInteger(0);

    private volatile boolean running = true;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean queueDeclared = new AtomicBoolean(false);

    private final List<Channel> channels = Collections.synchronizedList(new ArrayList<>());

    private final SkierRepository skierRepository;
    private final ResortRepository resortRepository;
    private final LongAdder databaseOperations = new LongAdder();
    private final LongAdder databaseErrors = new LongAdder();

    // New batch processing members
    private final BlockingQueue<LiftRideEvent> eventBatchQueue = new LinkedBlockingQueue<>(10000);
    private final LongAdder batchesProcessed = new LongAdder();
    private final LongAdder batchProcessTime = new LongAdder();
    private final LongAdder batchSizes = new LongAdder();

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
                RabbitMQConfig.AUTO_ACK
        );
    }

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

        this.gson = new Gson();

        CircuitBreakerConfig consumeConfig = new CircuitBreakerConfig(
                "RabbitMQ-Consume",
                5,
                10000
        );
        this.consumeMessageCircuitBreaker = new CircuitBreaker(consumeConfig, circuitBreakerTrips, circuitBreakerSuccessfulRetries);

        CircuitBreakerConfig processConfig = new CircuitBreakerConfig(
                "Message-Processing",
                50,
                5000
        );
        this.processMessageCircuitBreaker = new CircuitBreaker(processConfig, circuitBreakerTrips, circuitBreakerSuccessfulRetries);
    }

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
                    null
            );

            System.out.println("Queue declared successfully: " + queueName);
            System.out.println("Queue details - message count: " + declareOk.getMessageCount() +
                    ", consumer count: " + declareOk.getConsumerCount());
        }
    }

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
        System.out.println(" - Batch Size: " + RabbitMQConfig.BATCH_SIZE);
        System.out.println(" - Batch Flush Interval: " + RabbitMQConfig.BATCH_FLUSH_INTERVAL_MS + "ms");
        System.out.println("====================================================");

        // Start batch processor
        startBatchProcessor();

        // Start background monitoring tasks
        startStatsReporting();
        startQueueMonitoring();
        startCircuitBreakerMonitoring();

        // Start consumer threads
        for (int i = 0; i < initialThreads; i++) {
            startConsumerThread(i);
        }
    }

    private void startBatchProcessor() {
        scheduledExecutor.scheduleAtFixedRate(
                this::processBatch,
                RabbitMQConfig.BATCH_FLUSH_INTERVAL_MS,
                RabbitMQConfig.BATCH_FLUSH_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        System.out.println("Batch processor started with batch size: " + RabbitMQConfig.BATCH_SIZE +
                " and interval: " + RabbitMQConfig.BATCH_FLUSH_INTERVAL_MS + "ms");
    }

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
                System.err.println("!!!CRITICAL ERROR!!! Consumer thread #" + threadId + " terminated: " + e.getMessage());
                e.printStackTrace();
            } finally {
                activeThreads.decrementAndGet();
                System.out.println("Consumer thread #" + threadId + " is exiting!");
            }
        });
    }

    private void startConsumer(Channel channel, int threadId) {
        try {
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                    boolean processed = processMessageCircuitBreaker.execute(() -> {
                        LiftRideEvent liftRideEvent = gson.fromJson(message, LiftRideEvent.class);

                        // Add to batch queue instead of direct processing
                        boolean added = eventBatchQueue.offer(liftRideEvent);

                        if (!added) {
                            // Process directly if queue is full
                            processMessage(liftRideEvent);
                        }

                        messagesProcessed.increment();
                        messagesPerSecond.increment();

                        return true;
                    });

                    if (processed && !autoAck) {
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } else if (!processed && !autoAck) {
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    }
                } catch (Exception e) {
                    processingErrors.increment();
                    System.err.println("Thread #" + threadId + " error processing message: " + e.getMessage());

                    if (!autoAck) {
                        try {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                        } catch (IOException ackError) {
                            System.err.println("Failed to send NACK after error: " + ackError.getMessage());
                        }
                    }
                }
            };

            CancelCallback cancelCallback = consumerTag ->
                    System.out.println("Consumer " + consumerTag + " cancelled");

            System.out.println("Thread #" + threadId + " declaring consuming from queue: " + queueName);
            System.out.println("Thread #" + threadId + " prefetch count: " + prefetchCount);
            System.out.println("Thread #" + threadId + " autoAck: " + autoAck);

            String consumerTag = "consumer-" + threadId + "-" + System.currentTimeMillis();
            String actualTag = channel.basicConsume(queueName, autoAck, consumerTag, deliverCallback, cancelCallback);
            activeConsumers.incrementAndGet();

            System.out.println("Consumer registered successfully with tag: " + actualTag);

            while (running && !Thread.currentThread().isInterrupted()) {
                Thread.sleep(1000);
            }

            channel.basicCancel(actualTag);
            activeConsumers.decrementAndGet();

        } catch (Exception e) {
            System.err.println("Thread #" + threadId + " error in consumer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processBatch() {
        List<LiftRideEvent> batch = new ArrayList<>(RabbitMQConfig.BATCH_SIZE);
        int drained = eventBatchQueue.drainTo(batch, RabbitMQConfig.BATCH_SIZE);

        if (batch.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            boolean processed = processMessageCircuitBreaker.execute(() -> {
                try {
                    // Use the new buffered implementation
                    ((RedisSkierRepository)skierRepository).recordLiftRideBatch(batch);

                    // Update in-memory data structure (optional, can be removed if memory usage is a concern)
                    for (LiftRideEvent event : batch) {
                        int skierId = event.getSkierId();
                        skierData.computeIfAbsent(skierId, k ->
                                Collections.synchronizedList(new ArrayList<>())
                        ).add(event);
                    }

                    // Update statistics
                    databaseOperations.add(batch.size());
                    return true;
                } catch (Exception e) {
                    databaseErrors.add(batch.size());
                    System.err.println("Error batch processing in Redis: " + e.getMessage());
                    return false;
                }
            });

            if (!processed) {
                processingErrors.add(batch.size());
            }

            // Update batch metrics
            long processingTime = System.currentTimeMillis() - startTime;
            batchProcessTime.add(processingTime);
            batchesProcessed.increment();
            batchSizes.add(batch.size());

            if (drained % 20 == 0) { // Log only 5% of batches
                System.out.println("Processed batch of " + drained + " messages in " + processingTime + "ms");
            }

        } catch (Exception e) {
            processingErrors.add(batch.size());
            System.err.println("Circuit breaker prevented batch processing: " + e.getMessage());
        }
    }

    private void processMessage(LiftRideEvent event) {
        try {
            skierRepository.recordLiftRide(
                    event.getSkierId(),
                    event.getResortId(),
                    event.getLiftId(),
                    event.getSeasonId(),
                    event.getDayId(),
                    event.getTime()
            );

            resortRepository.recordSkierVisit(
                    event.getResortId(),
                    event.getSkierId(),
                    event.getSeasonId(),
                    event.getDayId()
            );

            databaseOperations.increment();

        } catch (Exception e) {
            databaseErrors.increment();
            System.err.println("Error storing data in Redis: " + e.getMessage());
        }

        skierData.computeIfAbsent(event.getSkierId(), k ->
                Collections.synchronizedList(new ArrayList<>())
        ).add(event);
    }

    private void startQueueMonitoring() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                Channel monitorChannel = connection.createChannel();

                AMQP.Queue.DeclareOk queueInfo = monitorChannel.queueDeclarePassive(queueName);
                int messagesInQueue = queueInfo.getMessageCount();

                System.out.println("\n=== Queue Status - " +
                        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME) + " ===");
                System.out.println("Current queue depth: " + messagesInQueue + " messages");
                System.out.println("Active consumer threads: " + activeThreads.get());
                System.out.println("Active consumers: " + activeConsumers.get());
                System.out.println("Batch queue size: " + eventBatchQueue.size());

                adjustThreadCount(messagesInQueue);

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

                System.out.println("Consume Message Circuit: " + consumeMessageCircuitBreaker.getState() +
                        " (Failures: " + consumeMessageCircuitBreaker.getFailureCount() +
                        ", Successes: " + consumeMessageCircuitBreaker.getSuccessCount() + ")");

                System.out.println("Process Message Circuit: " + processMessageCircuitBreaker.getState() +
                        " (Failures: " + processMessageCircuitBreaker.getFailureCount() +
                        ", Successes: " + processMessageCircuitBreaker.getSuccessCount() + ")");

                System.out.println("Total circuit breaker trips: " + circuitBreakerTrips.sum());
                System.out.println("Successful retries: " + circuitBreakerSuccessfulRetries.sum());

            } catch (Exception e) {
                System.err.println("Error monitoring circuit breakers: " + e.getMessage());
            }
        }, 10000, 10000, TimeUnit.MILLISECONDS);
    }

    private void startStatsReporting() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                long processed = messagesProcessed.sum();
                long processedPerSecond = messagesPerSecond.sumThenReset();
                long errors = processingErrors.sum();
                int numSkiers = skierData.size();
                int active = activeThreads.get();
                int consumers = activeConsumers.get();
                long dbOps = databaseOperations.sum();
                long dbErrors = databaseErrors.sum();

                // Batch processing stats
                long batches = batchesProcessed.sum();
                long avgBatchTime = batches > 0 ? batchProcessTime.sum() / batches : 0;
                long avgBatchSize = batches > 0 ? batchSizes.sum() / batches : 0;
                int batchQueueSize = eventBatchQueue.size();

                System.out.println("\n=== Performance Statistics - " +
                        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME) + " ===");
                System.out.println("Active consumer threads: " + active + " (consumers: " + consumers + ")");
                System.out.println("Messages processed per second: " + processedPerSecond);
                System.out.println("Total messages processed: " + processed);
                System.out.println("Processing errors: " + errors);
                System.out.println("Distinct skiers tracked: " + numSkiers);
                System.out.println("Database operations: " + dbOps);
                System.out.println("Database errors: " + dbErrors);

                // Batch stats
                System.out.println("Batch processing: " + batches + " batches, avg time: " +
                        avgBatchTime + "ms, avg size: " + avgBatchSize +
                        ", queue size: " + batchQueueSize);

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

    private void adjustThreadCount(int messagesInQueue) {
        int currentThreads = activeThreads.get();

        if (messagesInQueue > 1000 && currentThreads < maxThreads) {
            // Calculate threads to add based on queue depth
            int threadsToAdd = Math.min(
                    Math.max(5, messagesInQueue / 1000), // Based on queue depth
                    maxThreads - currentThreads
            );
            System.out.println("Adding " + threadsToAdd + " consumer threads due to high queue depth: " + messagesInQueue);

            for (int i = 0; i < threadsToAdd; i++) {
                startConsumerThread(-1); // -1 indicates a dynamically added thread
            }
        } else if (messagesInQueue < RabbitMQConfig.LOW_QUEUE_THRESHOLD && currentThreads > initialThreads) {
            // Queue is nearly empty, we can reduce threads
            System.out.println("Queue depth is low. Could reduce by " +
                    Math.min(RabbitMQConfig.THREADS_TO_REMOVE, currentThreads - initialThreads) +
                    " threads when current work completes.");
        }
    }

    public void shutdown() {
        System.out.println("Shutting down RabbitMQ Consumer...");
        running = false;

        // Process any remaining messages in the batch queue
        System.out.println("Processing final batch of " + eventBatchQueue.size() + " messages");
        processBatch();

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

        System.out.println("RabbitMQ Consumer shutdown complete. Processed " +
                messagesProcessed.sum() + " messages.");
        shutdownLatch.countDown();
    }

    public void waitForShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    public long getMessagesProcessed() {
        return messagesProcessed.sum();
    }

    public long getProcessingErrors() {
        return processingErrors.sum();
    }

    public int getActiveThreads() {
        return activeThreads.get();
    }

    public long getBatchesProcessed() {
        return batchesProcessed.sum();
    }

    public long getAverageBatchTime() {
        long batches = batchesProcessed.sum();
        return batches > 0 ? batchProcessTime.sum() / batches : 0;
    }

    public int getBatchQueueSize() {
        return eventBatchQueue.size();
    }

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
}