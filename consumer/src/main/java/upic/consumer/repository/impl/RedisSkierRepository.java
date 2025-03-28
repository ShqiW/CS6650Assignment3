package upic.consumer.repository.impl;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import upic.consumer.model.LiftRideEvent;
import upic.consumer.repository.RedisConnector;
import upic.consumer.repository.SkierRepository;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class RedisSkierRepository implements SkierRepository {
    private static final Logger LOGGER = Logger.getLogger(RedisSkierRepository.class.getName());

    // Buffer for batched writes
    private final BlockingQueue<List<LiftRideEvent>> writeBuffer = new LinkedBlockingQueue<>(100);

    // Thread pool for processing the buffer
    private final ScheduledExecutorService bufferProcessor;

    // Statistics
    private final LongAdder totalEvents = new LongAdder();
    private final LongAdder totalBatches = new LongAdder();
    private final LongAdder bufferOverflows = new LongAdder();
    private final LongAdder redisErrors = new LongAdder();
    private final LongAdder totalWriteTime = new LongAdder();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    // Configuration
    private static final int BUFFER_PROCESSOR_INTERVAL_MS = 50;
    private static final int MAX_BATCH_COMBINE = 10;
    private static final int STATS_REPORT_INTERVAL_MS = 10000;

    public RedisSkierRepository() {
        // Initialize the buffer processor thread pool
        this.bufferProcessor = Executors.newScheduledThreadPool(2);

        // Start the buffer processor
        bufferProcessor.scheduleAtFixedRate(
                this::processBuffer,
                BUFFER_PROCESSOR_INTERVAL_MS,
                BUFFER_PROCESSOR_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        // Start stats reporting
        bufferProcessor.scheduleAtFixedRate(
                this::reportStats,
                STATS_REPORT_INTERVAL_MS,
                STATS_REPORT_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        LOGGER.info("Redis buffer initialized with capacity: " + writeBuffer.remainingCapacity());

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /**
     * Process batches from the write buffer
     */
    private void processBuffer() {
        // Collection to combine multiple batches
        List<LiftRideEvent> combinedBatch = new ArrayList<>(1000);

        // Drain up to MAX_BATCH_COMBINE batches from the buffer
        int drainedBatches = 0;
        while (drainedBatches < MAX_BATCH_COMBINE && !writeBuffer.isEmpty()) {
            List<LiftRideEvent> batch = writeBuffer.poll();
            if (batch != null && !batch.isEmpty()) {
                combinedBatch.addAll(batch);
                drainedBatches++;
            }
        }

        if (combinedBatch.isEmpty()) {
            return;
        }

        // Process the combined batch
        long startTime = System.currentTimeMillis();

        try (Jedis jedis = RedisConnector.getResource()) {
            Pipeline pipeline = jedis.pipelined();

            // Process each event in the combined batch
            for (LiftRideEvent event : combinedBatch) {
                int skierId = event.getSkierId();
                int resortId = event.getResortId();
                int liftId = event.getLiftId();
                int seasonId = event.getSeasonId();
                int dayId = event.getDayId();
                int time = event.getTime();
                int vertical = liftId * 10;

                // Simplified data model - use hash instead of multiple keys
                String skierDayKey = "skier:" + skierId + ":season:" + seasonId + ":day:" + dayId;

                // Increment vertical for this skier/day
                pipeline.hincrBy(skierDayKey, "vertical", vertical);

                // Add to lift rides list
                pipeline.sadd(skierDayKey + ":lifts", String.valueOf(liftId));

                // Record day skied
                pipeline.sadd("skier:" + skierId + ":days:" + seasonId, String.valueOf(dayId));

                // Record resort visit
                pipeline.sadd("resort:" + resortId + ":day:" + dayId + ":season:" + seasonId + ":skiers",
                        String.valueOf(skierId));
            }

            // Execute all commands in a single network round-trip
            pipeline.sync();

            // Update statistics
            totalEvents.add(combinedBatch.size());
            totalBatches.add(drainedBatches);
            totalWriteTime.add(System.currentTimeMillis() - startTime);

            // Log processing (only occasionally to reduce noise)
            if (Math.random() < 0.01) {
                LOGGER.info("Processed " + combinedBatch.size() + " events in " +
                        (System.currentTimeMillis() - startTime) + "ms (combined " +
                        drainedBatches + " batches)");
            }

        } catch (Exception e) {
            redisErrors.increment();
            LOGGER.log(Level.SEVERE, "Error writing batch to Redis", e);

            // In a real system, you might want to implement retry logic here
            // or write failed events to a dead letter queue
        }
    }

    /**
     * Report buffer statistics periodically
     */
    private void reportStats() {
        long events = totalEvents.sum();
        long batches = totalBatches.sum();
        long errors = redisErrors.sum();
        long overflows = bufferOverflows.sum();
        long writeTime = totalWriteTime.sum();

        double avgWriteTime = batches > 0 ? (double) writeTime / batches : 0;
        double avgBatchSize = batches > 0 ? (double) events / batches : 0;

        LOGGER.info("=== Redis Buffer Stats ===");
        LOGGER.info("Total events: " + events);
        LOGGER.info("Total batches: " + batches);
        LOGGER.info("Buffer overflows: " + overflows);
        LOGGER.info("Redis errors: " + errors);
        LOGGER.info("Avg write time: " + String.format("%.2f", avgWriteTime) + "ms");
        LOGGER.info("Avg batch size: " + String.format("%.2f", avgBatchSize) + " events");
        LOGGER.info("Current buffer size: " + writeBuffer.size() + " batches");
    }

    /**
     * Shutdown the buffer processor and process any remaining data
     */
    public void shutdown() {
        if (isRunning.compareAndSet(true, false)) {
            LOGGER.info("Shutting down Redis buffer processor...");

            // Process any remaining data
            processRemainingData();

            // Shutdown the thread pool
            bufferProcessor.shutdown();
            try {
                if (!bufferProcessor.awaitTermination(5, TimeUnit.SECONDS)) {
                    bufferProcessor.shutdownNow();
                }
            } catch (InterruptedException e) {
                bufferProcessor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            LOGGER.info("Redis buffer processor shutdown complete");
        }
    }

    /**
     * Process any remaining data in the buffer
     */
    private void processRemainingData() {
        LOGGER.info("Processing remaining " + writeBuffer.size() + " batches in buffer");
        while (!writeBuffer.isEmpty()) {
            processBuffer();
        }
    }

    @Override
    public void recordLiftRideBatch(List<LiftRideEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        // Attempt to add the batch to the buffer
        boolean added = writeBuffer.offer(new ArrayList<>(events));

        if (!added) {
            // Buffer is full - handle overflow
            bufferOverflows.increment();

            // Processing directly as a fallback
            try (Jedis jedis = RedisConnector.getResource()) {
                Pipeline pipeline = jedis.pipelined();

                for (LiftRideEvent event : events) {
                    int skierId = event.getSkierId();
                    int resortId = event.getResortId();
                    int liftId = event.getLiftId();
                    int seasonId = event.getSeasonId();
                    int dayId = event.getDayId();
                    int vertical = liftId * 10;

                    // Use same data model as in processBuffer
                    String skierDayKey = "skier:" + skierId + ":season:" + seasonId + ":day:" + dayId;
                    pipeline.hincrBy(skierDayKey, "vertical", vertical);
                    pipeline.sadd(skierDayKey + ":lifts", String.valueOf(liftId));
                    pipeline.sadd("skier:" + skierId + ":days:" + seasonId, String.valueOf(dayId));
                    pipeline.sadd("resort:" + resortId + ":day:" + dayId + ":season:" + seasonId + ":skiers",
                            String.valueOf(skierId));
                }

                pipeline.sync();
                totalEvents.add(events.size());

            } catch (Exception e) {
                redisErrors.increment();
                LOGGER.log(Level.SEVERE, "Error handling buffer overflow", e);
            }
        }
    }

    @Override
    public void recordLiftRide(int skierId, int resortId, int liftId, int seasonId, int dayId, int time) {
        // Create a single event and add it to a batch
        LiftRideEvent event = new LiftRideEvent(skierId, resortId, liftId, seasonId, dayId, time);
        recordLiftRideBatch(Collections.singletonList(event));
    }

    @Override
    public int getDaysSkiedInSeason(int skierId, int seasonId) {
        try (Jedis jedis = RedisConnector.getResource()) {
            String dayKey = "skier:" + skierId + ":days:" + seasonId;
            Set<String> days = jedis.smembers(dayKey);
            return days.size();
        }
    }

    @Override
    public Map<String, Integer> getVerticalTotalsByDay(int skierId, int seasonId) {
        Map<String, Integer> results = new HashMap<>();
        try (Jedis jedis = RedisConnector.getResource()) {
            String dayKey = "skier:" + skierId + ":days:" + seasonId;
            Set<String> days = jedis.smembers(dayKey);

            for (String day : days) {
                String skierDayKey = "skier:" + skierId + ":season:" + seasonId + ":day:" + day;
                String vertical = jedis.hget(skierDayKey, "vertical");
                if (vertical != null) {
                    results.put(day, Integer.parseInt(vertical));
                }
            }
        }
        return results;
    }

    @Override
    public Map<String, List<Integer>> getLiftsByDay(int skierId, int seasonId) {
        Map<String, List<Integer>> results = new HashMap<>();
        try (Jedis jedis = RedisConnector.getResource()) {
            String dayKey = "skier:" + skierId + ":days:" + seasonId;
            Set<String> days = jedis.smembers(dayKey);

            for (String day : days) {
                String liftsKey = "skier:" + skierId + ":season:" + seasonId + ":day:" + day + ":lifts";
                Set<String> liftsStr = jedis.smembers(liftsKey);
                List<Integer> lifts = liftsStr.stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
                results.put(day, lifts);
            }
        }
        return results;
    }
}