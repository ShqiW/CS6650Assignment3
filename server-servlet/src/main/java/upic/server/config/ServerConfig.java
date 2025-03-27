package upic.server.config;

/**
 * Configuration class for the server component.
 * Centralizes all configuration parameters to make tuning easier.
 */
public class ServerConfig {
//    // AWS SQS configuration (kept for backward compatibility)
//    public static final String AWS_REGION = System.getProperty("aws.region", "us-west-2");
//    public static final String SQS_QUEUE_URL = System.getProperty("aws.queueUrl",
//            "https://sqs.us-west-2.amazonaws.com/145832436892/ski-lift-events");

    // RabbitMQ specific configuration
    public static final int RABBITMQ_CHANNEL_POOL_SIZE = Integer.parseInt(
            System.getProperty("rabbitmq.channelPoolSize", "200"));  //100-200
    public static final int RABBITMQ_PREFETCH_COUNT = Integer.parseInt(
            System.getProperty("rabbitmq.prefetchCount", "10"));
    public static final boolean USE_RABBITMQ = Boolean.parseBoolean(
            System.getProperty("messaging.useRabbitMQ", "true"));

    // Thread pool configuration
    public static final int CORE_POOL_SIZE = Integer.parseInt(
            System.getProperty("server.corePoolSize", "100"));
    public static final int MAX_POOL_SIZE = Integer.parseInt(
            System.getProperty("server.maxPoolSize", "200"));
    public static final int QUEUE_CAPACITY = Integer.parseInt(
            System.getProperty("server.queueCapacity", "500"));
    public static final int KEEP_ALIVE_TIME = Integer.parseInt(
            System.getProperty("server.keepAliveTime", "60"));

    // Async processing configuration
    public static final int ASYNC_TIMEOUT = Integer.parseInt(
            System.getProperty("server.asyncTimeout", "30000"));

    // Message batch processing configuration
    public static final int BATCH_SIZE = Integer.parseInt(
            System.getProperty("server.batchSize", "100"));  //50-100
    public static final long BATCH_FLUSH_INTERVAL_MS = Long.parseLong(
            System.getProperty("server.batchFlushInterval", "50")); //decrease for high frequency

    // Performance monitoring configuration
    public static final long STATS_INTERVAL_MS = Long.parseLong(
            System.getProperty("server.statsInterval", "60000"));

    // Business logic validation
    public static final int MAX_SKIER_ID = Integer.parseInt(
            System.getProperty("server.maxSkierId", "100000"));
    public static final int MAX_RESORT_ID = Integer.parseInt(
            System.getProperty("server.maxResortId", "10"));
    public static final int MAX_LIFT_ID = Integer.parseInt(
            System.getProperty("server.maxLiftId", "40"));
    public static final int MAX_DAY_ID = Integer.parseInt(
            System.getProperty("server.maxDayId", "1"));
    public static final int SEASON_ID = Integer.parseInt(
            System.getProperty("server.seasonId", "2025"));
    public static final int MAX_TIME = Integer.parseInt(
            System.getProperty("server.maxTime", "360"));

    /**
     * Calculate optimal thread count based on system resources.
     * @return the optimal thread count
     */
    public static int getOptimalThreadCount() {
        int processors = Runtime.getRuntime().availableProcessors();
        long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);

        // 10 threads per core, but limited by available memory (1MB per thread)
        int threadsByMemory = (int)(maxMemory / 1);
        int threadsByCpu = processors * 10;

        return Math.min(MAX_POOL_SIZE, Math.min(threadsByCpu, threadsByMemory));
//        return 500;
    }

    /**
     * Determine optimal batch size based on queue depth
     * @param queueDepth current approximate number of messages in queue
     * @return the optimal batch size
     */
    public static int getOptimalBatchSize(int queueDepth) {
        if (queueDepth > 10000) {
            return 20; // Increase batch size when queue is deep
        } else if (queueDepth > 5000) {
            return 15;
        } else {
            return BATCH_SIZE;
        }
    }
}