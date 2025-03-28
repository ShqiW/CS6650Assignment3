package upic.consumer.config;

/**
 * Configuration class for RabbitMQ consumer component.
 * Centralizes all RabbitMQ-related consumer parameters.
 */
public class RabbitMQConfig {
    // RabbitMQ connection parameters
    public static final String HOST = EnvLoader.getEnv("RABITMQ_BASE_ADDRESS", "34.209.244.208");
    public static final int PORT = Integer.parseInt(System.getProperty("rabbitmq.port", "5672"));
    public static final String VIRTUAL_HOST = System.getProperty("rabbitmq.virtualHost", "/");
    public static final String USERNAME = System.getProperty("rabbitmq.username", "admin");
    public static final String PASSWORD = System.getProperty("rabbitmq.password", "rmq");

    // Queue configuration
    public static final String QUEUE_NAME = System.getProperty("rabbitmq.queueName", "ski-lift-events");
    public static final boolean QUEUE_DURABLE = Boolean
            .parseBoolean(System.getProperty("rabbitmq.queueDurable", "true"));
    public static final boolean QUEUE_EXCLUSIVE = Boolean
            .parseBoolean(System.getProperty("rabbitmq.queueExclusive", "false"));
    public static final boolean QUEUE_AUTO_DELETE = Boolean
            .parseBoolean(System.getProperty("rabbitmq.queueAutoDelete", "false"));

    // Consumer configuration
    public static final int PREFETCH_COUNT = Integer.parseInt(System.getProperty("rabbitmq.prefetchCount", "500")); // 200-500
    public static final int CONSUMER_THREAD_COUNT = Integer
            .parseInt(System.getProperty("rabbitmq.consumerThreads", "128")); // 64-128
    public static final int MAX_CONSUMER_THREAD_COUNT = Integer
            .parseInt(System.getProperty("rabbitmq.maxConsumerThreads", "128"));
    public static final boolean AUTO_ACK = Boolean.parseBoolean(System.getProperty("rabbitmq.autoAck", "false"));

    // Connection pooling parameters
    public static final int CONNECTION_TIMEOUT = Integer
            .parseInt(System.getProperty("rabbitmq.connectionTimeout", "30000"));
    public static final int HEARTBEAT_TIMEOUT = Integer.parseInt(System.getProperty("rabbitmq.heartbeatTimeout", "60"));
    public static final int NETWORK_RECOVERY_INTERVAL = Integer
            .parseInt(System.getProperty("rabbitmq.networkRecoveryInterval", "10000"));

    // Auto-scaling configuration
    public static final int HIGH_QUEUE_THRESHOLD = Integer
            .parseInt(System.getProperty("consumer.highQueueThreshold", "10000"));
    public static final int LOW_QUEUE_THRESHOLD = Integer
            .parseInt(System.getProperty("consumer.lowQueueThreshold", "100"));
    public static final int THREADS_TO_ADD = Integer.parseInt(System.getProperty("consumer.threadsToAdd", "10"));
    public static final int THREADS_TO_REMOVE = Integer.parseInt(System.getProperty("consumer.threadsToRemove", "5"));

    // Performance monitoring
    public static final long STATS_INTERVAL_MS = Long.parseLong(System.getProperty("consumer.statsInterval", "5000"));

    /**
     * Calculate optimal thread count based on CPU cores and I/O intensity.
     * 
     * @return Recommended number of consumer threads
     */
    public static int calculateOptimalThreadCount() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        // For I/O bound operations, we typically want more threads than cores
        // A common formula is: threads = cores * (1 + wait_time / service_time)
        // Assuming a 1:10 ratio of service to wait time for RabbitMQ operations
        return Math.min(MAX_CONSUMER_THREAD_COUNT, availableProcessors * 11);
    }

}