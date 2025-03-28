package upic.server.config;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import upic.server.messaging.RabbitMQChannelPool;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Configuration class for RabbitMQ connection.
 * Centralizes all RabbitMQ-related connection parameters.
 */
public class RabbitMQConfig {
    // RabbitMQ connection parameters
    private static final String HOST = EnvLoader.getEnv("RABITMQ_BASE_ADDRESS",
            "35.160.7.42");

    // private static final String HOST = "35.160.7.42";

    private static final int PORT = Integer.parseInt(System.getProperty("rabbitmq.port", "5672"));
    private static final String VIRTUAL_HOST = System.getProperty("rabbitmq.virtualHost", "/");
    private static final String USERNAME = System.getProperty("rabbitmq.username", "admin");
    private static final String PASSWORD = System.getProperty("rabbitmq.password", "rmq");

    // Queue configuration
    private static final String QUEUE_NAME = System.getProperty("rabbitmq.queueName", "ski-lift-events");
    private static final boolean QUEUE_DURABLE = Boolean
            .parseBoolean(System.getProperty("rabbitmq.queueDurable", "true"));
    private static final boolean QUEUE_EXCLUSIVE = Boolean
            .parseBoolean(System.getProperty("rabbitmq.queueExclusive", "false"));
    private static final boolean QUEUE_AUTO_DELETE = Boolean
            .parseBoolean(System.getProperty("rabbitmq.queueAutoDelete", "false"));

    // Connection pooling parameters
    private static final int CONNECTION_TIMEOUT = Integer
            .parseInt(System.getProperty("rabbitmq.connectionTimeout", "30000"));
    private static final int HEARTBEAT_TIMEOUT = Integer
            .parseInt(System.getProperty("rabbitmq.heartbeatTimeout", "60"));

    // Connection factory singleton
    private static ConnectionFactory factory = null;
    private static final Logger LOGGER = Logger.getLogger(RabbitMQChannelPool.class.getName());

    /**
     * Creates and returns a ConnectionFactory with the configured parameters.
     * 
     * @return A configured RabbitMQ ConnectionFactory
     */
    public static synchronized ConnectionFactory getConnectionFactory() {
        if (factory == null) {
            factory = new ConnectionFactory();

            factory.setHost(HOST);
            factory.setPort(PORT);
            factory.setVirtualHost(VIRTUAL_HOST);
            factory.setUsername(USERNAME);
            factory.setPassword(PASSWORD);
            factory.setConnectionTimeout(CONNECTION_TIMEOUT);
            factory.setRequestedHeartbeat(HEARTBEAT_TIMEOUT);

            LOGGER.info(HOST + ":" + PORT + " " + VIRTUAL_HOST + " " + USERNAME + " " + PASSWORD);
            // Performance tuning
            factory.setAutomaticRecoveryEnabled(true);
            factory.setNetworkRecoveryInterval(10000); // 10 seconds between recovery attempts
        }
        return factory;
    }

    /**
     * Creates a new connection to RabbitMQ.
     * 
     * @return A new Connection object
     * @throws IOException      If connection cannot be established
     * @throws TimeoutException If connection times out
     */
    public static Connection createConnection() throws IOException, TimeoutException {
        try {
            return getConnectionFactory().newConnection();
        } catch (IOException e) {
            LOGGER.severe("Failed to create RabbitMQ connection: " + e.getMessage());
            throw new IOException(
                    e.getMessage() + getConnectionFactory().getHost() + ":" + getConnectionFactory().getPort());
        } catch (TimeoutException e) {
            LOGGER.severe("Failed to create RabbitMQ connection: " + e.getMessage());
            throw new TimeoutException(
                    e.getMessage() + getConnectionFactory().getHost() + ":" + getConnectionFactory().getPort());
        }

    }

    /**
     * Get the queue name.
     * 
     * @return The configured queue name
     */
    public static String getQueueName() {
        return QUEUE_NAME;
    }

    /**
     * Get the queue durability setting.
     * 
     * @return True if the queue should be durable
     */
    public static boolean isQueueDurable() {
        return QUEUE_DURABLE;
    }

    /**
     * Get the queue exclusivity setting.
     * 
     * @return True if the queue should be exclusive
     */
    public static boolean isQueueExclusive() {
        return QUEUE_EXCLUSIVE;
    }

    /**
     * Get the queue auto-delete setting.
     * 
     * @return True if the queue should auto-delete
     */
    public static boolean isQueueAutoDelete() {
        return QUEUE_AUTO_DELETE;
    }
}