package upic.consumer;

import upic.consumer.config.RedisConfig;
import upic.consumer.messaging.RabbitMQConsumer;
import upic.consumer.repository.RedisConnector;

/**
 * Main class to start the RabbitMQ consumer application with Redis support.
 * This can be used as an entry point for deploying the consumer as a standalone application.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("Starting Ski Resort RabbitMQ Consumer with Redis");

        try {
            RedisConfig redisConfig = RedisConfig.getDefaultConfig();
            System.out.println("Redis configuration loaded: " + redisConfig.getHost() + ":" + redisConfig.getPort());
            // System.in.read();
            RedisConnector.initPool(redisConfig);
            System.out.println("Redis connection pool initialized");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down Redis connection pool...");
                RedisConnector.closePool();
                System.out.println("Redis connection pool closed");
            }));

            RabbitMQConsumer.main(args);
        } catch (Exception e) {
            System.err.println("Error initializing application: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}