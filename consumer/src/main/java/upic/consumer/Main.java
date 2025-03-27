package upic.consumer;

import upic.consumer.messaging.RabbitMQConsumer;

/**
 * Main class to start the RabbitMQ consumer application.
 * This can be used as an entry point for deploying the consumer as a standalone application.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("Starting Ski Resort RabbitMQ Consumer");

        // Simply delegate to the consumer's main method
        RabbitMQConsumer.main(args);
    }
}