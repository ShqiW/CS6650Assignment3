package upic.server.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import upic.server.config.RabbitMQConfig;
import upic.server.config.ServerConfig;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A thread-safe pool of RabbitMQ channels.
 * This implementation uses a BlockingQueue to manage channel resources.
 */
public class RabbitMQChannelPool {
    private static final Logger LOGGER = Logger.getLogger(RabbitMQChannelPool.class.getName());

    private final Connection connection;
    private final BlockingQueue<Channel> channelPool;
    private final AtomicInteger totalChannels = new AtomicInteger(0);
    private final int maxChannels;
    private final String queueName;
    private final boolean queueDurable;
    private final boolean queueExclusive;
    private final boolean queueAutoDelete;

    /**
     * Creates a new channel pool with the specified size.
     * @param poolSize The maximum number of channels in the pool
     * @throws IOException If a connection or channel cannot be created
     * @throws TimeoutException If connection establishment times out
     */
    public RabbitMQChannelPool(int poolSize) throws IOException, TimeoutException {
        this.maxChannels = poolSize;
        this.connection = RabbitMQConfig.createConnection();
        this.channelPool = new LinkedBlockingQueue<>(poolSize);
        this.queueName = RabbitMQConfig.getQueueName();
        this.queueDurable = RabbitMQConfig.isQueueDurable();
        this.queueExclusive = RabbitMQConfig.isQueueExclusive();
        this.queueAutoDelete = RabbitMQConfig.isQueueAutoDelete();

        // Initialize the pool with a few channels
        int initialChannels = Math.min(poolSize / 4, 10);
        for (int i = 0; i < initialChannels; i++) {
            channelPool.offer(createChannel());
        }

        LOGGER.info("Created RabbitMQ channel pool with max size: " + poolSize +
                ", initially filled with: " + initialChannels + " channels");
    }

    /**
     * Gets a channel from the pool, or creates a new one if needed.
     * @return A Channel ready for use
     * @throws IOException If a new channel cannot be created
     * @throws InterruptedException If thread is interrupted while waiting
     */
    public Channel getChannel() throws IOException, InterruptedException {
        Channel channel = channelPool.poll();
        if (channel == null) {
            // No channel available in the pool
            if (totalChannels.get() < maxChannels) {
                // We can create a new channel
                try {
                    channel = createChannel();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Failed to create new channel", e);
                    throw e;
                }
            } else {
                // We've reached the maximum, wait for a channel to be returned
                channel = channelPool.take();
            }
        }

        return channel;
    }

    /**
     * Returns a channel to the pool.
     * @param channel The channel to return
     */
    public void returnChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            channelPool.offer(channel);
        } else {
            // If the channel is closed, decrement the count
            totalChannels.decrementAndGet();
        }
    }

    /**
     * Creates a new channel and declares the queue.
     * @return A new Channel with the queue declared
     * @throws IOException If channel creation fails
     */
    private Channel createChannel() throws IOException {
        Channel channel = connection.createChannel();

        // Declare the queue
        channel.queueDeclare(
                queueName,
                queueDurable,
                queueExclusive,
                queueAutoDelete,
                null
        );

        // Set QoS/prefetch - this limits the number of unacknowledged messages
        // that a channel will accept, important for load balancing across consumers
        channel.basicQos(ServerConfig.RABBITMQ_PREFETCH_COUNT);

        totalChannels.incrementAndGet();
        return channel;
    }

    /**
     * Close all channels and the connection.
     */
    public void close() {
        // Drain the pool and close each channel
        Channel channel;
        while ((channel = channelPool.poll()) != null) {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (IOException | TimeoutException e) {
                LOGGER.log(Level.WARNING, "Error closing channel", e);
            }
        }

        // Close the connection
        try {
            if (connection.isOpen()) {
                connection.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error closing RabbitMQ connection", e);
        }

        LOGGER.info("RabbitMQ channel pool closed");
    }

    /**
     * Get the number of channels currently in the pool.
     * @return The number of available channels
     */
    public int getAvailableChannels() {
        return channelPool.size();
    }

    /**
     * Get the total number of channels created by this pool.
     * @return The total number of channels
     */
    public int getTotalChannels() {
        return totalChannels.get();
    }
}