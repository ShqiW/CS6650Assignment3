package upic.consumer.repository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import upic.consumer.config.RedisConfig;

public class RedisConnector {
    private static JedisPool jedisPool;

    public static void initPool(RedisConfig config) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getMaxTotal());
        poolConfig.setMaxIdle(config.getMaxIdle());
        poolConfig.setMinIdle(config.getMinIdle());
        poolConfig.setMaxWaitMillis(config.getMaxWaitMillis());
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        poolConfig.setTestWhileIdle(false);
        poolConfig.setTestOnCreate(false);
        jedisPool = new JedisPool(poolConfig,
                config.getHost(),
                config.getPort(),
                2000,
                config.getPassword());
        try (Jedis jedis = jedisPool.getResource()) {
            System.out.println("Connected to Redis: " + jedis.ping());
        } catch (Exception e) {
            System.err.println("Failed to connect to Redis: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static Jedis getResource() {
        return jedisPool.getResource();
    }

    public static void closePool() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }
}
