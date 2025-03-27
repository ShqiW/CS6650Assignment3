package upic.consumer.repository.impl;


import redis.clients.jedis.Jedis;
import upic.consumer.repository.RedisConnector;
import upic.consumer.repository.ResortRepository;

public class RedisResortRepository implements ResortRepository {

    @Override
    public void recordSkierVisit(int resortId, int skierId, int seasonId, int dayId) {
        try (Jedis jedis = RedisConnector.getResource()) {
            String resortSkiersKey = "resort:" + resortId + ":day:" + dayId + ":season:" + seasonId + ":skiers";
            jedis.sadd(resortSkiersKey, String.valueOf(skierId));
        }
    }

    @Override
    public int getUniqueSkiersCount(int resortId, int dayId, int seasonId) {
        try (Jedis jedis = RedisConnector.getResource()) {
            String resortSkiersKey = "resort:" + resortId + ":day:" + dayId + ":season:" + seasonId + ":skiers";
            long count = jedis.scard(resortSkiersKey);
            return Long.valueOf(count).intValue();
        }
    }
}