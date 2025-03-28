package upic.consumer.repository.impl;

import redis.clients.jedis.Jedis;
import upic.consumer.model.LiftRideEvent;
import upic.consumer.repository.RedisConnector;
import upic.consumer.repository.ResortRepository;

import java.util.List;

public class RedisResortRepository implements ResortRepository {

    @Override
    public void recordSkierVisitBatch(List<LiftRideEvent> events) {
        // We don't need to implement this separately anymore
        // The SkierRepository handles resort visits during batch processing
    }

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