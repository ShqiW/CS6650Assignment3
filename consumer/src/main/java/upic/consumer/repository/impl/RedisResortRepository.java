package upic.consumer.repository.impl;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import upic.consumer.model.LiftRideEvent;
import upic.consumer.repository.RedisConnector;
import upic.consumer.repository.ResortRepository;

import java.util.List;

public class RedisResortRepository implements ResortRepository {

    @Override
    public void recordSkierVisitBatch(List<LiftRideEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        try (Jedis jedis = RedisConnector.getResource()) {
            Pipeline pipeline = jedis.pipelined();

            for (LiftRideEvent event : events) {
                String resortSkiersKey = "resort:" + event.getResortId() +
                        ":day:" + event.getDayId() +
                        ":season:" + event.getSeasonId() + ":skiers";
                pipeline.sadd(resortSkiersKey, String.valueOf(event.getSkierId()));
            }

            pipeline.sync();
        }
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