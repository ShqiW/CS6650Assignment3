package upic.consumer.repository.impl;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import upic.consumer.model.LiftRideEvent;
import upic.consumer.repository.RedisConnector;
import upic.consumer.repository.SkierRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisSkierRepository implements SkierRepository {

    @Override
    public void recordLiftRideBatch(List<LiftRideEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        try (Jedis jedis = RedisConnector.getResource()) {
            Pipeline pipeline = jedis.pipelined();

            for (LiftRideEvent event : events) {
                int skierId = event.getSkierId();
                int resortId = event.getResortId();
                int liftId = event.getLiftId();
                int seasonId = event.getSeasonId();
                int dayId = event.getDayId();
                int time = event.getTime();
                int vertical = liftId * 10;

                String dayKey = "skier:" + skierId + ":days:" + seasonId;
                pipeline.sadd(dayKey, String.valueOf(dayId));

                String liftRideKey = "skier:" + skierId + ":day:" + dayId + ":season:" + seasonId + ":lifts";
                pipeline.rpush(liftRideKey, String.valueOf(liftId));

                String timeKey = "skier:" + skierId + ":day:" + dayId + ":season:" + seasonId + ":time:" + liftId;
                pipeline.set(timeKey, String.valueOf(time));

                String verticalKey = "skier:" + skierId + ":day:" + dayId + ":season:" + seasonId + ":vertical";
                pipeline.incrBy(verticalKey, vertical);
            }

            pipeline.sync();
        }
    }

    @Override
    public void recordLiftRide(int skierId, int resortId, int liftId, int seasonId, int dayId, int time) {
        int vertical = liftId * 10;

        try (Jedis jedis = RedisConnector.getResource()) {
            Pipeline pipeline = jedis.pipelined();

            String dayKey = "skier:" + skierId + ":days:" + seasonId;
            pipeline.sadd(dayKey, String.valueOf(dayId));

            String liftRideKey = "skier:" + skierId + ":day:" + dayId + ":season:" + seasonId + ":lifts";
            pipeline.rpush(liftRideKey, String.valueOf(liftId));

            String timeKey = "skier:" + skierId + ":day:" + dayId + ":season:" + seasonId + ":time:" + liftId;
            pipeline.set(timeKey, String.valueOf(time));

            String verticalKey = "skier:" + skierId + ":day:" + dayId + ":season:" + seasonId + ":vertical";
            pipeline.incrBy(verticalKey, vertical);

            pipeline.sync();
        }
    }

    @Override
    public int getDaysSkiedInSeason(int skierId, int seasonId) {
        try (Jedis jedis = RedisConnector.getResource()) {
            String dayKey = "skier:" + skierId + ":days:" + seasonId;
            Set<String> days = jedis.smembers(dayKey);
            return days.size();
        }
    }

    @Override
    public Map<String, Integer> getVerticalTotalsByDay(int skierId, int seasonId) {
        Map<String, Integer> results = new HashMap<>();
        try (Jedis jedis = RedisConnector.getResource()) {
            String dayKey = "skier:" + skierId + ":days:" + seasonId;
            Set<String> days = jedis.smembers(dayKey);

            for (String day : days) {
                String verticalKey = "skier:" + skierId + ":day:" + day + ":season:" + seasonId + ":vertical";
                String value = jedis.get(verticalKey);
                if (value != null) {
                    results.put(day, Integer.parseInt(value));
                }
            }
        }
        return results;
    }

    @Override
    public Map<String, List<Integer>> getLiftsByDay(int skierId, int seasonId) {
        Map<String, List<Integer>> results = new HashMap<>();
        try (Jedis jedis = RedisConnector.getResource()) {
            String dayKey = "skier:" + skierId + ":days:" + seasonId;
            Set<String> days = jedis.smembers(dayKey);

            for (String day : days) {
                String liftRideKey = "skier:" + skierId + ":day:" + day + ":season:" + seasonId + ":lifts";
                List<String> liftsStr = jedis.lrange(liftRideKey, 0, -1);
                List<Integer> lifts = liftsStr.stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
                results.put(day, lifts);
            }
        }
        return results;
    }
}