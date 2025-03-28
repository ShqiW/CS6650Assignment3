package upic.consumer.repository.impl;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import upic.consumer.repository.RedisConnector;
import upic.consumer.repository.SkierRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisSkierRepository implements SkierRepository {

    @Override
    public void recordLiftRide(int skierId, int resortId, int liftId, int seasonId, int dayId, int time) {
        // 计算垂直高度
        int vertical = liftId * 10;

        try (Jedis jedis = RedisConnector.getResource()) {
            // 使用管道以提高性能
            Pipeline pipeline = jedis.pipelined();
            
            // skier: s
            // days: c
            // day: d
            // season: q
            // lifts: l
            // resort: r
            // vertical: v
            // time: t

            // 记录滑雪者的滑雪日
            String dayKey = "s" + skierId + "c" + seasonId;
            pipeline.sadd(dayKey, String.valueOf(dayId));

            // 记录滑雪者在特定日期的乘坐
            String liftRideKey = "s" + skierId + "d" + dayId + "q" + seasonId + "l";
            pipeline.rpush(liftRideKey, String.valueOf(liftId));

            // 记录滑雪时间
            String timeKey = "s" + skierId + "d" + dayId + "q" + seasonId + "t" + liftId;
            pipeline.set(timeKey, String.valueOf(time));

            // 更新垂直总和
            String verticalKey = "s" + skierId + "d" + dayId + "q" + seasonId + "v";
            pipeline.incrBy(verticalKey, vertical);

            // 执行所有命令
            pipeline.sync();
        }
    }

    @Override
    public int getDaysSkiedInSeason(int skierId, int seasonId) {
        try (Jedis jedis = RedisConnector.getResource()) {
            String dayKey = "s" + skierId + "c" + seasonId;
            Set<String> days = jedis.smembers(dayKey);
            return days.size();
        }
    }

    @Override
    public Map<String, Integer> getVerticalTotalsByDay(int skierId, int seasonId) {
        Map<String, Integer> results = new HashMap<>();
        try (Jedis jedis = RedisConnector.getResource()) {
            String dayKey = "s" + skierId + "c" + seasonId;
            Set<String> days = jedis.smembers(dayKey);

            for (String day : days) {
                String verticalKey = "s" + skierId + "d" + day + "q" + seasonId + "v";
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
            String dayKey = "s" + skierId + "c" + seasonId;
            Set<String> days = jedis.smembers(dayKey);

            for (String day : days) {
                String liftRideKey = "s" + skierId + "d" + day + "q" + seasonId + "l";
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