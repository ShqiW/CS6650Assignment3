package upic.consumer.repository.impl;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import upic.consumer.model.LiftRideEvent;
import upic.consumer.repository.FastRepository;
import upic.consumer.repository.RedisConnector;

import java.util.List;

public class FastLiftRideRepository implements FastRepository {
    private final Gson gson = new Gson();

    @Override
    public void recordEvent(LiftRideEvent event) {
        try (Jedis jedis = RedisConnector.getResource()) {
            // 将事件转换为JSON并存储在列表中 - 单一操作
            jedis.rpush("ski:events", event.toString());
        }
    }

    @Override
    public void recordEventBatch(List<LiftRideEvent> events) {
        if (events == null || events.isEmpty()) return;

        try (Jedis jedis = RedisConnector.getResource()) {
            Pipeline pipeline = jedis.pipelined();

            for (LiftRideEvent event : events) {
                pipeline.rpush("ski:events", event.toString());
            }

            pipeline.sync();
        }
    }
}