package upic.consumer.repository;

import upic.consumer.model.LiftRideEvent;
import java.util.List;

public interface FastRepository {
    /**
     * 记录单个滑雪事件，采用高性能单一操作方式
     */
    void recordEvent(LiftRideEvent event);

    /**
     * 批量记录滑雪事件，进一步提高性能
     */
    void recordEventBatch(List<LiftRideEvent> events);
}