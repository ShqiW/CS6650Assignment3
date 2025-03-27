package upic.consumer.repository;

public interface ResortRepository {
    /**
     * 记录滑雪者到达度假村
     */
    void recordSkierVisit(int resortId, int skierId, int seasonId, int dayId);

    /**
     * 获取度假村某天的唯一滑雪者数量
     */
    int getUniqueSkiersCount(int resortId, int dayId, int seasonId);
}
