package upic.consumer.repository;

import java.util.List;
import java.util.Map;

public interface SkierRepository {
    /**
     * 记录滑雪者乘坐缆车事件
     */
    void recordLiftRide(int skierId, int resortId, int liftId, int seasonId, int dayId, int time);

    /**
     * 获取滑雪者在季节中滑雪的天数
     */
    int getDaysSkiedInSeason(int skierId, int seasonId);

    /**
     * 获取滑雪者在每个滑雪日的垂直总和
     */
    Map<String, Integer> getVerticalTotalsByDay(int skierId, int seasonId);

    /**
     * 获取滑雪者在每个滑雪日乘坐的缆车列表
     */
    Map<String, List<Integer>> getLiftsByDay(int skierId, int seasonId);
}
