package upic.consumer.repository;

import upic.consumer.model.LiftRideEvent;

import java.util.List;
import java.util.Map;

public interface SkierRepository {
    /**
     * Record a single lift ride event
     */
    void recordLiftRide(int skierId, int resortId, int liftId, int seasonId, int dayId, int time);

    /**
     * Record a batch of lift ride events
     */
    void recordLiftRideBatch(List<LiftRideEvent> events);

    /**
     * Get the number of days skied in a season
     */
    int getDaysSkiedInSeason(int skierId, int seasonId);

    /**
     * Get vertical totals by day
     */
    Map<String, Integer> getVerticalTotalsByDay(int skierId, int seasonId);

    /**
     * Get lifts ridden by day
     */
    Map<String, List<Integer>> getLiftsByDay(int skierId, int seasonId);
}