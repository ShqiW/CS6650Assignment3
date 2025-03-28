package upic.consumer.repository;

import upic.consumer.model.LiftRideEvent;

import java.util.List;
import java.util.Map;

public interface SkierRepository {
    void recordLiftRide(int skierId, int resortId, int liftId, int seasonId, int dayId, int time);
    int getDaysSkiedInSeason(int skierId, int seasonId);
    Map<String, Integer> getVerticalTotalsByDay(int skierId, int seasonId);
    Map<String, List<Integer>> getLiftsByDay(int skierId, int seasonId);

    // Added batch method
    void recordLiftRideBatch(List<LiftRideEvent> events);
}
