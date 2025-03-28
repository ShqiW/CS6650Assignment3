package upic.consumer.repository;

import upic.consumer.model.LiftRideEvent;

import java.util.List;

public interface ResortRepository {
    void recordSkierVisit(int resortId, int skierId, int seasonId, int dayId);
    int getUniqueSkiersCount(int resortId, int dayId, int seasonId);

    // Added batch method
    void recordSkierVisitBatch(List<LiftRideEvent> events);
}