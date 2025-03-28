package upic.consumer.repository;

import upic.consumer.model.LiftRideEvent;

import java.util.List;

public interface ResortRepository {
    /**
     * Record a skier visit to a resort
     */
    void recordSkierVisit(int resortId, int skierId, int seasonId, int dayId);

    /**
     * Record a batch of skier visits
     */
    void recordSkierVisitBatch(List<LiftRideEvent> events);

    /**
     * Get the count of unique skiers at a resort on a specific day
     */
    int getUniqueSkiersCount(int resortId, int dayId, int seasonId);
}