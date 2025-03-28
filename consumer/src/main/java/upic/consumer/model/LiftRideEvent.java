package upic.consumer.model;

public class LiftRideEvent {
    private int skierId;
    private int resortId;
    private int liftId;
    private int seasonId;
    private int dayId;
    private int time;

    // Default constructor needed for GSON
    public LiftRideEvent() {
    }

    public LiftRideEvent(int skierId, int resortId, int liftId,
            int seasonId, int dayId, int time) {
        this.skierId = skierId;
        this.resortId = resortId;
        this.liftId = liftId;
        this.seasonId = seasonId;
        this.dayId = dayId;
        this.time = time;
    }

    public LiftRideEvent(String message) {
        String[] parts = message.split(",");
        if (parts.length != 6) {
            throw new IllegalArgumentException("Invalid message format");
        }
        this.skierId = Integer.parseInt(parts[0]);
        this.resortId = Integer.parseInt(parts[1]);
        this.liftId = Integer.parseInt(parts[2]);
        this.seasonId = Integer.parseInt(parts[3]);
        this.dayId = Integer.parseInt(parts[4]);
        this.time = Integer.parseInt(parts[5]);
    }

    // Getters and setters
    public int getSkierId() {
        return skierId;
    }

    public void setSkierId(int skierId) {
        this.skierId = skierId;
    }

    public int getResortId() {
        return resortId;
    }

    public void setResortId(int resortId) {
        this.resortId = resortId;
    }

    public int getLiftId() {
        return liftId;
    }

    public void setLiftId(int liftId) {
        this.liftId = liftId;
    }

    public int getSeasonId() {
        return seasonId;
    }

    public void setSeasonId(int seasonId) {
        this.seasonId = seasonId;
    }

    public int getDayId() {
        return dayId;
    }

    public void setDayId(int dayId) {
        this.dayId = dayId;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return skierId + "," + resortId + "," + liftId + "," + seasonId + "," + dayId + "," + time;
    }

}