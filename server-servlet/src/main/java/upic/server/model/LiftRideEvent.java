package upic.server.model;

public class LiftRideEvent {
  private int skierId;
  private int resortId;
  private int liftId;
  private int seasonId;
  private int dayId;
  private int time;

  // Default constructor needed for GSON
  public LiftRideEvent() {}

  public LiftRideEvent(int skierId, int resortId, int liftId,
      int seasonId, int dayId, int time) {
    this.skierId = skierId;
    this.resortId = resortId;
    this.liftId = liftId;
    this.seasonId = seasonId;
    this.dayId = dayId;
    this.time = time;
  }

  // Getters and setters
  public int getSkierId() { return skierId; }
  public void setSkierId(int skierId) { this.skierId = skierId; }
  public int getResortId() { return resortId; }
  public void setResortId(int resortId) { this.resortId = resortId; }
  public int getLiftId() { return liftId; }
  public void setLiftId(int liftId) { this.liftId = liftId; }
  public int getSeasonId() { return seasonId; }
  public void setSeasonId(int seasonId) { this.seasonId = seasonId; }
  public int getDayId() { return dayId; }
  public void setDayId(int dayId) { this.dayId = dayId; }
  public int getTime() { return time; }
  public void setTime(int time) { this.time = time; }

  @Override
  public String toString() {
    return "LiftRideEvent{" +
        "skierId=" + skierId +
        ", resortId=" + resortId +
        ", liftId=" + liftId +
        ", seasonId=" + seasonId +
        ", dayId=" + dayId +
        ", time=" + time +
        '}';
  }

  }

