package upic.client.model;
public class LiftRideEvent {
  private int skierId;
  private int resortId;
  private int liftId;
  private int seasonId;
  private int dayId;
  private int time;

  public LiftRideEvent(int skierId, int resortId, int liftId, int seasonId, int dayId, int time) {
    this.skierId = skierId;
    this.resortId = resortId;
    this.liftId = liftId;
    this.seasonId = seasonId;
    this.dayId = dayId;
    this.time = time;
  }

  // Getters and setters
  public int getSkierId() { return skierId; }
  public int getResortId() { return resortId; }
  public int getLiftId() { return liftId; }
  public int getSeasonId() { return seasonId; }
  public int getDayId() { return dayId; }
  public int getTime() { return time; }
}
