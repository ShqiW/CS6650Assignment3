package upic.client.producer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import upic.client.model.LiftRideEvent;

public class EventGenerator implements Runnable {
  private final BlockingQueue<LiftRideEvent> queue;
  private volatile boolean running = true;
  private int requestCount = 0;

  public EventGenerator(BlockingQueue<LiftRideEvent> queue, int requestCount) {
    this.queue = queue;
    this.requestCount =requestCount;
  }

  @Override
  public void run() {

    for (int i = 0; i < requestCount; i++)
    {
      try {
        queue.put(generateEvent());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private LiftRideEvent generateEvent() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    return new LiftRideEvent(
        random.nextInt(1, 100001),  // skierId
        random.nextInt(1, 11),      // resortId
        random.nextInt(1, 41),      // liftId
        2025,                       // seasonId
        1,                          // dayId
        random.nextInt(1, 361)      // time
    );
  }

  public void stop() {
    running = false;
  }
}