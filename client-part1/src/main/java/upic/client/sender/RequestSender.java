package upic.client.sender;

import com.google.gson.Gson;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import upic.client.config.ClientConfig;
import upic.client.model.LiftRideEvent;

/**
 * Improved RequestSender with better error handling and connection management
 */
public class RequestSender implements Runnable {
  private final BlockingQueue<LiftRideEvent> queue;
  private final int requestCount;
  private final Gson gson = new Gson();
  private final LongAdder successCount;
  private final LongAdder failureCount;
  private final LongAdder timeoutCount = new LongAdder();
  private final LongAdder retryCount = new LongAdder();

  // Improved HTTP client with better connection management
  private final HttpClient client;

  private final HttpRequest.Builder requestTemplate;

  public RequestSender(BlockingQueue<LiftRideEvent> queue, int requestCount, LongAdder successCount,
                       LongAdder failureCount
  ) {
    this.queue = queue;
    this.requestCount = requestCount;
    this.successCount = successCount;
    this.failureCount = failureCount;

    // Create an optimized HttpClient
    this.client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(ClientConfig.CONNECTION_TIMEOUT_SECONDS))
            .executor(Executors.newFixedThreadPool(ClientConfig.CONNECTION_POOL_SIZE))
            .version(ClientConfig.USE_HTTP2 ? HttpClient.Version.HTTP_2 : HttpClient.Version.HTTP_1_1)
            .build();

    this.requestTemplate = HttpRequest.newBuilder()
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(ClientConfig.REQUEST_TIMEOUT_SECONDS));
  }

  @Override
  public void run() {
    try {
      ArrayList<Future<?>> response = new ArrayList<>();
      for (int i = 0; i < requestCount; i++) {
        if(queue.isEmpty()){
//          System.out.println("Queue is empty");
          return;
        }
        LiftRideEvent event = queue.take();

        sendRequest(event);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private boolean sendRequest(LiftRideEvent event) {
    String json = gson.toJson(event);
    String url = String.format("%s/resorts/%d/seasons/%d/days/%d/skiers/%d",
            ClientConfig.BASE_URL,
            event.getResortId(),
            event.getSeasonId(),
            event.getDayId(),
            event.getSkierId());

    HttpRequest request = requestTemplate.copy()
            .uri(URI.create(url))
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();

    for (int attempt = 0; attempt < ClientConfig.MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

        // Check response status code
        if (response.statusCode() == 201 || response.statusCode() == 200) {
          successCount.increment();
          return true;
        }

        // Differentiate between client and server errors
        if (response.statusCode() >= 400 && response.statusCode() < 500) {
          // Client error, no need to retry
          if (Math.random() < 0.01) { // Only log 1% of errors to reduce noise
            System.out.println("Client error (" + response.statusCode() + "): " + response.body());
          }
          failureCount.increment();
          return false;
        }

        // Server error, should retry
        if (Math.random() < 0.05) { // Only log 5% of errors
          System.out.println("Client-request Sender: Server error (" + response.statusCode() + "), retrying...");
        }

      } catch (java.net.http.HttpTimeoutException e) {
        // Handle timeout exceptions separately
        timeoutCount.increment();
        if (Math.random() < 0.01) { // Only log 1% of timeouts
          System.out.println("Request timed out, retrying...");
        }
      } catch (Exception e) {
        // Other exceptions
        if (Math.random() < 0.01) { // Only log 1% of exceptions
          System.out.println("Client request failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
      }

      // Just retry immediately without any delay
      if (attempt < ClientConfig.MAX_RETRY_ATTEMPTS - 1) {
        retryCount.increment();
      }
    }
    failureCount.increment();
    return false;
  }

  // Get statistics
  public long getTimeoutCount() {
    return timeoutCount.sum();
  }

  public long getRetryCount() {
    return retryCount.sum();
  }

  /**
   * Request throttler to limit request rate
   */
  static class RequestThrottler {
    private final int requestsPerSecond;
    private final long nanosPerRequest;
    private long lastRequestTime = System.nanoTime();

    public RequestThrottler(int requestsPerSecond) {
      this.requestsPerSecond = requestsPerSecond;
      this.nanosPerRequest = 1_000_000_000L / requestsPerSecond;
    }

    public void throttle() throws InterruptedException {
      long now = System.nanoTime();
      long elapsed = now - lastRequestTime;

      if (elapsed < nanosPerRequest) {
        long sleepTime = (nanosPerRequest - elapsed) / 1_000_000;
        if (sleepTime > 0) {
          Thread.sleep(sleepTime);
        }
      }

      lastRequestTime = System.nanoTime();
    }
  }
}