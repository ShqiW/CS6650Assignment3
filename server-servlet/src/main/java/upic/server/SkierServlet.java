package upic.server;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import upic.server.config.ServerConfig;
import upic.server.messaging.RabbitMQPublisher;
import upic.server.model.ErrorResponse;
import upic.server.model.LiftRideEvent;
import upic.server.model.SuccessResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(urlPatterns = "/skiers/*", asyncSupported = true)
public class SkierServlet extends HttpServlet {
  private final Gson gson = new Gson();
  private RabbitMQPublisher publisher;
  private ExecutorService executorService;
  private ScheduledExecutorService scheduledExecutor;

  // Performance metrics
  private final LongAdder requestsReceived = new LongAdder();
  private final LongAdder requestsProcessed = new LongAdder();
  private final LongAdder requestsFailed = new LongAdder();
  private final LongAdder messagesSent = new LongAdder();
  private final LongAdder messagingErrors = new LongAdder();
  private final LongAdder validationErrors = new LongAdder();

  // Health metrics
  private final Runtime runtime = Runtime.getRuntime();
  private static final long STATS_INTERVAL_MS = 60000; // 1 minute

  @Override
  public void init() throws ServletException {
    super.init();

    try {
      // Load configuration
      int corePoolSize = ServerConfig.CORE_POOL_SIZE;
      int channelPoolSize = ServerConfig.RABBITMQ_CHANNEL_POOL_SIZE;

      // Initialize RabbitMQ publisher
      try {
        publisher = new RabbitMQPublisher(channelPoolSize);
        getServletContext().log("RabbitMQ publisher initialized");
      } catch (Exception e) {
        getServletContext().log("Failed to initialize RabbitMQ publisher: " + e.getMessage(), e);
        throw new ServletException("Failed to initialize RabbitMQ publisher", e);
      }

      // Initialize thread pools - use optimal thread count
      int optimalThreads = ServerConfig.getOptimalThreadCount();
      getServletContext().log("Using optimal thread count: " + optimalThreads);

      executorService = Executors.newFixedThreadPool(optimalThreads);
      scheduledExecutor = Executors.newScheduledThreadPool(2);

      // Schedule stats reporting
      scheduledExecutor.scheduleAtFixedRate(
              this::reportStats,
              STATS_INTERVAL_MS,
              STATS_INTERVAL_MS,
              TimeUnit.MILLISECONDS
      );

      getServletContext().log("SkierServlet initialized successfully");

    } catch (Exception e) {
      getServletContext().log("Initialization failed: " + e.getMessage());
      throw new ServletException("Cannot initialize servlet", e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res)
          throws IOException {
    res.setContentType("application/json");
    PrintWriter out = res.getWriter();

    // Create JSON response
    String jsonResponse = "{"
            + "\"message\": \"Welcome to Ski Resort API\","
            + "\"usage\": \"Please use POST method to submit ski lift rides\","
            + "\"example\": {"
            + "    \"skierId\": 123,"
            + "    \"resortId\": 5,"
            + "    \"liftId\": 15,"
            + "    \"seasonId\": 2025,"
            + "    \"dayId\": 1,"
            + "    \"time\": 217"
            + "  }"
            + "}";

    out.print(jsonResponse);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
          throws ServletException, IOException {
    // Increment request counter
    requestsReceived.increment();

    // Start async processing
    final AsyncContext asyncContext = req.startAsync();
    asyncContext.setTimeout(ServerConfig.ASYNC_TIMEOUT); // configurable timeout

    // Process request asynchronously
    executorService.submit(() -> {
      try {
        processRequest(asyncContext);
        requestsProcessed.increment();
      } catch (Exception e) {
        handleProcessingError(asyncContext, e);
        requestsFailed.increment();
      }
    });
  }

  private void processRequest(AsyncContext asyncContext) throws IOException {
    HttpServletRequest req = (HttpServletRequest) asyncContext.getRequest();
    HttpServletResponse resp = (HttpServletResponse) asyncContext.getResponse();
    resp.setContentType("application/json");
    PrintWriter writer = resp.getWriter();

    String requestId = UUID.randomUUID().toString();

    // Validate URL path
    String urlPath = req.getPathInfo();
    int urlResortId = 0, urlSeasonId = 0, urlDayId = 0, urlSkierId = 0;

    if (urlPath != null && urlPath.startsWith("/")) {
      String[] parts = urlPath.substring(1).split("/");
      if (parts.length == 8) {
        try {
          urlResortId = Integer.parseInt(parts[1]);
          urlSeasonId = Integer.parseInt(parts[3]);
          urlDayId = Integer.parseInt(parts[5]);
          urlSkierId = Integer.parseInt(parts[7]);
        } catch (NumberFormatException ignored) {
          // Will be handled by isUrlValid
        }
      }
    }

    boolean isValid = isUrlValid(urlPath);
    if (!isValid) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ErrorResponse error = new ErrorResponse("Invalid URL path", HttpServletResponse.SC_BAD_REQUEST);
      writer.write(gson.toJson(error));
      validationErrors.increment();
      asyncContext.complete();
      return;
    }

    // Parse and validate JSON payload
    BufferedReader reader = req.getReader();
    LiftRideEvent liftRide;

    try {
      liftRide = gson.fromJson(reader, LiftRideEvent.class);

      // Handle null liftRide (which can happen if JSON is valid but empty)
      if (liftRide == null) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        ErrorResponse error = new ErrorResponse("Empty or invalid JSON payload", HttpServletResponse.SC_BAD_REQUEST);
        writer.write(gson.toJson(error));
        validationErrors.increment();
        asyncContext.complete();
        return;
      }
    } catch (JsonSyntaxException e) {
      // CHANGED: Explicitly handle JSON parsing errors
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ErrorResponse error = new ErrorResponse("Invalid JSON format: " + e.getMessage(), HttpServletResponse.SC_BAD_REQUEST);
      writer.write(gson.toJson(error));
      validationErrors.increment();
      asyncContext.complete();
      return;
    }

    if (!isValidLiftRide(liftRide)) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ErrorResponse error = new ErrorResponse("Invalid lift ride data", HttpServletResponse.SC_BAD_REQUEST);
      writer.write(gson.toJson(error));
      validationErrors.increment();
      asyncContext.complete();
      return;
    }

    // Validate URL parameters match JSON body
    if (liftRide.getResortId() != urlResortId ||
            liftRide.getSeasonId() != urlSeasonId ||
            liftRide.getDayId() != urlDayId ||
            liftRide.getSkierId() != urlSkierId) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ErrorResponse error = new ErrorResponse(
              "Mismatch between URL parameters and JSON body",
              HttpServletResponse.SC_BAD_REQUEST);
      writer.write(gson.toJson(error));
      validationErrors.increment();
      asyncContext.complete();
      return;
    }

    try {
      // Send message to RabbitMQ
      String messageBody = gson.toJson(liftRide);
      boolean publishSuccess = publisher.publishMessage(requestId, messageBody);

      if (publishSuccess) {
        messagesSent.increment();

        // Return success response
        resp.setStatus(HttpServletResponse.SC_CREATED);
        SuccessResponse success = new SuccessResponse("Lift ride recorded successfully, request ID: " + requestId);
        writer.write(gson.toJson(success));
      } else {
        // Handle publish failure
        messagingErrors.increment();
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        ErrorResponse error = new ErrorResponse(
                "Failed to publish message to queue",
                HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        writer.write(gson.toJson(error));
      }

      // Complete async context
      asyncContext.complete();

    } catch (Exception e) {
      // Handle other exceptions
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      ErrorResponse error = new ErrorResponse("Server error: " + e.getMessage(),
              HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      writer.write(gson.toJson(error));
      asyncContext.complete();
    }
  }

  private void handleProcessingError(AsyncContext asyncContext, Exception e) {
    try {
      HttpServletResponse resp = (HttpServletResponse) asyncContext.getResponse();
      resp.setContentType("application/json");
      PrintWriter writer = resp.getWriter();

      System.err.println("Error processing request: " + e.getMessage());
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      ErrorResponse error = new ErrorResponse("Server error: " + e.getMessage(),
              HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      writer.write(gson.toJson(error));
    } catch (IOException ioe) {
      System.err.println("Error writing error response: " + ioe.getMessage());
    } finally {
      asyncContext.complete();
    }
  }

  private void reportStats() {
    long received = requestsReceived.sum();
    long processed = requestsProcessed.sum();
    long failed = requestsFailed.sum();
    long sent = messagesSent.sum();
    long messaging_errors = messagingErrors.sum();
    long validation_errors = validationErrors.sum();
    long usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    long maxMemoryMB = runtime.maxMemory() / (1024 * 1024);

    getServletContext().log(String.format(
            "STATS: Received=%d, Processed=%d, Failed=%d, Sent=%d, Messaging_Errors=%d, Validation_Errors=%d, Memory=%dMB/%dMB",
            received, processed, failed, sent, messaging_errors, validation_errors, usedMemoryMB, maxMemoryMB
    ));
  }

  private boolean isValidLiftRide(LiftRideEvent liftRide) {
    if (liftRide == null) {
      return false;
    }
    return liftRide.getSkierId() > 0 && liftRide.getSkierId() <= ServerConfig.MAX_SKIER_ID &&
            liftRide.getResortId() > 0 && liftRide.getResortId() <= ServerConfig.MAX_RESORT_ID &&
            liftRide.getLiftId() > 0 && liftRide.getLiftId() <= ServerConfig.MAX_LIFT_ID &&
            liftRide.getSeasonId() == ServerConfig.SEASON_ID &&
            liftRide.getDayId() == ServerConfig.MAX_DAY_ID &&
            liftRide.getTime() > 0 && liftRide.getTime() <= ServerConfig.MAX_TIME;
  }

  private boolean isUrlValid(String urlPath) {
    // Expected format: /resorts/{resortId}/seasons/{seasonId}/days/{dayId}/skiers/{skierId}
    if (urlPath == null || urlPath.isEmpty()) {
      return false;
    }

    if (urlPath.startsWith("/")) {
      urlPath = urlPath.substring(1);
    }

    String[] urlParts = urlPath.split("/");

    if (urlParts.length != 8) {
      return false;
    }

    if (!urlParts[0].equals("resorts") ||
            !urlParts[2].equals("seasons") ||
            !urlParts[4].equals("days") ||
            !urlParts[6].equals("skiers")) {
      return false;
    }

    try {
      int resortId = Integer.parseInt(urlParts[1]);
      int seasonId = Integer.parseInt(urlParts[3]);
      int dayId = Integer.parseInt(urlParts[5]);
      int skierId = Integer.parseInt(urlParts[7]);

      return resortId > 0 && resortId <= ServerConfig.MAX_RESORT_ID &&
              seasonId == ServerConfig.SEASON_ID &&
              dayId == ServerConfig.MAX_DAY_ID &&
              skierId > 0 && skierId <= ServerConfig.MAX_SKIER_ID;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Override
  public void destroy() {
    // Shutdown publisher
    if (publisher != null) {
      publisher.shutdown();
      getServletContext().log("RabbitMQ publisher closed");
    }

    // Shutdown thread pools
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }
    }

    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
      try {
        if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduledExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduledExecutor.shutdownNow();
      }
    }

    super.destroy();
  }
}