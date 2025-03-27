package upic.client;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import upic.client.config.ClientConfig;
import upic.client.model.LiftRideEvent;
import upic.client.producer.EventGenerator;
import upic.client.sender.RequestSender;

public class SkiResortClient {
  private final BlockingQueue<LiftRideEvent> eventQueue;
//  private final BlockingQueue<Boolean> resultQueue;
  private final LongAdder successCount = new LongAdder();
  private final LongAdder failureCount = new LongAdder();
  private final ExecutorService executor;
  private final int initialThreads;
  private final int totalRequests;
  private final int requestsPerThread;

  // Performance tracking variables
  private final LongAdder totalLatency = new LongAdder();
  private final AtomicInteger completedRequests = new AtomicInteger(0);
  private volatile boolean printStats = true;

  public SkiResortClient(int initialThreads, int totalRequests, int requestsPerThread) {
    // Load configuration from system properties or use defaults
//    this.initialThreads = Integer.parseInt(
//            System.getProperty("client.initialThreads", String.valueOf(ClientConfig.INITIAL_THREADS)));
//    this.totalRequests = Integer.parseInt(
//            System.getProperty("client.totalRequests", String.valueOf(ClientConfig.TOTAL_REQUESTS)));
//    this.requestsPerThread = Integer.parseInt(
//            System.getProperty("client.requestsPerThread", String.valueOf(ClientConfig.REQUESTS_PER_THREAD)));


    this.initialThreads = initialThreads;
    this.totalRequests = totalRequests;
    this.requestsPerThread = requestsPerThread;


    // Create event queue
    this.eventQueue = new LinkedBlockingQueue<>(totalRequests);
//    this.resultQueue = new LinkedBlockingQueue<>(totalRequests);
    // Create thread pool with custom thread factory for better naming
    this.executor = Executors.newCachedThreadPool(new ThreadFactory() {
      private final AtomicInteger counter = new AtomicInteger(0);
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("request-sender-" + counter.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      }
    });



    // Start stats reporting thread
//    startStatsReportingThread();
  }

  public void start() {
    System.out.println("Starting client with improved configuration...");
    System.out.println("Configuration:");
    System.out.println(" - Initial Threads: " + initialThreads);
    System.out.println(" - Total Requests: " + totalRequests);
    System.out.println(" - Requests Per Thread: " + requestsPerThread);
    System.out.println(" - Queue Size: " + ClientConfig.QUEUE_SIZE);
    System.out.println(" - Max Retry Attempts: " + ClientConfig.MAX_RETRY_ATTEMPTS);
    System.out.println(" - Connection Timeout: " + ClientConfig.CONNECTION_TIMEOUT_SECONDS + "s");


    // Start event generator
    EventGenerator generator = new EventGenerator(eventQueue,totalRequests);
    Thread generatorThread = new Thread(generator, "event-generator");
    generatorThread.setDaemon(true);
    generatorThread.start();

    System.out.println("Started EventGenerator");
      try {
          generatorThread.join();
      } catch (InterruptedException e) {
          throw new RuntimeException(e);
      }
    System.out.println("EventGenerator filled "+eventQueue.size()+" events");
    long startTime = System.currentTimeMillis();

      // Initial phase with initialThreads
    System.out.println("Starting initial phase with " + initialThreads + " threads");
    Future<?>[] tasks = new Future<?>[initialThreads];

    for (int i = 0; i < initialThreads; i++) {
      tasks[i]=executor.submit(new RequestSender(
              eventQueue,
              requestsPerThread,
              successCount,
              failureCount
      ));
    }
    System.out.println("Submited initial phase with " + initialThreads + " threads");

    CompletableFuture<?>[] completableTasks = Arrays.stream(tasks)
            .map(future -> CompletableFuture.supplyAsync(() -> {
              try {
                future.get(); // Wait for this task to complete
                return true;
              } catch (Exception e) {
                return false;
              }
            }))
            .toArray(CompletableFuture[]::new);

// This will complete when any of the tasks complete
    try {
      CompletableFuture.anyOf(completableTasks).get();
      System.out.println("At least one task has completed!");
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("Error waiting for tasks: " + e.getMessage());
      Thread.currentThread().interrupt();
    }


//    Arrays.stream(tasks).forEach(task -> {
//      try {
//        task.get();
//      } catch (InterruptedException | ExecutionException e) {
//        System.out.println("Error waiting for task completion: " + e.getMessage());
//        Thread.currentThread().interrupt();
//      }
//    });

    try {
//      initialLatch.await();

      System.out.println("Initial phase completed");

      // Calculate remaining requests
      int completedRequests = initialThreads * requestsPerThread;
      int remainingRequests = totalRequests - completedRequests;

      if (remainingRequests > 0) {
        // Calculate optimal thread count based on system resources
        int additionalThread = ClientConfig.ADDITIONAL_THREAD;
//        int additionalThread = ClientConfig.getOptimalThreadCount();
        int requestsPerRemaining = remainingRequests / additionalThread;
//
//        // Ensure each thread gets at least 10 requests
//        if (requestsPerRemaining < 10) {
//          additionalThread = Math.max(1, remainingRequests / 10);
//          requestsPerRemaining = remainingRequests / additionalThread;
//        }
//        additionalThread=32;
        System.out.println(" - Remaining Requests: " + remainingRequests);
        System.out.println(" - Additional Threads: " + additionalThread);
        System.out.println(" - Requests Per Additional Thread: " + remainingRequests / additionalThread);

        tasks = new Future[additionalThread];
        for (int i = 0; i < additionalThread; i++) {
//          System.out.println("Creating thread "+i);
          tasks[i]=executor.submit(new RequestSender(
                  eventQueue,
                  requestsPerRemaining,
                  successCount,
                  failureCount
          ));
        }
        Arrays.stream(tasks).forEach(task -> {
          try {
            task.get();
          } catch (InterruptedException e) {
            System.out.println("Error waiting for task completion: " + e.getMessage());
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
              throw new RuntimeException(e);
          }
        });
//        remainingLatch.await();
        System.out.println("Remaining phase completed");
      }
      else{
        System.out.println("No  more threads to complete initial phase");
      }
    } finally {

      System.out.println("Process finally block");
      // Stop stats thread and event generator
      printStats = false;
//      generator.stop();
//      generatorThread

      // Shutdown executor and wait for termination
      executor.shutdown();
      try {
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    long endTime = System.currentTimeMillis();
    printFinalResults(endTime - startTime);
  }

//  private void startStatsReportingThread() {
//    Thread statsThread = new Thread(() -> {
//      try {
//        long lastTime = System.currentTimeMillis();
//        long lastSuccessCount = 0;
//
//        while (printStats) {
//          Thread.sleep(5000); // Report every 5 seconds
//
//          long currentTime = System.currentTimeMillis();
//          long currentSuccessCount = successCount.sum();
//          long currentFailureCount = failureCount.sum();
//          long totalCount = currentSuccessCount + currentFailureCount;
//
//          long timeDelta = (currentTime - lastTime) / 1000;
//          long successDelta = currentSuccessCount - lastSuccessCount;
//          double currentRate = timeDelta > 0 ? successDelta / (double)timeDelta : 0;
//
//          System.out.println("\nCurrent Statistics:");
//          System.out.println("- Success: " + currentSuccessCount +
//                  " | Failures: " + currentFailureCount +
//                  " | Total: " + totalCount);
//          System.out.println("- Current rate: " + String.format("%.2f", currentRate) + " req/sec");
//          System.out.println("- Progress: " + String.format("%.1f%%", (totalCount * 100.0) / totalRequests));
//
//          lastTime = currentTime;
//          lastSuccessCount = currentSuccessCount;
//        }
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//      }
//    }, "stats-reporter");
//
//    statsThread.setDaemon(true);
//    statsThread.start();
//    System.out.println("Test quit or not");
//  }

  private void printFinalResults(long wallTime) {
    long successfulRequests = successCount.sum();
    long failedRequests = failureCount.sum();

    System.out.println("\nFinal Results:");
    System.out.println("Total Requests: " + totalRequests);
    System.out.println("Successful Requests: " + successfulRequests);
    System.out.println("Failed Requests: " + failedRequests);
    System.out.println("Wall Time: " + wallTime + " ms");
    double throughput = (successfulRequests * 1000.0) / wallTime;
    double successRate = (successfulRequests * 100.0) / totalRequests;
    System.out.println("Throughput: " + String.format("%.2f", throughput) + " requests/second");
    System.out.println("Success Rate: " + String.format("%.2f", successRate) + "%");

    if (completedRequests.get() > 0) {
      double avgLatency = totalLatency.sum() / (double)completedRequests.get();
      System.out.println("Average Latency: " + String.format("%.2f", avgLatency) + " ms/request");
    }

    // Print JVM stats
    Runtime runtime = Runtime.getRuntime();
    long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    long totalMemory = runtime.totalMemory() / (1024 * 1024);
    System.out.println("Memory Usage: " + usedMemory + "MB / " + totalMemory + "MB");
  }

  public static void main(String[] args) throws InterruptedException {
    // Run single thread benchmark first if requested
    boolean runBenchmark = true;
    int initialThreads = 32;
    int totalRequests = 50000;
    int requestsPerThread = 1000;

    System.out.println(Arrays.toString(args));

    if (args.length > 0) {
      try {
      if (args.length >= 1) {
        initialThreads = Integer.parseInt(args[0]);
      }
      if (args.length >= 2) {
        totalRequests = Integer.parseInt(args[1]);
      }
      if (args.length >= 3) {
        requestsPerThread = Integer.parseInt(args[2]);
      }
      if (args.length >= 4) {
        runBenchmark = Boolean.parseBoolean(args[3]);
      }
      } catch (NumberFormatException e) {
      System.out.println("Invalid arguments. Using default values.");
      }
    }
    
    System.out.println("Running parameters:");
    System.out.println(" - Initial Threads: " + initialThreads);
    System.out.println(" - Total Requests: " + totalRequests);
    System.out.println(" - Requests Per Thread: " + requestsPerThread);
    System.out.println(" - Run Benchmark: " + runBenchmark);

    if (runBenchmark) {
      new SingleThreadBenchmark().runBenchmark();
    }

    // Then run the improved client
    SkiResortClient client =new SkiResortClient(
            initialThreads,
            totalRequests,
            requestsPerThread
    );

    client.start();

    System.out.println("QUIT!!!!");

    System.exit(0);
  }
}