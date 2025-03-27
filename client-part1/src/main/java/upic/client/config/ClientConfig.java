package upic.client.config;

public class ClientConfig {
    // Base URL pointing to the AWS load balancer
//    public static final String BASE_URL = System.getProperty("client.baseUrl", "http://35.91.156.128:8080/server-2.0-SNAPSHOT/skiers");
//    public static final String BASE_URL = System.getProperty("client.baseUrl", "http://cs6650-lb-2server-755356855.us-west-2.elb.amazonaws.com:8080/server-2.0-SNAPSHOT/skiers");
    public static final String BASE_URL = System.getProperty("client.baseUrl", "http://CS6650-LB-4server-302525209.us-west-2.elb.amazonaws.com:8080/server-2.0-SNAPSHOT/skiers");
//  public static final String BASE_URL = "http://35.91.119.93:8081/skiers"; // server-Springboot


  // Performance configuration
  public static final int TOTAL_REQUESTS = 50000;
  public static final int INITIAL_THREADS = 32;
  public static final int REQUESTS_PER_THREAD = 1000;
  public static final int MAX_RETRY_ATTEMPTS = 5;
  public static final int QUEUE_SIZE = 1000;

  // Connection configuration
  public static final int CONNECTION_TIMEOUT_SECONDS = 20;
  public static final int REQUEST_TIMEOUT_SECONDS = 15;
  public static final boolean USE_HTTP2 = true;
  public static final int CONNECTION_POOL_SIZE = 1;

  // Retry configuration
  public static final long INITIAL_BACKOFF_MS = 100;
  public static final long MAX_BACKOFF_MS = 5000;
  public static final double BACKOFF_MULTIPLIER = 2.0;

  // Rate limiting configuration
  public static final int REQUESTS_PER_SECOND_LIMIT = 500; //500-1000

  public static final int ADDITIONAL_THREAD = 100; //100-200

  // Dynamic thread calculation
  public static int getOptimalThreadCount() {
    int processors = Runtime.getRuntime().availableProcessors();
    long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);

    // Calculate based on CPU cores, allocating 8 threads per core
    int threadsByCpu = processors * 8;
//
    // Calculate based on available memory, allocating 1 thread per 5MB of heap
    int threadsByMemory = (int)(maxMemory / 5);

    // Use the minimum of both calculations, capped at 300 to avoid excessive connections
    return Math.min(300, Math.min(threadsByCpu, threadsByMemory));
//    return 400;
  }

  public String getBaseUrl(){return BASE_URL;}
  public int getTotalRequests(){return TOTAL_REQUESTS;}
  public int getInitialThreads(){return INITIAL_THREADS;}
  public int getRequestsPerThread(){return REQUESTS_PER_THREAD;}
  public int getMaxRetryAttempts(){return MAX_RETRY_ATTEMPTS;}
  public int getQueueSize(){return QUEUE_SIZE;}
}