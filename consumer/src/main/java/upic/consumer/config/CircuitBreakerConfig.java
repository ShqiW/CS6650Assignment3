package upic.consumer.config;

/**
 * Configuration for a circuit breaker.
 */
public class CircuitBreakerConfig {
    private final String name;
    private final int failureThreshold;
    private final long resetTimeoutMs;

    /**
     * Creates a new circuit breaker configuration.
     *
     * @param name The name of the circuit breaker
     * @param failureThreshold The number of consecutive failures before opening the circuit
     * @param resetTimeoutMs The time in milliseconds before attempting to reset the circuit
     */
    public CircuitBreakerConfig(String name, int failureThreshold, long resetTimeoutMs) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
    }

    public String getName() {
        return name;
    }

    public int getFailureThreshold() {
        return failureThreshold;
    }

    public long getResetTimeoutMs() {
        return resetTimeoutMs;
    }
}