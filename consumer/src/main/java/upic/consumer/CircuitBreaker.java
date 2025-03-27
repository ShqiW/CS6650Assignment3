package upic.consumer;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import upic.consumer.config.CircuitBreakerConfig;

/**
 * Generic circuit breaker implementation to improve system resilience.
 * Prevents cascading failures by automatically detecting failures and
 * temporarily blocking operations that are likely to fail.
 */
public class CircuitBreaker {
    // Circuit breaker states
    public enum State {
        CLOSED,     // Normal operation, requests pass through
        OPEN,       // Circuit is tripped, requests are blocked
        HALF_OPEN   // Testing if the system has recovered
    }

    private final CircuitBreakerConfig config;
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicReference<LocalDateTime> lastStateChange = new AtomicReference<>(LocalDateTime.now());
    private final AtomicReference<LocalDateTime> lastFailure = new AtomicReference<>(LocalDateTime.now());
    private final AtomicReference<LocalDateTime> lastSuccess = new AtomicReference<>(LocalDateTime.now());
    private final LongAdder failureCount = new LongAdder();
    private final LongAdder successCount = new LongAdder();
    private final LongAdder consecutiveFailures = new LongAdder();

    // Counters for metrics reporting
    private final LongAdder totalTrips;
    private final LongAdder successfulRetries;

    /**
     * Creates a new circuit breaker with the given configuration.
     *
     * @param config The circuit breaker configuration
     * @param totalTripsCounter Counter for total circuit trips
     * @param successfulRetriesCounter Counter for successful retries
     */
    public CircuitBreaker(CircuitBreakerConfig config, LongAdder totalTripsCounter, LongAdder successfulRetriesCounter) {
        this.config = config;
        this.totalTrips = totalTripsCounter;
        this.successfulRetries = successfulRetriesCounter;
    }

    /**
     * Execute a function with circuit breaker protection.
     *
     * @param supplier The function to execute
     * @param <T> The return type of the function
     * @return The result of the function
     * @throws RuntimeException if the circuit is open or the function throws an exception
     */
    public <T> T execute(Supplier<T> supplier) {
        // Check if circuit is open
        if (isOpen()) {
            // If it's time to try again, transition to half-open
            if (shouldAttemptReset()) {
                transitionTo(State.HALF_OPEN);
            } else {
                // Otherwise, fail fast
                throw new RuntimeException("Circuit breaker is open for " + config.getName());
            }
        }

        try {
            // Execute the function
            T result = supplier.get();

            // Handle success
            onSuccess();
            return result;
        } catch (Exception e) {
            // Handle failure
            onFailure();
            throw e;
        }
    }

    /**
     * Handle a successful execution.
     */
    private void onSuccess() {
        successCount.increment();
        lastSuccess.set(LocalDateTime.now());

        // Reset consecutive failures
        consecutiveFailures.reset();

        // If we're in half-open state and this succeeded, close the circuit
        if (state.get() == State.HALF_OPEN) {
            transitionTo(State.CLOSED);
            successfulRetries.increment();
        }
    }

    /**
     * Handle a failed execution.
     */
    private void onFailure() {
        failureCount.increment();
        consecutiveFailures.increment();
        lastFailure.set(LocalDateTime.now());

        // If we have too many consecutive failures, open the circuit
        if (consecutiveFailures.sum() >= config.getFailureThreshold()) {
            if (state.get() != State.OPEN) {
                transitionTo(State.OPEN);
                totalTrips.increment();
            }
        }
    }

    /**
     * Check if the circuit is currently open (preventing executions).
     */
    public boolean isOpen() {
        return state.get() == State.OPEN;
    }

    /**
     * Check if it's time to attempt to reset the circuit.
     */
    private boolean shouldAttemptReset() {
        if (state.get() != State.OPEN) {
            return false;
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime changeTime = lastStateChange.get();

        // Check if the reset timeout has elapsed
        return now.isAfter(changeTime.plusNanos(config.getResetTimeoutMs() * 1_000_000));
    }

    /**
     * Transition to a new state.
     */
    private void transitionTo(State newState) {
        State oldState = state.getAndSet(newState);
        if (oldState != newState) {
            lastStateChange.set(LocalDateTime.now());
            System.out.println("Circuit breaker " + config.getName() + " state changed from " +
                    oldState + " to " + newState);

            // Reset consecutive failures on transition to HALF_OPEN
            if (newState == State.HALF_OPEN) {
                consecutiveFailures.reset();
            }
        }
    }

    /**
     * Get the current state of the circuit breaker.
     */
    public State getState() {
        return state.get();
    }

    /**
     * Get the number of failures since the circuit breaker was created.
     */
    public long getFailureCount() {
        return failureCount.sum();
    }

    /**
     * Get the number of successes since the circuit breaker was created.
     */
    public long getSuccessCount() {
        return successCount.sum();
    }

    /**
     * Reset the circuit breaker to its initial state.
     */
    public void reset() {
        state.set(State.CLOSED);
        lastStateChange.set(LocalDateTime.now());
        consecutiveFailures.reset();
    }
}