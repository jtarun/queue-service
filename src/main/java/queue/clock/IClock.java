package queue.clock;


/**
 * This is a clock interface that can be injected in time-dependent classes. This abstraction is
 * useful during testing with a custom clock.
 */
public interface IClock {

  long getCurrentTimestampMs();

  void setCurrentTimestampMs(long timestamp) throws UnsupportedOperationException;

  void waitFor(long timeInMs) throws InterruptedException ;
}
