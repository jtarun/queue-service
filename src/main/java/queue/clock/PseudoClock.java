package queue.clock;


/**
 * Custom implementation of IClock interface, which is useful during testing.
 */
public class PseudoClock implements IClock {
  private long timestamp;

  public PseudoClock() {
    this.timestamp = 0;
  }

  public PseudoClock(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public long getCurrentTimestampMs() {
    return timestamp;
  }

  @Override
  public void setCurrentTimestampMs(long timestamp) throws UnsupportedOperationException {
    this.timestamp = timestamp;
  }

  @Override
  public void waitFor(long timeInMs) throws InterruptedException {
    this.timestamp += timeInMs;
  }
}
