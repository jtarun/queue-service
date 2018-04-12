package queue.clock;

/**
 * Concrete implementation of IClock interface providing System clock time.
 */
public class SystemClock implements IClock {

  @Override
  public long getCurrentTimestampMs() {
    return System.currentTimeMillis();
  }

  @Override
  public void setCurrentTimestampMs(long timestamp) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("System clock is not allowed to be set.");
  }

  @Override
  public void waitFor(long timeInMs) throws InterruptedException {
    Thread.sleep(timeInMs);
  }
}
