package queue.exception;

public class QueueServiceException extends Exception {

  private static final long serialVersionUID = 1L;

  public QueueServiceException() {
    super();
  }

  public QueueServiceException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueueServiceException(String message) {
    super(message);
  }

}
