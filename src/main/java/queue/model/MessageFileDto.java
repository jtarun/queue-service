package queue.model;

public class MessageFileDto {
  private final String operation;
  private final long currentTimestamp;
  private final Message message;

  public MessageFileDto(String operation, long currentTimestamp, Message message) {
    this.operation = operation;
    this.currentTimestamp = currentTimestamp;
    this.message = message;
  }

  public String getOperation() {
    return operation;
  }

  public long getCurrentTimestamp() {
    return currentTimestamp;
  }

  public Message getMessage() {
    return message;
  }
}
