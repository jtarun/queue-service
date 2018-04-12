package queue.model;

import queue.exception.QueueServiceException;

public class MessageDeleteRequest {
  private Message message;

  public MessageDeleteRequest() {
    this.message = null;
  }

  public MessageDeleteRequest(Message message) {
    this.message = message;
  }

  public MessageDeleteRequest withMessage(Message message) {
    this.message = message;
    return this;
  }

  public Message getMessage() {
    return message;
  }

  public static void validate(MessageDeleteRequest deleteRequest)
      throws QueueServiceException {

    if (deleteRequest == null) {
      throw new QueueServiceException("Request param is null");
    }

    if (deleteRequest.getMessage() == null) {
      throw new QueueServiceException("Message is null");
    }

    if (deleteRequest.getMessage().getHandle() == null
        || deleteRequest.getMessage().getHandle().isEmpty()) {
      throw new QueueServiceException("Message handle is null");
    }

  }
}
