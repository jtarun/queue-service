package queue.model;


import queue.exception.QueueServiceException;

public class MessagePushRequest {

  private String message;
  private String uri;
  private String handle;

  public MessagePushRequest() {
    this.message = null;
    this.uri = null;
  }

  public MessagePushRequest(String message, String uri) {
    this.message = message;
    this.uri = uri;
  }

  public MessagePushRequest withMessage(String message) {
    this.message = message;
    return this;
  }

  public MessagePushRequest withUri(String uri) {
    this.uri = uri;
    return this;
  }

  public MessagePushRequest withHandle(String handle) {
    this.handle = handle;
    return this;
  }

  public String getMessage() {
    return message;
  }

  public String getUri() {
    return uri;
  }

  public String getHandle() {
    return handle;
  }

  public static void validate(MessagePushRequest pushRequest)
      throws QueueServiceException {

    if (pushRequest == null) {
      throw new QueueServiceException("Request param is null");
    }

    if (pushRequest.getUri() == null || pushRequest.getUri().isEmpty()) {
      throw new QueueServiceException("Queue name is null or empty");
    }

    if (pushRequest.getMessage() == null || pushRequest.getMessage().isEmpty()) {
      throw new QueueServiceException("Message content is either null or empty");
    }
  }

}
