package queue.model;

import queue.exception.QueueServiceException;

public class MessagePullRequest {
  private static final int DEFAULT_VISIBILITY_TIMEOUT_SEC = 30;

  private String uri;
  private int visibilityTimeout;

  public MessagePullRequest() {
    this.uri = null;
    this.visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT_SEC;
  }

  public MessagePullRequest withUri(String uri) {
    this.uri = uri;
    return this;
  }

  public MessagePullRequest withVisibilityTimeout(int timeoutInSec) {
    this.visibilityTimeout = timeoutInSec;
    return this;
  }

  public String getUri() {
    return uri;
  }

  public int getVisibilityTimeout() {
    return visibilityTimeout;
  }

  public static void validate(MessagePullRequest pullRequest) throws QueueServiceException {
    if (pullRequest == null) {
      throw new QueueServiceException("Request param is null");
    }

    if (pullRequest.getUri() == null || pullRequest.getUri().isEmpty()) {
      throw new QueueServiceException("Queue name is null or empty");
    }

    if (pullRequest.getVisibilityTimeout() < 30) {
      throw new QueueServiceException("Visibility timeout cannot be less than 30 seconds");
    }
  }

}
