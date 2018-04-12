package queue.model;


public class Message {
  private String content;
  private String handle;
  private String uri;
  private long visibilityTimestamp;

  public Message() {
  }

  public Message(String uri, String handle, String content, long visibilityTimestamp) {
    this.uri = uri;
    this.handle = handle;
    this.content = content;
    this.visibilityTimestamp = visibilityTimestamp;
  }

  public Message withContent(String content) {
    this.content = content;
    return this;
  }

  public Message withUri(String uri) {
    this.uri = uri;

    return this;
  }

  public Message withHandle(String handle) {
    this.handle = handle;
    return this;
  }

  public Message withVisibilityTimestamp(long visiblityTimestamp) {
    this.visibilityTimestamp = visiblityTimestamp;
    return this;
  }

  public String getContent() {
    return content;
  }

  public String getHandle() {
    return handle;
  }

  public String getUri() {
    return uri;
  }

  public long getVisibilityTimestamp() {
    return visibilityTimestamp;
  }

  public void setVisibilityTimestamp(long visibilityTimestamp) {
    this.visibilityTimestamp = visibilityTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Message message = (Message) o;

    if (visibilityTimestamp != message.visibilityTimestamp) {
      return false;
    }
    if (content != null ? !content.equals(message.content) : message.content != null) {
      return false;
    }
    if (handle != null ? !handle.equals(message.handle) : message.handle != null) {
      return false;
    }
    return uri != null ? uri.equals(message.uri) : message.uri == null;
  }

  @Override
  public int hashCode() {
    int result = content != null ? content.hashCode() : 0;
    result = 31 * result + (handle != null ? handle.hashCode() : 0);
    result = 31 * result + (uri != null ? uri.hashCode() : 0);
    result = 31 * result + (int) (visibilityTimestamp ^ (visibilityTimestamp >>> 32));
    return result;
  }
}
