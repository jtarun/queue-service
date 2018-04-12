package queue;


import queue.clock.IClock;
import queue.clock.SystemClock;
import queue.exception.QueueServiceException;
import queue.model.Message;
import queue.model.MessageDeleteRequest;
import queue.model.MessagePullRequest;
import queue.model.MessagePushRequest;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;


/**
 * InMemoryQueueService maintains collection of main queues and corresponding in-flight queues
 * for each of these queues. When a message is pushed, it goes into one of the main queues
 * sorted in the order of visibilityTimestamp. When a message is pulled, the message with least
 * visibilityTimestamp is obtained from the main queue, its visibilityTimestamp is increased
 * by visibilityTimeout period and then it is moved to in-flight queue. A message is deleted
 * from in-flight queue if its visibilityTimestamp is more than current timestamp, otherwise it
 * is moved to main queue by a scheduler thread.
 */
public class InMemoryQueueService implements QueueService {

  private final IClock clock;
  private Map<String, PriorityBlockingQueue<Message>> queues = new ConcurrentHashMap<>();
  private Map<String, PriorityBlockingQueue<Message>> inFlightQueues = new ConcurrentHashMap<>();

  public InMemoryQueueService() {
    this.clock = new SystemClock();
  }

  public InMemoryQueueService(IClock clock) {
    this.clock = clock;
  }

  public void processInFlightQueue(String uri) {

    PriorityBlockingQueue<Message> inFlightQueue = inFlightQueues.get(uri);

    if (inFlightQueue == null || inFlightQueue.isEmpty()) {
      return;
    }

    PriorityBlockingQueue<Message> queue = queues.get(uri);

    // If visibility time of in-flight queue is over, remove and push it to main queue.
    for (Message message : inFlightQueue) {
      if (message.getVisibilityTimestamp() > clock.getCurrentTimestampMs()) {
        break;
      }
      if (inFlightQueue.remove(message)) {
        queue.add(message);
      }
    }

  }

  public void processAllInFlightQueues() {
    // move the messages from in-flight queue to main queue whose visibilityTs is expired.
    for (Map.Entry<String, PriorityBlockingQueue<Message>> inFlightQueueEntry :
        inFlightQueues.entrySet()) {

      String queue = inFlightQueueEntry.getKey();
      processInFlightQueue(queue);
    }
  }

  @Override
  public void push(MessagePushRequest request) throws QueueServiceException {
    MessagePushRequest.validate(request);

    // Delay feature can be easily added by adding the delay time to visibleAtTs.
    long visibleAtTs = clock.getCurrentTimestampMs();

    Message message = new Message()
        .withContent(request.getMessage())
        .withUri(request.getUri())
        .withVisibilityTimestamp(visibleAtTs);

    if (request.getHandle() == null || request.getHandle().isEmpty()) {
      message.withHandle(CommonHelperUtil.getRandomHandle());
    } else {
      message.withHandle(request.getHandle());
    }

    addMessageToMainQueue(message);
  }

  @Override
  public Message pull(MessagePullRequest request) throws QueueServiceException {
    MessagePullRequest.validate(request);

    String uri = request.getUri();

    // check if queue exists.
    PriorityBlockingQueue<Message> queue = queues.get(uri);
    if (queue == null) {
      throw new QueueServiceException("Queue " + uri + " does not exist");
    }

    processInFlightQueue(uri);

    // If queue is empty or any message has not yet reached visibility timestamp, return null.
    long currentTime = clock.getCurrentTimestampMs();
    if (queue.isEmpty() || currentTime < queue.peek().getVisibilityTimestamp()) {
      return null;
    }

    int visibilityTimeout = request.getVisibilityTimeout();
    Message message = queue.poll();
    message.setVisibilityTimestamp(visibilityTimeout * 1000 + clock.getCurrentTimestampMs());

    // Put the message to in-flight queue, which will be cleared by the worker.
    addMessageToInFlightQueue(message);

    return message;
  }

  @Override
  public void delete(MessageDeleteRequest request) throws QueueServiceException {
    MessageDeleteRequest.validate(request);

    Message message = request.getMessage();
    String handle = message.getHandle();
    String uri = message.getUri();

    // Check if queue exists.
    PriorityBlockingQueue<Message> queue = inFlightQueues.get(uri);

    // Check if queue is invalid.
    if (queue == null && queues.get(uri) == null) {
      throw new QueueServiceException("Queue " + uri + " does not exist");
    } else if (queue == null) {
      return;
    }

    // Delete the message only if it exists in in-flight queue.
    // If the message has been moved to main queue, this consumer should not delete it.
    for (Message messageItem : queue) {
      if (messageItem.getHandle().equals(handle)) {
        if (messageItem.getVisibilityTimestamp() > clock.getCurrentTimestampMs()) {
          queue.remove(messageItem);

          System.out.println("Message with handle " + handle + " has been deleted successfully from"
              + " queue " + uri);
        }
        return;
      }
    }

    System.out.println("Message with handle " + handle + " not found in queue " + uri);
  }

  public void addMessageToMainQueue(Message message) {
    addMessageToQueue(queues, message);
  }

  public void addMessageToInFlightQueue(Message message) {
    addMessageToQueue(inFlightQueues, message);
  }

  public void addMessageToQueue(Map<String, PriorityBlockingQueue<Message>> queues,
                                Message message) {
    queues.computeIfAbsent(message.getUri(), k ->
        new PriorityBlockingQueue<>(10, messagePriorityComparator()))
        .add(message);
  }

  public void removeMessageFromMainQueue(Message message) {
    removeMessageFromQueue(queues, message);
  }

  public void removeMessageFromInFlightQueue(Message message) {
    removeMessageFromQueue(inFlightQueues, message);
  }

  public void removeMessageFromQueue(Map<String, PriorityBlockingQueue<Message>> queues,
                                     Message message) {
    PriorityBlockingQueue<Message> queue = queues.get(message.getUri());
    if (queue == null) {
      // ignore the message.
      return;
    }

    // remove if handle matches with provided message.
    Message foundMsg = null;
    for (Message msg : queue) {
      if (msg.getHandle().equals(message.getHandle())) {
        foundMsg = msg;
      }
    }
    if (foundMsg != null) {
      queue.remove(foundMsg);
    }
  }

  public void deleteFromInFlightQueue(Message message, Long currentTs) {
    // delete message from in-flight queue if visibilityTimestamp criteria satisfies.
    PriorityBlockingQueue<Message> inFlightQueue = inFlightQueues.get(message.getUri());
    if (inFlightQueue.contains(message)
        && currentTs <= message.getVisibilityTimestamp()) {

      inFlightQueue.remove(message);
    }
  }

  private Comparator<Message> messagePriorityComparator() {
    return (m1, m2) -> {
      Long ts1 = m1.getVisibilityTimestamp();
      Long ts2 = m2.getVisibilityTimestamp();
      return Long.compare(ts1, ts2);
    };
  }

}
