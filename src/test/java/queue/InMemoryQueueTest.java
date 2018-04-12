package queue;


import queue.clock.IClock;
import queue.clock.PseudoClock;
import queue.exception.QueueServiceException;
import queue.model.Message;
import queue.model.MessageDeleteRequest;
import queue.model.MessagePullRequest;
import queue.model.MessagePushRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InMemoryQueueTest {

  private QueueService queueService;
  private IClock clock;
  private long startTime;

  @Before
  public void before() {
    this.startTime = 0;
    this.clock = new PseudoClock(startTime);
    queueService = new InMemoryQueueService(clock);
  }

  @Test
  public void messagePushTest() throws QueueServiceException {
    String expectedMessage = "Push Message 1";
    String queueName = "Queue1";

    MessagePushRequest pushRequest = new MessagePushRequest()
        .withMessage(expectedMessage)
        .withUri(queueName);

    queueService.push(pushRequest);

    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queueName);

    Message message = queueService.pull(pullRequest);
    Assert.assertEquals("Message content does not match", message.getContent(), expectedMessage);
    Assert.assertEquals("Message uri does not match", message.getUri(), queueName);
  }

  @Test(expected = QueueServiceException.class)
  public void messagePullQueueNotPresentTest() throws QueueServiceException {
    String queueName = "Queue1";
    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queueName);

    try {
      Message message = queueService.pull(pullRequest);
    } catch (QueueServiceException e) {
      String exceptionMsg = e.getMessage();
      Assert.assertTrue(exceptionMsg.contains("Queue"));
      Assert.assertTrue(exceptionMsg.contains("does not exist"));
      throw e;
    }
  }

  @Test
  public void messagePullBeforeVisibilityTest() throws QueueServiceException {
    String expectedMessage = "Push Message 1";
    String queueName = "Queue1";

    MessagePushRequest pushRequest = new MessagePushRequest()
        .withMessage(expectedMessage)
        .withUri(queueName);

    // increase the clock time by 10 seconds
    long currentTime = clock.getCurrentTimestampMs();
    clock.setCurrentTimestampMs(currentTime + 10 * 1000);

    queueService.push(pushRequest);

    // restore the clock
    clock.setCurrentTimestampMs(currentTime);

    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queueName);

    Message message = queueService.pull(pullRequest);

    Assert.assertTrue("Null message was expected", message == null);
  }

  @Test
  public void messageDeleteAfterVisibilityTimeoutTest() throws QueueServiceException {
    // expected to pull the same message again from queue.
    String expectedMessage = "Push Message 1";
    String queueName = "Queue1";

    MessagePushRequest pushRequest = new MessagePushRequest()
        .withMessage(expectedMessage)
        .withUri(queueName);

    queueService.push(pushRequest);

    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queueName)
        .withVisibilityTimeout(60); // 60 seconds

    Message message = queueService.pull(pullRequest);

    // advance clock such that visibility timeout is over.
    long currentTs = clock.getCurrentTimestampMs();
    clock.setCurrentTimestampMs(currentTs + 65 * 1000); // advance by 65  seconds.

    MessageDeleteRequest deleteRequest = new MessageDeleteRequest()
        .withMessage(message);
    queueService.delete(deleteRequest);

    // pull the message again.
    message = queueService.pull(pullRequest);

    Assert.assertEquals("Message content does not match", message.getContent(), expectedMessage);
    Assert.assertEquals("Message uri does not match", message.getUri(), queueName);
  }

  @Test
  public void messageVisibilityTimeoutTest() throws QueueServiceException, InterruptedException {
    String expectedMessage = "Push Message 1";
    String queueName = "Queue1";

    MessagePushRequest pushRequest = new MessagePushRequest()
        .withMessage(expectedMessage)
        .withUri(queueName);

    queueService.push(pushRequest);

    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queueName)
        .withVisibilityTimeout(60); // 60 seconds

    Message message = queueService.pull(pullRequest);

    // advance the clock to a value greater than visibility timeout
    long currentTs = clock.getCurrentTimestampMs();
    clock.setCurrentTimestampMs(currentTs + 65 * 1000); // advance by 65 seconds

    // pull the message again.
    message = queueService.pull(pullRequest);

    Assert.assertEquals("Message content does not match", message.getContent(), expectedMessage);
    Assert.assertEquals("Message uri does not match", message.getUri(), queueName);
  }

  @Test
  public void messageDeleteTest() throws QueueServiceException {
    String expectedMessage = "Push Message 1";
    String queueName = "Queue1";

    MessagePushRequest pushRequest = new MessagePushRequest()
        .withMessage(expectedMessage)
        .withUri(queueName);

    queueService.push(pushRequest);

    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queueName)
        .withVisibilityTimeout(60); // 60 seconds

    Message message = queueService.pull(pullRequest);

    MessageDeleteRequest deleteRequest = new MessageDeleteRequest()
        .withMessage(message);

    queueService.delete(deleteRequest);

    // advance clock such that visibility timeout is over.
    long currentTs = clock.getCurrentTimestampMs();
    clock.setCurrentTimestampMs(currentTs + 65 * 1000); // advance by 65 seconds.

    // pull the message again.
    message = queueService.pull(pullRequest);

    Assert.assertTrue("null message was expected", message == null);
  }

}
