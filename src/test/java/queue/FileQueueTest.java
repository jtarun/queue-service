package queue;

import queue.clock.IClock;
import queue.clock.PseudoClock;
import queue.exception.QueueServiceException;
import queue.model.Message;
import queue.model.MessageDeleteRequest;
import queue.model.MessagePullRequest;
import queue.model.MessagePushRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileQueueTest {

  private FileQueueService fileQueueService;
  private IClock clock;
  private final String baseFilePath = ".";
  private final String queue1 = "Queue1";

  @Before
  public void before() {
    this.clock = new PseudoClock(0);
    this.fileQueueService = new FileQueueService(baseFilePath, clock);
  }

  @Test
  public void pushAndPullMessageTest() throws QueueServiceException {

    String expectedMessage = "Message 1";

    MessagePushRequest pushRequest = new MessagePushRequest()
        .withMessage(expectedMessage)
        .withUri(queue1);

    fileQueueService.push(pushRequest);

    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queue1);
    Message message = fileQueueService.pull(pullRequest);

    Assert.assertEquals("Message content does not match", message.getContent(), expectedMessage);
    Assert.assertEquals("Message uri does not match", message.getUri(), queue1);
  }

  @Test
  public void deleteMessageBeforeVisibilityTimeoutTest() throws QueueServiceException {

    String expectedMessage = "Message 1";

    MessagePushRequest pushRequest = new MessagePushRequest()
        .withMessage(expectedMessage)
        .withUri(queue1);

    fileQueueService.push(pushRequest);

    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queue1)
        .withVisibilityTimeout(60); // 60 seconds
    Message message = fileQueueService.pull(pullRequest);

    // delete the message immediately.
    MessageDeleteRequest deleteRequest = new MessageDeleteRequest()
        .withMessage(message);
    fileQueueService.delete(deleteRequest);

    // Advance the clock by visibility timeout and pull again.
    clock.setCurrentTimestampMs(clock.getCurrentTimestampMs() + 65 * 1000); // 65 seconds

    message = fileQueueService.pull(pullRequest);
    Assert.assertEquals("Message is not deleted", null, message);

  }


  @Test
  public void deleteMessageAfterVisibilityTimeoutTest() throws QueueServiceException {
    // Message is expected to be put back to queue from in-flight queue if it is deleted after
    // visisbility timeout period.

    String expectedMessage = "Message 1";

    MessagePushRequest pushRequest = new MessagePushRequest()
        .withMessage(expectedMessage)
        .withUri(queue1);

    fileQueueService.push(pushRequest);

    MessagePullRequest pullRequest = new MessagePullRequest()
        .withUri(queue1)
        .withVisibilityTimeout(60); // 60 seconds
    Message message = fileQueueService.pull(pullRequest);

    // Advance the clock by visibility timeout and then delete the message.
    clock.setCurrentTimestampMs(clock.getCurrentTimestampMs() + 65 * 1000); // 65 seconds
    MessageDeleteRequest deleteRequest = new MessageDeleteRequest()
        .withMessage(message);
    fileQueueService.delete(deleteRequest);

    message = fileQueueService.pull(pullRequest);

    Assert.assertTrue("Message should not be null",  message != null);
    Assert.assertEquals("Message is not deleted", expectedMessage, message.getContent());
  }

  @After
  public void clean() throws IOException {
    // delete all created files here.
    Path path = Paths.get(baseFilePath, FileQueueService.QUEUE_FILE_NAME);
    Files.deleteIfExists(path);
  }
}
