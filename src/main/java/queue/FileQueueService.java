package queue;

import queue.clock.IClock;
import queue.clock.SystemClock;
import queue.exception.QueueServiceException;
import queue.model.Message;
import queue.model.MessageDeleteRequest;
import queue.model.MessageFileDto;
import queue.model.MessagePullRequest;
import queue.model.MessagePushRequest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;



/**
 * FileQueueService logs all the operations in a QUEUE file and maintains an in-memory queue.
 * FileLock is obtained on the file before performing any operation, hence all operations are
 * blocking, synchronous and protects against each other within the same JVM or among different
 * JVMs.
 */
public class FileQueueService implements QueueService {
  public static final String QUEUE_FILE_NAME = "QUEUE";
  private static final String SERDE_SEP = "#$";
  private static final String SERDE_SEP_PATTERN = "\\#\\$";

  private final String baseFilePath;
  private final IClock clock;
  private final File queueFile;

  private int currentReadLine = 0;
  private InMemoryQueueService inMemoryQueueService;

  public FileQueueService() {
    this.baseFilePath = ".";
    this.clock = new SystemClock();
    this.queueFile = getQueueFile();
    this.inMemoryQueueService = new InMemoryQueueService(clock);
  }

  public FileQueueService(String baseFilePath) {
    this.baseFilePath = baseFilePath;
    this.clock = new SystemClock();
    this.queueFile = getQueueFile();
    this.inMemoryQueueService = new InMemoryQueueService(clock);
  }

  public FileQueueService(String baseFilePath, IClock clock) {
    this.baseFilePath = baseFilePath;
    this.clock = clock;
    this.queueFile = getQueueFile();
    this.inMemoryQueueService = new InMemoryQueueService(clock);
  }

  private File getQueueFile() {
    Path path = Paths.get(baseFilePath, QUEUE_FILE_NAME);
    return path.toFile();
  }

  @Override
  public void push(MessagePushRequest messagePushRequest) throws QueueServiceException {

    MessagePushRequest.validate(messagePushRequest);

    try {

      Message message = new Message()
          .withContent(messagePushRequest.getMessage())
          .withHandle(messagePushRequest.getHandle())
          .withUri(messagePushRequest.getUri())
          .withVisibilityTimestamp(clock.getCurrentTimestampMs());

      if (message.getHandle() == null || message.getHandle().isEmpty()) {
        message.withHandle(CommonHelperUtil.getRandomHandle());
      }

      safePushOperation(message);

    } catch (IOException e) {
      throw new QueueServiceException("Push message failed due to IO error, ", e);
    }

  }

  @Override
  public Message pull(MessagePullRequest messagePullRequest) throws QueueServiceException {

    MessagePullRequest.validate(messagePullRequest);

    try {
      return safePullOperation(messagePullRequest);

    } catch (IOException e) {
      throw new QueueServiceException("Push message failed due to IO error, ", e);
    }
  }

  @Override
  public void delete(MessageDeleteRequest messageDeleteRequest) throws QueueServiceException {

    MessageDeleteRequest.validate(messageDeleteRequest);

    try {

      safeDeleteOperation(messageDeleteRequest.getMessage());

    } catch (IOException e) {
      throw new QueueServiceException("Push message failed due to IO error, ", e);
    }

  }

  private Message safePullOperation(MessagePullRequest pullRequest)
      throws IOException, QueueServiceException {

    try (RandomAccessFile raf = new RandomAccessFile(queueFile, "rw");
         FileOutputStream fos = new FileOutputStream(queueFile, true);
         FileInputStream fis = new FileInputStream(queueFile);
         BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
         BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
         LineNumberReader lineReader = new LineNumberReader(br);
         FileLock fileLock = raf.getChannel().lock(0L, Long.MAX_VALUE, true)) {

      // Replay the file logs to update in-memory queue. There might be multiple processes or
      // threads which could have updated the queue file.
      refreshQueue(lineReader);

      Message message = inMemoryQueueService.pull(pullRequest);
      if (message == null) {
        return null;
      }

      // Write into the file that message is being consumed by a consumer upto visibilityTimestamp.
      String serializedMessage = serializeCSV(Operation.PULL.name(), message);

      // Write message in a separate line in file queue.
      bw.write(serializedMessage);
      bw.newLine();

      currentReadLine++;

      return message;
    }
  }

  private void safePushOperation(Message message) throws IOException {

    /*
     * Push just writes a push operation log in the file. Actual push happens during pull
     * API call.
     */

    try (FileOutputStream fos = new FileOutputStream(queueFile, true);
         BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
         FileLock fileLock = fos.getChannel().lock()) {

      // Construct a serialized message.
      String serializedMessage = serializeCSV(Operation.PUSH.name(), message);

      // Write message in a separate line in file queue.
      bw.write(serializedMessage);
      bw.newLine();
    }
  }

  private void safeDeleteOperation(Message message) throws IOException, QueueServiceException {

    /*
     * Delete just writes a delete operation log in the file. Actual delete happens during pull
     * API call.
     */

    try (FileOutputStream fos = new FileOutputStream(queueFile, true);
         BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
         FileLock fileLock = fos.getChannel().lock()) {

      // Construct a serialized delete message.
      String serializedMessage = serializeCSV(Operation.DELETE.name(), message);

      // Write message in a separate line in file queue.
      bw.write(serializedMessage);
      bw.newLine();

    }
  }

  public void refreshQueue(LineNumberReader lineReader) throws IOException, QueueServiceException {

    // Skip all the lines upto current read line.
    int lineNumber = 0;
    while (lineNumber < currentReadLine) {
      lineReader.readLine();
      lineNumber++;
    }

    // Read any new unread message from file and feed to in-memory queue.
    String serMessage = lineReader.readLine();
    while (serMessage != null) {
      lineNumber++;
      MessageFileDto messageFileDto = deserializeCSV(serMessage);

      Operation operation = Operation.valueOf(messageFileDto.getOperation());
      long operationTs = messageFileDto.getCurrentTimestamp();
      Message message = messageFileDto.getMessage();

      if (operation.equals(Operation.PUSH)) {
        // add message to main queue.
        inMemoryQueueService.addMessageToMainQueue(message);
      } else if (operation.equals(Operation.PULL)) {
        // remove message if present in main queue.
        inMemoryQueueService.removeMessageFromMainQueue(message);
        // remove message if present in in-flight queue.
        inMemoryQueueService.removeMessageFromInFlightQueue(message);
        // add message to in-flight queue.
        inMemoryQueueService.addMessageToInFlightQueue(message);
      } else {
        inMemoryQueueService.deleteFromInFlightQueue(message, operationTs);
      }

      serMessage = lineReader.readLine();
    }

    inMemoryQueueService.processAllInFlightQueues();

    currentReadLine = lineNumber;
  }

  private String serializeCSV(String operation, Message message) {
    return operation + SERDE_SEP + clock.getCurrentTimestampMs() + SERDE_SEP + message.getUri()
        + SERDE_SEP + message.getHandle() + SERDE_SEP + message.getContent() + SERDE_SEP
        + Long.toString(message.getVisibilityTimestamp());
  }

  private MessageFileDto deserializeCSV(String serMessage) {
    String[] tokens = serMessage.split(SERDE_SEP_PATTERN);
    String operation = tokens[0];
    long currentTs = Long.valueOf(tokens[1]);
    String uri = tokens[2];
    String handle = tokens[3];
    String content = tokens[4];
    long visibilityTimestamp = Long.valueOf(tokens[5]);
    Message message = new Message(uri, handle, content, visibilityTimestamp);
    return new MessageFileDto(operation, currentTs, message);
  }

  enum Operation {
    PUSH, PULL, DELETE
  }
}
