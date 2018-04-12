package queue;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import queue.exception.QueueServiceException;
import queue.model.Message;
import queue.model.MessageDeleteRequest;
import queue.model.MessagePullRequest;
import queue.model.MessagePushRequest;

public class SqsQueueService implements QueueService {

  private final AmazonSQS sqsClient;

  public SqsQueueService(AmazonSQSClient sqsClient) {
    this.sqsClient = sqsClient;
  }

  @Override
  public void push(MessagePushRequest request) throws QueueServiceException {
    try {
      sqsClient.sendMessage(toSQSSendMessageRequest(request));
    } catch (Exception e) {
      throw new QueueServiceException("push to SQS failed");
    }
  }

  @Override
  public Message pull(MessagePullRequest request) throws QueueServiceException {
    try {
      ReceiveMessageResult result = sqsClient.receiveMessage(toSQSReceiveRequest(request));

      com.amazonaws.services.sqs.model.Message sqsMessage =
          result.getMessages().isEmpty() ? null : result.getMessages().get(0);

      if (sqsMessage == null) {
        return null;
      }

      return new Message()
          .withContent(sqsMessage.getBody())
          .withUri(request.getUri())
          .withHandle(sqsMessage.getReceiptHandle());

    } catch (Exception e) {
      throw new QueueServiceException("pull from SQS failed.", e);
    }
  }

  @Override
  public void delete(MessageDeleteRequest request) throws QueueServiceException {
    try {
      sqsClient.deleteMessage(toSQSDeleteRequest(request));
    } catch (Exception e) {
      throw new QueueServiceException("Failed to delete message from SQS : " + request);
    }
  }

  private SendMessageRequest toSQSSendMessageRequest(MessagePushRequest request) {
    return new SendMessageRequest()
        .withQueueUrl(request.getUri())
        .withMessageBody(request.getMessage());
  }

  private ReceiveMessageRequest toSQSReceiveRequest(MessagePullRequest request) {
    return new ReceiveMessageRequest()
        .withQueueUrl(request.getUri())
        .withMaxNumberOfMessages(1)
        .withVisibilityTimeout(request.getVisibilityTimeout());
  }

  private DeleteMessageRequest toSQSDeleteRequest(MessageDeleteRequest request) {

    return
        new DeleteMessageRequest()
            .withQueueUrl(request.getMessage().getUri())
            .withReceiptHandle(request.getMessage().getHandle());
  }

}
