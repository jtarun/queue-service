package queue;

import queue.exception.QueueServiceException;

import queue.model.Message;
import queue.model.MessageDeleteRequest;
import queue.model.MessagePullRequest;
import queue.model.MessagePushRequest;


public interface QueueService {

  /**
   * This function pushes the message to required queue uri. If no handle(identifier) is provided
   * for the message, a unique handle per queue is generated internally. Message handle is returned
   * when the message is pulled from the queue.
   *
   * @param messagePushRequest Request object containing push configurations and data.
   * @throws QueueServiceException if the push is unsuccessful.
   */
  void push(MessagePushRequest messagePushRequest) throws QueueServiceException;

  /**
   * This function pulls a visible message from a given queue. Pulled message may be in FIFO order,
   * but that depends on implementation. The message contains a unique handle per queue,
   * which can be used to perform any other operation on the message, like delete.
   *
   * @param messagePullRequest Request object containing pull configuration.
   * @return Message pulled out of given queue.
   * @throws QueueServiceException if the pull is unsuccessful.
   */
  Message pull(MessagePullRequest messagePullRequest) throws QueueServiceException;

  /**
   * Provided a message handle, this function can delete a message from a given queue.
   *
   * @param messageDeleteRequest Delete object.
   * @throws QueueServiceException if deletion is unsuccessful.
   */
  void delete(MessageDeleteRequest messageDeleteRequest) throws QueueServiceException;

}
