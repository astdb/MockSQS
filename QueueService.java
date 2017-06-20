package com.QueueEmulator;

public interface QueueService {
  // Interface defining message queue service public API
  // - push
  //   pushes a message onto a queue.
  // - pull
  //   retrieves a single message from a queue.
  // - delete
  //   deletes a message from the queue that was received by pull().
  //

  // push a given message onto a specified queue
  public boolean push(String message, String queueName);

  // receive message from specified queue
  public Message pull(String queueName);

  // delete received message from a specified queue
  public void delete(Long receiptHandle, String queueName);

}
