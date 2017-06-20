package com.QueueEmulator;

import com.amazonaws.services.sqs.AmazonSQSClient;

public class SqsQueueService implements QueueService {
  
  // This file is a placeholder for an AWS-backed implementation of QueueService.  It is included
  // primarily so you can quickly assess your choices for method signatures in QueueService in
  // terms of how well they map to the implementation intended for a production environment.
  //

  public SqsQueueService(AmazonSQSClient sqsClient) {
    
  }
}
