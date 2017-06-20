
package com.QueueEmulator;

import java.util.*;
import java.util.concurrent.TimeUnit;

// SQS Emulator test client class

public class SQSEClient {
    public static void main(String[] args) throws InterruptedException {
        // create new IMqueue via IMQService instance
        System.out.println("\nCreating an InMemoryQueueService and adding some messages..");
        InMemoryQueueService iqs1 = new InMemoryQueueService();
        String QUEUE_01 = "Queue_01";
        boolean created = iqs1.createQueue(QUEUE_01);

        if(created) {
            System.out.println("Created: Queue ID <" + QUEUE_01 + ">)\n");
        } else {
            System.out.println("Failed to create queue.");
            return;
        }

        // push message(s) onto it
        iqs1.push("First Message", QUEUE_01);
        // Thread.sleep(1000);
        iqs1.push("Second Message", QUEUE_01);
        // Thread.sleep(1000);
        iqs1.push("Third Message", QUEUE_01);
        // Thread.sleep(1000);
        iqs1.push("Fourth Message", QUEUE_01);
        // Thread.sleep(1000);
        iqs1.push("Fifth Message", QUEUE_01);
        // Thread.sleep(1000);
        iqs1.push("Sixth Message", QUEUE_01);
        // Thread.sleep(1000);

        iqs1.printQueueService();
        iqs1.printQueue(QUEUE_01);
        
        // retrieve messages from it
        System.out.println("\nRetrieving messages..");
        Message msg1 = iqs1.pull(QUEUE_01);
        Message msg2 = iqs1.pull(QUEUE_01);
        iqs1.printQueue(QUEUE_01);

        System.out.println("\nDeleting retrieved messages..");
        iqs1.delete(msg1.getReceiptHandle(), QUEUE_01);
        iqs1.delete(msg2.getReceiptHandle(), QUEUE_01);
        iqs1.printQueue(QUEUE_01);

        Thread backgroundThread = new Thread(new Runnable() {
            public void run() {
                try {
                    // retrieve some more messages from it
                    System.out.println("\n\tRetrieving a few more messages (secondary thread)");
                    Message msg3 = iqs1.pull(QUEUE_01);
                    Message msg4 = iqs1.pull(QUEUE_01);
                    iqs1.printQueue(QUEUE_01);

                    System.out.println("\n\tWait and retrieve another message (secondary thread)");
                    Thread.sleep(4000);
                    Message msg5 = iqs1.pull(QUEUE_01);
                    System.out.println("\tRetrieved message: <" + msg5.getMessageContent() + " / RH: " + msg5.getReceiptHandle() + "> (secondary thread)");

                    System.out.println("\n\tSleeping a bit - to get messages back to visibility (secondary thread)");
                    Thread.sleep(6000);
                    iqs1.printQueue(QUEUE_01);
                } catch (InterruptedException e) {
                    System.err.println("Caught InterruptedException: " + e.getMessage());
                }
            }
        });
        backgroundThread.start();        

        // delete queue
        String QUEUE_02 = "Queue_02";
        created = iqs1.createQueue(QUEUE_02);

        if(created) {
            System.out.println("Created: Queue ID <" + QUEUE_02 + ">)\n");
        } else {
            System.out.println("Failed to create queue.");
            return;
        }

        System.out.println("\nDeleting queue..");
        boolean deleted = iqs1.deleteQueue(QUEUE_02);
        if(deleted) {
            System.out.println("Queue deleted successfully.");
        } else {
            System.out.println("Failed to delete queue.");
        }

        System.out.println("\nTrying to print deleted queue");
        iqs1.printQueue(QUEUE_02);
    }
}