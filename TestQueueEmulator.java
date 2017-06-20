
package com.QueueEmulator;

// this class runs a comprehensive test suite for InMemoryQueue's public API

import java.util.Date;

public class TestQueueEmulator {
    public static void main(String[] args) throws InterruptedException {
        long VISIBILITY_TIMEOUT = 5000;    // message visibility timeout (in ms)
        long MAX_QUEUE_MESSAGES = 100000;   // max. no of messages per queue
        long MAX_QS_CAP = 60;              // max. no. or queues permitted per InMemoryQueueService

        // -------------------- InMemoryQueueService Tests --------------------
        // Test InMemoryQueueService creation
        System.out.println("\n========================================================\nTesting InMemoryQueueService creation..");
        InMemoryQueueService iqs1 = new InMemoryQueueService();

        if(iqs1 != null) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED.");
            return;
        }

        // Test in-memory queue creation through the in-memory queue service
        System.out.println("\n========================================================\nTesting InMemoryQueue creation..");

        // invalid queue names - null
        String q1 = null;
        boolean created = iqs1.createQueue(q1);
        if(!created && iqs1.queueCount() == 0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (in memory queue created with null queue name).");
            return;
        }
        iqs1.printQueueService();

        // invalid queue names - whitespace
        q1 = "       ";
        created = iqs1.createQueue(q1);
        if(!created && iqs1.queueCount() == 0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (in memory queue created with whitespace queue name).");
            return;
        }
        iqs1.printQueueService();

        // invalid queue names - empty string
        q1 = "";
        created = iqs1.createQueue(q1);
        if(!created && iqs1.queueCount() == 0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (in memory queue created with empty string queue name).");
            return;
        }
        iqs1.printQueueService();

        // invalid queue names - queuename > 80 chars
        q1 = "There is one change to the language. Go now supports type aliases to support gradual code repair while moving a type between packages.";
        created = iqs1.createQueue(q1);
        if(!created && iqs1.queueCount() == 0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (in memory queue created with > [max. queuename length] string queue name).");
            return;
        }
        iqs1.printQueueService();

        String QUEUE_01 = "Queue_01";
        boolean created1 = iqs1.createQueue(QUEUE_01);

        if(created1 && iqs1.queueCount() == 1) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (could not create queue with valid queuename <" + QUEUE_01 + ">).");
            return;
        }
        iqs1.printQueueService();

        System.out.println("\n========================================================\nTesting InMemoryQueue creation (second queue)");
        String QUEUE_02 = "Queue_02";
        boolean created2 = iqs1.createQueue(QUEUE_02);

        if(created2 && iqs1.queueCount() == 2) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (could not create queue with valid queuename <" + QUEUE_02 + ">).");
            return;
        }
        iqs1.printQueueService();

        // Test deleting queues
        System.out.println("\n========================================================\nTesting InMemoryQueue deletion: invalid parameters..");
        boolean deleted0 = iqs1.deleteQueue("");
        if(!deleted0){
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (deleteQueue() called with null parameter).");
            return;
        }

        deleted0 = iqs1.deleteQueue("         ");
        if(!deleted0){
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (deleteQueue() called with whitespace parameter).");
            return;
        }

        deleted0 = iqs1.deleteQueue(null);
        if(!deleted0){
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (deleteQueue() called with null parameter).");
            return;
        }

        deleted0 = iqs1.deleteQueue("NonExistentQueue");
        if(!deleted0){
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (deleteQueue() called with non-existent parameter).");
            return;
        }

        System.out.println("\n========================================================\nTesting InMemoryQueue deletion: deleting <" + QUEUE_01 + ">..");
        boolean deleted1 = iqs1.deleteQueue(QUEUE_01);

        if(deleted1 && iqs1.queueCount() == 1) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (could not delete queue <" + QUEUE_01 + ">).");
            return;
        }
        iqs1.printQueueService();

        System.out.println("\n========================================================\nTesting InMemoryQueue deletion: deleting <" + QUEUE_02 + ">..");
        boolean deleted2 = iqs1.deleteQueue(QUEUE_02);

        if(deleted2 && iqs1.queueCount() == 0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (could not create queue with valid queuename <" + QUEUE_02 + ">).");
            return;
        }
        iqs1.printQueueService();

    
        System.out.println("\n========================================================\nTesting deletion on empty queue service..");
        boolean deleted3 = iqs1.deleteQueue(QUEUE_02);

        if(!deleted3 && iqs1.queueCount() == 0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (deletion should have failed on empty queue service).");
            return;
        }
        iqs1.printQueueService();

        // Test message publishing to queue
        System.out.println("\n========================================================\nTesting message push() to queue..");

        InMemoryQueueService iqs2 = new InMemoryQueueService();
        String QUEUE_03 = "QUEUE_03";
        iqs2.createQueue(QUEUE_03);

        boolean p0 = iqs2.push("", QUEUE_03);
        if(!p0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (push() succeeded with invalid parameters).");
            return;
        }

        p0 = iqs2.push("  ", QUEUE_03);
        if(!p0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (push() succeeded with invalid parameters).");
            return;
        }

        p0 = iqs2.push(null, QUEUE_03);
        if(!p0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (push() succeeded with invalid parameters).");
            return;
        }

        p0 = iqs2.push("Message X", "");
        if(!p0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (push() succeeded with invalid parameters).");
            return;
        }

        p0 = iqs2.push("Message X", "     ");
        if(!p0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (push() succeeded with invalid parameters).");
            return;
        }

        p0 = iqs2.push("Message X", null);
        if(!p0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (push() succeeded with invalid parameters).");
            return;
        }

        p0 = iqs2.push("Message X", "NonExistentQueue");
        if(!p0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (push() succeeded with invalid parameters).");
            return;
        }

        p0 = iqs2.push(null, null);
        if(!p0) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (push() succeeded with invalid parameters).");
            return;
        }

        boolean p1 = iqs2.push("Test message 01", QUEUE_03);
        boolean p2 = iqs2.push("Test message 02", QUEUE_03);
        boolean p3 = iqs2.push("Test message 03", QUEUE_03);
        boolean p4 = iqs2.push("Test message 04", QUEUE_03);

        if(p1 && p2 && p3 && p4 && iqs2.getQueueMessageCount(QUEUE_03) == 4) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (failed to push messages onto queue <" + QUEUE_03 + ">).");
            return;
        }
        iqs2.printQueue(QUEUE_03);

        // Test message retrieval from queue
        System.out.println("\n========================================================\nTesting message pull() from queue..");

        // invalid pull parameters (empty/whitespace/null queuenames)
        Message msg0 = iqs2.pull("");
        if(msg0 == null) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (pull() succeeded with invalid parameters).");
            return;
        }

        msg0 = iqs2.pull("     ");
        if(msg0 == null) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (pull() succeeded with invalid parameters).");
            return;
        }

        msg0 = iqs2.pull(null);
        if(msg0 == null) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (pull() succeeded with invalid parameters).");
            return;
        }

        Message msg1 = iqs2.pull(QUEUE_03);
        Message msg2 = iqs2.pull(QUEUE_03);

        if(msg1.getMessageContent().equals("Test message 01") && msg2.getMessageContent().equals("Test message 02")) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (failed to retrieve messages correctly from queue <" + QUEUE_03 + ">).");
            return;
        }
        iqs2.printQueue(QUEUE_03);

        // Test message deletion
        System.out.println("\n========================================================\nTesting message delete() from queue..");

        // test delete() with invalid parameters
        iqs2.delete(null, QUEUE_03);
        if(iqs2.getQueueMessageCount(QUEUE_03) == 4) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (delete() succeeded with invalid parameters / null receipt handle).");
            return;
        }

        iqs2.delete(new Long(32168465L), "");
        if(iqs2.getQueueMessageCount(QUEUE_03) == 4) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (delete() succeeded with invalid parameters / empty queue name).");
            return;
        }

        iqs2.delete(new Long(32168465L), "       ");
        if(iqs2.getQueueMessageCount(QUEUE_03) == 4) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (delete() succeeded with invalid parameters / whitespace queue name).");
            return;
        }

        iqs2.delete(new Long(32168465L), null);
        if(iqs2.getQueueMessageCount(QUEUE_03) == 4) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (delete() succeeded with invalid parameters / null queue name).");
            return;
        }

        iqs2.delete(msg1.getReceiptHandle(), QUEUE_03);
        iqs2.delete(msg2.getReceiptHandle(), QUEUE_03);
        
        Message msg3 = iqs2.pull(QUEUE_03);
        if(msg3.getMessageContent().equals("Test message 03") && iqs2.getQueueMessageCount(QUEUE_03) == 2) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (failed to delete messages correctly from queue <" + QUEUE_03 + ">).");
            return;
        }
        iqs2.printQueue(QUEUE_03);

        // Test visibility timeout
        System.out.println("\n========================================================\nTesting message visibility timeout..");
        Message msg4 = iqs2.pull(QUEUE_03);     // this should pull out "Message 04", as "Message 03" just got pulled into msg3
        Thread.sleep(6000);    // wait out the visibility timeout (set to 5000ms)
        Message msg5 = iqs2.pull(QUEUE_03);     // this should pull out "Message 03", as it should now have reappeared at queue head

        // test the messages
        if(msg4.getMessageContent().equals("Test message 04") && msg5.getMessageContent().equals("Test message 03")) {
            System.out.println("PASSED.");
        } else {
            System.out.println("FAILED (visibility timeout failure).");
            return;
        }
        iqs2.printQueue(QUEUE_03);

        // Test max queue service capacity limit
        System.out.println("\n========================================================\nTest maximum queue service capacity");
        InMemoryQueueService iqs3 = new InMemoryQueueService();
        long j = 0;
        boolean c = false;

        for(j = 0; j < MAX_QS_CAP-1; j++) { // fill queue service upto one less than capacity
            c = iqs1.createQueue("Queue_" + j);

            if(!c) {
                System.out.println("FAILED (failed to create " + j + "th queue)");
                return;
            }
        }

        j++;
        c = iqs1.createQueue("Queue_" + j); // add last queue, filling queue service to capacity
        if(!c) {
            System.out.println("FAILED (failed to create " + j + "th queue)");
            return;
        }

        j++;
        c = iqs1.createQueue("Queue_" + j);  // add another queue over capacity, this should fail
        if(!c) {
            System.out.println("PASSED (failed to create " + j + "th queue [over limit])");
        }

        // Test max queue capacity limit
        System.out.println("\n========================================================\nTest maximum queue capacity");
        long t1 = new Date().getTime();
        String QUEUE_04 = "QUEUE_04";
        iqs2.createQueue(QUEUE_04);
        boolean pushStatus = false;
        long i = 0;
        for(i = 0; i < MAX_QUEUE_MESSAGES-1; i++) {    // fill queue upto one less than capacity
            pushStatus = iqs2.push("Test message", QUEUE_04);

            if(!pushStatus) {
                System.out.println("FAILED (failed to push " + i + "th message)");
                return;
            }
        }
        i++;
        pushStatus = iqs2.push("Test message " + i, QUEUE_04);   // add last message, filling queue

        if(!pushStatus) {
            System.out.println("FAILED (failed to push " + i + "th message)");
            return;
        }

        i++;
        pushStatus = iqs2.push("Test message " + i, QUEUE_04);   // add message over capacity
        if(!pushStatus) {
            System.out.println("PASSED (failed to push " + i + "th message [over capacity])");
        }
        long t2 = new Date().getTime();
        long t3 = t2 - t1;
        System.out.println("(" + t3 + "ms)");
    }
}