package com.QueueEmulator;

import java.util.*;

// InMemoryQueueService implements an in-memory message queue service (based on the QueueService interface).

public class InMemoryQueueService implements QueueService {
  
  // Pool of in-memory queues managed by this service - it is a map of unique String IDs 
  // mapping to InMemoryQueue objects. 
  // Clients provide the queue name at queue creation which can be used to 
  // publish to and consume from a queue on a particular queue service instance.
  private HashMap<String,InMemoryQueue> queues;

  // default visibility timeout for a message on a queue on this service (in milliseconds)
  private static final long MESSAGE_TIMEOUT = 5000;

  // max. number of queues provided by service
  private long MAX_QUEUES = 60;

  // max. length of a queue name
  private long MAX_QUEUENAME_LEN = 80;

  // Constructor
  public InMemoryQueueService() {
    this.queues = new HashMap<String,InMemoryQueue>();
  }

  // Create a new message queue on this service: returns true if successful 
  public synchronized boolean createQueue(String queueName) {
    // basic validation
    if(queueName == null || queueName.trim().equals("") || (queueName.trim().length() > this.MAX_QUEUENAME_LEN) || this.queues.containsKey(queueName.trim())) {
      // invalid queuename
      return false;
    }

    // check current pool size on this service and create queue if there's space
    if(this.queues.size() < MAX_QUEUES) {
      queueName = queueName.trim();
      this.queues.put(queueName, new InMemoryQueue(queueName));
      return true;
    }

    // max queue limit reached for this service - could not create queue
    return false;
  }

  // return number of queues configured on this service
  public synchronized int queueCount() {
    return this.queues.size();
  }

  // Push a message with given content onto a specified queue - returns true if message published successfully
  // For this implementation, all messages have String contents
  public synchronized boolean push(String messageContent, String queueName) {
    // TODO: check for max. message size
    if(queueName == null || queueName.trim().equals("") || messageContent == null || messageContent.trim().equals("")) {
      return false;
    }

    // check if queue with given name exists
    if(this.queues.containsKey(queueName.trim())) {
      InMemoryQueue queue = this.queues.get(queueName);

      // add message to queue if queue is below max capacity
      if(!queue.full()) {
        this.queues.get(queueName).push(new InMemoryMessage(MESSAGE_TIMEOUT, messageContent));
        return true;
      }

      return false;   // failed: queue full
    }

    return false;   // failed: no such queue exists
  }

  // Receive message from specified queue
  public synchronized Message pull(String queueName) {
    if(queueName == null || queueName.trim().equals("")) {
      return null;
    }

    // find queue by the name of queueName and get message from it
    queueName = queueName.trim();
    if(this.queues.containsKey(queueName)) {
      return this.queues.get(queueName).getMessage();
    }

    return null;
  }

  // delete received message from a specific queue - needs receipt handle and queue ID
  public synchronized void delete(Long receiptHandle, String queueName) {
    if(queueName == null || queueName.trim().equals("") || receiptHandle == null) {
      return;
    }

    // find queue by queueName and delete message from it
    queueName = queueName.trim();
    if(this.queues.containsKey(queueName)) {
      this.queues.get(queueName).delete(receiptHandle);
    }
  }

  // delete a queue configured on this queue service
  public synchronized boolean deleteQueue(String queueName) {
    if(queueName == null || queueName.trim().equals("")) {
      return false;
    }

    queueName = queueName.trim();
    if(this.queues.containsKey(queueName)) {
      this.queues.remove(queueName);
      return true;
    }

    // no such queue
    return false;
  }

  // get number of messages on a given queue
  public int getQueueMessageCount(String queueName) {
    if(this.queues.containsKey(queueName)) {
      return this.queues.get(queueName).getMessageCount();
    }

    return -1;
  }

  // print out names of queues configured on this queueservice
  public synchronized void printQueueService() {
    if(this.queues.size() == 0) {
      System.out.println("\n<This queue service is empty>");
      return;
    }

    System.out.println("\nCONFIGURED QUEUES ON THIS SERVICE");
    System.out.println("---------------------------------");

    Iterator it = this.queues.entrySet().iterator();
    while (it.hasNext()) {
        Map.Entry pair = (Map.Entry)it.next();
        System.out.println("Queue Name: <" + pair.getKey() + ">");
    }
  }

  // print contents of a given queue
  public synchronized void printQueue(String queueName) {
    if(queueName == null || queueName.trim().equals("")) {
      System.out.println("\nCannot print queue: invalid queuename.");
    }

    if(this.queues.containsKey(queueName)) {
      this.queues.get(queueName).printQueue();
    } else {
      System.out.println("No such queue: QID <" + queueName + ">");
    }
  }
}

// class representing an in-memory message queue
class InMemoryQueue {
    // unique identifier for this queue in InMemoryQueueService (set permanently at queue creation)
    private final String queueName;

    // max number of messages that can be in visibility timeout on this queue at a time
    private static final long MAX_INFLIGHT_MESSAGES = 20000;

    // max number of messages that can be in the queue at a time
    private static final long MAX_QUEUE_MESSAGES = 100000;    

    // Map of receipt handles - each RH is tied to a unique Long key
    // each time a message is retrieved, an RH will be issued and added to this map.
    // and deleted at message deletion / timeout expiry.
    private HashMap<Long,Object> receiptHandles = new HashMap<Long,Object>();

    // Collection of message objects on this queue
    private ArrayList<InMemoryMessage> messageQueue;

    // create a queue object with a given identifier
    protected InMemoryQueue(String qn) {
        // if(qn == null || qn.trim().equals("")) {
            this.queueName = qn;
            this.messageQueue = new ArrayList<InMemoryMessage>();
        // }
    }

    // publish message onto this queue
    protected synchronized boolean push(InMemoryMessage msg) {
        if(msg != null) {
            if((this.getInFlightCount() < this.MAX_INFLIGHT_MESSAGES) && !this.full()) {
                this.messageQueue.add(msg);
                return true;
            }

            // max inflight message limit reached (overlimit) or max message count reached for this queue
            return false;
        }

        return false;   // invalid message
    }

    // get message from this queue
    protected synchronized InMemoryMessage getMessage() {
        ListIterator li = this.messageQueue.listIterator();

        // iterate from head of message queue, and return the first visible message
        while(li.hasNext()) {
            InMemoryMessage msg = (InMemoryMessage)li.next();
            if(msg.visible()) {
                // create a unique receipt handle, record it and return message
                Long receiptHandle = this.createReceiptHandle();

                while (this.receiptHandles.containsKey(receiptHandle)) {
                    receiptHandle = this.createReceiptHandle();
                }

                this.receiptHandles.put(receiptHandle, null);
                msg.setReceiptHandle(receiptHandle);
                msg.retrieve();
                return msg;
            }
        }

        return null;
    }

    // delete (hopefully processed) message from this queue
    protected synchronized void delete(Long receiptHandle) {
         ListIterator li = this.messageQueue.listIterator();

        // iterate from end of message list
        while(li.hasNext()) {
            InMemoryMessage m = (InMemoryMessage)li.next();

            // to be eligible for deletion, a message must be in visibility timeout and have a matching receipt handle
            if(!m.visible() && m.getReceiptHandle().equals(receiptHandle)) {
                li.remove();    // delete message
                
                // delete used receipt handle
                if(this.receiptHandles.containsKey(receiptHandle)) {
                    this.receiptHandles.remove(receiptHandle);
                    return;
                }
            }
        }
    }

    // get number of messages in visibility timeout on this queue
    protected synchronized long getInFlightCount() {
        long count = 0;
        for (InMemoryMessage msg: this.messageQueue) {
            if(!msg.visible()){
                count++;
            }
        }

        return count;
    }

    // return message queue size
    protected int getMessageCount() {
        return this.messageQueue.size();
    }

    // get number of maximum inflight messages allowed on this queue
    protected synchronized long getMaxInFlight() {
        return this.MAX_INFLIGHT_MESSAGES;
    }

    // get number of maximum messages allowed on this queue
    protected synchronized long getMaxMessages() {
        return this.MAX_QUEUE_MESSAGES;
    }

    // indicate if the queue has reached maximum number of messages
    protected synchronized boolean full() {
        if(this.messageQueue.size() < MAX_QUEUE_MESSAGES) {
            return false;   // got space for more 
        }

        return true;    // queue full
    }

    // utility method - generate a Long identifier to be used as the receipt handle candidate for retrieved message
    private Long createReceiptHandle() {
        Random r = new Random();
        Long rhc = new Long(r.nextLong());

        if(rhc < 0) {
            rhc = rhc * (-1);
        }

        return rhc;
    }

    protected synchronized void printQueue() {
        long count = 0;
        System.out.println("\nMESSAGES ON THIS QUEUE (NAME: " + queueName + ")");
        System.out.println("---------------------------------");
        
        for (InMemoryMessage msg: this.messageQueue) {
            msg.printMessage();
        }
    }
}