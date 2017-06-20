package com.QueueEmulator;

import java.util.*;
import java.io.*;
import java.nio.file.*;

// FileQueueService implements a filesystem-based message queue service (based on the QueueService interface)

public class FileQueueService implements QueueService {

  // FileQueueService attributes

  // queueServiceRoot is the filesystem root directory for storing queues of this service
  // to access a particular queue, it's assumed that cross-JVM clients would have knowledge of the root
  // directory the queue they need to access lives in. 
  // This must be in the form of "/root/somefolder/anotherfolder/folderofqueues" i.e. full path starting with / 
  // and ending with only the queue root folder name with no following '/'
  private String queueServiceRoot;

  // default visibility timeout for a message on a queue on this service (in milliseconds)
  private static final long MESSAGE_TIMEOUT = 5000;

  // max. number of queues provided by service
  private long MAX_QUEUES = 60;

  // max. length of the String queue name
  private long MAX_QUEUENAME_LEN = 80;


  // Constructor
  public FileQueueService(String qsr) {
    if(qsr != null || !qsr.trim().equals("")) {
      // TODO: validate qsr contains a valid full pathname e.g. '/var/local/queues'

      this.queueServiceRoot = qsr;

      // create file queue root if it doesn't exist
      File f = new File(this.queueServiceRoot);

      // file.mkdir() will atomically test the existance of the directory and if not, create it
      if(!f.mkdir()) {
        // already exists

      } else {
        // successfully created

      }
    } else {
      throw new IllegalArgumentException("FileQueueService root cannot be null or empty.");
    }
  }

  // Create a new message queue on this service: returns true if successful 
  public synchronized boolean createQueue(String queueName) {
    // basic validation
    if(queueName == null || queueName.trim().equals("") || (queueName.trim().length() > this.MAX_QUEUENAME_LEN)) {
      // invalid queuename
      return false;
    }

    // check current pool size on this service and create queue if there's space
    if(this.queueCount() < MAX_QUEUES) {
      queueName = queueName.trim();
      
      // check if queue exists and if not, create
      File f = new File(this.queueServiceRoot + "/" + queueName);
      if(!f.mkdir()) {
        // mkdir will atomically test the existance of the provided queue root directory and if not, create it. 
        // No further action needed on queue creation (creation of the queue root directory).

      }

      return true;
    }

    // max queue limit reached for this service - could not create queue
    return false;
  }

  // Push a message with given content onto a specified queue - returns true if message published successfully
  // For this implementation, all messages have String contents
  public synchronized boolean push(String messageContent, String queueName) {
    // TODO: check for max. message size
    if(queueName == null || queueName.trim().equals("") || messageContent == null || messageContent.trim().equals("")) {
      return false;
    }

    // check if queue with given name exists
    File f = new File(this.queueServiceRoot + "/" + queueName);
    
    if(f.isDirectory()) {
      // obtain lock on queue
      File lockFile = new File(this.queueServiceRoot + "/" + queueName + "/.lock");
      try {        
        this.lock(lockFile); 
      } catch(InterruptedException e) {
        System.err.println("Caught InterruptedException while attempting an access lock on queue <" + queueName + ">: " + e.getMessage());
        return false;
      }

      // Messages on the messages file have are recorded one per line with format "last_retrieved_time, message_content"
      String message = 0 + ", " + messageContent + "\n";
      try (PrintWriter pw = new PrintWriter(new FileWriter(this.queueServiceRoot + "/" + queueName + "/messages", true))) {  // append
        pw.println(message);
        
      } catch(IOException e) {
        System.err.println("Caught IOException while opening message store for queue <" + queueName + ">: " + e.getMessage());
        return false;
      } finally {
        unlock(lockFile);
      }

      return false;   // failed: queue full
    }

    return false;   // failed: no such queue exists
  }

  // receive message from specified queue
  public synchronized Message pull(String queueName) {
    if(queueName == null || queueName.trim().equals("")) {
      return null;
    }

    queueName = queueName.trim();
    
    // check if queue with given name exists
    File f = new File(this.queueServiceRoot + "/" + queueName);

    if(f.isDirectory()) {
      // obtain lock on queue
      File lockFile = new File(this.queueServiceRoot + "/" + queueName + "/.lock");
      try {        
        this.lock(lockFile); 
      } catch(InterruptedException e) {
        System.err.println("Caught InterruptedException while attempting an access lock on queue <" + queueName + ">: " + e.getMessage());
        return null;
      }

      // read next-up message, return, unlock queue

      return null;   // failed: queue full
    }

    return null;
  }

  // obtain access lock on a given queue
  private synchronized void lock(File lock) throws InterruptedException {
    while (!lock.mkdir()) {
      Thread.sleep(50);
    }
  }

  // release access lock on a given queue
  private synchronized void unlock(File lock) {
    lock.delete();
  }
  

  // delete received message from a specified queue
  public synchronized void delete(Long receiptHandle, String queueName) {

    return;
  }

  public synchronized long queueCount() {
    try {
      return Files.find(Paths.get("/tmp"), 1, (path, attributes) -> attributes.isDirectory()).count() - 1;
    } catch(IOException e) {
      return -1;
    }
    
  }
}