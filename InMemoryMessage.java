package com.QueueEmulator;

import java.util.*;

// class representing an in-memory message

class InMemoryMessage implements Message {
    private long messageID;
    private Long receiptHandle;
    private Object messageContent;
    private long timeCreated;
    private long timeRetrieved;
    private long visibilityTimeoutDuration;

    protected InMemoryMessage(long timeout, Object content) {
        this.visibilityTimeoutDuration = timeout;
        this.messageContent = content;
        this.receiptHandle = null;
        this.timeRetrieved = 0;
        this.timeCreated = new Date().getTime();
    }

    public Object getMessageContent() {
        return this.messageContent;
    }

    public Long getReceiptHandle() {
        return this.receiptHandle;
    }

    protected void setMessageID(long mid) {
        this.messageID = mid;
    }

    protected void setReceiptHandle(Long rh) {
        if(rh != null) {
            this.receiptHandle = rh;
        }
    }

    protected Long getTimeCreated() {
        return this.timeCreated;
    }

    protected boolean visible() {
        if(this.timeRetrieved == 0) {
            return true;
        }

        long now = new Date().getTime();
        if((now - timeRetrieved) < visibilityTimeoutDuration) {
            return false;
        }

        this.receiptHandle = null;
        this.timeRetrieved = 0;
        return true;
    }

    protected boolean retrieve() {
        // set time retrieved to calculate visibility in visible()
        this.timeRetrieved = new Date().getTime();
        return true;
    }

    protected synchronized void printMessage() {
        if(this.visible()){
           System.out.print("VISIBLE | ");
        } else {
           System.out.print("INVISIBLE | ");
        }

        System.out.print("\"" + this.getMessageContent() + "\" | Created at: " + this.getTimeCreated() + "\n");
    }
}