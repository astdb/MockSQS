
package com.QueueEmulator;

// interface defining public API of a Message: once received, clients 
// will be able to retrieve message content and a receipt handle. 

public interface Message {
    // return message content
    public Object getMessageContent();

    // return message receipt handle
    public Long getReceiptHandle();
}