package com.convert.rabbit;

/**
 * A message to be sent through the exchange/workerQueue
 * 
 */
public interface Message {

    String getRoutingKey();

    boolean isImmedaite();

    boolean isMandatory();

    byte[] getBody();

}
