package com.convert.rabbit.exception;

/**
 * This class is a wrapper around all exceptions thrown by the RabbitMQ java API exceptions.
 * 
 */
public class ConvertAmqpException extends RuntimeException {

    private static final long serialVersionUID = 9060030038385404977L;

    public ConvertAmqpException(Exception e) {
        super(e);
    }
}
