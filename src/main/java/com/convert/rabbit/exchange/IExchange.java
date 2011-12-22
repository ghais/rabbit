/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rabbit.exchange;

import java.io.IOException;
import java.util.concurrent.Future;

import com.convert.rabbit.Message;
import com.convert.rabbit.exception.ConvertAmqpException;

/**
 * the exchange service.
 *
 */
public interface IExchange {

    /**
     * Publish a message to the given exchange.
     * 
     * @param msg
     *            the message to publish.
     * @throws ConvertAmqpException
     *             a runtime exception in case of any exceptions.
     * @throws IOException
     */
    void publish(Message msg) throws ConvertAmqpException, IOException;

    /**
     * Publish a message to the exchange asynchronously.
     *
     * @param msg
     *            the message to publish.
     * @return a future for the completion of the operation.
     */
    Future<Void> asyncPublish(Message msg);

    /**
     * Shuts down this exchange service.
     */
    void shutdown();
}
