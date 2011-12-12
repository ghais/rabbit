/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rabbit.exchange.workerqueue;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.Test;

import com.convert.rabbit.Client;
import com.convert.rabbit.Message;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 *
 */
public class WorkerQueueTest {

    private static final Logger LOG = Logger.getLogger(WorkerQueueTest.class.getName());
    @Test
    public void testSingleProducerConcurrentConsumer() throws IOException, InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);

        final String exchangeName = UUID.randomUUID().toString();
        final String queueName = UUID.randomUUID().toString();
        final Client client = new Client();
        WorkerQueue queue = client.getWorkerQueue(exchangeName, queueName); // just creates the exchange.
        for (int i = 0; i < 1000; i++) {
            final String msg = "Message # " + i;
            queue.publish(new Message() {

                @Override
                public boolean isMandatory() {
                    return false;
                }

                @Override
                public boolean isImmedaite() {
                    return false;
                }

                @Override
                public String getRoutingKey() {
                    return "x.y";
                }

                @Override
                public byte[] getBody() {
                    return msg.getBytes();
                }
            });
        }

        ConnectionFactory factory = new ConnectionFactory();
        final Connection connection = factory.newConnection();
        Runnable r = new Runnable() {

            @Override
            public void run() {
                Channel c;
                try {
                    c = connection.createChannel();
                    c.queueBind(queueName, exchangeName, "");
                    QueueingConsumer consumer = new QueueingConsumer(c);
                    c.basicConsume(queueName, true, consumer);
                    while (true) {
                        Delivery d = consumer.nextDelivery(1000);

                        if (null == d) {
                            break;
                        }
                        LOG.info("Received: " + new String(d.getBody()));
                        counter.incrementAndGet();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (ShutdownSignalException e) {
                    throw new RuntimeException(e);
                } catch (ConsumerCancelledException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Thread t1 = new Thread(r);
        Thread t2 = new Thread(r);
        t1.start();
        t2.start();

        t1.join();
        t2.join();
        Channel c = connection.createChannel();
        c.exchangeDelete(exchangeName);
        c.queueDelete(queueName);
        assertEquals(counter.get(), 1000);
        connection.close();
        client.close();
    }

}
