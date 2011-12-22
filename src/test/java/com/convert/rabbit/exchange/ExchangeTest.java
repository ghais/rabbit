/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rabbit.exchange;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.junit.Test;

import com.convert.rabbit.Client;
import com.convert.rabbit.Message;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;


/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 *
 */
public class ExchangeTest {

    private final static Logger LOG = Logger.getLogger(ExchangeTest.class.getName());

    @Test
    public void test_two_concurrent_threads() throws InterruptedException, IOException {

        int count = 0;
        final String exchangeName = UUID.randomUUID().toString();
        final Client client = new Client();
        final Exchange exchange = client.getExchange(exchangeName);
        Runnable r = new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    final String message = "Hello world, this is message # " + i;

                    try {
                        exchange.publish(new Message() {

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
                                return (message).getBytes();
                            }
                        });
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        };

        Thread t1 = new Thread(r);
        Thread t2 = new Thread(r);

        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        Channel c = connection.createChannel();
        String queueName = c.queueDeclare().getQueue();
        c.queueBind(queueName, exchangeName, "#");

        QueueingConsumer consumer = new QueueingConsumer(c);
        c.basicConsume(queueName, consumer);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        while (true) {
            Delivery d = consumer.nextDelivery(2000);
            if (null == d) {
                break;
            }
            LOG.info("Received: " + new String(d.getBody()));
            count++;
        }

        c.exchangeDelete(exchangeName);
        assertEquals(2000, count);
        connection.close();
        client.close();
    }

    @Test
    public void testRedeclaringTheExchange() throws InterruptedException, IOException {
        Client client = new Client();
        String exchangeName = UUID.randomUUID().toString();
        new Exchange(client, exchangeName);
        new Exchange(client, exchangeName);
        assertTrue("We shouldn't receive any excptions if we redeclare an exchange", true);
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        Channel c = connection.createChannel();
        c.exchangeDelete(exchangeName);
        client.close();
        connection.close();
    }

    @Test
    public void testAsyncPublish() throws IOException, InterruptedException, ExecutionException {
        Client client = new Client();
        String exchangeName = UUID.randomUUID().toString();
        Exchange exchange = new Exchange(client, exchangeName);

        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        Channel c = connection.createChannel();
        String queueName = c.queueDeclare().getQueue();
        c.queueBind(queueName, exchangeName, "#");

        List<Future<Void>> futures = new ArrayList<Future<Void>>(1000);
        for (int i = 0; i < 1000; i++) {
            final String message = "Hello world, this is message # " + i;

            futures.add(exchange.asyncPublish(new Message() {

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
                    return (message).getBytes();
                }
            }));
            LOG.info("Sent: " + message);
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        QueueingConsumer consumer = new QueueingConsumer(c);
        c.basicConsume(queueName, consumer);

        int count = 0;
        while (true) {
            Delivery d = consumer.nextDelivery(2000);
            if (null == d) {
                break;
            }
            LOG.info("Received: " + new String(d.getBody()));
            count++;
        }

        assertEquals(1000, count);

        c.exchangeDelete(exchangeName);
        client.close();
        connection.close();
    }

}
