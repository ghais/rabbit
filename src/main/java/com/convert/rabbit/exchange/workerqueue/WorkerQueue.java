package com.convert.rabbit.exchange.workerqueue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.convert.rabbit.Client;
import com.convert.rabbit.Message;
import com.convert.rabbit.exception.ConvertAmqpException;
import com.convert.rabbit.exchange.Exchange;
import com.convert.rabbit.exchange.IExchange;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * A worker queue implementation that publishes messages to a given exchange/queue
 *
 */
public class WorkerQueue implements IExchange {

    private final Exchange _exchange;

    private final String _workerQueueName;

    private final GenericObjectPool _channelPool;

    private final ExecutorService _executorService;


    /**
     * Create a worker queue that publishes to the given exchange/queue
     *
     * @param exchange
     * @param workerQueueName
     */
    public WorkerQueue(Exchange exchange, String workerQueueName) {
        this._exchange = exchange;
        this._workerQueueName = workerQueueName;
        this._executorService = Executors.newCachedThreadPool();
        this._channelPool = new GenericObjectPool(new PoolableObjectFactory() {

            @Override
            public boolean validateObject(Object obj) {
                Channel c = (Channel) obj;
                return c.isOpen();
            }

            @Override
            public void passivateObject(Object obj) throws Exception {
            }

            @Override
            public Object makeObject() throws Exception {
                Connection connection = _exchange.getClient().getConnection();
                Channel c = connection.createChannel();
                if (null != _exchange.getClient().getMessageListener()) {
                    c.addReturnListener(_exchange.getClient().getMessageListener());
                    c.addShutdownListener(_exchange.getClient().getMessageListener());
                }
                Map<String, Object> args = new HashMap<String, Object>();
                args.put("x-ha-policy", "all"); // mirror queues on all available nodes.
                c.queueDeclare(_workerQueueName, false, false, false, args);
                c.queueBind(_workerQueueName, _exchange.getName(), "");
                return c;
            }

            @Override
            public void destroyObject(Object obj) throws Exception {
                Channel c = (Channel) obj;
                c.close();
            }

            @Override
            public void activateObject(Object obj) throws Exception {
            }
        });

    }

    /**
     * @return
     */
    public Channel borrowChannel() {
        try {
            return (Channel) this._channelPool.borrowObject();
        } catch (Exception e) {
            throw new ConvertAmqpException(e);
        }
    }

    /**
     * Publish a message to the queue.
     *
     * @param msg
     *            the message to publish
     * @throws ConvertAmqpException
     *             in case we encouter an {@link IOException}
     */
    @Override
    public void publish(Message msg) throws ConvertAmqpException {
        Channel channel = this.borrowChannel();
        try {
            channel.basicPublish(_exchange.getName(), "", MessageProperties.PERSISTENT_BASIC, msg.getBody());
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        } finally {
            this.returnChannel(channel);
        }
    }

    /**
     * @param channel
     */
    private void returnChannel(Channel channel) {
        try {
            this._channelPool.returnObject(channel);
        } catch (Exception e) {
            throw new ConvertAmqpException(e);
        }
    }

    public Client getClient() {
        return this._exchange.getClient();
    }

    public String getName() {
        return this._workerQueueName;
    }

    @Override
    public Future<Void> asyncPublish(final Message msg) {
        Callable<Void> callable = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                publish(msg);
                return null;
            }
        };
        return _executorService.submit(callable);
    }

    @Override
    public void shutdown() {
        _exchange.shutdown();
        _executorService.shutdown();
    }

}
