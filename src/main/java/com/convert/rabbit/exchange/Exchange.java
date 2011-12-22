package com.convert.rabbit.exchange;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.convert.rabbit.Client;
import com.convert.rabbit.Message;
import com.convert.rabbit.exception.ConvertAmqpException;
import com.rabbitmq.client.Channel;

/**
 * This is a thread safe way to talk to an exchange. All the channels used by the exchange are pooled.
 *
 */
public class Exchange implements IExchange {

    private final String _name;

    private final Client _client;

    private final GenericObjectPool _channelPool;

    private final ExecutorService _executorService = Executors.newCachedThreadPool();

    /**
     * An exchange that is is declared from the given client for the given name.
     *
     * @param client
     * @param name
     */
    public Exchange(Client client, String name) {
        this._client = client;
        this._name = name;

        _channelPool = new GenericObjectPool(new PoolableObjectFactory() {

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
                Channel c = _client.getConnection().createChannel();
                if (null != _client.getMessageListener()) {
                    c.addReturnListener(_client.getMessageListener());
                    c.addShutdownListener(_client.getMessageListener());
                }
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

        try {
            Channel c = this.borrowChannel();
            c.exchangeDeclare(_name, "topic", true, false, null);
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        }

    }

    /**
     * Publish a given message to this exchange.
     *
     * @param msg
     *            the message.
     *
     * @throws ConvertAmqpException
     *             in case we encounter an {@link IOException}.
     */
    @Override
    public void publish(Message msg) throws ConvertAmqpException {
        Channel channel = this.borrowChannel();
        try {
            channel.basicPublish(_name, msg.getRoutingKey(), msg.isMandatory(), msg.isImmedaite(), null, msg.getBody());
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        } finally {
            this.returnChannel(channel);
        }
    }

    /**
     * Publish a given message to this exchange asynchronously.
     *
     * @param msg
     *            the message.
     *
     * @return a {@link Future}
     * @throws ConvertAmqpException
     *             in case we encounter an {@link IOException}.
     */
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

    /**
     * @return
     */
    private Channel borrowChannel() {
        try {
            return (Channel) this._channelPool.borrowObject();
        } catch (Exception e) {
            throw new ConvertAmqpException(e);
        }
    }

    /**
     * Get the client
     *
     * @return
     */
    public Client getClient() {
        return _client;
    }

    public String getName() {
        return this._name;
    }

    /**
     *
     */
    public void shutdown() {
        _executorService.shutdown();
    }

}
