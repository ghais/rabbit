package com.convert.rabbit.exchange;

import static com.yammer.metrics.Metrics.newMeter;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.convert.rabbit.Client;
import com.convert.rabbit.Message;
import com.convert.rabbit.exception.ConvertAmqpException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.rabbitmq.client.Channel;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.MeterMetric;

/**
 * This is a thread safe way to talk to an exchange. All the channels used by the exchange are pooled.
 * 
 */
public class Exchange implements IExchange {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    private final String _name;

    private final Client _client;

    private final GenericObjectPool _channelPool;

    private final ListeningExecutorService _executorService = MoreExecutors.listeningDecorator(Executors
            .newCachedThreadPool());

    private final MeterMetric requests;

    private final MeterMetric asyncRequests;

    /**
     * An exchange that is is declared from the given client for the given name.
     * 
     * @param client
     * @param name
     * @throws IOException
     */
    public Exchange(Client client, String name) throws IOException {
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

        Channel c = this.borrowChannel();
        c.exchangeDeclare(_name, "topic", true, false, null);

        requests = newMeter(Exchange.class, this._name + " requests", "requests", SECONDS);

        asyncRequests = newMeter(Exchange.class, this._name + " async-requests", "async-requests", SECONDS);

        Metrics.newGauge(Exchange.class, this._name + " active-channels", new GaugeMetric<Integer>() {

            @Override
            public Integer value() {
                return _channelPool.getNumActive();
            }
        });

        Metrics.newGauge(Exchange.class, this._name + " idle-channels", new GaugeMetric<Integer>() {

            @Override
            public Integer value() {
                return _channelPool.getNumIdle();
            }
        });

    }

    /**
     * Publish a given message to this exchange.
     * 
     * @param msg
     *            the message.
     * 
     * 
     * @throws IOException
     */
    @Override
    public void publish(Message msg) throws IOException {
        requests.mark();
        Channel channel = this.borrowChannel();
        try {
            channel.basicPublish(_name, msg.getRoutingKey(), msg.isMandatory(), msg.isImmedaite(), null, msg.getBody());
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
     */
    @Override
    public ListenableFuture<Void> asyncPublish(final Message msg) {
        asyncRequests.mark();
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
            LOG.severe(e.getMessage());
            // swallowed.
        }
    }

    /**
     * @return
     * @throws Exception
     */
    private Channel borrowChannel() throws IOException {
        try {
            return (Channel) this._channelPool.borrowObject();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            LOG.severe(e.getMessage());
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
     * @throws Exception
     * 
     */
    @Override
    public void shutdown() throws IOException {
        _executorService.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!_executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                _executorService.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!_executorService.awaitTermination(60, TimeUnit.SECONDS))
                    LOG.severe("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            _executorService.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

        try {
            _channelPool.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
