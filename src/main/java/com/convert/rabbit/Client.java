package com.convert.rabbit;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.convert.rabbit.exception.ConvertAmqpException;
import com.convert.rabbit.exchange.Exchange;
import com.convert.rabbit.exchange.workerqueue.WorkerQueue;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;

/**
 * The client is the top level abstraction. It provides a thread safe mechanism to talk to RabbitMQ. <br>
 * It provides getExchange and getWorkerQueue methods that use pooled channels to talk to the respective exchanges and
 * workerQueues.
 *
 */
public class Client {

    /**
     * A listener that combines the {@link ReturnListener} and the {@link ShutdownListener}
     *
     * @author Ghais Issa <ghais.issa@convertglobal.com>
     *
     */
    public static interface MessageListener extends ReturnListener, ShutdownListener {};

    private final ConcurrentMap<String, Exchange> _exchanges = new ConcurrentHashMap<String, Exchange>();

    private final ConcurrentMap<String, WorkerQueue> _workerQueues = new ConcurrentHashMap<String, WorkerQueue>();

    private final ConnectionFactory _factory;

    private final Connection _connection;

    private final MessageListener _messageListener;

    /**
     * Creates a default connection bound to the local host and default port.
     *
     * @throws ConvertAmqpException
     *             in case of an {@link IOException} when trying to create the connection.
     */
    public Client() throws ConvertAmqpException {
        _factory = new ConnectionFactory();
        try {
            _connection = _factory.newConnection();
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        }
        _messageListener = null;
    }

    /**
     * Creates a Client with the given message listener.
     *
     * @param MessageListener
     *            a message listener to use for all messages.
     *
     * @throws ConvertAmqpException
     *             a runtime exception in case of an {@link IOException}.
     * @throws NullPointerException
     *             if the messageListener is null.
     */
    public Client(MessageListener messageListener) throws ConvertAmqpException, NullPointerException {
        _factory = new ConnectionFactory();
        try {
            _connection = _factory.newConnection();
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        }
        this._messageListener = checkNotNull(messageListener);
    }

    /**
     * Create a client that connects to rabbitmq on the given host address.
     *
     * @param host
     *            the host address.
     * @throws ConvertAmqpException
     *             in case of an {@link IOException}
     */
    public Client(String host) throws ConvertAmqpException {
        _factory = new ConnectionFactory();
        _factory.setHost(host);
        try {
            _connection = _factory.newConnection();
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        }
        this._messageListener = null;
    }

    /**
     * A client that connects to a host and uses the listener to respnd to meesaging events.
     *
     * @param host
     *            the host address.
     * @param listener
     *            a {@link MessageListener} implementation
     * @throws NullPointerException
     *             if the listener is null.
     * @throws ConvertAmqpException
     *             in case of an {@link IOException}
     */
    public Client(String host, MessageListener listener) throws NullPointerException, ConvertAmqpException {
        _factory = new ConnectionFactory();
        _factory.setHost(host);
        try {
            _connection = _factory.newConnection();
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        }
        this._messageListener = checkNotNull(listener);
    }

    public Client(String host, int port) {
        _factory = new ConnectionFactory();
        _factory.setHost(host);
        _factory.setPort(port);

        try {
            _connection = _factory.newConnection();
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        }
        this._messageListener = null;
    }

    /**
     * Create a client that connects to a RabbitMQ server at the given host and port, and responds to events with the
     * given listener
     *
     * @param host
     * @param port
     * @param listener
     * @throws NullPointerException
     *             if the listener is null.
     * @throws ConvertAmqpException
     *             in case of an {@link IOException}
     */
    public Client(String host, int port, MessageListener listener) throws NullPointerException, ConvertAmqpException {
        _factory = new ConnectionFactory();
        _factory.setHost(host);
        _factory.setPort(port);

        try {
            _connection = _factory.newConnection();
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        }
        this._messageListener = checkNotNull(listener);

    }

    /**
     * Return an exchange with the given name if one is already cached, or creates a new instance and cache it
     * otherwise.
     *
     * @param name
     *            the name of the exchange
     * @return an instance of the exchange with the given name
     */
    public Exchange getExchange(String name) {
        Exchange exchange = _exchanges.get(name);
        if (null != exchange) {
            return exchange;
        }

        exchange = new Exchange(this, name);
        Exchange temp = _exchanges.putIfAbsent(name, exchange);
        if (null != temp) {
            return temp;
        }
        return exchange;
    }

    /**
     * Create a worker queue bound to a given exchange and a given name.
     *
     * @param exchangeName
     * @param queueName
     * @return
     */
    public WorkerQueue getWorkerQueue(String exchangeName, String queueName) {
        WorkerQueue workerQueue = _workerQueues.get(queueName);
        if (null != workerQueue) {
            return workerQueue;
        }

        Exchange exchange = getExchange(exchangeName);
        workerQueue = new WorkerQueue(exchange, queueName);
        WorkerQueue temp = _workerQueues.putIfAbsent(queueName, workerQueue);
        if (null != temp) {
            return temp;
        }

        return workerQueue;
    }

    /**
     * Return the message lister if one is associated with this client or null otherwise.
     *
     * @return the message listener if one exists or null otherwise.
     */
    public MessageListener getMessageListener() {
        return _messageListener;
    }

    /**
     * @return the connection
     */
    public Connection getConnection() {
        return _connection;
    }

    private <T> T checkNotNull(T t) {
        if (null == t) {
            throw new NullPointerException();
        }
        return t;
    }

    public synchronized void close() {
        for (Exchange e : _exchanges.values()) {
            e.shutdown();
        }
        try {
            _connection.close();
        } catch (IOException e) {
            throw new ConvertAmqpException(e);
        }
    }
}
