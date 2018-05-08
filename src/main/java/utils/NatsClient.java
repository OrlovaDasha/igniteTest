package utils;

import domain.TestObject;
import io.nats.client.Connection;
import io.nats.streaming.Message;
import io.nats.client.NATSException;
import io.nats.client.Nats;
import io.nats.streaming.*;

import java.io.*;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public class NatsClient {

    private StreamingConnectionFactory cf;
    private StreamingConnection sc;

    public NatsClient(String clusterId, String clientId) {
        cf = new StreamingConnectionFactory(clusterId, clientId);
        try {
            this.sc  = cf.createConnection();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    public  void subscribe(String subject, MessageHandler messageHandler) {
        assert this.sc != null;
        try {
            this.sc.subscribe(subject, messageHandler, new SubscriptionOptions.Builder().startWithLastReceived().build());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void publish(String subject, Object object) {
        assert this.sc != null;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
            outputStream.writeObject(object);
            this.sc.publish(subject, byteArrayOutputStream.toByteArray());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Object geObjectFromMessage(Message message) {
        Object object = null;
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message.getData());
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            object =  objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return object;
    }


}

