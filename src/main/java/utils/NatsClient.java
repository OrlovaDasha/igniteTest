package utils;

import io.nats.client.Connection;
import io.nats.client.Message;
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


}

