import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NATSException;
import io.nats.client.Nats;
import io.nats.streaming.*;

import java.io.*;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public class NatsExample {

    private StreamingConnectionFactory cf;
    private StreamingConnection sc;

    public NatsExample(String clusterId, String clientId) {
        cf = new StreamingConnectionFactory(clusterId, clientId);
        try {
            this.sc  = cf.createConnection();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    public  void subscribe(String subject) {
        assert this.sc != null;
        try {
            Subscription sub = this.sc.subscribe(subject, new io.nats.streaming.MessageHandler() {
                @Override
                public void onMessage(io.nats.streaming.Message message) {
                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message.getData());
                    ObjectInputStream objectInputStream = null;
                    TestObject testObject = null;
                    try {
                        objectInputStream = new ObjectInputStream(byteArrayInputStream);
                        testObject = (TestObject) objectInputStream.readObject();
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    System.out.printf("Received a message: %s\n", testObject);
                }
            }, new SubscriptionOptions.Builder().startWithLastReceived().build());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void publish(String subject, TestObject testObject) {
        assert this.sc != null;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
            outputStream.writeObject(testObject);
            this.sc.publish(subject, byteArrayOutputStream.toByteArray());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

