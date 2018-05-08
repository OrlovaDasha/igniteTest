package services;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import domain.TestObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.bson.Document;
import utils.NatsClient;
import utils.TestObjectCRUD;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Properties;
import java.util.function.Consumer;

public class MongoDBService {

    private static MongoClient client;
    private static MongoDatabase db;
    private static MongoCredential credential;
    private static Properties properties;
    private static final String CACHE_NAME = "test";

    static {
        properties = new Properties();
        properties.setProperty("host", "localhost");
        properties.setProperty("port", "27017");
        properties.setProperty("dbname", "test");
        properties.setProperty("login", "dasha");
        properties.setProperty("password", "12345");
    }

    public  void connectToMongo() {
            client = new MongoClient(properties.getProperty("host"), Integer.valueOf(properties.getProperty("port")));
            db = client.getDatabase(properties.getProperty("dbname"));
            credential = MongoCredential.createCredential(properties.getProperty("login"),
                    properties.getProperty("dbname"), properties.getProperty("password").toCharArray());
    }

    public void init (Ignite ignite){
        try (IgniteCache<Long, TestObject> cache = ignite.getOrCreateCache(CACHE_NAME)) {
            try (IgniteDataStreamer<Long, TestObject> stream = ignite.dataStreamer(CACHE_NAME)) {
                for (String collectionName : db.listCollectionNames()) {
                    MongoCollection collection = db.getCollection(collectionName);
                    collection.find().forEach((Consumer<? extends Document>) document -> {
                        TestObject testObject = TestObjectCRUD.fromDocument(document);
                        stream.addData(testObject.getId(), testObject);
                    });
                }
            }
        }
    }

    public void listenForUpdates(NatsClient nats, String collectionName) {
        MongoCollection collection = db.getCollection(collectionName);
        collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).forEach((Block<ChangeStreamDocument<Document>>) documentChangeStreamDocument -> {
            System.out.println(documentChangeStreamDocument);
            TestObject testObject = TestObjectCRUD.fromDocument(documentChangeStreamDocument.getFullDocument());
            System.out.println(testObject);
            nats.publish("toCache", testObject);
        });
    }

    public void listenForQueries(NatsClient nats, String collectionName) {
        MongoCollection collection = db.getCollection(collectionName);
        nats.subscribe("fromCache", message -> {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message.getData());
                ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
                Long id = (Long) objectInputStream.readObject();
                TestObjectCRUD testObjectCRUD = new TestObjectCRUD();
                testObjectCRUD.setTable(collection);
                TestObject testObject = testObjectCRUD.getById(id);
                nats.publish("toCache", testObject);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
    }

    public static MongoClient getClient() {
        return client;
    }

    public static void setClient(MongoClient client) {
        MongoDBService.client = client;
    }

    public static MongoDatabase getDb() {
        return db;
    }

    public static void setDb(MongoDatabase db) {
        MongoDBService.db = db;
    }
}
