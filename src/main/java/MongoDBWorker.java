import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.bson.Document;

import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Set;

public class MongoDBWorker {

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

    private static void connectToMongo() {
            client = new MongoClient(properties.getProperty("host"), Integer.valueOf(properties.getProperty("port")));
            db = client.getDatabase(properties.getProperty("dbname"));
            credential = MongoCredential.createCredential(properties.getProperty("login"),
                    properties.getProperty("dbname"), properties.getProperty("password").toCharArray());
    }

        private static void init (Ignite ignite){
            try (IgniteCache<Long, TestObject> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                try (IgniteDataStreamer<Long, TestObject> stream = ignite.dataStreamer(CACHE_NAME)) {
                    for (String collectionName : db.listCollectionNames()) {
                        MongoCollection collection = db.getCollection(collectionName);
                        TestObjectCRUD testObjectCRUD = new TestObjectCRUD();
                        testObjectCRUD.setTable(collection);
                        stream.addData(testObjectCRUD.getAll());
                    }
                }
            }
        }

        private static void create () {
            MongoCollection collection = db.getCollection("testObjects");
            TestObjectCRUD testObjectCRUD = new TestObjectCRUD();
            testObjectCRUD.setTable(collection);

            for (Long i = 0L; i < 1_000_000; i++) {
                testObjectCRUD.add(new TestObject(i, "name" + i, "description"));
            }
        }

        public static void main (String[]args){
            Block<ChangeStreamDocument<Document>> printBlock = new Block<ChangeStreamDocument<Document>>() {
                @Override
                public void apply(ChangeStreamDocument<Document> documentChangeStreamDocument) {
                    System.out.println(documentChangeStreamDocument);
                }
            };

            connectToMongo();
            //Ignite ignite = Ignition.start("example-ignite.xml"))
            MongoCollection collection = db.getCollection("testObjects");
            collection.watch().forEach(printBlock);
        }
    }
