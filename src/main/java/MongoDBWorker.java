import com.mongodb.client.MongoCollection;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import services.MongoDBService;
import utils.NatsClient;
import utils.TestObjectCRUD;

public class MongoDBWorker {

    private final static String COLLECTION_NAME = "testObjects";

    public static void main(String[] args) {
        MongoDBService mongoDBService = new MongoDBService();
        mongoDBService.connectToMongo();
        Ignite ignite = Ignition.start("example-ignite.xml");
       // mongoDBService.init(ignite);
        NatsClient natsClient = new NatsClient("test-cluster", "mongo");
        mongoDBService.listenForQueries(natsClient, COLLECTION_NAME);
        mongoDBService.listenForUpdates(natsClient, COLLECTION_NAME);
    }
}
