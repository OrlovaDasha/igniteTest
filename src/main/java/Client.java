import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.stream.StreamVisitor;

import java.util.Random;

public class Client {
    private static final String CACHE_NAME = "test";
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("example-ignite.xml")) {
            if (ignite.cluster().forServers().nodes().isEmpty()) {
                return;
            }

            CacheConfiguration<Long, TestObject> testObjectCacheConf = new CacheConfiguration<>(CACHE_NAME);

            try (IgniteCache<Long, TestObject> cache = ignite.getOrCreateCache(testObjectCacheConf)) {

                try (IgniteDataStreamer<Long, TestObject> stream = ignite.dataStreamer(cache.getName())) {


                    for (int i = 1; i <= 1000; i++) {
                        Long idx = RANDOM.nextLong();

                        stream.addData(idx, new TestObject(idx, "name" + idx, "description"));
                        if (i % 500 == 0)
                            System.out.println("Number of tuples streamed into Ignite: " + i);
                    }
                }
            }
        }
    }
}

