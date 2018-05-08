package examples;

import domain.TestObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;

public class UpdateClient {
    private static final String CACHE_NAME = "test";

    public static void main(String[] args) {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("example-ignite.xml")) {
            try(IgniteCache<Long, TestObject> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                try (IgniteDataStreamer<Long, TestObject> stream = ignite.dataStreamer(CACHE_NAME)) {
                    stream.allowOverwrite(true);
                    for (long i = 0L; i < 100; i++) {
                        stream.addData(i, new TestObject(i, "name" + i, "description" + i));
                    }
                }
            }
        }
    }
}
