package examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;

import java.util.UUID;

import org.apache.ignite.events.EventType.*;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

public class Listener {
    private static final String CACHE_NAME = "test";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException, InterruptedException {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> TestObjectCacheService events example started.");

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                // This optional local callback is called for each event notification
                // that passed remote predicate listener.
                IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                    @Override public boolean apply(UUID uuid, CacheEvent evt) {
                        System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() +
                                ", oldVal=" + evt.oldValue() + ", newVal=" + evt.newValue());

                        return true; // Continue listening.
                    }
                };

                // Remote listener which only accepts events for keys that are
                // greater or equal than 10 and if event node is primary for this key.
                IgnitePredicate<CacheEvent> rmtLsnr = new IgnitePredicate<CacheEvent>() {
                    @Override public boolean apply(CacheEvent evt) {
                        System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() +
                                ", oldVal=" + evt.oldValue() + ", newVal=" + evt.newValue());

                       return true;
                    }
                };

                System.out.println("Before listening");

                // Subscribe to specified cache events on all nodes that have cache running.
                // TestObjectCacheService events are explicitly enabled in examples/config/example-ignite.xml file.
                ignite.events(ignite.cluster().forCacheNodes(CACHE_NAME)).remoteListen(locLsnr, null,
                        EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);

                // Wait for a while while callback is notified about remaining puts.
                Thread.sleep(15000);
            }
            finally {
                System.out.println("End listening");
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }
}
