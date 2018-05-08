package examples;

import domain.TestObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;

public class ClientListener {

    public static void main(String[] args) throws InterruptedException {
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("example-ignite.xml")) {
            try (IgniteCache<Long, TestObject> cache = ignite.getOrCreateCache("test")) {

                ContinuousQuery<Long, TestObject> continuousQuery = new ContinuousQuery<>();

                continuousQuery.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Long, TestObject>() {
                    @Override
                    public boolean apply(Long aLong, TestObject testObject) {
                        return true;
                    }
                }));

                continuousQuery.setLocalListener(new CacheEntryUpdatedListener<Long, TestObject>() {
                    @Override
                    public void onUpdated(Iterable<CacheEntryEvent<? extends Long, ? extends TestObject>> iterable) throws CacheEntryListenerException {
                        for (CacheEntryEvent<? extends Long, ? extends TestObject> e : iterable) {
                            System.out.println("Updated = [" + e.getKey() + " " + e.getEventType() + " " + e.getOldValue() + " " + e.getValue());
                        }
                    }
                });



                try (QueryCursor<Cache.Entry<Long, TestObject>> cur = cache.query(continuousQuery)) {
                    for (Long i = 0L; i < 100; i++) {
                        cache.put(i, new TestObject(i, "name" + i, "description"));
                    }

                    for (Long i = 0L; i < 100; i++) {
                        cache.put(i, new TestObject(i, "name" + i, "description" + i));
                    }

                    for (Long i = 0L; i < 100; i++) {
                        cache.remove(i);
                    }
                    Thread.sleep(30000);
                }
            }
        }
    }
}
