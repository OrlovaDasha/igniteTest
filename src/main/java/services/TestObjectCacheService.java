package services;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import domain.TestObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import utils.NatsClient;

import javax.cache.event.CacheEntryEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestObjectCacheService {
    private LoadingCache<Long, TestObject> cache;
    private IgniteCache<Long, TestObject> igniteCache;
    private static final String CACHE_NAME = "test";

    public void createCache(Ignite ignite) {
        NatsClient natsClient = new NatsClient("test-cluster", "subscriber");
        igniteCache = ignite.getOrCreateCache(CACHE_NAME);

        natsClient.subscribe("toCache", message -> {
            TestObject testObject = (TestObject) natsClient.geObjectFromMessage(message);
            System.out.println("Received: " + testObject);
            igniteCache.put(testObject.getId(), testObject);
        });

        CacheLoader<Long, TestObject> loader = new CacheLoader<Long, TestObject>() {
            @Override
            public TestObject load(Long key) throws Exception {
                TestObject value = igniteCache.get(key);
                CountDownLatch doneSignal;
                boolean done = false;
                if (value == null) {
                    natsClient.publish("fromCache", key);
                    doneSignal = new CountDownLatch(1);
                    natsClient.subscribe("result", message -> {
                        TestObject testObject = (TestObject) natsClient.geObjectFromMessage(message);
                        System.out.println("Result received: " + testObject);
                        igniteCache.put(testObject.getId(), testObject);
                        doneSignal.countDown();
                    });
                    done = doneSignal.await(1000, TimeUnit.MILLISECONDS);
                    if (done) {
                        value = igniteCache.get(key);
                    } else {
                        value = new TestObject(null, null, null);
                    }
                }
                return value;
            }
        };
        cache = CacheBuilder.newBuilder().build(loader);

       ContinuousQuery<Long, TestObject> continuousQuery = new ContinuousQuery<>();
       continuousQuery.setInitialQuery(new ScanQuery<>((IgniteBiPredicate<Long, TestObject>) (aLong, testObject) -> true));
       continuousQuery.setLocalListener(iterable -> {
            for (CacheEntryEvent<? extends Long, ? extends TestObject> e : iterable) {
                System.out.println("Updated = [" + e.getKey() + " " + e.getEventType() + " " + e.getOldValue() + " " + e.getValue());
            }
        });
       igniteCache.query(continuousQuery).forEach(entry -> cache.put(entry.getKey(), entry.getValue()));
    }

    public LoadingCache<Long, TestObject> getCache() {
        return cache;
    }

    public IgniteCache<Long, TestObject> getIgniteCache() {
        return igniteCache;
    }

}
