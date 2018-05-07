import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GuavaCacheWorker {
    public static LoadingCache<Long, List<TestObject>> cache;
    public static final String CACHE_NAME = "test";
    public static ContinuousQuery<Long, List<TestObject>> continuousQuery = new ContinuousQuery<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("example-ignite.xml")) {
            try (IgniteCache<Long, List<TestObject>> igniteCache = ignite.getOrCreateCache(CACHE_NAME)) {
                CacheLoader<Long, List<TestObject>> loader = new CacheLoader<Long, List<TestObject>>() {
                    @Override
                    public List<TestObject> load(Long key) throws Exception {
                        List<TestObject> value = igniteCache.get(key);
                        if (value == null)  {
                            value = Collections.EMPTY_LIST;
                        }
                        return value;
                    }
                };

                cache = CacheBuilder.newBuilder().build(loader);

                ExecutorService executorService = Executors.newSingleThreadExecutor();
                Runnable runnable = () -> {
                    continuousQuery.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Long, List<TestObject>>() {
                        @Override
                        public boolean apply(Long aLong, List<TestObject> testObject) {
                            return true;
                        }
                    }));
                    continuousQuery.setLocalListener(new CacheEntryUpdatedListener<Long, List<TestObject>>() {
                        @Override
                        public void onUpdated(Iterable<CacheEntryEvent<? extends Long, ? extends List<TestObject>>> iterable) throws CacheEntryListenerException {
                            for (CacheEntryEvent<? extends Long, ? extends List<TestObject>> e : iterable) {
                                cache.put(e.getKey(), e.getValue());
                                System.out.println("Updated = [" + e.getKey() + " " + e.getEventType() + " " + e.getOldValue() + " " + e.getValue());
                            }
                        }
                    });
                    try (QueryCursor<javax.cache.Cache.Entry<Long, List<TestObject>>> cur = igniteCache.query(continuousQuery)) {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                };
                executorService.submit(runnable);

                long startTime = System.nanoTime();
                List<TestObject> value = cache.get(1L);
                System.out.println(value);
                long endTime = System.nanoTime() - startTime;
                System.out.println("End reading = " + String.format("%.5f", endTime / (float) Math.pow(10, 9)));

                startTime = System.nanoTime();
                value = cache.get(1L);
                System.out.println(value);
                endTime = System.nanoTime() - startTime;
                System.out.println("End reading = " + String.format("%.5f", endTime / (float) Math.pow(10, 9)));

                Thread.sleep(10000);
                startTime = System.nanoTime();
                value = cache.get(1L);
                System.out.println(value);
                endTime = System.nanoTime() - startTime;
                System.out.println("End reading = " + String.format("%.5f", endTime / (float) Math.pow(10, 9)));

                executorService.shutdown();
            }
        }
    }
}
