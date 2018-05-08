import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import domain.TestObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import services.TestObjectCacheService;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GuavaCacheWorker {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start("example-ignite.xml");
        TestObjectCacheService testObjectCacheService = new TestObjectCacheService();
        testObjectCacheService.createCache(ignite);
        LoadingCache<Long, TestObject> cache = testObjectCacheService.getCache();

        long startTime = System.nanoTime();
        TestObject value = cache.get(1L);
        System.out.println(value);
        long endTime = System.nanoTime() - startTime;
        System.out.println("End reading = " + String.format("%.5f", endTime / (float) Math.pow(10, 9)));

        startTime = System.nanoTime();
        value = cache.get(1L);
        System.out.println(value);
        endTime = System.nanoTime() - startTime;
        System.out.println("End reading = " + String.format("%.5f", endTime / (float) Math.pow(10, 9)));


    }
}


