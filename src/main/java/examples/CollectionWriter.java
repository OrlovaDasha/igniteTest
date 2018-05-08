package examples;

import domain.TestObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.ArrayList;
import java.util.List;

public class CollectionWriter {
    private static final String CACHE_NAME = "test";
    public static void main(String[] args) {
        try(Ignite ignite = Ignition.start("example-ignite.xml")) {

            List<TestObject> testObjectList = new ArrayList<>();
            for (Long i = 0L; i < 10_000; i++) {
                testObjectList.add(new TestObject(i, "name" + i, "description"));
            }

            long end_time = writeFullCollection(ignite, testObjectList);
            System.out.println("Writer = " + end_time / Math.pow(10, 9));
        }
    }

    private static Long writeFullCollection(Ignite ignite, List<TestObject> testObjectList) {
        CacheConfiguration<Long, List<TestObject>> cacheConfiguration = new CacheConfiguration<>(CACHE_NAME);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        try (IgniteCache<Long, List<TestObject>> cache = ignite.getOrCreateCache(cacheConfiguration)) {

            long start_time = System.nanoTime();
            for (Long i = 8478L; i < 10_000; i++) {
                System.out.println(i);
                cache.put(i, testObjectList);
            }
            long end_time = System.nanoTime() - start_time;
            return end_time;
        }
    }

    private static Long writeCollectionSeparately(Ignite ignite, List<TestObject> testObjectList) {
        CacheConfiguration<String, TestObject> cacheConfiguration = new CacheConfiguration<>(CACHE_NAME);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        try (IgniteCache<String, TestObject> cache = ignite.getOrCreateCache(cacheConfiguration)) {
            long start_time = System.nanoTime();
            for (Long i = 0L; i < 20; i++) {
                for (TestObject testObject : testObjectList) {
                    System.out.println(i + " " + testObject.getId());
                    cache.put(i + " " + testObject.getId(), testObject);
                }
            }
            long end_time = System.nanoTime() - start_time;
            return end_time;
        }
    }
}
