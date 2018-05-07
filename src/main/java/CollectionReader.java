import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;

import javax.cache.Cache;
import java.util.List;

public class CollectionReader {
    public static final String CACHE_NAME = "test";

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("example-ignite.xml")) {
            long end_time = readFromFullCollection(ignite, "1");
            System.out.println("Reader= " + end_time / (float) Math.pow(10, 9));
        }
        System.out.println("END Writer");
    }

    private static Long readFromFullCollection(Ignite ignite, String id) {
        CacheConfiguration<String, List<TestObject>> cacheConfiguration = new CacheConfiguration<>(CACHE_NAME);

        try (IgniteCache<String, List<TestObject>> cache = ignite.getOrCreateCache(cacheConfiguration)) {
            long start_time = System.nanoTime();
            Long size = (long) cache.get(id).size();
            System.out.println(size);
            long end_time = System.nanoTime() - start_time;
            return end_time;
        }
    }

    private static Long readFromSeparateCollection(Ignite ignite, String collectionId) {
        CacheConfiguration<String, TestObject> cacheConfiguration = new CacheConfiguration<>(CACHE_NAME);

        try (IgniteCache<String, BinaryObject> cache = ignite.getOrCreateCache(cacheConfiguration).withKeepBinary()) {
            long start_time = System.nanoTime();
            ScanQuery<String, BinaryObject> scan = new ScanQuery<>(
                    (IgniteBiPredicate<String, BinaryObject>) (key, testObject) -> {
                        return key.startsWith(collectionId + " ");
                    }
            );

            List<Cache.Entry<String, BinaryObject>> list = cache.query(scan).getAll();
            Long size = list != null ? (long) list.size() : 0;
            System.out.println(size);
            long end_time = System.nanoTime() - start_time;
            return end_time;
        }finally {
            ignite.destroyCache(CACHE_NAME);
        }
    }
}
