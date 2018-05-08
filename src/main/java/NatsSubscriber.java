public class NatsSubscriber {
    public static void main(String[] args) throws InterruptedException {
        NatsExample natsExample = new NatsExample("test-cluster", "subscriber");
        natsExample.subscribe("foo");
        Thread.sleep(9000);
    }
}
