package example.clientmulti;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MockMultiConsumerExperiment {

    public static void main(String[] args) throws InterruptedException {
        String topic = "test-topic-new";
        String groupId = "experiment-group";
        int numConsumers = 6;

        ExecutorService executor = Executors.newCachedThreadPool();
        List<MockSimpleConsumer> consumers = new ArrayList<>();

        // Start consumers
        for (int i = 0; i < numConsumers; i++) {
            String consumerId = "consumer-" + i;
            MockSimpleConsumer consumer = new MockSimpleConsumer(consumerId, topic, groupId);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        // Let them run for 10 seconds
        Thread.sleep(10000);

        // Kill one consumer
        System.out.println("\n=== Simulating failure: stopping consumer-0 ===\n");
        consumers.get(0).shutdown();

        // Let remaining run for 10 more seconds
        Thread.sleep(10000);

        // Kill one consumer
        System.out.println("\n=== Simulating failure: stopping consumer-1 ===\n");
        consumers.get(1).shutdown();

        // Let remaining run for 10 more seconds
        Thread.sleep(10000);

        // Kill one consumer
        System.out.println("\n=== Simulating failure: stopping consumer-2 ===\n");
        consumers.get(2).shutdown();

        // Let remaining run for 10 more seconds
        Thread.sleep(10000);

        // Restart one consumer
        System.out.println("\n=== Simulating restart: restarting consumer-1 ===\n");
        MockSimpleConsumer newConsumer1 = new MockSimpleConsumer("consumer-1", topic, groupId); // Create a new instance
        consumers.set(1, newConsumer1);
        executor.submit(newConsumer1);

        // Let remaining run for 10 more seconds
        Thread.sleep(10000);

        // Start one consumer
        System.out.println("\n=== Simulating restart: start consumer-2 ===\n");
        MockSimpleConsumer newConsumer2 = new MockSimpleConsumer("consumer-2", topic, groupId); // Create a new instance
        consumers.set(2, newConsumer2);
        executor.submit(newConsumer2);

        // Let remaining run for 10 more seconds
        Thread.sleep(10000);

        // Shutdown all
        System.out.println("\n=== Shutting down all consumers ===\n");
        for (MockSimpleConsumer consumer : consumers) {
            consumer.shutdown();
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
