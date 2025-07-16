package example.kafkaclient;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

public class MockSimpleConsumer implements Runnable, ConsumerRebalanceListener {

    private final String id;
    private final KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private Instant lastRevokedTime;
    private Instant lastAssignedTime;

    public MockSimpleConsumer(String id, String topic, String groupId) {
        this.id = id;
        Properties props = new Properties();
        props.put("bootstrap.servers", "<BOOTSTRAP_SERVER>");
        props.put(SASL_JAAS_CONFIG,         "org.apache.kafka.common.security.plain.PlainLoginModule required username='<API_KEY>' password='<API_SECRET>';");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getCanonicalName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(SECURITY_PROTOCOL_CONFIG,        "SASL_SSL");
        props.put(SASL_MECHANISM,                  "PLAIN");

        // Add for New protocol
        props.put("group.protocol", "consumer");
        
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic), this);
    }

    @Override
    public void run() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[%s] Received: partition=%d offset=%d value=%s%n",
                        id, record.partition(), record.offset(), record.value());
                }
            }
        } catch (WakeupException e) {
            if (running) throw e;  // only rethrow if unexpected
        } finally {
            consumer.close();
            shutdownLatch.countDown();
            System.out.printf("[%s] Closed%n", id);
        }
    }

    public void shutdown() throws InterruptedException {
        running = false;
        consumer.wakeup();
        shutdownLatch.await();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        lastRevokedTime = Instant.now();
        System.out.printf("[%s] Partitions revoked: %s%n", id, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        lastAssignedTime = Instant.now();
        if (lastRevokedTime != null) {
            Duration downtime = Duration.between(lastRevokedTime, lastAssignedTime);
            System.out.printf("[%s] Downtime due to rebalancing: %d ms%n", id, downtime.toMillis());
        }
        System.out.printf("[%s] Partitions assigned: %s%n", id, partitions);
    }
}
