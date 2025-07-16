package example.clientmulti;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.common.config.SaslConfigs.*;


public class MockSimpleProducer {

    public static void main(String[] args) throws InterruptedException {
        String topic = "test-topic-new";

        Properties props = new Properties();
        props.put("bootstrap.servers", "<BOOTSTRAP_SERVER>");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(SASL_JAAS_CONFIG,         "org.apache.kafka.common.security.plain.PlainLoginModule required username='<API_KEY>' password='<API_SECRET>';");

        props.put(KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getCanonicalName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ACKS_CONFIG,                   "all");
        props.put(SECURITY_PROTOCOL_CONFIG,      "SASL_SSL");
        props.put(SASL_MECHANISM,                "PLAIN");
        // Add for New protocol
        props.put("group.protocol", "consumer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int i = 0;
        try {
            while (true) {
                String message = "message-" + i++;
                producer.send(new ProducerRecord<>(topic, "key-"+i, message));
                System.out.println("[Producer] Sent: " + message);
                Thread.sleep(200);  // send 5 messages per second
            }
        } finally {
            producer.close();
        }
    }
}