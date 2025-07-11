package example.kafkastream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public final class TargetWordProcessorExample {

    // Set of target words to count
    private static final Set<String> TARGET_WORDS = new HashSet<>(Arrays.asList("kafka", "stream"));

    static class TargetWordCountProcessor implements Processor<String,String, String,String>{
        private KeyValueStore<String, Integer> kvStore;


        @Override
        public void init(final ProcessorContext<String, String> context) {
            context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
                    while (iter.hasNext()) {
                        final KeyValue<String, Integer> entry = iter.next();
                        context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
                    }
                }
            });
            kvStore = context.getStateStore("Counts");
        }

        @Override
        public void process(final Record<String, String> record) {
            final String[] words = record.value().toLowerCase(Locale.getDefault()).split("\\W+");

            for (final String word : words) {
                final Integer oldValue = kvStore.get(word);
                if(TARGET_WORDS.contains(word)) {
                    if (oldValue == null) {
                        kvStore.put(word, 1);
                    } else {
                        kvStore.put(word, oldValue + 1);
                    }
                }
            }
        }

        @Override
        public void close() {
            // close any resources managed by this processor
            // Note: Do not close any StateStores as these are managed by the library
        }
    }


    public static void main(final String[] args) throws Exception {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }

        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Topology builder = new Topology();
        builder.addSource("Source", "streams-plaintext-input");

        builder.addProcessor("Process", TargetWordCountProcessor::new, "Source");

        builder.addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("Counts"),
                        Serdes.String(),
                        Serdes.Integer()),
                "Process");

        builder.addSink("Sink", "target-wordcount-processor-output", "Process");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("target-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
