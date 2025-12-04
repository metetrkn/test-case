package org.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaEmailConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEmailConsumer.class);

    private final String topic;
    private final String groupId;
    private final int consumerIndex;
    private final ExecutorService emailExecutor;

    // 1. Field to store the dynamic batch size
    private final int batchSize;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private KafkaConsumer<String, String> consumer;

    // 2. Updated Constructor
    public KafkaEmailConsumer(String topic, String groupId, int consumerIndex, ExecutorService emailExecutor, int batchSize) {
        this.topic = topic;
        this.groupId = groupId;
        this.consumerIndex = consumerIndex;
        this.emailExecutor = emailExecutor;
        this.batchSize = batchSize; // Store the value passed from Main
    }

    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 3. Apply the dynamic batch size
        // If Main passed 10, this is 10. If Main passed 40, this is 40.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(batchSize));

        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
            logger.info("Started consumer {} for topic: {} with BatchSize: {}", consumerIndex, topic, batchSize);

            while (running.get()) {
                ConsumerRecords<String, String> records;

                try {
                    records = consumer.poll(Duration.ofMillis(100));
                } catch (WakeupException e) {
                    continue;
                }

                if (records.isEmpty()) continue;

                List<CompletableFuture<Void>> futures = new ArrayList<>();

                for (ConsumerRecord<String, String> record : records) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        try {
                            JSONObject json = new JSONObject(record.value());
                            String to = json.getString("to");
                            String subject = json.getString("subject");
                            String body = json.getString("body");
                            long creationTime = json.getLong("createdAt");

                            EmailSender.sendEmail(to, subject, body);

                            long sendTime = System.currentTimeMillis();
                            logger.info("Topic: {} | Consumer: {} | Created: {} | Sent: {}",
                                    topic, consumerIndex, creationTime, sendTime);

                        } catch (JSONException e) {
                            logger.warn("SKIPPING bad JSON in {}", topic);
                        } catch (Exception e) {
                            logger.error("Retry needed: {}", e.getMessage());
                            throw new RuntimeException(e);
                        }
                    }, emailExecutor);
                    futures.add(future);
                }

                try {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                    consumer.commitSync();
                } catch (Exception e) {
                    logger.error("Batch failed. Not committing.");
                }
            }
        } catch (Exception e) {
            logger.error("Critical Error: {}", e.getMessage(), e);
        } finally {
            if (consumer != null) consumer.close();
            logger.info("Consumer {} stopped gracefully.", consumerIndex);
        }
    }
}