package org.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String HIGH_TOPIC = "high-priority-mails";
    private static final String LOW_TOPIC  = "low-priority-mails";
    // Using "-tuned" group to ensure a fresh start for this new logic
    private static final String HIGH_GROUP = "high-mail-consumer-group-tuned";
    private static final String LOW_GROUP  = "low-mail-consumer-group-tuned";

    private static final ExecutorService consumerRunnerPool = Executors.newFixedThreadPool(11);
    private static final ExecutorService highWorkers;
    private static final ExecutorService lowWorkers;

    private static final List<KafkaEmailConsumer> activeConsumers = new ArrayList<>();

    // --- 1. DEFINE YOUR PERFORMANCE HERE ---
    // This number sets BOTH the Thread Pool Size AND the Kafka Batch Size.
    private static final int HIGH_THREADS = 10;
    private static final int LOW_THREADS  = 40;

    static {
        logger.info("Configuring System | High Speed: {} | Low Speed: {}", HIGH_THREADS, LOW_THREADS);

        // 2. Create Pools based on the defined numbers
        highWorkers = Executors.newFixedThreadPool(HIGH_THREADS);
        lowWorkers  = Executors.newFixedThreadPool(LOW_THREADS);
    }

    public static void main(String[] args) throws Exception {

        // --- Start High Priority Consumers ---
        for (int i = 0; i < 4; i++) {
            // 3. Pass HIGH_THREADS as the 'batchSize'
            KafkaEmailConsumer consumer = new KafkaEmailConsumer(
                    HIGH_TOPIC, HIGH_GROUP, i, highWorkers, HIGH_THREADS
            );
            activeConsumers.add(consumer);
            consumerRunnerPool.submit(consumer);
        }

        // --- Start Low Priority Consumers ---
        for (int i = 0; i < 7; i++) {
            // 4. Pass LOW_THREADS as the 'batchSize'
            KafkaEmailConsumer consumer = new KafkaEmailConsumer(
                    LOW_TOPIC, LOW_GROUP, i, lowWorkers, LOW_THREADS
            );
            activeConsumers.add(consumer);
            consumerRunnerPool.submit(consumer);
        }

        // --- Graceful Shutdown ---
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping consumers...");
            for (KafkaEmailConsumer c : activeConsumers) c.shutdown();
            consumerRunnerPool.shutdown();
            await(consumerRunnerPool, 20);
            highWorkers.shutdown();
            lowWorkers.shutdown();
            await(highWorkers, 10);
            await(lowWorkers, 10);
            logger.info("Bye!");
        }));

        Thread.currentThread().join();
    }

    private static void await(ExecutorService exec, long seconds) {
        try {
            if (!exec.awaitTermination(seconds, TimeUnit.SECONDS)) {
                logger.error("Forcing shutdown for {}", exec);
                exec.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}