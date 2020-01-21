package demo.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    public final static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public final static String GROUP_ID = "my-sixth-application";
    public final static String OFFSET = "earliest";
    public final static String TOPIC = "first_topic";

    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerThread(CountDownLatch latch, String bootstrapServers, String groupId, String topic) {
        this.latch = latch;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET);
        this.consumer = new KafkaConsumer<String, String>(properties);
        this.consumer.subscribe(Collections.singleton(TOPIC));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + " Value: " + record.value());
                    logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        } catch (WakeupException ex) {
            logger.info("Received shutdown signal!");
        } finally {
            consumer.close();
            // tell our main code that we are done  with the consumer
            latch.countDown();
        }

    }

    public void shutdown() {
        // the wakeup() method is a special method  to interrupt consumer.poll()
        // it will throw WakeUpException
        consumer.wakeup();
    }
}
