package demo.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public final static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public final static String OFFSET = "earliest";
    public final static String TOPIC = "first_topic";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET);

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
        int offsetToReadFrom = 15;
        consumer.assign(Collections.singletonList(partitionToReadFrom));
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;

        // poll for new Data
        while (numberOfMessagesToRead > 0){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesToRead--;
                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
            }
        }

        logger.info("Exiting the application!");
    }
}
