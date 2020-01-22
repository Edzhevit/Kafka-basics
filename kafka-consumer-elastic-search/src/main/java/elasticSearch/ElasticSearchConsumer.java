package elasticSearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private final static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private final static String GROUP_ID = "kafka-demo-elastic-search";
    private final static String OFFSET = "earliest";
    private final static String FALSE = "false";
    private final static String ONE_HUNDRED = "100";

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // poll for new Data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            int recordCount = records.count();
            logger.info("Received: " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {

                // 2 strategies for finding ID

                // 1. Kafka generic ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try {
                    // 2. Twitter feed specific ID
                    String id = extractIdFromTweet(record.value());

                    // where we insert data into ElasticSearch
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                            .source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
                } catch (NullPointerException e){
                    logger.warn("Skipping bad data: " + record.value());
                }
            }

            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing the offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close the client gracefully
//        client.close();
    }

    private static String extractIdFromTweet(String tweet) {
        // gson library
        return jsonParser
                .parse(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private static RestHighLevelClient createClient() {

        // replace with your own credentials
        String hostName = "";
        String username = "";
        String password = "";

        // don't do if you run on local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));


        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ONE_HUNDRED);

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe Consumer to our topics
        consumer.subscribe(Collections.singleton(topic));

        return consumer;
    }
}
