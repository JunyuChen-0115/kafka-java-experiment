package org.junyu.kafka.tutorial3;


import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private static JsonParser jsonParser = new JsonParser();

    private static RestHighLevelClient restClient;

    public ElasticSearchConsumer() {
        restClient = createClient();
    }

    public RestHighLevelClient createClient() {
        logger.info("Initializing RestHighLevelClient...");
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
        return client;
    }

    public KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        String topic = "tbbt_episodes";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public void indexDocument(Map<String, Object> jsonMap) throws IOException {
        logger.info("Indexing document {}", jsonMap.toString());
        IndexRequest indexRequest = new IndexRequest("shows", "tbbt");
        indexRequest.source(jsonMap);
        IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);
        logger.info("Index response received. Id: {}", indexResponse.getId());
    }

    public void indexDocument(String jsonString) throws IOException {
        logger.info("Indexing document {}", jsonString);
        String id = extractIdFromRecord(jsonString);
        // Idempotence: specify id to avoid inserting duplicate records.
        IndexRequest indexRequest = new IndexRequest("shows", "tbbt", id);
        indexRequest.source(jsonString, XContentType.JSON);
        IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);
        logger.info("Index response received. Id: {}", indexResponse.getId());
    }

    private String extractIdFromRecord(String jsonString) {
        return jsonParser.parse(jsonString)
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        ElasticSearchConsumer consumer = new ElasticSearchConsumer();
        KafkaConsumer<String, String> kafkaConsumer = consumer.createKafkaConsumer();

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    consumer.indexDocument(record.value());
                    Thread.sleep(200);
                }
            }
        } catch (IOException | InterruptedException e) {
            logger.info("An exception occurred when indexing.",e);
        }

        restClient.close();

    }
}
