package org.junyu.kafka.tutorial2;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.Iterator;
import java.util.Properties;

public class ShowProducer {

    private Logger logger = LoggerFactory.getLogger(ShowProducer.class);

    private String filepath;

    private String topic;

    public ShowProducer(String filePath, String topic) {
        this.filepath = filePath;
        this.topic = topic;
    }

    public void start() {
        JSONParser parser = new JSONParser();
        try {
            logger.info("Parsing json file: {}", filepath);
            Object obj = parser.parse(new FileReader(filepath));
            JSONObject jsonObject = (JSONObject) obj;
            String name = (String) jsonObject.get("name");
            JSONObject embedded = (JSONObject)jsonObject.get("_embedded");
            JSONArray episodeList = (JSONArray) embedded.get("episodes");

            KafkaProducer<String, String> producer = initializeProducer();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Stopping application");
                producer.close();
            }));

            Iterator<JSONObject> iterator = episodeList.iterator();
            while (iterator.hasNext()) {
                JSONObject episode = iterator.next();
                Long episodeId = (Long)episode.get("id");
                logger.info("Creating record for episode {}", episodeId);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, episode.toString());
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            logger.error("An error occurred when sending message.", e);
                        }
                    }
                });
            }

        } catch (Exception e) {
            logger.error("An exception occurred", e);
        }
    }

    public KafkaProducer<String, String> initializeProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Config for safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    public static void main(String[] args) {
        String filePath = "/Users/junyuchen/Downloads/tbbt-data.json";
        String topic = "tbbt_episodes";
        ShowProducer producer = new ShowProducer(filePath,topic);
        producer.start();
    }
}
