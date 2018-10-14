package org.junyu.kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static final String bootstrapServers = "127.0.0.1:9092";



    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String topic = "first_topic";
        for (int i = 0; i < 10; i++) {
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            logger.info("key: " + key);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: {} \nPartition: {} \nOffset: {}\nTimestamp: {}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());

                    } else {
                        logger.error("An exception occurred", exception);
                    }
                }
            }).get();
        }

        producer.close();
    }
}
