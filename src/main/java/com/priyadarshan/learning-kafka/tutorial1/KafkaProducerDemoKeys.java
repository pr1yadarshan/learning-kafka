import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerDemoKeys {

    public static void main(String[] args) {

        final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        final String KAFKA_TOPIC = "firstTopic";

        Logger logger = LoggerFactory.getLogger(KafkaProducerDemoKeys.class);

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, "hello world from java");

        //send data
        //since producer.record is asynchronous, we have to use flush
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    //record sent successfully
                    logger.info("Received new metadata. \n" +
                            "Topic" + recordMetadata.topic() + "\n" +
                            "Partition" + recordMetadata.partition() + "\n" +
                            "Offset" + recordMetadata.offset() + "\n" +
                            "TimeStamp" + recordMetadata.timestamp() + "\n");
                } else {
                    // handle error
                    logger.error("Error while producing : ", e);
                }
            }
        });

        //flush
        producer.flush();

        //flush and close
        producer.close();


    }
}