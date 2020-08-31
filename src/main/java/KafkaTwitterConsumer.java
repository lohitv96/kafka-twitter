import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.log4j.BasicConfigurator;

public class KafkaTwitterConsumer {

    public static void main(String[] args) {
        System.out.println("Started running the Kafka Consumer");

        // configure basic console logging
        BasicConfigurator.configure();

        // try defining a new consumer
        try (Consumer<Long, String> consumer = getConsumer()) {
            while (true) {
                // subscribe to the message topic
                consumer.subscribe(Arrays.asList(KafkaConfiguration.TOPIC));
                // obtain the consumer records for the topic
                ConsumerRecords<Long, String> records = consumer.poll(1000);
                System.out.println("Size: " + records.count());
                // print the retieved records
                for (ConsumerRecord<Long, String> record : records) {
                    System.out.println("Received a message: " + record.key() + " " + record.value());
                }
            }
        } finally {
            System.out.println("End");
        }
    }

    private static Consumer<Long, String> getConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfiguration.GROUP);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaConsumer<Long, String>(properties);
    }

}