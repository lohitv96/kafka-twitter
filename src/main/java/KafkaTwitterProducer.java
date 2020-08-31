import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


import com.google.gson.Gson;
import org.apache.log4j.BasicConfigurator;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;



public class KafkaTwitterProducer {
    public static void main(String[] args) {

        System.out.println("Started running the Kafka Producer");

        // configure basic console logging
        BasicConfigurator.configure();

        // create a basic blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);

        // set up details for user authentication method for suthenticating with Twitter API
        Authentication authentication = new OAuth1(
                TwitterConfiguration.CONSUMER_KEY,
                TwitterConfiguration.CONSUMER_SECRET,
                TwitterConfiguration.ACCESS_TOKEN,
                TwitterConfiguration.TOKEN_SECRET);

        // filters and returns matched tweets
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // provide the search string to be passed
        endpoint.trackTerms(Collections.singletonList(TwitterConfiguration.HASHTAG));

        // building the client using the authentication information defined above
        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue)) // queue to put strings as the stream processes it
                .build();

        // connect with the twitter server
        client.connect();

        // create a GSON object to define Java objects from JSON serialisation data
        Gson gson = new Gson();

        // try defining a new producer
        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
                // convert JSON string value to an object of Tweet Class
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                System.out.printf("Fetched tweet id %d\n", tweet.getId());
                long key = tweet.getId();
                String msg = tweet.getText();
                // create a producer record object in the topic with the given key and value pair
                ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConfiguration.TOPIC, key, msg);
                // send the log to the message topic
                producer.send(record);
                System.out.println("Message sent");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }

    private static Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}