import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;



public class twitterKafka {


    private static final String topic = "TwitterCapstone";


    public static void run() throws InterruptedException {


        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id","camus");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
                producerConfig);
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList("Trump",
                "#MAGA"));
        String consumerKey= twitterSourceConst.CONSUMER_KEY_KEY;
        String consumerSecret=twitterSourceConst.CONSUMER_SECRET_KEY;
        String accessToken=twitterSourceConst.ACCESS_TOKEN_KEY;
        String accessTokenSecret=twitterSourceConst.ACCESS_TOKEN_SECRET_KEY;
        Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken,
                accessTokenSecret);
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();
        client.connect();
        for (int msgRead = 0; msgRead < 10000000; msgRead++) {
            KeyedMessage<String, String> message = null;
            try {
                message = new KeyedMessage<String, String>(topic, queue.take());
            } catch (InterruptedException e) {
//e.printStackTrace();
                System.out.println("Stream has ended");
            }
            producer.send(message);
        }
        producer.close();
        client.stop();
    }
    public static void main(String[] args) {
        try {
            twitterKafka.run();
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}