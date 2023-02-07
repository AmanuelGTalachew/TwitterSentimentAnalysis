package com.amanuel.sentiment_analysis.service;

import com.amanuel.sentiment_analysis.utils.ConfigUtils;
import com.amanuel.sentiment_analysis.utils.SetupRule;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TweetFetcher {
    final Logger logger = LoggerFactory.getLogger(TweetFetcher.class);
    private KafkaProducer<String, String> producer;
    private Map<String, String> trackRules;
    private boolean isDone = false;
    private static final String API_URL = "https://api.twitter.com/2/tweets/search/stream";

    public void connectAndFetchTweet() throws IOException, URISyntaxException {
        logger.info("Connect to twitter to fetch the tweet and feed it to Kafka");

        ConfigUtils.configureTwitterCredentials();
        ConfigUtils.configureKafkaConfiguration();

        trackRules = new HashMap<>();
        trackRules.put("storm", "winter storm #winterstorm");

        SetupRule.setupRules(ConfigUtils.bearerToken, trackRules);

        producer = createKafkaProducer();
        HttpResponse response = createTwitterClientAndConnect();
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
            String tweet = reader.readLine();
            while (!isDone && tweet != null) {
                System.out.println(tweet);
                producer.send(new ProducerRecord<>(ConfigUtils.topic, null, tweet),
                        (recordMetadata, e) -> {
                            if (e != null)
                                logger.error("An error happened", e);
                        });
                tweet = reader.readLine();
            }
        }
    }

    private HttpResponse createTwitterClientAndConnect() throws IOException, URISyntaxException {
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder(API_URL);

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", ConfigUtils.bearerToken));

        return httpClient.execute(httpGet);
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ProducerConfig.ACKS_CONFIG, ConfigUtils.acksConfig);
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, ConfigUtils.maxInFlightConn);
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, ConfigUtils.compressionType);
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, ConfigUtils.lingerConfig);
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        return new KafkaProducer<>(prop);
    }

    public void disconnectAndCleanUp() {
        isDone = true;
        producer.flush();
        producer.close();
    }

}
