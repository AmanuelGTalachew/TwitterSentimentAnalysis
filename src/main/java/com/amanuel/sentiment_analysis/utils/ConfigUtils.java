package com.amanuel.sentiment_analysis.utils;

import com.amanuel.sentiment_analysis.service.TweetFetcher;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.Objects;

public class ConfigUtils {

    final static String TWITTER_CONFIG_FILE = "twitterConfig.json";
    final static String KAFKA_CONFIG_FILE = "kafkaConfig.json";

    public static String consumerKey;
    public static String consumerSecret;
    public static String accessToken;
    public static String accessTokenSecret;
    public static String bearerToken;

    public static String bootstrapServers;
    public static String topic;
    public static String acksConfig;
    public static String maxInFlightConn;
    public static String compressionType;
    public static String lingerConfig;


    public static void configureTwitterCredentials() throws FileNotFoundException {
        Reader temp = new FileReader(Objects.requireNonNull(TweetFetcher.class.getClassLoader().
                getResource(TWITTER_CONFIG_FILE)).getFile());
        Object twitterConfig = JsonParser.parseReader(temp);

        JsonObject tempJsonObject = (JsonObject) twitterConfig;
        consumerKey = tempJsonObject.get("consumer_key").toString();
        consumerKey = consumerKey.substring(1, consumerKey.length() - 1);
        consumerSecret = tempJsonObject.get("consumer_secret").toString();
        consumerSecret = consumerSecret.substring(1, consumerSecret.length() - 1);
        accessToken = tempJsonObject.get("access_token").toString();
        accessToken = accessToken.substring(1, accessToken.length() - 1);
        accessTokenSecret = tempJsonObject.get("access_token_secret").toString();
        accessTokenSecret = accessTokenSecret.substring(1, accessTokenSecret.length() - 1);
        bearerToken = tempJsonObject.get("bearer_token").toString();
        bearerToken = bearerToken.substring(1, bearerToken.length() - 1);
    }

    public static void configureKafkaConfiguration() throws FileNotFoundException {
        Object kafkaConfig = JsonParser.parseReader(new FileReader(Objects.requireNonNull(TweetFetcher.class.getClassLoader().
                getResource(KAFKA_CONFIG_FILE)).getFile()));

        JsonObject tempJsonObject = (JsonObject) kafkaConfig;
        bootstrapServers = String.valueOf(tempJsonObject.get("bootstrap_servers"));
        bootstrapServers = bootstrapServers.substring(1, bootstrapServers.length() - 1);
        topic = String.valueOf(tempJsonObject.get("topic"));
        topic = topic.substring(1, topic.length() - 1);
        compressionType = String.valueOf(tempJsonObject.get("compression_type"));
        compressionType = compressionType.substring(1, compressionType.length() - 1);
        lingerConfig = String.valueOf(tempJsonObject.get("linger_config"));
        lingerConfig = lingerConfig.substring(1, lingerConfig.length() - 1);
        acksConfig = String.valueOf(tempJsonObject.get("acks_config"));
        acksConfig = acksConfig.substring(1, acksConfig.length() - 1);
        maxInFlightConn = String.valueOf(tempJsonObject.get("max_in_flight_conn"));
        maxInFlightConn = maxInFlightConn.substring(1, maxInFlightConn.length() - 1);
    }
}