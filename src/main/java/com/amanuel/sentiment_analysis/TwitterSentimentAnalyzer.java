package com.amanuel.sentiment_analysis;

import com.amanuel.sentiment_analysis.service.SentimentAnalyzerService;
import com.amanuel.sentiment_analysis.service.TweetFetcher;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

public class TwitterSentimentAnalyzer {
    public static void main(String[] args) {
        try {
            Thread fetchThread = new Thread(() -> {
                TweetFetcher tweetFetcher = new TweetFetcher();
                try {
                    tweetFetcher.connectAndFetchTweet();
                } catch (IOException | URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            });

            fetchThread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
