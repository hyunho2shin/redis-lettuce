package com.redis.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Scanner;

class RedisLettucePubsub {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisLettucePubsub.class);

    private String NEWS_CHANNEL = "news:";
    private String WEATHER = "weather";
    private String SPORTS = "sports";

    public static void main(String[] args) {
        RedisLettucePubsub lettucePubsub = new RedisLettucePubsub();

        RedisClient redisClient = RedisClient.create(CommandTemplate.getRedisUri());

        StatefulRedisPubSubConnection<String, String> pubSubConnection = redisClient.connectPubSub();

        // pub/sub (Flux + scanner + await)
        lettucePubsub.subscribe(pubSubConnection);
        lettucePubsub.publish(pubSubConnection);

        Mono.delay(Duration.ofSeconds(1)).block();
    }

    public void subscribe(StatefulRedisPubSubConnection<String, String> pubSubConnection) {
        pubSubConnection.addListener(new RedisPubSubListener<String, String>() {
            @Override
            public void message(String channel, String message) {
                logger.info("message channel = " + channel + ", message = " + message);
            }

            @Override
            public void message(String pattern, String channel, String message) {
                logger.info("message pattern = " + pattern + ", channel = " + channel + ", message = " + message);
            }

            @Override
            public void subscribed(String channel, long 구독count) {
                logger.info("subscribed: channel = " + channel + ", 구독count = " + 구독count);
            }

            @Override
            public void psubscribed(String pattern, long 구독count) {
                logger.info("psubscribed: pattern = " + pattern + ", 구독count = " + 구독count);
            }

            @Override
            public void unsubscribed(String channel, long 구독count) {
                logger.info("unsubscribed: channel = " + channel + ", 구독count = " + 구독count);
            }

            @Override
            public void punsubscribed(String channel, long 구독count) {
                logger.info("punsubscribed: channel = " + channel + ", 구독count = " + 구독count);
            }
        });

        RedisPubSubAsyncCommands<String, String> redisPubSubAsyncCommands = pubSubConnection.async();

        // 구독 시작
        redisPubSubAsyncCommands.subscribe(NEWS_CHANNEL + WEATHER);
        redisPubSubAsyncCommands.psubscribe(NEWS_CHANNEL + "*");
    }

    public void publish(StatefulRedisPubSubConnection<String, String> pubSubConnection) {
        RedisPubSubAsyncCommands<String, String> redisPubSubAsyncCommands = pubSubConnection.async();

        CommandAction action = (redisCommands) -> {
            Scanner scanner = null;

            while (true) {
                Mono.delay(Duration.ofMillis(500)).block(); // 500ms 지연

                scanner = new Scanner(System.in);

                System.out.print("input :: ");

                String message = scanner.nextLine();

                if (message.toUpperCase().matches("STOP|END|EXIT")) {
                    logger.info(message + "...");
                    redisPubSubAsyncCommands.unsubscribe(NEWS_CHANNEL + WEATHER);
                    redisPubSubAsyncCommands.punsubscribe(NEWS_CHANNEL + "*");
                    break;
                }

                String channel = (message.indexOf(WEATHER) > -1) ? WEATHER : SPORTS;
                redisCommands.publish(NEWS_CHANNEL + channel, message);
            }
            scanner.close();
        };

        CommandTemplate.commandAction(action);
    }
}