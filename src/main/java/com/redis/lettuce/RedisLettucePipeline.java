package com.redis.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.internal.Futures;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

class RedisLettucePipeline {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisLettucePipeline.class);

    public static void main(String[] args) {
        RedisLettucePipeline lettucePipe = new RedisLettucePipeline();

        final String redisKey = "lettuce:pipe:";

        Instant now = Instant.now();

        lettucePipe.addPipeline(redisKey);

        logger.info("time : {}ms", Duration.between(now, Instant.now()).toMillis());
    }

    public void addPipeline(String redisKey) {
        RedisClient redisClient = RedisClient.create(CommandTemplate.getRedisUri());

        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisAsyncCommands<String, String> redisAsyncCommands = connection.async();

        // warm-up
        redisAsyncCommands.get("warmup").toCompletableFuture().join();
        redisAsyncCommands.get("warmup").toCompletableFuture().join();
        redisAsyncCommands.get("warmup").toCompletableFuture().join();

        connection.setAutoFlushCommands(false); // 자동 flush 비활성화 (명령어 버퍼링)

        List<RedisFuture<?>> futures = new ArrayList<>();
        final String value = "value";
        final int loopCount = 50000;

        for (int i = 0; i < loopCount; i++) {
//            String key = redisKey + StringUtils.leftPad(i + "", 4, "0");
            String key = redisKey + i;
            futures.add(redisAsyncCommands.set(key, value));
        }

        connection.flushCommands(); // 명령어 일괄 실행
        Futures.allOf(futures).join(); // 완료상태까지 대기

        connection.setAutoFlushCommands(true); // 자동 flush 활성화

        connection.close();
        redisClient.shutdown();
    }
}