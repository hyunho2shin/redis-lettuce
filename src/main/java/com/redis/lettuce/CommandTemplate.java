package com.redis.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class CommandTemplate {
    /* io.lettuce.core.AbstractRedisAsyncCommands
       AbstractRedisReactiveCommands (flux)
       RedisCommandBuilder
       io.lettuce.core.protocol.CommandType */

    public static void commandAction(CommandAction commandAction) {
        RedisClient redisClient = RedisClient.create(getRedisUri());

        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> redisCommands = connection.sync();

        // execute
        commandAction.doInExecute(redisCommands);

        // after
        connection.close();
        redisClient.shutdown();
    }

    public static RedisURI getRedisUri() {
        return RedisURI.builder()
                .withHost("localhost")
                .withPort(6379)
                .withDatabase(1)
                .build();
    }
}
