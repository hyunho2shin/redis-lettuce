package com.redis.lettuce;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RedisLettuceHash {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisLettuceHash.class);

    public static void main(String[] args) {
        RedisLettuceHash lettuceHash = new RedisLettuceHash();
        lettuceHash.hash();
    }

    public void hash() {
        final String redisKey = "lettuce:hash";

        CommandAction action = (redisCommands -> {
            RedisHashCommands<String, String> hashCommands = redisCommands;

            // String field = "roleCode";
            // String value = "A0001";
            // redisCommands.hset(redisKey, field, value);

            Map<String, String> map = new HashMap<>();
            map.put("roleCode", "A0001");
            map.put("dvsnCode", "B");
            map.put("agge", "36");
            redisCommands.hmset(redisKey, map);

            String roleCode = hashCommands.hget(redisKey, "roleCode");
            logger.info("roleCode : [{}]", roleCode);

            List<KeyValue<String, String>> hmgetList = hashCommands.hmget(redisKey, "roleCode", "dvsnCode");
            logger.info("hmget : [{}]", hmgetList);

            Map<String, String> hgetall = hashCommands.hgetall(redisKey);
            logger.info("hgetall : [{}]", hgetall);

            redisCommands.expire(redisKey, Duration.ofMinutes(1));
        });

        CommandTemplate.commandAction(action);
    }
}
