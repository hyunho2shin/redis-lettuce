package com.redis.lettuce;

import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisStringCommands;

import java.time.Duration;

class RedisLettuceMultiExec {
    private String keyProduct = "lettuce:product"; // max:1000

    public static void main(String[] args) {
        RedisLettuceMultiExec lettuceMain = new RedisLettuceMultiExec();

        lettuceMain.set();
        lettuceMain.firstComeFirstServed();
    }

    public void set() {
        CommandAction action = (redisCommands) -> {
            RedisStringCommands<String, String> stringCommands = redisCommands;

            stringCommands.set(keyProduct, "3");
//            redisCommands.expire(keyProduct, Duration.ofMinutes(5)); // TTL
        };
        CommandTemplate.commandAction(action);
    }

    // 선착 event, 1000ea, 1~5는 더지급
    public void firstComeFirstServed() {
        CommandAction action = (redisCommands) -> {
            String redisKey = "lettuce:user:0001";

            TransactionResult transactionResult = null;
            do {
                redisCommands.watch(keyProduct);
                redisCommands.multi();

                redisCommands.incr(keyProduct); // 0 index

                transactionResult = redisCommands.exec();
            } while (transactionResult.wasDiscarded());

            redisCommands.set(redisKey, Long.toString(transactionResult.get(0)));
            redisCommands.unwatch();
        };
        CommandTemplate.commandAction(action);
    }
}


