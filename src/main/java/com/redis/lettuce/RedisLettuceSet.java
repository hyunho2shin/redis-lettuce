package com.redis.lettuce;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisLettuceSet {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisLettuceSet.class);

    private final String REDIS_KEY = "roleCode:";
    private final String shinRedisKey = REDIS_KEY + "shin";
    private final String yoonRedisKey = REDIS_KEY + "yoon";
    private final String gangRedisKey = REDIS_KEY + "gang";

    public static void main(String[] args) {
        RedisLettuceSet lettuceMain = new RedisLettuceSet();

        lettuceMain.sadd();
        lettuceMain.commandBasic();
        lettuceMain.commandSet();
    }

    public void sadd() {
        CommandAction action = (redisCommands) -> {
            RedisSetCommands<String, String> setCommands = redisCommands;

            String[] shinValues = {"A001", "A002", "A003", "A005"};
            setCommands.sadd(shinRedisKey, shinValues);

            String[] kimValues = {"A003", "A004", "A005", "A006"};
            setCommands.sadd(yoonRedisKey, kimValues);

            String[] gangValues = {"A004", "A005", "A007"};
            setCommands.sadd(gangRedisKey, gangValues);

            smembers(redisCommands, shinRedisKey);
            smembers(redisCommands, yoonRedisKey);
            smembers(redisCommands, gangRedisKey);
            System.out.println();
        };
        CommandTemplate.commandAction(action);
    }

    public void commandBasic() {
        CommandAction action = (redisCommands) -> {
            RedisSetCommands<String, String> setCommands = redisCommands;

            long scard = setCommands.scard(shinRedisKey); // scard
            logger.info(" {} scard : {}", shinRedisKey, scard);

            long srem = setCommands.srem(shinRedisKey, "A001", "A002"); // srem
            logger.info(" {} srem : {}, delvalue : {}", shinRedisKey, srem, "A001,A002");
            smembers(redisCommands, shinRedisKey);

            boolean sismember = setCommands.sismember(yoonRedisKey, "A004");
            logger.info(" {} sismember >> {} : {}", yoonRedisKey, "A004", sismember);

            setCommands.smove(yoonRedisKey, gangRedisKey, "A006");
            logger.info("smove {} => {}, value:{}", yoonRedisKey, gangRedisKey, "A006");

            smembers(redisCommands, gangRedisKey);
        };
        CommandTemplate.commandAction(action);
    }

    public void commandSet() {
        CommandAction action = (redisCommands) -> {
            RedisSetCommands<String, String> setCommands = redisCommands;

            Set<String> sinterSet = setCommands.sinter(shinRedisKey, yoonRedisKey, gangRedisKey); // 교집합
            Set<String> sunionSet = setCommands.sunion(shinRedisKey, yoonRedisKey, gangRedisKey); // 합집합
            Set<String> sdiffSet = setCommands.sdiff(shinRedisKey, yoonRedisKey, gangRedisKey);  // 차집합

            logger.info("교집합 sinterSet : {}", sinterSet);
            logger.info("합집합 sunionSet : {}", sunionSet);
            logger.info("차집합 sdiffSet  : {}", sdiffSet);
            System.out.println();

            // sinterstore
            long sinterstore = setCommands.sinterstore("sinterstore", shinRedisKey, yoonRedisKey, gangRedisKey);
            logger.info("sinterstore count : {}", sinterstore);
            smembers(redisCommands, "sinterstore");

            // sunionstore
            long sunionstore = setCommands.sunionstore("sunionstore", shinRedisKey, yoonRedisKey, gangRedisKey);
            logger.info("sunionstore count : {}", sunionstore);
            smembers(redisCommands, "sunionstore");

            // sdiffstore
            long sdiffstore = setCommands.sdiffstore("sdiffstore", shinRedisKey, yoonRedisKey, gangRedisKey);
            logger.info("sdiffstore count : {}", sdiffstore);
            smembers(redisCommands, "sdiffstore");
        };
        CommandTemplate.commandAction(action);
    }

    private final String[] CODE_LIST = {"A001", "A002", "A003", "A004", "A005", "A006", "A007"};

    private void smembers(RedisCommands<String, String> redisCommands, String redisKey) {
        Set<String> members = redisCommands.smembers(redisKey);
        List<String> sortList = sorted(members); // 정렬

        for (int i = 0; i < CODE_LIST.length; i++) { // 부가적인
            if (!sortList.contains(CODE_LIST[i])) {
                sortList.add(i, "    ");
            }
        }

        logger.info("key={}, value:{}", redisKey, sortList);
        redisCommands.expire(redisKey, Duration.ofMinutes(1));
    }

    public List<String> sorted(Set<String> setList) {
        return setList.stream().sorted().collect(Collectors.toList());
    }
}