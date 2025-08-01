package com.redis.lettuce;

import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.codec.RedisCodec;
import io.netty.util.internal.ThreadLocalRandom;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
//import org.apache.commons.lang3.RandomUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

class RedisLettuceZSet {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisLettuceZSet.class);

    String redisKey = "lettuce:zaddArgs";

    public static void main(String[] args) {
        RedisLettuceZSet lettuceZSet = new RedisLettuceZSet();

        lettuceZSet.zadd();         // 추가
        lettuceZSet.zincrby();      // 멤버별 score 증가
        lettuceZSet.zrange();       // [WITHSCORE] 오름차순 조회
        lettuceZSet.zrevrange();    // 내림차순 조회
        lettuceZSet.zrem();         // 멤버 삭제
        lettuceZSet.zscore();       // 멤버 score 조회
        lettuceZSet.zrank();        // 오름차순 순위 조회, zrevrank:내림차순 순위 조회
        lettuceZSet.zcard();        // zcard 전체 count, zcount 점수 범위 count
    }

    // 추가
    public void zadd() {
        CommandAction action = (redisCommands) -> {
            RedisSortedSetCommands<String, String> zsetCommands = redisCommands;

            ZAddArgs zAddArgs = ZAddArgs.Builder.nx(); // NX, XX, CH, LT, GT
            // NX: 없을 때만 추가, XX: 존재할때만 갱신
            // CH: zadd는 새로추가된 멤버의수 반환, CH사용시 기존 멤버의 score변경도 포함하여 반환(score가 변경된 경우만)
            // LT: 기존 score보다 낮은 경우만 갱신, GT: 기존 score보다 클경우만 갱신 - 6.2.0
            //      XX, NX와 함께 사용 안됨, LT와 GT는 동시 사용 안됨.
            //      멤버가 없는 경우는 새로추가됨.
            List<ScoredValue<String>> scoredValueList = new ArrayList<>();

            for (int i = 0; i < 5000; i++) {
//                double score = RandomUtils.nextDouble(0, 30);
                double score = ThreadLocalRandom.current().nextDouble(0, 30);

                ScoredValue<String> scoredValue = ScoredValue.just(score, "value" + i);
                scoredValueList.add(scoredValue);
            }

            long result = zsetCommands.zadd(redisKey, zAddArgs, scoredValueList.toArray(new ScoredValue[0]));

            redisCommands.expire(redisKey, Duration.ofMinutes(2)); // TTL
            // zaddincr : score 하나의 멤버만 증가시킴
            logger.info("result : {}", result);
        };
        CommandTemplate.commandAction(action);
    }

    // 멤버 score 증가
    public void zincrby() {
        CommandAction action = (redisCommands) -> {
            RedisSortedSetCommands<String, String> zsetCommands = redisCommands;

            double zincrby = zsetCommands.zincrby(redisKey, 20d, "value1000");
            logger.info("zincrby : {}", zincrby);
        };
        CommandTemplate.commandAction(action);
    }

    // 오름차순 조회
    public void zrange() {
        CommandAction action = (redisCommands) -> {
            RedisSortedSetCommands<String, String> zsetCommands = redisCommands;

            List<String> range = zsetCommands.zrange(redisKey, 0, 10);

            logger.info("range size : {}", range.size());
            logger.info("value :");
            for (String value : range) {
                logger.info("    {}", value);
            }

            List<ScoredValue<String>> rangeWithScores = zsetCommands.zrangeWithScores(redisKey, 0, 10);
            logger.info("rangeWithScores size : {}", rangeWithScores.size());
            for (ScoredValue<String> scoredValue : rangeWithScores) {
                logger.info("    {} : {}", scoredValue.getScore(), scoredValue.getValue());
            }

            List<String> zrangebyscore = zsetCommands.zrangebyscore(redisKey, Range.create(10, 12));
            logger.info("zrangebyscore size : {}", zrangebyscore.size());
            logger.info("value :");
            for (String value : zrangebyscore) {
                logger.info("    {}", value);
            }
        };
        CommandTemplate.commandAction(action);
    }

    // 내림차순 조회
    public void zrevrange() {
        CommandAction action = (redisCommands) -> {
            RedisSortedSetCommands<String, String> zsetCommands = redisCommands;

            List<String> zrevrange = zsetCommands.zrevrange(redisKey, 0, 10);

            logger.info("zrevrange size : {}", zrevrange.size());
            logger.info("value :");
            for (String value : zrevrange) {
                logger.info("    {}", value);
            }

            List<ScoredValue<String>> zrevrangeWithScore = zsetCommands.zrevrangeWithScores(redisKey, 0, 10);
            logger.info("zrevrangeWithScore size : {}", zrevrangeWithScore.size());
            for (ScoredValue<String> scoredValue : zrevrangeWithScore) {
                logger.info("    {} : {}", scoredValue.getScore(), scoredValue.getValue());
            }
        };
        CommandTemplate.commandAction(action);
    }

    // 멤버 삭제
    public void zrem() {
        CommandAction action = (redisCommands) -> {
            RedisSortedSetCommands<String, String> zsetCommands = redisCommands;

            long zremCount = zsetCommands.zrem(redisKey, "value111", "value1", "value122", "value9999");
            logger.info("zrem count : {}", zremCount);

            Range<String> range = Range.create("a", "v");
            long zremrangebylex = zsetCommands.zremrangebylex(redisKey, range); // member의 사전적 순서로 삭제
            logger.info("zremrangebylex : {}", zremrangebylex);

            long zremrangebyrank = zsetCommands.zremrangebyrank(redisKey, 1, 100);
            logger.info("zremrangebyrank : {}", zremrangebyrank);

            long zremrangebyscore = zsetCommands.zremrangebyscore(redisKey, Range.create(15, 20));
            logger.info("zremrangebyscore : {}", zremrangebyscore);
        };
        CommandTemplate.commandAction(action);
    }

    // 멤버 score 조회
    public void zscore() {
        CommandAction action = (redisCommands) -> {
            RedisSortedSetCommands<String, String> zsetCommands = redisCommands;

            // double score = redisCommands.zscore(redisKey, "value1113");
            // logger.info("score : {}", score);

            // data 없을 경우 NPE 방지하려면 Object로 받기
            Object score = zsetCommands.zscore(redisKey, "value9999");
            logger.info("score : {}", score);
        };
        CommandTemplate.commandAction(action);
    }

    // zrank 오름차순, zrevrank 내림차순 순위
    public void zrank() {
        CommandAction action = (redisCommands) -> {
            RedisSortedSetCommands<String, String> zsetCommands = redisCommands;

            long zrank = zsetCommands.zrank(redisKey, "value1113");
            logger.info("zrank : {}", zrank);

            long zrevrank = zsetCommands.zrevrank(redisKey, "value1113");
            logger.info("zrevrank : {}", zrevrank);
        };
        CommandTemplate.commandAction(action);
    }

    // zcard 전체 count, zcount 범위 count
    public void zcard() {
        CommandAction action = (redisCommands) -> {
            RedisSortedSetCommands<String, String> zsetCommands = redisCommands;

            long zcard = zsetCommands.zcard(redisKey);
            logger.info("zcard : {}", zcard);

            long zcount = zsetCommands.zcount(redisKey, Range.create(15, 20));
            logger.info("zcount : {}", zcount);
        };
        CommandTemplate.commandAction(action);
    }
}

