package com.redis.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class RedisLettuceString {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisLettuceString.class);

    private String redisKey = "lettuce:string";
    private String value = "value";

    public static void main(String[] args) {
        RedisLettuceString lettuceMain = new RedisLettuceString();

        lettuceMain.setGet();
        lettuceMain.incrDecr();
        lettuceMain.substring();
        lettuceMain.schedulerLock();
    }

    // deprecated (6.2.0): SETNX, SETEX, PSETEX, GETSET
    // STRALGO(LCS - 7.0.0) → 두 String 값 간의 최장 공통 부분 수열 찾기.

    // 중복 실행 방지용 분산 lock
    // 중복 결제/주문 요청 lock

    public void setGet() {
        CommandAction action = (redisCommands) -> {
            RedisStringCommands<String, String> stringCommands = redisCommands;

            String set = stringCommands.set(redisKey, value);
            logger.info("set : {}", set);

            String get = stringCommands.get(redisKey);
            logger.info("get : {}", get);

            redisCommands.expire(redisKey, Duration.ofMinutes(1)); // TTL
            long ttl = redisCommands.ttl(redisKey);
            logger.info("ttl : {}", ttl);

            stringCommands.set(redisKey, value + "_n");
            ttl = redisCommands.ttl(redisKey);
            logger.info("value change ttl : {}", ttl);

            // GETDEL 6.2.0
            String getdel = stringCommands.getdel(redisKey);
            logger.info("getdel : {}", getdel);

            get = stringCommands.get(redisKey);
            logger.info("get : {}", get);

            Duration duration = Duration.ofMinutes(3);
            SetArgs setArgs = SetArgs.Builder.nx().ex(duration)
//                    .keepttl() // 6.0이, 값변경시 기존 TTL유지 (PX(ms), EX(s) 만료 옵션과 동시 사용안됨)
//                    사용자 인증 토큰을 10분마다 갱신하면 TTL은 유지하는경우 활용
                    ;
             set = stringCommands.set(redisKey, value + "_new", setArgs);
        };
        CommandTemplate.commandAction(action);
    }

    public void incrDecr() {
        CommandAction action = (redisCommands) -> {
            RedisStringCommands<String, String> stringCommands = redisCommands;

            long decr = stringCommands.decr(redisKey);
            logger.info("decr : {}", decr);

            long incr = stringCommands.incr(redisKey);
            logger.info("incr : {}", incr);

            long incrby = stringCommands.incrby(redisKey, 10);
            logger.info("incrby : {}", incrby);

            long decrby = stringCommands.decrby(redisKey, 3);
            logger.info("decrby : {}", decrby);

            double incrbyfloat = stringCommands.incrbyfloat(redisKey, 0.123456789);
            logger.info("incrbyfloat : {}", incrbyfloat);

            redisCommands.expire(redisKey, Duration.ofMinutes(2)); // TTL
        };
        CommandTemplate.commandAction(action);
    }

    public void substring() {
        CommandAction action = (redisCommands) -> {
            RedisStringCommands<String, String> stringCommands = redisCommands;

            long append = stringCommands.append(redisKey, "_addStr");
            logger.info("append : {}", append);

            long strlen = stringCommands.strlen(redisKey);
            logger.info("strlen : {}", strlen);

            String getrange = stringCommands.getrange(redisKey, 0, 4);
            logger.info("getrange : {}", getrange);

            long setrange = stringCommands.setrange(redisKey, 6, "updateStr");
            logger.info("setrange : {}", setrange);

            long bitcount = stringCommands.bitcount(redisKey);
            logger.info("bitcount : {}", bitcount);
        };
        CommandTemplate.commandAction(action);
    }

    /* 응용 */
    public void schedulerLock() {
        RedisClient redisClient = RedisClient.create(CommandTemplate.getRedisUri());
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> redisCommands = connection.sync();

        RedisStringCommands<String, String> stringCommands = redisCommands;

        int parallelCount = Runtime.getRuntime().availableProcessors();
        CountDownLatch doneSignal = new CountDownLatch(parallelCount);

        SetArgs setArgs = SetArgs.Builder.nx()
//                .xx() // 값이 없는경우만 SET (NX와 EX 동시 사용불가)
                .ex(Duration.ofMinutes(1)); // nx + TTL (1분)

        Flux.range(1, parallelCount)
                .parallel()
                .runOn(Schedulers.parallel())
                .subscribe(id -> {
                    String set = stringCommands.set(redisKey, value + "-" + id, setArgs);

                    logger.info("set : {}, id : {}", (set != null), id);
                    doneSignal.countDown();
                });

        try {
            doneSignal.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}

        connection.close();
        redisClient.shutdown();
    }
}