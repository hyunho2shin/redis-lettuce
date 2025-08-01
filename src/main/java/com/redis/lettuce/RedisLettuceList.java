package com.redis.lettuce;

import io.lettuce.core.KeyValue;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.LPosArgs;
import io.lettuce.core.api.sync.RedisListCommands;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class RedisLettuceList {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisLettuceList.class);

    private final String redisKey = "lettuce:list";
    private final String[] valueAry = {"value01", "value02", "value03", "value04", "value05"};
    private final String[] logAry = {"log01", "log02", "log03", "log04", "log05"};

    public static void main(String[] args) {
        RedisLettuceList lettuceMain = new RedisLettuceList();

        lettuceMain.fifo();       // doc descri
        lettuceMain.lifo();       // doc descri
        lettuceMain.range_index_len();
        lettuceMain.insert_set_rem();
        lettuceMain.ltrim_blmove();
        lettuceMain.lpos();
        lettuceMain.blpop();
        lettuceMain.blmoveQueue();
    }

    // 큐(Queue) : RPush로 요소추가, LPOP으로 소비 (FIFO-선입선출) → 파이프라인
    // 스택(Stack) : LPush로 요소추가, LPOP으로 소비 (LIFO-후입선출) → 탐쌓고꺼내기
    // Redis 같은 큐는 파이프라인 시스템에 사용
    //   - Producer (생산자): 메시지추가 (RPUSH-오른쪽)
    //   - Consumer (소비자): 메시지소비 (LPOP-왼쪽)

    public void fifo() {
        CommandAction action = (redisCommands -> {
            RedisListCommands<String, String> listCommands = redisCommands;

            listCommands.rpush(redisKey, valueAry);

            String lpop = null;
            logger.info("fifo");
            while ((lpop = redisCommands.lpop(redisKey)) != null) {
                logger.info("    {}", lpop);
            }
        });
        CommandTemplate.commandAction(action);
    }

    public void lifo() {
        CommandAction action = (redisCommands -> {
            RedisListCommands<String, String> listCommands = redisCommands;

            listCommands.lpush(redisKey, valueAry);

            String lpop = null;
            logger.info("lifo");
            while ((lpop = listCommands.lpop(redisKey)) != null) {
                logger.info("    {}", lpop);
            }
        });
        CommandTemplate.commandAction(action);
    }

    public void range_index_len() {
        CommandAction action = (redisCommands -> {
            RedisListCommands<String, String> listCommands = redisCommands;

            listCommands.rpush(redisKey, valueAry);

            List<String> lrange = listCommands.lrange(redisKey, 2, 4);
            logger.info("lrange : {}", lrange);

            String lindex = listCommands.lindex(redisKey, 1);
            logger.info("lindex : {}", lindex);

            long llen = listCommands.llen(redisKey);
            logger.info("llen : {}", llen);

            redisCommands.expire(redisKey, Duration.ofMinutes(2)); // TTL
        });
        CommandTemplate.commandAction(action);
    }

    public void insert_set_rem() {
        CommandAction action = (redisCommands -> {
            RedisListCommands<String, String> listCommands = redisCommands;

            long linsert = listCommands.linsert(redisKey, true, valueAry[2], "log01");
            logger.info("linsert : {}", linsert);

            String lset = listCommands.lset(redisKey, 3, "log02");
            logger.info("lset : {}", lset);

            listCommands.rpush(redisKey, valueAry[0], valueAry[1], valueAry[2]
                    ,valueAry[0], valueAry[0], valueAry[0]);
            long lrem = listCommands.lrem(redisKey, 4, valueAry[0]);
            logger.info("lrem : {}", lrem);

            redisCommands.expire(redisKey, Duration.ofMinutes(2)); // TTL
        });
        CommandTemplate.commandAction(action);
    }

    public void ltrim_blmove() {
        CommandAction action = (redisCommands -> {
            RedisListCommands<String, String> listCommands = redisCommands;

            String newKey = redisKey + "_N";

            listCommands.rpush(redisKey, valueAry);
            listCommands.rpush(newKey, valueAry);

//            String ltrim = listCommands.ltrim(redisKey, 0, 2);
//            logger.info("ltrim : {}", ltrim);

            LMoveArgs lMoveArgs = LMoveArgs.Builder.leftRight();
            listCommands.lmove(redisKey, newKey, lMoveArgs);

            List<String> lrange = listCommands.lrange(redisKey, 0, -1);
            logger.info("lrange : {}", redisKey, lrange);

            List<String> lrange_n = listCommands.lrange(newKey, 0, -1);
            logger.info("lrange : {}", newKey, lrange_n);

            redisCommands.expire(redisKey, Duration.ofMinutes(2));  // TTL
            redisCommands.expire(newKey, Duration.ofMinutes(2));    // TTL
        });
        CommandTemplate.commandAction(action);
    }

    public void lpos() {
        CommandAction action = (redisCommands -> {
            RedisListCommands<String, String> listCommands = redisCommands;

            listCommands.rpush(redisKey, valueAry);

            LPosArgs lPosArgs = LPosArgs.Builder.rank(1).maxlen(5);
            Object lpos = listCommands.lpos(redisKey, valueAry[4], 3, lPosArgs); // NPE 오류 가능

            logger.info("lpos : {}", lpos);

            redisCommands.expire(redisKey, Duration.ofMinutes(2));  // TTL
        });
        CommandTemplate.commandAction(action);
    }

    /* 응용 */
    public void blpop() {
        System.out.println("========== START ==========");

        CountDownLatch doneSignal = new CountDownLatch(1);

        Mono.just("DUMMY")
                .publishOn(Schedulers.boundedElastic()) // 비동기
                .subscribe(obj -> {

                    CommandAction action = (redisCommands -> {
                        RedisListCommands<String, String> listCommands = redisCommands;

                        // BLPop
                        logger.info("blpop wait...");  // 0이면 영원히 대기
                        KeyValue<String, String> blpopResult = listCommands.blpop(10, redisKey);

                        logger.info("blpop result : {}", (blpopResult != null));
                        if (blpopResult != null) {
                            logger.info("    {} = {}", blpopResult.getKey(), blpopResult.getValue());
                        }
                        doneSignal.countDown();
                    });
                    CommandTemplate.commandAction(action);
                });

        // 5초 대기
        Mono.delay(Duration.ofSeconds(5)).block();

        CommandAction action = (redisCommands -> {
            RedisListCommands<String, String> listCommands = redisCommands;
            // rpush
            logger.info("rpush....");
            listCommands.rpush(redisKey, "value");
        });
        CommandTemplate.commandAction(action);

        try {
            doneSignal.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}

        System.out.println("========== END ==========");
    }

    public void blmoveQueue() {
        CountDownLatch doneSignal = new CountDownLatch(1);

        Mono.just("DUMMY") // Worker Thread
                .publishOn(Schedulers.boundedElastic()) // 비동기
                .subscribe(obj -> {
                    CommandAction action = (redisCommands -> {
                        RedisListCommands<String, String> listCommands = redisCommands;

                        LMoveArgs lMoveArgs = LMoveArgs.Builder.leftRight();

                        while (true) {
                            // BRpopLPush(deprecated) => LMove, BLMove - 6.2.0
                            // Timeout EX = 1 minute
                            String blmove = listCommands.blmove(redisKey, redisKey + ":bak", lMoveArgs, 10);

                            if (blmove != null) {
                                logger.info("blmove result : {}", blmove);
                            }
                        }
                    });
                    CommandTemplate.commandAction(action);
                });

        CommandAction action = (redisCommands -> {
            RedisListCommands<String, String> listCommands = redisCommands;

            Scanner scanner = null;
            while (true) {
                Mono.delay(Duration.ofMillis(500)).block(); // 500ms 지연

                scanner = new Scanner(System.in);
                System.out.print("input :: ");

                String message = scanner.nextLine();

                if (message.toUpperCase().matches("STOP|END|EXIT")) {
                    doneSignal.countDown();
                    break;
                }
                // rpush
                listCommands.rpush(redisKey, message);
            }
            scanner.close();
        });
        CommandTemplate.commandAction(action);

        try {
            doneSignal.await();
        } catch (InterruptedException e) {}
    }
}