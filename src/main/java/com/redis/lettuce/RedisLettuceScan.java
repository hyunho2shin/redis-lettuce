package com.redis.lettuce;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RedisLettuceScan {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisLettuceScan.class);

    public static void main(String[] args) {
        // log level 변경
        /*ch.qos.logback.classic.Logger lettuceLogger =
              (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("io.lettuce.core");
        lettuceLogger.setLevel(ch.qos.logback.classic.Level.INFO);

        ch.qos.logback.classic.Logger reactorLogger =
              (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("io.netty");
        reactorLogger.setLevel(ch.qos.logback.classic.Level.INFO);*/
        // src/main/resource/logback.xml 설정

        final String redisKey = "incr_number:";

        // add
        RedisLettucePipeline addPipeline = new RedisLettucePipeline();
        addPipeline.addPipeline(redisKey);
        // scan
        RedisLettuceScan lettuceMain = new RedisLettuceScan();
        List<String> redisKeyList = lettuceMain.scan(redisKey);
        // del
        lettuceMain.deletePipeline(redisKeyList);

    }

    public List<String> scan(String redisKey) {
        List<String> redisKeyList = new ArrayList<>();

        CommandAction action = (redisCommands) -> {
            ScanArgs scanArgs = ScanArgs.Builder.matches(redisKey + "*").limit(1000);
            int count = 0;
            String getCursor = "0";
            KeyScanCursor<String> keyScanCursor = null;

            do {
                ScanCursor scanCursor = ScanCursor.of(getCursor);
                keyScanCursor = redisCommands.scan(scanCursor, scanArgs);

                List<String> keyList = keyScanCursor.getKeys();
                redisKeyList.addAll(keyList);
                count += keyList.size();

                logger.info(" keyList.size() : {}, count: {}", keyList.size(), count);

                getCursor = keyScanCursor.getCursor();
            } while (!keyScanCursor.isFinished());
        };
        CommandTemplate.commandAction(action);

        return redisKeyList;
    }

    public void deletePipeline(List<String> redisKeyList) {
        CommandAction action = (redisCommands) -> {
//            long del = redisCommands.del(redisKeyList.toArray(new String[0]));
            long del = redisCommands.unlink(redisKeyList.toArray(new String[0]));
            logger.info("del size : {}", del);
        };
        CommandTemplate.commandAction(action);
    }
}