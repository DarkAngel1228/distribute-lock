package com.example.distributelock.controller;

import com.example.distributelock.lock.RedisLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@RestController
@Slf4j
public class RedisLockController {
    @Autowired
    private RedisTemplate redisTemplate;

    @RequestMapping("redisLock")
    public String redisLock() {
        String key = "redisKey";
        String value = UUID.randomUUID().toString();
        RedisCallback<Boolean> redisCallback = redisConnection -> {
            // 设置nx
            RedisStringCommands.SetOption setOption = RedisStringCommands.SetOption.ifAbsent();
            // 设置过期时间
            Expiration seconds = Expiration.seconds(30);
            // 序列化
            byte[] redisKey = redisTemplate.getKeySerializer().serialize(key);
            byte[] redisValue = redisTemplate.getValueSerializer().serialize(value);

            Boolean result = redisConnection.set(redisKey, redisValue, seconds, setOption);
            return result;

        };
        Boolean lock = (boolean)redisTemplate.execute(redisCallback);

        if (lock) {
            log.info("我进入了锁！");
            try {
                Thread.sleep(15000);

            } catch (InterruptedException e) {
                e.printStackTrace(); 
            } finally {
                String script = "if redis.call(\"get\",KEYS[1])== ARGV[1] then\n" +
                        " return redis.call(\"del\", KEYS[1])\n" +
                        "else\n" +
                        " return 0\n" +
                        "end";
                RedisScript<Object> redisScript = RedisScript.of(script, Boolean.class);
                List<String> keys = Arrays.asList(key);
                Boolean result = (Boolean) redisTemplate.execute(redisScript, keys, value);

                log.info("释放锁的结果：" + result);
            }
        }


        return "";
    }
}
