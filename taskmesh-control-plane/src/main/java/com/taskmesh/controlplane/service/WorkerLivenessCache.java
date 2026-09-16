package com.taskmesh.controlplane.service;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import io.lettuce.core.RedisException;

/**
 * A TTL'd projection of worker liveness in Redis.
 * <p>
 * This is deliberately a cache and nothing more. PostgreSQL's
 * {@code workers} table is authoritative; Redis holds a key per live worker
 * that expires on its own if heartbeats stop, which gives fast liveness
 * detection without a polling sweep. Every operation here is best-effort:
 * if Redis is down, the exception is swallowed and logged, because a
 * registration or heartbeat must still succeed and be durably recorded in
 * PostgreSQL. Nothing in the claim path reads these keys - job claiming is
 * decided entirely by PostgreSQL.
 */
@Component
public class WorkerLivenessCache {

    private static final Logger log = LoggerFactory.getLogger(WorkerLivenessCache.class);
    private static final String KEY_PREFIX = "taskmesh:worker:";
    private static final Duration TTL = Duration.ofSeconds(15);

    private final StringRedisTemplate redis;

    public WorkerLivenessCache(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public void markAlive(String workerId) {
        try {
            redis.opsForValue().set(key(workerId), "1", TTL);
        } catch (DataAccessException | RedisException e) {
            log.warn("Could not record liveness for worker {} in Redis; PostgreSQL remains authoritative", workerId, e);
        }
    }

    public void clear(String workerId) {
        try {
            redis.delete(key(workerId));
        } catch (DataAccessException | RedisException e) {
            log.warn("Could not clear liveness for worker {} in Redis", workerId, e);
        }
    }

    public boolean isAlive(String workerId) {
        try {
            return Boolean.TRUE.equals(redis.hasKey(key(workerId)));
        } catch (DataAccessException | RedisException e) {
            log.warn("Could not read liveness for worker {} from Redis", workerId, e);
            return false;
        }
    }

    private String key(String workerId) {
        return KEY_PREFIX + workerId;
    }
}
