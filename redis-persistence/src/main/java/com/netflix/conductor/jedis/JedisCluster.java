package com.netflix.conductor.jedis;

import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;
import redis.clients.jedis.util.SafeEncoder;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisCluster implements JedisCommands
{
    private final redis.clients.jedis.JedisCluster jedis;

    public JedisCluster(redis.clients.jedis.JedisCluster jedisCluster)
    {
        this.jedis = jedisCluster;
    }

    @Override
    public String set(String key, String value)
    {
        return jedis.set(key, value);
    }

    @Override
    public String set(String key, String value, SetParams params)
    {
        return jedis.set(key, value, params);
    }

    @Override
    public String get(String key)
    {
        return jedis.get(key);
    }

    @Override
    public Boolean exists(String key)
    {
        return jedis.exists(key);
    }

    @Override
    public Long persist(String key)
    {
        return jedis.persist(key);
    }

    @Override
    public String type(String key)
    {
        return jedis.type(key);
    }

    @Override
    public byte[] dump(String key)
    {
        return jedis.dump(key);
    }

    @Override
    public String restore(String key, int ttl, byte[] serializedValue)
    {
        return jedis.restore(key, ttl, serializedValue);
    }

    @Override
    public String restoreReplace(String key, int ttl, byte[] serializedValue)
    {
        throw new RuntimeException("Unsupported operation");
    }

    @Override
    public Long expire(String key, int seconds)
    {
        return jedis.expire(key, seconds);
    }

    @Override
    public Long pexpire(String key, long milliseconds)
    {
        return jedis.pexpire(key, milliseconds);
    }

    @Override
    public Long expireAt(String key, long unixTime)
    {
        return jedis.expireAt(key, unixTime);
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp)
    {
        return jedis.pexpireAt(key, millisecondsTimestamp);
    }

    @Override
    public Long ttl(String key)
    {
        return jedis.ttl(key);
    }

    @Override
    public Long pttl(String key)
    {
        return jedis.pttl(key);
    }

    @Override
    public Long touch(String key)
    {
        return jedis.touch(key);
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value)
    {
        return jedis.setbit(key, offset, value);
    }

    @Override
    public Boolean setbit(String key, long offset, String value)
    {
        return jedis.setbit(key, offset, value);
    }

    @Override
    public Boolean getbit(String key, long offset)
    {
        return jedis.getbit(key, offset);
    }

    @Override
    public Long setrange(String key, long offset, String value)
    {
        return jedis.setrange(key, offset, value);
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset)
    {
        return jedis.getrange(key, startOffset, endOffset);
    }

    @Override
    public String getSet(String key, String value)
    {
        return jedis.getSet(key, value);
    }

    @Override
    public Long setnx(String key, String value)
    {
        return jedis.setnx(key, value);
    }

    @Override
    public String setex(String key, int seconds, String value)
    {
        return jedis.setex(key, seconds, value);
    }

    @Override
    public String psetex(String key, long milliseconds, String value)
    {
        return jedis.psetex(key, milliseconds, value);
    }

    @Override
    public Long decrBy(String key, long integer)
    {
        return jedis.decrBy(key, integer);
    }

    @Override
    public Long decr(String key)
    {
        return jedis.decr(key);
    }

    @Override
    public Long incrBy(String key, long integer)
    {
        return jedis.incrBy(key, integer);
    }

    @Override
    public Double incrByFloat(String key, double value)
    {
        return jedis.incrByFloat(key, value);
    }

    @Override
    public Long incr(String key)
    {
        return jedis.incr(key);
    }

    @Override
    public Long append(String key, String value)
    {
        return jedis.append(key, value);
    }

    @Override
    public String substr(String key, int start, int end)
    {
        return jedis.substr(key, start, end);
    }

    @Override
    public Long hset(String key, String field, String value)
    {
        return jedis.hset(key, field, value);
    }

    @Override
    public Long hset(String key, Map<String, String> hash)
    {
        return jedis.hset(key, hash);
    }

    @Override
    public String hget(String key, String field)
    {
        return jedis.hget(key, field);
    }

    @Override
    public Long hsetnx(String key, String field, String value)
    {
        return jedis.hsetnx(key, field, value);
    }

    @Override
    public String hmset(String key, Map<String, String> hash)
    {
        return jedis.hmset(key, hash);
    }

    @Override
    public List<String> hmget(String key, String... fields)
    {
        return jedis.hmget(key, fields);
    }

    @Override
    public Long hincrBy(String key, String field, long value)
    {
        return jedis.hincrBy(key, field, value);
    }

    @Override
    public Double hincrByFloat(String key, String field, double value)
    {
        return jedis.hincrByFloat(key.getBytes(), field.getBytes(), value);
    }

    @Override
    public Boolean hexists(String key, String field)
    {
        return jedis.hexists(key, field);
    }

    @Override
    public Long hdel(String key, String... field)
    {
        return jedis.hdel(key, field);
    }

    @Override
    public Long hlen(String key)
    {
        return jedis.hlen(key);
    }

    @Override
    public Set<String> hkeys(String key)
    {
        return jedis.hkeys(key);
    }

    @Override
    public List<String> hvals(String key)
    {
        return jedis.hvals(key);
    }

    @Override
    public Map<String, String> hgetAll(String key)
    {
        return jedis.hgetAll(key);
    }

    @Override
    public Long rpush(String key, String... string)
    {
        return jedis.rpush(key, string);
    }

    @Override
    public Long lpush(String key, String... string)
    {
        return jedis.lpush(key, string);
    }

    @Override
    public Long llen(String key)
    {
        return jedis.llen(key);
    }

    @Override
    public List<String> lrange(String key, long start, long end)
    {
        return jedis.lrange(key, start, end);
    }

    @Override
    public String ltrim(String key, long start, long end)
    {
        return jedis.ltrim(key, start, end);
    }

    @Override
    public String lindex(String key, long index)
    {
        return jedis.lindex(key, index);
    }

    @Override
    public String lset(String key, long index, String value)
    {
        return jedis.lset(key, index, value);
    }

    @Override
    public Long lrem(String key, long count, String value)
    {
        return jedis.lrem(key, count, value);
    }

    @Override
    public String lpop(String key)
    {
        return jedis.lpop(key);
    }

    @Override
    public String rpop(String key)
    {
        return jedis.rpop(key);
    }

    @Override
    public Long sadd(String key, String... member)
    {
        return jedis.sadd(key, member);
    }

    @Override
    public Set<String> smembers(String key)
    {
        return jedis.smembers(key);
    }

    @Override
    public Long srem(String key, String... member)
    {
        return jedis.srem(key, member);
    }

    @Override
    public String spop(String key)
    {
        return jedis.spop(key);
    }

    @Override
    public Set<String> spop(String key, long count)
    {
        return jedis.spop(key, count);
    }

    @Override
    public Long scard(String key)
    {
        return jedis.scard(key);
    }

    @Override
    public Boolean sismember(String key, String member)
    {
        return jedis.sismember(key, member);
    }

    @Override
    public String srandmember(String key)
    {
        return jedis.srandmember(key);
    }

    @Override
    public List<String> srandmember(String key, int count)
    {
        return jedis.srandmember(key, count);
    }

    @Override
    public Long strlen(String key)
    {
        return jedis.strlen(key);
    }

    @Override
    public Long zadd(String key, double score, String member)
    {
        return jedis.zadd(key, score, member);
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params)
    {
        return jedis.zadd(key, score, member, params);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers)
    {
        return jedis.zadd(key, scoreMembers);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params)
    {
        return jedis.zadd(key, scoreMembers, params);
    }

    @Override
    public Set<String> zrange(String key, long start, long end)
    {
        return jedis.zrange(key, start, end);
    }

    @Override
    public Long zrem(String key, String... member)
    {
        return jedis.zrem(key, member);
    }

    @Override
    public Double zincrby(String key, double score, String member)
    {
        return jedis.zincrby(key, score, member);
    }

    @Override
    public Double zincrby(String key, double score, String member, ZIncrByParams params)
    {
        return jedis.zincrby(key, score, member, params);
    }

    @Override
    public Long zrank(String key, String member)
    {
        return jedis.zrank(key, member);
    }

    @Override
    public Long zrevrank(String key, String member)
    {
        return jedis.zrevrank(key, member);
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end)
    {
        return jedis.zrevrange(key, start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end)
    {
        return jedis.zrangeWithScores(key, start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end)
    {
        return jedis.zrevrangeWithScores(key, start, end);
    }

    @Override
    public Long zcard(String key)
    {
        return jedis.zcard(key);
    }

    @Override
    public Double zscore(String key, String member)
    {
        return jedis.zscore(key, member);
    }

    @Override
    public List<String> sort(String key)
    {
        return jedis.sort(key);
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters)
    {
        return jedis.sort(key, sortingParameters);
    }

    @Override
    public Long zcount(String key, double min, double max)
    {
        return jedis.zcount(key, min, max);
    }

    @Override
    public Long zcount(String key, String min, String max)
    {
        return jedis.zcount(key, min, max);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max)
    {
        return jedis.zrangeByScore(key, min, max);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max)
    {
        return jedis.zrangeByScore(key, min, max);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min)
    {
        return jedis.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count)
    {
        return jedis.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min)
    {
        return jedis.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count)
    {
        return jedis.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count)
    {
        return jedis.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max)
    {
        return jedis.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min)
    {
        return jedis.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count)
    {
        return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count)
    {
        return jedis.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max)
    {
        return jedis.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min)
    {
        return jedis.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count)
    {
        return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count)
    {
        return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count)
    {
        return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end)
    {
        return jedis.zremrangeByRank(key, start, end);
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end)
    {
        return jedis.zremrangeByScore(key, start, end);
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end)
    {
        return jedis.zremrangeByScore(key, start, end);
    }

    @Override
    public Long zlexcount(String key, String min, String max)
    {
        return jedis.zlexcount(key, min, max);
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max)
    {
        return jedis.zrangeByLex(key, min, max);
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count)
    {
        return jedis.zrangeByLex(key, min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min)
    {
        return jedis.zrevrangeByLex(key, max, min);
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count)
    {
        return jedis.zrevrangeByLex(key, max, min, offset, count);
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max)
    {
        return jedis.zremrangeByLex(key, min, max);
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value)
    {
        return jedis.linsert(key, where, pivot, value);
    }

    @Override
    public Long lpushx(String key, String... string)
    {
        return jedis.lpushx(key, string);
    }

    @Override
    public Long rpushx(String key, String... string)
    {
        return jedis.rpushx(key, string);
    }

    @Override
    public List<String> blpop(int timeout, String key)
    {
        return jedis.blpop(timeout, key);
    }

    @Override
    public List<String> brpop(int timeout, String key)
    {
        return jedis.brpop(timeout, key);
    }

    @Override
    public Long del(String key)
    {
        return jedis.del(key);
    }

    @Override
    public Long unlink(String key)
    {
        return jedis.unlink(key);
    }

    @Override
    public String echo(String string)
    {
        return jedis.echo(string);
    }

    @Override
    public Long move(String key, int dbIndex)
    {
        throw new RuntimeException("Unsupported operation");
    }

    @Override
    public Long bitcount(String key)
    {
        return jedis.bitcount(key);
    }

    @Override
    public Long bitcount(String key, long start, long end)
    {
        return jedis.bitcount(key, start, end);
    }

    @Override
    public Long bitpos(String key, boolean value)
    {
        throw new RuntimeException("Unsupported operation");
    }

    @Override
    public Long bitpos(String key, boolean value, BitPosParams params)
    {
        throw new RuntimeException("Unsupported operation");
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor)
    {
        return jedis.hscan(key, cursor);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params)
    {
        ScanResult<Map.Entry<byte[], byte[]>> scanResult = jedis.hscan(key.getBytes(), cursor.getBytes(), params);

        List<Map.Entry<String, String>>  results = new ArrayList<>();

        for(Map.Entry<byte[], byte[]> result : scanResult.getResult())
        {
            results.add(new AbstractMap.SimpleEntry<>(SafeEncoder.encode(result.getKey()), SafeEncoder.encode(result.getValue())));
        }

        new ScanResult<>(scanResult.getCursor(), results);
        return jedis.hscan(key, cursor);
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor)
    {
        return jedis.sscan(key, cursor);
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor, ScanParams params)
    {
        ScanResult<byte[]> result = jedis.sscan(key.getBytes(), cursor.getBytes(), params);

        List<String> results = new ArrayList<>();
        List<byte[]> rawResults = result.getResult();

        for (byte[] bs : rawResults) {
            results.add(SafeEncoder.encode(bs));
        }

        return new ScanResult<>(result.getCursor(), results);
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor)
    {
        return jedis.zscan(key, cursor);
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params)
    {
        return jedis.zscan(key, cursor);
    }

    @Override
    public Long pfadd(String key, String... elements)
    {
        return jedis.pfadd(key, elements);
    }

    @Override
    public long pfcount(String key)
    {
        return jedis.pfcount(key);
    }

    @Override
    public Long geoadd(String key, double longitude, double latitude, String member)
    {
        return jedis.geoadd(key, longitude, latitude, member);
    }

    @Override
    public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap)
    {
        return jedis.geoadd(key, memberCoordinateMap);
    }

    @Override
    public Double geodist(String key, String member1, String member2)
    {
        return jedis.geodist(key, member1, member2);
    }

    @Override
    public Double geodist(String key, String member1, String member2, GeoUnit unit)
    {
        return jedis.geodist(key, member1, member2, unit);
    }

    @Override
    public List<String> geohash(String key, String... members)
    {
        return jedis.geohash(key, members);
    }

    @Override
    public List<GeoCoordinate> geopos(String key, String... members)
    {
        return jedis.geopos(key, members);
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit)
    {
        return jedis.georadius(key, longitude, latitude, radius, unit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius,
                                                     GeoUnit unit)
    {
        return jedis.georadiusReadonly(key, longitude, latitude, radius, unit);
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit,
                                             GeoRadiusParam param)
    {
        return jedis.georadius(key, longitude, latitude, radius, unit, param);
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius,
                                                     GeoUnit unit, GeoRadiusParam param)
    {
        return jedis.georadiusReadonly(key, longitude, latitude, radius, unit, param);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit)
    {
        return jedis.georadiusByMember(key, member, radius, unit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit)
    {
        return jedis.georadiusByMemberReadonly(key, member, radius, unit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit,
                                                     GeoRadiusParam param)
    {
        return jedis.georadiusByMember(key, member, radius, unit, param);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit,
                                                             GeoRadiusParam param)
    {
        return jedis.georadiusByMemberReadonly(key, member, radius, unit, param);
    }

    @Override
    public List<Long> bitfield(String key, String... arguments)
    {
        return jedis.bitfield(key, arguments);
    }

    @Override
    public Long hstrlen(String key, String field)
    {
        return jedis.hstrlen(key, field);
    }
}