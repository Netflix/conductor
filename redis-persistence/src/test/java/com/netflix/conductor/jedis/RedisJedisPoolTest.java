/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.jedis;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

public class RedisJedisPoolTest
{

    private final Jedis jedis = mock(Jedis.class);
    private final JedisSentinelPool jedisPool = mock(JedisSentinelPool.class);
    private final RedisJedisPool redisJedisPool = new RedisJedisPool(jedisPool);

    @Before
    public void init() {
        when(this.jedisPool.getResource()).thenReturn(this.jedis);
    }

    @Test
    public void testSet() {
        redisJedisPool.set("key", "value");
        redisJedisPool.set("key", "value", SetParams.setParams());
    }

    @Test
    public void testGet() {
        redisJedisPool.get("key");
    }

    @Test
    public void testExists() {
        redisJedisPool.exists("key");
    }

    @Test
    public void testPersist() {
        redisJedisPool.persist("key");
    }

    @Test
    public void testType() {
        redisJedisPool.type("key");
    }

    @Test
    public void testExpire() {
        redisJedisPool.expire("key", 1337);
    }

    @Test
    public void testPexpire() {
        redisJedisPool.pexpire("key", 1337);
    }

    @Test
    public void testExpireAt() {
        redisJedisPool.expireAt("key", 1337);
    }

    @Test
    public void testPexpireAt() {
        redisJedisPool.pexpireAt("key", 1337);
    }

    @Test
    public void testTtl() {
        redisJedisPool.ttl("key");
    }

    @Test
    public void testPttl() {
        redisJedisPool.pttl("key");
    }

    @Test
    public void testSetbit() {
        redisJedisPool.setbit("key", 1337, "value");
        redisJedisPool.setbit("key", 1337, true);
    }

    @Test
    public void testGetbit() {
        redisJedisPool.getbit("key", 1337);
    }

    @Test
    public void testSetrange() {
        redisJedisPool.setrange("key", 1337, "value");
    }

    @Test
    public void testGetrange() {
        redisJedisPool.getrange("key", 1337, 1338);
    }

    @Test
    public void testGetSet() {
        redisJedisPool.getSet("key", "value");
    }

    @Test
    public void testSetnx() {
        redisJedisPool.setnx("test", "value");
    }

    @Test
    public void testSetex() {
        redisJedisPool.setex("key", 1337, "value");
    }

    @Test
    public void testPsetex() {
        redisJedisPool.psetex("key", 1337, "value");
    }

    @Test
    public void testDecrBy() {
        redisJedisPool.decrBy("key", 1337);
    }

    @Test
    public void testDecr() {
        redisJedisPool.decr("key");
    }

    @Test
    public void testIncrBy() {
        redisJedisPool.incrBy("key", 1337);
    }

    @Test
    public void testIncrByFloat() {
        redisJedisPool.incrByFloat("key", 1337);
    }

    @Test
    public void testIncr() {
        redisJedisPool.incr("key");
    }

    @Test
    public void testAppend() {
        redisJedisPool.append("key", "value");
    }

    @Test
    public void testSubstr() {
        redisJedisPool.substr("key", 1337, 1338);
    }

    @Test
    public void testHset() {
        redisJedisPool.hset("key", "field", "value");
    }

    @Test
    public void testHget() {
        redisJedisPool.hget("key", "field");
    }

    @Test
    public void testHsetnx() {
        redisJedisPool.hsetnx("key", "field", "value");
    }

    @Test
    public void testHmset() {
        redisJedisPool.hmset("key", new HashMap<String, String>());
    }

    @Test
    public void testHmget() {
        redisJedisPool.hmget("key", "fields");
    }

    @Test
    public void testHincrBy() {
        redisJedisPool.hincrBy("key", "field", 1337);
    }

    @Test
    public void testHincrByFloat() {
        redisJedisPool.hincrByFloat("key", "field", 1337);
    }

    @Test
    public void testHexists() {
        redisJedisPool.hexists("key", "field");
    }

    @Test
    public void testHdel() {
        redisJedisPool.hdel("key", "field");
    }

    @Test
    public void testHlen() {
        redisJedisPool.hlen("key");
    }

    @Test
    public void testHkeys() {
        redisJedisPool.hkeys("key");
    }

    @Test
    public void testHvals() {
        redisJedisPool.hvals("key");
    }

    @Test
    public void testGgetAll() {
        redisJedisPool.hgetAll("key");
    }

    @Test
    public void testRpush() {
        redisJedisPool.rpush("key", "string");
    }

    @Test
    public void testLpush() {
        redisJedisPool.lpush("key", "string");
    }

    @Test
    public void testLlen() {
        redisJedisPool.llen("key");
    }

    @Test
    public void testLrange() {
        redisJedisPool.lrange("key", 1337, 1338);
    }

    @Test
    public void testLtrim() {
        redisJedisPool.ltrim("key", 1337, 1338);
    }

    @Test
    public void testLindex() {
        redisJedisPool.lindex("key", 1337);
    }

    @Test
    public void testLset() {
        redisJedisPool.lset("key", 1337, "value");
    }

    @Test
    public void testLrem() {
        redisJedisPool.lrem("key", 1337, "value");
    }

    @Test
    public void testLpop() {
        redisJedisPool.lpop("key");
    }

    @Test
    public void testRpop() {
        redisJedisPool.rpop("key");
    }

    @Test
    public void testSadd() {
        redisJedisPool.sadd("key", "member");
    }

    @Test
    public void testSmembers() {
        redisJedisPool.smembers("key");
    }

    @Test
    public void testSrem() {
        redisJedisPool.srem("key", "member");
    }

    @Test
    public void testSpop() {
        redisJedisPool.spop("key");
        redisJedisPool.spop("key", 1337);
    }

    @Test
    public void testScard() {
        redisJedisPool.scard("key");
    }

    @Test
    public void testSismember() {
        redisJedisPool.sismember("key", "member");
    }

    @Test
    public void testSrandmember() {
        redisJedisPool.srandmember("key");
        redisJedisPool.srandmember("key", 1337);
    }

    @Test
    public void testStrlen() {
        redisJedisPool.strlen("key");
    }

    @Test
    public void testZadd() {
        redisJedisPool.zadd("key", new HashMap<>());
        redisJedisPool.zadd("key", new HashMap<>(), ZAddParams.zAddParams());
        redisJedisPool.zadd("key", 1337, "members");
        redisJedisPool.zadd("key", 1337, "members", ZAddParams.zAddParams());
    }

    @Test
    public void testZrange() {
        redisJedisPool.zrange("key", 1337, 1338);
    }

    @Test
    public void testZrem() {
        redisJedisPool.zrem("key", "member");
    }

    @Test
    public void testZincrby() {
        redisJedisPool.zincrby("key", 1337, "member");
        redisJedisPool.zincrby("key", 1337, "member", ZIncrByParams.zIncrByParams());
    }

    @Test
    public void testZrank() {
        redisJedisPool.zrank("key", "member");
    }

    @Test
    public void testZrevrank() {
        redisJedisPool.zrevrank("key", "member");
    }

    @Test
    public void testZrevrange() {
        redisJedisPool.zrevrange("key", 1337, 1338);
    }

    @Test
    public void testZrangeWithScores() {
        redisJedisPool.zrangeWithScores("key", 1337, 1338);
    }

    @Test
    public void testZrevrangeWithScores() {
        redisJedisPool.zrevrangeWithScores("key", 1337, 1338);
    }

    @Test
    public void testZcard() {
        redisJedisPool.zcard("key");
    }

    @Test
    public void testZscore() {
        redisJedisPool.zscore("key", "member");
    }

    @Test
    public void testSort() {
        redisJedisPool.sort("key");
        redisJedisPool.sort("key", new SortingParams());
    }

    @Test
    public void testZcount() {
        redisJedisPool.zcount("key", "min", "max");
        redisJedisPool.zcount("key", 1337, 1338);
    }

    @Test
    public void testZrangeByScore() {
        redisJedisPool.zrangeByScore("key", "min", "max");
        redisJedisPool.zrangeByScore("key", 1337, 1338);
        redisJedisPool.zrangeByScore("key", "min", "max", 1337, 1338);
        redisJedisPool.zrangeByScore("key", 1337, 1338, 1339, 1340);
    }


    @Test
    public void testZrevrangeByScore() {
        redisJedisPool.zrevrangeByScore("key", "max", "min");
        redisJedisPool.zrevrangeByScore("key", 1337, 1338);
        redisJedisPool.zrevrangeByScore("key", "max", "min", 1337, 1338);
        redisJedisPool.zrevrangeByScore("key", 1337, 1338, 1339, 1340);
    }

    @Test
    public void testZrangeByScoreWithScores() {
        redisJedisPool.zrangeByScoreWithScores("key", "min", "max");
        redisJedisPool.zrangeByScoreWithScores("key", "min", "max", 1337, 1338);
        redisJedisPool.zrangeByScoreWithScores("key", 1337, 1338);
        redisJedisPool.zrangeByScoreWithScores("key", 1337, 1338, 1339, 1340);
    }

    @Test
    public void testZrevrangeByScoreWithScores() {
        redisJedisPool.zrevrangeByScoreWithScores("key", "max", "min");
        redisJedisPool.zrevrangeByScoreWithScores("key", "max", "min", 1337, 1338);
        redisJedisPool.zrevrangeByScoreWithScores("key", 1337, 1338);
        redisJedisPool.zrevrangeByScoreWithScores("key", 1337, 1338, 1339, 1340);
    }

    @Test
    public void testZremrangeByRank() {
        redisJedisPool.zremrangeByRank("key", 1337, 1338);
    }

    @Test
    public void testZremrangeByScore() {
        redisJedisPool.zremrangeByScore("key", "start", "end");
        redisJedisPool.zremrangeByScore("key", 1337, 1338);
    }

    @Test
    public void testZlexcount() {
        redisJedisPool.zlexcount("key", "min", "max");
    }

    @Test
    public void testZrangeByLex() {
        redisJedisPool.zrangeByLex("key", "min", "max");
        redisJedisPool.zrangeByLex("key", "min", "max", 1337, 1338);
    }

    @Test
    public void testZrevrangeByLex() {
        redisJedisPool.zrevrangeByLex("key", "max", "min");
        redisJedisPool.zrevrangeByLex("key", "max", "min", 1337, 1338);
    }

    @Test
    public void testZremrangeByLex() {
        redisJedisPool.zremrangeByLex("key", "min", "max");
    }

    @Test
    public void testLinsert() {
        redisJedisPool.linsert("key", ListPosition.AFTER, "pivot", "value");
    }

    @Test
    public void testLpushx() {
        redisJedisPool.lpushx("key", "string");
    }

    @Test
    public void testRpushx() {
        redisJedisPool.rpushx("key", "string");
    }

    @Test
    public void testBlpop() {
        redisJedisPool.blpop(1337, "arg");
    }

    @Test
    public void testBrpop() {
        redisJedisPool.brpop(1337, "arg");
    }

    @Test
    public void testDel() {
        redisJedisPool.del("key");
    }

    @Test
    public void testEcho() {
        redisJedisPool.echo("string");
    }

    @Test
    public void testMove() {
        redisJedisPool.move("key", 1337);
    }

    @Test
    public void testBitcount() {
        redisJedisPool.bitcount("key");
        redisJedisPool.bitcount("key", 1337, 1338);
    }

    @Test
    public void testBitpos() {
        redisJedisPool.bitpos("key", true);
    }

    @Test
    public void testHscan() {
        redisJedisPool.hscan("key", "cursor");
        redisJedisPool.hscan("key", "cursor", new ScanParams());
    }

    @Test
    public void testSscan() {
        redisJedisPool.sscan("key", "cursor");
        redisJedisPool.sscan("key", "cursor", new ScanParams());
    }

    @Test
    public void testZscan() {
        redisJedisPool.zscan("key", "cursor");
        redisJedisPool.zscan("key", "cursor", new ScanParams());
    }

    @Test
    public void testPfadd() {
        redisJedisPool.pfadd("key", "elements");
    }

    @Test
    public void testPfcount() {
        redisJedisPool.pfcount("key");
    }

    @Test
    public void testGeoadd() {
        redisJedisPool.geoadd("key", new HashMap<>());
        redisJedisPool.geoadd("key", 1337, 1338, "member");
    }

    @Test
    public void testGeodist() {
        redisJedisPool.geodist("key", "member1", "member2");
        redisJedisPool.geodist("key", "member1", "member2", GeoUnit.KM);
    }

    @Test
    public void testGeohash() {
        redisJedisPool.geohash("key", "members");
    }

    @Test
    public void testGeopos() {
        redisJedisPool.geopos("key", "members");
    }

    @Test
    public void testGeoradius() {
        redisJedisPool.georadius("key", 1337, 1338, 32, GeoUnit.KM);
        redisJedisPool.georadius("key", 1337, 1338, 32, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
    }

    @Test
    public void testGeoradiusByMember() {
        redisJedisPool.georadiusByMember("key", "member", 1337, GeoUnit.KM);
        redisJedisPool.georadiusByMember("key", "member", 1337, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
    }

    @Test
    public void testBitfield() {
        redisJedisPool.bitfield("key", "arguments");
    }
}