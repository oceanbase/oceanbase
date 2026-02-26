/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/table/redis/ob_redis_common.h"
#include "share/table/redis/ob_redis_parser.h"
#include "observer/table/redis/ob_redis_command_factory.h"
#include "lib/utility/ob_fast_convert.h"

#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
namespace table
{

const char *ObRedisUtil::REDIS_CRLF = "\r\n";

const ObString ObRedisUtil::REDIS_INDEX_NAME = "index";
const ObString ObRedisUtil::VALUE_PROPERTY_NAME = "value";
const ObString ObRedisUtil::EXPIRE_TS_PROPERTY_NAME = "EXPIRE_TS";
const ObString ObRedisUtil::INSERT_TS_PROPERTY_NAME = "insert_ts";
const ObString ObRedisUtil::REDIS_PROPERTY_NAME = "REDIS_CODE_STR";
const ObString ObRedisUtil::DB_PROPERTY_NAME = "db";
const ObString ObRedisUtil::RKEY_PROPERTY_NAME = "rkey";
const ObString ObRedisUtil::HASH_SET_META_MEMBER = "0";

const ObString ObRedisUtil::IS_DATA_PROPERTY_NAME = "is_data";
const ObString ObRedisUtil::SCORE_PROPERTY_NAME = ObString::make_string("score");
const ObString ObRedisUtil::STRING_TTL_DEFINITION = ObString::make_string("expire_ts + INTERVAL 0 SECOND");

const ObString ObRedisUtil::INCRBY_CMD_NAME = ObString::make_string("incrby");
const ObString ObRedisUtil::DECRBY_CMD_NAME = ObString::make_string("decrby");
const ObString ObRedisUtil::INCR_CMD_NAME = ObString::make_string("incr");
const ObString ObRedisUtil::DECR_CMD_NAME = ObString::make_string("decr");

////////////////////////////////////////////////////////////////////////////////////
const char *ObRedisFmt::MINUS_ONE = ":-1\r\n";
const char *ObRedisFmt::OK = "+OK\r\n";
const char *ObRedisFmt::ZERO = ":0\r\n";
const char *ObRedisFmt::EMPTY_ARRAY = "*0\r\n";
const char *ObRedisFmt::NULL_BULK_STRING = "$-1\r\n";
const char *ObRedisFmt::NULL_ARRAY = "*-1\r\n";

//**************
// ObRedisInfo
//**************

const char *ObRedisInfoV1::DB_NAME = "obkv_redis";
const char *ObRedisInfoV1::TABLE_SQLS[] =
{
  // string
  "create table if not exists obkv_redis.obkv_redis_string_table("
  "db bigint,"
  "rkey varbinary(16384),"
  "expire_ts timestamp(6) default null,"
  "value varbinary(1048576), "
  "primary key(db, rkey)) "
  "KV_ATTRIBUTES ='{\"Redis\": {\"isTTL\": true, \"model\": \"string\"}}'"
  "partition by key(db, rkey) partitions ",
  // hash
  "create table if not exists obkv_redis.obkv_redis_hash_table("
  "db bigint,"
  "rkey varbinary(16384),"
  "expire_ts timestamp(6) default null,"
  "insert_ts timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),"
  "value varbinary(1048576) default null,"
  "vk varbinary(16384) GENERATED ALWAYS AS (substr(rkey,9,conv(substr(rkey,1,8),16,10))) VIRTUAL,"
  "PRIMARY KEY(db, rkey))"
  "KV_ATTRIBUTES ='{\"Redis\": {\"isTTL\": true, \"model\": \"hash\"}}'"
  "PARTITION BY KEY(db, vk) PARTITIONS ",
  // list
  "create table if not exists obkv_redis.obkv_redis_list_table("
  "db BIGINT,"
  "rkey varbinary(16384), "
  "expire_ts timestamp(6) default null,"
  "insert_ts timestamp(6) DEFAULT CURRENT_TIMESTAMP(6), "
  "is_data tinyint(1) default 1,"
  "value varbinary(1048576) DEFAULT NULL, "
  "`index` BIGINT,"
  "PRIMARY KEY(db, rkey, is_data, `index`))"
  "KV_ATTRIBUTES ='{\"Redis\": {\"isTTL\": true, \"model\": \"list\"}}'"
  "PARTITION BY KEY(db, rkey) PARTITIONS ",
  // set
  "create table if not exists obkv_redis.obkv_redis_set_table("
  "db bigint,"
  "rkey varbinary(16384),"
  "expire_ts timestamp(6) default null,"
  "insert_ts timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),"
  "vk varbinary(16384) GENERATED ALWAYS AS (substr(rkey,9,conv(substr(rkey,1,8),16,10))) VIRTUAL,"
  "PRIMARY KEY(db, rkey))"
  "KV_ATTRIBUTES ='{\"Redis\": {\"isTTL\": true, \"model\": \"set\"}}'"
  "PARTITION BY KEY(db, vk) PARTITIONS ",
  // zset
  "create table if not exists obkv_redis.obkv_redis_zset_table("
  "db bigint,"
  "rkey varbinary(16384),"
  "expire_ts timestamp(6) default null,"
  "insert_ts timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),"
  "score double default null,"
  "vk varbinary(16384) GENERATED ALWAYS AS (substr(rkey,9,conv(substr(rkey,1,8),16,10))) VIRTUAL,"
  "index index_score(db, vk, score) local,"
  "PRIMARY KEY(db, rkey))"
  "KV_ATTRIBUTES ='{\"Redis\": {\"isTTL\": true, \"model\": \"zset\"}}'"
  "PARTITION BY KEY(db, vk) PARTITIONS "
};

const char *ObRedisInfoV1::ALL_KV_REDIS_TABLE_SQLS[] =
{
  // set
  "\"sdiff\", \"obkv_redis_set_table\"",
  "\"sinter\", \"obkv_redis_set_table\"",
  "\"sunion\", \"obkv_redis_set_table\"",
  "\"sadd\", \"obkv_redis_set_table\"",
  "\"sdiffstore\", \"obkv_redis_set_table\"",
  "\"sinterstore\", \"obkv_redis_set_table\"",
  "\"sunionstore\", \"obkv_redis_set_table\"",
  "\"scard\", \"obkv_redis_set_table\"",
  "\"sismember\", \"obkv_redis_set_table\"",
  "\"smembers\", \"obkv_redis_set_table\"",
  "\"smove\", \"obkv_redis_set_table\"",
  "\"spop\", \"obkv_redis_set_table\"",
  "\"srandmember\", \"obkv_redis_set_table\"",
  "\"srem\", \"obkv_redis_set_table\"",
  // zset
  "\"zadd\", \"obkv_redis_zset_table\"",
  "\"zcard\", \"obkv_redis_zset_table\"",
  "\"zrem\", \"obkv_redis_zset_table\"",
  "\"zincrby\", \"obkv_redis_zset_table\"",
  "\"zscore\", \"obkv_redis_zset_table\"",
  "\"zrank\", \"obkv_redis_zset_table\"",
  "\"zrevrank\", \"obkv_redis_zset_table\"",
  "\"zrange\", \"obkv_redis_zset_table\"",
  "\"zrevrange\", \"obkv_redis_zset_table\"",
  "\"zcount\", \"obkv_redis_zset_table\"",
  "\"zremrangebyrank\", \"obkv_redis_zset_table\"",
  "\"zrangebyscore\", \"obkv_redis_zset_table\"",
  "\"zrevrangebyscore\", \"obkv_redis_zset_table\"",
  "\"zremrangebyscore\", \"obkv_redis_zset_table\"",
  "\"zinterstore\", \"obkv_redis_zset_table\"",
  "\"zunionstore\", \"obkv_redis_zset_table\"",
  // hash
  "\"hset\", \"obkv_redis_hash_table\"",
  "\"hmset\", \"obkv_redis_hash_table\"",
  "\"hsetnx\", \"obkv_redis_hash_table\"",
  "\"hget\", \"obkv_redis_hash_table\"",
  "\"hmget\", \"obkv_redis_hash_table\"",
  "\"hlen\", \"obkv_redis_hash_table\"",
  "\"hgetall\", \"obkv_redis_hash_table\"",
  "\"hkeys\", \"obkv_redis_hash_table\"",
  "\"hvals\", \"obkv_redis_hash_table\"",
  "\"hexists\", \"obkv_redis_hash_table\"",
  "\"hdel\", \"obkv_redis_hash_table\"",
  "\"hincrby\", \"obkv_redis_hash_table\"",
  "\"hincrbyfloat\", \"obkv_redis_hash_table\"",
  // list
  "\"lpush\", \"obkv_redis_list_table\"",
  "\"lpushx\", \"obkv_redis_list_table\"",
  "\"rpush\", \"obkv_redis_list_table\"",
  "\"rpushx\", \"obkv_redis_list_table\"",
  "\"lpop\", \"obkv_redis_list_table\"",
  "\"rpop\", \"obkv_redis_list_table\"",
  "\"lindex\", \"obkv_redis_list_table\"",
  "\"lset\", \"obkv_redis_list_table\"",
  "\"lrange\", \"obkv_redis_list_table\"",
  "\"ltrim\", \"obkv_redis_list_table\"",
  "\"linsert\", \"obkv_redis_list_table\"",
  "\"llen\", \"obkv_redis_list_table\"",
  "\"lrem\", \"obkv_redis_list_table\"",
  "\"rpoplpush\", \"obkv_redis_list_table\"",
  // string
  "\"getset\", \"obkv_redis_string_table\"",
  "\"setbit\", \"obkv_redis_string_table\"",
  "\"incr\", \"obkv_redis_string_table\"",
  "\"incrby\", \"obkv_redis_string_table\"",
  "\"decr\", \"obkv_redis_string_table\"",
  "\"decrby\", \"obkv_redis_string_table\"",
  "\"append\", \"obkv_redis_string_table\"",
  "\"bitcount\", \"obkv_redis_string_table\"",
  "\"get\", \"obkv_redis_string_table\"",
  "\"getbit\", \"obkv_redis_string_table\"",
  "\"getrange\", \"obkv_redis_string_table\"",
  "\"incrbyfloat\", \"obkv_redis_string_table\"",
  "\"mget\", \"obkv_redis_string_table\"",
  "\"mset\", \"obkv_redis_string_table\"",
  "\"set\", \"obkv_redis_string_table\"",
  "\"psetex\", \"obkv_redis_string_table\"",
  "\"setex\", \"obkv_redis_string_table\"",
  "\"setnx\", \"obkv_redis_string_table\"",
  "\"setrange\", \"obkv_redis_string_table\"",
  "\"strlen\", \"obkv_redis_string_table\"",
  // generic
  "\"pexpireat\", \"obkv_redis_string_table\"",
  "\"expireat\", \"obkv_redis_string_table\"",
  "\"expire\", \"obkv_redis_string_table\"",
  "\"pexpire\", \"obkv_redis_string_table\"",
  "\"pttl\", \"obkv_redis_string_table\"",
  "\"ttl\", \"obkv_redis_string_table\"",
  "\"exists\", \"obkv_redis_string_table\"",
  "\"type\", \"obkv_redis_string_table\"",
  "\"persist\", \"obkv_redis_string_table\"",
  "\"del\", \"obkv_redis_string_table\""
};

const char *ObRedisInfoV1::STRING_TABLE_NAME = "obkv_redis_string_table";
const char *ObRedisInfoV1::LIST_TABLE_NAME = "obkv_redis_list_table";
const char *ObRedisInfoV1::SET_TABLE_NAME = "obkv_redis_set_table";
const char *ObRedisInfoV1::ZSET_TABLE_NAME = "obkv_redis_zset_table";
const char *ObRedisInfoV1::HASH_TABLE_NAME = "obkv_redis_hash_table";


}  // end namespace table
}  // namespace oceanbase
