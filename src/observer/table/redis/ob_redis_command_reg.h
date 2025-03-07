/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "share/table/redis/ob_redis_common.h"
#include "cmd/ob_redis_list_cmd.h"
#include "cmd/ob_redis_set_cmd.h"
#include "cmd/ob_redis_zset_cmd.h"
#include "cmd/ob_redis_hash_cmd.h"
#include "cmd/ob_redis_string_cmd.h"
#include "cmd/ob_redis_generic_cmd.h"

namespace oceanbase
{
namespace table
{
template <int>
struct ObRedisCommandTypeTraits {
  constexpr static bool registered_ = false;
  constexpr static const char *cmd_name_ = nullptr;
  constexpr static bool is_support_group_ = false;
  typedef char RedisCommand;
};

template <typename T>
struct ObRedisCommandTraits {
  constexpr static int type_ = 0;
};

#define REGISTER_REDIS_COMMAND_INNER(type, command, cmd_name, support_group) \
  template<>                                                                 \
  struct ObRedisCommandTypeTraits<type>                                      \
  {                                                                          \
    constexpr static bool registered_ = true;                                \
    constexpr static const char *cmd_name_ = cmd_name;                       \
    constexpr static bool is_support_group_ = support_group;                 \
    typedef command RedisCommand;                                            \
  };                                                                         \
  template<>                                                                 \
  struct ObRedisCommandTraits<command>                                       \
  {                                                                          \
    constexpr static int type_ = type;                                       \
  };

#define REGISTER_REDIS_COMMAND(type, command, cmd_name) \
  REGISTER_REDIS_COMMAND_INNER(type, command, cmd_name, false)

// REGISTER_REDIS_COMMAND(method_type, command)
// regeister command here, cmd name need to be all capitalized！
// Set
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SDIFF, SDiff, "sdiff");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SINTER, SInter, "sinter");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SUNION, SUnion, "sunion");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_SADD, SAdd, "sadd", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SDIFFSTORE, SDiffStore, "sdiffstore");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SINTERSTORE, SInterStore, "sinterstore");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SUNIONSTORE, SUnionStore, "sunionstore");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SCARD, SCard, "scard");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_SISMEMBER, SIsMember, "sismember", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SMEMBERS, SMembers, "smembers");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SMOVE, SMove, "smove");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SPOP, SPop, "spop");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SRANDMEMBER, SRandMember, "srandmember");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SREM, SRem, "srem");

// ZSet
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_ZADD, ZAdd, "zadd", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZCARD, ZCard, "zcard");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZREM, ZRem, "zrem");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZINCRBY, ZIncrBy, "zincrby");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_ZSCORE, ZScore, "zscore", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZRANK, ZRank, "zrank");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZREVRANK, ZRevRank, "zrevrank");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZRANGE, ZRange, "zrange");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZREVRANGE, ZRevRange, "zrevrange");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZCOUNT, ZCount, "zcount");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZREMRANGEBYRANK, ZRemRangeByRank, "zremrangebyrank");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZRANGEBYSCORE, ZRangeByScore, "zrangebyscore");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZREVRANGEBYSCORE, ZRevRangeByScore, "zrevrangebyscore");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZREMRANGEBYSCORE, ZRemRangeByScore, "zremrangebyscore");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZINTERSTORE, ZInterStore, "zinterstore");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_ZUNIONSTORE, ZUnionStore, "zunionstore");

// Hash
// REGISTER_REDIS_COMMAND(REDIS_COMMAND_HSET, HSet, "hset");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_HSET, HSet, "hset", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HMSET, HMSet, "hmset");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_HSETNX, HSetNX, "hsetnx", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_HGET, HGet, "hget", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HMGET, HMGet, "hmget");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HLEN, HLen, "hlen");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HGETALL, HGetAll, "hgetall");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HKEYS, HKeys, "hkeys");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HVALS, HVals, "hvals");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_HEXISTS, HExists, "hexists", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HDEL, HDel, "hdel");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HINCRBY, HIncrBy, "hincrby");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_HINCRBYFLOAT, HIncrByFloat, "hincrbyfloat");

// List
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_LPUSH, LPush, "lpush", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_LPUSHX, LPushX, "lpushx", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_RPUSH, RPush, "rpush", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_RPUSHX, RPushX, "rpushx", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_LPOP, LPop, "lpop", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_RPOP, RPop, "rpop", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_LINDEX, LIndex, "lindex");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_LSET, LSet, "lset");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_LRANGE, LRange, "lrange");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_LTRIM, LTrim, "ltrim");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_LINSERT, LInsert, "linsert");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_LLEN, LLen, "llen");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_LREM, LRem, "lrem");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_RPOPLPUSH, RpopLpush, "rpoplpush");

// String
REGISTER_REDIS_COMMAND(REDIS_COMMAND_GETSET, GetSet, "getset");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SETBIT, SetBit, "setbit");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_INCR, Incr, "incr", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_INCRBY, IncrBy, "incrby", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_DECR, Decr, "decr", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_DECRBY, DecrBy, "decrby", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_APPEND, Append, "append", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_BITCOUNT, BitCount, "bitcount", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_GET, Get, "get", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_GETBIT, GetBit, "getbit", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_GETRANGE, GetRange, "getrange", true);
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_INCRBYFLOAT, IncrByFloat, "incrbyfloat", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_MGET, MGet, "mget");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_MSET, MSet, "mset");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_SET, Set, "set", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_PSETEX, PSetEx, "psetex");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SETEX, SetEx, "setex");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_SETNX, SetNx, "setnx", true);
REGISTER_REDIS_COMMAND(REDIS_COMMAND_SETRANGE, SetRange, "setrange");
REGISTER_REDIS_COMMAND_INNER(REDIS_COMMAND_STRLEN, StrLen, "strlen", true);

// Generic
REGISTER_REDIS_COMMAND(REDIS_COMMAND_PEXPIREAT, ObRedisPExpireAt, "pexpireat");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_EXPIREAT, ObRedisExpireAt, "expireat");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_PEXPIRE, ObRedisPExpire, "pexpire");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_EXPIRE, ObRedisExpire, "expire");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_PTTL, ObRedisPTTL, "pttl");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_TTL, ObRedisTTL, "ttl");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_EXISTS, ObRedisExists, "exists");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_TYPE, ObRedisType, "type");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_PERSIST, ObRedisPersist, "persist");
REGISTER_REDIS_COMMAND(REDIS_COMMAND_DEL, ObRedisDel, "del");
#undef REGISTER_REDIS_COMMAND

}  // end namespace table
}  // end namespace oceanbase
