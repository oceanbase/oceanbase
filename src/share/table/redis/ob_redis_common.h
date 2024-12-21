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

#ifndef OCEANBASE_SHARE_TABLE_OB_REDIS_COMMON_H_
#define OCEANBASE_SHARE_TABLE_OB_REDIS_COMMON_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace table
{
class ObRedisRequest;
class ObRedisUtil
{
public:
  static const char REDIS_LF = '\n';
  static const char REDIS_CR = '\r';
  static const char ARRAY_FLAG = '*';
  static const char BULK_FLAG = '$';
  static const char SIMPLE_ERR_FLAG = '-';
  static const char SIMPLE_STR_FLAG = '+';
  static const char INT_FLAG = ':';

  static const char *REDIS_CRLF;

  static const int HEADER_LEN = 3;  // "*\r\n"
  static const int FLAG_LEN = 1;    // "*"
  static const int CRLF_LEN = 2;    // "\r\n"
public:
  static const int COMPLEX_ROWKEY_NUM = 2;    // (db, rkey)
  static const int LIST_ROWKEY_NUM = 4;    // (db, key, is_data, idx)
  static const int64_t STRING_ROWKEY_SIZE = 2;
  static const int64_t STRING_SET_PROPERTY_SIZE = 2;
  static const int64_t HASH_ZSET_PROPERTY_SIZE = 3;
  static const ObString REDIS_DB_NAME;
  static const ObString REDIS_INDEX_NAME;
  static const ObString VALUE_PROPERTY_NAME;
  static const ObString INSERT_TS_PROPERTY_NAME;
  // Redis command property name in the request entity
  static const ObString REDIS_PROPERTY_NAME;
  static const ObString DB_PROPERTY_NAME;
  static const ObString RKEY_PROPERTY_NAME;
  static const ObString IS_DATA_PROPERTY_NAME;
  static const ObString SCORE_PROPERTY_NAME;
  static const ObString STRING_TTL_DEFINITION;
  static const ObString INCRBY_CMD_NAME;
  static const ObString DECRBY_CMD_NAME;
  static const ObString INCR_CMD_NAME;
  static const ObString DECR_CMD_NAME;
  static const int64_t COL_IDX_DB = 0;
  static const int64_t COL_IDX_RKEY = 1;
  static const int64_t COL_IDX_EXPIRE_TS = 2;
  static const int64_t COL_IDX_INSERT_TS = 3;
  static const int64_t COL_IDX_IS_DATA = 4; // for list
  static const int64_t PK_IDX_IS_DATA = 2; // for list
  static const int64_t STRING_COL_IDX_EXPIRE_TS = 2;
  static const int64_t STRING_COL_IDX_VALUE = 3;
  static const ObString EXPIRE_TS_PROPERTY_NAME;
  static const ObString HASH_SET_META_MEMBER;

  static const int64_t SEC_TO_US = 1000 * 1000;
  static const int64_t MS_TO_US = 1000;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisUtil);
};

class ObRedisFmt
{
public:
  static const char *MINUS_ONE;
  static const char *OK;
  static const char *EMPTY_ARRAY;
  static const char *ZERO;
  static const char *NULL_BULK_STRING; // client display '(nil)'
  static const char *NULL_ARRAY; // client display '(nil)'

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisFmt);
};

// The sequence of RedisCommandType is associated with that of ObTableProccessType.
// If you want to change it, you need to change ObTableProccessType simultaneously
enum RedisCommandType {
  REDIS_COMMAND_INVALID = 0,
  // List
  REDIS_COMMAND_LINDEX,
  REDIS_COMMAND_LSET,
  REDIS_COMMAND_LRANGE,
  REDIS_COMMAND_LTRIM,
  REDIS_COMMAND_LPUSH,
  REDIS_COMMAND_LPUSHX,
  REDIS_COMMAND_RPUSH,
  REDIS_COMMAND_RPUSHX,
  REDIS_COMMAND_LPOP,
  REDIS_COMMAND_RPOP,
  REDIS_COMMAND_LREM,
  REDIS_COMMAND_RPOPLPUSH,
  REDIS_COMMAND_LINSERT,
  REDIS_COMMAND_LLEN,
  // Set
  REDIS_COMMAND_SDIFF,
  REDIS_COMMAND_SDIFFSTORE,
  REDIS_COMMAND_SINTER,
  REDIS_COMMAND_SINTERSTORE,
  REDIS_COMMAND_SUNION,
  REDIS_COMMAND_SUNIONSTORE,
  REDIS_COMMAND_SADD,
  REDIS_COMMAND_SCARD,
  REDIS_COMMAND_SISMEMBER,
  REDIS_COMMAND_SMEMBERS,
  REDIS_COMMAND_SMOVE,
  REDIS_COMMAND_SPOP,
  REDIS_COMMAND_SRANDMEMBER,
  REDIS_COMMAND_SREM,
  // ZSET
  REDIS_COMMAND_ZADD,
  REDIS_COMMAND_ZCARD,
  REDIS_COMMAND_ZREM,
  REDIS_COMMAND_ZINCRBY,
  REDIS_COMMAND_ZSCORE,
  REDIS_COMMAND_ZRANK,
  REDIS_COMMAND_ZREVRANK,
  REDIS_COMMAND_ZRANGE,
  REDIS_COMMAND_ZREVRANGE,
  REDIS_COMMAND_ZREMRANGEBYRANK,
  REDIS_COMMAND_ZCOUNT,
  REDIS_COMMAND_ZRANGEBYSCORE,
  REDIS_COMMAND_ZREVRANGEBYSCORE,
  REDIS_COMMAND_ZREMRANGEBYSCORE,
  REDIS_COMMAND_ZINTERSTORE,
  REDIS_COMMAND_ZUNIONSTORE,
  // Hash
  REDIS_COMMAND_HSET,
  REDIS_COMMAND_HMSET,
  REDIS_COMMAND_HSETNX,
  REDIS_COMMAND_HGET,
  REDIS_COMMAND_HMGET,
  REDIS_COMMAND_HGETALL,
  REDIS_COMMAND_HVALS,
  REDIS_COMMAND_HKEYS,
  REDIS_COMMAND_HEXISTS,
  REDIS_COMMAND_HDEL,
  REDIS_COMMAND_HINCRBY,
  REDIS_COMMAND_HINCRBYFLOAT,
  REDIS_COMMAND_HLEN,
  // string
  REDIS_COMMAND_GETSET,
  REDIS_COMMAND_SETBIT,
  REDIS_COMMAND_INCR,
  REDIS_COMMAND_INCRBY,
  REDIS_COMMAND_DECR,
  REDIS_COMMAND_DECRBY,
  REDIS_COMMAND_APPEND,
  REDIS_COMMAND_BITCOUNT,
  REDIS_COMMAND_GET,
  REDIS_COMMAND_GETBIT,
  REDIS_COMMAND_GETRANGE,
  REDIS_COMMAND_INCRBYFLOAT,
  REDIS_COMMAND_MGET,
  REDIS_COMMAND_MSET,
  REDIS_COMMAND_SET,
  REDIS_COMMAND_PSETEX,
  REDIS_COMMAND_SETEX,
  REDIS_COMMAND_SETNX,
  REDIS_COMMAND_SETRANGE,
  REDIS_COMMAND_STRLEN,
  // generic commands
  REDIS_COMMAND_TTL,
  REDIS_COMMAND_PTTL,
  REDIS_COMMAND_EXPIRE,
  REDIS_COMMAND_PEXPIRE,
  REDIS_COMMAND_EXPIREAT,
  REDIS_COMMAND_PEXPIREAT,
  REDIS_COMMAND_DEL,
  REDIS_COMMAND_EXISTS,
  REDIS_COMMAND_TYPE,
  REDIS_COMMAND_PERSIST,
  // append new redis cmd_name type here
  REDIS_COMMAND_MAX
};

enum ObRedisModel {
  STRING,
  HASH,
  LIST,
  ZSET,
  SET,
  INVALID
};

class ObRedisInfoV1
{
public:
  static const char *DB_NAME;
  static const char *TABLE_SQLS[];
  static const char *ALL_KV_REDIS_TABLE_SQLS[];
  static const char* STRING_TABLE_NAME;
  static const char* LIST_TABLE_NAME;
  static const char* SET_TABLE_NAME;
  static const char* ZSET_TABLE_NAME;
  static const char* HASH_TABLE_NAME;

public:
  static const int8_t REDIS_MODEL_NUM = 5;
  static const int64_t COMMAND_NUM = 87;
  static const uint64_t GB = 1024 * 1024 * 1024;
  // for tenant memory <= 2G
  static const uint64_t PARTITION_NUM = 38;
  // for tenant memoory > 2G
  static const uint64_t PARTITION_NUM_LARGER = 97;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisInfoV1);
};

}  // end namespace table
}  // end namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_REDIS_COMMON_H_ */
