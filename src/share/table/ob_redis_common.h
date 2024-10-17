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

#include "lib/utility/ob_macro_utils.h"
#include "share/table/ob_table.h"
#include "lib/oblog/ob_log_module.h"
#include "observer/table/ob_table_rpc_processor_util.h"
#include "share/table/ob_table_rpc_struct.h"

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
  static const char *FMT_MINUS_ONE;
  static const char *NULL_BULK_STRING; // client display '(nil)'
  static const char *NULL_ARRAY; // client display '(nil)'

  static const int HEADER_LEN = 3;  // "*\r\n"
  static const int FLAG_LEN = 1;    // "*"
  static const int CRLF_LEN = 2;    // "\r\n"
  static const int ROWKEY_IS_DATA_IDX = 2;

  static const ObString FMT_OK;
  static const ObString FMT_SYNTAX_ERR;
  static const ObString FMT_FLOAT_ERR;
  static const char *const FMT_EMPTY_ARRAY;
  static const char *const FMT_ZERO;
public:
  static const int LIST_ROWKEY_NUM = 3;    // db + key + index
  static const ObString REDIS_DB_NAME;
  static const ObString REDIS_INDEX_NAME;
  static const char *const REDIS_VALUE_CNAME;
  static const ObString REDIS_VALUE_NAME;
  static const char *const REDIS_EXPIRE_CNAME;
  static const ObString REDIS_EXPIRE_NAME;
  // Redis command property name in the request entity
  static const char *const REDIS_CODE_PROPERTY_CNAME;
  static const ObString REDIS_PROPERTY_NAME;
  static const ObString DB_PROPERTY_NAME;
  static const ObString RKEY_PROPERTY_NAME;
  static const ObString INSERT_TS_PROPERTY_NAME;
  static const ObString SCORE_PROPERTY_NAME;
  static const int COMPLEX_ROWKEY_NUM = 4;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisUtil);
};

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
  REDIS_COMMAND_LDEL,
  // Set
  REDIS_COMMAND_SDIFF,
  REDIS_COMMAND_SDIFFSTORE,
  REDIS_COMMAND_SINTER,
  REDIS_COMMAND_SINTERSTORE,
  REDIS_COMMAND_SUNION,
  REDIS_COMMAND_SUNIONSTORE,
  REDIS_COMMAND_SADD,
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
  // string
  REDIS_COMMAND_GETSET,
  REDIS_COMMAND_SETBIT,
  REDIS_COMMAND_INCR,
  REDIS_COMMAND_INCRBY,
  REDIS_COMMAND_DECR,
  REDIS_COMMAND_DECRBY,
  // append new redis cmd_name type here
  REDIS_COMMAND_MAX
};

enum ObRedisModel {
  STRING,
  HASH,
  SET,
  ZSET,
  LIST,
  INVALID
};

// Format conversion in redis protocol and obtable
class RedisOperationHelper
{
public:
  /**
   * Get the decoded message from the entity
   * @param entity Entity object
   * @param decode_msgs The decoded array of messages
   * @return retcode
   */
  static int get_decoded_msg_from_entity(ObIAllocator &allocator,
                                         const ObITableEntity &entity,
                                         ObString &cmd_name,
                                         ObArray<ObString> &args);
  /**
   * Get the Redis cmd_name type according to the cmd_name
   * @param cmd_name Redis cmd name
   * @return Redis cmd type
   */
  static RedisCommandType get_command_type(const ObString &cmd_name);

  static observer::ObTableProccessType redis_cmd_to_proccess_type(const RedisCommandType &redis_cmd_type);

  static int get_lock_key(ObIAllocator &allocator, const ObRedisRequest &redis_req, ObString &lock_key);
};

// Encapsulate the input and output parameters of redis
class ObRedisRequest
{
public:
  explicit ObRedisRequest(common::ObIAllocator &allocator, const ObTableOperationRequest &request)
      : allocator_(allocator),
        request_(request),
        cmd_name_(),
        args_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisArgs")),
        db_(-1)
  {}
  ~ObRedisRequest()
  {}
  TO_STRING_KV(K_(request), K_(cmd_name), K_(args), K_(db));
  // Parse request_ as cmd_name_, args_
  int decode();

  OB_INLINE const ObString &get_cmd_name() const
  {
    return cmd_name_;
  }

  OB_INLINE RedisCommandType get_command_type() const
  {
    return RedisOperationHelper::get_command_type(cmd_name_);
  }
  OB_INLINE const ObArray<ObString> &get_args() const
  {
    return args_;
  }

  OB_INLINE const ObTableOperationRequest &get_request() const
  {
    return request_;
  }

  OB_INLINE const ObITableEntity &get_entity() const
  {
    return request_.table_operation_.entity();
  }

  OB_INLINE int64_t get_db() const
  {
    return db_;
  }

private:
  common::ObIAllocator &allocator_;
  const ObTableOperationRequest &request_;
  ObString cmd_name_;
  ObArray<ObString> args_;
  // all redis command include db info
  int64_t db_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisRequest);
};

class ObRedisResponse
{
public:
  ObRedisResponse(common::ObIAllocator &allocator, ObTableOperationResult &result)
      : allocator_(allocator),
        result_(result)
  {}
  ~ObRedisResponse()
  {}
  TO_STRING_KV(K_(result));
  OB_INLINE ObTableOperationResult &get_result()
  {
    return result_;
  }
  // set res to redis fmt
  int set_res_int(int64_t res);
  int set_res_simple_string(const ObString &res);
  int set_res_bulk_string(const ObString &res);
  int set_res_array(const ObIArray<ObString> &res);
  int set_res_error(const ObString &err_msg);
  // table_api failed, return error code
  void return_table_error(int ret);
  // Some common hard-coded redis protocol return values, directly returned
  int set_fmt_res(const ObString &fmt_res);

private:
  common::ObIAllocator &allocator_;
  ObTableOperationResult &result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedisResponse);
};

}  // end namespace table
}  // end namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_REDIS_COMMON_H_ */
