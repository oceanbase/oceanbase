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
#define USING_LOG_PREFIX SERVER
#include "ob_redis_hash_cmd.h"
#include "observer/table/redis/operator/ob_redis_hash_operator.h"
#include "lib/utility/ob_fast_convert.h"
#include "share/table/redis/ob_redis_util.h"
#include "share/table/redis/ob_redis_error.h"

namespace oceanbase
{
using namespace commom;
namespace table
{
int HSet::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (args.count() % 2 == 0) {
    // key field1 value1 field2 value2 ...
    LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::WRONG_ARGS_ERR);
  } else if (OB_FAIL(field_val_map_.create(args.count() / 2, ObMemAttr(MTL_ID(), "RedisHSet")))) {
    LOG_WARN("fail to create field value map", K(ret));
  }
  for (int i = args.count() - 1; OB_SUCC(ret) && i > 0; i -= 2) {
    const ObString &value = args.at(i);
    const ObString &field = args.at(i - 1);
    if (OB_FAIL(field_val_map_.set_refactored(field, value))) {
      if (ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to add member to set", K(ret), K(i), K(field), K(value));
      }
    }
  }
  if (OB_SUCC(ret)) {
    key_ = args.at(0);
    is_inited_ = true;
  }
  return ret;
}

int HSet::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("hset not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hset(redis_ctx.get_request_db(), key_, field_val_map_))) {
    LOG_WARN("fail to do hset", K(ret), K(redis_ctx.get_request_db()), K(key_));
  }

  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////
int HMSet::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("hmset not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hmset(redis_ctx.get_request_db(), key_, field_val_map_))) {
    LOG_WARN("fail to do hmset", K(ret), K(redis_ctx.get_request_db()), K(key_));
  }

  return ret;
}

int HSetNX::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (OB_FAIL(args.at(1, field_))) {
    LOG_WARN("fail to get field", K(ret));
  } else if (OB_FAIL(args.at(2, new_value_))) {
    LOG_WARN("fail to get new value", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int HSetNX::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("hsetnx not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hsetnx(redis_ctx.get_request_db(), key_, field_, new_value_))) {
    LOG_WARN("fail to do hsetnx", K(ret), K(redis_ctx.get_request_db()), K(key_), K(field_), K(new_value_));
  }

  return ret;
}

int HExists::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (OB_FAIL(args.at(1, sub_key_))) {
    LOG_WARN("fail to get field", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int HExists::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("HExists not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hexists(redis_ctx.get_request_db(), key_, sub_key_))) {
    LOG_WARN("fail to do HExists", K(ret), K(redis_ctx.get_request_db()), K(key_), K(sub_key_));
  }

  return ret;
}

int HGet::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (OB_FAIL(args.at(1, sub_key_))) {
    LOG_WARN("fail to get field", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int HGet::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("HGet not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hget(redis_ctx.get_request_db(), key_, sub_key_))) {
    LOG_WARN("fail to do HGet", K(ret), K(redis_ctx.get_request_db()), K(key_), K(sub_key_));
  }

  return ret;
}

int HGetAll::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int HGetAll::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("HGetAll not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hget_all(redis_ctx.get_request_db(), key_, need_fields_, need_vals_))) {
    LOG_WARN("fail to do HGetAll", K(ret), K(redis_ctx.get_request_db()), K(key_), K(need_fields_), K(need_vals_));
  }

  return ret;
}

int HLen::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int HLen::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("HLen not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hlen(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("fail to do HLen", K(ret), K(redis_ctx.get_request_db()), K(key_));
  }

  return ret;
}

int HMGet::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (OB_FAIL(fields_.reserve(args.count() - 1))) {
    LOG_WARN("fail to create field value map", K(ret), K(args.count() - 1));
  }
  // HMGET does not remove duplicate keys
  for (int i = 1; OB_SUCC(ret) && i < args.count(); ++i) {
    if (OB_FAIL(fields_.push_back(args.at(i)))) {
      LOG_WARN("fail to push back field", K(ret), K(i), K(args.at(i)));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int HMGet::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("HMGet not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hmget(redis_ctx.get_request_db(), key_, fields_))) {
    LOG_WARN("fail to do HMGet", K(ret), K(redis_ctx.get_request_db()), K(key_), K(fields_));
  }

  return ret;
}

int HDel::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (OB_FAIL(field_set_.create(args.count() / 2, ObMemAttr(MTL_ID(), "RedisHDel")))) {
    LOG_WARN("fail to create field value map", K(ret));
  }
  // HMGET does not remove duplicate keys
  for (int i = 1; OB_SUCC(ret) && i < args.count(); ++i) {
    if (OB_FAIL(field_set_.set_refactored(args.at(i)))) {
      if (ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to add member to set", K(ret), K(i), K(args.at(i)));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int HDel::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("HDel not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hdel(redis_ctx.get_request_db(), key_, field_set_))) {
    LOG_WARN("fail to do HDel", K(ret), K(redis_ctx.get_request_db()), K(key_));
  }

  return ret;
}

int HIncrBy::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString incr_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (OB_FAIL(args.at(1, field_))) {
    LOG_WARN("fail to get field", K(ret));
  } else if (OB_FAIL(args.at(2, incr_str))) {
    LOG_WARN("fail to get increment str", K(ret));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(incr_str, increment_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::HASH_VALUE_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(incr_str));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int HIncrBy::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("HIncrBy not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hincrby(redis_ctx.get_request_db(), key_, field_, increment_))) {
    LOG_WARN("fail to do hincrby", K(ret), K(key_), K(field_), K(increment_));
  }

  return ret;
}

int HIncrByFloat::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString incr_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (OB_FAIL(args.at(1, field_))) {
    LOG_WARN("fail to get field", K(ret));
  } else if (OB_FAIL(args.at(2, incr_str))) {
    LOG_WARN("fail to get increment str", K(ret));
  } else if (OB_FAIL(ObRedisHelper::string_to_long_double(incr_str, increment_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::FLOAT_ERR);
    LOG_WARN("fail to convert string to double", K(ret), K(incr_str));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int HIncrByFloat::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("HIncrByFloat not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hincrbyfloat(redis_ctx.get_request_db(), key_, field_, increment_))) {
    LOG_WARN("fail to do HIncrByFloat", K(ret), K(key_), K(field_));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
