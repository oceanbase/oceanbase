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
#include "ob_redis_string_cmd.h"
#include "observer/table/redis/operator/ob_redis_string_operator.h"
#include "lib/utility/ob_fast_convert.h"
#include "share/table/redis/ob_redis_util.h"
#include "share/table/redis/ob_redis_error.h"

namespace oceanbase
{
using namespace commom;
namespace table
{

int Append::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString value;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init append", K(ret));
  } else if (OB_FAIL(args.at(1, val_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int Append::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("append not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_append(redis_ctx.get_request_db(), key_, val_))) {
    LOG_WARN("fail to do zadd", K(ret));
  }

  return ret;
}

int BitCount::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  int64_t args_count = args.count();
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init bitcount", K(ret));
  } else if (OB_FAIL(args.at(idx++, key_))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (idx < args_count) {
    start_ = args.at(idx++);
    if (idx == args_count) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
      LOG_WARN("invalid cmd args count", K(ret), K(args));
    } else if (idx < args_count) {
      end_ = args.at(idx++);
      if (idx != args_count) {
        RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
        LOG_WARN("invalid cmd args count", K(ret), K(args));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int BitCount::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("bit count not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_bit_count(redis_ctx.get_request_db(), key_, start_, end_))) {
    LOG_WARN("fail to bit count", K(ret));
  }

  return ret;
}

int Get::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init get", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int Get::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("get not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_get(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("fail to do get", K(ret));
  }

  return ret;
}

int GetBit::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString offset_str;
  bool is_valid = false;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init gitbit", K(ret));
  } else if (OB_FAIL(args.at(1, offset_))) {
    LOG_WARN("fail to get offset", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int GetBit::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gitbit not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_get_bit(redis_ctx.get_request_db(), key_, offset_))) {
    LOG_WARN("fail to do gitbit", K(ret));
  }

  return ret;
}

int GetRange::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init getrange", K(ret));
  } else if (OB_FAIL(args.at(1, start_))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(args.at(2, end_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int GetRange::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("getrange not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_get_range(redis_ctx.get_request_db(), key_, start_, end_))) {
    LOG_WARN("fail to do getrange", K(ret));
  }

  return ret;
}

int IncrByFloat::init(const common::ObIArray<common::ObString> &args, ObString &fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init IncrByFloat", K(ret));
  } else if (OB_FAIL(args.at(1, incr_))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (incr_ == ObString("inf") || incr_ == ObString("-inf") || incr_ == ObString("+inf")) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INF_ERR);
    LOG_WARN("fail to init IncrByFloat", K(ret), K(args));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int IncrByFloat::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("IncrByFloat not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_incr_by_float(redis_ctx.get_request_db(), key_, incr_))) {
    LOG_WARN("fail to do IncrByFloat", K(ret));
  }

  return ret;
}

int MGet::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init MGet", K(ret));
  } else {
    keys_ = &args;
    is_inited_ = true;
  }
  return ret;
}

int MGet::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mget not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_mget(redis_ctx.get_request_db(), *keys_))) {
    LOG_WARN("fail to mget", K(ret));
  }

  return ret;
}

int MSet::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  int64_t args_count = args.count();
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init MSet", K(ret));
  } else if (args_count % 2 != 0) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("unexpected args count", K(ret), K(args_count));
  } else if (OB_FAIL(field_val_map_.create(args.count() / 2, ObMemAttr(MTL_ID(), "RedisMSet")))) {
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

int MSet::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mset not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_mset(redis_ctx.get_request_db(), field_val_map_))) {
    LOG_WARN("fail to mset", K(ret), K(redis_ctx.get_request_db()), K_(key));
  }

  return ret;
}

int Set::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString value;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init Set", K(ret));
  } else if (OB_FAIL(args.at(1, set_arg_.value_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    set_arg_.cmd_name_ = attr_.cmd_name_;
    is_inited_ = true;
  }
  return ret;
}

int Set::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Set not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_set(redis_ctx.get_request_db(), key_, set_arg_))) {
    LOG_WARN("fail to Do set", K(ret));
  }

  return ret;
}

int PSetEx::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString expire_time_str_;
  bool is_valid = false;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init PSetEx", K(ret));
  } else if (OB_FAIL(args.at(1, set_arg_.expire_str_))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(args.at(2, set_arg_.value_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    set_arg_.cmd_name_ = attr_.cmd_name_;
    set_arg_.is_ms_ = true;
    is_inited_ = true;
  }

  return ret;
}

int SetEx::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SetEx", K(ret));
  } else if (OB_FAIL(args.at(1, set_arg_.expire_str_))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(args.at(2, set_arg_.value_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    set_arg_.cmd_name_ = attr_.cmd_name_;
    is_inited_ = true;
  }
  return ret;
}

int SetNx::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString value;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SetNx", K(ret));
  } else if (OB_FAIL(args.at(1, set_arg_.value_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    set_arg_.cmd_name_ = attr_.cmd_name_;
    set_arg_.nx_ = true;
    is_inited_ = true;
  }
  return ret;
}

int SetRange::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SetRange", K(ret));
  } else if (OB_FAIL(args.at(1, offset_))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(args.at(2, value_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int SetRange::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("SetRange not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_setrange(redis_ctx.get_request_db(), key_, offset_, value_))) {
    LOG_WARN("fail to do setrange", K(ret));
  }

  return ret;
}

int StrLen::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init StrLen", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int StrLen::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("StrLen not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_strlen(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("fail to do strlen", K(ret));
  }

  return ret;
}

int GetSet::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init GetSet", K(ret));
  } else if (OB_FAIL(args.at(1, new_value_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int GetSet::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("GetSet not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_get_set(redis_ctx.get_request_db(), key_, new_value_))) {
    LOG_WARN("fail to do GetSet", K(ret), K(redis_ctx.get_request_db()), K(key_), K(new_value_));
  }

  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////
int SetBit::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString offset_str;
  ObString value_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SetBit", K(ret));
  } else if (OB_FAIL(args.at(1, offset_str))) {
    LOG_WARN("fail to get offset", K(ret));
  } else if (OB_FAIL(args.at(2, value_str))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (value_str.length() != 1 || (value_str[0] != '0' && value_str[0] != '1')) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::BIT_INTEGER_ERR);
    LOG_WARN("value should be 0 or 1", K(ret), K(value_str));
  } else {
    value_ = value_str[0] == '0' ? 0 : 1;
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int32_t>(offset_str, offset_))) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
      LOG_WARN("fail to get int from str", K(ret), K(offset_str));
    } else if (offset_ < 0) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::BIT_OFFSET_INTEGER_ERR);
      LOG_WARN("offset should be in [0, 2^32 - 1]", K(ret), K(offset_));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int SetBit::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("SetBit not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_set_bit(redis_ctx.get_request_db(), key_, offset_, value_))) {
    LOG_WARN("fail to do SetBit", K(ret), K(redis_ctx.get_request_db()), K(key_), K(offset_), K(value_));
  }

  return ret;
}

int IncrBy::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init IncrBy", K(ret));
  } else if (attr_.cmd_name_ == ObRedisUtil::INCRBY_CMD_NAME || attr_.cmd_name_ == ObRedisUtil::DECRBY_CMD_NAME) {
    if (OB_FAIL(args.at(1, incr_))) {
      LOG_WARN("fail to get increment", K(ret), K(attr_.cmd_name_));
    }
  } else if (attr_.cmd_name_ == ObRedisUtil::INCR_CMD_NAME || attr_.cmd_name_ == ObRedisUtil::DECR_CMD_NAME) {
    incr_ = "1";
  } else {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("invalid cmd name", K(ret), K(attr_.cmd_name_));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int IncrBy::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("IncrBy not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_incrby(redis_ctx.get_request_db(), key_, incr_, is_incr_))) {
    LOG_WARN("fail to do IncrBy", K(ret), K(redis_ctx.get_request_db()), K(key_), K(incr_));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
