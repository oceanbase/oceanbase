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

namespace oceanbase
{
using namespace commom;
namespace table
{
int GetSet::init(const ObIArray<ObString> &args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    LOG_WARN("fail to init zadd", K(ret));
  } else if (OB_FAIL(args.at(0, key_))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(args.at(1, new_value_))) {
    LOG_WARN("fail to get value", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int GetSet::apply(ObRedisCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_get_set(redis_ctx.get_request_db(), key_, new_value_))) {
    LOG_WARN("fail to do zadd", K(ret));
  }

  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////
int SetBit::init(const ObIArray<ObString> &args)
{
  int ret = OB_SUCCESS;
  ObString offset_str;
  ObString value_str;
  if (OB_FAIL(init_common(args))) {
    LOG_WARN("fail to init zadd", K(ret));
  } else if (OB_FAIL(args.at(0, key_))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(args.at(1, offset_str))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(args.at(2, value_str))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (value_str.length() != 1 || (value_str[0] != '0' && value_str[0] != '1')) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value should be 0 or 1", K(ret), K(value_str));
  } else {
    value_ = value_str[0] == '0' ? 0 : 1;
    bool is_valid = false;
    offset_ = ObFastAtoi<int32_t>::atoi(
        offset_str.ptr(), offset_str.ptr() + offset_str.length(), is_valid);
    if (!is_valid || offset_ < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("offset should be in [0, 2^32 - 1]", K(ret), K(is_valid), K(offset_));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int SetBit::apply(ObRedisCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_set_bit(redis_ctx.get_request_db(), key_, offset_, value_))) {
    LOG_WARN("fail to do zadd", K(ret));
  }

  return ret;
}

int IncrBy::init(const ObIArray<ObString> &args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    LOG_WARN("fail to init zadd", K(ret));
  } else if (OB_FAIL(args.at(0, key_))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (attr_.cmd_name_ == "INCRBY" || attr_.cmd_name_ == "DECRBY") {
    if (OB_FAIL(args.at(1, incr_))) {
      LOG_WARN("fail to get key", K(ret));
    }
  } else if (attr_.cmd_name_ == "INCR" || attr_.cmd_name_ == "DECR") {
    incr_ = "1";
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cmd name", K(ret), K(attr_.cmd_name_));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int IncrBy::apply(ObRedisCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  StringCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_incrby(redis_ctx.get_request_db(), key_, incr_, is_incr_))) {
    LOG_WARN("fail to do zadd", K(ret));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
