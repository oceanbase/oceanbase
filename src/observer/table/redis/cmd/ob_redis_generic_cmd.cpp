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
#include "ob_redis_generic_cmd.h"
#include "observer/table/redis/operator/ob_redis_generic_operator.h"
#include "lib/utility/ob_fast_convert.h"
#include "share/table/redis/ob_redis_util.h"
#include "share/table/redis/ob_redis_error.h"

namespace oceanbase
{
using namespace commom;
namespace table
{
int GenericCommand::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;

  ObString expire_str;
  bool is_valid = false;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObRedisExpire::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;

  ObString expire_str;
  bool is_valid = false;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else if (OB_FAIL(args.at(1, expire_str))) {
    LOG_WARN("fail to get expire str", K(ret));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(expire_str, expire_time_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(expire_str));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObRedisExpire::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  GenericCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expire command not inited", K(ret));
  } else if (expire_time_ <= 0) {
    ObSEArray<ObString, 1> mock_arr("RedisExpire", OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(mock_arr.push_back(key_))) {
      LOG_WARN("fail to push key", K(ret), K(key_));
    } else if (OB_FAIL(cmd_op.do_del(redis_ctx.get_request_db(), mock_arr))) {
      LOG_WARN("fail to do expire", K(ret));
    }
  } else {
    if (OB_FAIL(cmd_op.do_expire(redis_ctx.get_request_db(), key_, expire_time_, unit_))) {
      LOG_WARN("fail to do expire", K(ret));
    }
  }

  return ret;
}

int ObRedisExpireAt::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  GenericCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expireAt command not inited", K(ret));
  } else if (expire_time_ <= 0) {
    ObSEArray<ObString, 1> mock_arr("RedisExpire", OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(mock_arr.push_back(key_))) {
      LOG_WARN("fail to push key", K(ret), K(key_));
    } else if (OB_FAIL(cmd_op.do_del(redis_ctx.get_request_db(), mock_arr))) {
      LOG_WARN("fail to do expire", K(ret));
    }
  } else {
    if (OB_FAIL(cmd_op.do_expire_at(redis_ctx.get_request_db(), key_, expire_time_, unit_))) {
      LOG_WARN("fail to do expireAt", K(ret));
    }
  }

  return ret;
}

int ObRedisTTL::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  GenericCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl command not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_ttl(redis_ctx.get_request_db(), key_, unit_))) {
    LOG_WARN("fail to do ttl", K(ret));
  }

  return ret;
}

int ObRedisExists::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else {
    keys_ = &args;
    is_inited_ = true;
  }
  return ret;
}

int ObRedisExists::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  GenericCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("exists command not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_exists(redis_ctx.get_request_db(), *keys_))) {
    LOG_WARN("fail to do exists", K(ret));
  }

  return ret;
}

int ObRedisType::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  GenericCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("type command not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_type(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("fail to do type", K(ret));
  }

  return ret;
}

int ObRedisPersist::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  GenericCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("persist command not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_persist(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("fail to do persist", K(ret));
  }

  return ret;
}

int ObRedisDel::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init common", K(ret));
  } else {
    keys_ = &args;
    is_inited_ = true;
  }
  return ret;
}

int ObRedisDel::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  GenericCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("del command not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_del(redis_ctx.get_request_db(), *keys_))) {
    LOG_WARN("fail to do del", K(ret));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
