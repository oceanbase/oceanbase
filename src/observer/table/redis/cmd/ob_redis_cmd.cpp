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
#include "ob_redis_cmd.h"
#include "src/share/table/redis/ob_redis_error.h"

namespace oceanbase
{
namespace table
{
const char *ObRedisAttr::INVALID_NAME = "INVALID";

int RedisCommand::init_common(const common::ObIArray<common::ObString> &args)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    // 1. check entity
    // redis arity refers to the total length including the command itself, for example, "set key value" arity = 3.
    // but args does not include the command itself, that is, only "key" and "value ", so + 1 is required.
    int argc = args.count() + 1;
    bool is_argc_valid = (attr_.arity_ >= 0 && argc == attr_.arity_) || (attr_.arity_ < 0 && argc >= (-attr_.arity_));
    if (!is_argc_valid) {
      ret = OB_KV_REDIS_ERROR;
      LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
    } else if (OB_FAIL(args.at(0, key_))) {
      LOG_WARN("fail to get command key from args", K(ret));
    }
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////
int ObRedisOp::init(ObRedisSingleCtx &redis_ctx, RedisCommand *redis_cmd, ObTableGroupType type)
{
  int ret = OB_SUCCESS;
  redis_cmd_ = redis_cmd;

  if (ObRedisHelper::is_read_only_command(redis_cmd->cmd_type())) {
    outer_allocator_ = &redis_ctx.allocator_; // refer to rpc allocator
    response_.set_allocator(outer_allocator_);
  } else {
    response_.set_allocator(&allocator_);
  }
  response_.set_req_type(redis_ctx.request_.get_req_type());

  // ObITableOp init
  type_ = type;
  req_ = redis_ctx.rpc_req_;
  timeout_ts_ = redis_ctx.timeout_ts_;
  timeout_ = redis_ctx.timeout_;
  tablet_id_ = redis_ctx.tablet_id_;
  ls_id_ = redis_ctx.ls_id_;
  // ObRedisOp init
  db_ = redis_ctx.get_request_db();

  return ret;
}

int ObRedisOp::get_key(ObString &key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(redis_cmd_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null redis cmd", K(ret));
  } else {
    key = redis_cmd_->key();
  }
  return ret;
}

void ObRedisOp::reset()
{
  ObITableOp::reset();
  result_.reset();
  if (OB_NOT_NULL(redis_cmd_)) {
    redis_cmd_->~RedisCommand();
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("redis cmd is NULL", K(lbt()));
  }
  redis_cmd_ = nullptr;
  db_= 0;
  if (allocator_.used() > 0) {
    allocator_.reset();
  }
  default_entity_factory_.free_and_reuse();
  ls_id_ = ObLSID::INVALID_LS_ID;
  outer_allocator_ = nullptr;
  response_.reset();
  tablet_id_.reset();
  result_.reset();
}

int ObRedisOp::get_result(ObITableResult *&result)
{
  return response_.get_result(result);
}
}  // namespace table
}  // namespace oceanbase
