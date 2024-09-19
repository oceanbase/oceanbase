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
  ObITableEntity *entity = nullptr;
  if (OB_ISNULL(entity = default_entity_factory_.alloc())) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    result_.set_entity(entity);
    // ObITableOp init
    type_ = type;
    req_ = redis_ctx.rpc_req_;
    timeout_ts_ = redis_ctx.timeout_ts_;
    timeout_ = redis_ctx.timeout_;
    tablet_id_ = redis_ctx.tablet_id_;
    ls_id_ = redis_ctx.ls_id_;
    // ObRedisOp init
    db_ = redis_ctx.get_request_db();
    if (OB_FAIL(ob_write_string(allocator_, redis_ctx.request_.get_table_name(), table_name_))) {
      LOG_WARN("fail to write string", K(ret), K(table_name_));
    }
  }

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
  if (OB_NOT_NULL(redis_cmd_)) {
    redis_cmd_->~RedisCommand();
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("redis cmd is NULL", K(lbt()));
  }
  redis_cmd_ = nullptr;
  table_name_.reset();
  db_= 0;
  allocator_.reset();
  default_entity_factory_.reset();
  ls_id_ = ObLSID::INVALID_LS_ID;
}

}  // namespace table
}  // namespace oceanbase
