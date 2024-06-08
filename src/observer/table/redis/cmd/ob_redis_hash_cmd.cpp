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

namespace oceanbase
{
using namespace commom;
namespace table
{
int HSet::init(const ObIArray<ObString> &args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    LOG_WARN("fail to init zadd", K(ret));
  } else if (args.count() % 2 == 0) {
    // key field1 value1 field2 value2 ...
    ret = OB_ERR_INVALID_INPUT_ARGUMENT;
    LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
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

int HSet::apply(ObRedisCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hset(redis_ctx.get_request_db(), key_, field_val_map_))) {
    LOG_WARN("fail to do zadd", K(ret));
  }

  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////
int HMSet::apply(ObRedisCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  HashCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_hmset(redis_ctx.get_request_db(), key_, field_val_map_))) {
    LOG_WARN("fail to do zadd", K(ret));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
