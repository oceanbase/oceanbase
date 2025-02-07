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
#include "ob_redis_set_cmd.h"
#include "observer/table/redis/operator/ob_redis_set_operator.h"

namespace oceanbase
{
namespace table
{

int SetAgg::init(const ObIArray<ObString> &args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    LOG_WARN("fail to init sdiff", K(ret));
  } else {
    keys_ = &args;
    is_inited_ = true;
  }
  return ret;
}

int SetAgg::apply(ObRedisCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sdiff not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_aggregate(redis_ctx.get_request_db(), *keys_, agg_func_))) {
    LOG_WARN("fail to do set diff", K(ret), K_(keys));
  }
  return ret;
}

int SAdd::init(const ObIArray<ObString> &args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    LOG_WARN("fail to init sdiff", K(ret));
  } else if (OB_FAIL(args.at(0, key_))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(members_.create(args.count()))) {
    LOG_WARN("fail to create hash set", K(ret), K(args.count()));
  }

  for (int i = args.count() - 1; OB_SUCC(ret) && i > 0; --i) {
    if (OB_FAIL(members_.set_refactored(args.at(i)))) {
      if (ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to push members", K(ret), K(i), K(args.at(i)));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int SAdd::apply(ObRedisCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sdiff not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_sadd(redis_ctx.get_request_db(), key_, members_))) {
    LOG_WARN("fail to do set diff", K(ret), K(key_));
  }
  return ret;
}

int SetAggStore::init(const ObIArray<ObString> &args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    LOG_WARN("fail to init sdiff", K(ret));
  } else if (OB_FAIL(args.at(0, dest_))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(keys_.reserve(args.count() - 1))) {
    LOG_WARN("fail to reserve space", K(ret), K(args.count()));
  }

  for (int i = 1; OB_SUCC(ret) && i < args.count(); ++i) {
    if (OB_FAIL(keys_.push_back(args.at(i)))) {
      LOG_WARN("fail to push back key0", K(ret), K(i), K(args.at(i)));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int SetAggStore::apply(ObRedisCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sdiff not inited", K(ret));
  } else if (OB_FAIL(
                 cmd_op.do_aggregate_store(redis_ctx.get_request_db(), dest_, keys_, agg_func_))) {
    LOG_WARN("fail to do set diff", K(ret), K_(keys));
  }
  return ret;
}
}  // namespace table
}  // namespace oceanbase
