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
#include "src/share/table/redis/ob_redis_error.h"

namespace oceanbase
{
namespace table
{

int SetAgg::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SetAgg", K(ret));
  } else {
    keys_ = &args;
    is_inited_ = true;
  }
  return ret;
}

int SetAgg::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SetAgg not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_aggregate(redis_ctx.get_request_db(), *keys_, agg_func_))) {
    LOG_WARN("fail to do SetAgg", K(ret), K_(keys));
  }
  return ret;
}

int SAdd::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SAdd", K(ret));
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

int SAdd::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SAdd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_sadd(redis_ctx.get_request_db(), key_, members_))) {
    LOG_WARN("fail to do sadd", K(ret), K(key_));
  }
  return ret;
}

int SetAggStore::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SetAggStore", K(ret));
  } else if (OB_FAIL(args.at(0, dest_))) {
    LOG_WARN("fail to get dest", K(ret));
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

int SetAggStore::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SetAggStore not inited", K(ret));
  } else if (OB_FAIL(
                 cmd_op.do_aggregate_store(redis_ctx.get_request_db(), dest_, keys_, agg_func_))) {
    LOG_WARN("fail to do SetAggStore", K(ret), K_(keys));
  }
  return ret;
}

int SCard::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SCard", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int SCard::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SCard not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_scard(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("fail to do SCard", K(ret), K(key_));
  }
  return ret;
}

int SPop::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init spop", K(ret));
  } else if (args.count() > 1 && OB_FAIL(args.at(1, count_str_))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (args.count() > 2) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("invalid argument num", K(ret), K(args.count()));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int SPop::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SPop not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_spop(redis_ctx.get_request_db(), key_, count_str_))) {
    LOG_WARN("fail to do pop", K(ret), K_(key), K_(count_str));
  }
  return ret;
}

int SIsMember::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SIsMember", K(ret));
  } else if (OB_FAIL(args.at(1, sub_key_))) {
    LOG_WARN("fail to get member", K(ret));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int SIsMember::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SIsMember not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_sismember(redis_ctx.get_request_db(), key_, sub_key_))) {
    LOG_WARN("fail to do SIsMember", K(ret), K(key_));
  }
  return ret;
}

int SRandMember::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SRandMember", K(ret));
  } else if (args.count() > 1 && OB_FAIL(args.at(1, count_str_))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (args.count() > 2) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("invalid argument num", K(ret), K(args.count()));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int SRandMember::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SRandMember not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_srand_member(redis_ctx.get_request_db(), key_, count_str_))) {
    LOG_WARN("fail to do set rand member", K(ret), K_(key));
  }
  return ret;
}

int SMembers::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SMembers", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int SMembers::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SMembers not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_smembers(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("fail to do SMembers", K(ret), K(key_));
  }
  return ret;
}

int SMove::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SMove", K(ret));
  } else if (OB_FAIL(args.at(0, src_))) {
    LOG_WARN("fail to get source", K(ret));
  } else if (OB_FAIL(args.at(1, dest_))) {
    LOG_WARN("fail to get dest", K(ret));
  } else if (OB_FAIL(args.at(2, member_))) {
    LOG_WARN("fail to get member", K(ret));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int SMove::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SMove not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_smove(redis_ctx.get_request_db(), src_, dest_, member_))) {
    LOG_WARN("fail to do SMove", K(ret), K(src_), K(dest_), K(member_));
  }
  return ret;
}

int SRem::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init SRem", K(ret));
  } else if (OB_FAIL(members_.create(args.count()))) {
    LOG_WARN("fail to create hash set", K(ret), K(args.count()));
  }

  for (int i = 1; OB_SUCC(ret) && i < args.count(); ++i) {
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

int SRem::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  SetCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("SRem not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_srem(redis_ctx.get_request_db(), key_, members_))) {
    LOG_WARN("fail to do set rem", K(ret), K_(key));
  }
  return ret;
}
}  // namespace table
}  // namespace oceanbase
