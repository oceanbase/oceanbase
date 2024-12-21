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
#include "ob_redis_list_cmd.h"
#include "observer/table/redis/operator/ob_redis_list_operator.h"
#include "lib/utility/ob_fast_convert.h"
#include "share/table/redis/ob_redis_util.h"
#include "share/table/redis/ob_redis_error.h"

using namespace oceanbase::table;
using namespace oceanbase::common;

int Push::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else if (OB_FAIL(values_.reserve(args.count() - 1))) {
    LOG_WARN("failed to reserve array", K(ret), K(args.count()));
  } else {
    for (int64_t i = 1; OB_SUCC(ret) && i < args.count(); ++i) {
      ObString value;
      if (OB_FAIL(args.at(i, value))) {
        LOG_WARN("failed to get value", K(ret), K(i), K(args));
      } else if (OB_FAIL(values_.push_back(value))) {
        LOG_WARN("failed to push back value", K(ret), K(value));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  return ret;
}

int LPush::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_push(redis_ctx.get_request_db(), key_, values_, true /*is_push_left*/, false /*need_exist*/))) {
    LOG_WARN("failed to do list lpush", K(ret), K(key_), K(values_));
  }
  return ret;
}

int LPushX::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_push(redis_ctx.get_request_db(), key_, values_, true /*is_push_left*/, true /*need_exist*/))) {
    LOG_WARN("failed to do list lpush", K(ret), K(key_), K(values_));
  }
  return ret;
}

int RPush::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_push(redis_ctx.get_request_db(), key_, values_, false /*is_push_left*/, false /*need_exist*/))) {
    LOG_WARN("failed to do list lpush", K(ret), K(key_), K(values_));
  }
  return ret;
}

int RPushX::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_push(redis_ctx.get_request_db(), key_, values_, false /*is_push_left*/, true /*need_exist*/))) {
    LOG_WARN("failed to do list lpush", K(ret), K(key_), K(values_));
  }
  return ret;
}

int Pop::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    if (is_pop_left_) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::WRONG_ARGS_LPOP_ERR);
    } else {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::WRONG_ARGS_RPOP_ERR);
    }
    LOG_WARN("fail to init cmd", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int LPop::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_pop(redis_ctx.get_request_db(), key_, true /*is_pop_left*/))) {
    LOG_WARN("failed to do list lpop", K(ret), K(key_));
  }
  return ret;
}

int RPop::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_pop(redis_ctx.get_request_db(), key_, false /*is_pop_left*/))) {
    LOG_WARN("failed to do list rpop", K(ret), K(key_));
  }
  return ret;
}

int LIndex::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  ObString index_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else if (OB_FAIL(args.at(1, index_str))) {
    LOG_WARN("failed to get index", K(ret), K(args));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(index_str, offset_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisFmt::NULL_BULK_STRING);
    LOG_WARN("fail to get int from str", K(ret), K(index_str));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int LIndex::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_index(redis_ctx.get_request_db(), key_, offset_))) {
    LOG_WARN("failed to do list lindex", K(ret), K(key_), K(offset_));
  }
  return ret;
}

int LSet::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  ObString index_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else if (OB_FAIL(args.at(1, index_str))) {
    LOG_WARN("failed to get index", K(ret), K(args));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(index_str, offset_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::NO_SUCH_KEY_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(index_str));
  } else if (OB_FAIL(args.at(2, value_))) {
    LOG_WARN("failed to get value", K(ret), K(args));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int LSet::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_set(redis_ctx.get_request_db(), key_, offset_, value_))) {
    LOG_WARN("failed to do list lset", K(ret), K(key_), K(offset_), K(value_));
  }
  return ret;
}

int LRange::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  ObString start_str;
  ObString stop_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else if (OB_FAIL(args.at(1, start_str))) {
    LOG_WARN("failed to get start", K(ret), K(args));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(start_str, start_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(start_str));
  } else if (OB_FAIL(args.at(2, stop_str))) {
    LOG_WARN("failed to get end", K(ret), K(args));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(stop_str, end_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(stop_str));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int LRange::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_range(redis_ctx.get_request_db(), key_, start_, end_))) {
    LOG_WARN("failed to do list lrange", K(ret), K(key_), K(start_), K(end_));
  }
  return ret;
}

int LTrim::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  ObString start_str;
  ObString stop_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else if (OB_FAIL(args.at(1, start_str))) {
    LOG_WARN("failed to get start", K(ret), K(args));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(start_str, start_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(start_str));
  } else if (OB_FAIL(args.at(2, stop_str))) {
    LOG_WARN("failed to get end", K(ret), K(args));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(stop_str, end_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(stop_str));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int LTrim::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_trim(redis_ctx.get_request_db(), key_, start_, end_))) {
    LOG_WARN("failed to do list ltrim", K(ret), K(key_), K(start_), K(end_));
  }
  return ret;
}

int LInsert::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  ObString before_pivot_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else if (OB_FAIL(args.at(1, before_pivot_str))) {
    LOG_WARN("failed to get start", K(ret), K(args));
  } else if (OB_FAIL(args.at(2, pivot_))) {
    LOG_WARN("failed to get pivot", K(ret), K(args));
  } else if (OB_FAIL(args.at(3, value_))) {
    LOG_WARN("failed to get value", K(ret), K(args));
  } else {
    ObArenaAllocator tmp_allocator("RedisLInst", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObString up_str;
    if (OB_FAIL(ob_simple_low_to_up(tmp_allocator, before_pivot_str, up_str))) {
      LOG_WARN("failed to convert pos str to upper case", K(ret), K(before_pivot_str));
    } else if (up_str == ObString::make_string("BEFORE")) {
      is_before_pivot_ = true;
    } else if (up_str == ObString::make_string("AFTER")) {
      is_before_pivot_ = false;
    } else {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
      LOG_WARN("invalid position", K(ret), K(before_pivot_str));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  return ret;
}

int LInsert::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_insert(redis_ctx.get_request_db(), key_, is_before_pivot_, pivot_, value_))) {
    LOG_WARN("failed to do list linsert", K(ret), K(key_), K(is_before_pivot_), K(pivot_), K(value_));
  }
  return ret;
}

int LLen::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  ObString before_pivot_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int LLen::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_get_len(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("failed to do list linsert", K(ret), K(key_));
  }
  return ret;
}

int LRem::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  ObString count_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else if (OB_FAIL(args.at(1, count_str))) {
    LOG_WARN("failed to get count", K(ret), K(args));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(count_str, count_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(count_str));
  } else if (OB_FAIL(args.at(2, value_))) {
    LOG_WARN("failed to get value", K(ret), K(args));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int LRem::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_rem(redis_ctx.get_request_db(), key_, count_, value_))) {
    LOG_WARN("failed to do list lrem", K(ret), K(key_), K(count_), K(value_));
  }
  return ret;
}

int RpopLpush::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  ObString count_str;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init cmd", K(ret));
  } else if (OB_FAIL(args.at(1, dest_key_))) {
    LOG_WARN("failed to get dest_key", K(ret), K(args));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int RpopLpush::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ListCommandOperator cmd_op(redis_ctx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd not inited", K(ret));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("rpoplpush cmd not supported yet", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "rpoplpush");
  }
  return ret;
}
