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
#include "ob_redis_zset_cmd.h"
#include "observer/table/redis/operator/ob_redis_zset_operator.h"
#include "lib/utility/ob_fast_convert.h"
#include "share/table/redis/ob_redis_util.h"
#include "share/table/redis/ob_redis_error.h"

namespace oceanbase
{
using namespace commom;
namespace table
{
const ObString ZSetCommand::WITHSCORE = "withscore";
const ObString ZSetCommand::WITHSCORES = "withscores";
const ObString ZSetCommand::LIMIT = "limit";
const ObString ZSetCommand::WEIGHTS = "weights";
const ObString ZSetCommand::AGGREGATE = "aggregate";
const ObString ZSetCommand::POS_INF = "+inf";
const ObString ZSetCommand::NEG_INF = "-inf";

int ZSetCommand::strntod_with_inclusive(const ObString &str, bool &inclusive, double &d, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty string", K(ret));
  } else {
    int offset = 0;
    if (str[0] == '(') {
      inclusive = false;
      offset = 1;
    } else {
      inclusive = true;
    }

    ObString str_cmp(str.length() - offset, str.ptr() + offset);
    if (str_cmp.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid empty string", K(ret));
    } else if (str_cmp.case_compare(POS_INF) == 0) {
      d = INFINITY;
    } else if (str_cmp.case_compare(NEG_INF) == 0) {
      d = -INFINITY;
    } else if (OB_FAIL(ObRedisHelper::string_to_double(str_cmp, d))) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::MIN_MAX_FLOAT_ERR);
      LOG_WARN("fail to convert string to double ", K(ret), K(str_cmp));
    }
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

int ZAdd::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else if (args.count() % 2 == 0) {
    // key score1 member1 score2 member2 ...
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
  } else if (OB_FAIL(mem_score_map_.create(args.count() / 2, ObMemAttr(MTL_ID(), "RedisZAdd")))) {
    LOG_WARN("fail to create member-score map", K(ret));
  }
  for (int i = args.count() - 1; OB_SUCC(ret) && i > 0; i -= 2) {
    const ObString &member = args.at(i);
    const ObString &score_str = args.at(i - 1);
    double unused_score = 0;
    int hash_ret = mem_score_map_.get_refactored(member, unused_score);
    if (hash_ret == OB_SUCCESS) {
      // continue
    } else if (hash_ret != OB_HASH_NOT_EXIST) {
      ret = hash_ret;
      LOG_WARN("fail to find if member exist in set", K(ret), K(i), K(member), K(score_str));
    } else {
      bool is_valid = false;
      int err = 0;
      char *end_ptr = nullptr;
      double score = 0.0;
      if (OB_FAIL(ObRedisHelper::string_to_double(score_str, score))) {
        RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::FLOAT_ERR);
        LOG_WARN("fail to convert string to double ", K(ret), K(score_str));
      } else if (OB_FAIL(mem_score_map_.set_refactored(member, score))) {
        LOG_WARN("fail to add member to set", K(ret), K(i), K(member), K(score));
      }
    }
  }
  if (OB_SUCC(ret)) {
    key_ = args.at(0);
    is_inited_ = true;
  }
  return ret;
}

int ZAdd::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_zadd(redis_ctx.get_request_db(), key_, mem_score_map_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(key_));
  }

  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

int ZCard::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else {
    key_ = args.at(0);
    is_inited_ = true;
  }
  return ret;
}

int ZCard::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_zcard(redis_ctx.get_request_db(), key_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(key_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

int ZRem::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else if (OB_FAIL(members_.create(args.count() - 1, ObMemAttr(MTL_ID(), "RedisZrem")))) {
    // -1 means remove $key argument
    LOG_WARN("fail to reserve members", K(ret), K(args.count() - 1));
  }
  for (int i = 1; OB_SUCC(ret) && i < args.count(); ++i) {
    if (OB_FAIL(members_.set_refactored(args.at(i), 0/*not cover exists object*/))) {
      if (ret != OB_HASH_EXIST) {
        LOG_WARN("fail to push back member", K(ret), K(i), K(args.at(i)));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCC(ret)) {
    key_ = args.at(0);
    is_inited_ = true;
  }

  return ret;
}

int ZRem::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_srem(redis_ctx.get_request_db(), key_, members_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(key_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

int ZIncrBy::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else {
    int err = 0;
    char *endptr = NULL;
    if (OB_FAIL(ObRedisHelper::string_to_double(args.at(1), increment_))) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::FLOAT_ERR);
      LOG_WARN("fail to convert string to double ", K(ret), K(args.at(1)));
    } else {
      key_ = args.at(0);
      member_ = args.at(2);
      is_inited_ = true;
    }
  }
  return ret;
}

int ZIncrBy::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_zincrby(redis_ctx.get_request_db(), key_, member_, increment_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(key_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

int ZScore::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else {
    key_ = args.at(0);
    sub_key_ = args.at(1);
    is_inited_ = true;
  }
  return ret;
}

int ZScore::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_zscore(redis_ctx.get_request_db(), key_, sub_key_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(key_), K(sub_key_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int ZRank::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else if (args.count() > 3) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
  } else {
    zrange_ctx_.key_ = args.at(0);
    member_ = args.at(1);
  }

  // check WITHSCORE
  if (OB_SUCC(ret) && args.count() == 3) {
    if (args.at(2).case_compare(WITHSCORE) != 0) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
      LOG_WARN("third argument should be withscore", K(ret));
    } else {
      zrange_ctx_.with_scores_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ZRank::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (FALSE_IT(zrange_ctx_.db_ = redis_ctx.get_request_db())) {
  } else if (OB_FAIL(cmd_op.do_zrank(
                 redis_ctx.get_request_db(), member_, zrange_ctx_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(member_), K(zrange_ctx_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int ZRange::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else if (args.count() > 4) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(args.at(1), zrange_ctx_.start_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(args.at(1)));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(args.at(2), zrange_ctx_.end_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(args.at(2)));
  } else  {
    zrange_ctx_.key_ = args.at(0);
  }

  // check WITHSCORE
  if (OB_SUCC(ret) && args.count() == 4) {
    if (args.at(3).case_compare(WITHSCORES) != 0) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
      LOG_WARN("4th argument should be withscore", K(ret), K(args.at(3)));
    } else {
      zrange_ctx_.with_scores_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ZRange::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (FALSE_IT(zrange_ctx_.db_ = redis_ctx.get_request_db())) {
  } else if (OB_FAIL(cmd_op.do_zrange(redis_ctx.get_request_db(), zrange_ctx_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(zrange_ctx_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int ZRemRangeByRank::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(args.at(1), zrange_ctx_.start_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(args.at(1)));
  } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(args.at(2), zrange_ctx_.end_))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
    LOG_WARN("fail to get int from str", K(ret), K(args.at(2)));
  } else {
    zrange_ctx_.key_ = args.at(0);
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ZRemRangeByRank::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (FALSE_IT(zrange_ctx_.db_ = redis_ctx.get_request_db())) {
  } else if (OB_FAIL(cmd_op.do_zrem_range_by_rank(redis_ctx.get_request_db(), zrange_ctx_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(zrange_ctx_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

int ZRangeByScore::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else if (args.count() > 7) {
    // key min max WITHSCORES LIMIT offset count
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
  } else {
    zrange_ctx_.key_ = args.at(idx++);
  }

  // check min max
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(
                 strntod_with_inclusive(args.at(idx++), zrange_ctx_.min_inclusive_, zrange_ctx_.min_, fmt_err_msg))) {
    LOG_WARN("fail to do convert string to double", K(ret), K(args.at(1)));
  } else if (OB_FAIL(
                 strntod_with_inclusive(args.at(idx++), zrange_ctx_.max_inclusive_, zrange_ctx_.max_, fmt_err_msg))) {
    LOG_WARN("fail to do convert string to double", K(ret), K(args.at(2)));
  }

  while (OB_SUCC(ret) && idx < args.count()) {
    ObString option = args.at(idx++);
    if (option.case_compare(WITHSCORES) == 0) {
      // check WITHSCORE
      zrange_ctx_.with_scores_ = true;
    } else if (option.case_compare(LIMIT) == 0) {
      // check LIMIT offset count
      if ((args.count() - idx) < 2) {
        RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
        LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
      } else {
        bool is_valid = false;
        ObString offset_str = args.at(idx++);
        ObString limit_str = args.at(idx++);
        if (OB_FAIL(ObRedisHelper::get_int_from_str<int32_t>(offset_str, zrange_ctx_.offset_))) {
          RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
          LOG_WARN("fail to get int from str", K(ret), K(offset_str));
        } else if (OB_FAIL(ObRedisHelper::get_int_from_str<int32_t>(limit_str, zrange_ctx_.limit_))) {
          RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
          LOG_WARN("fail to get int from str", K(ret), K(limit_str));
        } else if (zrange_ctx_.limit_ < 0) {
          // NOTE: It must be INT32_MAX, it can't be -1, because if it is -1, the offset parameter will not take
          // effect.
          zrange_ctx_.limit_ = INT32_MAX;
        }
      }
    } else {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
      LOG_WARN("the argument should be withscore", K(ret), K(args.at(3)));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ZRangeByScore::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (FALSE_IT(zrange_ctx_.db_ = redis_ctx.get_request_db())) {
  } else if (OB_FAIL(cmd_op.do_zrange_by_score(redis_ctx.get_request_db(), zrange_ctx_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(zrange_ctx_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int ZRevRangeByScore::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ZRangeByScore::init(args, fmt_err_msg))) {
    LOG_WARN("fail to init ZRangeByScore", K(ret));
  } else {
    // swap min max
    std::swap(zrange_ctx_.min_, zrange_ctx_.max_);
    std::swap(zrange_ctx_.min_inclusive_, zrange_ctx_.max_inclusive_);
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

int ZRemRangeByScore::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else {
    zrange_ctx_.key_ = args.at(0);
    if (OB_FAIL(strntod_with_inclusive(args.at(1), zrange_ctx_.min_inclusive_, zrange_ctx_.min_, fmt_err_msg))) {
      LOG_WARN("fail to do convert string to double", K(ret), K(args.at(1)));
    } else if (OB_FAIL(
                   strntod_with_inclusive(args.at(2), zrange_ctx_.max_inclusive_, zrange_ctx_.max_, fmt_err_msg))) {
      LOG_WARN("fail to do convert string to double", K(ret), K(args.at(2)));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ZRemRangeByScore::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (FALSE_IT(zrange_ctx_.db_ = redis_ctx.get_request_db())) {
  } else if (OB_FAIL(cmd_op.do_zrem_range_by_score(redis_ctx.get_request_db(), zrange_ctx_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(zrange_ctx_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

int ZCount::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else {
    key_ = args.at(0);
    if (OB_FAIL(strntod_with_inclusive(args.at(1), min_inclusive_, min_, fmt_err_msg))) {
      LOG_WARN("fail to do convert string to double", K(ret), K(args.at(1)));
    } else if (OB_FAIL(strntod_with_inclusive(args.at(2), max_inclusive_, max_, fmt_err_msg))) {
      LOG_WARN("fail to do convert string to double", K(ret), K(args.at(2)));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ZCount::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_zcount(
                 redis_ctx.get_request_db(), key_, min_, min_inclusive_, max_, max_inclusive_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(key_),
                K(min_), K(min_inclusive_), K(max_), K(max_inclusive_));
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

// ZUNIONSTORE/ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]]
// [AGGREGATE <SUM | MIN | MAX>]
int ZSetAggCommand::init(const common::ObIArray<common::ObString> &args, ObString& fmt_err_msg)
{
  int ret = OB_SUCCESS;
  int64_t arg_idx = 0;
  int64_t num_keys = 0;
  // check destination numkeys
  if (OB_FAIL(init_common(args))) {
    RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
    LOG_WARN("fail to init zadd", K(ret));
  } else {
    dest_ = args.at(arg_idx++);
    ObString num_key_str = args.at(arg_idx++);
    if (OB_FAIL(ObRedisHelper::get_int_from_str<int64_t>(num_key_str, num_keys))) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::INTEGER_ERR);
      LOG_WARN("fail to get int from str", K(ret), K(num_key_str));
    } else if (num_keys <= 0) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
      LOG_WARN("invalid start argument", K(ret), K(num_key_str));
    } else if ((args.count() - arg_idx) < num_keys) {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
      LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
    } else if (OB_FAIL(keys_.reserve(num_keys))) {
      LOG_WARN("fail to reserve space for keys", K(ret), K(num_keys));
    } else if (OB_FAIL(weights_.reserve(num_keys))) {
      LOG_WARN("fail to reserve space for weights_", K(ret), K(num_keys));
    }
  }

  // check key [key ...]
  for (int64_t i = 0; OB_SUCC(ret) && i < num_keys; i++) {
    if (OB_FAIL(keys_.push_back(args.at(arg_idx++)))) {
      LOG_WARN("fail to push back array", K(ret), K(i), K(args.at(i)));
    }
  }

  bool has_weight = false;
  while (OB_SUCC(ret) && arg_idx < args.count()) {
    ObString option = args.at(arg_idx++);
    if (option.case_compare(WEIGHTS) == 0) {
      // check [WEIGHTS weight [weight ...]]
      has_weight = true;
      if ((args.count() - arg_idx) < num_keys) {
        RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
        LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < num_keys; i++) {
        ObString weight_str = args.at(arg_idx++);
        double weight = 0.0;
        if (OB_FAIL(ObRedisHelper::string_to_double(weight_str, weight))) {
          RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::FLOAT_ERR);
          LOG_WARN("fail to convert string to double ", K(ret), K(weight_str));
        } else if (OB_FAIL(weights_.push_back(weight))) {
          LOG_WARN("fail to push back array", K(ret), K(i), K(weight));
        }
      }
    } else if (option.case_compare(AGGREGATE) == 0) {
      // check [AGGREGATE <SUM | MIN | MAX>]
      if ((args.count() - arg_idx) < 1) {
        RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
        LOG_WARN("invalid argument num", K(ret), K(attr_), K(args.count()));
      } else {
        ObString agg_str = args.at(arg_idx++);
        if (agg_str.case_compare("sum") == 0) {
          agg_type_ = AggType::SUM;
        } else if (agg_str.case_compare("min") == 0) {
          agg_type_ = AggType::MIN;
        } else if (agg_str.case_compare("max") == 0) {
          agg_type_ = AggType::MAX;
        } else {
          RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
          LOG_WARN("invalid aggregate argument", K(ret), K(agg_str));
        }
      }
    } else {
      RECORD_REDIS_ERROR(fmt_err_msg, ObRedisErr::SYNTAX_ERR);
      LOG_WARN("invalid aggregate argument", K(ret), K(option));
    }
  }

  if (!has_weight) {
    double default_weight = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < num_keys; i++) {
      if (OB_FAIL(weights_.push_back(default_weight))) {
        LOG_WARN("fail to push back array", K(ret), K(i), K(default_weight));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ZSetAggCommand::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("can not apply ZSetAggCommand", K(ret), K(redis_ctx));
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int ZUnionStore::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_zunion_store(
                 redis_ctx.get_request_db(), dest_, keys_, weights_, agg_type_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()),
          K(dest_), K(keys_), K(weights_), K(agg_type_));
  }

  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int ZInterStore::apply(ObRedisSingleCtx &redis_ctx)
{
  int ret = OB_SUCCESS;
  ZSetCommandOperator cmd_op(redis_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("zadd not inited", K(ret));
  } else if (OB_FAIL(cmd_op.do_zinter_store(
                 redis_ctx.get_request_db(), dest_, keys_, weights_, agg_type_))) {
    LOG_WARN("fail to do zadd", K(ret), K(redis_ctx.get_request_db()), K(dest_), K(keys_), K(weights_));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
