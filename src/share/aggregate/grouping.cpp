/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL_ENG

#include "grouping.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_grouping_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                            ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  agg = nullptr;
  bool has_distinct = aggr_info.has_distinct_;
  ObExprOperatorType agg_func = aggr_info.get_expr_type();
  agg = nullptr;
  if (agg_func == T_FUN_GROUPING) {
    if (lib::is_mysql_mode()) {
      ret = init_agg_func<GroupingAggregate<T_FUN_GROUPING, VEC_TC_INTEGER>>(
        agg_ctx, agg_col_id, has_distinct, allocator, agg);
    } else {
      ret = init_agg_func<GroupingAggregate<T_FUN_GROUPING, VEC_TC_NUMBER>>(
        agg_ctx, agg_col_id, has_distinct, allocator, agg);
    }
  } else if (agg_func == T_FUN_GROUPING_ID) {
    if (lib::is_mysql_mode()) {
      ret = init_agg_func<GroupingAggregate<T_FUN_GROUPING_ID, VEC_TC_INTEGER>>(
        agg_ctx, agg_col_id, has_distinct, allocator, agg);
    } else {
      ret = init_agg_func<GroupingAggregate<T_FUN_GROUPING_ID, VEC_TC_NUMBER>>(
        agg_ctx, agg_col_id, has_distinct, allocator, agg);
    }
  } else if (agg_func == T_FUN_GROUP_ID) {
    if (lib::is_mysql_mode()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported in mysql", K(ret));
    } else {
      ret = init_agg_func<GroupingAggregate<T_FUN_GROUP_ID, VEC_TC_NUMBER>>(
        agg_ctx, agg_col_id, has_distinct, allocator, agg);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid aggregate function", K(ret), K(agg_func));
  }
  return ret;
}

} // end helper

static bool is_rollup_expr(const ObAggrInfo &aggr_info, ObExpr *expr, int64_t seq)
{
  int ret = false;
  if (has_exist_in_array(aggr_info.hash_rollup_info_->gby_exprs_, expr)
      || !has_exist_in_array(aggr_info.hash_rollup_info_->expand_exprs_, expr)) {
    // do nothing
  } else {
    ObIArray<ObExpr *> &expand_exprs = aggr_info.hash_rollup_info_->expand_exprs_;
    bool found = false;
    for (int i = expand_exprs.count() - seq - 1; !found && i >= 0; i--) {
      found = expr == aggr_info.hash_rollup_info_->expand_exprs_.at(i);
    }
    ret = !found;
  }
  return ret;
}

static bool is_grouping_expr(const ObAggrInfo &aggr_info, ObExpr *expr, int64_t seq)
{
  bool  ret = false;
  ObIArray<ObExpr *> &group_exprs = aggr_info.grouping_set_info_->groupset_exprs_.at(seq);
  if (has_exist_in_array(group_exprs, expr)) {
    ret = false;
  } else {
    for (int i = 0; !ret && i < aggr_info.grouping_set_info_->groupset_exprs_.count(); i++) {
      if (i == seq) { continue; }
      ret = has_exist_in_array(aggr_info.grouping_set_info_->groupset_exprs_.at(i), expr);
    }
    if (!ret) {
      ret = has_exist_in_array(aggr_info.grouping_set_info_->pruned_exprs_, expr);
    }
  }
  LOG_DEBUG("grouping expr", KP(expr), K(*aggr_info.grouping_set_info_), K(ret));
  return ret;
}
bool is_grouping(const ObAggrInfo &aggr_info, const int64_t seq)
{
  int ret = false;
  OB_ASSERT(aggr_info.param_exprs_.count() == 1 && aggr_info.param_exprs_.at(0) != nullptr);
  ObExpr *param_expr = aggr_info.param_exprs_.at(0);
  if (aggr_info.hash_rollup_info_ != nullptr) {
    ret = is_rollup_expr(aggr_info, param_expr, seq);
  } else {
    ret = is_grouping_expr(aggr_info, param_expr, seq);
  }
  return ret;
}

int get_grouping_id(const ObAggrInfo &aggr_info, const int64_t seq,
                    number::ObCompactNumber *grouping_id)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &param_exprs = aggr_info.param_exprs_;
  int512_t res = 0, base = 1;
  if (OB_UNLIKELY(param_exprs.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected empty param exprs", K(ret));
  }
  for (int i = param_exprs.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    ObExpr *grouping_expr = param_exprs.at(i);
    if (OB_ISNULL(grouping_expr)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null expr", K(ret));
    } else if ((aggr_info.hash_rollup_info_ != nullptr && is_rollup_expr(aggr_info, grouping_expr, seq))
               || (aggr_info.grouping_set_info_ != nullptr && is_grouping_expr(aggr_info, grouping_expr, seq))) {
      res += base;
    }
    if (OB_SUCC(ret)) {
      base = base << 1;
    }
  }
  if (OB_SUCC(ret)) {
    number::ObNumber tmp_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(wide::to_number(res, 0, tmp_alloc, tmp_nmb))) {
      SQL_LOG(WARN, "to_number failed", K(ret));
    } else {
      grouping_id->desc_ = tmp_nmb.d_;
      MEMCPY(grouping_id->digits_, tmp_nmb.get_digits(), tmp_nmb.d_.len_ * sizeof(uint32_t));
    }
  }
  return ret;
}

int get_grouping_id(const ObAggrInfo &aggr_info, const int64_t seq,
                    int64_t *grouping_id)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &param_exprs = aggr_info.param_exprs_;
  int64_t res = 0;
  int64_t base = 1;
  if (OB_UNLIKELY(param_exprs.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected empty param exprs", K(ret));
  } else if (OB_ISNULL(grouping_id)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected null", K(ret));
  }
  for (int i = param_exprs.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    ObExpr *grouping_expr = param_exprs.at(i);
    if (OB_ISNULL(grouping_expr)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null expr", K(ret));
    } else if ((aggr_info.hash_rollup_info_ != nullptr && is_rollup_expr(aggr_info, grouping_expr, seq))
               || (aggr_info.grouping_set_info_ != nullptr && is_grouping_expr(aggr_info, grouping_expr, seq))) {
      res += base;
    }
    if (OB_SUCC(ret)) {
      base = base << 1;
    }
  }
  if (OB_SUCC(ret)) {
    *grouping_id = res;
  }
  return ret;
}

int get_group_id(const HashRollupRTInfo *hash_rollup_info, const int64_t seq, int64_t &group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(seq >= hash_rollup_info->expand_exprs_.count()) || OB_UNLIKELY(seq < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sequence id", K(ret), K(seq), K(hash_rollup_info->expand_exprs_.count()));
  } else if (seq == 0) {
    group_id = 0;
  } else {
    ObExpr *cur_rollup = hash_rollup_info->expand_exprs_.at(seq);
    int64_t appear_cnt = 0;
    for (int i = hash_rollup_info->expand_exprs_.count() - 1; i > seq; i--) {
      appear_cnt += (hash_rollup_info->expand_exprs_.at(i) == cur_rollup);
    }
    if (has_exist_in_array(hash_rollup_info->gby_exprs_, cur_rollup)) {
      group_id = appear_cnt;
    } else if (appear_cnt > 0) {
      group_id = appear_cnt - 1;
    } else {
      group_id = 0;
    }
  }
  return ret;
}

static bool is_same_group_exprs(const ObIArray<ObExpr *> &l_exprs, const ObIArray<ObExpr *> &r_exprs)
{
  bool ret = true;
  ret = l_exprs.count() == r_exprs.count();
  for (int i = 0; ret && i < l_exprs.count(); i++) {
    ret = (l_exprs.at(i) == r_exprs.at(i));
  }
  return ret;
}

int get_group_id(const GroupingSetRTInfo *grouping_set_info, const int64_t seq, int64_t &group_id)
{
  int ret = OB_SUCCESS;
  group_id = 0;
  if (OB_UNLIKELY(seq >= grouping_set_info->groupset_exprs_.count() || OB_UNLIKELY(seq < 0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sequence id", K(ret), K(seq), K(grouping_set_info->groupset_exprs_.count()));
  } else {
    int64_t appear_cnt = 0;
    const ObIArray<ObExpr *> &group_exprs = grouping_set_info->groupset_exprs_.at(seq);
    for (int i = grouping_set_info->groupset_exprs_.count() - 1; i > seq; i--) {
      appear_cnt += is_same_group_exprs(grouping_set_info->groupset_exprs_.at(i), group_exprs);
    }
    group_id = appear_cnt;
  }
  return ret;
}

int get_group_id(const ObAggrInfo &aggr_info, const int64_t seq, number::ObCompactNumber *nmb_group_id)
{
  int ret = OB_SUCCESS;
  int64_t group_id = 0;
  if (aggr_info.grouping_set_info_ != nullptr
      && OB_FAIL(get_group_id(aggr_info.grouping_set_info_, seq, group_id))) {
    LOG_WARN("get group id failed", K(ret));
  } else if (aggr_info.hash_rollup_info_ != nullptr
             && OB_FAIL(get_group_id(aggr_info.hash_rollup_info_, seq, group_id))) {
  } else {
    number::ObNumber tmp_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(wide::to_number(group_id, 0, tmp_alloc, tmp_nmb))) {
      SQL_LOG(WARN, "to_number failed", K(ret));
    } else {
      nmb_group_id->desc_ = tmp_nmb.d_;
      MEMCPY(nmb_group_id->digits_, tmp_nmb.get_digits(), tmp_nmb.d_.len_ * sizeof(uint32_t));
    }
  }
  return ret;
}
} // end aggregate
} // end share
} // end oceanbase