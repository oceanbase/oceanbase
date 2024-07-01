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
#include "sys_bit.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_sysbit_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                          ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_SYS_BIT_AGG(op_type)                                                                  \
  do {                                                                                             \
    if (in_tc == VEC_TC_INTEGER) {                                                                 \
      ret = init_agg_func<SysBitAggregate<op_type, VEC_TC_INTEGER, VEC_TC_UINTEGER>>(              \
        agg_ctx, agg_col_id, has_distinct, allocator, agg);                                        \
    } else {                                                                                       \
      ret = init_agg_func<SysBitAggregate<op_type, VEC_TC_UINTEGER, VEC_TC_UINTEGER>>(             \
        agg_ctx, agg_col_id, has_distinct, allocator, agg);                                        \
    }                                                                                              \
  } while (false)

  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  bool has_distinct = aggr_info.has_distinct_;
  if (OB_UNLIKELY(aggr_info.param_exprs_.count() != 1
                  || (aggr_info.param_exprs_.at(0)->get_vec_value_tc() != VEC_TC_UINTEGER
                      && aggr_info.param_exprs_.at(0)->get_vec_value_tc() != VEC_TC_INTEGER)
                      && aggr_info.param_exprs_.at(0)->get_vec_value_tc() != VEC_TC_BIT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param exprs", K(ret), K(aggr_info));
  } else if (OB_UNLIKELY(aggr_info.expr_->get_vec_value_tc() != VEC_TC_UINTEGER)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected result expr", K(ret), K(aggr_info));
  } else {
    VecValueTypeClass in_tc = aggr_info.param_exprs_.at(0)->get_vec_value_tc();
    ObExprOperatorType fn_type = aggr_info.get_expr_type();
    if (fn_type == T_FUN_SYS_BIT_OR) {
      INIT_SYS_BIT_AGG(T_FUN_SYS_BIT_OR);
    } else if (fn_type == T_FUN_SYS_BIT_AND) {
      INIT_SYS_BIT_AGG(T_FUN_SYS_BIT_AND);
    } else if (fn_type == T_FUN_SYS_BIT_XOR) {
      INIT_SYS_BIT_AGG(T_FUN_SYS_BIT_XOR);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sysbit operator", K(ret), K(fn_type));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("init sysbit functions failed", K(ret));
    }
  }
  return ret;
}
}
} // end aggregate
} // end share
} // end oceanbase