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
#define USING_LOG_PREFIX SQL

#include "single_row.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
  // TODO: add approx_distinct_count
  // TODO: add sysbit_ops
#define INIT_GENERAL_CASE(func_type, vec_tc)                                                       \
  case (vec_tc): {                                                                                 \
    ret =                                                                                          \
      init_agg_func<SingleRowAggregate<func_type, vec_tc, vec_tc>>(agg_ctx, i, allocator, agg);    \
    if (OB_FAIL(ret)) { SQL_LOG(WARN, "init aggregate failed", K(vec_tc), K(*aggr_info.expr_)); }  \
  } break

#define INIT_GENERAL_MIN_CASE(vec_tc) INIT_GENERAL_CASE(T_FUN_MIN, vec_tc)

#define INIT_GENERAL_MAX_CASE(vec_tc) INIT_GENERAL_CASE(T_FUN_MAX, vec_tc)

#define INIT_GENERAL_COUNT_SUM_CASE(vec_tc) INIT_GENERAL_CASE(T_FUN_COUNT_SUM, vec_tc)

#define INIT_COUNT_CASE(vec_tc)                                                                    \
  case (vec_tc): {                                                                                 \
    if (lib::is_oracle_mode()) {                                                                   \
      ret = init_agg_func<SingleRowAggregate<T_FUN_COUNT, vec_tc, VEC_TC_NUMBER>>(agg_ctx, i,      \
                                                                                  allocator, agg); \
    } else {                                                                                       \
      ret = init_agg_func<SingleRowAggregate<T_FUN_COUNT, vec_tc, VEC_TC_INTEGER>>(                \
        agg_ctx, i, allocator, agg);                                                               \
    }                                                                                              \
    if (OB_FAIL(ret)) { SQL_LOG(WARN, "init aggregate failed", K(vec_tc), K(*aggr_info.expr_)); }  \
  } break

#define INIT_SUM_TO_NMB_CASE(vec_tc)                                                               \
  ret = init_agg_func<SingleRowAggregate<T_FUN_SUM, vec_tc, VEC_TC_NUMBER>>(agg_ctx, i, allocator, \
                                                                            agg);                  \
  if (OB_FAIL(ret)) { SQL_LOG(WARN, "init aggregate failed", K(vec_tc), K(*aggr_info.expr_)); }

#define INIT_SUM_TO_DEC_CASE(vec_tc, dec_tc)                                                       \
  ret = init_agg_func<SingleRowAggregate<T_FUN_SUM, vec_tc, dec_tc>>(agg_ctx, i, allocator, agg);  \
  if (OB_FAIL(ret)) {                                                                              \
    SQL_LOG(WARN, "init aggregate failed", K(vec_tc), K(dec_tc), K(*aggr_info.expr_));             \
  }

namespace helper
{
static int init_single_row_sum_agg(VecValueTypeClass in_tc, VecValueTypeClass out_tc,
                                   RuntimeContext &agg_ctx, int64_t i, ObIAllocator &allocator,
                                   IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(i);
  switch (in_tc) {
  case VEC_TC_INTEGER: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_INTEGER);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_INTEGER, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_INTEGER, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_UINTEGER: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_UINTEGER);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_UINTEGER, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_UINTEGER, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT32: {
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT32);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT32, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT32, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT64: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT64);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT64, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT64, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT128: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT128, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT128, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT256: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT256);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT256, VEC_TC_DEC_INT256);
      break;
    }
    case VEC_TC_DEC_INT512: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT256, VEC_TC_DEC_INT512);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT512: {
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT512);
      break;
    }
    case VEC_TC_DEC_INT512: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT512, VEC_TC_DEC_INT512);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_NUMBER: {
    if (out_tc != VEC_TC_NUMBER) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      INIT_SUM_TO_NMB_CASE(VEC_TC_NUMBER);
    }
    break;
  }
  case VEC_TC_FLOAT: {
    if (out_tc != VEC_TC_FLOAT) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret =
        init_agg_func<SingleRowAggregate<T_FUN_SUM, VEC_TC_FLOAT, VEC_TC_FLOAT>>(agg_ctx, i, allocator, agg);
    }
    break;
  }
  case VEC_TC_DOUBLE: {
    if (out_tc != VEC_TC_DOUBLE) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret =
        init_agg_func<SingleRowAggregate<T_FUN_SUM, VEC_TC_DOUBLE, VEC_TC_DOUBLE>>(agg_ctx, i, allocator, agg);
    }
    break;
  }
  case VEC_TC_FIXED_DOUBLE: {
    if (out_tc != VEC_TC_FIXED_DOUBLE) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = init_agg_func<SingleRowAggregate<T_FUN_SUM, VEC_TC_FIXED_DOUBLE, VEC_TC_FIXED_DOUBLE>>(
        agg_ctx, i, allocator, agg);
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    break;
  }
  }
  if (OB_FAIL(ret)) {
    SQL_LOG(WARN, "init fast single aggregate failed", K(in_tc), K(out_tc));
  }
  return ret;
}
int init_single_row_aggregates(RuntimeContext &agg_ctx, ObIAllocator &allocator,
                               ObIArray<IAggregate *> &aggregates)
{
  int ret = OB_SUCCESS;
  IAggregate *agg= nullptr;
  for (int i = 0; OB_SUCC(ret) && i < agg_ctx.aggr_infos_.count(); i++) {
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(i);
    agg = nullptr;
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null aggregate expr", K(ret));
    } else if (aggr_info.get_expr_type() == T_FUN_COUNT && aggr_info.param_exprs_.empty()) {
      if (lib::is_mysql_mode()) {
        ret = init_agg_func<SingleRowAggregate<T_FUN_COUNT, VEC_TC_INTEGER, VEC_TC_INTEGER>>(
          agg_ctx, i, allocator, agg);
      } else {
        ret = init_agg_func<SingleRowAggregate<T_FUN_COUNT, VEC_TC_INTEGER, VEC_TC_NUMBER>>(
          agg_ctx, i, allocator, agg);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("init single row aggregate failed", K(ret));
      }
    } else if (aggr_info.is_implicit_first_aggr()) {
      // ObDatumMeta &res_meta = aggr_info.expr_->datum_meta_;
      // VecValueTypeClass res_vec =
      //   get_vec_value_tc(res_meta.type_, res_meta.scale_, res_meta.precision_);
      // switch (res_vec) {
      //   LST_DO_CODE(INIT_GENERAL_CASE, AGG_VEC_TC_LIST);
      //   default: {
      //     ret = OB_ERR_UNEXPECTED;
      //     SQL_LOG(WARN, "unexpected vector type class", K(res_vec), K(*aggr_info.expr_));
      //     break;
      //   }
      // }
      // do nothing
    } else {
      ObDatumMeta &res_meta = aggr_info.expr_->datum_meta_;
      ObDatumMeta &child_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
      VecValueTypeClass res_vec =
        get_vec_value_tc(res_meta.type_, res_meta.scale_, res_meta.precision_);
      VecValueTypeClass param_vec =
        get_vec_value_tc(child_meta.type_, child_meta.scale_, child_meta.precision_);
      switch(aggr_info.get_expr_type()) {
      case T_FUN_COUNT: {
        switch (param_vec) {
          LST_DO_CODE(INIT_COUNT_CASE, AGG_VEC_TC_LIST);
          default: {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
        }
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "init count aggregate failed", K(ret));
        }
        break;
      }
      case T_FUN_SUM: {
        if (OB_FAIL(init_single_row_sum_agg(param_vec, res_vec, agg_ctx, i, allocator, agg))) {
          SQL_LOG(WARN, "init sum agg failed", K(param_vec), K(res_vec), K(ret));
        }
        break;
      }
      case T_FUN_MIN: {
        switch (param_vec) {
          LST_DO_CODE(INIT_GENERAL_MIN_CASE, AGG_VEC_TC_LIST);
          default: {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "unexpected vector type class", K(param_vec), K(*aggr_info.expr_));
          }
        }
        break;
      }
      case T_FUN_MAX: {
        switch (param_vec) {
          LST_DO_CODE(INIT_GENERAL_MAX_CASE, AGG_VEC_TC_LIST);
          default: {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "unexpected vector type class", K(param_vec), K(*aggr_info.expr_));
          }
        }
        break;
      }
      case T_FUN_COUNT_SUM: {
        switch (param_vec) {
          LST_DO_CODE(INIT_GENERAL_COUNT_SUM_CASE, AGG_VEC_TC_LIST);
          default: {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "unexpected vector type class", K(param_vec), K(*aggr_info.expr_));
          }
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        SQL_LOG(WARN, "not supported aggregate operation", K(ret), K(*aggr_info.expr_));
        break;
      }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(aggregates.push_back(agg))) {
      SQL_LOG(WARN, "push back element failed", K(ret));
    }
  } // end for
  return ret;
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase