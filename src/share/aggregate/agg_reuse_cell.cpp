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

#define USING_LOG_PREFIX SHARE

#include "agg_reuse_cell.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
int ReuseAggCellMgr::init(RuntimeContext &agg_ctx)
{
#define DO_INIT_CELL(agg_func)                                                                     \
  do {                                                                                             \
    if (OB_ISNULL(tmp_store_vals_.at(i) =                                                          \
                    allocator_.alloc(ReuseAggCell<agg_func>::stored_size()))) {                    \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                             \
      LOG_WARN("allocate memory failed", K(ret));                                                  \
    }                                                                                              \
  } while (false)

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(agg_ctx.aggr_infos_.count() <= 0)) {
    // do nothing
  } else if (OB_FAIL(tmp_store_vals_.prepare_allocate(agg_ctx.aggr_infos_.count()))) {
    LOG_WARN("prepare allocate failed", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < agg_ctx.aggr_infos_.count(); i++) {
      ObExprOperatorType expr_type = agg_ctx.aggr_infos_.at(i).get_expr_type();
      switch (expr_type) {
      case T_FUN_MAX:
      case T_FUN_MIN: {
        DO_INIT_CELL(T_FUN_MAX);
        break;
      }
      case T_FUN_WM_CONCAT:
      case T_FUN_KEEP_WM_CONCAT:
      case T_FUN_GROUP_CONCAT: {
        DO_INIT_CELL(T_FUN_GROUP_CONCAT);
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT: {
        DO_INIT_CELL(T_FUN_APPROX_COUNT_DISTINCT);
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        DO_INIT_CELL(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS);
        break;
      }
      default: {
        tmp_store_vals_.at(i) = nullptr;
      }
      }
    }
  }
  return ret;

#undef DO_INIT_CELL
}

int ReuseAggCellMgr::save(RuntimeContext &agg_ctx, const char *agg_row)
{
#define DO_SAVE(agg_func)                                                                          \
  do {                                                                                             \
    if (OB_ISNULL(tmp_store_vals_.at(i))) {                                                        \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      LOG_WARN("unexpected null value", K(ret));                                                   \
    } else if (OB_FAIL(                                                                            \
                 ReuseAggCell<agg_func>::save(agg_ctx, i, agg_row, tmp_store_vals_.at(i)))) {      \
      LOG_WARN("save failed", K(ret));                                                             \
    }                                                                                              \
  } while (false)

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(agg_ctx.aggr_infos_.count() <= 0)) {
    // do nothing
  } else {
    for (int i = 0; OB_SUCC(ret) && i < agg_ctx.aggr_infos_.count(); i++) {
      if (agg_ctx.aggr_infos_.at(i).is_implicit_first_aggr()) { continue; }
      ObExprOperatorType expr_type = agg_ctx.aggr_infos_.at(i).get_expr_type();
      switch (expr_type) {
      case T_FUN_MIN:
      case T_FUN_MAX: {
        DO_SAVE(T_FUN_MAX);
        break;
      }
      case T_FUN_WM_CONCAT:
      case T_FUN_KEEP_WM_CONCAT:
      case T_FUN_GROUP_CONCAT: {
        DO_SAVE(T_FUN_GROUP_CONCAT);
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT: {
        DO_SAVE(T_FUN_APPROX_COUNT_DISTINCT);
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        DO_SAVE(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS);
        break;
      }
      default: {
        break;
      }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(save_extra_stores(agg_ctx, agg_row))) {
    LOG_WARN("save extra stores failed", K(ret));
  }
  return ret;

#undef DO_SAVE
}

int ReuseAggCellMgr::restore(RuntimeContext &agg_ctx, char *agg_row)
{
#define DO_RESTORE(agg_func)                                                                       \
  do {                                                                                             \
    if (OB_ISNULL(tmp_store_vals_.at(i))) {                                                        \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      LOG_WARN("unexpected null value", K(ret));                                                   \
    } else if (OB_FAIL(                                                                            \
                 ReuseAggCell<agg_func>::restore(agg_ctx, i, agg_row, tmp_store_vals_.at(i)))) {   \
      LOG_WARN("save failed", K(ret));                                                             \
    }                                                                                              \
  } while (false)

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(agg_ctx.aggr_infos_.count() <= 0)) {
    // do nothing
  } else {
    for (int i = 0; OB_SUCC(ret) && i < agg_ctx.aggr_infos_.count(); i++) {
      if (agg_ctx.aggr_infos_.at(i).is_implicit_first_aggr()) { continue; }
      ObExprOperatorType expr_type = agg_ctx.aggr_infos_.at(i).get_expr_type();
      switch(expr_type) {
      case T_FUN_MIN:
      case T_FUN_MAX: {
        DO_RESTORE(T_FUN_MAX);
        break;
      }
      case T_FUN_WM_CONCAT:
      case T_FUN_KEEP_WM_CONCAT:
      case T_FUN_GROUP_CONCAT: {
        DO_RESTORE(T_FUN_GROUP_CONCAT);
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT: {
        DO_RESTORE(T_FUN_APPROX_COUNT_DISTINCT);
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        DO_RESTORE(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS);
        break;
      }
      default: {
        break;
      }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(restore_extra_stores(agg_ctx, agg_row))) {
    LOG_WARN("restore extra stores failed", K(ret));
  }
  return ret;

#undef DO_RESTORE
}

int ReuseAggCellMgr::save_extra_stores(RuntimeContext &agg_ctx, const char *agg_row)
{
  int ret = OB_SUCCESS;
  if (agg_ctx.has_extra_) {
    extra_store_idx_ = *reinterpret_cast<const int32_t *>(agg_row + agg_ctx.row_meta().extra_idx_offset_);
    if (OB_UNLIKELY(extra_store_idx_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid extra store idx", K(ret), K(extra_store_idx_));
    }
  }
  return ret;
}

int ReuseAggCellMgr::restore_extra_stores(RuntimeContext &agg_ctx, char *agg_row)
{
  int ret = OB_SUCCESS;
  if (agg_ctx.has_extra_) {
    if (OB_UNLIKELY(extra_store_idx_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid extra store idx", K(ret), K(extra_store_idx_));
    } else {
      *reinterpret_cast<int32_t *>(agg_row + agg_ctx.row_meta().extra_idx_offset_) = extra_store_idx_;
      extra_store_idx_ = -1;
    }
  }
  return ret;
}
}
}
}