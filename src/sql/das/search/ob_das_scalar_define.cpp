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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_scalar_primary_ror_op.h"
#include "sql/das/search/ob_das_scalar_index_ror_op.h"
#include "sql/das/search/ob_das_sort_op.h"
#include "sql/das/search/ob_das_bitmap_op.h"

namespace oceanbase
{
namespace sql
{

ERRSIM_POINT_DEF(EN_FORCE_PRIMARY_ROR_SCAN, "Force to use primary ror scan");

OB_SERIALIZE_MEMBER((ObDASScalarCtDef, ObIDASSearchCtDef), has_main_scan_, has_index_scan_);

OB_SERIALIZE_MEMBER((ObDASScalarRtDef, ObIDASSearchRtDef));

OB_SERIALIZE_MEMBER((ObDASScalarScanCtDef, ObIDASSearchCtDef),
                    ref_table_id_,
                    access_column_ids_,
                    schema_version_,
                    table_param_,
                    pd_expr_spec_,
                    result_output_,
                    rowkey_exprs_,
                    table_scan_opt_,
                    pre_query_range_,
                    pre_range_graph_,
                    flags_);

OB_DEF_SERIALIZE(ObDASScalarScanRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_schema_version_,
              timeout_ts_,
              tx_lock_timeout_,
              sql_mode_,
              scan_flag_,
              key_ranges_,
              really_need_rowkey_order_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASScalarScanRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_schema_version_,
              timeout_ts_,
              tx_lock_timeout_,
              sql_mode_,
              scan_flag_,
              key_ranges_,
              really_need_rowkey_order_);
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASScalarScanRtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_schema_version_,
              timeout_ts_,
              tx_lock_timeout_,
              sql_mode_,
              scan_flag_,
              key_ranges_,
              really_need_rowkey_order_);
  return len;
}

const ObDASScalarScanCtDef* ObDASScalarCtDef::get_index_scan_ctdef() const
{
  const ObDASScalarScanCtDef* index_scan_ctdef = nullptr;
  if (has_index_scan_ && children_cnt_ > 0 && nullptr != children_[0]) {
    index_scan_ctdef = static_cast<const ObDASScalarScanCtDef *>(children_[0]);
    if (index_scan_ctdef->is_primary_table_scan_) {
      index_scan_ctdef = nullptr;
    }
  }
  return index_scan_ctdef;
}

const ObDASScalarScanCtDef* ObDASScalarCtDef::get_main_scan_ctdef() const
{
  const ObDASScalarScanCtDef* main_scan_ctdef = nullptr;
  if (has_main_scan_) {
    if (children_cnt_ == 1 && nullptr != children_[0]) {
      main_scan_ctdef = static_cast<const ObDASScalarScanCtDef *>(children_[0]);
    } else if (children_cnt_ == 2 && nullptr != children_[1]) {
      main_scan_ctdef = static_cast<const ObDASScalarScanCtDef *>(children_[1]);
    }

    if (nullptr != main_scan_ctdef && !main_scan_ctdef->is_primary_table_scan_) {
      main_scan_ctdef = nullptr;
    }
  }
  return main_scan_ctdef;
}

ObDASScalarScanRtDef* ObDASScalarRtDef::get_index_scan_rtdef() const
{
  ObDASScalarScanRtDef* index_scan_rtdef = nullptr;
  if (children_cnt_ > 0 && nullptr != children_[0]) {
    index_scan_rtdef = static_cast<ObDASScalarScanRtDef *>(children_[0]);
  }
  return index_scan_rtdef;
}

ObDASScalarScanRtDef* ObDASScalarRtDef::get_main_scan_rtdef() const
{
  ObDASScalarScanRtDef* main_scan_rtdef = nullptr;
  if (children_cnt_ == 1 && nullptr != children_[0]) {
    main_scan_rtdef = static_cast<ObDASScalarScanRtDef *>(children_[0]);
  } else if (children_cnt_ == 2 && nullptr != children_[1]) {
    main_scan_rtdef = static_cast<ObDASScalarScanRtDef *>(children_[1]);
  }
  return main_scan_rtdef;
}

int ObDASScalarRtDef::compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  const ObDASScalarCtDef* scalar_ctdef = get_ctdef();
  if (OB_ISNULL(scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_UNLIKELY(children_cnt_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children count is not expected", K(ret), K(children_cnt_));
  } else if (scalar_ctdef->has_index_scan()) {
    ObDASScalarScanRtDef* index_scan_rtdef = get_index_scan_rtdef();
    if (OB_ISNULL(index_scan_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan rtdef is null", K(ret));
    } else if (OB_FAIL(index_scan_rtdef->get_cost(search_ctx, cost))) {
      LOG_WARN("failed to get index scan cost", K(ret));
    }
  } else {
    // primary table can not estimate row count
    cost = search_ctx.get_row_count();
  }

  return ret;
}

int ObDASScalarRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASSearchCost cost;
  const ObDASScalarCtDef* scalar_ctdef = get_ctdef();
  if (OB_ISNULL(scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_FAIL(get_cost(search_ctx, cost))) {
    LOG_WARN("failed to compute cost", K(ret));
  } else {
    ObDASScalarScanRtDef *index_scan_rtdef = get_index_scan_rtdef();
    const ObDASScalarScanCtDef *index_scan_ctdef = scalar_ctdef->get_index_scan_ctdef();
    ObDASScalarScanRtDef *primary_scan_rtdef = get_main_scan_rtdef();
    const ObDASScalarScanCtDef *primary_scan_ctdef = scalar_ctdef->get_main_scan_ctdef();

    if (scalar_ctdef->has_index_scan() && scalar_ctdef->has_main_scan()) {
      // choose the scan with lower cost
      // If 8 times the lead_cost is less than the index scan cost, it is considered that scanning the primary table has a lower cost.
      // This is because the primary table can directly provide the primary key order without extra processing,
      // and is more advantageous when scanning fewer rows.
      LOG_TRACE("index or primary scan", K(lead_cost.cost()), K(cost.cost()), K(cost.cost() >> 3));
      if (/*lead_cost.cost() >= (cost.cost() >> 3) &&*/ !EN_FORCE_PRIMARY_ROR_SCAN) {
        // scan index table
        if (OB_ISNULL(index_scan_rtdef) || OB_ISNULL(index_scan_ctdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret));
        } else if (OB_FAIL(index_scan_rtdef->generate_op(lead_cost, search_ctx, op))) {
          LOG_WARN("failed to generate index scan op", K(ret));
        }
      } else {
        // scan primary table
        if (OB_ISNULL(primary_scan_rtdef) || OB_ISNULL(primary_scan_ctdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret));
        } else if (OB_FAIL(primary_scan_rtdef->generate_op(lead_cost, search_ctx, op))) {
          LOG_WARN("failed to generate primary scan op", K(ret));
        }
      }
    } else if (scalar_ctdef->has_index_scan()) {
      if (OB_ISNULL(index_scan_rtdef) || OB_ISNULL(index_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(index_scan_rtdef->generate_op(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate index scan op", K(ret));
      }
    } else if (scalar_ctdef->has_main_scan()) {
      if (OB_ISNULL(primary_scan_rtdef) || OB_ISNULL(primary_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(primary_scan_rtdef->generate_op(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate primary scan op", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid cost", K(ret));
    }
  }
  return ret;
}

int ObDASScalarScanRtDef::init_pd_op(ObExecContext &exec_ctx, const ObDASScalarScanCtDef &scalar_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p_row2exprs_projector_)) {
    p_row2exprs_projector_ = new(&row2exprs_projector_) ObRow2ExprsProjector(exec_ctx.get_allocator());
  }
  if (nullptr == p_pd_expr_op_) {
    if (OB_ISNULL(eval_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(eval_ctx_));
    } else if (FALSE_IT(p_pd_expr_op_ = new(&pd_expr_op_) ObPushdownOperator(*eval_ctx_,
                                                                             scalar_ctdef.pd_expr_spec_,
                                                                             scan_flag_.enable_rich_format_))) {
    } else if (OB_FAIL(pd_expr_op_.init_pushdown_storage_filter())) {
      LOG_WARN("init pushdown storage filter failed", K(ret));
    }
  }
  return ret;
}

int ObDASScalarScanRtDef::compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  const ObDASScalarScanCtDef* ctdef = get_ctdef();
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_FAIL(search_ctx.estimate_row_count(ctdef, this, row_count))) {
    LOG_WARN("failed to estimate row count", K(ret));
  } else {
    cost = ObDASSearchCost(row_count);
  }
  return ret;
}

int ObDASScalarScanRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  // is_rowkey_order_scan_
  // ├── YES: ROR (Primary/Index ROR)
  // └── NO:  need_rowkey_order_
  //          ├── NO:  Scan Op
  //          └── YES: Scan Op + Bitmap/Sort Op
  const ObDASScalarScanCtDef *ctdef = get_ctdef();
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (ctdef->is_rowkey_order_scan_) {
    // ROR scan
    if (ctdef->is_primary_table_scan_) {
      ObDASScalarPrimaryROROpParam op_param(ctdef, this);
      ObDASScalarPrimaryROROp *primary_ror_op = nullptr;
      if (OB_FAIL(search_ctx.create_op(op_param, primary_ror_op))) {
        LOG_WARN("failed to create primary ror op", K(ret));
      } else if (OB_ISNULL(primary_ror_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else {
        op = static_cast<ObIDASSearchOp *>(primary_ror_op);
      }
    } else {
      ObDASScalarIndexROROpParam op_param(ctdef, this);
      ObDASScalarIndexROROp *index_ror_op = nullptr;
      if (OB_FAIL(search_ctx.create_op(op_param, index_ror_op))) {
        LOG_WARN("failed to create index ror op", K(ret));
      } else if (OB_ISNULL(index_ror_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else {
        op = static_cast<ObIDASSearchOp *>(index_ror_op);
      }
    }
  } else {
    // non-ROR scan
    ObDASScalarScanOpParam op_param(ctdef, this);
    ObDASScalarScanOp *non_ror_scan_op = nullptr;
    if (OB_FAIL(search_ctx.create_op(op_param, non_ror_scan_op))) {
      LOG_WARN("failed to create scan op", K(ret));
    } else if (OB_ISNULL(non_ror_scan_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else if (!really_need_rowkey_order_ && !ctdef->is_search_index_) {
      op = static_cast<ObIDASSearchOp *>(non_ror_scan_op);
    } else {
      if (search_ctx.get_rowid_type() == DAS_ROWID_TYPE_UINT64) {
        ObDASBitmapOpParam bitmap_op_param(non_ror_scan_op);
        ObDASBitmapOp *bitmap_op = nullptr;
        if (OB_FAIL(search_ctx.create_op(bitmap_op_param, bitmap_op))) {
          LOG_WARN("failed to create bitmap op", K(ret));
        } else if (OB_ISNULL(bitmap_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret));
        } else {
          op = static_cast<ObIDASSearchOp *>(bitmap_op);
        }
      } else {
        ObDASSortOpParam sort_op_param(non_ror_scan_op);
        ObDASSortOp *sort_op = nullptr;
        if (OB_FAIL(search_ctx.create_op(sort_op_param, sort_op))) {
          LOG_WARN("failed to create sort op", K(ret));
        } else if (OB_ISNULL(sort_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret));
        } else {
          op = static_cast<ObIDASSearchOp *>(sort_op);
        }
      }
    }
  }
  return ret;
}


} // namespace sql
} // namespace oceanbase