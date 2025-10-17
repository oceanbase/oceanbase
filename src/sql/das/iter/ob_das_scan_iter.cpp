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
#include "sql/das/iter/ob_das_scan_iter.h"
#include "storage/tx_storage/ob_access_service.h"
#include "src/sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (param.type_ != ObDASIterType::DAS_ITER_SCAN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    const ObDASScanCtDef *scan_ctdef = (static_cast<ObDASScanIterParam&>(param)).scan_ctdef_;
    output_ = &scan_ctdef->result_output_;
    tsc_service_ = is_virtual_table(scan_ctdef->ref_table_id_) ? GCTX.vt_par_ser_
                              : scan_ctdef->is_external_table_ ? GCTX.et_access_service_
                                                               : MTL(ObAccessService *);
  }

  return ret;
}

int ObDASScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  // NOTE: need_switch_param_ should have been set before call reuse().
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else if (OB_FAIL(tsc_service_->reuse_scan_iter(scan_param_->need_switch_param_, result_))) {
    LOG_WARN("failed to reuse storage scan iter", K(ret));
  } else {
    scan_param_->key_ranges_.reuse();
    scan_param_->ss_key_ranges_.reuse();
    scan_param_->mbr_filters_.reuse();
  }
  return ret;
}

int ObDASScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_)) {
    if (OB_FAIL(tsc_service_->revert_scan_iter(result_))) {
      LOG_WARN("failed to revert storage scan iter", K(ret));
    }
    result_ = nullptr;
  }
  return ret;
}

int ObDASScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else if (OB_UNLIKELY(nullptr != result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not null result iter ptr before do table scan", K(ret), KP_(result));
  } else if (OB_FAIL(tsc_service_->table_scan(*scan_param_, result_))) {
    if (OB_SNAPSHOT_DISCARDED == ret && scan_param_->fb_snapshot_.is_valid()) {
      ret = OB_INVALID_QUERY_TIMESTAMP;
    } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", KPC_(scan_param), K(ret));
    }
  }
  LOG_DEBUG("[DAS ITER] scan iter do table scan", KPC_(scan_param), K(ret));

  return ret;
}

int ObDASScanIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else if (OB_FAIL(tsc_service_->table_rescan(*scan_param_, result_))) {
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param_->fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      }
    LOG_WARN("failed to rescan tablet", K(scan_param_->tablet_id_), K(ret));
  } else {
    // reset need_switch_param_ after real rescan.
    scan_param_->need_switch_param_ = false;
  }
  LOG_DEBUG("[DAS ITER] das scan iter rescan", KPC_(scan_param), K(ret));

  return ret;
}

int ObDASScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(scan_param_ != nullptr? scan_param_->index_id_ : 0);

  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (OB_FAIL(result_->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObDASScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(scan_param_ != nullptr? scan_param_->index_id_ : 0);

  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (OB_FAIL(result_->get_next_rows(count, capacity))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  LOG_TRACE("[DAS ITER] scan iter get next rows", K(count), K(capacity), KPC_(scan_param), K(ret));
  const ObBitVector *skip = nullptr;
  PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_, count, skip);
  return ret;
}

void ObDASScanIter::clear_evaluated_flag()
{
  OB_ASSERT(nullptr != scan_param_);
  if (OB_NOT_NULL(scan_param_->op_)) {
    scan_param_->op_->clear_evaluated_flag();
  }
}

int ObDASScanIter::set_scan_rowkey(ObEvalCtx *eval_ctx,
                                   const ObIArray<ObExpr *> &rowkey_exprs,
                                   const ObDASScanCtDef *lookup_ctdef,
                                   ObIAllocator *alloc,
                                   int64_t group_id)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  if (OB_ISNULL(eval_ctx) || OB_UNLIKELY(rowkey_exprs.empty()) || OB_ISNULL(lookup_ctdef) || OB_ISNULL(alloc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid eval ctx, rowkey exprs, lookup ctdef, or allocator",
             K(eval_ctx), K(rowkey_exprs), K(lookup_ctdef), K(alloc), K(ret));
  } else {
    ObObj *obj_ptr = nullptr;
    void *buf = nullptr;
    int64_t rowkey_cnt = rowkey_exprs.count();
    if (OB_ISNULL(buf = alloc->alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(rowkey_cnt), K(ret));
    } else {
      obj_ptr = new (buf) ObObj(rowkey_cnt);
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
      ObObj tmp_obj;
      const ObExpr *expr = rowkey_exprs.at(i);
      ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx);
      if (OB_UNLIKELY(T_PSEUDO_GROUP_ID == expr->type_ || T_PSEUDO_ROW_TRANS_INFO_COLUMN == expr->type_)) {
        // skip.
      } else if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("failed to convert datum to obj", K(ret));
      } else if (OB_FAIL(ob_write_obj(*alloc, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("failed to deep copy rowkey", K(ret), K(tmp_obj));
      }
    }

    if (OB_SUCC(ret)) {
      ObRowkey row_key(obj_ptr, rowkey_cnt);
      if (OB_FAIL(range.build_range(lookup_ctdef->ref_table_id_, row_key))) {
        LOG_WARN("failed to build lookup range", K(ret), K(lookup_ctdef->ref_table_id_), K(row_key));
      } else if (FALSE_IT(range.group_idx_ = ObNewRange::get_group_idx(group_id))) {
      } else if (OB_FAIL(scan_param_->key_ranges_.push_back(range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      } else {
        scan_param_->is_get_ = true;
      }
    }
  }
  LOG_DEBUG("set scan iter scan rowkey", K(range), K(ret));

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
