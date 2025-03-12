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
#include "sql/das/iter/ob_das_functional_lookup_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_func_data_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_ir_define.h"
#include "storage/concurrency_control/ob_data_validation_service.h"
#include "sql/das/iter/ob_das_text_retrieval_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASFuncLookupIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (param.type_ != ObDASIterType::DAS_ITER_FUNC_LOOKUP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    ObDASFuncLookupIterParam &lookup_param = static_cast<ObDASFuncLookupIterParam&>(param);
    state_ = LookupState::INDEX_SCAN;
    index_end_ = false;
    default_batch_row_count_ = lookup_param.default_batch_row_count_;
    lookup_rowkey_cnt_ = 0;
    lookup_row_cnt_ = 0;
    index_table_iter_ = lookup_param.index_table_iter_;
    data_table_iter_ = lookup_param.data_table_iter_;
    index_ctdef_ = lookup_param.index_ctdef_;
    index_rtdef_ = lookup_param.index_rtdef_;
    lookup_ctdef_ = lookup_param.lookup_ctdef_;
    lookup_rtdef_ = lookup_param.lookup_rtdef_;
    start_table_scan_ = false;
    trans_desc_ = lookup_param.trans_desc_;
    snapshot_ = lookup_param.snapshot_;
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), ObModIds::OB_SQL_TABLE_LOOKUP, ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(lookup_memctx_, param))) {
      LOG_WARN("failed to create lookup memctx", K(ret));
    } else if (OB_FAIL(rowkey_exprs_.push_back(lookup_param.doc_id_expr_))) {
      LOG_WARN("failed to assign rowkey exprs", K(ret));
    } else if (rowkey_exprs_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey exprs count", K(rowkey_exprs_.count()), K(ret));
    }
  }
  return ret;
}

void ObDASFuncLookupIter::reset_lookup_state()
{
  lookup_row_cnt_ = 0;
  lookup_rowkey_cnt_ = 0;
  index_end_ = false;
  state_ = LookupState::INDEX_SCAN;
  if (!is_first_lookup_) {
    data_table_iter_->reuse();
  }
  if (OB_NOT_NULL(lookup_memctx_)) {
    lookup_memctx_->reset_remain_one_page();
  }
  trans_info_array_.reuse();
}

int ObDASFuncLookupIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  ObDASScanIter *index_table_iter = static_cast<ObDASScanIter *>(index_table_iter_);
  storage::ObTableScanParam &index_scan_param = index_table_iter->get_scan_param();
  if (!index_scan_param.key_ranges_.empty()) {
    index_scan_param.key_ranges_.reuse();
  }
  if (start_table_scan_) {
    if (OB_FAIL(index_table_iter_->reuse())) {
      LOG_WARN("failed to reuse index table iter", K(ret));
    } else if (is_first_lookup_ &&OB_FAIL(data_table_iter_->reuse())) {
      LOG_WARN("failed to reuse data table iter", K(ret));
    } else if (OB_FAIL(ObDASLookupIter::inner_reuse())) {
      LOG_WARN("failed to reuse das lookup iter", K(ret));
    } else {
      trans_info_array_.reuse();
    }
  }
  return ret;
}

int ObDASFuncLookupIter::inner_release()
{
  int ret = OB_SUCCESS;
  start_table_scan_ = false;
  if (OB_FAIL(ObDASLocalLookupIter::inner_release())) {
    LOG_WARN("failed to release lookup iter", K(ret));
  }
  return ret;
}

int ObDASFuncLookupIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  start_table_scan_ = true;
  OB_ASSERT(index_table_iter_->get_type() == DAS_ITER_SCAN);
  ObDASScanIter *index_table_iter = static_cast<ObDASScanIter *>(index_table_iter_);
  storage::ObTableScanParam &index_scan_param = index_table_iter->get_scan_param();
  const ObDASScanCtDef *index_ctdef = static_cast<const ObDASScanCtDef *>(index_ctdef_);
  ObDASScanRtDef *index_rtdef = static_cast<ObDASScanRtDef *>(index_rtdef_);
  if (OB_UNLIKELY(index_scan_param.key_ranges_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key ranges count", K(index_scan_param.key_ranges_.count()), K(ret));
  } else if (OB_FAIL(index_table_iter_->do_table_scan())) {
    if (OB_SNAPSHOT_DISCARDED == ret && index_scan_param.fb_snapshot_.is_valid()) {
      ret = OB_INVALID_QUERY_TIMESTAMP;
    } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to do partition scan", K(index_scan_param), K(ret));
    }
  }
  return ret;
}

int ObDASFuncLookupIter::rescan()
{
  int ret = OB_SUCCESS;
  // only rescan index table, data table will be rescan in do_lookup.
  ObDASScanIter *index_table_iter = static_cast<ObDASScanIter *>(index_table_iter_);
  storage::ObTableScanParam &index_scan_param = index_table_iter->get_scan_param();
  if (OB_UNLIKELY(!start_table_scan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rescan, should do table scan first", K(ret));
  } else if (OB_UNLIKELY(index_scan_param.key_ranges_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key ranges count", K(index_scan_param.key_ranges_.count()), K(ret));
  } else if (OB_FAIL(index_table_iter_->rescan())) {
    LOG_WARN("failed to rescan index table iter", K(ret));
  }
  return ret;
}

int ObDASFuncLookupIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObDASScanIter *index_table_iter = static_cast<ObDASScanIter *>(index_table_iter_);
  storage::ObTableScanParam &index_scan_param = index_table_iter->get_scan_param();
  OB_ASSERT(index_table_iter_->get_type() == DAS_ITER_SCAN);
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  const bool use_simulate_batch_row_cnt = simulate_batch_row_cnt > 0 && simulate_batch_row_cnt < default_batch_row_count_;
  int64_t default_row_batch_cnt  = use_simulate_batch_row_cnt ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  if (index_scan_param.key_ranges_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(index_scan_param.key_ranges_.count() != 1 || default_row_batch_cnt != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key ranges count", K(index_scan_param.key_ranges_.count()), K(default_row_batch_cnt), K(ret));
  } else if (OB_FAIL(ObDASLocalLookupIter::inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row from function lookup iter", K(ret));
    }
  } else if (OB_UNLIKELY(lookup_row_cnt_ > 1 || lookup_rowkey_cnt_ > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lookup row count", K_(lookup_row_cnt), K_(lookup_rowkey_cnt), K(ret));
  }
  return ret;
}

int ObDASFuncLookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObDASScanIter *index_table_iter = static_cast<ObDASScanIter *>(index_table_iter_);
  storage::ObTableScanParam &index_scan_param = index_table_iter->get_scan_param();
  OB_ASSERT(index_table_iter_->get_type() == DAS_ITER_SCAN);
  cap_ = index_scan_param.key_ranges_.count();
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  const bool use_simulate_batch_row_cnt = simulate_batch_row_cnt > 0 && simulate_batch_row_cnt < default_batch_row_count_;
  int64_t default_row_batch_cnt  = use_simulate_batch_row_cnt ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  if (index_scan_param.key_ranges_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(default_row_batch_cnt < cap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key ranges count", K(default_row_batch_cnt), K(capacity),
      K_(cap), K(index_scan_param.key_ranges_.count()), K(ret));
  } else if (OB_FAIL(ObDASLookupIter::inner_get_next_rows(count, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row from function lookup iter", K(ret));
    }
  }
  return ret;
}

int ObDASFuncLookupIter::add_rowkey()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_FUNC_DATA);
  if (OB_ISNULL(eval_ctx_) || OB_UNLIKELY(1 != rowkey_exprs_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid eval ctx or rowkey exprs", K_(eval_ctx), K_(rowkey_exprs), K(ret));
  } else {
    ObDASScanIter *index_iter = static_cast<ObDASScanIter *>(index_table_iter_);
    ObDASFuncDataIter *merge_iter = static_cast<ObDASFuncDataIter *>(data_table_iter_);
    ObDocId doc_id;
    const ObExpr *expr = rowkey_exprs_.at(0);
    ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx_);
    doc_id.from_string(col_datum.get_string());
    if (OB_UNLIKELY(!doc_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid doc id", K(doc_id));
    } else if (OB_FAIL(merge_iter->add_doc_id(doc_id))) {
      LOG_WARN("failed to add doc id", K(ret));
    }
    LOG_DEBUG("push doc id to tr iter", K(doc_id), K(ret));
  }
  return ret;
}

int ObDASFuncLookupIter::add_rowkeys(int64_t storage_count)
{
  int ret = OB_SUCCESS;
  // for limit case, can do better, add_rowkeys(limit_count)
  if (OB_UNLIKELY(storage_count != cap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(storage_count), K(cap_));
  } else if (OB_FAIL(ObDASLocalLookupIter::add_rowkeys(storage_count))) {
    LOG_WARN("failed to add rowkeys", K(ret));
  }
  return ret;
}

int ObDASFuncLookupIter::do_index_lookup()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_FUNC_DATA);
  ObDASScanIter *index_table_iter = static_cast<ObDASScanIter *>(index_table_iter_);
  storage::ObTableScanParam &index_scan_param = index_table_iter->get_scan_param();
  ObDASFuncDataIter *merge_iter = static_cast<ObDASFuncDataIter *>(data_table_iter_);
  if (merge_iter->has_main_lookup_iter()) {
    storage::ObTableScanParam &main_lookup_param = merge_iter->get_main_lookup_scan_param();
    int64 group_id = 0;
    if (OB_UNLIKELY(!main_lookup_param.key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected key ranges count", K(main_lookup_param.key_ranges_.count()), K(ret));
    } else if (DAS_OP_TABLE_SCAN != index_ctdef_->op_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index op type", K(index_ctdef_->op_type_), K(ret));
    } else {
      const ObDASScanCtDef *index_ctdef = static_cast<const ObDASScanCtDef*>(index_ctdef_);
      if (nullptr != index_ctdef->group_id_expr_) {
        group_id = index_ctdef->group_id_expr_->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_scan_param.key_ranges_.count(); i++) {
      ObRowkey row_key = index_scan_param.key_ranges_.at(i).start_key_;
      ObNewRange range;
      range.build_range(merge_iter->get_main_lookup_ctdef()->ref_table_id_, row_key);
      int64_t group_idx = ObNewRange::get_group_idx(group_id);
      range.group_idx_ = group_idx;
      main_lookup_param.key_ranges_.push_back(range);
    }
    if (OB_SUCC(ret)) {
      main_lookup_param.is_get_ = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_first_lookup_) {
    is_first_lookup_ = false;
    if (OB_FAIL(data_table_iter_->do_table_scan())) {
      if (OB_SNAPSHOT_DISCARDED == ret && lookup_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to do partition scan", K(lookup_param_), K(ret));
      }
    }
  } else if (OB_FAIL(data_table_iter_->rescan())) {
    LOG_WARN("failed to rescan data table", K(ret));
  }
  return ret;
}

int ObDASFuncLookupIter::check_index_lookup()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_FUNC_DATA);
  if (GCONF.enable_defensive_check()) {
    if (OB_UNLIKELY(lookup_rowkey_cnt_ != lookup_row_cnt_)) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      ObString func_name = ObString::make_string("check_lookup_row_cnt");
      LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
      LOG_ERROR("Fatal Error!!! Catch a defensive error!",
          K(ret), K_(lookup_rowkey_cnt), K_(lookup_row_cnt));
    }
  }
  return ret;
}

void ObDASFuncLookupIter::clear_evaluated_flag()
{
  index_table_iter_->clear_evaluated_flag();
  data_table_iter_->clear_evaluated_flag();
}

}  // namespace sql
}  // namespace oceanbase
