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
#include "sql/das/ob_das_batch_scan_op.h"
#include "sql/engine/ob_bit_vector.h"
#include "storage/access/ob_table_scan_iterator.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
namespace sql
{
ObDASBatchScanOp::ObDASBatchScanOp(ObIAllocator &op_alloc)
  : ObDASScanOp(op_alloc),
    batch_result_(nullptr),
    group_flags_(nullptr),
    flag_memory_size_(0),
    next_group_idx_(0),
    group_cnt_(0),
    batch_iter_creator_(op_alloc),
    result_outputs_(nullptr)
{
}

int ObDASBatchScanOp::init_group_flags(int64_t max_group_cnt)
{
  int ret = OB_SUCCESS;
  int64_t memory_size = ObBitVector::memory_size(max_group_cnt);
  if (memory_size > flag_memory_size_) {
    void *buf = op_alloc_.alloc(memory_size);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate flag memory size failed", K(ret), K(memory_size));
    } else {
      group_flags_ = to_bit_vector(buf);
      flag_memory_size_ = memory_size;
    }
  }
  if (OB_SUCC(ret)) {
    group_flags_->init(max_group_cnt);
  }
  return ret;
}

int ObDASBatchScanOp::open_op()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  reset_access_datums_ptr();
  if (OB_FAIL(init_scan_param())) {
    LOG_WARN("init scan param failed", K(ret));
  } else if (OB_FAIL(tsc_service.table_scan(scan_param_, batch_result_))) {
    if (OB_SNAPSHOT_DISCARDED == ret && scan_param_.fb_snapshot_.is_valid()) {
      ret = OB_INVALID_QUERY_TIMESTAMP;
    } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(scan_param_), K(ret));
    }
  } else if (OB_FAIL(switch_batch_iter())) {
    LOG_WARN("get result iterator form batch result iter failed", K(ret));
  } else {
    LOG_DEBUG("batch table scan begin", K_(scan_param), KPC_(result));
  }
  return ret;
}

int ObDASBatchScanOp::release_op()
{
  int ret = OB_NOT_SUPPORTED;
  scan_param_.partition_guard_ = nullptr;
  scan_param_.destroy_schema_guard();
  batch_result_ = nullptr;
  result_ = nullptr;
  batch_iter_creator_.destory();

  return ret;
}

int ObDASBatchScanOp::add_group_flag(int64_t group_idx)
{
  int ret = OB_SUCCESS;
  ObPosArray &pos_array = scan_param_.range_array_pos_;
  int64_t range_pos_cnt = pos_array.count();
  int64_t last_range_cnt = range_pos_cnt > 0 ? pos_array.at(range_pos_cnt - 1) + 1 : 0;
  int64_t range_cnt = scan_param_.key_ranges_.count();
  bool empty_range = (range_cnt == last_range_cnt);
  if (!empty_range) {
    if (OB_FAIL(scan_param_.range_array_pos_.push_back(range_cnt - 1))) {
      LOG_WARN("store key range array pos failed", K(ret), K(scan_param_.key_ranges_));
    } else {
      group_flags_->set(group_idx);
    }
  }
  if (OB_SUCC(ret)) {
    group_cnt_ = group_idx + 1;
  }
  return ret;
}

int ObDASBatchScanOp::rescan()
{
  int &ret = errcode_;
  //The first batched das rescan need to rescan ObTableScanIterIterator
  //and then only need to switch iterator
  ObITabletScan &tsc_service = get_tsc_service();
  reset_access_datums_ptr();
  if (OB_FAIL(tsc_service.table_rescan(scan_param_, batch_result_))) {
    LOG_WARN("rescan the table iterator failed", K(ret));
  } else if (OB_FAIL(switch_batch_iter())) {
    LOG_WARN("get next iter from batch result failed", K(ret));
  }
  return ret;
}

int ObDASBatchScanOp::reuse_iter()
{
  int &ret = errcode_;
  ObITabletScan &tsc_service = get_tsc_service();
  if (OB_FAIL(tsc_service.reuse_scan_iter(scan_param_.need_switch_param_, batch_result_))) {
    LOG_WARN("reuse scan iterator failed", K(ret));
  } else {
    scan_param_.key_ranges_.reuse();
    scan_param_.range_array_pos_.reuse();
    result_ = nullptr;
    next_group_idx_ = 0;
    group_cnt_ = 0;
  }
  return ret;
}

int ObDASBatchScanOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
  DASFoldIterator *fold_iter = nullptr;
  int64_t group_cnt = scan_param_.range_array_pos_.count();
  if (OB_FAIL(batch_iter_creator_.get_fold_iter(fold_iter))) {
    LOG_WARN("allocate fold iterator failed", K(ret));
  } else if (OB_FAIL(fold_iter->init_iter(*scan_ctdef_, *scan_rtdef_, group_cnt))) {
    LOG_WARN("init iterator failed", K(ret));
  } else {
    result_outputs_ = &fold_iter->get_output_exprs();
    if (OB_FAIL(ObDASScanOp::decode_task_result(task_result))) {
      LOG_WARN("decode das scan task result failed", K(ret));
    } else {
      fold_iter->set_result_iter(result_);
      batch_result_ = fold_iter;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(switch_batch_iter())) {
      LOG_WARN("switch batch iter failed", K(ret));
    }
  }
  return ret;
}

int ObDASBatchScanOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  DASExpandIterator *expand_iter = nullptr;
  if (OB_FAIL(batch_iter_creator_.get_expand_iter(expand_iter))) {
    LOG_WARN("allocate expand iterator failed", K(ret));
  } else if (OB_FAIL(expand_iter->init_iter(*scan_ctdef_, *scan_rtdef_, batch_result_, result_))) {
    LOG_WARN("init iterator failed", K(ret));
  } else {
    result_ = expand_iter;
    result_outputs_ = &(expand_iter->get_output_exprs());
    if (OB_FAIL(ObDASScanOp::fill_task_result(task_result, has_more, memory_limit))) {
      LOG_WARN("fill task result failed", K(ret));
    }
  }
  return ret;
}

int ObDASBatchScanOp::switch_batch_iter()
{
  int ret = OB_SUCCESS;
  //When group_cnt<=0 means that das batch scan does not rely on group flags to determine
  //whether the group range is empty.
  //At this time, the parameter boundary of das batch scan is consistent with
  //the boundary of range_array_pos,
  //such as remote execution of das batch scan
  if (OB_UNLIKELY(group_cnt_ <= 0)) {
    ret = batch_result_->get_next_iter(result_);
  } else if (OB_UNLIKELY(next_group_idx_ >= group_cnt_)) {
    ret = OB_ITER_END;
    LOG_DEBUG("group iterator reach end", K(ret), K(next_group_idx_), K(group_cnt_));
  } else if (OB_LIKELY(group_flags_->at(next_group_idx_))) {
    ret = batch_result_->get_next_iter(result_);
  } else if (OB_FAIL(batch_iter_creator_.get_empty_iter(result_))) {
    LOG_WARN("create empty iter failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    ++next_group_idx_;
  }
  return ret;
}

ObArrayWrap<uint8_t> ObDASBatchScanOp::to_bit_array() const
{
  int64_t count = flag_memory_size_ / sizeof(uint8_t);
  uint8_t *data = reinterpret_cast<uint8_t*>(group_flags_);
  return ObArrayWrap<uint8_t>(data, count);
}

OB_SERIALIZE_MEMBER((ObDASBatchScanOp, ObDASScanOp),
                    scan_param_.range_array_pos_);

int ObDASBatchScanOp::DASExpandIterator::init_iter(const ObDASScanCtDef &scan_ctdef,
                                                   ObDASScanRtDef &scan_rtdef,
                                                   ObNewIterIterator *batch_iter,
                                                   ObNewRowIterator *iter)
{
  int ret = OB_SUCCESS;
  //append group id pseudo column to output exprs
  int64_t output_cnt = scan_ctdef.pd_expr_spec_.access_exprs_.count() + 1;
  output_exprs_.set_allocator(&scan_rtdef.stmt_allocator_);
  output_exprs_.set_capacity(output_cnt);
  if (OB_FAIL(append(output_exprs_, scan_ctdef.pd_expr_spec_.access_exprs_))) {
    LOG_WARN("append storage output exprs to expand iterator failed", K(ret));
  } else if (OB_FAIL(output_exprs_.push_back(scan_ctdef.group_id_expr_))) {
    LOG_WARN("store group id expr to output exprs failed", K(ret));
  } else {
    eval_ctx_ = &scan_rtdef.p_pd_expr_op_->get_eval_ctx();
    batch_iter_ = batch_iter;
    iter_ = iter;
  }
  return ret;
}

int ObDASBatchScanOp::DASExpandIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  while (OB_SUCC(ret) && !got_row) {
    if (OB_FAIL(iter_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from result failed", K(ret));
      } else if (OB_FAIL(batch_iter_->get_next_iter(iter_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next iterator from batch iter failed", K(ret));
        } else {
          LOG_DEBUG("reach the batch iterator end", K(ret));
        }
      } else {
        ++group_idx_;
      }
    } else {
      got_row = true;
      ObExpr *expr = output_exprs_.at(output_exprs_.count() - 1);
      if (OB_UNLIKELY(expr->type_ != T_PSEUDO_GROUP_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr type is invalid", K(ret), K(expr->type_));
      } else {
        // store group index at expand iterator's the last column
        expr->locate_datum_for_write(*eval_ctx_).set_int(group_idx_);
        expr->get_eval_info(*eval_ctx_).evaluated_ = true;
        LOG_DEBUG("store group idx to output exprs", K(ret), K(group_idx_));
      }
    }
  }
  return ret;
}

int ObDASBatchScanOp::DASExpandIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  while (OB_SUCC(ret) && !got_row) {
    if (OB_FAIL(iter_->get_next_rows(count, capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next rows from result failed", K(ret), K(count), K(capacity));
      } else if (OB_FAIL(batch_iter_->get_next_iter(iter_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next iterator from batch iter failed", K(ret));
        } else {
          LOG_DEBUG("reach the batch iterator end", K(ret));
        }
      } else {
        ++group_idx_;
      }
    } else {
      got_row = true;
      ObExpr *expr = output_exprs_.at(output_exprs_.count() - 1);
      if (OB_UNLIKELY(expr->type_ != T_PSEUDO_GROUP_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr type is invalid", K(ret), K(expr->type_));
      } else {
        // store group index at expand iterator's the last column
        ObDatumVector group_id_datums = expr->locate_expr_datumvector(*eval_ctx_);
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          group_id_datums.at(i)->set_int(group_idx_);
        }
        expr->get_eval_info(*eval_ctx_).cnt_ = count;
        expr->get_eval_info(*eval_ctx_).evaluated_ = true;
        LOG_DEBUG("store group idx to output exprs", K(ret), K(group_idx_), K(count));
      }
    }
  }
  return ret;
}

int ObDASBatchScanOp::DASFoldIterator::init_iter(const ObDASScanCtDef &scan_ctdef,
                                                 ObDASScanRtDef &scan_rtdef,
                                                 int64_t group_cnt)
{
  int ret = OB_SUCCESS;
  //append group id pseudo column to output exprs
  int64_t output_cnt = scan_ctdef.pd_expr_spec_.access_exprs_.count() + 1;
  output_exprs_.set_allocator(&scan_rtdef.stmt_allocator_);
  output_exprs_.set_capacity(output_cnt);
  group_cnt_ = group_cnt;
  if (OB_FAIL(append(output_exprs_, scan_ctdef.pd_expr_spec_.access_exprs_))) {
    LOG_WARN("append storage output exprs to expand iterator failed", K(ret));
  } else if (OB_FAIL(output_exprs_.push_back(scan_ctdef.group_id_expr_))) {
    LOG_WARN("store group id expr to output exprs failed", K(ret));
  } else {
    eval_ctx_ = &scan_rtdef.p_pd_expr_op_->get_eval_ctx();
    new(&last_row_) LastDASStoreRow(scan_rtdef.scan_allocator_);
    last_row_.reuse_ = true;
  }
  return ret;
}

int ObDASBatchScanOp::DASFoldIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  int64_t last_group_id = OB_NOT_NULL(last_row_.store_row_) ?
      last_row_.store_row_->cells()[output_exprs_.count() - 1].get_int() : OB_INVALID_INDEX;
  if (OB_UNLIKELY(OB_INVALID_INDEX == last_group_id)) {
    const ObExpr *expr = output_exprs_.at(output_exprs_.count() - 1);
    ObDatum &current_group_id = expr->locate_expr_datum(*eval_ctx_);
    if (OB_FAIL(iter_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from iter failed", K(ret));
      }
    } else if (OB_LIKELY(current_group_id.get_int() > group_id_)) {
      //no row belong to current group iterator, save current row and return OB_ITER_END
      if (OB_FAIL(last_row_.save_store_row(output_exprs_, *eval_ctx_))) {
        LOG_WARN("save store row failed", K(ret));
      } else {
        ret = OB_ITER_END;
        LOG_DEBUG("current iterator reach end", K(ret), K_(group_id), K(current_group_id));
      }
    } else if (expr->locate_expr_datum(*eval_ctx_).get_int() < group_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last iterator has not reach iter end", K(ret),
               K(group_id_), "current_group_id", expr->locate_expr_datum(*eval_ctx_));
    } else {
      LOG_DEBUG("current row match current group id", K_(group_id), K(current_group_id),
                "current_row", ROWEXPR2STR(*eval_ctx_, output_exprs_));
    }
  } else if (OB_UNLIKELY(group_id_ < last_group_id)) {
    ret = OB_ITER_END;
    LOG_DEBUG("current iterator reach end", K(ret), K(last_group_id), K(group_id_));
  } else if (OB_UNLIKELY(group_id_ > last_group_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last iterator has not reach iter end", K(ret), K(group_id_), K(last_group_id));
  } else if (OB_FAIL(last_row_.store_row_->to_expr(output_exprs_, *eval_ctx_))) {
    LOG_WARN("to expr skip const failed", K(ret));
  } else {
    last_row_.store_row_->cells()[output_exprs_.count() - 1].set_int(OB_INVALID_INDEX);
    LOG_DEBUG("last row match current group id", K_(group_id),
              "current_row", ROWEXPR2STR(*eval_ctx_, output_exprs_));
  }
  return ret;
}

int ObDASBatchScanOp::DASFoldIterator::get_next_iter(ObNewRowIterator *&iter)
{
  int ret = OB_SUCCESS;
  if (group_id_ >= group_cnt_) {
    ret = OB_ITER_END;
    LOG_DEBUG("get next iter reach iter end", K(ret), K(group_id_), K(group_cnt_));
  } else {
    ++group_id_;
    iter = this;
  }
  return ret;
}

int ObDASBatchScanOp::DASBatchIterCreator::get_empty_iter(ObNewRowIterator *&empty_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(empty_iter_)) {
    ret = alloc_iter(empty_iter_);
  }
  if (OB_SUCC(ret)) {
    empty_iter = empty_iter_;
  }
  return ret;
}

int ObDASBatchScanOp::DASBatchIterCreator::get_expand_iter(DASExpandIterator *&expand_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expand_iter_)) {
    ret = alloc_iter(expand_iter_);
  }
  if (OB_SUCC(ret)) {
    expand_iter = expand_iter_;
  }
  return ret;
}

int ObDASBatchScanOp::DASBatchIterCreator::get_fold_iter(DASFoldIterator *&fold_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fold_iter_)) {
    ret = alloc_iter(fold_iter_);
  }
  if (OB_SUCC(ret)) {
    fold_iter = fold_iter_;
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
