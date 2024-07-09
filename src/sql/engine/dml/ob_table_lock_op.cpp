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
#include "ob_table_lock_op.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
namespace sql
{
OB_SERIALIZE_MEMBER((ObTableLockOpInput, ObTableModifyOpInput));

OB_DEF_SERIALIZE(ObTableLockSpec)
{
  int ret = OB_SUCCESS;
  int64_t tbl_cnt = lock_ctdefs_.count();
  BASE_SER((ObTableLockSpec, ObTableModifySpec));
  OB_UNIS_ENCODE(for_update_wait_us_);
  OB_UNIS_ENCODE(skip_locked_);
  OB_UNIS_ENCODE(tbl_cnt);
  int64_t index_cnt = 1;
  for (int64_t i = 0; OB_SUCC(ret) && i < tbl_cnt; ++i) {
    OB_UNIS_ENCODE(index_cnt);
    ObLockCtDef *lock_ctdef = lock_ctdefs_.at(i).at(0);
    if (OB_ISNULL(lock_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lock_ctdef is nullptr", K(ret));
    }
    OB_UNIS_ENCODE(*lock_ctdef);
  }

  OB_UNIS_ENCODE(is_multi_table_skip_locked_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableLockSpec)
{
  int ret = OB_SUCCESS;
  int64_t tbl_cnt = 0;
  BASE_DESER((ObTableLockSpec, ObTableModifySpec));
  OB_UNIS_DECODE(for_update_wait_us_);
  OB_UNIS_DECODE(skip_locked_);
  OB_UNIS_DECODE(tbl_cnt);
  if (OB_SUCC(ret) && tbl_cnt > 0) {
    OZ(lock_ctdefs_.allocate_array(alloc_, tbl_cnt));
  }
  ObDMLCtDefAllocator<ObLockCtDef> lock_ctdef_allocator(alloc_);
  for (int64_t i = 0; OB_SUCC(ret) && i < tbl_cnt; ++i) {
    int64_t index_cnt = 0;
    OB_UNIS_DECODE(index_cnt);
    CK(1 == index_cnt);
    OZ(lock_ctdefs_.at(i).allocate_array(alloc_, index_cnt));
    for (int64_t j = 0; OB_SUCC(ret) && j < index_cnt; ++j) {
      ObLockCtDef *lock_ctdef = lock_ctdef_allocator.alloc();
      if (OB_ISNULL(lock_ctdef)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc lock_ctdef failed", K(ret));
      }
      OB_UNIS_DECODE(*lock_ctdef);
      lock_ctdefs_.at(i).at(j) = lock_ctdef;
    }
  }

  OB_UNIS_DECODE(is_multi_table_skip_locked_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLockSpec)
{
  int64_t len = 0;
  int64_t tbl_cnt = lock_ctdefs_.count();
  BASE_ADD_LEN((ObTableLockSpec, ObTableModifySpec));
  OB_UNIS_ADD_LEN(for_update_wait_us_);
  OB_UNIS_ADD_LEN(skip_locked_);
  OB_UNIS_ADD_LEN(tbl_cnt);
  for (int64_t i = 0; i < tbl_cnt; ++i) {
    int64_t index_cnt = lock_ctdefs_.at(i).count();
    OB_UNIS_ADD_LEN(index_cnt);
    for (int64_t j = 0; j < index_cnt; ++j) {
      ObLockCtDef *lock_ctdef = lock_ctdefs_.at(i).at(j);
      if (lock_ctdef != nullptr) {
        OB_UNIS_ADD_LEN(*lock_ctdef);
      }
    }
  }

  OB_UNIS_ADD_LEN(is_multi_table_skip_locked_);
  return len;
}

ObTableLockSpec::ObTableLockSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObTableModifySpec(alloc, type),
    for_update_wait_us_(-1),
    skip_locked_(false),
    lock_ctdefs_(),
    is_multi_table_skip_locked_(false),
    alloc_(alloc)
{}

ObTableLockSpec::~ObTableLockSpec()
{}


ObTableLockOp::ObTableLockOp(ObExecContext &exec_ctx,
                             const ObOpSpec &spec,
                             ObOpInput *input)
  : ObTableModifyOp(exec_ctx, spec, input),
    savepoint_no_(),
    need_return_row_(false)
{
}

int ObTableLockOp::inner_open()
{
  int ret = OB_SUCCESS;
  //execute lock with das
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.lock_ctdefs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("del ctdef is invalid", K(ret), KP(this));
  } else if (OB_UNLIKELY(iter_end_)) {
    //do nothing
  } else if (OB_FAIL(inner_open_with_das())) {
    LOG_WARN("inner open with das failed", K(ret));
  }
  return ret;
}

int ObTableLockOp::inner_open_with_das()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_lock_rtdef())) {
    LOG_WARN("init lock rtdef failed", K(ret), K(MY_SPEC.lock_ctdefs_.count()));
  }
  return ret;
}

int ObTableLockOp::init_lock_rtdef()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = NULL;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(lock_rtdefs_.allocate_array(ctx_.get_allocator(), MY_SPEC.lock_ctdefs_.count()))) {
    LOG_WARN("allocate lock rtdef failed", K(ret), K(MY_SPEC.lock_ctdefs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < lock_rtdefs_.count(); ++i) {
    LockRtDefArray &rtdefs = lock_rtdefs_.at(i);
    const ObLockCtDef *lock_ctdef = MY_SPEC.lock_ctdefs_.at(i).at(0);
    int64_t wait_us = MY_SPEC.for_update_wait_us_ > 0 ?
        MY_SPEC.for_update_wait_us_ + my_session->get_query_start_time() :
        MY_SPEC.for_update_wait_us_;
    if (OB_FAIL(rtdefs.allocate_array(ctx_.get_allocator(), MY_SPEC.lock_ctdefs_.at(i).count()))) {
      LOG_WARN("allocate lock rtdefs failed", K(ret), K(MY_SPEC.lock_ctdefs_.at(i).count()));
    } else if (OB_FAIL(ObDMLService::init_lock_rtdef(dml_rtctx_, *lock_ctdef, rtdefs.at(0), wait_us))) {
      LOG_WARN("init lock rtdef failed", K(ret));
    }
  }
  return ret;
}

int ObTableLockOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_), K(iter_end_));
    ret = OB_ITER_END;
  } else {
    need_return_row_ = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(get_next_row_from_child())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          iter_end_ = true;
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(lock_row_to_das())) {
        LOG_WARN("write row to das failed", K(ret));
      } else if (OB_FAIL(submit_row_by_strategy())) {
        LOG_WARN("submit row by strategy failed", K(ret));
      } else if (is_error_logging_ && err_log_rt_def_.first_err_ret_ != OB_SUCCESS) {
        clear_evaluated_flag();
        err_log_rt_def_.curr_err_log_record_num_++;
        err_log_rt_def_.reset();
        continue;
      } else if (need_return_row_) {
        //break to output this row
        break;
      }
    }

    if (OB_SUCC(ret) && iter_end_ && dml_rtctx_.das_ref_.has_task()) {
      //DML operator reach iter end,
      //now submit the remaining rows in the DAS Write Buffer to the storage
      if (OB_FAIL(submit_all_dml_task())) {
        LOG_WARN("failed to submit the remaining dml tasks", K(ret));
      }
      //to post process the DML info after writing all data to the storage
      ret = write_rows_post_proc(ret);
    }
    if (OB_SUCC(ret) && iter_end_) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObTableLockOp::submit_row_by_strategy()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.is_skip_locked()) {
    need_return_row_ = true;
    if (OB_FAIL(discharge_das_write_buffer())) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret
          && OB_TRANSACTION_SET_VIOLATION != ret
          && OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
        LOG_WARN("failed to lock row with das", K(ret));
      } else if (MY_SPEC.is_nowait() && OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
        ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
      }
    }
  } else if (OB_FAIL(lock_one_row_post_proc())) {
    LOG_WARN("lock one row post proc failed", K(ret));
  }
  return ret;
}

int ObTableLockOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_), K(iter_end_));
    brs_.end_ = true;
    brs_.size_ = 0;
  } else {
    need_return_row_ = false;
    const ObBatchRows * child_brs = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(get_next_batch_from_child(max_row_cnt, child_brs))) {
        if (OB_ITER_END == ret) {
          iter_end_ = true;
          ret = OB_SUCCESS;
          break;
        }
      } else if (child_brs->size_ == 0 && child_brs->end_) {
        iter_end_ = true;
        brs_.end_ = true;
        brs_.size_ = 0;
        break;
      } else if (OB_FAIL(lock_batch_to_das(child_brs))) {
        LOG_WARN("write row to das failed", K(ret));
      } else if (is_error_logging_ && err_log_rt_def_.first_err_ret_ != OB_SUCCESS) {
        clear_evaluated_flag();
        err_log_rt_def_.curr_err_log_record_num_++;
        err_log_rt_def_.reset();
        continue;
      } else if (!brs_.skip_->is_all_true(brs_.size_)) {
        //this batch has not been skipped for all rows, need break to output this batch
        break;
      }
    }
  }
  if (OB_SUCC(ret) && iter_end_ && dml_rtctx_.das_ref_.has_task()) {
    //DML operator reach iter end,
    //now submit the remaining rows in the DAS Write Buffer to the storage
    if (OB_FAIL(submit_all_dml_task())) {
      LOG_WARN("failed to submit the remaining dml tasks", K(ret));
    }
    //to post process the DML info after writing all data to the storage
    ret = write_rows_post_proc(ret);
  }
  return ret;
}

// this func only work for for update skip locked
OB_INLINE int ObTableLockOp::lock_one_row_post_proc()
{
  int ret = OB_SUCCESS;

  if (MY_SPEC.is_multi_table_skip_locked_ &&
      OB_FAIL(ObSqlTransControl::create_anonymous_savepoint(ctx_, savepoint_no_))) {
    LOG_WARN("fail to get save point", K(ret));
  } else if (OB_FAIL(submit_all_dml_task())) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret &&
        OB_TRANSACTION_SET_VIOLATION != ret &&
        OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
      LOG_WARN("submit all dml task failed", K(ret));
    } else if (MY_SPEC.is_skip_locked()) {
      ret = OB_SUCCESS;
      dml_rtctx_.reuse(); //reuse current context to lock the next row
      need_return_row_ = false;
    }
  } else {
    need_return_row_ = true;
  }

  // if fail must rollback to save point
  if (OB_SUCC(ret) && !need_return_row_ && MY_SPEC.is_multi_table_skip_locked_) {
    if (OB_FAIL(ObSqlTransControl::rollback_savepoint(ctx_, savepoint_no_))) {
      LOG_WARN("fail to rollback save point", K(ret));
    }
  }
  return ret;
}

int ObTableLockOp::write_rows_post_proc(int last_errno)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(last_errno)) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret &&
        OB_TRANSACTION_SET_VIOLATION != ret &&
        OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
      LOG_WARN("failed to lock row with das", K(ret));
    } else if (MY_SPEC.is_skip_locked()) {
      ret = OB_SUCCESS;
    } else if (MY_SPEC.is_nowait() && OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
    }
  }
  return ret;
}

int ObTableLockOp::calc_tablet_loc(const ObLockCtDef &lock_ctdef,
                                   ObLockRtDef &lock_rtdef,
                                   ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_dist_das_) {
    if (lock_ctdef.multi_ctdef_ != nullptr) {
      ObExpr *calc_part_id_expr = lock_ctdef.multi_ctdef_->calc_part_id_expr_;
      ObObjectID partition_id = OB_INVALID_ID;
      ObTabletID tablet_id;
      ObDASTableLoc &table_loc = *lock_rtdef.das_rtdef_.table_loc_;
      if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(calc_part_id_expr, eval_ctx_, partition_id, tablet_id))) {
        LOG_WARN("calc part and tablet id by expr failed", K(ret));
      } else if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(table_loc, tablet_id, tablet_loc))) {
        LOG_WARN("extended tablet loc failed", K(ret));
      }
    }
  } else {
    //direct lock to storage
    tablet_loc = (MY_INPUT.get_tablet_loc() != nullptr ?
        MY_INPUT.get_tablet_loc() : MY_INPUT.get_table_loc()->get_first_tablet_loc());
  }
  return ret;
}

int ObTableLockOp::lock_row_to_das()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;

  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is null", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.lock_ctdefs_.count(); ++i) {
    const ObTableLockSpec::LockCtDefArray &ctdefs = MY_SPEC.lock_ctdefs_.at(i);
    LockRtDefArray &rtdefs = lock_rtdefs_.at(i);
    //lock each table with fetched row
    const ObLockCtDef &lock_ctdef = *ctdefs.at(0);
    ObLockRtDef &lock_rtdef = rtdefs.at(0);
    ObDASTabletLoc *tablet_loc = nullptr;
    bool is_skipped = false;
    ++lock_rtdef.cur_row_num_;
    if (OB_FAIL(ObDMLService::process_lock_row(lock_ctdef, lock_rtdef, is_skipped, *this))) {
      LOG_WARN("process lock row failed", K(ret));
    } else if (OB_UNLIKELY(is_skipped)) {
    } else if (OB_FAIL(calc_tablet_loc(lock_ctdef, lock_rtdef, tablet_loc))) {
      LOG_WARN("calc partition key failed", K(ret));
    } else if (OB_FAIL(ObDMLService::lock_row(lock_ctdef, lock_rtdef, tablet_loc, dml_rtctx_))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret &&
          OB_TRANSACTION_SET_VIOLATION != ret &&
          OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
        LOG_WARN("failed to lock row with das", K(ret));
      } else if (MY_SPEC.is_nowait() && OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
        ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
      }
    }
  }
  return ret;
}

int ObTableLockOp::lock_batch_to_das(const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;

  // Note: there are three evalctx involved in das lock:
  // 1. eval_ctx_,
  // 2. das_ctx_.eval_ctx_,
  // 3. lock_rtdef.das_rtdef_.eval_ctx_
  // They all referrenced to operator eval_ctx_, therefore, set batch_idx for
  // eval_ctx_ would set them all
  ObEvalCtx::BatchInfoScopeGuard operator_evalctx_guard(eval_ctx_);
  operator_evalctx_guard.set_batch_size(child_brs->size_);
  (void) brs_.copy(child_brs);
  for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; i++) {
    need_return_row_ = false;
    if (child_brs->skip_->at(i)) {
      continue;
    }
    operator_evalctx_guard.set_batch_idx(i);
    if (OB_FAIL(lock_row_to_das())) {
      LOG_WARN("Failed to lock das row", K(i), K(ret));
    } else if (OB_FAIL(submit_row_by_strategy())) {
      LOG_WARN("submit row by strategy failed", K(ret));
    } else if (MY_SPEC.is_skip_locked() && !need_return_row_) {
      //lock conflict, skip it
      brs_.skip_->set(i);
    }
  }
  clear_evaluated_flag();

  return ret;
}

OB_INLINE int ObTableLockOp::get_next_batch_from_child(const int64_t max_row_cnt,
                                                       const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  clear_datum_eval_flag();
  if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
    LOG_WARN("fail to get next batch", K(ret));
  } else if (OB_LIKELY(!child_brs->end_ && child_brs->size_ > 0)) {
    PRINT_VECTORIZED_ROWS(SQL, TRACE, eval_ctx_, child_->get_spec().output_, child_brs->size_,
                         child_brs->skip_);
  }
  return ret;
}

int ObTableLockOp::inner_close()
{
  return ObTableModifyOp::inner_close();
}

int ObTableLockOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    lock_rtdefs_.release_array();
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(iter_end_)) {
      //do nothing
    } else if (OB_FAIL(init_lock_rtdef())) {
      LOG_WARN("init insert rtdef failed", K(ret));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
