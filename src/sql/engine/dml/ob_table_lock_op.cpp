/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_table_lock_op.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/table/ob_table_scan_op.h"

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

int ObTableLockOp::handle_gi_task_not_found(GIPrepareTaskMap *gi_prepare_map,
                                            const ObTableID &table_loc_id,
                                            const ObTableID &ref_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_gi_task_from_subtree_tsc(gi_prepare_map, table_loc_id, ref_table_id))) {
    LOG_WARN("failed to get gi task from subtree tsc", K(ret), K(MY_SPEC.get_id()));
  }
  return ret;
}

int ObTableLockOp::get_gi_task_from_subtree_tsc(GIPrepareTaskMap *gi_prepare_map,
                                                const ObTableID &table_loc_id,
                                                const ObTableID &ref_table_id)
{
  int ret = OB_SUCCESS;
  bool has_task = false;
  ObTabletID target_tablet_id;
  ObGranuleTaskInfo gi_task_info;
  ObDASCtx &das_ctx = ctx_.get_das_ctx();
  ObSEArray<const ObTableScanSpec *, 4> matched_tscs;
  if (OB_ISNULL(gi_prepare_map)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gi prepare map is null", K(ret));
  } else if (OB_FAIL(collect_gi_tsc_specs(&MY_SPEC, table_loc_id, matched_tscs))) {
    LOG_WARN("failed to collect gi tsc specs", K(ret), K(MY_SPEC.get_id()));
  } else if (matched_tscs.empty()) {
    // Missing a scheduled scan below the lock op indicates an invalid plan.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no matched gi tsc below lock op", K(ret), K(MY_SPEC.get_id()),
             K(table_loc_id), K(ref_table_id), K(lbt()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < matched_tscs.count(); ++i) {
      const ObTableScanSpec *tsc_spec = matched_tscs.at(i);
      if (OB_ISNULL(tsc_spec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null tsc spec", K(ret));
      } else if (OB_FAIL(gi_prepare_map->get_refactored(tsc_spec->get_id(), gi_task_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_DEBUG("matched tsc has no gi task", K(tsc_spec->get_id()), K(MY_SPEC.get_id()));
        } else {
          LOG_WARN("failed to get gi task of tsc", K(ret), K(tsc_spec->get_id()));
        }
      } else {
        ObTabletID lock_tablet_id;
        if (OB_ISNULL(gi_task_info.tablet_loc_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null tablet loc in gi task", K(ret), K(tsc_spec->get_id()));
        } else if (OB_FAIL(resolve_lock_tablet_id(*tsc_spec, gi_task_info.tablet_loc_->tablet_id_,
                                                  ref_table_id, lock_tablet_id))) {
          LOG_WARN("failed to resolve lock tablet id", K(ret), K(tsc_spec->get_id()));
        } else if (!has_task) {
          has_task = true;
          target_tablet_id = lock_tablet_id;
        } else if (target_tablet_id != lock_tablet_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mapped lock tablet id conflict between tscs", K(ret),
                   K(target_tablet_id), K(lock_tablet_id), K(MY_SPEC.get_id()));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!has_task) {
    iter_end_ = true;
  } else {
    ObDASTableLoc *table_loc = nullptr;
    ObDASTabletLoc *target_tablet_loc = nullptr;
    if (OB_ISNULL(table_loc = das_ctx.get_table_loc_by_id(table_loc_id, ref_table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get table location by table id failed", K(ret), K(table_loc_id), K(ref_table_id));
    } else if (OB_FAIL(table_loc->get_tablet_loc_by_id(target_tablet_id, target_tablet_loc))) {
      // Rebind the shared task's tablet ID to this worker's tablet location.
      LOG_WARN("get tablet loc by id failed", K(ret), K(target_tablet_id), KPC(table_loc), K(MY_SPEC.get_id()));
    } else if (OB_ISNULL(target_tablet_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target tablet loc not found", K(ret), K(target_tablet_id), KPC(table_loc), K(MY_SPEC.get_id()));
    } else {
      get_input()->set_table_loc(table_loc);
      get_input()->set_tablet_loc(target_tablet_loc);
      LOG_DEBUG("lock op consume a gi task from subtree tsc", K(MY_SPEC.get_id()),
                K(target_tablet_id), KPC(get_input()->get_tablet_loc()));
    }
  }
  return ret;
}

int ObTableLockOp::collect_gi_tsc_specs(const ObOpSpec *spec,
                                        const ObTableID &table_loc_id,
                                        ObIArray<const ObTableScanSpec *> &matched_tscs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null spec", K(ret));
  } else if (PHY_TABLE_SCAN == spec->get_type() ||
             PHY_ROW_SAMPLE_SCAN == spec->get_type() ||
             PHY_BLOCK_SAMPLE_SCAN == spec->get_type()) {
    const ObTableScanSpec &tsc_spec = static_cast<const ObTableScanSpec &>(*spec);
    if (!tsc_spec.use_dist_das() && tsc_spec.gi_above_ && tsc_spec.get_table_loc_id() == table_loc_id) {
      if (OB_FAIL(matched_tscs.push_back(&tsc_spec))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < spec->get_child_cnt(); ++i) {
      if (OB_FAIL(SMART_CALL(collect_gi_tsc_specs(spec->get_child(i), table_loc_id, matched_tscs)))) {
        LOG_WARN("collect gi tsc specs failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLockOp::resolve_lock_tablet_id(const ObTableScanSpec &tsc_spec,
                                          const ObTabletID &tsc_tablet_id,
                                          const ObTableID &ref_table_id,
                                          ObTabletID &lock_tablet_id)
{
  int ret = OB_SUCCESS;
  ObDASCtx &das_ctx = ctx_.get_das_ctx();
  ObDASTableLoc *tsc_table_loc = nullptr;
  ObDASTabletLoc *tsc_tablet_loc = nullptr;
  if (OB_ISNULL(tsc_table_loc = das_ctx.get_table_loc_by_id(
                    tsc_spec.get_table_loc_id(), tsc_spec.get_loc_ref_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tsc table loc failed", K(ret), K(tsc_spec.get_table_loc_id()),
             K(tsc_spec.get_loc_ref_table_id()));
  } else if (OB_FAIL(tsc_table_loc->get_tablet_loc_by_id(tsc_tablet_id, tsc_tablet_loc))) {
    LOG_WARN("get tsc tablet loc failed", K(ret), K(tsc_tablet_id), KPC(tsc_table_loc));
  } else if (OB_ISNULL(tsc_tablet_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tsc tablet loc not found", K(ret), K(tsc_tablet_id), KPC(tsc_table_loc));
  } else if (tsc_spec.get_loc_ref_table_id() == ref_table_id) {
    lock_tablet_id = tsc_tablet_id;
  } else {
    ObDASTabletLoc *lock_tablet_loc = ObDASUtils::get_related_tablet_loc(
        *tsc_tablet_loc, ref_table_id);
    if (OB_ISNULL(lock_tablet_loc) &&
        OB_FAIL(das_ctx.build_related_tablet_loc(*tsc_tablet_loc))) {
      // PX does not serialize related-tablet references, so rebuild a missing one.
      LOG_WARN("build related tablet loc failed", K(ret), K(tsc_tablet_id), K(ref_table_id));
    } else if (OB_ISNULL(lock_tablet_loc) &&
               OB_ISNULL(lock_tablet_loc = ObDASUtils::get_related_tablet_loc(*tsc_tablet_loc, ref_table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get related tablet loc failed", K(ret), K(tsc_tablet_id), K(ref_table_id));
    }
    if (OB_SUCC(ret)) {
      lock_tablet_id = lock_tablet_loc->tablet_id_;
    }
  }
  return ret;
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

  return ret;
}

OB_INLINE int ObTableLockOp::get_next_batch_from_child(const int64_t max_row_cnt,
                                                       const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
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
