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

#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/table/ob_table_lookup_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_group_scan_op.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObTableLookupSpec, ObOpSpec),
                    calc_part_id_expr_,
                    loc_meta_,
                    scan_ctdef_,
                    flashback_item_.need_scn_,
                    flashback_item_.flashback_query_expr_,
                    flashback_item_.flashback_query_type_,
                    batch_rescan_,
                    rowkey_exprs_);

ObTableLookupOp::ObTableLookupOp(ObExecContext &exec_ctx,
                                 const ObOpSpec &spec,
                                 ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    scan_rtdef_(),
    allocator_(),
    das_ref_(eval_ctx_, exec_ctx),
    lookup_result_(),
    index_group_cnt_(1),
    lookup_group_cnt_(1),
    lookup_rowkey_cnt_(0),
    lookup_row_cnt_(0),
    state_(INDEX_SCAN),
    index_end_(false)
{
}

int ObTableLookupOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = NULL;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  if (OB_ISNULL(sql_ctx = ctx_.get_sql_ctx())
      || OB_ISNULL(sql_ctx->schema_guard_)
      || OB_ISNULL(MY_SPEC.calc_part_id_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(sql_ctx), KP(MY_SPEC.calc_part_id_expr_));
  } else if (OB_FAIL(init_das_lookup_rtdef())) {
    LOG_WARN("init das lookup rtdef", K(ret));
  } else {
    ObMemAttr mem_attr;
    mem_attr.tenant_id_ = my_session->get_effective_tenant_id();
    mem_attr.label_ = ObModIds::OB_SQL_TABLE_LOOKUP;
    allocator_.set_attr(mem_attr);
    das_ref_.set_mem_attr(mem_attr);
    das_ref_.set_expr_frame_info(&MY_SPEC.plan_->get_expr_frame_info());
  }
  if (OB_SUCC(ret)) {
    if (child_->get_spec().is_table_scan()) {
      //global index scan and its lookup maybe share some expr,
      //so remote lookup task change its datum ptr,
      //and also lead index scan touch the wild datum ptr
      //so need to associate the result iterator of scan and lookup
      //resetting the index scan result datum ptr will also reset the lookup result datum ptr
      static_cast<ObTableScanOp*>(child_)->das_ref_.set_lookup_iter(&lookup_result_);
    } else if (PHY_GRANULE_ITERATOR == child_->get_spec().get_type()
               && child_->get_child()->get_spec().is_table_scan()) {
      static_cast<ObTableScanOp*>(child_->get_child())->das_ref_.set_lookup_iter(&lookup_result_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid child of table lookup", K(child_->get_spec().get_type()), K(ret));
    }
  }
  LOG_DEBUG("open table lookup", K(MY_SPEC));
  return ret;
}

int ObTableLookupOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : DEFAULT_BATCH_ROW_COUNT;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  do {
    switch (state_) {
      case INDEX_SCAN : {
        lookup_rowkey_cnt_ = 0;
        int64_t start_group_idx = index_group_cnt_ - 1;
        while (OB_SUCC(ret) && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          clear_evaluated_flag();
          if (OB_FAIL(child_->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row from child failed", K(ret));
            } else if (MY_SPEC.batch_rescan_) {
              //switch to next index iterator, call child's rescan
              if (OB_FAIL(child_->rescan())) {
                if (OB_ITER_END != ret) {
                  LOG_WARN("rescan index operator failed", K(ret));
                }
              } else {
                ++index_group_cnt_;
                LOG_DEBUG("switch to next index batch to fetch rowkey", K(index_group_cnt_), K(lookup_rowkey_cnt_));
              }
            }
          } else {
            if (OB_FAIL(process_data_table_rowkey())) {
              LOG_WARN("process data table rowkey with das failed", K(ret));
            } else {
              ++lookup_rowkey_cnt_;
            }
          }
        } // while end
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          state_ = DISTRIBUTED_LOOKUP;
          set_index_end(OB_ITER_END == ret);
          ret = OB_SUCCESS;
          OZ(init_das_group_range(start_group_idx, index_group_cnt_));
        }
        break;
      }
      case DISTRIBUTED_LOOKUP : {
        lookup_row_cnt_ = 0;
        if (OB_FAIL(do_table_lookup())) {
          LOG_WARN("do table lookup failed", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }
      case OUTPUT_ROWS : {
        if (OB_FAIL(get_next_data_table_row())) {
          if (OB_ITER_END == ret) {
            if (OB_FAIL(check_lookup_row_cnt())) {
              LOG_WARN("check lookup row cnt failed", K(ret));
            } else if (need_next_index_batch()) {
              if (OB_FAIL(das_ref_.close_all_task())) {
                LOG_WARN("close all das task failed", K(ret));
              } else {
                state_ = INDEX_SCAN;
                das_ref_.reuse();
                allocator_.reuse();
                index_end_ = false;
              }
            } else {
              state_ = EXECUTION_FINISHED;
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          got_next_row = true;
          ++lookup_row_cnt_;
          LOG_DEBUG("got next row from table lookup",
                    K(MY_SPEC.loc_meta_), K(MY_SPEC.scan_ctdef_.ref_table_id_),
                    "main table output", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
        }
        break;
      }
      case EXECUTION_FINISHED : {
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          ret = OB_ITER_END;
        }
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  } while (!(got_next_row || OB_FAIL(ret)));

  return ret;
}

OB_INLINE int ObTableLookupOp::check_lookup_row_cnt()
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_defensive_check()
      && MY_SPEC.scan_ctdef_.pd_expr_spec_.pushdown_filters_.empty()) {
    if (OB_UNLIKELY(lookup_rowkey_cnt_ != lookup_row_cnt_)
        && index_group_cnt_ == lookup_group_cnt_) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      ObString func_name = ObString::make_string("check_lookup_row_cnt");
      LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
      LOG_ERROR("Fatal Error!!! Catch a defensive error!",
                K(ret), K_(lookup_rowkey_cnt), K_(lookup_row_cnt),
                "index_group_cnt", index_group_cnt_,
                "lookup_group_cnt", lookup_group_cnt_);
      //now to dump lookup das task info
      for (DASTaskIter task_iter = das_ref_.begin_task_iter(); !task_iter.is_end(); ++task_iter) {
        ObDASScanOp *das_op = static_cast<ObDASScanOp*>(*task_iter);
        LOG_INFO("dump TableLookup DAS Task range",
                 "scan_range", das_op->get_scan_param().key_ranges_,
                 "range_array_pos", das_op->get_scan_param().range_array_pos_,
                 "tablet_id", das_op->get_tablet_id());
      }
      LOG_INFO("dump TableLookup Info", "batch_scan", MY_SPEC.batch_rescan_,
               "lookup_ctdef", MY_SPEC.scan_ctdef_,
               "lookup_rtdef", scan_rtdef_);
    }
  }
  return ret;
}

int ObTableLookupOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool stop_loop = false;
  const ObBatchRows * child_brs = nullptr;
  int64_t batch_size = common::min(max_row_cnt, MY_SPEC.max_batch_size_);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(batch_size);
  batch_info_guard.set_batch_idx(0);
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : DEFAULT_BATCH_ROW_COUNT;
  do {
    switch (state_) {
      case INDEX_SCAN : {
        lookup_rowkey_cnt_ = 0;
        int64_t start_group_idx = index_group_cnt_ - 1;
        while (OB_SUCC(ret) && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          int64_t rowkey_batch_size = min(batch_size, default_row_batch_cnt - lookup_rowkey_cnt_);
          if (OB_FAIL(child_->get_next_batch(rowkey_batch_size, child_brs))) {
            LOG_WARN("get next row from child failed", K(ret));
          } else if (child_brs->size_ == 0 && child_brs->end_) {
            if (MY_SPEC.batch_rescan_) {
              if (OB_FAIL(child_->rescan())) {
                if (OB_ITER_END != ret) {
                  LOG_WARN("rescan index operator failed", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  set_index_end(true);
                  break;
                }
              } else {
                ++index_group_cnt_;
                LOG_DEBUG("switch to next index batch to fetch rowkey", K(index_group_cnt_), K(lookup_rowkey_cnt_));
              }
            } else {
              // index scan is finished, go to lookup stage
              set_index_end(true);
              break;
            }
          } else {
            // critical path: no child_brs sanity check
            set_index_end(true == child_brs->end_);
            clear_evaluated_flag();
            if (OB_FAIL(process_data_table_rowkeys(child_brs, batch_info_guard))) {
              LOG_WARN("process data table rowkey with das failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          state_ = DISTRIBUTED_LOOKUP;
          OZ(init_das_group_range(start_group_idx, index_group_cnt_));
        }
        LOG_DEBUG("index scan end", KPC(child_brs), K(index_end_),
                  K(index_group_cnt_), K(lookup_rowkey_cnt_),
                  K(lookup_group_cnt_), K(lookup_row_cnt_));
        break;
      }
      case DISTRIBUTED_LOOKUP : {
        lookup_row_cnt_ = 0;
        if (OB_FAIL(do_table_lookup())) {
          LOG_WARN("do table lookup failed", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }
      case OUTPUT_ROWS : {
        if (OB_SUCC(ret) && OB_FAIL(get_next_data_table_rows(brs_.size_, batch_size))) {
          if (OB_ITER_END == ret) {
          // OB_ITER_END is returned when all partition iterator has NO output
          if (OB_FAIL(check_lookup_row_cnt())) {
            LOG_WARN("check lookup row cnt failed", K(ret));
          } else if (need_next_index_batch()) { // index search does not reach end, continue index scan
            state_ = INDEX_SCAN;
            if (OB_FAIL(das_ref_.close_all_task())) {
              LOG_WARN("close all das task failed", K(ret));
            } else {
              das_ref_.reuse();
              allocator_.reuse();
            }
          } else {
              state_ = EXECUTION_FINISHED;
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          stop_loop = true;
          brs_.end_ = false;
          PRINT_VECTORIZED_ROWS(SQL, DEBUG, eval_ctx_, MY_SPEC.output_, brs_.size_, K(MY_SPEC.scan_ctdef_.ref_table_id_));
        }
        break;
      }
      case EXECUTION_FINISHED : {
        brs_.end_ = true;
        stop_loop = true;
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  } while (!(stop_loop || OB_FAIL(ret)));

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.end_ = true;
    LOG_DEBUG("inner_get_next_batch reach end");
  }
  return ret;
}

bool ObTableLookupOp::need_next_index_batch() const
{
  bool bret = false;
  if (!MY_SPEC.batch_rescan_) {
    bret = !index_end_;
  } else if (lookup_group_cnt_ >= index_group_cnt_) {
    bret = !index_end_;
  }
  return bret;
}

int ObTableLookupOp::init_das_group_range(int64_t cur_group_idx, int64_t group_size)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.batch_rescan_) {
    for (DASTaskIter task_iter = das_ref_.begin_task_iter(); !task_iter.is_end(); ++task_iter) {
      ObDASGroupScanOp *group_op = static_cast<ObDASGroupScanOp*>(*task_iter);
      group_op->init_group_range(cur_group_idx, group_size);
      LOG_DEBUG("set group info",
                "scan_range", group_op->get_scan_param().key_ranges_,
                K(*group_op));
    }
  }
  return ret;
}

int ObTableLookupOp::process_data_table_rowkeys(const ObBatchRows *brs,
                                                ObEvalCtx::BatchInfoScopeGuard &batch_info_guard)
{
  int ret = OB_SUCCESS;
  batch_info_guard.set_batch_size(brs->size_);
  for (auto i = 0; OB_SUCC(ret) && i < brs->size_; i++)
  {
    if (brs->skip_->at(i)) {
      continue;
    }
    batch_info_guard.set_batch_idx(i);
    if (OB_FAIL(process_data_table_rowkey())) {
      LOG_WARN("Failed to process_data_table_rowkey", K(ret), K(i));
    } else {
      ++lookup_rowkey_cnt_;
    }
  }
  return ret;
}

bool ObTableLookupOp::has_das_scan_op(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_op)
{
  if (MY_SPEC.batch_rescan_) {
    das_op = static_cast<ObDASScanOp*>(
        das_ref_.find_das_task(tablet_loc, DAS_OP_TABLE_BATCH_SCAN));
  } else {
    das_op = static_cast<ObDASScanOp*>(
        das_ref_.find_das_task(tablet_loc, DAS_OP_TABLE_SCAN));
  }
  return das_op != nullptr;
}

int ObTableLookupOp::process_data_table_rowkey()
{
  int ret = OB_SUCCESS;
  ObObjectID partition_id = ObExprCalcPartitionId::NONE_PARTITION_ID;
  ObTabletID tablet_id;
  ObDASScanOp *das_scan_op = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;

  if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(MY_SPEC.calc_part_id_expr_, eval_ctx_, partition_id, tablet_id))) {
    LOG_WARN("fail to calc part id", K(ret), KPC(MY_SPEC.calc_part_id_expr_));
  } else if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(*scan_rtdef_.table_loc_, tablet_id, tablet_loc))) {
    LOG_WARN("pkey to tablet loc failed", K(ret));
  } else if (OB_UNLIKELY(!has_das_scan_op(tablet_loc, das_scan_op))) {
    ObDASOpType op_type = MY_SPEC.batch_rescan_ ? DAS_OP_TABLE_BATCH_SCAN : DAS_OP_TABLE_SCAN;
    ObIDASTaskOp *tmp_op = nullptr;
    if (OB_FAIL(das_ref_.create_das_task(tablet_loc, op_type, tmp_op))) {
      LOG_WARN("prepare das task failed", K(ret));
    } else {
      das_scan_op = static_cast<ObDASScanOp*>(tmp_op);
      das_scan_op->set_scan_ctdef(&MY_SPEC.scan_ctdef_);
      das_scan_op->set_scan_rtdef(&scan_rtdef_);
      das_scan_op->set_can_part_retry(ctx_.get_my_session()->is_user_session());
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("table lookup process data table rowkey",
              "child output", ROWEXPR2STR(eval_ctx_, child_->get_spec().output_),
              K(partition_id), K(tablet_id));
    storage::ObTableScanParam &scan_param = das_scan_op->get_scan_param();
    ObNewRange lookup_range;
    if (OB_FAIL(build_data_table_range(lookup_range))) {
      LOG_WARN("build data table range failed", K(ret), KPC(tablet_loc));
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("store lookup key range failed", K(ret), K(scan_param));
    } else {
      scan_param.is_get_ = true;
    }
  }
  return ret;
}

int ObTableLookupOp::init_das_lookup_rtdef()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  ObTaskExecutorCtx &task_exec_ctx = ctx_.get_task_exec_ctx();
  ObDASCtx &das_ctx = ctx_.get_das_ctx();
  scan_rtdef_.timeout_ts_ = plan_ctx->get_ps_timeout_timestamp();
  scan_rtdef_.sql_mode_ = my_session->get_sql_mode();
  scan_rtdef_.stmt_allocator_.set_alloc(&das_ref_.get_das_alloc());
  scan_rtdef_.scan_allocator_.set_alloc(&das_ref_.get_das_alloc());
  int64_t schema_version = task_exec_ctx.get_query_tenant_begin_schema_version();
  scan_rtdef_.tenant_schema_version_ = schema_version;
  scan_rtdef_.eval_ctx_ = &get_eval_ctx();
  if (OB_FAIL(das_ctx.extended_table_loc(MY_SPEC.loc_meta_, scan_rtdef_.table_loc_))) {
    LOG_WARN("extended table location failed", K(ret), K(MY_SPEC.loc_meta_));
  } else if (OB_FAIL(MY_SPEC.flashback_item_.set_flashback_query_info(eval_ctx_, scan_rtdef_))) {
    LOG_WARN("failed to set flashback query snapshot version", K(ret));
  } else if (OB_FAIL(scan_rtdef_.init_pd_op(ctx_, MY_SPEC.scan_ctdef_))) {
    LOG_WARN("init pushdown storage filter failed", K(ret));
  }
  return ret;
}

int ObTableLookupOp::build_data_table_range(ObNewRange &lookup_range)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = MY_SPEC.rowkey_exprs_.count();
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new(buf) ObObj[rowkey_cnt];
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    ObObj tmp_obj;
    ObExpr *expr = MY_SPEC.rowkey_exprs_.at(i);
    ObDatum &col_datum = expr->locate_expr_datum(eval_ctx_);
    if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(allocator_, tmp_obj, obj_ptr[i]))) {
      LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
    }
  }
  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    uint64_t ref_table_id = MY_SPEC.scan_ctdef_.ref_table_id_;
    if (OB_FAIL(lookup_range.build_range(ref_table_id, table_rowkey))) {
      LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(table_rowkey));
    } else {
      lookup_range.group_idx_ = index_group_cnt_ - 1;
    }
    LOG_DEBUG("build data table range", K(ret), K(table_rowkey), K(lookup_range));
  }
  return ret;
}

int ObTableLookupOp::do_table_lookup()
{
  int ret = das_ref_.execute_all_task();
  if (OB_SUCC(ret)) {
    lookup_result_ = das_ref_.begin_result_iter();
  }
  return ret;
}

int ObTableLookupOp::get_next_data_table_row()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  if (OB_UNLIKELY(lookup_result_.is_end())) {
    ret = OB_ITER_END;
    LOG_DEBUG("lookup task is empty", K(ret), K(MY_SPEC.scan_ctdef_));
  }
  while (OB_SUCC(ret) && !got_row) {
    scan_rtdef_.p_pd_expr_op_->clear_datum_eval_flag();
    if (OB_FAIL(lookup_result_.get_next_row())) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(lookup_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task result failed", K(ret));
          }
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else {
      got_row = true;
    }
  }
  return ret;
}

int ObTableLookupOp::get_next_data_table_rows(int64_t &count,
                                              int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_rows = false;
  if (OB_UNLIKELY(lookup_result_.is_end())) {
    ret = OB_ITER_END;
    LOG_DEBUG("lookup task is empty", K(ret), K(MY_SPEC.scan_ctdef_));
  }
  while (OB_SUCC(ret) && !got_rows) {
    clear_evaluated_flag();
    ret = lookup_result_.get_next_rows(count, capacity);
    if (OB_ITER_END == ret && count > 0) {
      got_rows = true;
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(lookup_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task result failed", K(ret));
          } else {
            // do nothing, just return OB_ITER_END to notify the caller das scan
            // reach end
            LOG_DEBUG("das_ref_ reach end, stop lookup table");
          }
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else if (count == 0) {
      if (OB_FAIL(lookup_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task failed", K(ret));
          } else {
            // do nothing, just return OB_ITER_END to notify the caller das scan
            // reach end
            LOG_DEBUG("das_ref_ reach end, stop lookup table");
          }
      }
    } else {
      got_rows = true;
    }
  }
  if (OB_SUCC(ret) && got_rows) {
    lookup_row_cnt_ += count;
  }
  return ret;
}

void ObTableLookupOp::destroy()
{
  state_ = EXECUTION_FINISHED;
  index_end_ = true;
  das_ref_.reset();
  allocator_.reset();
  scan_rtdef_.~ObDASScanRtDef();
  ObOperator::destroy();
}

void ObTableLookupOp::reset_for_rescan()
{
  state_ = INDEX_SCAN;
  das_ref_.reuse();
  allocator_.reuse();
  scan_rtdef_.scan_allocator_.set_alloc(&allocator_);
  index_end_ = false;
  index_group_cnt_ = 1;
  lookup_group_cnt_ = 1;
}

int ObTableLookupOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(das_ref_.close_all_task())) {
    LOG_WARN("close all das task failed", K(ret));
  }

  return ret;
}

//TableLookup has its own rescan
int ObTableLookupOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to do inner rescan", K(ret));
  } else if (MY_SPEC.batch_rescan_ && lookup_group_cnt_ < index_group_cnt_) {
    LOG_DEBUG("rescan in group lookup, only need to switch iterator",
              K(lookup_group_cnt_), K(index_group_cnt_));
    ret = switch_lookup_result_iter();
  } else if (OB_FAIL(das_ref_.close_all_task())) {
    LOG_WARN("failed to close all das task", K(ret));
  } else if (OB_FAIL(child_->rescan())) {
    LOG_WARN("rescan operator failed", K(ret));
  } else {
    reset_for_rescan();
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObTableLookupOp::switch_lookup_result_iter()
{
  int ret = OB_SUCCESS;
  for (DASTaskIter task_iter = das_ref_.begin_task_iter();
      OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
    ObDASGroupScanOp *batch_op = static_cast<ObDASGroupScanOp*>(*task_iter);
    if (OB_FAIL(batch_op->switch_scan_group())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("switch batch iter failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ++lookup_group_cnt_;
    state_ = OUTPUT_ROWS;
    lookup_result_ = das_ref_.begin_result_iter();
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
