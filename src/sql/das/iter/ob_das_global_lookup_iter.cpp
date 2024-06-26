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
#include "sql/das/iter/ob_das_global_lookup_iter.h"
#include "sql/das/iter/ob_das_merge_iter.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASGlobalLookupIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASLookupIter::inner_init(param))) {
    LOG_WARN("failed to init das lookup iter", K(ret));
  } else if (param.type_ != ObDASIterType::DAS_ITER_GLOBAL_LOOKUP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    ObDASGlobalLookupIterParam &lookup_param = static_cast<ObDASGlobalLookupIterParam&>(param);
    can_retry_ = lookup_param.can_retry_;
    calc_part_id_ = lookup_param.calc_part_id_;
    // use lookup_memctx for lookup to avoid memory expansion during global index lookup
    lookup_rtdef_->scan_allocator_.set_alloc(&get_arena_allocator());
    lookup_rtdef_->stmt_allocator_.set_alloc(&get_arena_allocator());
    if (lookup_param.rowkey_exprs_->empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty global rowkey exprs", K(ret));
    } else if (OB_FAIL(rowkey_exprs_.assign(*lookup_param.rowkey_exprs_))) {
      LOG_WARN("failed to assign rowkey exprs", K(ret));
    }
  }
  return ret;
}

int ObDASGlobalLookupIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  // for global lookup, the reuse of index table will handled in TSC.
  if (OB_FAIL(data_table_iter_->reuse())) {
    LOG_WARN("failed to reuse data table iter", K(ret));
  } else if (OB_FAIL(ObDASLookupIter::inner_reuse())) {
    LOG_WARN("failed to reuse das lookup iter", K(ret));
  }
  index_ordered_idx_ = 0;
  return ret;
}

int ObDASGlobalLookupIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASLookupIter::inner_release())) {
    LOG_WARN("failed to release das lookup iter", K(ret));
  }
  return ret;
}

void ObDASGlobalLookupIter::reset_lookup_state()
{
  index_ordered_idx_ = 0;
  ObDASLookupIter::reset_lookup_state();
}

int ObDASGlobalLookupIter::add_rowkey()
{
  int ret = OB_SUCCESS;
  ObObjectID partition_id = OB_INVALID_ID;
  ObTabletID tablet_id(OB_INVALID_ID);
  ObDASScanOp *das_scan_op = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObDASCtx *das_ctx = nullptr;
  bool reuse_das_op = false;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_MERGE);
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(das_ctx = &exec_ctx_->get_das_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get das ctx", KPC_(exec_ctx));
  } else {
    ObDASMergeIter *merge_iter = static_cast<ObDASMergeIter*>(data_table_iter_);
    if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(calc_part_id_,
                                                                 *eval_ctx_,
                                                                 partition_id,
                                                                 tablet_id))) {
      LOG_WARN("failed to calc part id", K(ret), KPC(calc_part_id_));
    } else if (OB_FAIL(das_ctx->extended_tablet_loc(*lookup_rtdef_->table_loc_,
                                                    tablet_id,
                                                    tablet_loc))) {
      LOG_WARN("failed to get tablet loc by tablet_id", K(ret));
    } else if (OB_FAIL(merge_iter->create_das_task(tablet_loc, das_scan_op, reuse_das_op))) {
      LOG_WARN("failed to create das task", K(ret));
    } else if (!reuse_das_op) {
      das_scan_op->set_scan_ctdef(lookup_ctdef_);
      das_scan_op->set_scan_rtdef(lookup_rtdef_);
      das_scan_op->set_can_part_retry(can_retry_);
    }
  }
  if (OB_SUCC(ret)) {
    storage::ObTableScanParam &scan_param = das_scan_op->get_scan_param();
    ObNewRange lookup_range;
    int64_t group_id = 0;
    OB_ASSERT(DAS_OP_TABLE_SCAN == index_ctdef_->op_type_);
    const ObDASScanCtDef *index_ctdef = static_cast<const ObDASScanCtDef*>(index_ctdef_);
    if (nullptr != index_ctdef->group_id_expr_) {
      group_id = index_ctdef->group_id_expr_->locate_expr_datum(*eval_ctx_).get_int();
    }
    if (nullptr != index_ctdef->trans_info_expr_) {
      ObDatum *datum_ptr = nullptr;
      if (OB_FAIL(build_trans_info_datum(index_ctdef->trans_info_expr_, datum_ptr))) {
        LOG_WARN("failed to build trans info datum", K(ret));
      } else if (OB_ISNULL(datum_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(das_scan_op->trans_info_array_.push_back(datum_ptr))) {
        LOG_WARN("failed to push back trans info array", K(ret), KPC(datum_ptr));
      }
    }

    int64_t group_idx = ObNewRange::get_group_idx(group_id);
    index_ordered_idx_++;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_lookup_range(lookup_range))) {
      LOG_WARN("failed to build lookup range", K(ret));
    } else if (FALSE_IT(lookup_range.group_idx_ = group_idx)) {
    } else if (FALSE_IT(lookup_range.index_ordered_idx_ = index_ordered_idx_)) {
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("failed to push back lookup range", K(ret));
    } else {
      scan_param.is_get_ = true;
    }
    LOG_DEBUG("build global lookup range", K(lookup_range), K(ret));
  }

  return ret;
}

int ObDASGlobalLookupIter::add_rowkeys(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K_(eval_ctx));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(count);
    for(int i = 0; OB_SUCC(ret) && i < count; i++) {
      batch_info_guard.set_batch_idx(i);
      if(OB_FAIL(add_rowkey())) {
        LOG_WARN("failed to add rowkey", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObDASGlobalLookupIter::do_index_lookup()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_MERGE);
  ObDASMergeIter *merge_iter = static_cast<ObDASMergeIter*>(data_table_iter_);
  if (OB_FAIL(merge_iter->do_table_scan())) {
    LOG_WARN("failed to do global index lookup", K(ret));
  } else {
    merge_iter->set_merge_status(merge_iter->get_merge_type());
  }
  return ret;
}

int ObDASGlobalLookupIter::check_index_lookup()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(data_table_iter_->get_type() == DAS_ITER_MERGE);
  ObDASMergeIter *merge_iter = static_cast<ObDASMergeIter*>(data_table_iter_);
  if (GCONF.enable_defensive_check() &&
      lookup_ctdef_->pd_expr_spec_.pushdown_filters_.empty()) {
    if (OB_UNLIKELY(lookup_rowkey_cnt_ != lookup_row_cnt_)) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      ObSQLSessionInfo *my_session = exec_ctx_->get_my_session();
      ObString func_name = ObString::make_string("check_lookup_row_cnt");
      LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
      LOG_ERROR("Fatal Error!!! Catch a defensive error!",
          K(ret), K(lookup_rowkey_cnt_), K(lookup_row_cnt_),
          "main table id", lookup_ctdef_->ref_table_id_,
          KPC(my_session->get_tx_desc()));

      int64_t row_num = 0;
      for (DASTaskIter task_iter = merge_iter->begin_task_iter(); !task_iter.is_end(); ++task_iter) {
        ObDASScanOp *das_op = static_cast<ObDASScanOp*>(*task_iter);
        if (das_op->trans_info_array_.count() == das_op->get_scan_param().key_ranges_.count()) {
          for (int64_t i = 0; i < das_op->trans_info_array_.count(); i++) {
            row_num++;
            ObDatum *datum = das_op->trans_info_array_.at(i);
            LOG_ERROR("dump lookup range and trans info of global lookup das task",
                K(row_num), KPC(datum),
                K(das_op->get_scan_param().key_ranges_.at(i)),
                K(das_op->get_tablet_id()));
          }
        } else {
          for (int64_t i = 0; i < das_op->get_scan_param().key_ranges_.count(); i++) {
            row_num++;
            LOG_ERROR("dump lookup range of global lookup das task",
                K(row_num),
                K(das_op->get_scan_param().key_ranges_.at(i)),
                K(das_op->get_tablet_id()));
          }
        }
      }
    }
  }

  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_DUMP_WRITE_BUFFER);
  if (0 != simulate_error) {
    for (DASTaskIter task_iter = merge_iter->begin_task_iter(); !task_iter.is_end(); ++task_iter) {
      ObDASScanOp *das_op = static_cast<ObDASScanOp*>(*task_iter);
      for (int64_t i = 0; i < das_op->trans_info_array_.count(); i++) {
        ObDatum *datum = das_op->trans_info_array_.at(i);
        LOG_INFO("dump trans info of global lookup das task", K(i),
            KPC(das_op->trans_info_array_.at(i)),
            K(das_op->get_scan_param().key_ranges_.at(i)),
            K(das_op->get_tablet_id()));
      }
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
