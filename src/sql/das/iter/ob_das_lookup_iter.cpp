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
#include "sql/das/iter/ob_das_lookup_iter.h"
#include "sql/das/iter/ob_das_merge_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASLookupIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (param.type_ != ObDASIterType::DAS_ITER_LOOKUP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("specific_init with bad param type", K(param.type_));
  } else {
    ObDASLookupIterParam &lookup_param = static_cast<ObDASLookupIterParam&>(param);
    state_ = LookupState::INDEX_SCAN;
    index_end_ = false;
    default_batch_row_count_ = lookup_param.default_batch_row_count_;
    lookup_rowkey_cnt_ = 0;
    lookup_row_cnt_ = 0;
    index_table_iter_ = lookup_param.index_table_iter_;
    data_table_iter_ = lookup_param.data_table_iter_;
    can_retry_ = lookup_param.can_retry_;
    calc_part_id_ = lookup_param.calc_part_id_;
    lookup_ctdef_ = lookup_param.lookup_ctdef_;
    lookup_rtdef_ = lookup_param.lookup_rtdef_;
    rowkey_exprs_ = lookup_param.rowkey_exprs_;
    iter_alloc_ = new (iter_alloc_buf_) common::ObArenaAllocator();
    iter_alloc_->set_attr(ObMemAttr(MTL_ID(), "TableLookup"));
    lookup_rtdef_->scan_allocator_.set_alloc(iter_alloc_);
    lookup_rtdef_->stmt_allocator_.set_alloc(iter_alloc_);
  }
  return ret;
}

int ObDASLookupIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  // the reuse() of index table iter will be handled in TSC.
  if (OB_FAIL(data_table_iter_->reuse())) {
    LOG_WARN("failed to reuse data table iter", K(ret));
  }
  if (OB_NOT_NULL(iter_alloc_)) {
    iter_alloc_->reset_remain_one_page();
  }
  lookup_row_cnt_ = 0;
  lookup_rowkey_cnt_ = 0;
  index_end_ = false;
  state_ = LookupState::INDEX_SCAN;
  return ret;
}

int ObDASLookupIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(iter_alloc_)) {
    iter_alloc_->reset();
    iter_alloc_->~ObArenaAllocator();
    iter_alloc_ = nullptr;
  }
  index_table_iter_ = nullptr;
  data_table_iter_ = nullptr;
  return ret;
}

void ObDASLookupIter::reset_lookup_state()
{
  lookup_rowkey_cnt_ = 0;
  lookup_row_cnt_ = 0;
  if (OB_NOT_NULL(data_table_iter_)) {
    data_table_iter_->reuse();
  }
  if (OB_NOT_NULL(iter_alloc_)) {
    iter_alloc_->reset_remain_one_page();
  }
}

int ObDASLookupIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  do {
    switch (state_) {
      case INDEX_SCAN: {
        reset_lookup_state();
        while (OB_SUCC(ret) && !index_end_ && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          if (OB_FAIL(index_table_iter_->get_next_row())) {
            if(OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row from index table", K(ret));
            } else {
              index_end_ = true;
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(add_rowkey())) {
            LOG_WARN("failed to add row key", K(ret));
          } else {
            ++lookup_rowkey_cnt_;
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_LIKELY(lookup_rowkey_cnt_ > 0)) {
            state_ = DO_LOOKUP;
          } else {
            state_ = FINISHED;
          }
        }
        break;
      }

      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("failed to do index lookup", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }

      case OUTPUT_ROWS: {
        if (OB_FAIL(data_table_iter_->get_next_row())) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            if (OB_FAIL(check_index_lookup())) {
              LOG_WARN("failed to check table lookup", K(ret));
            } else {
              state_ = INDEX_SCAN;
            }
          } else {
            LOG_WARN("failed to get next row from data table", K(ret));
          }
        } else {
          got_next_row = true;
          ++lookup_row_cnt_;
        }
        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lookup state", K_(state));
      }
    }
  } while (!got_next_row && OB_SUCC(ret));

  return ret;
}

int ObDASLookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool get_next_rows = false;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : default_batch_row_count_;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  do {
    switch (state_) {
      case INDEX_SCAN: {
        reset_lookup_state();
        int64_t storage_count = 0;
        int64_t index_capacity = 0;
        while (OB_SUCC(ret) && !index_end_ && lookup_rowkey_cnt_ < default_row_batch_cnt) {
          storage_count = 0;
          index_capacity = std::min(max_size_, default_row_batch_cnt - lookup_rowkey_cnt_);
          if (OB_FAIL(index_table_iter_->get_next_rows(storage_count, index_capacity))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from index table", K(ret));
            } else {
              if (storage_count == 0) {
                index_end_ = true;
              }
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCC(ret) && storage_count > 0) {
            if (OB_FAIL(add_rowkeys(storage_count))) {
              LOG_WARN("failed to add row keys", K(ret));
            } else {
              lookup_rowkey_cnt_ += storage_count;
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_LIKELY(lookup_rowkey_cnt_ > 0)) {
            state_ = DO_LOOKUP;
          } else {
            state_ = FINISHED;
          }
        }
        break;
      }

      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("failed to do index lookup", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }

      case OUTPUT_ROWS: {
        count = 0;
        if (OB_FAIL(data_table_iter_->get_next_rows(count, capacity))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            if (count > 0) {
              lookup_row_cnt_ += count;
              get_next_rows = true;
            } else {
              if (OB_FAIL(check_index_lookup())) {
                LOG_WARN("failed to check table lookup", K(ret));
              } else {
                state_ = INDEX_SCAN;
              }
            }
          } else {
            LOG_WARN("failed to get next rows from data table", K(ret));
          }
        } else {
          lookup_row_cnt_ += count;
          get_next_rows = true;
        }
        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
    }
  } while (!get_next_rows && OB_SUCC(ret));

  return ret;
}

int ObDASLookupIter::build_lookup_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey_exprs_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(rowkey_exprs_), K(eval_ctx_));
  } else {
    int64_t rowkey_cnt = rowkey_exprs_->count();
    ObObj *obj_ptr = nullptr;
    void *buf = nullptr;
    if (OB_ISNULL(buf = iter_alloc_->alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(ret), K(rowkey_cnt));
    } else {
      obj_ptr = new (buf) ObObj[rowkey_cnt];
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      ObObj tmp_obj;
      ObExpr *expr = rowkey_exprs_->at(i);
      ObDatum &col_datum = expr->locate_expr_datum(*eval_ctx_);
      if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("failed to convert datum to obj", K(ret));
      } else if (OB_FAIL(ob_write_obj(*iter_alloc_, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("failed to deep copy rowkey", K(ret), K(tmp_obj));
      }
    }

    int64_t group_id = 0;
    if (OB_NOT_NULL(group_id_expr_)) {
      ObDatum &group_datum = group_id_expr_->locate_expr_datum(*eval_ctx_);
      OB_ASSERT(T_PSEUDO_GROUP_ID == group_id_expr_->type_);
      group_id = group_datum.get_int();
    }

    if (OB_SUCC(ret)) {
      ObRowkey row_key(obj_ptr, rowkey_cnt);
      if (OB_FAIL(range.build_range(lookup_ctdef_->ref_table_id_, row_key))) {
        LOG_WARN("failed to build lookup range", K(ret), K(lookup_ctdef_->ref_table_id_), K(row_key));
      } else {
        range.group_idx_ = group_id;
      }
      LOG_DEBUG("build lookup range", K(ret), K(row_key), K(range));
    }
  }

  return ret;
}

int ObDASLookupIter::build_trans_info_datum(const ObExpr *trans_info_expr, ObDatum *&datum_ptr)
{
  int ret = OB_SUCCESS;
  datum_ptr = nullptr;
  if (OB_ISNULL(trans_info_expr) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(trans_info_expr), K(eval_ctx_));
  } else {
    void *buf = nullptr;
    ObDatum &col_datum = trans_info_expr->locate_expr_datum(*eval_ctx_);
    int64_t pos = sizeof(ObDatum);
    int64_t len = sizeof(ObDatum) + col_datum.len_;
    if (OB_ISNULL(buf = iter_alloc_->alloc(len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(ret));
    } else if (FALSE_IT(datum_ptr = new (buf) ObDatum)) {
    } else if (OB_FAIL(datum_ptr->deep_copy(col_datum, static_cast<char*>(buf), len, pos))) {
      LOG_WARN("failed to deep copy datum", K(ret), K(pos), K(len));
    }
  }

  return ret;
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
    if (OB_FAIL(build_lookup_range(lookup_range))) {
      LOG_WARN("failed to build lookup range", K(ret));
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("failed to push back lookup range", K(ret));
    } else {
      scan_param.is_get_ = true;
    }
  }

  const ObExpr *trans_info_expr = lookup_ctdef_->trans_info_expr_;
  if (OB_SUCC(ret) && OB_NOT_NULL(trans_info_expr)) {
    void *buf = nullptr;
    ObDatum *datum_ptr = nullptr;
    if (OB_FAIL(build_trans_info_datum(trans_info_expr, datum_ptr))) {
      LOG_WARN("failed to build trans info datum", K(ret));
    } else if (OB_ISNULL(datum_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else if (OB_FAIL(das_scan_op->trans_info_array_.push_back(datum_ptr))) {
      LOG_WARN("failed to push back trans info array", K(ret), KPC(datum_ptr));
    }
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
  } else if (OB_FAIL(merge_iter->set_merge_status(merge_iter->get_merge_type()))) {
    LOG_WARN("failed to set merge status for das iter", K(ret));
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
            LOG_ERROR("dump GLobalIndexBack das task lookup range and trans info",
                K(row_num), KPC(datum),
                K(das_op->get_scan_param().key_ranges_.at(i)),
                K(das_op->get_tablet_id()));
          }
        } else {
          for (int64_t i = 0; i < das_op->get_scan_param().key_ranges_.count(); i++) {
            row_num++;
            LOG_ERROR("dump GLobalIndexBack das task lookup range",
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
        LOG_INFO("dump GLobalIndexBack das task trans info", K(i),
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
