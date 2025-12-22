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

#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "storage/ddl/ob_ddl_seq_generator.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "share/ob_heap_organized_table_util.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::common::serialization;


OB_SERIALIZE_MEMBER((ObPxMultiPartSSTableInsertOpInput, ObPxMultiPartModifyOpInput));
OB_SERIALIZE_MEMBER((ObPxMultiPartSSTableInsertVecOpInput, ObPxMultiPartSSTableInsertOpInput));

OB_SERIALIZE_MEMBER((ObPxMultiPartSSTableInsertSpec, ObPxMultiPartInsertSpec), flashback_query_expr_,
                     regenerate_heap_table_pk_);
OB_SERIALIZE_MEMBER((ObPxMultiPartSSTableInsertVecSpec, ObPxMultiPartSSTableInsertSpec));

int ObPxMultiPartSSTableInsertSpec::get_snapshot_version(ObEvalCtx &eval_ctx, int64_t &snapshot_version) const
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  snapshot_version = 0;
  if (OB_FAIL(flashback_query_expr_->eval(eval_ctx, datum))) {
    LOG_WARN("expr evaluate failed", K(ret));
  } else if (datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL value", K(ret));
  } else {
    snapshot_version = datum->get_int();
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_tablet_info_from_row(
    const ObExprPtrIArray &row,
    common::ObTabletID &tablet_id,
    storage::ObTabletSliceParam *tablet_slice_param)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  if (nullptr != tablet_slice_param) {
    tablet_slice_param->reset();
  }

  // 1. get tablet_id
  const int64_t part_id_idx = get_spec().row_desc_.get_part_id_index();
  if (NO_PARTITION_ID_FLAG == part_id_idx) {
    ObDASTableLoc *table_loc = ins_rtdef_.das_rtdef_.table_loc_;
    if (OB_ISNULL(table_loc) || table_loc->get_tablet_locs().size() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert table location is invalid", K(ret), KPC(table_loc));
    } else {
      tablet_id = table_loc->get_first_tablet_loc()->tablet_id_;
    }
  } else if (part_id_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, part_id_idx is not valid", K(ret), K(part_id_idx));
  } else if (row.count() > part_id_idx) {
    ObExpr *expr = row.at(part_id_idx);
    ObDatum &expr_datum = expr->locate_expr_datum(get_eval_ctx());
    tablet_id = expr_datum.get_int();
  }

  // 2. get slice param
  if (OB_SUCC(ret) && nullptr != tablet_slice_param) {
    bool found_slice_expr = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found_slice_expr && i < row.count(); ++i) {
      if (row.at(i)->type_ == ObItemType::T_PSEUDO_DDL_SLICE_ID) {
        ObDatum &expr_datum = row.at(i)->locate_expr_datum(get_eval_ctx());
        tablet_slice_param->slice_id_ = expr_datum.get_int();
        found_slice_expr = true;
      }
    }
    if (OB_SUCC(ret) && !found_slice_expr) {
      tablet_slice_param->slice_idx_ = ctx_.get_px_task_id();
    }
    if (OB_SUCC(ret) && tablet_slice_param->slice_idx_ >= ObTabletSliceParam::MAX_TABLET_SLICE_COUNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ddl_slice_id", K(ret), KPC(tablet_slice_param));
    }
  }

  return ret;
}

const ObPxMultiPartSSTableInsertSpec &ObPxMultiPartSSTableInsertOp::get_spec() const
{
  return static_cast<const ObPxMultiPartSSTableInsertSpec &>(spec_);
}

int ObPxMultiPartSSTableInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxMultiPartInsertOp::inner_open())) {
    LOG_WARN("inner open failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc handler is null", K(ret));
  } else {
    op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SSTABLE_INSERT_CG_ROW_COUNT;
    op_monitor_info_.otherstat_1_value_ = 0;
    op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::SSTABLE_INSERT_ROW_COUNT;
    op_monitor_info_.otherstat_2_value_ = 0;
    op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::DDL_TASK_ID;
    op_monitor_info_.otherstat_5_value_ = MY_SPEC.plan_->get_ddl_task_id();
    LOG_INFO("update table context", K(MY_SPEC.ins_ctdef_.das_ctdef_.table_id_), K(MY_SPEC.ins_ctdef_.das_ctdef_.index_tid_));
    if (OB_SUCC(ret)) {
      ddl_dag_ = ctx_.get_sqc_handler()->get_sub_coord().get_ddl_dag();
      if (OB_ISNULL(ddl_dag_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
      } else if (share::schema::is_vec_delta_buffer_type(ddl_dag_->get_ddl_table_schema().table_item_.index_type_)
          || share::schema::is_hybrid_vec_index_log_type(ddl_dag_->get_ddl_table_schema().table_item_.index_type_)
          || share::schema::is_vec_index_id_type(ddl_dag_->get_ddl_table_schema().table_item_.index_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector index type", K(ret), K(ddl_dag_->get_ddl_table_schema().table_item_.index_type_));
      } else if (OB_FAIL(check_need_idempotence())) {
        LOG_WARN("check need idempotence failed", K(ret));
      } else if (OB_FAIL(locate_exprs())) {
        LOG_WARN("locate exprs failed", K(ret));
      } else if (is_heap_plan() && OB_FAIL(heap_tablet_writer_map_.create(MAP_HASH_BUCKET_NUM, ObMemAttr(MTL_ID(), "tblt_writer_map")))) {
        LOG_WARN("init tablet writer map failed", K(ret));
      }
    }
  }
  return ret;
}

void ObPxMultiPartSSTableInsertOp::destroy()
{
  if (heap_tablet_writer_map_.created()) {
    TabletWriterMap::iterator iter = heap_tablet_writer_map_.begin();
    for (; iter != heap_tablet_writer_map_.end(); ++iter) {
      ObISliceWriter *slice_writer = iter->second;
      if (OB_NOT_NULL(slice_writer)) {
        slice_writer->~ObISliceWriter();
        allocator_.free(slice_writer);
      }
    }
  }
  heap_tablet_writer_map_.destroy();
  allocator_.reset();
}

int ObPxMultiPartSSTableInsertVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  UNUSED(max_row_cnt);
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else if (ddl_dag_->is_final_status() || is_all_partition_finished_) {
    brs_.end_ = true;
    brs_.size_ = 0;
  } else if (is_heap_plan()) {
    if (OB_FAIL(write_heap_slice_by_batch())) {
      LOG_WARN("heap tablet write row failed", K(ret));
    }
  } else {
    if (OB_FAIL(write_ordered_slice_by_batch())) {
      LOG_WARN("ordered tablet write row failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !brs_.end_) {
    if (OB_FAIL(finish_dag())) {
      LOG_WARN("finish dag failed", K(ret));
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else if (ddl_dag_->is_final_status() || is_all_partition_finished_) {
    ret = OB_ITER_END;
  } else if (is_heap_plan()) {
    if (OB_FAIL(write_heap_slice_by_row())) {
      LOG_WARN("heap tablet write row failed", K(ret));
    }
  } else {
    if (OB_FAIL(write_ordered_slice_by_row())) {
      LOG_WARN("ordered tablet write row failed", K(ret));
    }
    if (OB_SUCC(ret) && need_idempotent_table_autoinc_) {
      if (OB_FAIL(sync_table_level_autoinc_value())) {
        LOG_WARN("sync global autoinc value failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(finish_dag())) {
      LOG_WARN("finish dag failed", K(ret));
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::finish_dag()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else {
    // px thread should not exit here, otherwise the session finished and the ddl dag threads will stop
    if (OB_FAIL(ddl_dag_->set_px_finished())) {
      LOG_WARN("set px finished failed", K(ret));
    } else if (OB_FAIL(ddl_dag_->process())) {
      LOG_WARN("dag process failed", K(ret), K(ddl_dag_->get_dag_ret()), KPC(ddl_dag_));
    }

    if (OB_SUCC(ret)) {
      ret = ddl_dag_->get_dag_ret();
    } else if (ddl_dag_->is_dag_failed()) {
      int tmp_ret = ret;
      ret = ddl_dag_->get_dag_ret();
      LOG_WARN("dag failed, return first failed task's error code", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_next_row_from_child(ObInsertMonitor *insert_monitor)
{
  int ret = child_->get_next_row();
  if (OB_SUCC(ret) && nullptr != insert_monitor && nullptr != ddl_dag_) {
    insert_monitor->inserted_row_cnt_++;
    if (ddl_dag_->get_ddl_table_schema().table_item_.is_column_store_) {
      const int64_t cg_count = ddl_dag_->get_ddl_table_schema().storage_schema_->get_column_groups().count();
      insert_monitor->inserted_cg_row_cnt_ += cg_count;
    }
  }
  if (OB_ITER_END == ret) {
    is_all_partition_finished_ = true;
    FLOG_INFO("all partition iterate finished", KP(this));
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_next_batch_from_child(const int64_t max_batch_size, const ObBatchRows *&brs, ObInsertMonitor *insert_monitor)
{
  int ret = child_->get_next_batch(max_batch_size, brs);
  if (OB_SUCC(ret) && nullptr != insert_monitor && nullptr != ddl_dag_ && nullptr != brs) {
    insert_monitor->inserted_row_cnt_ += brs->size_;
    if (ddl_dag_->get_ddl_table_schema().table_item_.is_column_store_) {
      const int64_t cg_count = ddl_dag_->get_ddl_table_schema().storage_schema_->get_column_groups().count();
      insert_monitor->inserted_cg_row_cnt_ += brs->size_ * cg_count;
    }
  }
  if (OB_ITER_END == ret) {
    is_all_partition_finished_ = true;
    FLOG_INFO("all partition iterate finished", KP(this));
  }
  return ret;
}

bool ObPxMultiPartSSTableInsertOp::need_autoinc_by_row()
{
  return need_idempotent_table_autoinc_ || need_idempotent_doc_id_;
}

int ObPxMultiPartSSTableInsertOp::init_table_autoinc_param(const ObTabletID &tablet_id, const int64_t slice_idx, ObDDLAutoincParam &autoinc_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(slice_idx));
  } else {
    autoinc_param.need_autoinc_ = true;
    autoinc_param.autoinc_range_interval_ = rootserver::ObDDLSliceInfo::AUTOINC_RANGE_INTERVAL;
    autoinc_param.slice_count_ = ddl_dag_->get_total_slice_count();
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(ddl_dag_->get_tablet_context(tablet_id, tablet_context))) {
      LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
    } else {
      autoinc_param.slice_idx_ = tablet_context->table_slice_offset_ + slice_idx;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!autoinc_param.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("autoinc param is not valid", K(ret), K(autoinc_param));
    }
    LOG_TRACE("init table level autoinc param", K(autoinc_param));
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::init_tablet_autoinc_param(const ObTabletID &tablet_id, const int64_t slice_idx, ObDDLAutoincParam &autoinc_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(slice_idx));
  } else {
    autoinc_param.need_autoinc_ = true;
    autoinc_param.autoinc_range_interval_ = rootserver::ObDDLSliceInfo::AUTOINC_RANGE_INTERVAL;
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(ddl_dag_->get_tablet_context(tablet_id, tablet_context))) {
      LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
    } else {
      autoinc_param.slice_idx_ = slice_idx;
      autoinc_param.slice_count_ = tablet_context->slice_count_;
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::eval_current_row(const int64_t rowkey_column_count, blocksstable::ObDatumRow &current_row)
{
  int ret = OB_SUCCESS;
  const ObExprPtrIArray &exprs = get_spec().ins_ctdef_.new_row_;
  if (OB_UNLIKELY(rowkey_column_count <= 0 || !current_row.is_valid() || exprs.count() > current_row.get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey_column_count), K(current_row.get_capacity()), K(exprs.count()));
  } else {
    clear_evaluated_flag();
    ObEvalCtx &eval_ctx = get_eval_ctx();
    const int64_t extra_rowkey_column_count = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObDatum *datum = nullptr;
      const ObExpr *e = exprs.at(i);
      if (OB_ISNULL(e)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret), K(i));
      } else if (OB_FAIL(e->eval(eval_ctx, datum))) {
        LOG_WARN("evaluate expression failed", K(ret), K(i), KPC(e));
      } else {
        const int64_t store_position = i < rowkey_column_count ? i : i + extra_rowkey_column_count;
        current_row.storage_datums_[store_position].shallow_copy_from_datum(*datum);
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::eval_current_row(ObIArray<ObDatum *> &datums, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  datums.reuse();
  clear_evaluated_flag();
  const ObExprPtrIArray &exprs = get_spec().ins_ctdef_.new_row_;
  ObEvalCtx &eval_ctx = get_eval_ctx();
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObDatum *datum = nullptr;
    const ObExpr *e = exprs.at(i);
    if (OB_ISNULL(e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(e->eval(eval_ctx, datum))) {
      LOG_WARN("evaluate expression failed", K(ret), K(i), KPC(e));
    } else if (e->type_ == T_PSEUDO_HIDDEN_CLUSTERING_KEY) {
      ObDatum &datum = e->locate_datum_for_write(eval_ctx);
      if (OB_FAIL(share::ObHeapTableUtil::handle_hidden_clustering_key_column(eval_ctx.get_expr_res_alloc(), tablet_id, datum))) {
        LOG_WARN("set hidden clustering key column failed", K(ret), K(tablet_id), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(datums.push_back(datum))) {
        LOG_WARN("push back datum pointer failed", K(ret), KPC(datum));
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::eval_current_batch(ObIArray<ObIVector *> &vectors, const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  vectors.reuse();
  clear_evaluated_flag();
  const ObExprPtrIArray &exprs = get_spec().ins_ctdef_.new_row_;
  ObEvalCtx &eval_ctx = get_eval_ctx();
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    const ObExpr *e = exprs.at(i);
    if (OB_ISNULL(e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(e->eval_vector(eval_ctx, brs))) {
      LOG_WARN("evaluate expression failed", K(ret), K(i), KPC(e));
    } else if (e->type_ == T_PSEUDO_HIDDEN_CLUSTERING_KEY) {
      // Handle hidden clustering key column
      ObIVector *hidden_ck_vector = e->get_vector(eval_ctx);
      ObIVector *tablet_id_vector = nullptr;

      // Step 1: Get tablet_id_vector based on whether it's a partitioned table or not
      if (nullptr != tablet_id_expr_) {
        // Partitioned table: eval tablet_id_expr_ to get tablet_id_vector
        if (OB_FAIL(tablet_id_expr_->eval_vector(eval_ctx, brs))) {
          LOG_WARN("failed to eval tablet_id_expr vector", K(ret));
        } else {
          tablet_id_vector = tablet_id_expr_->get_vector(eval_ctx);
        }
      }

      // Step 2: Iterate through each row in the batch and set datum value
      // Only process active rows (skip filtered rows)
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < brs.size_; ++row_idx) {
        // Skip filtered rows according to skip_ bitmap
        if (!brs.all_rows_active_ && brs.skip_->at(row_idx)) {
          continue;  // Skip this row as it's been filtered out
        }

        // Get tablet_id for current row
        ObTabletID tablet_id;
        if (nullptr == tablet_id_expr_) {
          // Non-partitioned table: use non_partitioned_tablet_id_
          tablet_id = non_partitioned_tablet_id_;
        } else {
          // Partitioned table: get tablet_id from tablet_id_vector
          if (OB_ISNULL(tablet_id_vector)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet_id_vector is null", K(ret));
          } else {
            tablet_id = ObTabletID(tablet_id_vector->get_uint(row_idx));
          }
        }

        if (OB_SUCC(ret)) {
          // Get payload from hidden clustering key vector and construct datum
          ObDatum temp_datum;
          if (OB_FAIL(share::ObHeapTableUtil::handle_hidden_clustering_key_column(
                       allocator_, tablet_id, temp_datum))) {
            LOG_WARN("failed to handle hidden clustering key column", K(ret), K(tablet_id), K(row_idx));
          } else {
            // Set the updated datum back to the vector
            hidden_ck_vector->set_payload_shallow(row_idx, temp_datum.ptr_, temp_datum.len_);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObIVector *cur_vector = e->get_vector(eval_ctx);
      if (OB_FAIL(vectors.push_back(cur_vector))) {
        LOG_WARN("push back current vector failed", K(ret), K(i), KPC(cur_vector));
      }
    }
  }
  return ret;

}

int ObPxMultiPartSSTableInsertOp::locate_exprs()
{
  int ret = OB_SUCCESS;
  // init tablet id expr or non_partitioned_tablet_id_
  const ObExprPtrIArray &child_output_exprs = child_->get_spec().output_;
  const int64_t part_id_idx = get_spec().row_desc_.get_part_id_index();
  if (NO_PARTITION_ID_FLAG == part_id_idx) {
    ObDASTableLoc *table_loc = ins_rtdef_.das_rtdef_.table_loc_;
    if (OB_ISNULL(table_loc) || table_loc->get_tablet_locs().size() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert table location is invalid", K(ret), KPC(table_loc));
    } else {
      non_partitioned_tablet_id_ = table_loc->get_first_tablet_loc()->tablet_id_;
    }
  } else if (part_id_idx < 0 || part_id_idx >= child_output_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, part_id_idx is not valid", K(ret), K(part_id_idx), K(child_output_exprs.count()));
  } else {
    tablet_id_expr_ = child_output_exprs.at(part_id_idx);
  }

  if (OB_SUCC(ret)) {
    if (is_heap_plan()) {
      // init tablet_autoinc_expr_
      for (int64_t i = 0; OB_SUCC(ret) && i < child_output_exprs.count(); ++i) {
        if (child_output_exprs.at(i)->type_ == T_TABLET_AUTOINC_NEXTVAL) {
          tablet_autoinc_expr_ = child_output_exprs.at(i);
          break;
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(tablet_autoinc_expr_) && is_vec_gen_vid_) {
        tablet_autoinc_expr_ = child_output_exprs.at(child_output_exprs.count() - 1);
      }

      const ObExprPtrIArray &exprs = get_spec().ins_ctdef_.new_row_;
      bool is_found = false;
      for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < exprs.count(); ++i) {
        if (exprs.at(i) == tablet_autoinc_expr_) {
          tablet_autoinc_column_idx_ = i;
          is_found = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(tablet_autoinc_expr_) || !is_found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet autoinc expr not found", K(ret), KP(tablet_autoinc_expr_), K(tablet_autoinc_column_idx_), K(is_found));
        }
      }
    } else {
      // for iot table and idempotent ddl, init slice_info_expr_;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_output_exprs.count(); ++i) {
        if (child_output_exprs.at(i)->type_ == ObItemType::T_PSEUDO_DDL_SLICE_ID) {
          slice_info_expr_ = child_output_exprs.at(i);
          break;
        }
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::check_need_idempotence()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else {
    ObSqlCtx *sql_ctx = nullptr;
    const ObTableSchema *ddl_table_schema = nullptr;
    const ObTableSchema *data_table_schema = nullptr;
    if (OB_ISNULL(sql_ctx = ctx_.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, schema guard not be nullptr", K(ret));
    } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(MTL_ID(), MY_SPEC.plan_->get_ddl_table_id(), ddl_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(MY_SPEC.plan_->get_ddl_table_id()));
    } else if (OB_ISNULL(ddl_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret), K(MY_SPEC.plan_->get_ddl_table_id()));
    } else {
      is_vec_gen_vid_ = ddl_table_schema->is_vec_rowkey_vid_type();
      need_idempotent_tablet_autoinc_ = MY_SPEC.regenerate_heap_table_pk_ // ddl heap plan
                                        || (is_vec_gen_vid_ && ddl_dag_->get_ddl_task_param().is_offline_index_rebuild_); // generate vid for vector index in offline mode
      need_idempotent_table_autoinc_ = ddl_table_schema->get_autoinc_column_id() > 0
                                       && ddl_table_schema->get_autoinc_column_id() != OB_INVALID_ID
                                       && !is_incremental_direct_load(ddl_dag_->get_direct_load_type())
                                       && !MY_SPEC.regenerate_heap_table_pk_;
      if (ddl_table_schema->is_rowkey_doc_id()
          && ddl_dag_->get_ddl_task_param().is_offline_index_rebuild_
          && !is_incremental_direct_load(ddl_dag_->get_direct_load_type())) {
        if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(MTL_ID(), ddl_table_schema->get_data_table_id(), data_table_schema))) {
          LOG_WARN("get table schema failed", K(ret), K(ddl_table_schema->get_data_table_id()));
        } else if (OB_ISNULL(data_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is null", K(ret), K(ddl_table_schema->get_data_table_id()));
        } else {
          need_idempotent_doc_id_ = !data_table_schema->is_table_without_pk();
        }
      }
      LOG_TRACE("check need idempotent doc id or table autoinc", K(need_idempotent_doc_id_), K(need_idempotent_table_autoinc_), K(MY_SPEC.plan_->get_ddl_table_id()));
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::generate_tablet_active_rows(const ObIVector *tablet_id_vector,
    const ObBatchRows &brs, hash::ObHashMap<ObTabletID, ObHeapCsSliceWriter *, hash::NoPthreadDefendMode> &slice_writer_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == tablet_id_vector || !slice_writer_map.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(ret), KP(tablet_id_vector), K(slice_writer_map.created()));
  } else if (OB_FAIL(slice_writer_map.reuse())) {
    LOG_WARN("reuse slice writer map failed", K(ret));
  }
  ObTabletID tablet_id;
  ObISliceWriter *slice_writer = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
    if (!brs.all_rows_active_ && brs.skip_->at(i)) {
      continue;
    } else if (FALSE_IT(tablet_id = tablet_id_vector->get_int(i))) {
    } else if (OB_FAIL(get_or_create_heap_writer(tablet_id, true/*is_append_batch*/, slice_writer))) {
      LOG_WARN("get or create heap slice writer failed", K(ret));
    } else {
      ObHeapCsSliceWriter *heap_cs_slice_writer = static_cast<ObHeapCsSliceWriter *>(slice_writer);
      if (OB_ISNULL(heap_cs_slice_writer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg slice writer is null", K(ret));
      } else if (OB_FAIL(heap_cs_slice_writer->set_active_row(i))) {
        LOG_WARN("set active row failed", K(ret));
      } else if (OB_FAIL(slice_writer_map.set_refactored(tablet_id, heap_cs_slice_writer, 1/*overwrite*/))) {
        LOG_WARN("set slice writer into map failed", K(ret));
      }
    }
  }
  return ret;
}

// table autoinc not support batch interface, because its eval param is not vectorized
int ObPxMultiPartSSTableInsertOp::write_heap_slice_by_batch()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_) || OB_ISNULL(tablet_autoinc_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_), KP(tablet_autoinc_expr_));
  }
  const int64_t max_batch_size = get_spec().max_batch_size_;
  const ObBatchRows *brs = nullptr;
  ObEvalCtx &eval_ctx = get_eval_ctx();
  ObArray<ObIVector *> vectors;
  hash::ObHashMap<ObTabletID, ObHeapCsSliceWriter *, hash::NoPthreadDefendMode> slice_writer_map;
  int64_t unused_row_scan_cnt = 0;
  ObInsertMonitor insert_monitor(unused_row_scan_cnt, op_monitor_info_.otherstat_2_value_, op_monitor_info_.otherstat_1_value_);

  if (OB_SUCC(ret) && nullptr != tablet_id_expr_) {
    if (OB_FAIL(slice_writer_map.create(max_batch_size, ObMemAttr(MTL_ID(), "act_writer_map")))) {
      LOG_WARN("create slice writer map failed", K(ret));
    }
  }
  while (OB_SUCC(ret) && !is_all_partition_finished_) {
    if (OB_FAIL(get_next_batch_from_child(max_batch_size, brs, &insert_monitor))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed", K(ret));
      } else {
        is_all_partition_finished_ = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(brs->size_ <= 0)) {
      is_all_partition_finished_ = brs->end_;
    } else if (OB_FAIL(eval_current_batch(vectors, *brs))) {
      LOG_WARN("eval current batch failed", K(ret));
    } else {
      if (nullptr == tablet_id_expr_) { // non partition table
        ObISliceWriter *slice_writer = nullptr;
        if (OB_FAIL(get_or_create_heap_writer(non_partitioned_tablet_id_, true/*is_append_batch*/, slice_writer))) {
          LOG_WARN("get tablet slice writer failed", K(ret), K(non_partitioned_tablet_id_));
        } else {
          ObBatchSelector selector(*brs);
          if (OB_FAIL(slice_writer->append_current_batch(vectors, selector))) {
            LOG_WARN("append batch failed", K(ret));
          }
        }
      } else {
        if (OB_FAIL(tablet_id_expr_->eval_vector(eval_ctx, *brs))) {
          LOG_WARN("failed to eval vector", K(ret));
        } else {
          ObIVector *tablet_id_vector = tablet_id_expr_->get_vector(eval_ctx);
          if (OB_FAIL(generate_tablet_active_rows(tablet_id_vector, *brs, slice_writer_map))) {
            LOG_WARN("generate tablet active rows failed", K(ret), KPC(tablet_id_vector));
          }
        }
        hash::ObHashMap<ObTabletID, ObHeapCsSliceWriter *, hash::NoPthreadDefendMode>::iterator it = slice_writer_map.begin();
        for (; OB_SUCC(ret) && it != slice_writer_map.end(); ++it) {
          ObHeapCsSliceWriter *slice_writer = it->second;
          if (OB_ISNULL(slice_writer)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("slice writer is null", K(ret), KP(slice_writer));
          } else {
            const ObIArray<uint16_t> &active_array = slice_writer->get_active_array();
            ObBatchSelector selector(active_array.get_data(), active_array.count());
            if (OB_FAIL(slice_writer->append_current_batch(vectors, selector))) {
              LOG_WARN("append batch failed", K(ret));
            } else {
              slice_writer->reuse_active_array();
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && is_all_partition_finished_) {
    // close all slice writer
    TabletWriterMap::iterator iter = heap_tablet_writer_map_.begin();
    for (; OB_SUCC(ret) && iter != heap_tablet_writer_map_.end(); ++iter) {
      const ObTabletID &tablet_id = iter->first;
      ObISliceWriter *slice_writer = iter->second;
      if (OB_ISNULL(slice_writer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("slice writer is null", K(ret));
      } else if (OB_FAIL(slice_writer->close())) {
        LOG_WARN("close slice writer failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::write_heap_slice_by_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  }
  ObArray<ObDatum *> datums;
  int64_t unused_row_scan_cnt = 0;
  ObInsertMonitor insert_monitor(unused_row_scan_cnt, op_monitor_info_.otherstat_2_value_, op_monitor_info_.otherstat_1_value_);

  while (OB_SUCC(ret) && !is_all_partition_finished_) {
    ObTabletID tablet_id;
    ObISliceWriter *slice_writer = nullptr;
    if (OB_FAIL(get_next_row_from_child(&insert_monitor))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed", K(ret));
      } else {
        is_all_partition_finished_ = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(get_tablet_info_from_row(child_->get_spec().output_, tablet_id))) {
      LOG_WARN("get tablet id from row failed", K(ret), K(child_->get_spec().output_));
    } else if (OB_FAIL(eval_current_row(datums, tablet_id))) {
      LOG_WARN("eval current row failed", K(ret));
    } else if (OB_FAIL(get_or_create_heap_writer(tablet_id, false/*is_append_batch*/, slice_writer))) {
      LOG_WARN("get or create slice writer failed", K(ret), K(tablet_id));
    } else if (OB_FAIL(slice_writer->append_current_row(datums))) {
      LOG_WARN("append current row failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_all_partition_finished_) {
    // close all slice writer
    TabletWriterMap::iterator iter = heap_tablet_writer_map_.begin();
    for (; OB_SUCC(ret) && iter != heap_tablet_writer_map_.end(); ++iter) {
      const ObTabletID &tablet_id = iter->first;
      ObISliceWriter *slice_writer = iter->second;
      if (OB_ISNULL(slice_writer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("slice writer is null", K(ret));
      } else if (OB_FAIL(slice_writer->close())) {
        LOG_WARN("close slice writer failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_or_create_heap_writer(const ObTabletID &tablet_id, const bool is_append_batch, ObISliceWriter *&slice_writer)
{
  int ret = OB_SUCCESS;
  slice_writer = nullptr;
  if (OB_FAIL(heap_tablet_writer_map_.get_refactored(tablet_id, slice_writer))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get tablet writer failed", K(ret));
    } else {
      ret = OB_SUCCESS;
      const int64_t slice_idx = ctx_.get_px_task_id();
      const int64_t parallel_count = ctx_.get_sqc_handler()->get_sqc_ctx().get_task_count();
      ObWriteMacroParam write_param;
      if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id, slice_idx, -1/*cg_idx*/, ddl_dag_, 0/*max_batch_size*/, write_param))) {
        LOG_WARN("init write param failed", K(ret), K(tablet_id), K(slice_idx));
      } else if (ddl_dag_->get_ddl_table_schema().table_item_.is_column_store_) {
        const int64_t max_batch_size = is_append_batch ? get_spec().max_batch_size_ : 1;
        const bool direct_write_macro_block = false;
        if (OB_ISNULL(slice_writer = OB_NEWx(ObHeapCsSliceWriter, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for tablet writer failed", K(ret));
        } else if (OB_FAIL(static_cast<ObHeapCsSliceWriter *>(slice_writer)->init(write_param, parallel_count, tablet_autoinc_column_idx_, direct_write_macro_block, max_batch_size, need_idempotent_tablet_autoinc_))) {
          LOG_WARN("init tablet writer failed", K(ret), K(tablet_id), K(slice_idx), K(parallel_count), K(tablet_autoinc_column_idx_), K(max_batch_size));
        } else {
          LOG_TRACE("init heap cs slice writer", K(ret), KPC(slice_writer));
        }
      } else {
        if (OB_ISNULL(slice_writer = OB_NEWx(ObHeapRsSliceWriter, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for tablet writer failed", K(ret));
        } else if (OB_FAIL(static_cast<ObHeapRsSliceWriter *>(slice_writer)->init(write_param, parallel_count, tablet_autoinc_column_idx_, need_idempotent_tablet_autoinc_))) {
          LOG_WARN("init tablet writer failed", K(ret), K(tablet_id), K(slice_idx), K(parallel_count), K(tablet_autoinc_column_idx_));
        } else {
          LOG_TRACE("init heap rs slice writer", K(ret), KPC(slice_writer));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(heap_tablet_writer_map_.set_refactored(tablet_id, slice_writer))) {
          LOG_WARN("set tablet writer into map failed", K(ret), K(tablet_id), KPC(slice_writer));
        }
      }
      if (OB_FAIL(ret)) {
        if (nullptr != slice_writer) {
          slice_writer->~ObISliceWriter();
          allocator_.free(slice_writer);
          slice_writer = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::write_ordered_slice_by_batch()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_) || OB_ISNULL(slice_info_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_), KP(slice_info_expr_));
  }
  const int64_t max_batch_size = get_spec().max_batch_size_;
  const ObBatchRows *brs = nullptr;
  ObEvalCtx &eval_ctx = get_eval_ctx();

  ObArray<ObIVector *> vectors;
  ObISliceWriter *slice_writer = nullptr;
  ObTabletID tablet_id;
  int64_t slice_idx = -1;
  ObIVector *slice_info_vector = nullptr;
  ObIVector *tablet_id_vector = nullptr;
  bool need_update_tablet_range_count = true;
  int64_t unused_row_scan_cnt = 0;
  ObInsertMonitor insert_monitor(unused_row_scan_cnt, op_monitor_info_.otherstat_2_value_, op_monitor_info_.otherstat_1_value_);

  ObExecContext &exec_ctx = get_exec_ctx();
  bool is_vec_tablet_rebuild = GET_MY_SESSION(exec_ctx)->get_ddl_info().is_vec_tablet_rebuild();
  LOG_DEBUG("write_ordered_slice_by_batch", K(is_vec_tablet_rebuild),  K(GET_MY_SESSION(exec_ctx)->get_ddl_info()));

  while (OB_SUCC(ret) && !is_all_partition_finished_) {
    int64_t offset = 0;
    int64_t row_count = 0;
    if (OB_FAIL(get_next_batch_from_child(max_batch_size, brs, &insert_monitor))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed", K(ret));
      } else {
        is_all_partition_finished_ = true;
        ret = OB_SUCCESS;
      }
    } else if (FALSE_IT(ddl_dag_->set_vec_tablet_rebuild(is_vec_tablet_rebuild))) {
    } else if (OB_UNLIKELY(need_update_tablet_range_count) && OB_FAIL(ddl_dag_->update_tablet_range_count())) {
      LOG_WARN("update tablet range count failed", K(ret));
    } else if (FALSE_IT(need_update_tablet_range_count = false)) {
    } else if (OB_UNLIKELY(brs->size_ <= 0)) {
      is_all_partition_finished_ = brs->end_;
    } else if (OB_FAIL(eval_current_batch(vectors, *brs))) {
      LOG_WARN("eval current batch failed", K(ret));
    } else if (OB_FAIL(slice_info_expr_->eval_vector(eval_ctx, *brs))) {
      LOG_WARN("eval slice info expr failed", K(ret));
    }  else if (FALSE_IT(slice_info_vector = slice_info_expr_->get_vector(eval_ctx))) {
    } else if (nullptr != tablet_id_expr_) {
      if (OB_FAIL(tablet_id_expr_->eval_vector(eval_ctx, *brs))) {
        LOG_WARN("eval slice info expr failed", K(ret));
      } else {
        tablet_id_vector = tablet_id_expr_->get_vector(eval_ctx);
      }
    }
    while (OB_SUCC(ret) && offset < brs->size_) {
      if (OB_FAIL(get_continue_slice(tablet_id_vector, slice_info_vector, *brs, tablet_id, slice_idx, offset, row_count))) {
        LOG_WARN("get continue slice failed", K(ret));
      } else if (OB_UNLIKELY(offset >= brs->size_)) {
        // do nothing
      } else if (OB_FAIL(switch_slice_if_need(tablet_id, slice_idx, true/*is_append_batch*/, slice_writer))) {
        LOG_WARN("switch slice if need failed", K(ret), K(tablet_id), K(slice_idx));
      } else {
        ObBatchSelector selector(offset, row_count);
        if (OB_FAIL(slice_writer->append_current_batch(vectors, selector))) {
          LOG_WARN("append batch failed", K(ret));
        } else {
          offset += row_count;
        }
      }
    }
  }
  if (OB_SUCC(ret) && is_all_partition_finished_ && nullptr != slice_writer) {
    if (OB_FAIL(slice_writer->close())) {
      LOG_WARN("close slice writer failed", K(ret));
    }
  }
  // ignore ret
  if (nullptr != slice_writer) {
    slice_writer->~ObISliceWriter();
    ob_free(slice_writer);
    slice_writer = nullptr;
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::write_ordered_slice_by_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  }
  ObArray<ObDatum *> datums;
  ObTabletID tablet_id;
  ObTabletSliceParam slice_param;
  ObISliceWriter *slice_writer = nullptr;
  ObDDLAutoincParam autoinc_param;
  bool need_update_tablet_range_count = true;
  int64_t unused_row_scan_cnt = 0;
  ObInsertMonitor insert_monitor(unused_row_scan_cnt, op_monitor_info_.otherstat_2_value_, op_monitor_info_.otherstat_1_value_);

  ObExecContext &exec_ctx = get_exec_ctx();
  bool is_vec_tablet_rebuild = GET_MY_SESSION(exec_ctx)->get_ddl_info().is_vec_tablet_rebuild();
  LOG_DEBUG("write_ordered_slice_by_row", K(is_vec_tablet_rebuild),  K(GET_MY_SESSION(exec_ctx)->get_ddl_info()));

  while (OB_SUCC(ret) && !is_all_partition_finished_) {
    if (OB_FAIL(get_next_row_from_child(&insert_monitor))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed", K(ret));
      } else {
        is_all_partition_finished_ = true;
        ret = OB_SUCCESS;
      }
    } else if (FALSE_IT(ddl_dag_->set_vec_tablet_rebuild(is_vec_tablet_rebuild))) {
    } else if (OB_UNLIKELY(need_update_tablet_range_count) && OB_FAIL(ddl_dag_->update_tablet_range_count())) {
      LOG_WARN("update tablet range count failed", K(ret));
    } else if (FALSE_IT(need_update_tablet_range_count = false)) {
    } else if (OB_FAIL(get_tablet_info_from_row(child_->get_spec().output_, tablet_id, &slice_param))) {
      LOG_WARN("get tablet id from row failed", K(ret), K(child_->get_spec().output_));
    } else if (OB_FAIL(switch_slice_if_need(tablet_id, slice_param.slice_idx_, false/*is_append_batch*/, slice_writer, &autoinc_param))) {
      LOG_WARN("get or create slice writer failed", K(ret), K(tablet_id));
    } else if (autoinc_param.need_autoinc_ && FALSE_IT(
          get_eval_ctx().exec_ctx_.set_ddl_idempotent_autoinc_params(autoinc_param.slice_count_,
                                                                     autoinc_param.slice_idx_,
                                                                     slice_writer->get_row_count(),
                                                                     autoinc_param.autoinc_range_interval_))) {
    } else if (OB_FAIL(eval_current_row(datums, tablet_id))) {
      LOG_WARN("eval current row failed", K(ret));
    } else if (OB_FAIL(slice_writer->append_current_row(datums))) {
      LOG_WARN("append current row failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_all_partition_finished_ && nullptr != slice_writer) {
    if (need_idempotent_doc_id_ && OB_FAIL(sync_tablet_doc_id(slice_writer))) {
      LOG_WARN("sync tablet doc id failed", K(ret), KPC(slice_writer));
    } else if (OB_FAIL(slice_writer->close())) {
      LOG_WARN("close slice writer failed", K(ret));
    }
  }

  // ignore ret
  if (nullptr != slice_writer) {
    slice_writer->~ObISliceWriter();
    ob_free(slice_writer);
    slice_writer = nullptr;
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::switch_slice_if_need(
    const ObTabletID &tablet_id, const int64_t slice_idx, const bool is_append_batch,
    ObISliceWriter *&slice_writer, ObDDLAutoincParam *autoinc_param/* = nullptr */)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(slice_idx));
  } else if (OB_LIKELY(OB_NOT_NULL(slice_writer) && slice_writer->get_tablet_id() == tablet_id && slice_writer->get_slice_idx() == slice_idx)) {
    // do nothing
  } else {
    if (OB_NOT_NULL(slice_writer)) {
      if (need_idempotent_doc_id_ && OB_FAIL(sync_tablet_doc_id(slice_writer))) {
        LOG_WARN("sync tablet doc id failed", K(ret), KPC(slice_writer));
      } else if (OB_FAIL(slice_writer->close())) {
        LOG_WARN("close slice writer failed", K(ret));
      } else {
        slice_writer->~ObISliceWriter();
        ob_free(slice_writer);
        slice_writer = nullptr;
      }
    }

    ObWriteMacroParam write_param;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id, slice_idx, -1/*cg_idx*/, ddl_dag_, 0/*max_batch_size*/, write_param))) {
        LOG_WARN("init write param failed", K(ret), K(tablet_id), K(slice_idx));
    } else if (ddl_dag_->get_ddl_table_schema().table_item_.is_column_store_ || ddl_dag_->get_ddl_table_schema().table_item_.vec_dim_ > 0) {
      const bool direct_write_macro_block = false;
      if (OB_ISNULL(slice_writer = OB_NEW(ObCsSliceWriter, ObMemAttr(MTL_ID(), "cs_slice_writer")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for column store slice writer failed", K(ret));
      } else if (OB_FAIL(static_cast<ObCsSliceWriter *>(slice_writer)->init(write_param,
                                                                            direct_write_macro_block,
                                                                            is_append_batch,
                                                                            0/*max_batch_size(not used)*/))) {
        LOG_WARN("init column store slice writer failed", K(ret), K(write_param));
      }
    } else {
      if (OB_ISNULL(slice_writer = OB_NEW(ObRsSliceWriter, ObMemAttr(MTL_ID(), "rs_slice_writer")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for row store slice writer failed", K(ret));
      } else if (OB_FAIL(static_cast<ObRsSliceWriter *>(slice_writer)->init(write_param))) {
        LOG_WARN("init row store slice writer failed", K(ret), K(write_param));
      }
    }
    if (OB_SUCC(ret) && nullptr != autoinc_param) {
      if (need_idempotent_doc_id_) {
        if (OB_FAIL(init_tablet_autoinc_param(tablet_id, slice_idx, *autoinc_param))) {
          LOG_WARN("init tablet autoinc param failed", K(ret));
        }
      } else if (need_idempotent_table_autoinc_) {
        if (OB_FAIL(init_table_autoinc_param(tablet_id, slice_idx, *autoinc_param))) {
          LOG_WARN("init table autoinc param failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_continue_slice(
    const ObIVector *tablet_id_vector, const ObIVector *slice_info_vector_, const ObBatchRows &brs,
    ObTabletID &tablet_id, int64_t &slice_idx, int64_t &offset, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  slice_idx = -1;
  row_count = 0;
  for (int64_t i = offset; OB_SUCC(ret) && i < brs.size_; ++i) {
    if (brs.skip_->at(i)) {
      if (tablet_id.is_valid()) { // tablet id already set, but not continious now
        break;
      }
    } else {
      const ObTabletID cur_tablet_id = nullptr == tablet_id_vector ? non_partitioned_tablet_id_ : ObTabletID(tablet_id_vector->get_int(i));
      ObTabletSliceParam slice_param;
      slice_param.slice_id_ = slice_info_vector_->get_int(i);
      const int64_t cur_slice_idx = slice_param.slice_idx_;
      if (!tablet_id.is_valid()) { // the first valid row
        tablet_id = cur_tablet_id;
        slice_idx = cur_slice_idx;
        offset = i;
        row_count = 1;
      } else if (OB_LIKELY(cur_tablet_id == tablet_id && cur_slice_idx == slice_idx)) {
        ++row_count;
      } else {
        break;
      }
    }
  }
  if (OB_SUCC(ret) && !tablet_id.is_valid()) {
    offset = brs.size_;
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::get_data_tablet_id(const ObTabletID &tablet_id, ObTabletID &data_tablet_id)
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = nullptr;
  const ObTableSchema *ddl_table_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  data_tablet_id.reset();
  if (OB_ISNULL(sql_ctx = ctx_.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_) || OB_ISNULL(MY_SPEC.plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard, sql_ctx or plan is null", K(ret));
  } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(MTL_ID(), MY_SPEC.plan_->get_ddl_table_id(), ddl_table_schema))) {
    LOG_WARN("fail to get ddl table schema", K(ret), K(MY_SPEC.plan_->get_ddl_table_id()));
  } else if (OB_ISNULL(ddl_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl table schema is null", K(ret), K(MY_SPEC.plan_->get_ddl_table_id()));
  } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(MTL_ID(), ddl_table_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(ddl_table_schema->get_data_table_id()));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema is null", K(ret), K(ddl_table_schema->get_data_table_id()));
  } else if (!data_table_schema->is_partitioned_table()) {
    data_tablet_id = data_table_schema->get_tablet_id();
  } else {
    int64_t part_idx = OB_INVALID_INDEX;
    int64_t subpart_idx = OB_INVALID_INDEX;
    ObObjectID object_id;
    ObObjectID first_level_part_id;
    if (OB_FAIL(ddl_table_schema->get_part_idx_by_tablet(tablet_id, part_idx, subpart_idx))) {
      LOG_WARN("fail to get part idx by tablet", K(ret), K(tablet_id));
    } else if (OB_FAIL(data_table_schema->get_tablet_and_object_id_by_index(part_idx,
                                                                            subpart_idx,
                                                                            data_tablet_id,
                                                                            object_id,
                                                                            first_level_part_id))) {
      LOG_WARN("fail to get data tablet id", K(ret), K(part_idx), K(subpart_idx));
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::sync_tablet_doc_id(ObISliceWriter *slice_writer)
{
  int ret = OB_SUCCESS;
  ObDDLAutoincParam autoinc_param;
  ObDDLTabletContext *tablet_context = nullptr;
  if (OB_UNLIKELY(nullptr == ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else if (OB_UNLIKELY(nullptr == slice_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(slice_writer));
  } else {
    const ObTabletID tablet_id = slice_writer->get_tablet_id();
    const int64_t slice_idx = slice_writer->get_slice_idx();
    ObTabletID data_tablet_id;
    if (OB_FAIL(ddl_dag_->get_tablet_context(tablet_id, tablet_context))) {
      LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
    } else {
      const int64_t last_autoinc_val = ObDDLUtil::generate_idempotent_value(tablet_context->slice_count_,
                                                                            slice_idx,
                                                                            rootserver::ObDDLSliceInfo::AUTOINC_RANGE_INTERVAL,
                                                                            slice_writer->get_row_count());
      if (OB_FAIL(get_data_tablet_id(tablet_id, data_tablet_id))) {
        LOG_WARN("fail to get data tablet id", K(ret), K(tablet_id));
      } else if (OB_FAIL(ObDDLUtil::set_tablet_autoinc_seq(tablet_context->ls_id_, data_tablet_id, last_autoinc_val))) {
        LOG_WARN("set tablet autoinc seq failed", K(ret), KPC(slice_writer));
      }
    }
  }
  return ret;
}

int ObPxMultiPartSSTableInsertOp::sync_table_level_autoinc_value()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else {
    ObAutoincrementService &auto_service = ObAutoincrementService::get_instance();
    ObEvalCtx &eval_ctx = get_eval_ctx();
    ObPhysicalPlanCtx *plan_ctx = eval_ctx.exec_ctx_.get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical plan context is null", K(ret), K(plan_ctx));
    } else {
      ObIArray<AutoincParam> &autoinc_params = plan_ctx->get_autoinc_params();
      if (OB_FAIL(plan_ctx->sync_last_value_local())) {
        LOG_WARN("fail to sync last value local", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
        AutoincParam &autoinc_param = autoinc_params.at(i);
        autoinc_param.auto_increment_cache_size_ = 0; // set cache size to 0 to disable prefetch
        if (OB_FAIL(auto_service.sync_insert_value_global(autoinc_param))) {
          LOG_WARN("sync value failed", K(ret));
        }
      }
    }
  }
  return ret;
}
