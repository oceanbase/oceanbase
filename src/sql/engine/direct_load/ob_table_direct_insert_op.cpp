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
#include "ob_table_direct_insert_op.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/cmd/ob_table_direct_insert_service.h"
#include "observer/table_load/ob_table_load_service.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/blocksstable/ob_datum_row_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::common::serialization;
using namespace oceanbase::observer;

namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER((ObTableDirectInsertOpInput, ObPxMultiPartModifyOpInput));

OB_SERIALIZE_MEMBER((ObTableDirectInsertSpec, ObTableModifySpec),
                    row_desc_,
                    ins_ctdef_);

//////////////////////ObTableDirectInsertOp///////////////////
ObTableDirectInsertOp::ObTableDirectInsertOp(
    ObExecContext &exec_ctx,
    const ObOpSpec &spec,
    ObOpInput *input)
  : ObTableModifyOp(exec_ctx, spec, input),
    ins_rtdef_(),
    allocator_("DirectInsertOp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    px_task_id_(0),
    ddl_task_id_(0),
    table_ctx_(nullptr),
    datum_row_(nullptr),
    px_writer_(nullptr),
    tablet_id_(),
    is_partitioned_table_(false)
{
}

int ObTableDirectInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (OB_FAIL(ObDMLService::init_ins_rtdef(dml_rtctx_,
                                                  ins_rtdef_,
                                                  MY_SPEC.ins_ctdef_,
                                                  trigger_clear_exprs_,
                                                  fk_checkers_))) {
    LOG_WARN("init insert rtdef failed", K(ret));
  } else if (OB_UNLIKELY(!MY_SPEC.row_desc_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or row desc is invalid", K(ret), K(MY_SPEC.row_desc_));
  } else if (OB_FAIL(init_px_writer())) {
    LOG_WARN("failed to init px writer", KR(ret));
  }
  LOG_TRACE("table direct insert op", KR(ret), K_(MY_SPEC.row_desc), K_(MY_SPEC.ins_ctdef),
      K(MY_SPEC.is_vectorized()), K_(MY_SPEC.use_rich_format), K_(is_partitioned_table),
      K_(px_task_id), K_(ddl_task_id));
  return ret;
}

int ObTableDirectInsertOp::inner_close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObPhysicalPlan *plan = GET_PHY_PLAN_CTX(ctx_)->get_phy_plan();
  int error_code = (static_cast<const ObTableDirectInsertOpInput *>(input_))->get_error_code();
  if (OB_NOT_NULL(px_writer_)) {
    if (OB_LIKELY(OB_SUCCESS == error_code) && OB_TMP_FAIL(px_writer_->close())) {
      LOG_WARN("failed to close px writer", KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    px_writer_->reset();
  }
  if (OB_TMP_FAIL(ObTableDirectInsertService::close_task(plan->get_append_table_id(),
                                                         px_task_id_,
                                                         ddl_task_id_,
                                                         table_ctx_,
                                                         error_code))) {
    LOG_WARN("failed to close table direct insert task", KR(tmp_ret),
        K(plan->get_append_table_id()), K(px_task_id_), K(ddl_task_id_), K(error_code));
    ret = COVER_SUCC(tmp_ret);
  }
  if (OB_TMP_FAIL(ObTableModifyOp::inner_close())) {
    LOG_WARN("failed to inner close table modify", KR(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

void ObTableDirectInsertOp::destroy()
{
  if (OB_NOT_NULL(px_writer_)) {
    px_writer_->~ObTableLoadStoreTransPXWriter();
    px_writer_ = nullptr;
  }
  if (OB_NOT_NULL(datum_row_)) {
    datum_row_->~ObDatumRow();
    datum_row_ = nullptr;
  }
  allocator_.reset();
  ObTableModifyOp::destroy();
}

int ObTableDirectInsertOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null", K(ret));
  } else if (MY_SPEC.is_returning_) {
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail get next row from child", K(ret));
      }
    } else if (OB_FAIL(next_row())) {
      LOG_WARN("failed to process next row", KR(ret));
    }
  } else {
    do {
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail get next row from child", K(ret));
        }
      } else if (OB_FAIL(next_row())) {
        LOG_WARN("failed to process next row", KR(ret));
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

int ObTableDirectInsertOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  return MY_SPEC.use_rich_format_ ? next_vector(max_row_cnt) : next_batch(max_row_cnt);
}

// 向量化2.0
int ObTableDirectInsertOp::next_vector(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = get_eval_ctx();
  while (OB_SUCC(ret) && !brs_.end_) {
    clear_evaluated_flag();
    const ObBatchRows *child_brs = nullptr;
    if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch", K(ret));
      } else {
        brs_.size_ = 0;
        brs_.end_ = true;
        ret = OB_SUCCESS;
      }
    } else {
      brs_.copy(child_brs);
    }

    if (OB_SUCC(ret) && !brs_.end_) {
      if (OB_FAIL(ObDMLService::process_insert_batch(
          MY_SPEC.ins_ctdef_, *this, MY_SPEC.use_rich_format_))) {
        LOG_WARN("failed to process insert batch", KR(ret), K_(MY_SPEC.ins_ctdef), K_(MY_SPEC.use_rich_format));
      } else {
        const ExprFixedArray &dml_expr_array = MY_SPEC.ins_ctdef_.new_row_;
        const ExprFixedArray &expr_array = child_->get_spec().output_;
        ObIVector *tablet_id_vector = is_partitioned_table_ ? expr_array.at(0)->get_vector(eval_ctx) : nullptr;
        ObArray<ObIVector *> vectors;
        int64_t affected_rows = 0;
        if (OB_FAIL(vectors.prepare_allocate(dml_expr_array.count()))) {
          LOG_WARN("fail to prepare allocate", KR(ret), K(dml_expr_array.count()));
        } else {
          for (int64_t i = 0; i < dml_expr_array.count(); ++i) {
            vectors.at(i) = dml_expr_array.at(i)->get_vector(eval_ctx);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(write_vectors(tablet_id_vector, vectors, brs_))) {
          LOG_WARN("failed to write vectors", KR(ret));
        }
      }
    }
  } // end while
  return ret;
}

// 向量化1.0
int ObTableDirectInsertOp::next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = get_eval_ctx();
  while (OB_SUCC(ret) && !brs_.end_) {
    clear_evaluated_flag();
    const ObBatchRows *child_brs = nullptr;
    if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch", K(ret));
      } else {
        brs_.size_ = 0;
        brs_.end_ = true;
        ret = OB_SUCCESS;
      }
    } else {
      brs_.copy(child_brs);
    }

    if (OB_SUCC(ret) && !brs_.end_) {
      if (OB_FAIL(ObDMLService::process_insert_batch(
          MY_SPEC.ins_ctdef_, *this, MY_SPEC.use_rich_format_))) {
        LOG_WARN("failed to process insert batch", KR(ret), K_(MY_SPEC.ins_ctdef), K_(MY_SPEC.use_rich_format));
      } else {
        const ExprFixedArray &dml_expr_array = MY_SPEC.ins_ctdef_.new_row_;
        const ExprFixedArray &expr_array = child_->get_spec().output_;
        ObDatumVector tablet_id_datum_vector;
        ObArray<ObDatumVector> datum_vectors;
        int64_t affected_rows = 0;
        if (is_partitioned_table_) {
          tablet_id_datum_vector = expr_array.at(0)->locate_expr_datumvector(eval_ctx);
        }
        if (OB_FAIL(datum_vectors.prepare_allocate(dml_expr_array.count()))) {
          LOG_WARN("fail to prepare allocate", KR(ret), K(dml_expr_array.count()));
        } else {
          for (int64_t i = 0; i < dml_expr_array.count(); ++i) {
            datum_vectors.at(i) = dml_expr_array.at(i)->locate_expr_datumvector(eval_ctx);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(write_batch(tablet_id_datum_vector, datum_vectors, brs_))) {
          LOG_WARN("failed to write batch", KR(ret));
        }
      }
    }
  } // end while
  return ret;
}

int ObTableDirectInsertOp::next_row()
{
  int ret = OB_SUCCESS;
  bool is_skipped = false;
  clear_evaluated_flag();
  if (OB_FAIL(ObDMLService::process_insert_row(MY_SPEC.ins_ctdef_, ins_rtdef_, *this, is_skipped))) {
    LOG_WARN("failed to process insert row", KR(ret), K(ins_rtdef_.cur_row_num_));
  } else if (!is_skipped) {
    ObEvalCtx &eval_ctx = get_eval_ctx();
    const ExprFixedArray &dml_expr_array = MY_SPEC.ins_ctdef_.new_row_;
    const ObTabletID *tablet_id_ptr = nullptr;
    ObTabletID tablet_id;
    if (is_partitioned_table_) {
      const ExprFixedArray &expr_array = child_->get_spec().output_;
      ObExpr *expr = expr_array.at(0);
      ObDatum &expr_datum = expr->locate_expr_datum(eval_ctx);
      tablet_id = expr_datum.get_int();
      tablet_id_ptr = &tablet_id;
    }
    if (OB_FAIL(write_row(tablet_id_ptr, dml_expr_array))) {
      LOG_WARN("failed to write row", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ins_rtdef_.cur_row_num_++;
  }
  return ret;
}

int ObTableDirectInsertOp::write_vectors(
    ObIVector *tablet_id_vector,
    const ObIArray<ObIVector *> &vectors,
    const ObBatchRows &batch_rows)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_FAIL(px_writer_->write_vector(tablet_id_vector, vectors, batch_rows, affected_rows))) {
    LOG_WARN("failed to write vector", KR(ret));
  } else {
    GET_PHY_PLAN_CTX(ctx_)->add_row_matched_count(affected_rows);
    GET_PHY_PLAN_CTX(ctx_)->add_affected_rows(affected_rows);
  }
  return ret;
}

int ObTableDirectInsertOp::write_batch(
    const ObDatumVector &tablet_id_datum_vector,
    const ObIArray<ObDatumVector> &datum_vectors,
    const ObBatchRows &batch_rows)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_FAIL(px_writer_->write_batch(tablet_id_datum_vector, datum_vectors, batch_rows, affected_rows))) {
    LOG_WARN("failed to write batch", KR(ret));
  } else {
    GET_PHY_PLAN_CTX(ctx_)->add_row_matched_count(affected_rows);
    GET_PHY_PLAN_CTX(ctx_)->add_affected_rows(affected_rows);
  }
  return ret;
}

int ObTableDirectInsertOp::write_row(
    const ObTabletID *tablet_id_ptr,
    const ExprFixedArray &expr_array)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObEvalCtx &eval_ctx = get_eval_ctx();
  if (OB_ISNULL(datum_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum row", KR(ret), KP(datum_row_));
  } else if (is_partitioned_table_ && OB_ISNULL(tablet_id_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id_expr should not be null if partitioned table", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < expr_array.count()); ++i) {
      ObExpr *expr = expr_array.at(i);
      ObDatum &expr_datum = expr->locate_expr_datum(eval_ctx);
      datum_row_->storage_datums_[i].shallow_copy_from_datum(expr_datum);
    }

    if (OB_SUCC(ret)) {
      if (is_partitioned_table_) {
        if (OB_FAIL(px_writer_->write_row(*tablet_id_ptr, *datum_row_))) {
          LOG_WARN("failed to write row", KR(ret), KPC(datum_row_));
        }
      } else if (OB_FAIL(px_writer_->write_row(tablet_id_, *datum_row_))) {
        LOG_WARN("failed to write row", KR(ret), K(tablet_id_), KPC(datum_row_));
      }
      if (OB_SUCC(ret)) {
        affected_rows++;
      }
    }
  } // end if

  if (OB_SUCC(ret)) {
    GET_PHY_PLAN_CTX(ctx_)->add_row_matched_count(affected_rows);
    GET_PHY_PLAN_CTX(ctx_)->add_affected_rows(affected_rows);
  }
  return ret;
}

int ObTableDirectInsertOp::init_px_writer()
{
  int ret = OB_SUCCESS;
  const ObPhysicalPlan *plan = GET_PHY_PLAN_CTX(ctx_)->get_phy_plan();
  const ObIArray<uint64_t> &column_ids = MY_SPEC.ins_ctdef_.das_ctdef_.column_ids_;
  px_task_id_ = ctx_.get_px_task_id() + 1;
  ddl_task_id_ = plan->get_ddl_task_id();
  is_partitioned_table_ = !(NO_PARTITION_ID_FLAG == MY_SPEC.row_desc_.get_part_id_index());
  if (OB_FAIL(ObTableDirectInsertService::open_task(
      plan->get_append_table_id(), px_task_id_, ddl_task_id_, table_ctx_))) {
    LOG_WARN("failed to open table direct insert task", KR(ret),
        K(plan->get_append_table_id()), K(px_task_id_), K(ddl_task_id_));
  } else if (!MY_SPEC.is_vectorized() && OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(
      allocator_, column_ids.count(), datum_row_))) {
    LOG_WARN("failed to create datum row", KR(ret), K(column_ids.count()));
  } else if (OB_ISNULL(px_writer_ = OB_NEWx(ObTableLoadStoreTransPXWriter, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new ObTableLoadStoreTransPXWriter", KR(ret));
  } else {
    ObTableLoadStore store(table_ctx_);
    table::ObTableLoadTransId trans_id;
    trans_id.segment_id_ = px_task_id_;
    trans_id.trans_gid_ = 1;
    if (OB_FAIL(store.init())) {
      LOG_WARN("failed to init store", KR(ret));
    } else if (OB_FAIL(store.px_get_trans_writer(trans_id, *px_writer_))) {
      LOG_WARN("failed to get trans writer", KR(ret), K(trans_id));
    } else if (!is_partitioned_table_) {
      ObDASTableLoc *table_loc = ins_rtdef_.das_rtdef_.table_loc_;
      if (OB_ISNULL(table_loc) || table_loc->get_tablet_locs().size() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert table location is invalid", K(ret), KPC(table_loc));
      } else {
        tablet_id_ = table_loc->get_first_tablet_loc()->tablet_id_;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(px_writer_->prepare_write(column_ids,
                                           MY_SPEC.is_vectorized(),
                                           MY_SPEC.use_rich_format_))) {
        LOG_WARN("failed to prepare write", KR(ret), K(column_ids),
                 K(MY_SPEC.is_vectorized()), K(MY_SPEC.use_rich_format_));
      }
    }
  }
  return ret;
}


} // namespace sql
} // namespace oceanbase
