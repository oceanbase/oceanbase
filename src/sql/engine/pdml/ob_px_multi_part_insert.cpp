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
#include "sql/engine/pdml/ob_px_multi_part_insert.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/ob_dml_param.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
using namespace storage;
namespace sql {

OB_SERIALIZE_MEMBER((ObPxMultiPartInsertInput, ObPxModifyInput));

ObPxMultiPartInsert::ObPxMultiPartInsert(ObIAllocator& alloc)
    : ObTableModify(alloc),
      row_desc_(),
      table_desc_(),
      insert_projector_(NULL),
      insert_projector_size_(0),
      with_barrier_(false)
{}

ObPxMultiPartInsert::~ObPxMultiPartInsert()
{}

int ObPxMultiPartInsert::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObPxModifyInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  }
  return ret;
}

int ObPxMultiPartInsert::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;

  OZ(ObTableModify::inner_open(ctx));

  return ret;
}

int ObPxMultiPartInsert::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartInsertCtx* op_ctx = NULL;

  CK(table_desc_.is_valid());
  CK(row_desc_.is_valid());

  OZ(CREATE_PHY_OPERATOR_CTX(ObPxMultiPartInsertCtx, ctx, get_id(), get_type(), op_ctx));
  OZ(init_cur_row(*op_ctx, true));
  OZ(op_ctx->data_driver_.init(ctx.get_allocator(), table_desc_, this, this));
  OX(op_ctx->row_iter_wrapper_.set_insert_projector(insert_projector_, insert_projector_size_));
  OZ(op_ctx->row_iter_wrapper_.alloc_insert_row());
  if (OB_SUCC(ret) && with_barrier_) {
    ObPxModifyInput* input = GET_PHY_OP_INPUT(ObPxModifyInput, ctx, get_id());
    if (OB_ISNULL(input)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
    } else {
      op_ctx->data_driver_.set_with_barrier(get_id(), input);
    }
  }
  LOG_TRACE("check projector", "insert_projector", ObArrayWrap<int32_t>(insert_projector_, insert_projector_size_));

  return ret;
}

int ObPxMultiPartInsert::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartInsertCtx* op_ctx = nullptr;
  CK(OB_NOT_NULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartInsertCtx, ctx, get_id())));
  // ret = op_ctx->data_driver_.get_next_row(ctx, row);
  if (OB_SUCC(ret)) {
    if (is_returning_) {
      if (OB_FAIL(op_ctx->data_driver_.get_next_row(ctx, row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed get next row from data driver", K(ret));
        } else {
          LOG_TRACE("data driver has been iterated to end");
        }
      }
    } else {
      do {
        if (OB_FAIL(op_ctx->data_driver_.get_next_row(ctx, row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed get next row from data driver", K(ret));
          } else {
            LOG_TRACE("data driver has been iterated to end");
          }
        }
      } while (OB_SUCC(ret));
    }
  }
  LOG_TRACE("returned row", KPC(row));
  return ret;
}

int ObPxMultiPartInsert::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartInsertCtx* op_ctx = nullptr;
  CK(OB_NOT_NULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartInsertCtx, ctx, get_id())));
  OZ(ObTableModify::inner_close(ctx));
  OX(op_ctx->data_driver_.destroy());
  return ret;
}

int ObPxMultiPartInsert::fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session,
    const ObPhysicalPlan& my_phy_plan, const ObPhysicalPlanCtx& my_plan_ctx, ObDMLBaseParam& dml_param) const
{
  int ret = OB_SUCCESS;
  int64_t schema_version = 0;
  int64_t binlog_row_image = share::ObBinlogRowImage::FULL;
  if (OB_FAIL(my_phy_plan.get_base_table_version(index_tid, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(my_session.get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    dml_param.schema_version_ = schema_version;
    dml_param.is_total_quantity_log_ = (share::ObBinlogRowImage::FULL == binlog_row_image);
    dml_param.timeout_ = my_plan_ctx.get_ps_timeout_timestamp();
    dml_param.sql_mode_ = my_session.get_sql_mode();
    dml_param.tz_info_ = TZ_INFO(&my_session);
    dml_param.tenant_schema_version_ = my_plan_ctx.get_tenant_schema_version();
    dml_param.is_ignore_ = is_ignore_;
    dml_param.only_data_table_ = false;
    dml_param.virtual_columns_.reset();
    DLIST_FOREACH(node, get_virtual_column_exprs())
    {
      const ObColumnExpression* expr = static_cast<const ObColumnExpression*>(node);
      OZ(dml_param.virtual_columns_.push_back(expr));
    }
  }
  return ret;
}

int ObPxMultiPartInsert::process_row(
    ObExecContext& ctx, ObPxMultiPartInsertCtx* insert_ctx, const ObNewRow*& insert_row) const
{
  int ret = OB_SUCCESS;
  bool is_filtered = false;
  OZ(check_row_null(ctx, *insert_row, column_infos_), *insert_row);
  OZ(ObPhyOperator::filter_row_for_check_cst(insert_ctx->expr_ctx_, *insert_row, check_constraint_exprs_, is_filtered));
  OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
  return ret;
}

//////////// pdml data interface implementation: reader & writer ////////////

int ObPxMultiPartInsert::read_row(ObExecContext& ctx, const ObNewRow*& row, int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartInsertCtx* op_ctx = nullptr;
  CK(OB_NOT_NULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartInsertCtx, ctx, get_id())));
  CK(OB_NOT_NULL(child_op_));
  if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else if (OB_FAIL(copy_cur_row_by_projector(op_ctx->get_cur_row(), row))) {
    LOG_WARN("fail to get current row", K(ret));
  } else if (OB_FAIL(calc_row_for_pdml(ctx, op_ctx->get_cur_row()))) {
    LOG_WARN("fail to calc row", K(ret));
  } else {
    // real_idx got in CG
    const int64_t real_idx = row_desc_.get_part_id_index();
    if (NO_PARTITION_ID_FLAG == real_idx) {
      part_id = 0;
    } else if (real_idx >= row->count_ || real_idx < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real_idx is invalid", "count", row->count_, K(real_idx), K(ret));
    } else if (OB_FAIL(row->cells_[real_idx].get_int(part_id))) {
      LOG_WARN("fail get part id", K(ret), K(real_idx), "obj", row->cells_[real_idx], K(*row));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_row(ctx, op_ctx, row))) {
      LOG_WARN("fail process row", K(ret));
    }
  }
  return ret;
}

int ObPxMultiPartInsert::write_rows(ObExecContext& ctx, ObPartitionKey& pkey, ObPDMLRowIterator& iterator) const
{
  int ret = OB_SUCCESS;
  ObDMLBaseParam dml_param;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObPxMultiPartInsertCtx* op_ctx = NULL;

  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)));
  CK(OB_NOT_NULL(my_session = GET_MY_SESSION(ctx)));
  CK(OB_NOT_NULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx)));
  CK(OB_NOT_NULL(partition_service = executor_ctx->get_partition_service()));
  CK(OB_NOT_NULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartInsertCtx, ctx, get_id())));

  OZ(fill_dml_base_param(index_tid_, *my_session, *my_phy_plan_, *plan_ctx, dml_param));

  OZ(set_autoinc_param_pkey(ctx, pkey));

  OX(op_ctx->row_iter_wrapper_.set_iterator(iterator));

  if (OB_SUCC(ret)) {
    int64_t affected_rows = 0;
    if (OB_FAIL(partition_service->insert_rows(
            my_session->get_trans_desc(), dml_param, pkey, column_ids_, &op_ctx->row_iter_wrapper_, affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("insert row to partition storage failed", K(ret));
      }
    } else {
      if (!is_pdml_index_maintain_) {
        plan_ctx->add_affected_rows(affected_rows);
        plan_ctx->add_row_matched_count(affected_rows);
      }
      LOG_DEBUG("insert ok", K(pkey), K(is_pdml_index_maintain_), K(affected_rows));
    }
  }

  return ret;
}

int ObPxMultiPartInsert::ObPDMLRowIteratorWrapper::alloc_insert_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_ctx_.alloc_row_cells(insert_projector_size_, insert_row_))) {
    LOG_WARN("fail to create project row", K(ret), K(insert_projector_size_));
  }
  return ret;
}

int ObPxMultiPartInsert::ObPDMLRowIteratorWrapper::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObNewRow* full_row = nullptr;
  if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(iter_->get_next_row(full_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else if (OB_FAIL(project_row(*full_row, insert_row_))) {
    LOG_WARN("fail project new old row", K(*full_row), K(ret));
  } else {
    // todo OZ (validate_virtual_column(op_ctx->expr_ctx_, insert_row, 0));
    row = &insert_row_;
    LOG_TRACE("project insert row", K(*full_row), K_(insert_row));
  }
  return ret;
}

int ObPxMultiPartInsert::ObPDMLRowIteratorWrapper::project_row(const ObNewRow& input_row, ObNewRow& output_row) const
{
  int ret = OB_SUCCESS;
  if (output_row.count_ != insert_projector_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row size unmatch, check cg code", K(ret), K(output_row), K(insert_projector_size_));
  } else {
    for (int i = 0; i < insert_projector_size_; ++i) {
      output_row.cells_[i] = input_row.cells_[insert_projector_[i]];
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPxMultiPartInsert)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObPxMultiPartInsert, ObTableModify));
  OB_UNIS_ENCODE(row_desc_);
  OB_UNIS_ENCODE(table_desc_);
  OB_UNIS_ENCODE_ARRAY(insert_projector_, insert_projector_size_);
  OB_UNIS_ENCODE(with_barrier_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPxMultiPartInsert)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObPxMultiPartInsert, ObTableModify));
  OB_UNIS_DECODE(row_desc_);
  OB_UNIS_DECODE(table_desc_);
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(insert_projector_size_);
    if (insert_projector_size_ > 0) {
      if (OB_ISNULL(my_phy_plan_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my plan is null", K(ret));
      } else if (OB_ISNULL(insert_projector_ = static_cast<int32_t*>(
                               my_phy_plan_->get_allocator().alloc(sizeof(int32_t) * insert_projector_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K_(insert_projector_size));
      } else {
        OB_UNIS_DECODE_ARRAY(insert_projector_, insert_projector_size_);
      }
    } else {
      insert_projector_ = NULL;
    }
  }
  OB_UNIS_DECODE(with_barrier_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxMultiPartInsert)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObPxMultiPartInsert, ObTableModify));
  OB_UNIS_ADD_LEN(row_desc_);
  OB_UNIS_ADD_LEN(table_desc_);
  OB_UNIS_ADD_LEN_ARRAY(insert_projector_, insert_projector_size_);
  OB_UNIS_ADD_LEN(with_barrier_);
  return len;
}

}  // namespace sql
}  // namespace oceanbase
