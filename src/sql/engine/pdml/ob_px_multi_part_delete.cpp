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

#include "ob_px_multi_part_delete.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/ob_dml_param.h"
#include "storage/ob_partition_service.h"
#include "share/system_variable/ob_system_variable.h"
#include "lib/utility/utility.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::common::serialization;

OB_SERIALIZE_MEMBER((ObPxMultiPartDeleteInput, ObPxModifyInput));

OB_DEF_SERIALIZE(ObPxMultiPartDelete)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObPxMultiPartDelete, ObTableModify));
  OB_UNIS_ENCODE(row_desc_);
  OB_UNIS_ENCODE(table_desc_);
  OB_UNIS_ENCODE_ARRAY(delete_projector_, delete_projector_size_);
  OB_UNIS_ENCODE(with_barrier_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPxMultiPartDelete)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObPxMultiPartDelete, ObTableModify));
  OB_UNIS_DECODE(row_desc_);
  OB_UNIS_DECODE(table_desc_);
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(delete_projector_size_);
    if (delete_projector_size_ > 0) {
      if (OB_ISNULL(my_phy_plan_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("my plan is null", K(ret));
      } else if (OB_ISNULL(delete_projector_ = static_cast<int32_t*>(
                               my_phy_plan_->get_allocator().alloc(sizeof(int32_t) * delete_projector_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K_(delete_projector_size));
      } else {
        OB_UNIS_DECODE_ARRAY(delete_projector_, delete_projector_size_);
      }
    } else {
      delete_projector_ = NULL;
    }
  }
  OB_UNIS_DECODE(with_barrier_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxMultiPartDelete)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObPxMultiPartDelete, ObTableModify));
  OB_UNIS_ADD_LEN(row_desc_);
  OB_UNIS_ADD_LEN(table_desc_);
  OB_UNIS_ADD_LEN_ARRAY(delete_projector_, delete_projector_size_);
  OB_UNIS_ADD_LEN(with_barrier_);
  return len;
}

ObPxMultiPartDelete::ObPxMultiPartDelete(ObIAllocator& alloc)
    : ObTableModify(alloc),
      row_desc_(),
      table_desc_(),
      delete_projector_(NULL),
      delete_projector_size_(0),
      with_barrier_(false)
{}

ObPxMultiPartDelete::~ObPxMultiPartDelete()
{}

int ObPxMultiPartDelete::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObPxModifyInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  }
  return ret;
}

int ObPxMultiPartDelete::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartDeleteCtx* op_ctx = nullptr;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get pdml phy operator ctx", K(ret));
  } else if (!table_desc_.is_valid() || !row_desc_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or row desc is invalid", K(ret), K_(table_desc), K_(row_desc));
  } else {
    // Construct Table desc, initialize data driver
    op_ctx->row_iter_wrapper_.set_delete_projector(delete_projector_, delete_projector_size_);
    if (OB_FAIL(op_ctx->data_driver_.init(ctx.get_allocator(), table_desc_, this, this))) {
      LOG_WARN("failed to init the data driver of op ctx", K(ret));
    } else if (OB_FAIL(op_ctx->row_iter_wrapper_.alloc_delete_row())) {
      LOG_WARN("failed to alloc delete row", K(ret));
    } else if (with_barrier_) {
      ObPxModifyInput* input = GET_PHY_OP_INPUT(ObPxModifyInput, ctx, get_id());
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else {
        op_ctx->data_driver_.set_with_barrier(get_id(), input);
      }
    }
    LOG_TRACE("pdml delete op", K(ret), K_(table_desc), K_(row_desc), K(delete_projector_), K(delete_projector_size_));
  }
  return ret;
}

int ObPxMultiPartDelete::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartDeleteCtx* op_ctx = nullptr;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy operator ctx", K(ret), K(get_id()), K(get_name()));
  }
  // Therefore currently does not support pdml delete operator vomit row
  // Only then can delete return to row
  if (OB_SUCC(ret) && is_returning_) {
    // The current delete op needs to spit upward
    // Before spitting out, a projection operation is needed on the data
    if (OB_FAIL(op_ctx->data_driver_.get_next_row(ctx, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed get next row from data driver", K(ret));
      } else {
        LOG_TRACE("data driver has been iterated to end");
      }
    }
  } else if (OB_SUCC(ret)) {
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
  return ret;
}

int ObPxMultiPartDelete::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartDeleteCtx* op_ctx = nullptr;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObTableModify::inner_close(ctx)) {
    LOG_WARN("failed to cal table modify inner close", K(ret));
  } else {
    op_ctx->data_driver_.destroy();
  }
  return ret;
}

int ObPxMultiPartDelete::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartDeleteCtx* op_ctx = NULL;
  OZ(CREATE_PHY_OPERATOR_CTX(ObPxMultiPartDeleteCtx, ctx, get_id(), get_type(), op_ctx), get_type());
  CK(OB_NOT_NULL(op_ctx));
  OZ(init_cur_row(*op_ctx, true));
  return ret;
}

//////////// pdml data interface implementation: reader & writer ////////////

int ObPxMultiPartDelete::read_row(ObExecContext& ctx, const ObNewRow*& row, int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartDeleteCtx* op_ctx = nullptr;
  CK(OB_NOT_NULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartDeleteCtx, ctx, get_id())));
  // Read in data from child, if necessary (Update scene) can be cached in ctx,
  // This time spit out the old row, next time read_row spit out the new row
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else if (OB_FAIL(copy_cur_row_by_projector(op_ctx->get_cur_row(), row))) {
    LOG_WARN("fail to get current row", K(ret));
  } else if (OB_FAIL(calc_row_for_pdml(ctx, op_ctx->get_cur_row()))) {
    LOG_WARN("fail to calc row", K(ret));
  } else {
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
  return ret;
}

int ObPxMultiPartDelete::write_rows(ObExecContext& ctx, ObPartitionKey& pkey, ObPDMLRowIterator& dml_row_iter) const
{
  int ret = OB_SUCCESS;
  storage::ObDMLBaseParam dml_param;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* ps = NULL;
  ObPxMultiPartDeleteCtx* op_ctx = NULL;

  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get px multi part delete ctx failed", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_FAIL(fill_dml_base_param(index_tid_, *my_session, *my_phy_plan_, *plan_ctx, dml_param))) {
    LOG_WARN("failed to fill dml base param", K(ret));
  } else {
    op_ctx->row_iter_wrapper_.set_iterator(dml_row_iter);
    int64_t affected_rows = 0;
    if (OB_FAIL(ps->delete_rows(
            my_session->get_trans_desc(), dml_param, pkey, column_ids_, &op_ctx->row_iter_wrapper_, affected_rows))) {
      LOG_WARN("failed to write rows to storage layer",
          K(ret),
          K(is_returning_),
          K(index_tid_),
          K(dml_param),
          K(my_session->get_trans_desc()));
    } else {
      if (!is_pdml_index_maintain_) {
        plan_ctx->add_affected_rows(affected_rows);
        plan_ctx->add_row_deleted_count(affected_rows);
      }
      LOG_TRACE("pdml delete ok", K(pkey), K(is_pdml_index_maintain_), K(affected_rows));
    }
  }
  return ret;
}

ObDMLRowDesc& ObPxMultiPartDelete::get_dml_row_desc()
{
  return row_desc_;
}

ObDMLTableDesc& ObPxMultiPartDelete::get_dml_table_desc()
{
  return table_desc_;
}

int ObPxMultiPartDelete::fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session,
    const ObPhysicalPlan& my_phy_plan, const ObPhysicalPlanCtx& my_plan_ctx, storage::ObDMLBaseParam& dml_param) const
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
  }
  return ret;
}

/**ObPDMLRowIteratorWrapper**/
int ObPxMultiPartDelete::ObPDMLRowIteratorWrapper::alloc_delete_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_ctx_.alloc_row_cells(delete_projector_size_, delete_row_))) {
    LOG_WARN("fail to create project row", K(ret), K(delete_projector_size_));
  }
  return ret;
}

int ObPxMultiPartDelete::ObPDMLRowIteratorWrapper::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObNewRow* full_row = nullptr;
  if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(iter_->get_next_row(full_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else if (OB_FAIL(project_row(*full_row, delete_row_))) {
    LOG_WARN("fail project new old row", K(*full_row), K(ret));
  } else {
    row = &delete_row_;
    LOG_TRACE("project delete row", K(*full_row), K_(delete_row));
  }
  return ret;
}

int ObPxMultiPartDelete::ObPDMLRowIteratorWrapper::project_row(const ObNewRow& input_row, ObNewRow& output_row) const
{
  int ret = OB_SUCCESS;
  if (output_row.count_ != delete_projector_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row size unmatch, check cg code", K(ret), K(output_row), K(delete_projector_size_));
  } else {
    for (int i = 0; i < delete_projector_size_; ++i) {
      output_row.cells_[i] = input_row.cells_[delete_projector_[i]];
    }
  }
  return ret;
}

int ObPxMultiPartDelete::register_to_datahub(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (with_barrier_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      ObIAllocator& allocator = ctx.get_sqc_handler()->get_safe_allocator();
      void* buf = ctx.get_allocator().alloc(sizeof(ObBarrierWholeMsg::WholeMsgProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObBarrierWholeMsg::WholeMsgProvider* provider = new (buf) ObBarrierWholeMsg::WholeMsgProvider();
        ObSqcCtx& sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), *provider))) {
          LOG_WARN("fail add whole msg provider", K(ret));
        }
      }
    }
  }
  return ret;
}
