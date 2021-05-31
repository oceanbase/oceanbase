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

#include "ob_px_multi_part_update.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/ob_dml_param.h"
#include "storage/ob_partition_service.h"
#include "share/system_variable/ob_system_variable.h"
#include "lib/utility/utility.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::common::serialization;

OB_SERIALIZE_MEMBER((ObPxMultiPartUpdateInput, ObPxModifyInput));

OB_DEF_SERIALIZE(ObPxMultiPartUpdate)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObPxMultiPartUpdate, ObTableModify));
  OB_UNIS_ENCODE(updated_column_ids_);
  OB_UNIS_ENCODE(updated_column_infos_);
  OB_UNIS_ENCODE(row_desc_);
  OB_UNIS_ENCODE(table_desc_);
  OB_UNIS_ENCODE_ARRAY(updated_projector_, updated_projector_size_);
  OB_UNIS_ENCODE_ARRAY(old_projector_, old_projector_size_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPxMultiPartUpdate)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObPxMultiPartUpdate, ObTableModify));
  OB_UNIS_DECODE(updated_column_ids_);
  OB_UNIS_DECODE(updated_column_infos_);
  OB_UNIS_DECODE(row_desc_);
  OB_UNIS_DECODE(table_desc_);
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(updated_projector_size_);
    if (updated_projector_size_ > 0) {
      ObIAllocator& alloc = my_phy_plan_->get_allocator();
      if (OB_ISNULL(
              updated_projector_ = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * updated_projector_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K_(updated_projector_size));
      } else {
        OB_UNIS_DECODE_ARRAY(updated_projector_, updated_projector_size_);
      }
    } else {
      updated_projector_ = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(old_projector_size_);
    if (old_projector_size_ > 0) {
      ObIAllocator& alloc = my_phy_plan_->get_allocator();
      if (OB_ISNULL(old_projector_ = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * old_projector_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K_(old_projector_size));
      } else {
        OB_UNIS_DECODE_ARRAY(old_projector_, old_projector_size_);
      }
    } else {
      old_projector_ = NULL;
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxMultiPartUpdate)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObPxMultiPartUpdate, ObTableModify));
  OB_UNIS_ADD_LEN(updated_column_ids_);
  OB_UNIS_ADD_LEN(updated_column_infos_);
  OB_UNIS_ADD_LEN(row_desc_);
  OB_UNIS_ADD_LEN(table_desc_);
  OB_UNIS_ADD_LEN_ARRAY(updated_projector_, updated_projector_size_);
  OB_UNIS_ADD_LEN_ARRAY(old_projector_, old_projector_size_);
  return len;
}

ObPxMultiPartUpdate::ObPxMultiPartUpdate(ObIAllocator& alloc)
    : ObTableModify(alloc),
      updated_column_ids_(alloc),
      updated_column_infos_(alloc),
      row_desc_(),
      table_desc_(),
      old_projector_(nullptr),
      old_projector_size_(0),
      updated_projector_(nullptr),
      updated_projector_size_(0)
{}

ObPxMultiPartUpdate::~ObPxMultiPartUpdate()
{}

int ObPxMultiPartUpdate::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObPxModifyInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  }
  return ret;
}

int ObPxMultiPartUpdate::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartUpdateCtx* op_ctx = nullptr;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartUpdateCtx, ctx, get_id()))) {
    LOG_WARN("fail get multi part update ctx", K(ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (!table_desc_.is_valid() || !row_desc_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or row desc is invalid", K(ret), K_(table_desc), K_(row_desc));
  } else if (OB_FAIL(op_ctx->data_driver_.init(ctx.get_allocator(), table_desc_, this, this))) {
    LOG_WARN("fail init data driver", K(ret));
  } else if (FALSE_IT(op_ctx->row_iter_wrapper_.set_old_projector(old_projector_, old_projector_size_))) {
    LOG_WARN("fail set old projector for row_iter", K(ret));
  } else if (FALSE_IT(op_ctx->row_iter_wrapper_.set_updated_projector(updated_projector_, updated_projector_size_))) {
    LOG_WARN("fail set projector for row_iter", K(ret));
  } else if (FALSE_IT(op_ctx->row_iter_wrapper_.set_dml_row_checker(*this))) {
    // nop
  }
  LOG_TRACE("multi-part update open", K_(index_tid));
  return ret;
}

int ObPxMultiPartUpdate::on_process_new_row(ObExecContext& ctx, const common::ObNewRow& new_row) const
{
  int ret = OB_SUCCESS;
  OZ(check_row_null(ctx, new_row, column_infos_), new_row);
  return ret;
}

int ObPxMultiPartUpdate::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartUpdateCtx* op_ctx = nullptr;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (is_returning_) {
      if (OB_FAIL(op_ctx->data_driver_.get_next_row(ctx, row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed get next row from data driver", K(ret));
        }
      }
    } else {
      do {
        if (OB_FAIL(op_ctx->data_driver_.get_next_row(ctx, row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail get row from cache", K(ret));
          }
        } else {
          LOG_TRACE("multi-part update inner_get_next_row", K(*row));
        }
      } while (OB_SUCC(ret));
    }
  }
  return ret;
}

int ObPxMultiPartUpdate::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartUpdateCtx* op_ctx = nullptr;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    op_ctx->data_driver_.destroy();
  }
  LOG_TRACE("multi-part update close");
  return ret;
}

int ObPxMultiPartUpdate::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartUpdateCtx* op_ctx = NULL;
  OZ(CREATE_PHY_OPERATOR_CTX(ObPxMultiPartUpdateCtx, ctx, get_id(), get_type(), op_ctx), get_type());
  CK(OB_NOT_NULL(op_ctx));
  OZ(init_cur_row(*op_ctx, true));
  return ret;
}

//////////// pdml data interface implementation: reader & writer ////////////

int ObPxMultiPartUpdate::read_row(ObExecContext& ctx, const ObNewRow*& row, int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  ObPxMultiPartUpdateCtx* op_ctx = nullptr;
  ObPhysicalPlanCtx* plan_ctx = nullptr;
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
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
      } else {
        plan_ctx->add_row_matched_count(1);
      }
      LOG_TRACE("read row", K(*row), K(part_id));
    }
  }
  return ret;
}

int ObPxMultiPartUpdate::ObPDMLRowIteratorWrapper::init(ObPDMLRowIterator& iter)
{
  int ret = OB_SUCCESS;
  iter_ = &iter;
  if (OB_FAIL(op_ctx_.alloc_row_cells(old_projector_size_, old_row_))) {
    LOG_WARN("fail to create old project row", K(ret), K(old_projector_size_));
  } else if (OB_FAIL(op_ctx_.alloc_row_cells(updated_projector_size_, new_row_))) {
    LOG_WARN("fail to create new project row", K(ret), K(updated_projector_size_));
  }
  return ret;
}

int ObPxMultiPartUpdate::ObPDMLRowIteratorWrapper::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (has_got_old_row_) {
    LOG_DEBUG("get new row", K_(new_row));
    row = &new_row_;
    has_got_old_row_ = false;
    OB_ASSERT(row_checker_);
    if (OB_FAIL(row_checker_->on_process_new_row(op_ctx_.exec_ctx_, new_row_))) {
      LOG_WARN("fail process new row", K(ret));
    }
  } else {
    ObNewRow* full_row = nullptr;
    if (OB_ISNULL(iter_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(iter_->get_next_row(full_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail get next row from child", K(ret));
      }
    } else if (OB_FAIL(project_old_and_new_row(*full_row, old_row_, new_row_))) {
      LOG_WARN("fail project new old row", K(*full_row), K(ret));
    } else {
      row = &old_row_;
      has_got_old_row_ = true;
      LOG_TRACE("read update row", K(*full_row), K_(old_row), K_(new_row));
    }
  }
  return ret;
}

int ObPxMultiPartUpdate::ObPDMLRowIteratorWrapper::project_old_and_new_row(
    const ObNewRow& full_row, ObNewRow& old_row, ObNewRow& new_row) const
{
  int ret = OB_SUCCESS;
  if (new_row.count_ != updated_projector_size_ || old_row.count_ != old_projector_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row size unmatch, check cg code",
        K(new_row.count_),
        K(updated_projector_size_),
        K(old_row.count_),
        K(old_projector_size_),
        K(ret));
  } else {
    for (int i = 0; i < updated_projector_size_; ++i) {
      new_row.cells_[i] = full_row.cells_[updated_projector_[i]];
    }
    for (int i = 0; i < old_projector_size_; ++i) {
      old_row.cells_[i] = full_row.cells_[old_projector_[i]];
    }
  }
  return ret;
}
int ObPxMultiPartUpdate::write_rows(ObExecContext& ctx, ObPartitionKey& pkey, ObPDMLRowIterator& dml_row_iter) const
{
  int ret = OB_SUCCESS;
  storage::ObDMLBaseParam dml_param;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* ps = NULL;
  ObPxMultiPartUpdateCtx* op_ctx = nullptr;

  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPxMultiPartUpdateCtx, ctx, get_id()))) {
    LOG_WARN("fail get multi part update ctx", K(ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(fill_dml_base_param(index_tid_, *my_session, *my_phy_plan_, *plan_ctx, dml_param))) {
    LOG_WARN("fail fill dml base param", K(ret));
  } else if (OB_FAIL(op_ctx->row_iter_wrapper_.init(dml_row_iter))) {
    LOG_WARN("fail init row iter wrapper", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(ps->update_rows(my_session->get_trans_desc(),
            dml_param,
            pkey,
            column_ids_,
            updated_column_ids_,
            &op_ctx->row_iter_wrapper_,
            affected_rows))) {
      LOG_WARN("fail write rows to storage layer", K(ret));
    } else {
      if (!is_pdml_index_maintain_) {
        plan_ctx->add_affected_rows(affected_rows);
        plan_ctx->add_row_duplicated_count(affected_rows);
      }
      LOG_TRACE("update ok", K(pkey), K(is_pdml_index_maintain_), K(affected_rows));
    }
  }
  return ret;
}

int ObPxMultiPartUpdate::fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session,
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

int ObPxMultiPartUpdate::init_updated_column_count(common::ObIAllocator& allocator, int64_t count)
{
  UNUSED(allocator);
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(updated_column_infos_.prepare_allocate(count))) {
    SQL_ENG_LOG(WARN, "prepare allocate update column infos failed", K(ret), K(count));
  } else if (OB_FAIL(updated_column_ids_.prepare_allocate(count))) {
    SQL_ENG_LOG(WARN, "prepare allocate updated column ids failed", K(ret), K(count));
  }
  return ret;
}

int ObPxMultiPartUpdate::set_updated_column_info(
    int64_t array_index, uint64_t column_id, uint64_t project_index, bool auto_filled_timestamp)
{
  int ret = OB_SUCCESS;
  ColumnContent column;
  column.projector_index_ = project_index;
  column.auto_filled_timestamp_ = auto_filled_timestamp;
  CK(array_index >= 0 && array_index < updated_column_ids_.count());
  CK(array_index >= 0 && array_index < updated_column_infos_.count());
  if (OB_SUCC(ret)) {
    updated_column_ids_.at(array_index) = column_id;
    updated_column_infos_.at(array_index) = column;
  }
  return ret;
}

void ObPxMultiPartUpdate::set_updated_projector(int32_t* projector, int64_t projector_size)
{
  updated_projector_ = projector;
  updated_projector_size_ = projector_size;
}

void ObPxMultiPartUpdate::set_old_projector(int32_t* projector, int64_t projector_size)
{
  old_projector_ = projector;
  old_projector_size_ = projector_size;
}
