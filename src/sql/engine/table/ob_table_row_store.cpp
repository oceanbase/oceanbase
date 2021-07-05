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
#include "sql/engine/table/ob_table_row_store.h"

namespace oceanbase {
using namespace common;
namespace sql {
void ObTableRowStoreInput::set_deserialize_allocator(ObIAllocator* allocator)
{
  allocator_ = allocator;
  multi_row_store_.set_allocator(allocator);
}

int ObTableRowStoreInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  allocator_ = &ctx.get_allocator();
  // There may be multiple tasks of same plan, so we need to reset plan state.
  multi_row_store_.reset();
  ObIArray<ObTaskInfo::ObPartLoc>& part_locs = task_info.get_range_location().part_locs_;
  multi_row_store_.set_allocator(&ctx.get_allocator());
  if (OB_FAIL(multi_row_store_.init(part_locs.count()))) {
    LOG_WARN("allocate multi row store failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_locs.count(); ++i) {
    uint64_t value_ref_id = part_locs.at(i).value_ref_id_;
    if (value_ref_id == op.get_id()) {
      if (OB_FAIL(multi_row_store_.push_back(part_locs.at(i).row_store_))) {
        LOG_WARN("store partition row store failed", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableRowStoreInput)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(multi_row_store_.count());
  ARRAY_FOREACH(multi_row_store_, i)
  {
    if (OB_ISNULL(multi_row_store_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row store is null");
    }
    OB_UNIS_ENCODE(*multi_row_store_.at(i));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableRowStoreInput)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(multi_row_store_.count());
  ARRAY_FOREACH_NORET(multi_row_store_, i)
  {
    if (multi_row_store_.at(i) != NULL) {
      OB_UNIS_ADD_LEN(*multi_row_store_.at(i));
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObTableRowStoreInput)
{
  int ret = OB_SUCCESS;
  int64_t row_store_cnt = 0;
  OB_UNIS_DECODE(row_store_cnt);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(multi_row_store_.init(row_store_cnt))) {
      LOG_WARN("allocate multi row store failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_store_cnt; ++i) {
    void* ptr = NULL;
    if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObRowStore)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row store failed", K(sizeof(ObRowStore)));
    } else {
      ObRowStore* row_store = new (ptr) ObRowStore(*allocator_);
      OB_UNIS_DECODE(*row_store);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(multi_row_store_.push_back(row_store))) {
          LOG_WARN("store row_store failed", K(ret));
        }
      }
      if (OB_FAIL(ret) && row_store != NULL) {
        row_store->~ObRowStore();
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObTableRowStore, ObNoChildrenPhyOperator));

class ObTableRowStore::ObTableRowStoreCtx : public ObPhyOperatorCtx {
public:
  explicit ObTableRowStoreCtx(ObExecContext& ctx)
      : ObPhyOperatorCtx(ctx), row_store_it_(), row_store_idx_(OB_INVALID_INDEX)
  {}
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }

private:
  ObRowStore::Iterator row_store_it_;
  int64_t row_store_idx_;
  friend class ObTableRowStore;
};

int ObTableRowStore::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableRowStoreCtx* values_ctx = NULL;
  ObTableRowStoreInput* values_input = NULL;

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("fail to init operator context", K(ret));
  } else if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObTableRowStoreCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context");
  } else if (OB_ISNULL(values_input = GET_PHY_OP_INPUT(ObTableRowStoreInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator input");
  } else if (OB_UNLIKELY(values_input->multi_row_store_.empty()) || OB_ISNULL(values_input->multi_row_store_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi row store is invalid", K(ret), K_(values_input->multi_row_store));
  } else {
    values_ctx->row_store_idx_ = 0;
    values_ctx->row_store_it_ = values_input->multi_row_store_.at(0)->begin();
  }
  return ret;
}

// not used so far, may be use later
int ObTableRowStore::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableRowStoreCtx* values_ctx = NULL;
  ObTableRowStoreInput* values_input = NULL;
  if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan ObNoChildrenPhyOperator failed", K(ret));
  } else if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObTableRowStoreCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context");
  } else if (OB_ISNULL(values_input = GET_PHY_OP_INPUT(ObTableRowStoreInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator input");
  } else if (OB_UNLIKELY(values_input->multi_row_store_.empty()) || OB_ISNULL(values_input->multi_row_store_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi row store is invalid", K(ret), K_(values_input->multi_row_store));
  } else {
    values_ctx->row_store_idx_ = 0;
    values_ctx->row_store_it_ = values_input->multi_row_store_.at(0)->begin();
  }
  return ret;
}

int ObTableRowStore::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObTableRowStore::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableRowStoreCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
    LOG_WARN("create current row failed", K(ret), "#columns", get_column_count());
  }
  return ret;
}

int ObTableRowStore::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableRowStoreCtx* values_ctx = NULL;

  if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObTableRowStoreCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context");
  } else if (OB_FAIL(try_check_status(ctx))) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(values_ctx->row_store_it_.get_next_row(values_ctx->get_cur_row()))) {
    ObTableRowStoreInput* values_input = NULL;
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (OB_ISNULL(values_input = GET_PHY_OP_INPUT(ObTableRowStoreInput, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get physical operator input");
    } else if (values_ctx->row_store_idx_ < values_input->multi_row_store_.count() - 1) {
      // iterate next row_store
      ++values_ctx->row_store_idx_;
      ObIArray<ObRowStore*>& multi_row_store = values_input->multi_row_store_;
      if (OB_ISNULL(multi_row_store.at(values_ctx->row_store_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row store is null", K(multi_row_store), K(values_ctx->row_store_idx_));
      } else {
        values_ctx->row_store_it_ = multi_row_store.at(values_ctx->row_store_idx_)->begin();
        if (OB_FAIL(values_ctx->row_store_it_.get_next_row(values_ctx->get_cur_row())) &&
            OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &values_ctx->get_cur_row();
    LOG_DEBUG("get next row from multi row store", KPC(row));
  }
  return ret;
}

int ObTableRowStore::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTableRowStoreInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create table row store input", K(ret), "op_id", get_id(), "op_type", get_type());
  }
  UNUSED(input);
  return ret;
}

int64_t ObTableRowStore::to_string_kv(char* buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  int64_t pos = 0;
  return pos;
}
}  // namespace sql
}  // namespace oceanbase
