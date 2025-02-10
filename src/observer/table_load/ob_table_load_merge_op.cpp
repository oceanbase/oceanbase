/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_merge_op.h"
#include "lib/allocator/ob_malloc.h"
#include "observer/table_load/ob_table_load_merge_phase_op.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

DEFINE_ENUM_FUNC(ObTableLoadMergeOpType::Type, type, OB_TABLE_LOAD_MERGE_OP_TYPE_DEF,
                 ObTableLoadMergeOpType::);

ObTableLoadMergeOp::ObTableLoadMergeOp(ObTableLoadTableCtx *ctx, ObTableLoadStoreCtx *store_ctx,
                                       ObIAllocator *allocator, ObTableLoadMergeOp *parent)
  : ctx_(ctx), store_ctx_(store_ctx), allocator_(allocator), parent_(parent)
{
  childs_.set_block_allocator(ModulePageAllocator(*allocator_));
}

ObTableLoadMergeOp::ObTableLoadMergeOp(ObTableLoadMergeOp *parent)
  : ObTableLoadMergeOp(parent->ctx_, parent->store_ctx_, parent->allocator_, parent)
{
}

ObTableLoadMergeOp::~ObTableLoadMergeOp()
{
  for (int64_t i = 0; i < childs_.count(); ++i) {
    ObTableLoadMergeOp *child = childs_.at(i);
    child->~ObTableLoadMergeOp();
  }
  childs_.reset();
}

int ObTableLoadMergeOp::switch_parent_op()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parent is null", KR(ret));
  } else if (OB_FAIL(parent_->switch_next_op(false /*is_parent_called*/))) {
    LOG_WARN("fail to switch next op", KR(ret));
  }
  return ret;
}

int ObTableLoadMergeOp::switch_child_op(ObTableLoadMergeOpType::Type child_op_type)
{
  int ret = OB_SUCCESS;
  ObTableLoadMergeOp *child = nullptr;
  if (OB_FAIL(acquire_child_op(child_op_type, *allocator_, child))) {
    LOG_WARN("fail to acquire child op", K(ret), K(child_op_type));
  } else if (OB_FAIL(childs_.push_back(child))) {
    LOG_WARN("fail to push back", K(ret));
    child->~ObTableLoadMergeOp();
    child = nullptr;
  } else if (OB_FAIL(child->switch_next_op(true /*is_parent_called*/))) {
    LOG_WARN("fail to switch next op", K(ret));
  }
  return ret;
}

void ObTableLoadMergeOp::stop()
{
  for (int64_t i = 0; i < childs_.count(); ++i) {
    ObTableLoadMergeOp *child = childs_.at(i);
    child->stop();
  }
}

/**
 * RootOp
 */

ObTableLoadMergeRootOp::ObTableLoadMergeRootOp(ObTableLoadStoreCtx *store_ctx)
  : ObTableLoadMergeOp(store_ctx->ctx_, store_ctx, &store_ctx->merge_op_allocator_, nullptr),
    status_(Status::NONE)
{
}

int ObTableLoadMergeRootOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  ObTableLoadMergeOpType::Type child_op_type = ObTableLoadMergeOpType::INVALID_OP_TYPE;
  switch (status_) {
    case Status::NONE:
      status_ = Status::INSERT_PHASE;
      child_op_type = ObTableLoadMergeOpType::INSERT_PHASE;
      break;
    case Status::INSERT_PHASE: {
      bool have_local_unique_index = false;
      for (int i = 0; i < store_ctx_->index_store_table_ctxs_.size(); i++) {
        if (store_ctx_->index_store_table_ctxs_.at(i)->schema_->is_local_unique_index()) {
          have_local_unique_index = true;
          break;
        }
      }
      if (have_local_unique_index) {
        status_ = Status::DELETE_PHASE;
        child_op_type = ObTableLoadMergeOpType::DELETE_PHASE;
      } else {
        status_ = Status::COMPLETED;
      }
      break;
    }
    case Status::DELETE_PHASE:
      status_ = Status::ACK_PHASE;
      child_op_type = ObTableLoadMergeOpType::ACK_PHASE;
      break;
    case Status::ACK_PHASE:
      status_ = Status::COMPLETED;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", KR(ret), K(status_));
      break;
  }
  if (OB_SUCC(ret)) {
    if (Status::COMPLETED == status_) {
      FLOG_INFO("LOAD MERGE COMPLETED");
      store_ctx_->set_status_merged();
    } else if (OB_FAIL(switch_child_op(child_op_type))) {
      LOG_WARN("fail to switch child op", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMergeRootOp::acquire_child_op(ObTableLoadMergeOpType::Type child_op_type,
                                             ObIAllocator &allocator, ObTableLoadMergeOp *&child)
{
  int ret = OB_SUCCESS;
  child = nullptr;
  switch (child_op_type) {
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::INSERT_PHASE,
                                         ObTableLoadMergeInsertPhaseOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::DELETE_PHASE,
                                         ObTableLoadMergeDeletePhaseOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::ACK_PHASE,
                                         ObTableLoadMergeAckPhaseOp);
    OB_TABLE_LOAD_MERGE_UNEXPECTED_CHILD_OP_TYPE(child_op_type);
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
