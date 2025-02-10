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

#include "observer/table_load/ob_table_load_merge_phase_op.h"
#include "observer/table_load/ob_table_load_merge_data_table_op.h"
#include "observer/table_load/ob_table_load_merge_table_op.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

/**
 * ObTableLoadMergePhaseCtx
 */
ObTableLoadMergePhaseCtx::ObTableLoadMergePhaseCtx() : phase_(ObTableLoadMergerPhaseType::MAX_TYPE)
{
}

/**
 * ObTableLoadMergePhaseBaseOp
 */

ObTableLoadMergePhaseBaseOp::ObTableLoadMergePhaseBaseOp(ObTableLoadMergeOp *parent)
  : ObTableLoadMergeOp(parent), merge_phase_ctx_(nullptr)
{
}

ObTableLoadMergePhaseBaseOp::ObTableLoadMergePhaseBaseOp(ObTableLoadMergePhaseBaseOp *parent)
  : ObTableLoadMergeOp(parent), merge_phase_ctx_(parent->merge_phase_ctx_)
{
}

/**
 * ObTableLoadMergePhaseOp
 */
ObTableLoadMergePhaseOp::ObTableLoadMergePhaseOp(ObTableLoadMergeOp *parent)
  : ObTableLoadMergePhaseBaseOp(parent), status_(Status::NONE)
{
}

int ObTableLoadMergePhaseOp::acquire_child_op(ObTableLoadMergeOpType::Type child_op_type,
                                              ObIAllocator &allocator, ObTableLoadMergeOp *&child)
{
  int ret = OB_SUCCESS;
  child = nullptr;
  switch (child_op_type) {
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::DATA_TABLE,
                                         ObTableLoadMergeDataTableOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::DELETE_PHASE_DATA_TABLE,
                                         ObTableLoadMergeDeletePhaseDataTableOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::ACK_PHASE_DATA_TABLE,
                                         ObTableLoadMergeAckPhaseDataTableOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::INDEXES_TABLE,
                                         ObTableLoadMergeIndexesTableOp);
    OB_TABLE_LOAD_MERGE_UNEXPECTED_CHILD_OP_TYPE(child_op_type);
  }
  return ret;
}
/**
 * ObTableLoadMergeInsertPhaseOp
 */
ObTableLoadMergeInsertPhaseOp::ObTableLoadMergeInsertPhaseOp(ObTableLoadMergeOp *parent)
  : ObTableLoadMergePhaseOp(parent)

{
}

ObTableLoadMergeInsertPhaseOp::~ObTableLoadMergeInsertPhaseOp() {}

int ObTableLoadMergeInsertPhaseOp::inner_init()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("INSERT PHASE START");
  inner_phase_ctx_.phase_ = ObTableLoadMergerPhaseType::INSERT;
  if (ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_)) {
    if (store_ctx_->write_ctx_.is_fast_heap_table_) {
      inner_phase_ctx_.trans_param_ = store_ctx_->write_ctx_.trans_param_;
    } else if (OB_FAIL(store_ctx_->init_trans_param(inner_phase_ctx_.trans_param_))) {
      LOG_WARN("fail to init trans param", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    merge_phase_ctx_ = &inner_phase_ctx_;
  }
  return ret;
}

int ObTableLoadMergeInsertPhaseOp::inner_close()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("INSERT PHASE COMPLETED");
  return ret;
}

int ObTableLoadMergeInsertPhaseOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  if (is_parent_called && OB_FAIL(inner_init())) {
    LOG_WARN("fail to init", KR(ret));
  } else {
    ObTableLoadMergeOpType::Type child_op_type = ObTableLoadMergeOpType::INVALID_OP_TYPE;
    switch (status_) {
      case Status::NONE: {
        status_ = Status::DATA_MERGE;
        child_op_type = ObTableLoadMergeOpType::DATA_TABLE;
        break;
      }
      case Status::DATA_MERGE: {
        if (!store_ctx_->index_store_table_ctxs_.empty()) {
          status_ = Status::INDEX_MERGE;
          child_op_type = ObTableLoadMergeOpType::INDEXES_TABLE;
        } else {
          status_ = Status::COMPLETED;
        }
        break;
      }
      case Status::INDEX_MERGE: {
        status_ = Status::COMPLETED;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(status_));
        break;
      };
    }
    if (OB_SUCC(ret)) {
      if (Status::COMPLETED == status_) {
        if (OB_FAIL(inner_close())) {
          LOG_WARN("fail to close", KR(ret));
        } else if (OB_FAIL(switch_parent_op())) {
          LOG_WARN("fail to switch parent op", KR(ret));
        }
      } else if (OB_FAIL(switch_child_op(child_op_type))) {
        LOG_WARN("fail to switch child op", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadMergeDeletePhaseOp
 */
ObTableLoadMergeDeletePhaseOp::ObTableLoadMergeDeletePhaseOp(ObTableLoadMergeOp *parent)
  : ObTableLoadMergePhaseOp(parent)
{
}

ObTableLoadMergeDeletePhaseOp::~ObTableLoadMergeDeletePhaseOp() {}

int ObTableLoadMergeDeletePhaseOp::inner_init()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("DELETE PHASE START");
  inner_phase_ctx_.phase_ = ObTableLoadMergerPhaseType::DELETE;
  if (OB_FAIL(store_ctx_->init_trans_param(inner_phase_ctx_.trans_param_))) {
    LOG_WARN("fail to init trans param", KR(ret));
  } else {
    merge_phase_ctx_ = &inner_phase_ctx_;
  }
  return ret;
}

int ObTableLoadMergeDeletePhaseOp::inner_close()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("DELETE PHASE COMPLETED");
  return ret;
}

int ObTableLoadMergeDeletePhaseOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  if (is_parent_called && OB_FAIL(inner_init())) {
    LOG_WARN("fail to init", KR(ret));
  } else {
    ObTableLoadMergeOpType::Type child_op_type = ObTableLoadMergeOpType::INVALID_OP_TYPE;
    switch (status_) {
      case Status::NONE: {
        status_ = Status::DATA_MERGE;
        child_op_type = ObTableLoadMergeOpType::DELETE_PHASE_DATA_TABLE;
        break;
      }
      case Status::DATA_MERGE: {
        if (store_ctx_->index_store_table_ctxs_.size() > 0) {
          status_ = Status::INDEX_MERGE;
          child_op_type = ObTableLoadMergeOpType::INDEXES_TABLE;
        } else {
          status_ = Status::COMPLETED;
        }
        break;
      }
      case Status::INDEX_MERGE: {
        status_ = Status::COMPLETED;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(status_));
        break;
      };
    }
    if (OB_SUCC(ret)) {
      if (Status::COMPLETED == status_) {
        if (OB_FAIL(inner_close())) {
          LOG_WARN("fail to close", KR(ret));
        } else if (OB_FAIL(switch_parent_op())) {
          LOG_WARN("fail to switch parent op", KR(ret));
        }
      } else if (OB_FAIL(switch_child_op(child_op_type))) {
        LOG_WARN("fail to switch child op", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadMergeAckPhaseOp
 */

ObTableLoadMergeAckPhaseOp::ObTableLoadMergeAckPhaseOp(ObTableLoadMergeOp *parent)
  : ObTableLoadMergePhaseOp(parent)
{
}

ObTableLoadMergeAckPhaseOp::~ObTableLoadMergeAckPhaseOp() {}

int ObTableLoadMergeAckPhaseOp::inner_init()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ACK PHASE START");
  inner_phase_ctx_.phase_ = ObTableLoadMergerPhaseType::ACK;
  if (OB_FAIL(store_ctx_->init_trans_param(inner_phase_ctx_.trans_param_))) {
    LOG_WARN("fail to init trans param", KR(ret));
  } else {
    merge_phase_ctx_ = &inner_phase_ctx_;
  }
  return ret;
}

int ObTableLoadMergeAckPhaseOp::inner_close()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ACK PHASE COMPLETED");
  return ret;
}

int ObTableLoadMergeAckPhaseOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  if (is_parent_called && OB_FAIL(inner_init())) {
    LOG_WARN("fail to init", KR(ret));
  } else {
    ObTableLoadMergeOpType::Type child_op_type = ObTableLoadMergeOpType::INVALID_OP_TYPE;
    switch (status_) {
      case Status::NONE: {
        status_ = Status::DATA_MERGE;
        child_op_type = ObTableLoadMergeOpType::ACK_PHASE_DATA_TABLE;
        break;
      }
      case Status::DATA_MERGE: {
        status_ = Status::COMPLETED;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(status_));
        break;
      };
    }
    if (OB_SUCC(ret)) {
      if (Status::COMPLETED == status_) {
        if (OB_FAIL(inner_close())) {
          LOG_WARN("fail to close", KR(ret));
        } else if (OB_FAIL(switch_parent_op())) {
          LOG_WARN("fail to switch parent op", KR(ret));
        }
      } else if (OB_FAIL(switch_child_op(child_op_type))) {
        LOG_WARN("fail to switch child op", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase