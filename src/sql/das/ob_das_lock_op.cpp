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
#include "sql/das/ob_das_lock_op.h"
#include "share/ob_scanner.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "storage/tx_storage/ob_access_service.h"
namespace oceanbase
{
namespace common
{
namespace serialization
{
template <>
struct EnumEncoder<false, const sql::ObDASLockCtDef *> : sql::DASCtEncoder<sql::ObDASLockCtDef>
{
};

template <>
struct EnumEncoder<false, sql::ObDASLockRtDef *> : sql::DASRtEncoder<sql::ObDASLockRtDef>
{
};
} // end namespace serialization
} // end namespace common

using namespace common;
using namespace storage;
namespace sql
{
ObDASLockOp::ObDASLockOp(ObIAllocator &op_alloc)
  : ObIDASTaskOp(op_alloc),
    lock_ctdef_(nullptr),
    lock_rtdef_(nullptr),
    lock_buffer_(),
    affected_rows_(0)
{
}

int ObDASLockOp::open_op()
{
  int ret = OB_SUCCESS;
  ObDMLBaseParam dml_param;
  int64_t affected_rows;

  ObDASDMLIterator dml_iter(lock_ctdef_, lock_buffer_, op_alloc_);
  ObAccessService *as = MTL(ObAccessService *);
  storage::ObStoreCtxGuard store_ctx_guard;

  if (OB_FAIL(as->get_write_store_ctx_guard(ls_id_,
                                            lock_rtdef_->timeout_ts_,
                                            *trans_desc_,
                                            *snapshot_,
                                            write_branch_id_,
                                            store_ctx_guard))) {
    LOG_WARN("fail to get_write_access_tx_ctx_guard", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDMLService::init_dml_param(
      *lock_ctdef_, *lock_rtdef_, *snapshot_, write_branch_id_, op_alloc_, store_ctx_guard, dml_param))) {
    LOG_WARN("init dml param failed", K(ret));
  } else if (OB_FAIL(as->lock_rows(ls_id_,
                                   tablet_id_,
                                   *trans_desc_,
                                   dml_param,
                                   lock_rtdef_->for_upd_wait_time_,
                                   lock_ctdef_->lock_flag_,
                                   &dml_iter,
                                   affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("lock row to partition storage failed", K(ret));
    }
  } else {
    lock_rtdef_->affected_rows_ += affected_rows;
    affected_rows_ = affected_rows;
  }
  return ret;
}

int ObDASLockOp::release_op()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObDASLockOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASLockResult));
  CK(task_id_ == task_result->get_task_id());
#endif
  if (OB_SUCC(ret)) {
    ObDASLockResult *lock_result = static_cast<ObDASLockResult*>(task_result);
    lock_rtdef_->affected_rows_ += lock_result->get_affected_rows();
  }
  return ret;
}

int ObDASLockOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  UNUSED(memory_limit);
#if !defined(NDEBUG)
  CK(typeid(task_result) == typeid(ObDASLockResult));
#endif
  if (OB_SUCC(ret)) {
    ObDASLockResult &lock_result = static_cast<ObDASLockResult&>(task_result);
    lock_result.set_affected_rows(affected_rows_);
    has_more = false;
  }
  return ret;
}

int ObDASLockOp::init_task_info(uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (!lock_buffer_.is_inited()
      && OB_FAIL(lock_buffer_.init(CURRENT_CONTEXT->get_allocator(),
                                   row_extend_size,
                                   MTL_ID(),
                                   "DASLockBuffer"))) {
    LOG_WARN("init lock buffer failed", K(ret));
  }
  return ret;
}

int ObDASLockOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  if (remote_info != nullptr) {
      //DAS lock is executed remotely
      trans_desc_ = remote_info->trans_desc_;
      snapshot_ = &remote_info->snapshot_;
    }
  return ret;
}

int ObDASLockOp::write_row(const ExprFixedArray &row,
                           ObEvalCtx &eval_ctx,
                           ObChunkDatumStore::StoredRow *&stored_row,
                           bool &buffer_full)
{
  int ret = OB_SUCCESS;
  bool added = false;
  buffer_full = false;
  if (!lock_buffer_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer not inited", K(ret));
  } else if (OB_FAIL(lock_buffer_.try_add_row(row, &eval_ctx, das::OB_DAS_MAX_PACKET_SIZE, stored_row, added, true))) {
    LOG_WARN("try add row to lock buffer failed", K(ret), K(row), K(lock_buffer_));
  } else if (!added) {
    buffer_full = true;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASLockOp, ObIDASTaskOp),
                    lock_ctdef_,
                    lock_rtdef_,
                    lock_buffer_);

ObDASLockResult::ObDASLockResult()
  : ObIDASTaskResult(),
    affected_rows_(0)
{
}

ObDASLockResult::~ObDASLockResult()
{
}

int ObDASLockResult::init(const ObIDASTaskOp &op, common::ObIAllocator &alloc)
{
  UNUSED(op);
  UNUSED(alloc);
  return OB_SUCCESS;
}

int ObDASLockResult::reuse()
{
  int ret = OB_SUCCESS;
  affected_rows_ = 0;
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASLockResult, ObIDASTaskResult),
                    affected_rows_);
}  // namespace sql
}  // namespace oceanbase
