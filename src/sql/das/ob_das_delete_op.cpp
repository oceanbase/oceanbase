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
#include "sql/das/ob_das_delete_op.h"
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
struct EnumEncoder<false, const sql::ObDASDelCtDef *> : sql::DASCtEncoder<sql::ObDASDelCtDef>
{
};

template <>
struct EnumEncoder<false, sql::ObDASDelRtDef *> : sql::DASRtEncoder<sql::ObDASDelRtDef>
{
};
} // end namespace serialization
} // end namespace common

using namespace common;
using namespace storage;
using namespace share;
namespace sql
{
template <>
int ObDASIndexDMLAdaptor<DAS_OP_TABLE_DELETE, ObDASDMLIterator>::write_rows(const ObLSID &ls_id,
                                                                            const ObTabletID &tablet_id,
                                                                            const CtDefType &ctdef,
                                                                            RtDefType &rtdef,
                                                                            ObDASDMLIterator &iter,
                                                                            int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObAccessService *as = MTL(ObAccessService *);
  if (OB_FAIL(as->delete_rows(ls_id,
                              tablet_id,
                              *tx_desc_,
                              dml_param_,
                              ctdef.column_ids_,
                              &iter,
                              affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("delete rows to access service failed", K(ret));
    }
  } else if (!(ctdef.is_ignore_ || ctdef.table_param_.get_data_table().is_spatial_index())
      && 0 == affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows after do delete", K(affected_rows), K(ret));
  }
  return ret;
}

ObDASDeleteOp::ObDASDeleteOp(ObIAllocator &op_alloc)
  : ObIDASTaskOp(op_alloc),
    del_ctdef_(nullptr),
    del_rtdef_(nullptr),
    write_buffer_(),
    affected_rows_(0)
{
}

int ObDASDeleteOp::open_op()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDASDMLIterator dml_iter(del_ctdef_, write_buffer_, op_alloc_);
  ObDASIndexDMLAdaptor<DAS_OP_TABLE_DELETE, ObDASDMLIterator> del_adaptor;
  del_adaptor.tx_desc_ = trans_desc_;
  del_adaptor.snapshot_ = snapshot_;
  del_adaptor.ctdef_ = del_ctdef_;
  del_adaptor.rtdef_ = del_rtdef_;
  del_adaptor.related_ctdefs_ = &related_ctdefs_;
  del_adaptor.related_rtdefs_ = &related_rtdefs_;
  del_adaptor.tablet_id_ = tablet_id_;
  del_adaptor.ls_id_ = ls_id_;
  del_adaptor.related_tablet_ids_ = &related_tablet_ids_;
  del_adaptor.das_allocator_ = &op_alloc_;
  if (OB_FAIL(del_adaptor.write_tablet(dml_iter, affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("delete row to partition storage failed", K(ret));
    }
  } else {
    del_rtdef_->affected_rows_ += affected_rows;
    affected_rows_ = affected_rows;
  }
  return ret;
}

int ObDASDeleteOp::release_op()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObDASDeleteOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASDeleteResult));
  CK(task_id_ == task_result->get_task_id());
#endif
  if (OB_SUCC(ret)) {
    ObDASDeleteResult *del_result = static_cast<ObDASDeleteResult*>(task_result);
    del_rtdef_->affected_rows_ += del_result->get_affected_rows();
  }
  return ret;
}

int ObDASDeleteOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  UNUSED(memory_limit);
#if !defined(NDEBUG)
  CK(typeid(task_result) == typeid(ObDASDeleteResult));
#endif
  if (OB_SUCC(ret)) {
    ObDASDeleteResult &del_result = static_cast<ObDASDeleteResult&>(task_result);
    del_result.set_affected_rows(affected_rows_);
    has_more = false;
  }
  return ret;
}

int ObDASDeleteOp::init_task_info(uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (!write_buffer_.is_inited()
      && OB_FAIL(write_buffer_.init(op_alloc_, row_extend_size, MTL_ID(), "DASDeleteBuffer"))) {
    LOG_WARN("init delete buffer failed", K(ret));
  }
  return ret;
}

int ObDASDeleteOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  if (remote_info != nullptr) {
    //DAS delete is executed remotely
    trans_desc_ = remote_info->trans_desc_;
    snapshot_ = &remote_info->snapshot_;
  }
  return ret;
}

int ObDASDeleteOp::write_row(const ExprFixedArray &row,
                             ObEvalCtx &eval_ctx,
                             ObChunkDatumStore::StoredRow *&stored_row,
                             bool &buffer_full)
{
  int ret = OB_SUCCESS;
  bool added = false;
  buffer_full = false;
  if (!write_buffer_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer not inited", K(ret));
  } else if (OB_FAIL(write_buffer_.try_add_row(row, &eval_ctx, das::OB_DAS_MAX_PACKET_SIZE, stored_row, added, true))) {
    LOG_WARN("try add row to datum store failed", K(ret), K(row), K(write_buffer_));
  } else if (!added) {
    buffer_full = true;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASDeleteOp, ObIDASTaskOp),
                    del_ctdef_,
                    del_rtdef_,
                    write_buffer_);

ObDASDeleteResult::ObDASDeleteResult()
  : ObIDASTaskResult(),
    affected_rows_(0)
{
}

ObDASDeleteResult::~ObDASDeleteResult()
{
}

int ObDASDeleteResult::init(const ObIDASTaskOp &op, common::ObIAllocator &alloc)
{
  UNUSED(op);
  UNUSED(alloc);
  return OB_SUCCESS;
}

int ObDASDeleteResult::reuse()
{
  int ret = OB_SUCCESS;
  affected_rows_ = 0;
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASDeleteResult, ObIDASTaskResult),
                    affected_rows_);
}  // namespace sql
}  // namespace oceanbase
