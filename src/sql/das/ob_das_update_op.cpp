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
#include "sql/das/ob_das_update_op.h"
#include "share/ob_scanner.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/das/ob_das_utils.h"
#include "storage/tx_storage/ob_access_service.h"
namespace oceanbase
{
namespace common
{
namespace serialization
{
template <>
struct EnumEncoder<false, const sql::ObDASUpdCtDef *> : sql::DASCtEncoder<sql::ObDASUpdCtDef>
{
};

template <>
struct EnumEncoder<false, sql::ObDASUpdRtDef *> : sql::DASRtEncoder<sql::ObDASUpdRtDef>
{
};
} // end namespace serialization
} // end namespace common

using namespace common;
using namespace storage;
using namespace share;
namespace sql
{

class ObDASUpdIterator : public ObNewRowIterator
{
public:
  ObDASUpdIterator(const ObDASUpdCtDef *das_ctdef,
                   ObDASWriteBuffer &write_buffer,
                   ObIAllocator &alloc)
    : das_ctdef_(das_ctdef),
      write_buffer_(write_buffer),
      old_row_(nullptr),
      new_row_(nullptr),
      got_old_row_(false),
      allocator_(alloc)
  {
  }
  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override { return OB_NOT_IMPLEMENT; }
  ObDASWriteBuffer &get_write_buffer() { return write_buffer_; }
  virtual void reset() override { }
  int rewind(const ObDASDMLBaseCtDef *das_ctdef)
  {
    old_row_ = nullptr;
    new_row_ = nullptr;
    got_old_row_ = false;
    das_ctdef_ = static_cast<const ObDASUpdCtDef*>(das_ctdef);
    return OB_SUCCESS;
  }
private:
  const ObDASUpdCtDef *das_ctdef_;
  ObDASWriteBuffer &write_buffer_;
  ObNewRow *old_row_;
  ObNewRow *new_row_;
  ObDASWriteBuffer::Iterator result_iter_;
  bool got_old_row_;
  common::ObIAllocator &allocator_;
};

int ObDASUpdIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *sr = NULL;
  if (!got_old_row_) {
    got_old_row_ = true;
    if (OB_ISNULL(old_row_)) {
      if (OB_FAIL(ob_create_row(allocator_, das_ctdef_->old_row_projector_.count(), old_row_))) {
        LOG_WARN("create row buffer failed", K(ret), K(das_ctdef_->old_row_projector_.count()));
      } else if (OB_FAIL(write_buffer_.begin(result_iter_))) {
        LOG_WARN("begin datum result iterator failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_ISNULL(new_row_)) {
      if (OB_FAIL(ob_create_row(allocator_, das_ctdef_->new_row_projector_.count(), new_row_))) {
        LOG_WARN("create row buffer failed", K(ret), K(das_ctdef_->new_row_projector_.count()));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(result_iter_.get_next_row(sr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from result iterator failed", K(ret));
        }
      } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                         *sr,
                                                         das_ctdef_->old_row_projector_,
                                                         allocator_,
                                                         *old_row_))) {
        LOG_WARN("project old storage row failed", K(ret));
      } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                         *sr,
                                                         das_ctdef_->new_row_projector_,
                                                         allocator_,
                                                         *new_row_))) {
        LOG_WARN("project new storage row failed", K(ret));
      } else {
        row = old_row_;
        LOG_TRACE("DAS update get old row",
                  K_(das_ctdef_->old_row_projector),
                  K_(das_ctdef_->new_row_projector),
                  "table_id", das_ctdef_->table_id_,
                  "index_tid", das_ctdef_->index_tid_, KPC_(old_row),
                  KPC_(new_row));
      }
    }
  } else {
    got_old_row_ = false;
    if (OB_ISNULL(new_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new row is null", K(ret));
    } else {
      row = new_row_;
      LOG_DEBUG("DAS update get new row",
                "table_id", das_ctdef_->table_id_,
                "index_tid", das_ctdef_->index_tid_, KPC_(new_row));
    }
  }
  return ret;
}

template <>
int ObDASIndexDMLAdaptor<DAS_OP_TABLE_UPDATE, ObDASUpdIterator>::write_rows(const ObLSID &ls_id,
                                                                            const ObTabletID &tablet_id,
                                                                            const CtDefType &ctdef,
                                                                            RtDefType &rtdef,
                                                                            ObDASUpdIterator &iter,
                                                                            int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObAccessService *as = MTL(ObAccessService *);
  if (OB_FAIL(as->update_rows(ls_id,
                              tablet_id,
                              *tx_desc_,
                              dml_param_,
                              ctdef.column_ids_,
                              ctdef.updated_column_ids_,
                              &iter,
                              affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("update row to partition storage failed", K(ret));
    }
  }
  return ret;
}

ObDASUpdateOp::ObDASUpdateOp(ObIAllocator &op_alloc)
  : ObIDASTaskOp(op_alloc),
    upd_ctdef_(nullptr),
    upd_rtdef_(nullptr)
{
}

int ObDASUpdateOp::open_op()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDASUpdIterator upd_iter(upd_ctdef_, write_buffer_, op_alloc_);
  ObDASIndexDMLAdaptor<DAS_OP_TABLE_UPDATE, ObDASUpdIterator> upd_adaptor;
  upd_adaptor.tx_desc_ = trans_desc_;
  upd_adaptor.snapshot_ = snapshot_;
  upd_adaptor.ctdef_ = upd_ctdef_;
  upd_adaptor.rtdef_ = upd_rtdef_;
  upd_adaptor.related_ctdefs_ = &related_ctdefs_;
  upd_adaptor.related_rtdefs_ = &related_rtdefs_;
  upd_adaptor.tablet_id_ = tablet_id_;
  upd_adaptor.ls_id_ = ls_id_;
  upd_adaptor.related_tablet_ids_ = &related_tablet_ids_;
  upd_adaptor.das_allocator_ = &op_alloc_;
  if (OB_FAIL(upd_adaptor.write_tablet(upd_iter, affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("update row to partition storage failed", K(ret));
    }
  } else {
    upd_rtdef_->affected_rows_ += affected_rows;
  }
  return ret;
}

int ObDASUpdateOp::release_op()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObDASUpdateOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASUpdateResult));
  CK(task_id_ == task_result->get_task_id());
#endif
  if (OB_SUCC(ret)) {
    ObDASUpdateResult *del_result = static_cast<ObDASUpdateResult*>(task_result);
    upd_rtdef_->affected_rows_ += del_result->get_affected_rows();
  }
  return ret;
}

int ObDASUpdateOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(task_result) == typeid(ObDASUpdateResult));
#endif
  if (OB_SUCC(ret)) {
    ObDASUpdateResult &del_result = static_cast<ObDASUpdateResult&>(task_result);
    del_result.set_affected_rows(upd_rtdef_->affected_rows_);
    has_more = false;
  }
  return ret;
}

int ObDASUpdateOp::init_task_info()
{
  int ret = OB_SUCCESS;
  if (!write_buffer_.is_inited()
      && OB_FAIL(write_buffer_.init(op_alloc_, DAS_ROW_EXTEND_SIZE, tenant_id_, "DASUpdateBuffer"))) {
    LOG_WARN("init update buffer failed", K(ret));
  }
  return ret;
}

int ObDASUpdateOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  if (remote_info != nullptr) {
    //DAS update is executed remotely
    trans_desc_ = remote_info->trans_desc_;
    snapshot_ = &remote_info->snapshot_;
  }
  return ret;
}

int ObDASUpdateOp::write_row(const ExprFixedArray &row, ObEvalCtx &eval_ctx, bool &buffer_full, ObChunkDatumStore::StoredRow* &stored_row)
{
  int ret = OB_SUCCESS;
  bool added = false;
  buffer_full = false;
  if (OB_FAIL(write_buffer_.try_add_row(row, &eval_ctx, das::OB_DAS_MAX_PACKET_SIZE, added, true, stored_row))) {
    LOG_WARN("try add row to datum store failed", K(ret), K(row), K(write_buffer_));
  } else if (!added) {
    buffer_full = true;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASUpdateOp, ObIDASTaskOp),
                    upd_ctdef_,
                    upd_rtdef_,
                    write_buffer_);

ObDASUpdateResult::ObDASUpdateResult()
  : ObIDASTaskResult(),
    affected_rows_(0)
{
}

ObDASUpdateResult::~ObDASUpdateResult()
{
}

int ObDASUpdateResult::init(const ObIDASTaskOp &op)
{
  UNUSED(op);
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER((ObDASUpdateResult, ObIDASTaskResult),
                    affected_rows_);
}  // namespace sql
}  // namespace oceanbase
