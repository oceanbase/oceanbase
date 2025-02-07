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
#include "sql/das/ob_das_insert_op.h"
#include "share/ob_scanner.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/das/ob_das_extra_data.h"
#include "storage/ob_query_iterator_factory.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace common
{
namespace serialization
{
template <>
struct EnumEncoder<false, const sql::ObDASInsCtDef *> : sql::DASCtEncoder<sql::ObDASInsCtDef>
{
};

template <>
struct EnumEncoder<false, sql::ObDASInsRtDef *> : sql::DASRtEncoder<sql::ObDASInsRtDef>
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
int ObDASIndexDMLAdaptor<DAS_OP_TABLE_INSERT, ObDASDMLIterator>::write_rows(const ObLSID &ls_id,
                                                                            const ObTabletID &tablet_id,
                                                                            const CtDefType &ctdef,
                                                                            RtDefType &rtdef,
                                                                            ObDASDMLIterator &iter,
                                                                            int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObAccessService *as = MTL(ObAccessService *);
  if (ctdef.table_param_.get_data_table().is_mlog_table()
      && !ctdef.is_access_mlog_as_master_table_) {
    ObDASMLogDMLIterator mlog_iter(ls_id, tablet_id, dml_param_, &iter, DAS_OP_TABLE_INSERT);
    if (OB_FAIL(as->insert_rows(ls_id,
                                tablet_id,
                                *tx_desc_,
                                dml_param_,
                                ctdef.column_ids_,
                                &mlog_iter,
                                affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("insert rows to access service failed", K(ret));
      }
    }
  } else if (rtdef.use_put_) {
    ret = as->put_rows(ls_id,
                       tablet_id,
                       *tx_desc_,
                       dml_param_,
                       ctdef.column_ids_,
                       &iter,
                       affected_rows);
  } else {
    ret = as->insert_rows(ls_id,
                          tablet_id,
                          *tx_desc_,
                          dml_param_,
                          ctdef.column_ids_,
                          &iter,
                          affected_rows);
  }
  if (OB_FAIL(ret)) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("insert rows to access service failed", K(ret));
    }
  } else if (!(ctdef.is_ignore_ || ctdef.table_param_.get_data_table().is_domain_index())
      && 0 == affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows after do insert", K(affected_rows), K(ret));
  }
  return ret;
}

ObDASInsertOp::ObDASInsertOp(ObIAllocator &op_alloc)
  : ObIDASTaskOp(op_alloc),
    ins_ctdef_(nullptr),
    ins_rtdef_(nullptr),
    result_(nullptr),
    affected_rows_(0),
    is_duplicated_(false)
{
}

int ObDASInsertOp::open_op()
{
  int ret = OB_SUCCESS;
  if (ins_rtdef_->need_fetch_conflict_ && OB_FAIL(insert_row_with_fetch())) {
    LOG_WARN("fail to do insert with conflict fetch", K(ret), K(das_gts_opt_info_));
  } else if (!ins_rtdef_->need_fetch_conflict_ && OB_FAIL(insert_rows())) {
    LOG_WARN("fail to do insert", K(ret));
  }

  return ret;
}

int ObDASInsertOp::insert_rows()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDASDMLIterator dml_iter(ins_ctdef_, insert_buffer_, op_alloc_);
  ObDASIndexDMLAdaptor<DAS_OP_TABLE_INSERT, ObDASDMLIterator> ins_adaptor;
  ins_adaptor.tx_desc_ = trans_desc_;
  ins_adaptor.snapshot_ = snapshot_;
  ins_adaptor.write_branch_id_ = write_branch_id_;
  ins_adaptor.ctdef_ = ins_ctdef_;
  ins_adaptor.rtdef_ = ins_rtdef_;
  ins_adaptor.related_ctdefs_ = &related_ctdefs_;
  ins_adaptor.related_rtdefs_ = &related_rtdefs_;
  ins_adaptor.tablet_id_ = tablet_id_;
  ins_adaptor.ls_id_ = ls_id_;
  ins_adaptor.related_tablet_ids_ = &related_tablet_ids_;
  ins_adaptor.das_allocator_ = &op_alloc_;
  if (OB_FAIL(ins_adaptor.write_tablet(dml_iter, affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("insert rows to access service failed", K(ret));
    }
  } else {
    affected_rows_ = affected_rows;
  }
  return ret;
}
int ObDASInsertOp::insert_index_with_fetch(ObDMLBaseParam &dml_param,
                                           ObAccessService *as,
                                           ObDatumRowIterator &dml_iter,
                                           ObDASConflictIterator *result_iter,
                                           const ObDASInsCtDef *ins_ctdef,
                                           ObDASInsRtDef *ins_rtdef,
                                           storage::ObStoreCtxGuard &store_ctx_guard,
                                           const UIntFixedArray *duplicated_column_ids,
                                           common::ObTabletID tablet_id,
                                           transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow *insert_row = NULL;
  int64_t affected_rows = 0;
  if (OB_FAIL(ObDMLService::init_dml_param(*ins_ctdef,
                                           *ins_rtdef,
                                           *snapshot,
                                           write_branch_id_,
                                           op_alloc_,
                                           store_ctx_guard,
                                           dml_param))) {
    LOG_WARN("init index dml param failed", K(ret), KPC(ins_ctdef), KPC(ins_rtdef));
  }
  while (OB_SUCC(ret) && OB_SUCC(dml_iter.get_next_row(insert_row))) {
    blocksstable::ObDatumRowIterator *duplicated_rows = NULL;
    if (OB_ISNULL(insert_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert_row is null", K(ret));
    } else if (OB_FAIL(as->insert_row(ls_id_,
                                      tablet_id,
                                      *trans_desc_,
                                      dml_param,
                                      ins_ctdef->column_ids_,
                                      *duplicated_column_ids,
                                      *insert_row,
                                      INSERT_RETURN_ALL_DUP,
                                      affected_rows,
                                      duplicated_rows))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        ret = OB_SUCCESS;
        bool is_local_index_table = ins_ctdef->table_param_.get_data_table().is_index_local_storage();
        bool is_unique_index = ins_ctdef->table_param_.get_data_table().is_unique_index();
        if (is_local_index_table && !is_unique_index) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected duplicate key error", K(ret), K(ins_ctdef->table_param_.get_data_table()));
        } else if (OB_ISNULL(duplicated_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("duplicated_row is null", K(ret));
        } else if (OB_FAIL(result_iter->get_duplicated_iter_array().push_back(duplicated_rows))) {
          LOG_WARN("fail to push duplicated_row iter", K(ret));
        } else {
          LOG_DEBUG("insert one row and conflicted", KPC(insert_row));
          is_duplicated_ = true;
        }
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(duplicated_rows)) {
      ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
      duplicated_rows = NULL;
    }
  }

  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

int ObDASInsertOp::insert_row_with_fetch()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObNewRow *insert_row = NULL;
  ObDASConflictIterator *result_iter = nullptr;
  void *buf = nullptr;
  ObAccessService *as = MTL(ObAccessService *);
  ObDMLBaseParam dml_param;
  ObDASDMLIterator dml_iter(ins_ctdef_, insert_buffer_, op_alloc_);
  storage::ObStoreCtxGuard store_ctx_guard;
  transaction::ObTxReadSnapshot *snapshot = snapshot_;
  if (das_gts_opt_info_.get_specify_snapshot()) {
    transaction::ObTransService *txs = nullptr;
    if (das_gts_opt_info_.isolation_level_ != transaction::ObTxIsolationLevel::RC) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected isolation_level", K(ret), K(das_gts_opt_info_));
    } else if (OB_ISNULL(txs = MTL_WITH_CHECK_TENANT(transaction::ObTransService*, MTL_ID()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get_tx_service", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(txs->get_ls_read_snapshot(*trans_desc_,
                                                 das_gts_opt_info_.isolation_level_,
                                                 ls_id_,
                                                 THIS_WORKER.get_timeout_ts(),
                                                 *das_gts_opt_info_.get_response_snapshot()))) {
      LOG_WARN("fail to get ls read_snapshot", K(ret), K(ls_id_), K(THIS_WORKER.get_timeout_ts()));
    } else {
      snapshot = das_gts_opt_info_.get_response_snapshot();
      LOG_TRACE("succ get ls snaoshot", K(ls_id_), K(tablet_id_), KPC(snapshot));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ins_ctdef_->table_rowkey_types_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_rowkey_types is invalid", K(ret));
  } else if (OB_FAIL(as->get_write_store_ctx_guard(ls_id_,
                                                   ins_rtdef_->timeout_ts_,
                                                   *trans_desc_,
                                                   *snapshot,
                                                   write_branch_id_,
                                                   store_ctx_guard))) {
    LOG_WARN("fail to get_write_store_ctx_guard", K(ret), K(ls_id_));
  } else if (OB_ISNULL(buf = op_alloc_.alloc(sizeof(ObDASConflictIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate ObDASConflictIterator", K(ret));
  } else {
    result_iter = new(buf) ObDASConflictIterator(ins_ctdef_->table_rowkey_types_,
                                                 op_alloc_);
    result_ = result_iter;
  }

  // 1. insert primary table
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(insert_index_with_fetch(dml_param,
                                             as,
                                             dml_iter,
                                             result_iter,
                                             ins_ctdef_,
                                             ins_rtdef_,
                                             store_ctx_guard,
                                             &ins_ctdef_->table_rowkey_cids_,
                                             tablet_id_,
                                             snapshot))) {
    LOG_WARN("fail to insert primary table", K(ret));
  }

  // 2. insert unique index
  for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
    const ObDASInsCtDef *index_ins_ctdef = static_cast<const ObDASInsCtDef*>(related_ctdefs_.at(i));
    ObDASInsRtDef *index_ins_rtdef = static_cast<ObDASInsRtDef*>(related_rtdefs_.at(i));
    ObTabletID index_tablet_id = related_tablet_ids_.at(i);

    const bool is_local_unique_index = index_ins_ctdef->table_param_.get_data_table().is_unique_index();
    if (!is_local_unique_index) {
      // insert it later
    } else if (OB_FAIL(dml_iter.rewind(index_ins_ctdef))) {
      LOG_WARN("rewind dml iter failed", K(ret));
    } else if (OB_FAIL(insert_index_with_fetch(dml_param,
                                               as,
                                               dml_iter,
                                               result_iter,
                                               index_ins_ctdef,
                                               index_ins_rtdef,
                                               store_ctx_guard,
                                               &ins_ctdef_->table_rowkey_cids_,
                                               index_tablet_id,
                                               snapshot))) {
      LOG_WARN("fail to insert local unique index", K(ret), K(index_ins_ctdef->table_param_.get_data_table()));
    }
  }

  // 3. insert non_unique index
  for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
    const ObDASInsCtDef *index_ins_ctdef = static_cast<const ObDASInsCtDef*>(related_ctdefs_.at(i));
    ObDASInsRtDef *index_ins_rtdef = static_cast<ObDASInsRtDef*>(related_rtdefs_.at(i));
    ObTabletID index_tablet_id = related_tablet_ids_.at(i);
    const bool is_local_unique_index = index_ins_ctdef->table_param_.get_data_table().is_unique_index();
    if (is_local_unique_index) {
      // insert it before
    } else if (is_duplicated_) {
      LOG_TRACE("is duplicated before, not need write non_unique index");
    } else if (OB_FAIL(dml_iter.rewind(index_ins_ctdef))) {
      LOG_WARN("rewind dml iter failed", K(ret));
    } else {
      ObDASMLogDMLIterator mlog_iter(ls_id_, index_tablet_id, dml_param, &dml_iter, DAS_OP_TABLE_INSERT);
      ObDatumRowIterator *new_iter = nullptr;
      if (index_ins_ctdef->table_param_.get_data_table().is_mlog_table()
          && !index_ins_ctdef->is_access_mlog_as_master_table_) {
        new_iter = &mlog_iter;
      } else {
        new_iter = &dml_iter;
      }
      if (OB_FAIL(insert_index_with_fetch(dml_param,
                                          as,
                                          *new_iter,
                                          result_iter,
                                          index_ins_ctdef,
                                          index_ins_rtdef,
                                          store_ctx_guard,
                                          &(index_ins_ctdef->column_ids_),
                                          index_tablet_id,
                                          snapshot))) {
        // For non-unique local index,
        // We check for duplications on all columns because the partition key is not stored in storage level
        LOG_WARN("fail to insert non_unique index", K(ret), K(index_ins_ctdef->table_param_.get_data_table()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    result_iter->init_curr_iter();
  }

  return ret;
}

int ObDASInsertOp::store_conflict_row(ObDASInsertResult &ins_result)
{
  int ret = OB_SUCCESS;
  bool added = false;
  ObDatumRow *dup_row = nullptr;
  ObDASWriteBuffer &result_buffer = ins_result.get_result_buffer();
  ObDASWriteBuffer::DmlShadowRow ssr;
  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result_ can't be null", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ssr.init(op_alloc_, ins_ctdef_->table_rowkey_types_, false))) {
      LOG_WARN("init shadow stored row failed", K(ret), K(ins_ctdef_->table_rowkey_types_));
    }
  }
  while (OB_SUCC(ret) && OB_SUCC(result_->get_next_row(dup_row))) {
    LOG_DEBUG("fetch one conflict row", KPC(dup_row));
    if (OB_FAIL(ssr.shadow_copy(*dup_row))) {
      LOG_WARN("shadow copy ObNewRow failed", K(ret));
    } else if (OB_FAIL(result_buffer.try_add_row(ssr, das::OB_DAS_MAX_PACKET_SIZE, added))) {
      LOG_WARN("fail to add row to datum_store",K(ret));
    } else {
      ssr.reuse();
    }
  }

  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

int ObDASInsertOp::release_op()
{
  int ret = OB_SUCCESS;
  if (result_ != NULL) {
    result_->reset();
    result_ = NULL;
  }
  return ret;
}

int ObDASInsertOp::record_task_result_to_rtdef()
{
  int ret = OB_SUCCESS;
  ins_rtdef_->affected_rows_ += affected_rows_;
  ins_rtdef_->is_duplicated_ |= is_duplicated_;
  return ret;
}
int ObDASInsertOp::assign_task_result(ObIDASTaskOp *other)
{
  int ret = OB_SUCCESS;
  if (other->get_type() != get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task type", K(ret), KPC(other));
  } else {
    ObDASInsertOp *ins_op = static_cast<ObDASInsertOp *>(other);
    affected_rows_ = ins_op->get_affected_rows();
    is_duplicated_ = ins_op->get_is_duplicated();
  }
  return ret;
}

int ObDASInsertOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASInsertResult));
  CK(task_id_ == task_result->get_task_id());
#endif

  if (OB_SUCC(ret)) {
    ObDASInsertResult *insert_result = static_cast<ObDASInsertResult*>(task_result);
    if (ins_rtdef_->need_fetch_conflict_) {
      if (OB_FAIL(insert_result->init_result_newrow_iter(&ins_ctdef_->table_rowkey_types_))) {
        LOG_WARN("init insert result iterator failed", K(ret));
      } else {
        result_ = insert_result;
        affected_rows_ = insert_result->get_affected_rows();
        is_duplicated_ = insert_result->is_duplicated();
        if (das_gts_opt_info_.get_specify_snapshot()) {
          if (OB_FAIL(das_gts_opt_info_.get_response_snapshot()->assign(insert_result->get_response_snapshot()))) {
            LOG_WARN("fail to assign snapshot", K(ret));
          }
        }
      }
    } else {
      result_ = insert_result;
      affected_rows_ = insert_result->get_affected_rows();
      is_duplicated_ = insert_result->is_duplicated();
    }
  }
  return ret;
}

int ObDASInsertOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(task_result) == typeid(ObDASInsertResult));
#endif

  if (OB_SUCC(ret)) {
    ObDASInsertResult &ins_result = static_cast<ObDASInsertResult&>(task_result);
    // 只有need fetch conflict row, 回冲突行
    if (ins_rtdef_->need_fetch_conflict_) {
      if (OB_FAIL(store_conflict_row(ins_result))) {
        LOG_WARN("fail to fetch conflict row", K(ret));
      } else {
        ins_result.set_affected_rows(affected_rows_);
        ins_result.set_is_duplicated(is_duplicated_);
        has_more = false;
        memory_limit -= ins_result.get_result_buffer().get_mem_used();
        if (das_gts_opt_info_.get_specify_snapshot()) {
          if (OB_FAIL(ins_result.get_response_snapshot().assign(*das_gts_opt_info_.get_response_snapshot()))) {
            LOG_WARN("fail to assign snapshot", K(ret));
          }
        }
      }
    } else {
      ins_result.set_affected_rows(affected_rows_);
      has_more = false;
    }
  }

  return ret;
}

int ObDASInsertOp::init_task_info(uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (!insert_buffer_.is_inited()
      && OB_FAIL(insert_buffer_.init(op_alloc_, row_extend_size, MTL_ID(), "DASInsertBuffer"))) {
    LOG_WARN("init insert buffer failed", K(ret));
  }
  return ret;
}

int ObDASInsertOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIDASTaskOp::swizzling_remote_task(remote_info))) {
    LOG_WARN("fail to swizzling remote task", K(ret));
  } else if (remote_info != nullptr) {
    //DAS insert is executed remotely
    trans_desc_ = remote_info->trans_desc_;
  }
  return ret;
}

int ObDASInsertOp::write_row(const ExprFixedArray &row,
                             ObEvalCtx &eval_ctx,
                             ObChunkDatumStore::StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  bool added = false;
  if (!insert_buffer_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer not inited", K(ret));
  } else if (OB_FAIL(insert_buffer_.add_row(row, &eval_ctx, stored_row, true))) {
    LOG_WARN("add row to insert buffer failed", K(ret), K(row), K(insert_buffer_));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASInsertOp, ObIDASTaskOp),
                    ins_ctdef_,
                    ins_rtdef_,
                    insert_buffer_);

ObDASInsertResult::ObDASInsertResult()
  : ObIDASTaskResult(),
    affected_rows_(0),
    result_buffer_(),
    result_newrow_iter_(),
    output_types_(nullptr),
    is_duplicated_(false),
    response_snapshot_()
{
}

ObDASInsertResult::~ObDASInsertResult()
{
}

int ObDASInsertResult::get_next_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  ObDatumRow *result_row = NULL;
  if (OB_FAIL(result_newrow_iter_.get_next_row(result_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from result iter failed", K(ret));
    }
  } else {
    row = result_row;
  }
  return ret;
}

int ObDASInsertResult::link_extra_result(ObDASExtraData &extra_result, ObIDASTaskOp *task_op)
{
  UNUSED(extra_result);
  return OB_NOT_IMPLEMENT;
}

int ObDASInsertResult::init_result_newrow_iter(const ObjMetaFixedArray *output_types)
{
  int ret = OB_SUCCESS;
  output_types_ = output_types;
  if (OB_ISNULL(output_types_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("output types array is null", K(ret));
  } else if (OB_FAIL(result_buffer_.begin(result_newrow_iter_, *output_types_))) {
    LOG_WARN("begin datum result iterator failed", K(ret));
  }
  return ret;
}

void ObDASInsertResult::reset()
{
  output_types_ = nullptr;
}

int ObDASInsertResult::init(const ObIDASTaskOp &op, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  const ObDASInsertOp &ins_op = static_cast<const ObDASInsertOp&>(op);
  uint64_t tenant_id = 0;
  const ObDASBaseCtDef *base_ctdef = ins_op.get_ctdef();
  if (OB_ISNULL(base_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base ctdef is null", K(ret));
  } else {
    const ObDASDMLBaseCtDef *ins_ctdef = static_cast<const ObDASDMLBaseCtDef*>(base_ctdef);
    tenant_id = MTL_ID();
    // replace和insert_up拉回冲突数据的das_write_buff暂时不需要带pay_load
    if (!result_buffer_.is_inited()
        && OB_FAIL(result_buffer_.init(alloc, 0 /*row_extend_size*/, tenant_id, "DASInsRsultBuffer"))) {
      LOG_WARN("init result buffer failed", K(ret));
    }
  }
  return OB_SUCCESS;
}

int ObDASInsertResult::reuse()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != result_buffer_.get_row_cnt())) {
    ret = OB_NOT_SUPPORTED;
  } else {
    affected_rows_ = 0;
    is_duplicated_ = false;
    result_buffer_.~ObDASWriteBuffer();
    new(&result_buffer_) ObDASWriteBuffer();
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASInsertResult, ObIDASTaskResult),
                    affected_rows_,
                    result_buffer_,
                    is_duplicated_,
                    response_snapshot_);


void ObDASConflictIterator::reset()
{
  ObDuplicatedIterList::iterator iter = duplicated_iter_list_.begin();
  for (; iter != duplicated_iter_list_.end(); ++iter) {
    ObQueryIteratorFactory::free_insert_dup_iter(*iter);
  }
  duplicated_iter_list_.reset();
}

int ObDASConflictIterator::get_next_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool find_next_iter = false;
  ObDatumRow *dup_row = NULL;
  do {
    if (find_next_iter) {
      ++curr_iter_;
      find_next_iter = false;
    }
    if (curr_iter_ == duplicated_iter_list_.end()) {
      ret = OB_ITER_END;
      LOG_DEBUG("fetch conflict row iterator end");
    } else {
      blocksstable::ObDatumRowIterator *dup_row_iter = *curr_iter_;
      if (OB_ISNULL(dup_row_iter)) {
        find_next_iter = true;
      } else if (OB_FAIL(dup_row_iter->get_next_row(dup_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          find_next_iter = true;
        } else {
          LOG_WARN("get next row from duplicated iter failed", K(ret));
        }
      } else if (OB_ISNULL(dup_row)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(dup_row));
      } else {
        LOG_DEBUG("get one duplicate key", KPC(dup_row));
      }
    }
  } while (OB_SUCC(ret) && find_next_iter);

  if (OB_SUCC(ret)) {
    row = dup_row;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
