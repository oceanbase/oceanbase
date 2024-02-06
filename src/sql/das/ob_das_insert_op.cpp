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
  dml_param_.direct_insert_task_id_ = rtdef.direct_insert_task_id_;
  if (rtdef.use_put_) {
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
  } else if (!(ctdef.is_ignore_ || ctdef.table_param_.get_data_table().is_spatial_index())
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
    LOG_WARN("fail to do insert with conflict fetch", K(ret));
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
    ins_rtdef_->affected_rows_ += affected_rows;
    affected_rows_ = affected_rows;
  }
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
  if (ins_ctdef_->table_rowkey_types_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_rowkey_types is invalid", K(ret));
  } else if (OB_FAIL(ObDMLService::init_dml_param(*ins_ctdef_, *ins_rtdef_, *snapshot_, op_alloc_, dml_param))) {
    LOG_WARN("init dml param failed", K(ret), KPC_(ins_ctdef), KPC_(ins_rtdef));
  } else if (OB_ISNULL(buf = op_alloc_.alloc(sizeof(ObDASConflictIterator)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to allocate ObDASConflictIterator", K(ret));
  } else {
    result_iter = new(buf) ObDASConflictIterator(ins_ctdef_->table_rowkey_types_,
                                                 op_alloc_);
    result_ = result_iter;
  }

  while (OB_SUCC(ret) && OB_SUCC(dml_iter.get_next_row(insert_row))) {
    ObNewRowIterator *duplicated_rows = NULL;
    if (OB_ISNULL(insert_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert_row is null", K(ret));
    } else if (OB_FAIL(as->insert_row(ls_id_,
                                      tablet_id_,
                                      *trans_desc_,
                                      dml_param,
                                      ins_ctdef_->column_ids_,
                                      ins_ctdef_->table_rowkey_cids_,
                                      *insert_row,
                                      INSERT_RETURN_ALL_DUP,
                                      affected_rows,
                                      duplicated_rows))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        ret = OB_SUCCESS;
        if (OB_ISNULL(duplicated_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("duplicated_row is null", K(ret));
        } else if (OB_FAIL(result_iter->get_duplicated_iter_array().push_back(duplicated_rows))) {
          LOG_WARN("fail to push duplicated_row iter", K(ret));
        } else {
          LOG_DEBUG("insert one row and conflicted", KPC(insert_row));
          ins_rtdef_->is_duplicated_ = true;
          is_duplicated_ = true;
        }
      }
    }
  }
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
    const ObDASInsCtDef *index_ins_ctdef = static_cast<const ObDASInsCtDef*>(related_ctdefs_.at(i));
    ObDASInsRtDef *index_ins_rtdef = static_cast<ObDASInsRtDef*>(related_rtdefs_.at(i));
    ObTabletID index_tablet_id = related_tablet_ids_.at(i);
    if (OB_FAIL(dml_iter.rewind(index_ins_ctdef))) {
      LOG_WARN("rewind dml iter failed", K(ret));
    } else if (OB_FAIL(ObDMLService::init_dml_param(*index_ins_ctdef,
                                                    *index_ins_rtdef,
                                                    *snapshot_,
                                                    op_alloc_,
                                                    dml_param))) {
      LOG_WARN("init index dml param failed", K(ret), KPC(index_ins_ctdef), KPC(index_ins_rtdef));
    }

    const UIntFixedArray *duplicated_column_ids = nullptr;
    const bool is_local_unique_index = index_ins_ctdef->table_param_.get_data_table().is_unique_index();
    if (is_local_unique_index) {
      duplicated_column_ids = &(ins_ctdef_->table_rowkey_cids_);
    } else {
      // For non-unique local index, We check for duplications on all columns because the partition key is not stored in storage level
      duplicated_column_ids = &(index_ins_ctdef->column_ids_);
    }

    while (OB_SUCC(ret) && OB_SUCC(dml_iter.get_next_row(insert_row))) {
      ObNewRowIterator *duplicated_rows = NULL;
      if (OB_ISNULL(insert_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert_row is null", K(ret));
      } else if (OB_FAIL(as->insert_row(ls_id_,
                                        index_tablet_id,
                                        *trans_desc_,
                                        dml_param,
                                        index_ins_ctdef->column_ids_,
                                        *duplicated_column_ids,
                                        *insert_row,
                                        INSERT_RETURN_ALL_DUP,
                                        affected_rows,
                                        duplicated_rows))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          if (OB_ISNULL(duplicated_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("duplicated_row is null", K(ret));
          } else if (is_local_unique_index) {
            // push the duplicated_row of local unique index, ignore the duplicated_row of local non-unique index
            if (OB_FAIL(result_iter->get_duplicated_iter_array().push_back(duplicated_rows))) {
              LOG_WARN("fail to push duplicated_row iter", K(ret));
            } else {
              LOG_DEBUG("insert one row and conflicted", KPC(insert_row));
              ins_rtdef_->is_duplicated_ = true;
              is_duplicated_ = true;
            }
          } else {
            // 需要释放iter的内存， 否则会内存泄漏
            ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
          }
        }
      }
    }
    ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
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
  ObNewRow *dup_row = nullptr;
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
        ins_rtdef_->affected_rows_ += insert_result->get_affected_rows();
        ins_rtdef_->is_duplicated_ |= insert_result->is_duplicated();
      }
    } else {
      result_ = insert_result;
      ins_rtdef_->affected_rows_ += insert_result->get_affected_rows();
      ins_rtdef_->is_duplicated_ |= insert_result->is_duplicated();
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
  if (remote_info != nullptr) {
    //DAS insert is executed remotely
    trans_desc_ = remote_info->trans_desc_;
    snapshot_ = &remote_info->snapshot_;
  }
  return ret;
}

int ObDASInsertOp::write_row(const ExprFixedArray &row,
                             ObEvalCtx &eval_ctx,
                             ObChunkDatumStore::StoredRow *&stored_row,
                             bool &buffer_full)
{
  int ret = OB_SUCCESS;
  bool added = false;
  buffer_full = false;
  if (!insert_buffer_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer not inited", K(ret));
  } else if (OB_FAIL(insert_buffer_.try_add_row(row, &eval_ctx, das::OB_DAS_MAX_PACKET_SIZE, stored_row, added, true))) {
    LOG_WARN("try add row to insert buffer failed", K(ret), K(row), K(insert_buffer_));
  } else if (!added) {
    buffer_full = true;
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
    is_duplicated_(false)
{
}

ObDASInsertResult::~ObDASInsertResult()
{
}

int ObDASInsertResult::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObNewRow *result_row = NULL;
  if (OB_FAIL(result_newrow_iter_.get_next_row(result_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from result iter failed", K(ret));
    }
  } else {
    row = result_row;
  }
  return ret;
}

int ObDASInsertResult::get_next_rows(int64_t &count, int64_t capacity)
{
  UNUSED(count);
  UNUSED(capacity);
  return OB_NOT_IMPLEMENT;
}

int ObDASInsertResult::get_next_row()
{
int ret = OB_NOT_IMPLEMENT;
return ret;
}

int ObDASInsertResult::link_extra_result(ObDASExtraData &extra_result)
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
                    is_duplicated_);


void ObDASConflictIterator::reset()
{
  ObDuplicatedIterList::iterator iter = duplicated_iter_list_.begin();
  for (; iter != duplicated_iter_list_.end(); ++iter) {
    ObQueryIteratorFactory::free_insert_dup_iter(*iter);
  }
  duplicated_iter_list_.reset();
}

int ObDASConflictIterator::get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool find_next_iter = false;
  ObNewRow *dup_row = NULL;
  do {
    if (find_next_iter) {
      ++curr_iter_;
      find_next_iter = false;
    }
    if (curr_iter_ == duplicated_iter_list_.end()) {
      ret = OB_ITER_END;
      LOG_DEBUG("fetch conflict row iterator end");
    } else {
      ObNewRowIterator *dup_row_iter = *curr_iter_;
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

int ObDASConflictIterator::get_next_row()
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
