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

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_dag_insert_table_row_iterator.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_partition_merge_task.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace table;
using namespace sql;

/**
 * ObDirectLoadDagTabletSliceRowIterator
 */

ObDirectLoadDagTabletSliceRowIterator::ObDirectLoadDagTabletSliceRowIterator()
  : allocator_("TLD_RowIter"),
    insert_tablet_ctx_(nullptr),
    slice_idx_(-1),
    row_iters_(),
    dml_row_handler_(nullptr),
    pos_(0),
    row_count_(0),
    is_delete_full_row_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  row_iters_.set_block_allocator(ModulePageAllocator(allocator_));
}

ObDirectLoadDagTabletSliceRowIterator::~ObDirectLoadDagTabletSliceRowIterator()
{
  for (int64_t i = 0; i < row_iters_.count(); ++i) {
    ObDirectLoadIStoreRowIterator *row_iter = row_iters_.at(i);
    if (row_iter != nullptr) {
      row_iter->~ObDirectLoadIStoreRowIterator();
      allocator_.free(row_iter);
      row_iter = nullptr;
    }
  }
  row_iters_.reset();
  allocator_.reset();
}

int ObDirectLoadDagTabletSliceRowIterator::init(
  ObDirectLoadInsertTabletContext *insert_tablet_ctx, const int64_t slice_idx,
  ObDirectLoadPartitionMergeTask *merge_task,
  ObDirectLoadDMLRowHandler *dml_row_handler, bool is_delete_full_row)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDagTabletSliceRowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == insert_tablet_ctx || slice_idx < 0 || nullptr == merge_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(insert_tablet_ctx), K(slice_idx), KP(merge_task));
  } else {
    tablet_id_ = insert_tablet_ctx->get_tablet_id();
    insert_tablet_ctx_ = insert_tablet_ctx;
    slice_idx_ = slice_idx;
    dml_row_handler_ = dml_row_handler;
    is_delete_full_row_ = is_delete_full_row;
    if (OB_FAIL(merge_task->construct_row_iters(row_iters_, allocator_))) {
      LOG_WARN("fail to construct row iters", KR(ret));
    }
    // check row iters
    for (int64_t i = 0; OB_SUCC(ret) && i < row_iters_.count(); ++i) {
      ObDirectLoadIStoreRowIterator *row_iter = row_iters_.at(i);
      if (OB_ISNULL(row_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row iter is null", KR(ret), K(i), K(row_iters_));
      } else if (OB_UNLIKELY(!row_iter->is_valid() || row_iter->get_row_flag().get_column_count(
                                                        row_iter->get_column_count()) !=
                                                        insert_tablet_ctx_->get_column_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row iter", KR(ret), KPC(row_iter));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(row_handler_.init(insert_tablet_ctx_))) {
      LOG_WARN("fail to init row handler", KR(ret));
    } else if (OB_FAIL(row_handler_.switch_slice(slice_idx))) {
      LOG_WARN("fail to switch slice", KR(ret));
    } else if (OB_FAIL(do_init())) {
      LOG_WARN("fail to do init", KR(ret));
    } else {
      is_inited_ = true;
      FLOG_INFO("add sstable slice begin", K(tablet_id_), K(slice_idx_));
    }
  }
  return ret;
}

int ObDirectLoadDagTabletSliceRowIterator::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_handler_.close())) {
    LOG_WARN("fail to close row handler", KR(ret));
  } else {
    insert_tablet_ctx_->inc_row_count(row_count_);
    FLOG_INFO("add sstable slice end", K(tablet_id_), K(slice_idx_), K(row_count_));
  }
  return ret;
}

/**
 * ObDirectLoadDagInsertTableRowIterator
 */

ObDirectLoadDagInsertTableRowIterator::ObDirectLoadDagInsertTableRowIterator()
  : rowkey_column_count_(0), column_count_(0)
{
}

int ObDirectLoadDagInsertTableRowIterator::do_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_tablet_ctx_->init_datum_row(insert_datum_row_))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else if (OB_FAIL(insert_tablet_ctx_->init_datum_row(delete_datum_row_, true /*is_delete*/))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else {
    rowkey_column_count_ = insert_tablet_ctx_->get_rowkey_column_count();
    column_count_ = insert_tablet_ctx_->get_column_count();
  }
  return ret;
}

int ObDirectLoadDagInsertTableRowIterator::inner_get_next_row(ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;
  while (OB_SUCC(ret) && nullptr == result_row) {
    if (pos_ >= row_iters_.count()) {
      ret = OB_ITER_END;
    } else {
      ObDirectLoadIStoreRowIterator *row_iter = row_iters_.at(pos_);
      if (OB_FAIL(row_iter->get_next_row(datum_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          ++pos_; // switch next row iter
        }
      } else if (OB_ISNULL(datum_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected datum row is null", KR(ret));
      } else {
        ObDatumRow *datum_row2 = datum_row->is_delete_ ? &delete_datum_row_ : &insert_datum_row_;
        // rowkey cols
        if (row_iter->get_row_flag().uncontain_hidden_pk_) {
          uint64_t pk_seq = OB_INVALID_ID;
          if (OB_FAIL(row_iter->get_hide_pk_interval()->next_value(pk_seq))) {
            LOG_WARN("fail to get next pk seq", KR(ret));
          } else {
            datum_row2->storage_datums_[0].set_int(pk_seq);
          }
        } else {
          for (int64_t i = 0; i < rowkey_column_count_; ++i) {
            datum_row2->storage_datums_[i] = datum_row->storage_datums_[i];
          }
        }
        // normal cols
        if (OB_SUCC(ret) && (is_delete_full_row_ || !datum_row->is_delete_)) {
          for (int64_t
                 i = row_iter->get_row_flag().uncontain_hidden_pk_ ? 0 : rowkey_column_count_,
                 j = rowkey_column_count_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
               i < datum_row->count_; ++i, ++j) {
            datum_row2->storage_datums_[j] = datum_row->storage_datums_[i];
          }
        }
        if (OB_SUCC(ret)) {
          result_row = datum_row2;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableRowIterator::get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableRowIterator not init", KR(ret), KP(this));
  } else {
    ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(inner_get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(close())) {
          LOG_WARN("fail to close", KR(tmp_ret));
          ret = tmp_ret;
        }
      }
    } else if (OB_FAIL(row_handler_.handle_row(*datum_row))) {
      LOG_WARN("fail to handle row", KR(ret));
    } else {
      row = datum_row;
      ++row_count_;

      if (nullptr != dml_row_handler_) {
        // 只有堆表insert阶段会走到这里
        if (OB_UNLIKELY(datum_row->row_flag_.is_delete())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected delete row", KR(ret), KPC(datum_row));
        } else if (OB_FAIL(dml_row_handler_->handle_insert_row(tablet_id_, *datum_row))) {
          LOG_WARN("fail to handle insert row", KR(ret), KPC(datum_row));
        }
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadDagInsertTableBatchRowIterator
 */

int ObDirectLoadDagInsertTableBatchRowIterator::do_init()
{
  int ret = OB_SUCCESS;
  const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t rowkey_column_count = insert_tablet_ctx_->get_rowkey_column_count();
  const int64_t column_count = insert_tablet_ctx_->get_column_count();
  const int64_t max_batch_size = insert_tablet_ctx_->get_max_batch_size();
  const ObIArray<share::schema::ObColDesc> *col_descs = insert_tablet_ctx_->get_col_descs();
  const ObBitVector *col_nullables = insert_tablet_ctx_->get_col_nullables();
  ObDirectLoadInsertTableRowInfo row_info;
  ObDirectLoadRowFlag row_flag;
  if (OB_UNLIKELY(col_descs->count() != column_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", KR(ret), KPC(col_descs), K(column_count));
  } else if (OB_FAIL(insert_tablet_ctx_->get_row_info(row_info))) {
    LOG_WARN("fail to get row info", KR(ret));
  } else if (OB_FAIL(batch_rows_.init(*col_descs, col_nullables, max_batch_size, row_flag))) {
    LOG_WARN("fail to init batch rows", KR(ret));
  } else if (OB_FAIL(datum_rows_.vectors_.prepare_allocate(column_count + multi_version_col_cnt))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  } else {
    // init datum_rows_
    const ObIArray<ObDirectLoadVector *> &vectors = batch_rows_.get_vectors();
    datum_rows_.row_flag_ = row_info.row_flag_;
    datum_rows_.mvcc_row_flag_ = row_info.mvcc_row_flag_;
    datum_rows_.trans_id_ = row_info.trans_id_;
    for (int64_t i = 0; i < vectors.count(); ++i) {
      if (i < rowkey_column_count) {
        datum_rows_.vectors_.at(i) = vectors.at(i)->get_vector();
      } else {
        datum_rows_.vectors_.at(i + multi_version_col_cnt) = vectors.at(i)->get_vector();
      }
    }
    datum_rows_.vectors_.at(rowkey_column_count) = row_info.trans_version_vector_;
    datum_rows_.vectors_.at(rowkey_column_count + 1) = row_info.seq_no_vector_;
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowIterator::inner_get_next_batch(ObBatchDatumRows *&datum_rows)
{
  int ret = OB_SUCCESS;
  datum_rows = nullptr;
  batch_rows_.reuse();
  while (OB_SUCC(ret) && !batch_rows_.full()) {
    if (pos_ >= row_iters_.count()) {
      break;
    } else {
      ObDirectLoadIStoreRowIterator *row_iter = row_iters_.at(pos_);
      const ObDirectLoadRowFlag &row_flag = row_iter->get_row_flag();
      ObTabletCacheInterval *hide_pk_interval = row_iter->get_hide_pk_interval();
      int64_t start_pos = batch_rows_.size();
      batch_rows_.set_row_flag(row_flag);

      const ObDirectLoadDatumRow *datum_row = nullptr;
      while (OB_SUCC(ret) && !batch_rows_.full()) {
        if (OB_FAIL(row_iter->get_next_row(datum_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            ++pos_; // switch next row iter
            break;
          }
        } else if (OB_ISNULL(datum_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected datum row is null", KR(ret));
        }
        // 有删除行的情况不走batch模式
        else if (OB_UNLIKELY(datum_row->is_delete_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected delete row", KR(ret), KPC(datum_row));
        } else if (OB_FAIL(batch_rows_.append_row(*datum_row))) {
          LOG_WARN("fail to append row", KR(ret));
        }
      }

      if (OB_SUCC(ret) && row_flag.uncontain_hidden_pk_ && batch_rows_.size() > start_pos) {
        if (OB_FAIL(ObDirectLoadVectorUtils::batch_fill_hidden_pk(
              datum_rows_.vectors_.at(0), start_pos, batch_rows_.size() - start_pos,
              *hide_pk_interval))) {
          LOG_WARN("fail to batch fill hidden pk", KR(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (batch_rows_.empty()) {
      ret = OB_ITER_END;
    } else {
      datum_rows = &datum_rows_;
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowIterator::get_next_batch(const ObBatchDatumRows *&result_rows)
{
  int ret = OB_SUCCESS;
  result_rows = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableRowIterator not init", KR(ret), KP(this));
  } else {
    ObBatchDatumRows *datum_rows = nullptr;
    if (OB_FAIL(inner_get_next_batch(datum_rows))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next batch", KR(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(close())) {
          LOG_WARN("fail to close", KR(tmp_ret));
          ret = tmp_ret;
        }
      }
    } else if (OB_FAIL(row_handler_.handle_batch(*datum_rows))) {
      LOG_WARN("fail to handle row", KR(ret));
    } else {
      result_rows = datum_rows;
      row_count_ += datum_rows->row_count_;

      if (nullptr != dml_row_handler_) {
        if (OB_FAIL(dml_row_handler_->handle_insert_batch(tablet_id_, *datum_rows))) {
          LOG_WARN("fail to handle insert batch", KR(ret), KPC(datum_rows));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
