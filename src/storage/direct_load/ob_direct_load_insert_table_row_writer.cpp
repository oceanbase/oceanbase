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
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_insert_table_row_writer.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace share::schema;
using namespace sql;

/**
 * ObDirectLoadInsertTableBatchRowBufferWriter
 */

ObDirectLoadInsertTableBatchRowBufferWriter::ObDirectLoadInsertTableBatchRowBufferWriter()
  : allocator_("TLD_BatchBW"),
    insert_tablet_ctx_(nullptr),
    row_handler_(),
    buffer_(),
    datum_rows_(),
    tablet_id_(),
    slice_id_(0),
    row_count_(0),
    is_canceled_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadInsertTableBatchRowBufferWriter::~ObDirectLoadInsertTableBatchRowBufferWriter() {}

int ObDirectLoadInsertTableBatchRowBufferWriter::inner_init(
  ObDirectLoadInsertTabletContext *insert_tablet_ctx,
  const ObDirectLoadInsertTableRowInfo &row_info,
  ObIAllocator *lob_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == insert_tablet_ctx || !row_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(insert_tablet_ctx), K(row_info));
  } else {
    const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    const int64_t rowkey_column_count = insert_tablet_ctx->get_rowkey_column_count();
    const int64_t column_count = insert_tablet_ctx->get_column_count();
    const int64_t max_batch_size = insert_tablet_ctx->get_max_batch_size();
    const ObIArray<share::schema::ObColDesc> *col_descs = insert_tablet_ctx->get_col_descs();
    ObIVector *trans_version_vector = nullptr;
    ObIVector *seq_no_vector = nullptr;
    if (OB_UNLIKELY(col_descs->count() != column_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", KR(ret), KPC(col_descs), K(column_count));
    } else if (OB_FAIL(buffer_.init(*col_descs, max_batch_size))) {
      LOG_WARN("fail to init buffer", KR(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::make_const_multi_version_vector(
                 -row_info.trans_version_, allocator_, trans_version_vector))) {
      LOG_WARN("fail to make trans version vector", KR(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::make_const_multi_version_vector(
                 -row_info.seq_no_, allocator_, seq_no_vector))) {
      LOG_WARN("fail to make seq no vector", KR(ret));
    } else if (OB_FAIL(
                 datum_rows_.vectors_.prepare_allocate(column_count + multi_version_col_cnt))) {
      LOG_WARN("fail to prepare allocate", KR(ret));
    } else {
      datum_rows_.row_flag_ = row_info.row_flag_;
      datum_rows_.mvcc_row_flag_ = row_info.mvcc_row_flag_;
      datum_rows_.trans_id_ = row_info.trans_id_;
      const IVectorPtrs &vectors = buffer_.get_vectors();
      for (int64_t i = 0; i < vectors.count(); ++i) {
        if (i < rowkey_column_count) {
          datum_rows_.vectors_.at(i) = vectors.at(i);
        } else {
          datum_rows_.vectors_.at(i + multi_version_col_cnt) = vectors.at(i);
        }
      }
      datum_rows_.vectors_.at(rowkey_column_count) = trans_version_vector;
      datum_rows_.vectors_.at(rowkey_column_count + 1) = seq_no_vector;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_handler_.init(insert_tablet_ctx, lob_allocator))) {
        LOG_WARN("fail to init row handler", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      insert_tablet_ctx_ = insert_tablet_ctx;
      tablet_id_ = insert_tablet_ctx->get_tablet_id();
    }
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowBufferWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableBatchRowBufferWriter not init", KR(ret), KP(this));
  } else {
    if (!buffer_.empty() && OB_FAIL(flush_buffer())) {
      LOG_WARN("fail to flush buffer", KR(ret));
    } else if (OB_FAIL(row_handler_.close())) {
      LOG_WARN("fail to close row handler", KR(ret));
    } else {
      insert_tablet_ctx_->inc_row_count(row_count_);
    }
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowBufferWriter::flush_buffer()
{
  int ret = OB_SUCCESS;
  datum_rows_.row_count_ = buffer_.get_row_count();
  if (OB_FAIL(flush_batch(datum_rows_))) {
    LOG_WARN("fail to flush batch", KR(ret));
  } else {
    buffer_.reuse();
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowBufferWriter::flush_batch(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(before_flush_batch(datum_rows))) {
    LOG_WARN("fail to before flush batch", KR(ret));
  } else if (OB_FAIL(insert_tablet_ctx_->fill_sstable_slice(slice_id_, datum_rows))) {
    LOG_WARN("fail to fill sstable slice", KR(ret), K(slice_id_));
  } else if (OB_FAIL(after_flush_batch(datum_rows))) {
    LOG_WARN("fail to after flush batch", KR(ret));
  } else {
    row_count_ += datum_rows.row_count_;
  }
  return ret;
}

/**
 * ObDirectLoadInsertTableBatchRowDirectWriter
 */

int ObDirectLoadInsertTableBatchRowDirectWriter::init(
  ObDirectLoadInsertTabletContext *insert_tablet_ctx,
  const ObDirectLoadInsertTableRowInfo &row_info,
  ObDirectLoadDMLRowHandler *dml_row_handler,
  ObIAllocator *lob_allocator,
  ObLoadDataStat *job_stat)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableBatchRowDirectWriter init twice", KR(ret), KP(this));
  } else if (OB_FAIL(inner_init(insert_tablet_ctx, row_info, lob_allocator))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_UNLIKELY(nullptr == dml_row_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agrs", KR(ret), KP(dml_row_handler));
  } else if (OB_UNLIKELY(!insert_tablet_ctx->get_is_table_without_pk())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not heap table", KR(ret));
  } else if (OB_FAIL(init_sstable_slice())) {
    LOG_WARN("fail to init sstable slice", KR(ret));
  } else {
    const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    const int64_t column_count = insert_tablet_ctx->get_column_count();
    if (OB_FAIL(
          direct_datum_rows_.vectors_.prepare_allocate(column_count + multi_version_col_cnt))) {
      LOG_WARN("fail to prepare allocate", KR(ret));
    } else {
      for (int64_t i = 0; i < multi_version_col_cnt + 1; ++i) {
        direct_datum_rows_.vectors_.at(i) = datum_rows_.vectors_.at(i);
      }
      direct_datum_rows_.row_flag_ = row_info.row_flag_;
      direct_datum_rows_.mvcc_row_flag_ = row_info.mvcc_row_flag_;
      direct_datum_rows_.trans_id_ = row_info.trans_id_;
      dml_row_handler_ = dml_row_handler;
      job_stat_ = job_stat;
      expect_column_count_ = column_count - 1;
      row_flag_.uncontain_hidden_pk_ = true;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::init_sstable_slice()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_tablet_ctx_->get_write_ctx(write_ctx_))) {
    LOG_WARN("fail to get write ctx", KR(ret));
  } else if (OB_FAIL(insert_tablet_ctx_->open_sstable_slice(write_ctx_.start_seq_, write_ctx_.slice_idx_, slice_id_))) {
    LOG_WARN("fail to open sstable slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::close_sstable_slice()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_tablet_ctx_->close_sstable_slice(slice_id_, write_ctx_.slice_idx_))) {
    LOG_WARN("fail to close sstable slice", KR(ret));
  } else {
    slice_id_ = 0;
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::switch_sstable_slice()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(close_sstable_slice())) {
    LOG_WARN("fail to close sstable slice", KR(ret));
  } else if (OB_FAIL(init_sstable_slice())) {
    LOG_WARN("fail to init sstable slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::before_flush_batch(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (write_ctx_.pk_interval_.remain_count() < datum_rows.row_count_ &&
      OB_FAIL(switch_sstable_slice())) {
    LOG_WARN("fail to switch sstable slice", KR(ret));
  } else if (OB_FAIL(ObDirectLoadVectorUtils::batch_fill_hidden_pk(datum_rows.vectors_.at(0),
                                                                   0 /*start*/,
                                                                   datum_rows.row_count_,
                                                                   write_ctx_.pk_interval_))) {
    LOG_WARN("fail to batch fill hidden pk", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::after_flush_batch(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(job_stat_)) {
    ATOMIC_AAF(&job_stat_->store_.merge_stage_write_rows_, datum_rows.row_count_);
  }
  if (OB_FAIL(dml_row_handler_->handle_insert_batch(tablet_id_, datum_rows))) {
    LOG_WARN("fail to handle insert batch", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::append_batch(const IVectorPtrs &vectors,
                                                              const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableBatchRowDirectWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != expect_column_count_ || batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(expect_column_count_), K(vectors.count()), K(batch_size));
  } else {
    const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    for (int64_t i = 0; i < vectors.count(); ++i) {
      direct_datum_rows_.vectors_.at(i + multi_version_col_cnt + 1) = vectors.at(i);
    }
    direct_datum_rows_.row_count_ = batch_size;
    if (OB_FAIL(row_handler_.handle_batch(direct_datum_rows_))) {
      LOG_WARN("fail to handle batch", KR(ret));
    } else if (OB_FAIL(flush_batch(direct_datum_rows_))) {
      LOG_WARN("fail to flush batch", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::append_row(const IVectorPtrs &vectors,
                                                            const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableBatchRowDirectWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != expect_column_count_ || row_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(expect_column_count_), K(vectors.count()), K(row_idx));
  } else {
    bool is_full = false;
    // 先处理lob列, 降低内存压力
    if (OB_FAIL(row_handler_.handle_row(vectors, row_idx, row_flag_))) {
      LOG_WARN("fail to handle row", KR(ret));
    } else if (OB_FAIL(buffer_.append_row(vectors, row_idx, row_flag_, is_full))) {
      LOG_WARN("fail to append row", KR(ret));
    } else if (is_full) {
      if (OB_FAIL(flush_buffer())) {
        LOG_WARN("fail to flush buffer", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::append_row(const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableBatchRowDirectWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_row.count_ != expect_column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(expect_column_count_), K(datum_row.count_));
  } else {
    bool is_full = false;
    // 先处理lob列, 降低内存压力
    if (OB_FAIL(row_handler_.handle_row(const_cast<ObDirectLoadDatumRow &>(datum_row), row_flag_))) {
      LOG_WARN("fail to handle row", KR(ret));
    } else if (OB_FAIL(buffer_.append_row(datum_row, row_flag_, is_full))) {
      LOG_WARN("fail to append row", KR(ret));
    } else if (is_full) {
      if (OB_FAIL(flush_buffer())) {
        LOG_WARN("fail to flush buffer", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowDirectWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableBatchRowDirectWriter not init", KR(ret), KP(this));
  } else if (OB_FAIL(ObDirectLoadInsertTableBatchRowBufferWriter::close())) {
    LOG_WARN("fail to close buffer writer", KR(ret));
  } else if (OB_FAIL(close_sstable_slice())) {
    LOG_WARN("fail to close sstable slice", KR(ret));
  }
  return ret;
}

/*
 * ObDirectLoadInsertTableBatchRowStoreWriter
 */

int ObDirectLoadInsertTableBatchRowStoreWriter::init(
  ObDirectLoadInsertTabletContext *insert_tablet_ctx,
  const ObDirectLoadInsertTableRowInfo &row_info,
  const int64_t slice_id,
  ObDirectLoadDMLRowHandler *dml_row_handler,
  ObLoadDataStat *job_stat)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableBatchRowStoreWriter init twice", KR(ret), KP(this));
  } else if (OB_FAIL(inner_init(insert_tablet_ctx, row_info))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_UNLIKELY(slice_id <= 0 || nullptr == job_stat)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(slice_id), KP(job_stat));
  } else {
    slice_id_ = slice_id;
    dml_row_handler_ = dml_row_handler;
    job_stat_ = job_stat;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowStoreWriter::after_flush_batch(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  ATOMIC_AAF(&job_stat_->store_.merge_stage_write_rows_, datum_rows.row_count_);
  if (nullptr != dml_row_handler_ &&
      OB_FAIL(dml_row_handler_->handle_insert_batch(tablet_id_, datum_rows))) {
    LOG_WARN("fail to handle insert batch", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableBatchRowStoreWriter::write(ObDirectLoadIStoreRowIterator *row_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableBatchRowStoreWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(row_iter));
  } else {
    const ObDirectLoadRowFlag &row_flag = row_iter->get_row_flag();
    ObTabletCacheInterval *hide_pk_interval = row_iter->get_hide_pk_interval();
    int64_t start_pos = buffer_.get_row_count();
    bool is_full = false;
    const ObDirectLoadDatumRow *datum_row = nullptr;
    if (OB_UNLIKELY(row_flag.uncontain_hidden_pk_ && nullptr == hide_pk_interval)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid row iter", KR(ret), KPC(row_iter));
    }
    while (OB_SUCC(ret)) {
      if (OB_UNLIKELY(is_canceled_)) {
        ret = OB_CANCELED;
        LOG_WARN("merge task is canceled", KR(ret));
      } else if (OB_FAIL(row_iter->get_next_row(datum_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
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
      }
      // 先处理lob列, 降低内存压力
      else if (OB_FAIL(row_handler_.handle_row(const_cast<ObDirectLoadDatumRow &>(*datum_row), row_flag))) {
        LOG_WARN("fail to handle row", KR(ret), KPC(datum_row), K(row_flag));
      } else if (OB_FAIL(buffer_.append_row(*datum_row, row_flag, is_full))) {
        LOG_WARN("fail to append row", KR(ret));
      } else if (is_full) {
        if (row_flag.uncontain_hidden_pk_ &&
            OB_FAIL(ObDirectLoadVectorUtils::batch_fill_hidden_pk(datum_rows_.vectors_.at(0),
                                                                  start_pos,
                                                                  buffer_.get_row_count() - start_pos,
                                                                  *hide_pk_interval))) {
          LOG_WARN("fail to batch fill hidden pk", KR(ret));
        } else if (OB_FAIL(flush_buffer())) {
          LOG_WARN("fail to flush buffer", KR(ret));
        } else {
          start_pos = 0;
        }
      }
    }
    if (OB_SUCC(ret) && !buffer_.empty() && row_flag.uncontain_hidden_pk_) {
      if (OB_FAIL(ObDirectLoadVectorUtils::batch_fill_hidden_pk(datum_rows_.vectors_.at(0),
                                                                start_pos,
                                                                buffer_.get_row_count() - start_pos,
                                                                *hide_pk_interval))) {
        LOG_WARN("fail to batch fill hidden pk", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
