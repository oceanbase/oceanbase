/**
 * Copyright (c) 2025 OceanBase
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

#include "storage/direct_load/ob_direct_load_dag_insert_table_row_writer.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace share::schema;
using namespace sql;
using namespace share;

/**
 * ObDirectLoadDagInsertTableBatchRowDirectWriter
 */

ObDirectLoadDagInsertTableBatchRowDirectWriter::ObDirectLoadDagInsertTableBatchRowDirectWriter()
  : insert_tablet_ctx_(nullptr),
    dml_row_handler_(nullptr),
    allocator_(ObMemAttr(MTL_ID(), "storage_writer")),
    slice_writer_(nullptr),
    row_count_(0),
    is_inited_(false)
{
}

ObDirectLoadDagInsertTableBatchRowDirectWriter::~ObDirectLoadDagInsertTableBatchRowDirectWriter()
{
  OB_DELETEx(ObITabletSliceWriter, &allocator_, slice_writer_);
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::init(
  ObDirectLoadInsertTabletContext *insert_tablet_ctx, ObDirectLoadDMLRowHandler *dml_row_handler)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDagInsertTableBatchRowDirectWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == insert_tablet_ctx ||
                         !insert_tablet_ctx->get_is_table_without_pk() ||
                         nullptr == dml_row_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agrs", KR(ret), KP(insert_tablet_ctx), KP(dml_row_handler));
  } else {
    tablet_id_ = insert_tablet_ctx->get_tablet_id();
    insert_tablet_ctx_ = insert_tablet_ctx;
    dml_row_handler_ = dml_row_handler;
    if (OB_FAIL(row_handler_.init(insert_tablet_ctx_))) {
      LOG_WARN("fail to init row handler", KR(ret));
    } else if (OB_FAIL(init_batch_rows())) {
      LOG_WARN("fail to init batch rows", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::init_batch_rows()
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
  row_flag.uncontain_hidden_pk_ = true;
  if (OB_UNLIKELY(col_descs->count() != column_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", KR(ret), KPC(col_descs), K(column_count));
  } else if (OB_FAIL(insert_tablet_ctx_->get_row_info(row_info))) {
    LOG_WARN("fail to get row info", KR(ret));
  } else if (OB_FAIL(batch_rows_.init(*col_descs, col_nullables, max_batch_size, row_flag))) {
    LOG_WARN("fail to init batch rows", KR(ret));
  } else if (OB_FAIL(datum_rows_.vectors_.prepare_allocate(column_count + multi_version_col_cnt))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  } else if (OB_FAIL(direct_datum_rows_.vectors_.prepare_allocate(column_count +
                                                                  multi_version_col_cnt))) {
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
    // init direct_datum_rows_
    direct_datum_rows_.row_flag_ = row_info.row_flag_;
    direct_datum_rows_.mvcc_row_flag_ = row_info.mvcc_row_flag_;
    direct_datum_rows_.trans_id_ = row_info.trans_id_;
    for (int64_t i = 0; i < rowkey_column_count + multi_version_col_cnt; ++i) {
      direct_datum_rows_.vectors_.at(i) = datum_rows_.vectors_.at(i);
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::append_batch(
  const ObDirectLoadBatchRows &batch_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableBatchRowDirectWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(batch_rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(batch_rows));
  } else {
    const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    const ObIArray<ObDirectLoadVector *> &vectors = batch_rows.get_vectors();
    for (int64_t i = 1; i < vectors.count(); ++i) {
      direct_datum_rows_.vectors_.at(i + multi_version_col_cnt) = vectors.at(i)->get_vector();
    }
    direct_datum_rows_.row_count_ = batch_rows.size();
    if (OB_FAIL(flush_batch(direct_datum_rows_))) {
      LOG_WARN("fail to flush batch", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::append_selective(
  const ObDirectLoadBatchRows &batch_rows, const uint16_t *selector, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableBatchRowDirectWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(batch_rows.empty() || nullptr == selector || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(batch_rows), KP(selector), K(size));
  } else {
    int64_t remaining = size;
    while (OB_SUCC(ret) && remaining > 0) {
      const int64_t append_size = MIN(remaining, batch_rows_.remain_size());
      if (OB_FAIL(batch_rows_.append_selective(batch_rows, selector, append_size))) {
        LOG_WARN("fail to append selective", KR(ret));
      } else {
        remaining -= append_size;
        selector += append_size;
        if (batch_rows_.full() && OB_FAIL(flush_buffer())) {
          LOG_WARN("fail to flush buffer", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::append_row(
  const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableBatchRowDirectWriter not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(batch_rows_.append_row(datum_row))) {
      LOG_WARN("fail to append row", KR(ret));
    } else if (batch_rows_.full() && OB_FAIL(flush_buffer())) {
      LOG_WARN("fail to flush buffer", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::switch_slice(const bool is_final)
{
  int ret = OB_SUCCESS;
  // close slice
  if (nullptr != slice_writer_ && OB_FAIL(slice_writer_->close())) {
    LOG_WARN("fail to close slice writer", KR(ret));
  } else {
    OB_DELETEx(ObITabletSliceWriter, &allocator_, slice_writer_);
    allocator_.reuse();
  }
  // open slice
  if (OB_FAIL(ret)) {
  } else if (is_final) {
  } else if (OB_FAIL(insert_tablet_ctx_->get_write_ctx(write_ctx_))) {
    LOG_WARN("fail to get write ctx", KR(ret));
  } else if (OB_FAIL(ObDDLUtil::fill_writer_param(
               tablet_id_, write_ctx_.slice_idx_, -1 /*cg_idx*/, insert_tablet_ctx_->get_dag(),
               insert_tablet_ctx_->get_max_batch_size(), write_param_))) {
    LOG_WARN("fail to fill writer param", K(ret));
  } else {
    ObITabletSliceWriter *slice_writer = nullptr;
    if (OB_FAIL(ObDDLUtil::alloc_storage_macro_block_writer(write_param_,
                                                            allocator_,
                                                            slice_writer))) {
      LOG_WARN("fail to alloc storage macro block writer", K(ret), K(write_param_));
    } else {
      slice_writer_ = slice_writer;
    }
  }
  if (OB_SUCC(ret) && !is_final) {
    if (OB_FAIL(row_handler_.switch_slice(write_ctx_.slice_idx_))) {
      LOG_WARN("fail to switch slice", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::flush_buffer()
{
  int ret = OB_SUCCESS;
  datum_rows_.row_count_ = batch_rows_.size();
  if (OB_FAIL(flush_batch(datum_rows_))) {
    LOG_WARN("fail to flush batch", KR(ret));
  } else {
    batch_rows_.reuse();
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::flush_batch(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  const bool need_switch_slice =
    nullptr == slice_writer_ || write_ctx_.pk_interval_.remain_count() < datum_rows.row_count_;
  if (need_switch_slice && OB_FAIL(switch_slice())) {
    LOG_WARN("fail to switch slice", KR(ret));
  } else if (OB_FAIL(ObDirectLoadVectorUtils::batch_fill_hidden_pk(
               datum_rows.vectors_.at(0), 0 /*start*/, datum_rows.row_count_,
               write_ctx_.pk_interval_))) {
    LOG_WARN("fail to batch fill hidden pk", KR(ret));
  } else if (OB_FAIL(row_handler_.handle_batch(datum_rows))) {
    LOG_WARN("fail to handle batch", KR(ret));
  } else if (OB_FAIL(dml_row_handler_->handle_insert_batch(tablet_id_, datum_rows))) {
    LOG_WARN("fail to handle insert batch", KR(ret));
  } else if (OB_FAIL(slice_writer_->append_batch(datum_rows))) {
    LOG_WARN("fail to append batch", KR(ret));
  } else {
    row_count_ += datum_rows.row_count_;
  }
  return ret;
}

int ObDirectLoadDagInsertTableBatchRowDirectWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableBatchRowDirectWriter not init", KR(ret), KP(this));
  } else {
    if (!batch_rows_.empty() && OB_FAIL(flush_buffer())) {
      LOG_WARN("fail to flush buffer", KR(ret));
    } else if (OB_FAIL(switch_slice(true /*is_final*/))) {
      LOG_WARN("fail to switch slice", KR(ret));
    } else if (OB_FAIL(row_handler_.close())) {
      LOG_WARN("fail to close", KR(ret));
    } else {
      if (row_count_ > 0) {
        insert_tablet_ctx_->inc_row_count(row_count_);
      }
      FLOG_INFO("direct add sstable slice", K(tablet_id_), K(row_count_));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
