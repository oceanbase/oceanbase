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

#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace table;
using namespace sql;

ObDirectLoadInsertTableRowIterator::ObDirectLoadInsertTableRowIterator()
  : insert_tablet_ctx_(nullptr),
    row_iters_(nullptr),
    dml_row_handler_(nullptr),
    job_stat_(nullptr),
    rowkey_column_count_(0),
    column_count_(0),
    pos_(0),
    row_count_(0),
    is_inited_(false)
{
}

ObDirectLoadInsertTableRowIterator::~ObDirectLoadInsertTableRowIterator() {}

int ObDirectLoadInsertTableRowIterator::init(
  ObDirectLoadInsertTabletContext *insert_tablet_ctx,
  const ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
  ObDirectLoadDMLRowHandler *dml_row_handler,
  ObLoadDataStat *job_stat)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableRowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == insert_tablet_ctx || nullptr == job_stat)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(insert_tablet_ctx), KP(job_stat));
  } else {
    // check row iters
    for (int64_t i = 0; OB_SUCC(ret) && i < row_iters.count(); ++i) {
      ObDirectLoadIStoreRowIterator *row_iter = row_iters.at(i);
      if (OB_ISNULL(row_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row iter is null", KR(ret), K(i), K(row_iters));
      } else if (OB_UNLIKELY(!row_iter->is_valid() || row_iter->get_row_flag().get_column_count(
                                                        row_iter->get_column_count()) !=
                                                        insert_tablet_ctx->get_column_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row iter", KR(ret), KPC(row_iter));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(row_handler_.init(insert_tablet_ctx))) {
      LOG_WARN("fail to init row handler", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->init_datum_row(insert_datum_row_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->init_datum_row(delete_datum_row_, true /*is_delete*/))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      insert_tablet_ctx_ = insert_tablet_ctx;
      dml_row_handler_ = dml_row_handler;
      row_iters_ = &row_iters;
      job_stat_ = job_stat;
      rowkey_column_count_ = insert_tablet_ctx->get_rowkey_column_count();
      column_count_ = insert_tablet_ctx->get_column_count();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowIterator::get_next_row(const ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  ret = get_next_row(false, result_row);
  if (OB_UNLIKELY(ret != OB_ITER_END && ret != OB_SUCCESS)) {
    LOG_WARN("fail to get next row", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableRowIterator::get_next_row(const bool skip_lob,
                                                     const ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableRowIterator not init", KR(ret), KP(this));
  } else {
    ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(inner_get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else if (OB_FAIL(row_handler_.handle_row(*datum_row, skip_lob))) {
      LOG_WARN("fail to handle row", KR(ret));
    } else {
      result_row = datum_row;
      ++row_count_;
      ATOMIC_AAF(&job_stat_->store_.merge_stage_write_rows_, 1);
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowIterator::inner_get_next_row(ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;
  while (OB_SUCC(ret) && nullptr == result_row) {
    if (pos_ >= row_iters_->count()) {
      ret = OB_ITER_END;
    } else {
      ObDirectLoadIStoreRowIterator *row_iter = row_iters_->at(pos_);
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
        ObDatumRow *datum_row2 =
          datum_row->is_delete_ ? &delete_datum_row_ : &insert_datum_row_;
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
        if (OB_SUCC(ret) && !datum_row->is_delete_) {
          for (int64_t
                 i = row_iter->get_row_flag().uncontain_hidden_pk_ ? 0 : rowkey_column_count_,
                 j = rowkey_column_count_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
               i < datum_row->count_; ++i, ++j) {
            datum_row2->storage_datums_[j] = datum_row->storage_datums_[i];
          }
        }
        if (OB_SUCC(ret) && nullptr != dml_row_handler_) {
          // 只有堆表insert阶段会走到这里
          if (OB_UNLIKELY(datum_row->is_delete_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected delete row", KR(ret), KPC(datum_row));
          } else if (OB_FAIL(dml_row_handler_->handle_insert_row(insert_tablet_ctx_->get_tablet_id(), *datum_row2))) {
            LOG_WARN("fail to handle insert row", KR(ret), KPC(datum_row2));
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

int ObDirectLoadInsertTableRowIterator::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableRowIterator not init", KR(ret), KP(this));
  } else if (OB_FAIL(row_handler_.close())) {
    LOG_WARN("fail to close row handler", KR(ret));
  } else {
    insert_tablet_ctx_->inc_row_count(row_count_);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
