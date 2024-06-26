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

#include "storage/direct_load/ob_direct_load_fast_heap_table_builder.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_fast_heap_table.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace table;

/**
 * ObDirectLoadFastHeapTableBuildParam
 */

ObDirectLoadFastHeapTableBuildParam::ObDirectLoadFastHeapTableBuildParam()
  : insert_table_ctx_(nullptr), dml_row_handler_(nullptr)
{
}

ObDirectLoadFastHeapTableBuildParam::~ObDirectLoadFastHeapTableBuildParam() {}

bool ObDirectLoadFastHeapTableBuildParam::is_valid() const
{
  return tablet_id_.is_valid() && table_data_desc_.is_valid() && nullptr != insert_table_ctx_ &&
         nullptr != dml_row_handler_;
}

/**
 * RowIterator
 */

ObDirectLoadFastHeapTableBuilder::RowIterator::RowIterator() : datum_row_(nullptr) {}

ObDirectLoadFastHeapTableBuilder::RowIterator::~RowIterator() {}

int ObDirectLoadFastHeapTableBuilder::RowIterator::init(
  ObDirectLoadInsertTabletContext *insert_tablet_ctx,
  ObTableLoadSqlStatistics *sql_statistics,
  ObDirectLoadLobBuilder &lob_builder)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadFastHeapTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_FAIL(inner_init(insert_tablet_ctx, sql_statistics, lob_builder))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::RowIterator::set_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("RowIterator not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr != datum_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set row twice", KR(ret));
  } else {
    datum_row_ = &row;
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::RowIterator::inner_get_next_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("RowIterator not init", KR(ret), KP(this));
  } else if (nullptr == datum_row_) {
    ret = OB_ITER_END;
  } else {
    row = datum_row_;
    datum_row_ = nullptr;
  }
  return ret;
}

/**
 * ObDirectLoadFastHeapTableBuilder
 */

ObDirectLoadFastHeapTableBuilder::ObDirectLoadFastHeapTableBuilder()
  : insert_tablet_ctx_(nullptr),
    sql_statistics_(nullptr),
    current_slice_id_(0),
    row_count_(0),
    has_lob_(false),
    is_closed_(false),
    is_inited_(false)
{
}

ObDirectLoadFastHeapTableBuilder::~ObDirectLoadFastHeapTableBuilder()
{
}

int ObDirectLoadFastHeapTableBuilder::init(const ObDirectLoadFastHeapTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadFastHeapTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else if (OB_FAIL(
               param.insert_table_ctx_->get_tablet_context(param.tablet_id_, insert_tablet_ctx_))) {
    LOG_WARN("fail to get tablet context", KR(ret));
  } else {
    param_ = param;
    const bool online_opt_stat_gather = insert_tablet_ctx_->get_online_opt_stat_gather();
    has_lob_ = insert_tablet_ctx_->has_lob_storage();
    if (online_opt_stat_gather &&
        OB_FAIL(param.insert_table_ctx_->get_sql_statistics(sql_statistics_))) {
      LOG_WARN("fail to get sql statistics", KR(ret));
    } else if (has_lob_ && OB_FAIL(lob_builder_.init(insert_tablet_ctx_))) {
      LOG_WARN("fail to inner init sql statistics", KR(ret));
    } else if (OB_FAIL(init_sstable_slice_ctx())) {
      LOG_WARN("fail to init sstable slice ctx", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx_->init_datum_row(datum_row_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(row_iter_.init(insert_tablet_ctx_, sql_statistics_, lob_builder_))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::init_sstable_slice_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_tablet_ctx_->get_write_ctx(write_ctx_))) {
    LOG_WARN("fail to get write ctx", KR(ret));
  } else if (OB_FAIL(
               insert_tablet_ctx_->open_sstable_slice(write_ctx_.start_seq_, current_slice_id_))) {
    LOG_WARN("fail to open sstable slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::switch_sstable_slice()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_tablet_ctx_->close_sstable_slice(current_slice_id_))) {
    LOG_WARN("fail to close sstable slice builder", KR(ret));
  } else if (OB_FAIL(init_sstable_slice_ctx())) {
    LOG_WARN("fail to init sstable slice ctx", KR(ret));
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::append_row(const ObTabletID &tablet_id,
                                                 const table::ObTableLoadSequenceNo &seq_no,
                                                 const ObDatumRow &datum_row)
{
  UNUSED(tablet_id);
  UNUSED(seq_no);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadFastHeapTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fast heap table builder is closed", KR(ret));
  } else if (OB_FAIL(!datum_row.is_valid() ||
                     datum_row.get_column_count() != param_.table_data_desc_.column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), K(param_.table_data_desc_.column_count_));
  } else {
    uint64_t pk_seq = OB_INVALID_ID;
    int64_t affected_rows = 0;
    if (OB_FAIL(write_ctx_.pk_interval_.next_value(pk_seq))) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to get next pk seq", KR(ret));
      } else if (OB_FAIL(switch_sstable_slice())) {
        LOG_WARN("fail to switch sstable slice", KR(ret));
      } else if (OB_FAIL(write_ctx_.pk_interval_.next_value(pk_seq))) {
        LOG_WARN("fail to get next pk seq", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      datum_row_.storage_datums_[0].set_int(pk_seq);
      for (int64_t i = 0, j = HIDDEN_ROWKEY_COLUMN_NUM +
                              ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
           i < datum_row.count_; ++i, ++j) {
        datum_row_.storage_datums_[j] = datum_row.storage_datums_[i];
      }
      if (OB_FAIL(row_iter_.set_row(datum_row_))) {
        LOG_WARN("fail to set row", KR(ret));
      } else if (OB_FAIL(insert_tablet_ctx_->fill_sstable_slice(current_slice_id_, row_iter_,
                                                                affected_rows))) {
        LOG_WARN("fail to fill sstable slice", KR(ret));
      } else {
        ++row_count_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(param_.dml_row_handler_->handle_insert_row(datum_row_))) {
        LOG_WARN("fail to handle insert row", KR(ret), K_(datum_row));
      }
    }
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadFastHeapTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fast heap table builder is closed", KR(ret));
  } else {
    if (has_lob_ && OB_FAIL(lob_builder_.close())) {
      LOG_WARN("fail to close lob_builder", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx_->close_sstable_slice(current_slice_id_))) {
      LOG_WARN("fail to close sstable slice builder", KR(ret));
    } else {
      current_slice_id_ = 0;
      is_closed_ = true;
      insert_tablet_ctx_->inc_row_count(row_count_);
    }
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::get_tables(
  ObIArray<ObIDirectLoadPartitionTable *> &table_array, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadFastHeapTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fast heap table builder not closed", KR(ret));
  } else if (row_count_ == 0) {
    // do nothing
  } else {
    ObDirectLoadFastHeapTableCreateParam create_param;
    create_param.tablet_id_ = param_.tablet_id_;
    create_param.row_count_ = row_count_;
    ObDirectLoadFastHeapTable *fast_heap_table = nullptr;
    if (OB_ISNULL(fast_heap_table = OB_NEWx(ObDirectLoadFastHeapTable, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadFastHeapTable", KR(ret));
    } else if (OB_FAIL(fast_heap_table->init(create_param))) {
      LOG_WARN("fail to init sstable", KR(ret), K(create_param));
    } else if (OB_FAIL(table_array.push_back(fast_heap_table))) {
      LOG_WARN("fail to push back sstable", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != fast_heap_table) {
        fast_heap_table->~ObDirectLoadFastHeapTable();
        allocator.free(fast_heap_table);
        fast_heap_table = nullptr;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
