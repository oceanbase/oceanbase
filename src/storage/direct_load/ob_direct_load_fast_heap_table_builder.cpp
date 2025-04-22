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
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_stat_define.h"
#include "share/table/ob_table_load_define.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx.h"
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

/**
 * ObDirectLoadFastHeapTableBuildParam
 */

ObDirectLoadFastHeapTableBuildParam::ObDirectLoadFastHeapTableBuildParam()
  : snapshot_version_(0),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    insert_table_ctx_(nullptr),
    fast_heap_table_ctx_(nullptr),
    dml_row_handler_(nullptr)
{
}

ObDirectLoadFastHeapTableBuildParam::~ObDirectLoadFastHeapTableBuildParam()
{
}

bool ObDirectLoadFastHeapTableBuildParam::is_valid() const
{
  return tablet_id_.is_valid() && snapshot_version_ > 0 &&
         table_data_desc_.is_valid() && nullptr != col_descs_ && nullptr != insert_table_ctx_ &&
         nullptr != fast_heap_table_ctx_ && nullptr != dml_row_handler_ && nullptr != datum_utils_;
}

/**
 * ObDirectLoadFastHeapTableBuilder
 */

ObDirectLoadFastHeapTableBuilder::ObDirectLoadFastHeapTableBuilder()
  : allocator_("TLD_FastHTable"),
    slice_writer_allocator_("TLD_SliceWriter"),
    fast_heap_table_tablet_ctx_(nullptr),
    slice_writer_(nullptr),
    sql_statistics_(nullptr),
    row_count_(0),
    is_closed_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  slice_writer_allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadFastHeapTableBuilder::~ObDirectLoadFastHeapTableBuilder()
{
  if (nullptr != slice_writer_) {
    slice_writer_->~ObSSTableInsertSliceWriter();
    slice_writer_allocator_.free(slice_writer_);
    slice_writer_ = nullptr;
  }
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
  } else {
    param_ = param;
    if (OB_FAIL(param_.insert_table_ctx_->get_sql_statistics(sql_statistics_))) {
      LOG_WARN("fail to get sql statistics", KR(ret));
    } else if (OB_FAIL(param_.fast_heap_table_ctx_->get_tablet_context(
                 param_.tablet_id_, fast_heap_table_tablet_ctx_))) {
      LOG_WARN("fail to get tablet context", KR(ret));
    } else if (OB_FAIL(init_sstable_slice_ctx())) {
      LOG_WARN("fail to init sstable slice ctx", KR(ret));
    } else if (OB_FAIL(datum_row_.init(param.table_data_desc_.column_count_ +
                                       HIDDEN_ROWKEY_COLUMN_NUM +
                                       ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row_.storage_datums_[HIDDEN_ROWKEY_COLUMN_NUM].set_int(-param_.snapshot_version_); // fill trans_version
      datum_row_.storage_datums_[HIDDEN_ROWKEY_COLUMN_NUM + 1].set_int(0); // fill sql_no
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::init_sstable_slice_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fast_heap_table_tablet_ctx_->get_write_ctx(write_ctx_))) {
    LOG_WARN("fail to get write ctx", KR(ret));
  } else if (OB_FAIL(param_.insert_table_ctx_->construct_sstable_slice_writer(
               fast_heap_table_tablet_ctx_->get_target_tablet_id(),
               write_ctx_.start_seq_,
               slice_writer_,
               slice_writer_allocator_))) {
    LOG_WARN("fail to construct sstable slice writer", KR(ret));
  }
  return ret;
}

int ObDirectLoadFastHeapTableBuilder::switch_sstable_slice()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(slice_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null slice builder", KR(ret));
  } else if (OB_FAIL(slice_writer_->close())) {
    LOG_WARN("fail to close sstable slice builder", KR(ret));
  } else {
    slice_writer_->~ObSSTableInsertSliceWriter();
    slice_writer_allocator_.reuse();
    if (OB_FAIL(init_sstable_slice_ctx())) {
      LOG_WARN("fail to init sstable slice ctx", KR(ret));
    }
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
      if (OB_FAIL(slice_writer_->append_row(datum_row_))) {
        LOG_WARN("fail to append row", KR(ret));
      } else if (nullptr != sql_statistics_ &&
                 OB_FAIL(param_.insert_table_ctx_->update_sql_statistics(*sql_statistics_, datum_row_))) {
        LOG_WARN("fail to update sql statistics", KR(ret));
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
    if (OB_FAIL(slice_writer_->close())) {
      LOG_WARN("fail to close sstable slice writer", KR(ret));
    } else {
      param_.insert_table_ctx_->inc_row_count(row_count_);
      is_closed_ = true;
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
