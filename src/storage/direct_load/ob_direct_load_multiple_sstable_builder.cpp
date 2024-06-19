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

#include "storage/direct_load/ob_direct_load_multiple_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/*
 * ObDirectLoadMultipleSSTableBuildParam
 */

ObDirectLoadMultipleSSTableBuildParam::ObDirectLoadMultipleSSTableBuildParam()
  : datum_utils_(nullptr), file_mgr_(nullptr), extra_buf_(nullptr), extra_buf_size_(0)
{
}

ObDirectLoadMultipleSSTableBuildParam::~ObDirectLoadMultipleSSTableBuildParam()
{
}

bool ObDirectLoadMultipleSSTableBuildParam::is_valid() const
{
  return table_data_desc_.is_valid() && nullptr != datum_utils_ && nullptr != file_mgr_ &&
         nullptr != extra_buf_ && extra_buf_size_ > 0 && extra_buf_size_ % DIO_ALIGN_SIZE == 0;
}

/**
 * DataBlockFlushCallback
 */

ObDirectLoadMultipleSSTableBuilder::DataBlockFlushCallback::DataBlockFlushCallback()
  : index_block_writer_(nullptr), is_inited_(false)
{
}

ObDirectLoadMultipleSSTableBuilder::DataBlockFlushCallback::~DataBlockFlushCallback()
{
}

int ObDirectLoadMultipleSSTableBuilder::DataBlockFlushCallback::init(
  ObDirectLoadSSTableIndexBlockWriter *index_block_writer)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("DataBlockFlushCallback init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == index_block_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(index_block_writer));
  } else {
    index_block_writer_ = index_block_writer;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMultipleSSTableBuilder::DataBlockFlushCallback::write(char *buf, int64_t buf_size,
                                                                      int64_t offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("DataBlockFlushCallback not init", KR(ret), KP(this));
  } else {
    ObDirectLoadSSTableIndexEntry entry;
    entry.offset_ = offset;
    entry.size_ = buf_size;
    if (OB_FAIL(index_block_writer_->append_entry(entry))) {
      LOG_WARN("fail to append entry", KR(ret));
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableBuilder
 */

ObDirectLoadMultipleSSTableBuilder::ObDirectLoadMultipleSSTableBuilder()
  : allocator_("TLD_MSST_Build"),
    last_rowkey_allocator_("TLD_LastPK"),
    row_count_(0),
    is_closed_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  last_rowkey_allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleSSTableBuilder::~ObDirectLoadMultipleSSTableBuilder()
{
}

int ObDirectLoadMultipleSSTableBuilder::init(const ObDirectLoadMultipleSSTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    param_ = param;
    int64_t dir_id = -1;
    if (OB_FAIL(param_.file_mgr_->alloc_dir(dir_id))) {
      LOG_WARN("fail to alloc dir", KR(ret));
    } else if (OB_FAIL(param_.file_mgr_->alloc_file(dir_id, index_file_handle_))) {
      LOG_WARN("fail to alloc file", KR(ret));
    } else if (OB_FAIL(param_.file_mgr_->alloc_file(dir_id, data_file_handle_))) {
      LOG_WARN("fail to alloc file", KR(ret));
    } else if (OB_FAIL(index_block_writer_.init(param_.table_data_desc_.sstable_index_block_size_,
                                                ObCompressorType::NONE_COMPRESSOR))) {
      LOG_WARN("fail to init index block writer", KR(ret));
    } else if (OB_FAIL(callback_.init(&index_block_writer_))) {
      LOG_WARN("fail to init data block callback", KR(ret));
    } else if (OB_FAIL(data_block_writer_.init(param_.table_data_desc_.sstable_data_block_size_,
                                               param_.table_data_desc_.compressor_type_,
                                               param_.extra_buf_, param_.extra_buf_size_,
                                               &callback_))) {
      LOG_WARN("fail to init data block writer", KR(ret));
    } else if (OB_FAIL(index_block_writer_.open(index_file_handle_))) {
      LOG_WARN("fail to open file", KR(ret));
    } else if (OB_FAIL(data_block_writer_.open(data_file_handle_))) {
      LOG_WARN("fail to open file", KR(ret));
    } else {
      first_rowkey_.set_min_rowkey();
      last_rowkey_.set_min_rowkey();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableBuilder::append_row(const ObTabletID &tablet_id,
                                                   const table::ObTableLoadSequenceNo &seq_no,
                                                   const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple sstable builder is closed", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() ||
                         datum_row.get_column_count() != param_.table_data_desc_.column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param_), K(datum_row));
  } else {
    if (OB_FAIL(row_.from_datums(tablet_id, datum_row.storage_datums_, datum_row.count_,
                                 param_.table_data_desc_.rowkey_column_num_, seq_no))) {
      LOG_WARN("fail to from datum row", KR(ret));
    } else if (OB_FAIL(append_row(row_))) {
      LOG_WARN("fail to append row", KR(ret), K(row_));
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableBuilder::append_row(const RowType &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple sstable builder is closed", KR(ret));
  } else if (OB_UNLIKELY(!row.is_valid() || row.rowkey_.datum_array_.count_ !=
                                              param_.table_data_desc_.rowkey_column_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param_), K(row));
  } else {
    if (OB_FAIL(check_rowkey_order(row.rowkey_))) {
      LOG_WARN("fail to check rowkey order", KR(ret), K(row.rowkey_));
    } else if (OB_FAIL(data_block_writer_.append_row(row))) {
      LOG_WARN("fail to append row", KR(ret));
    } else if (OB_FAIL(save_last_rowkey(row.rowkey_))) {
      LOG_WARN("fail to save rowkey", KR(ret), K(row.rowkey_));
    } else if (first_rowkey_.is_min_rowkey() &&
               OB_FAIL(first_rowkey_.deep_copy(row.rowkey_, allocator_))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
    } else {
      ++row_count_;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableBuilder::check_rowkey_order(const RowkeyType &rowkey) const
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_FAIL(rowkey.compare(last_rowkey_, *param_.datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare rowkey", KR(ret));
  } else if (OB_UNLIKELY(0 == cmp_ret)) {
    ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    LOG_WARN("rowkey duplicate", KR(ret), K(last_rowkey_), K(rowkey));
  } else if (OB_UNLIKELY(cmp_ret < 0)) {
    ret = OB_ROWKEY_ORDER_ERROR;
    LOG_WARN("rowkey order error", KR(ret), K(last_rowkey_), K(rowkey));
  }
  return ret;
}

int ObDirectLoadMultipleSSTableBuilder::save_last_rowkey(const RowkeyType &rowkey)
{
  int ret = OB_SUCCESS;
  last_rowkey_allocator_.reuse();
  if (OB_FAIL(last_rowkey_.deep_copy(rowkey, last_rowkey_allocator_))) {
    LOG_WARN("fail to deep copy rowkey", KR(ret));
  }
  return ret;
}

int ObDirectLoadMultipleSSTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple sstable builder is closed", KR(ret));
  } else {
    if (OB_FAIL(data_block_writer_.close())) {
      LOG_WARN("fail to close data block writer", KR(ret));
    } else if (OB_FAIL(index_block_writer_.close())) {
      LOG_WARN("fail to close index block writer", KR(ret));
    } else {
      is_closed_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableBuilder::get_tables(
  ObIArray<ObIDirectLoadPartitionTable *> &table_array, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple sstable builder is not closed", KR(ret));
  } else if (row_count_ == 0) {
    // do nothing
  } else {
    ObDirectLoadMultipleSSTableFragment fragment;
    ObDirectLoadMultipleSSTableCreateParam create_param;
    fragment.index_block_count_ = index_block_writer_.get_block_count();
    fragment.data_block_count_ = data_block_writer_.get_block_count();
    fragment.index_file_size_ = index_block_writer_.get_file_size();
    fragment.data_file_size_ = data_block_writer_.get_file_size();
    fragment.row_count_ = row_count_;
    fragment.max_data_block_size_ = data_block_writer_.get_max_block_size();
    create_param.tablet_id_ = param_.tablet_id_;
    create_param.rowkey_column_num_ = param_.table_data_desc_.rowkey_column_num_;
    create_param.column_count_ = param_.table_data_desc_.column_count_;
    create_param.index_block_size_ = param_.table_data_desc_.sstable_index_block_size_;
    create_param.data_block_size_ = param_.table_data_desc_.sstable_data_block_size_;
    create_param.index_block_count_ = index_block_writer_.get_block_count();
    create_param.data_block_count_ = data_block_writer_.get_block_count();
    create_param.row_count_ = row_count_;
    create_param.max_data_block_size_ = data_block_writer_.get_max_block_size();
    create_param.start_key_ = first_rowkey_;
    create_param.end_key_ = last_rowkey_;
    if (OB_FAIL(fragment.index_file_handle_.assign(index_file_handle_))) {
      LOG_WARN("fail to assign index file handle", KR(ret));
    } else if (OB_FAIL(fragment.data_file_handle_.assign(data_file_handle_))) {
      LOG_WARN("fail to assign data file handle", KR(ret));
    } else if (OB_FAIL(create_param.fragments_.push_back(fragment))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadMultipleSSTable *sstable = nullptr;
      if (OB_ISNULL(sstable = OB_NEWx(ObDirectLoadMultipleSSTable, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadMultipleSSTable", KR(ret));
      } else if (OB_FAIL(sstable->init(create_param))) {
        LOG_WARN("fail to init sstable", KR(ret), K(create_param));
      } else if (OB_FAIL(table_array.push_back(sstable))) {
        LOG_WARN("fail to push back ssstable", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != sstable) {
          sstable->~ObDirectLoadMultipleSSTable();
          allocator.free(sstable);
          sstable = nullptr;
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
