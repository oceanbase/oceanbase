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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_builder.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/*
 * ObDirectLoadMultipleHeapTableBuildParam
 */

ObDirectLoadMultipleHeapTableBuildParam::ObDirectLoadMultipleHeapTableBuildParam()
  : file_mgr_(nullptr), extra_buf_(nullptr), extra_buf_size_(0), index_dir_id_(-1), data_dir_id_(-1)
{
}

ObDirectLoadMultipleHeapTableBuildParam::~ObDirectLoadMultipleHeapTableBuildParam()
{
}

bool ObDirectLoadMultipleHeapTableBuildParam::is_valid() const
{
  return table_data_desc_.is_valid() && nullptr != file_mgr_ && nullptr != extra_buf_ &&
         extra_buf_size_ > 0 && extra_buf_size_ % DIO_ALIGN_SIZE == 0 && index_dir_id_ > 0 &&
         data_dir_id_ > 0;
}

/**
 * ObDirectLoadMultipleHeapTableBuilder
 */

ObDirectLoadMultipleHeapTableBuilder::ObDirectLoadMultipleHeapTableBuilder()
  : index_entry_count_(0), row_count_(0), is_closed_(false), is_inited_(false)
{
}

ObDirectLoadMultipleHeapTableBuilder::~ObDirectLoadMultipleHeapTableBuilder()
{
}

int ObDirectLoadMultipleHeapTableBuilder::init(const ObDirectLoadMultipleHeapTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleHeapTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    param_ = param;
    if (OB_FAIL(param_.file_mgr_->alloc_file(param_.index_dir_id_, index_file_handle_))) {
      LOG_WARN("fail to alloc file", KR(ret));
    } else if (OB_FAIL(param_.file_mgr_->alloc_file(param_.data_dir_id_, data_file_handle_))) {
      LOG_WARN("fail to alloc file", KR(ret));
    } else if (OB_FAIL(index_block_writer_.init(param_.table_data_desc_.sstable_index_block_size_,
                                                param_.table_data_desc_.compressor_type_))) {
      LOG_WARN("fail to init index block writer", KR(ret));
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
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableBuilder::append_row(const ObTabletID &tablet_id,
                                                     const table::ObTableLoadSequenceNo &seq_no,
                                                     const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple heap table builder is closed", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() ||
                         datum_row.get_column_count() != param_.table_data_desc_.column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param_), K(datum_row));
  } else {
    row_.tablet_id_ = tablet_id;
    if (OB_FAIL(row_.from_datums(datum_row.storage_datums_, datum_row.count_, seq_no))) {
      LOG_WARN("fail to from datum row", KR(ret));
    } else if (OB_FAIL(append_row(row_))) {
      LOG_WARN("fail to append row", KR(ret), K(row_));
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableBuilder::append_row(const RowType &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple heap table builder is closed", KR(ret));
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(row));
  } else {
    if (row.tablet_id_ != last_tablet_index_.tablet_id_) {
      if (last_tablet_index_.tablet_id_.is_valid()) {
        // check tablet is in order
        int cmp_ret = row.tablet_id_.compare(last_tablet_index_.tablet_id_);
        if (cmp_ret < 0) {
          ret = OB_ROWKEY_ORDER_ERROR;
          LOG_WARN("unexpected tablet id", KR(ret), K(last_tablet_index_.tablet_id_),
                   K(row.tablet_id_));
        } else {
          // save last tablet index
          if (OB_FAIL(index_block_writer_.append_index(last_tablet_index_))) {
            LOG_WARN("fail to append tablet index", KR(ret));
          } else {
            ++index_entry_count_;
          }
        }
      }
      if (OB_SUCC(ret)) {
        last_tablet_index_.tablet_id_ = row.tablet_id_;
        last_tablet_index_.row_count_ = 0;
        last_tablet_index_.fragment_idx_ = 0;
        last_tablet_index_.offset_ = callback_.get_data_block_offset();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_block_writer_.write_item(row))) {
        LOG_WARN("fail to append row", KR(ret));
      } else {
        ++last_tablet_index_.row_count_;
        ++row_count_;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple heap table builder is closed", KR(ret));
  } else {
    if (last_tablet_index_.tablet_id_.is_valid()) {
      // save last tablet index
      if (OB_FAIL(index_block_writer_.append_index(last_tablet_index_))) {
        LOG_WARN("fail to append tablet index", KR(ret));
      } else {
        ++index_entry_count_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_block_writer_.close())) {
        LOG_WARN("fail to close data block writer", KR(ret));
      } else if (OB_FAIL(index_block_writer_.close())) {
        LOG_WARN("fail to close index block writer", KR(ret));
      } else {
        is_closed_ = true;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableBuilder::get_tables(
  ObIArray<ObIDirectLoadPartitionTable *> &table_array, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleHeapTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple heap table builder is not closed", KR(ret));
  } else if (row_count_ == 0) {
    // do nothing
  } else {
    ObDirectLoadMultipleHeapTableDataFragment data_fragment;
    ObDirectLoadMultipleHeapTableCreateParam create_param;
    data_fragment.block_count_ = data_block_writer_.get_block_count();
    data_fragment.file_size_ = data_block_writer_.get_file_size();
    data_fragment.row_count_ = row_count_;
    data_fragment.max_block_size_ = data_block_writer_.get_max_block_size();
    create_param.column_count_ = param_.table_data_desc_.column_count_;
    create_param.index_block_size_ = param_.table_data_desc_.sstable_index_block_size_;
    create_param.data_block_size_ = param_.table_data_desc_.sstable_data_block_size_;
    create_param.index_block_count_ = index_block_writer_.get_block_count();
    create_param.data_block_count_ = data_block_writer_.get_block_count();
    create_param.index_file_size_ = index_block_writer_.get_file_size();
    create_param.data_file_size_ = data_block_writer_.get_file_size();
    create_param.index_entry_count_ = index_entry_count_;
    create_param.row_count_ = row_count_;
    create_param.max_data_block_size_ = data_block_writer_.get_max_block_size();
    if (OB_FAIL(data_fragment.file_handle_.assign(data_file_handle_))) {
      LOG_WARN("fail to assign data file handle", KR(ret));
    }  else if (OB_FAIL(create_param.index_file_handle_.assign(index_file_handle_))) {
      LOG_WARN("fail to assign file handle", KR(ret));
    } else if (OB_FAIL(create_param.data_fragments_.push_back(data_fragment))) {
      LOG_WARN("fail to push back data fragment", KR(ret));
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadMultipleHeapTable *heap_table = nullptr;
      if (OB_ISNULL(heap_table = OB_NEWx(ObDirectLoadMultipleHeapTable, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadMultipleHeapTable", KR(ret));
      } else if (OB_FAIL(heap_table->init(create_param))) {
        LOG_WARN("fail to init heap_table", KR(ret), K(create_param));
      } else if (OB_FAIL(table_array.push_back(heap_table))) {
        LOG_WARN("fail to push back heap table", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != heap_table) {
          heap_table->~ObDirectLoadMultipleHeapTable();
          allocator.free(heap_table);
          heap_table = nullptr;
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
