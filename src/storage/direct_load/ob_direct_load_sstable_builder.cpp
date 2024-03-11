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

#include "storage/direct_load/ob_direct_load_sstable_builder.h"
#include "observer/table_load/ob_table_load_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadIndexBlock
 */

int64_t ObDirectLoadIndexBlock::get_item_num_per_block(int64_t block_size)
{
  ObDirectLoadIndexBlockHeader header;
  ObDirectLoadIndexBlockItem item;
  const int64_t header_size = header.get_serialize_size();
  const int64_t item_size = item.get_serialize_size();
  int64_t item_num_per_block = 0;
  if (block_size > header_size) {
    item_num_per_block = (block_size - header_size) / item_size;
  }
  return item_num_per_block;
}
/**
 * ObDirectLoadSSTableBuilder
 */

int ObDirectLoadSSTableBuilder::init(const ObDirectLoadSSTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadSSTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    const uint64_t tenant_id = MTL_ID();
    param_ = param;
    start_key_.set_min_rowkey();
    end_key_.set_min_rowkey();
    int64_t dir_id = -1;
    if (OB_ISNULL(param_.file_mgr_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid file mgr", KR(ret), K(file_mgr_));
    } else if (OB_FAIL(param_.file_mgr_->alloc_dir(dir_id))) {
      LOG_WARN("fail to alloc dir", KR(ret));
    } else if (OB_FAIL(param_.file_mgr_->alloc_file(dir_id, data_file_handle_))) {
      LOG_WARN("fail to alloc datafragment", KR(ret));
    } else if (OB_FAIL(param_.file_mgr_->alloc_file(dir_id, index_file_handle_))) {
      LOG_WARN("fail to alloc index fragment", KR(ret));
    } else if (OB_FAIL(index_block_writer_.init(tenant_id,
                                                param_.table_data_desc_.sstable_index_block_size_,
                                                index_file_handle_))) {
      LOG_WARN("fail to init index block writer", KR(ret),
               K(param_.table_data_desc_.sstable_index_block_size_), K(index_file_handle_));
    } else if (OB_FAIL(data_block_writer_.init(tenant_id,
                                               param_.table_data_desc_.sstable_data_block_size_,
                                               data_file_handle_, &index_block_writer_))) {
      LOG_WARN("fail to init data block writer", KR(ret),
               K(param_.table_data_desc_.sstable_data_block_size_), K(data_file_handle_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadSSTableBuilder::append_row(const ObTabletID &tablet_id, const table::ObTableLoadSequenceNo &seq_no, const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load sstable builder is closed", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() ||
                         datum_row.get_column_count() != param_.table_data_desc_.column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param_), K(datum_row));
  } else {
    ObDatumRowkey key(datum_row.storage_datums_, param_.table_data_desc_.rowkey_column_num_);
    ObDirectLoadExternalRow external_row;
    OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, simple_sstable_append_row_time_us);
    if (OB_FAIL(check_rowkey_order(key))) {
      LOG_WARN("fail to check rowkey order", KR(ret), K(datum_row));
    } else if (OB_FAIL(external_row.from_datums(datum_row.storage_datums_, datum_row.count_,
                                                param_.table_data_desc_.rowkey_column_num_, seq_no))) {
      LOG_WARN("fail to from datum row", KR(ret));
    } else if (OB_FAIL(data_block_writer_.append_row(external_row))) {
      LOG_WARN("fail to append row to data block writer", KR(ret), K(external_row));
    } else if (start_key_.is_min_rowkey()) {
      if (OB_FAIL(key.deep_copy(start_key_, allocator_))) {
        LOG_WARN("fail to deep copy", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableBuilder::append_row(const ObDirectLoadExternalRow &external_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load sstable builder is closed", KR(ret));
  } else if (OB_UNLIKELY(!external_row.is_valid()) ||
             (external_row.rowkey_datum_array_.count_ !=
              param_.table_data_desc_.rowkey_column_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param_), K(external_row));
  } else {
    ObDatumRowkey key(external_row.rowkey_datum_array_.datums_,
                      param_.table_data_desc_.rowkey_column_num_);
    OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, simple_sstable_append_row_time_us);
    if (OB_FAIL(check_rowkey_order(key))) {
      LOG_WARN("fail to check rowkey order", KR(ret), K(external_row));
    } else if (OB_FAIL(data_block_writer_.append_row(external_row))) {
      LOG_WARN("fail to append row to data block writer", KR(ret), K(external_row));
    } else if (start_key_.is_min_rowkey()) {
      if (OB_FAIL(key.deep_copy(start_key_, allocator_))) {
        LOG_WARN("fail to deep copy", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load sstable is closed", KR(ret));
  } else {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, simple_sstable_append_row_time_us);
    if (OB_FAIL(data_block_writer_.close())) {
      LOG_WARN("fail to close data block writer", KR(ret), K(data_block_writer_));
    } else if (OB_FAIL(index_block_writer_.close())) {
      LOG_WARN("fail to close index block writer", KR(ret), K(index_block_writer_));
    }
  }
  if (OB_SUCC(ret)) {
    is_closed_ = true;
  }
  return ret;
}

int ObDirectLoadSSTableBuilder::get_tables(ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                                           ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load sstable not closed", KR(ret));
  } else {
    const int64_t index_item_num_per_block = ObDirectLoadIndexBlock::get_item_num_per_block(
      param_.table_data_desc_.sstable_index_block_size_);
    ObDirectLoadSSTableCreateParam param;
    param.tablet_id_ = param_.tablet_id_;
    param.rowkey_column_count_ = param_.table_data_desc_.rowkey_column_num_;
    param.column_count_ = param_.table_data_desc_.column_count_;
    param.data_block_size_ = param_.table_data_desc_.sstable_data_block_size_;
    param.index_block_size_ = param_.table_data_desc_.sstable_index_block_size_;
    param.index_item_count_ = index_block_writer_.get_total_index_size();
    param.row_count_ = data_block_writer_.get_total_row_count();
    param.index_block_count_ =
      (param.index_item_count_ + index_item_num_per_block - 1) / index_item_num_per_block;
    param.start_key_ = start_key_;
    param.end_key_ = end_key_;
    if (param.row_count_ > 0) {
      ObDirectLoadSSTableFragment fragment;
      fragment.meta_.row_count_ = data_block_writer_.get_total_row_count();
      fragment.meta_.index_item_count_ = index_block_writer_.get_total_index_size();
      fragment.meta_.occupy_size_ = data_block_writer_.get_file_size();
      fragment.meta_.index_block_count_ = param.index_block_count_;
      if (OB_FAIL(fragment.data_file_handle_.assign(data_file_handle_))) {
        LOG_WARN("fail to assign data file handle", KR(ret));
      } else if (OB_FAIL(fragment.index_file_handle_.assign(index_file_handle_))) {
        LOG_WARN("fail to assign index file handle", KR(ret));
      } else if (OB_FAIL(param.fragments_.push_back(fragment))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadSSTable *sstable = nullptr;
      if (OB_ISNULL(sstable = OB_NEWx(ObDirectLoadSSTable, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadSSTable", KR(ret));
      } else if (OB_FAIL(sstable->init(param))) {
        LOG_WARN("fail to init sstable", KR(ret));
      } else if (OB_FAIL(table_array.push_back(sstable))) {
        LOG_WARN("fail to push back sstable", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != sstable) {
          sstable->~ObDirectLoadSSTable();
          allocator.free(sstable);
          sstable = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableBuilder::check_rowkey_order(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 1;
  if (OB_FAIL(rowkey.compare(end_key_, *param_.datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare", KR(ret));
  } else if (cmp_ret > 0) {
    rowkey_allocator_.reuse();
    if (OB_FAIL(rowkey.deep_copy(end_key_, rowkey_allocator_))) {
      LOG_WARN("fail to deep copy", KR(ret));
    }
  } else if (cmp_ret == 0) {
    ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    LOG_INFO("rowkey == last rowkey", K(ret), K(cmp_ret), K(end_key_), K(rowkey));
  } else {
    ret = OB_ROWKEY_ORDER_ERROR;
    LOG_INFO("rowkey < last rowkey", K(ret), K(cmp_ret), K(end_key_), K(rowkey));
  }
  return ret;
}
/**
 * ObDirectLoadDataBlockWriter2
 */

ObDirectLoadDataBlockWriter2::ObDirectLoadDataBlockWriter2()
  : tenant_id_(OB_INVALID_ID),
    header_length_(0),
    buf_pos_(0),
    buf_size_(0),
    total_row_count_(0),
    row_count_(0),
    file_size_(0),
    buf_(nullptr),
    allocator_("TLD_DBWriter"),
    is_inited_(false),
    is_closed_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadDataBlockWriter2::~ObDirectLoadDataBlockWriter2() { reset(); }

void ObDirectLoadDataBlockWriter2::reset()
{
  is_inited_ = false;
  is_closed_ = false;
  assign(0, 0, nullptr);
  file_io_handle_.reset();
  buf_ = nullptr;
  allocator_.reset();
}

void ObDirectLoadDataBlockWriter2::assign(const int64_t buf_pos, const int64_t buf_cap, char *buf)
{
  buf_pos_ = buf_pos;
  buf_size_ = buf_cap;
  buf_ = buf;
}

int ObDirectLoadDataBlockWriter2::init(uint64_t tenant_id, int64_t buf_size,
                                      const ObDirectLoadTmpFileHandle &file_handle,
                                      ObDirectLoadIndexBlockWriter *index_block_writer)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDataBlockWriter2 init twice", KR(ret), KP(this));
  } else if (buf_size < DIRECT_LOAD_DEFAULT_SSTABLE_INDEX_BLOCK_SIZE ||
             buf_size % DIO_ALIGN_SIZE != 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(buf_size));
  } else if (OB_FAIL(file_io_handle_.open(file_handle))) {
    LOG_WARN("fail to open file handle", KR(ret));
  } else {
    if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate buffer", KR(ret), K(buf_size));
    } else {
      header_length_ = header_.get_serialize_size();
      assign(header_length_, buf_size, buf_);
      index_block_writer_ = index_block_writer;
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadDataBlockWriter2::write_item(const ObDirectLoadExternalRow &external_row)
{
  int ret = OB_SUCCESS;
  int64_t offset = buf_pos_;
  int64_t data_size = external_row.get_serialize_size();
  if (data_size + buf_pos_ > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(external_row.serialize(buf_, buf_size_, buf_pos_))) {
    LOG_WARN("fail to serialize datum_row", KR(ret), KP(buf_), K(buf_size_), K(buf_pos_));
  }
  if (OB_SUCC(ret)) {
    row_count_++;
    header_.last_row_offset_ = offset;
  }
  return ret;
}

int ObDirectLoadDataBlockWriter2::write_large_item(const ObDirectLoadExternalRow &external_row,
                                                  int64_t new_buf_size)
{
  int ret = OB_SUCCESS;
  char *new_buf;
  const int64_t align_buf_size = upper_align(new_buf_size, DIO_ALIGN_SIZE);
  ObMemAttr attr(MTL_ID(), "TLD_LargeBuf");
  if (OB_ISNULL(new_buf = static_cast<char *>(ob_malloc(align_buf_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", KR(ret), K(align_buf_size));
  } else {
    int64_t new_buf_pos = header_length_;
    if (OB_FAIL(external_row.serialize(new_buf, align_buf_size, new_buf_pos))) {
      LOG_WARN("fail to serialize datum_row", KR(ret), K(align_buf_size), K(new_buf_pos));
    } else {
      header_.last_row_offset_ = header_length_;
      buf_pos_ = new_buf_pos;
      row_count_++;
      if (OB_FAIL(flush_buffer(align_buf_size, new_buf))) {
        LOG_WARN("fail to flush buffer", KR(ret));
      }
    }
  }
  if (nullptr != new_buf) {
    ob_free(new_buf);
  }
  return ret;
}

int ObDirectLoadDataBlockWriter2::append_row(const ObDirectLoadExternalRow &external_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDataBlockWriter2 not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(write_item(external_row))) {
      if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        LOG_WARN("fail to write item", KR(ret));
      } else {
        if (buf_pos_ != header_length_ && (OB_FAIL(flush_buffer(buf_size_, buf_)))) {
          LOG_WARN("fail to flush buffer", KR(ret));
        } else {
          int64_t total_size = external_row.get_serialize_size() + header_length_;
          if (total_size > buf_size_) {
            if (OB_FAIL(write_large_item(external_row, total_size))) {
              LOG_WARN("fail to write item", KR(ret), K(external_row), K(total_size));
            }
          } else {
            if (OB_FAIL(write_item(external_row))) {
              LOG_WARN("fail to write item", KR(ret), K(external_row));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    total_row_count_++;
  }
  return ret;
}

int ObDirectLoadDataBlockWriter2::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDataBlockWriter2 not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObDirectLoadDataBlockWriter2 is closed", KR(ret));
  } else if (buf_pos_ > header_length_ && OB_FAIL(flush_buffer(buf_size_, buf_))) {
    LOG_WARN("fail to flush buffer", KR(ret));
  } else if (OB_FAIL(file_io_handle_.wait())) {
    LOG_WARN("fail to wait io finish", KR(ret));
  } else {
    reset();
    is_closed_ = true;
  }
  return ret;
}

int ObDirectLoadDataBlockWriter2::flush_buffer(int64_t buf_size, char *buf)
{
  int ret = OB_SUCCESS;
  header_.size_ = buf_pos_;
  header_.occupy_size_ = buf_size;
  int64_t pos = 0;
  if (OB_FAIL(header_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize data block header", KR(ret), K(buf_size), KP(buf_), K(pos));
  } else {
    if (OB_FAIL(file_io_handle_.write(buf, buf_size))) {
      LOG_WARN("fail to do aio write data file", KR(ret));
    } else {
      ObDirectLoadIndexBlockItem item;
      assign(header_length_, buf_size_, buf_);
      file_size_ += buf_size;
      item.end_offset_ = file_size_;
      if (OB_FAIL(index_block_writer_->append_row(row_count_, item))) {
        LOG_WARN("fail to append row index", KR(ret));
      } else {
        row_count_ = 0;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadIndexBlockWriter
 */

ObDirectLoadIndexBlockWriter::ObDirectLoadIndexBlockWriter()
  : tenant_id_(OB_INVALID_ID),
    header_length_(0),
    buf_pos_(0),
    buf_size_(0),
    item_size_(0),
    row_count_(0),
    total_index_size_(0),
    offset_(0),
    buf_(nullptr),
    allocator_("TLD_IBWriter"),
    is_inited_(false),
    is_closed_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadIndexBlockWriter::~ObDirectLoadIndexBlockWriter() { reset(); }

void ObDirectLoadIndexBlockWriter::reset()
{
  is_inited_ = false;
  is_closed_ = false;
  assign(0, 0, nullptr);
  file_io_handle_.reset();
  buf_ = nullptr;
  allocator_.reset();
}

void ObDirectLoadIndexBlockWriter::assign(const int64_t buf_pos, const int64_t buf_cap, char *buf)
{
  buf_pos_ = buf_pos;
  buf_size_ = buf_cap;
  buf_ = buf;
}

int ObDirectLoadIndexBlockWriter::init(uint64_t tenant_id, int64_t buf_size,
                                       const ObDirectLoadTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadIndexBlockWriter init twice", KR(ret), KP(this));
  } else if (buf_size < DIRECT_LOAD_DEFAULT_SSTABLE_INDEX_BLOCK_SIZE ||
             buf_size % DIO_ALIGN_SIZE != 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(buf_size));
  } else if (OB_FAIL(file_io_handle_.open(file_handle))) {
    LOG_WARN("fail to open file handle", KR(ret));
  } else {
    if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate buffer", KR(ret), K(buf_size));
    } else {
      header_length_ = header_.get_serialize_size();
      assign(header_length_, buf_size, buf_);
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadIndexBlockWriter::append_row(int64_t row_count,
                                             const ObDirectLoadIndexBlockItem &item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadIndexBlockWriter not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(write_item(item))) {
      if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        LOG_WARN("fail to write item", KR(ret), K(item));
      } else {
        if (OB_FAIL(flush_buffer())) {
          LOG_WARN("fail to flush buffer", KR(ret));
        } else if (OB_FAIL(write_item(item))) {
          LOG_WARN("fail to write item", KR(ret), K(item));
        }
      }
    }
    if (OB_SUCC(ret)) {
      offset_ = item.end_offset_;
      row_count_ += row_count;
      item_size_++;
    }
  }
  return ret;
}

int ObDirectLoadIndexBlockWriter::write_item(const ObDirectLoadIndexBlockItem &item)
{
  int ret = OB_SUCCESS;
  int64_t data_size = item.get_serialize_size();
  if (data_size + buf_pos_ > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(item.serialize(buf_, buf_size_, buf_pos_))) {
    LOG_WARN("fail to serialize datum_row", KR(ret));
  }
  return ret;
}

int ObDirectLoadIndexBlockWriter::flush_buffer()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  header_.row_count_ = row_count_;
  if (OB_FAIL(header_.serialize(buf_, buf_size_, pos))) {
    LOG_WARN("fail to serialize data block header", KR(ret), K(buf_size_), K(pos), KP(buf_));
  } else {
    if (OB_FAIL(file_io_handle_.write(buf_, buf_size_))) {
      LOG_WARN("fail to do aio write index file", KR(ret));
    } else {
      header_.start_offset_ = offset_;
      total_index_size_ += item_size_;
      item_size_ = 0;
      row_count_ = 0;
      assign(header_length_, buf_size_, buf_);
    }
  }
  return ret;
}

int ObDirectLoadIndexBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadIndexBlockWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObDirectLoadIndexBlockWriter is closed", KR(ret));
  } else if (buf_pos_ > header_length_ && OB_FAIL(flush_buffer())) {
    LOG_WARN("fail to flush buffer", KR(ret));
  } else if (OB_FAIL(file_io_handle_.wait())) {
    LOG_WARN("fail to wait io finish", KR(ret));
  } else {
    reset();
    is_closed_ = true;
  }
  return ret;
}

/**
 * ObDirectLoadIndexBlockReader
 */

ObDirectLoadIndexBlockReader::ObDirectLoadIndexBlockReader()
  : tenant_id_(OB_INVALID_ID),
    buf_(nullptr),
    buf_size_(0),
    header_length_(0),
    item_size_(0),
    index_item_num_per_block_(0),
    io_timeout_ms_(0),
    allocator_("TLD_IBReader"),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

int ObDirectLoadIndexBlockReader::init(uint64_t tenant_id, int64_t buf_size,
                                       const ObDirectLoadTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadIndexBlockReader init twice", KR(ret), KP(this));
  } else if (buf_size < 0 || buf_size % DIO_ALIGN_SIZE != 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(buf_size));
  } else if (OB_FAIL(file_io_handle_.open(file_handle))) {
    LOG_WARN("fail to open file handle", KR(ret));
  } else {
    if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate buffer", KR(ret), K(buf_size));
    } else {
      ObDirectLoadIndexBlockItem item;
      header_length_ = header_.get_serialize_size();
      item_size_ = item.get_serialize_size();
      if (item_size_ == 0) {
        ret = OB_ERROR;
        LOG_WARN("divide zero", K(ret), K(item_size_));
      } else {
        index_item_num_per_block_ = ObDirectLoadIndexBlock::get_item_num_per_block(buf_size);
        assign(buf_size, buf_);
        tenant_id_ = tenant_id;
        io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
        is_inited_ = true;
      }
    }
  }
  return ret;
}

void ObDirectLoadIndexBlockReader::assign(const int64_t buf_size, char *buf)
{
  buf_size_ = buf_size;
  buf_ = buf;
}

int ObDirectLoadIndexBlockReader::change_fragment(const ObDirectLoadTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_io_handle_.open(file_handle))) {
    LOG_WARN("fail to change file handle", KR(ret));
  }
  return ret;
}

// 读对应索引项块
int ObDirectLoadIndexBlockReader::read_buffer(int64_t idx)
{
  int ret = OB_SUCCESS;
  uint64_t offset = 0;
  offset = buf_size_ * idx;
  if (OB_FAIL(file_io_handle_.pread(buf_, buf_size_, offset))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to do pread from index file", KR(ret));
    }
  }
  return ret;
}

//读对应索引块下标的索引项
int ObDirectLoadIndexBlockReader::get_index_info(int64_t idx, ObDirectLoadIndexInfo &info)
{
  int ret = OB_SUCCESS;
  ObDirectLoadIndexBlockItem start_item;
  ObDirectLoadIndexBlockItem end_item;
  int64_t num_index = idx / index_item_num_per_block_;
  int64_t offset = idx % index_item_num_per_block_;
  int64_t buf_pos = 0;
  if (OB_FAIL(read_buffer(num_index))) {
    LOG_WARN("fail to read buffer", KR(ret));
  } else if (OB_FAIL(header_.deserialize(buf_, buf_size_, buf_pos))) {
    LOG_WARN("fail to deserialize header", KR(ret));
  } else {
    if (0 == offset) {
      info.offset_ = header_.start_offset_;
    } else {
      buf_pos = header_length_ + (offset - 1) * item_size_;
      if (OB_FAIL(start_item.deserialize(buf_, buf_size_, buf_pos))) {
        LOG_WARN("fail to deserialize item", KR(ret), K(buf_size_), K(buf_pos));
      } else {
        info.offset_ = start_item.end_offset_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(end_item.deserialize(buf_, buf_size_, buf_pos))) {
        LOG_WARN("fail to deserialize item", KR(ret), K(buf_size_), K(buf_pos));
      } else {
        info.size_ = end_item.end_offset_ - info.offset_;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadDataBlockReader2
 */

ObDirectLoadDataBlockReader2::ObDirectLoadDataBlockReader2()
  : buf_(nullptr), buf_pos_(0), buf_size_(0), is_inited_(false)
{
}

void ObDirectLoadDataBlockReader2::assign(const int64_t buf_pos, const int64_t buf_size, char *buf)
{
  buf_pos_ = buf_pos;
  buf_size_ = buf_size;
  buf_ = buf;
}

void ObDirectLoadDataBlockReader2::reset()
{
  is_inited_ = false;
  assign(0, 0, nullptr);
  buf_ = nullptr;
}

int ObDirectLoadDataBlockReader2::init(int64_t buf_size, char *buf, int64_t cols_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDataBlockReader2 init twice", KR(ret), KP(this));
  } else if (buf_size <= 0 || buf_size % DIO_ALIGN_SIZE != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(buf_size));
  } else {
    assign(0, buf_size, buf);
    if (OB_FAIL(header_.deserialize(buf_, buf_size_, buf_pos_))) {
      LOG_WARN("fail to deserialize header", KR(ret), K(buf_), K(buf_size_), K(buf_pos_));
    } else if (header_.occupy_size_ > buf_size_) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf is not enough", KR(ret), K(header_.occupy_size_), K(buf_size_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadDataBlockReader2::get_next_item(const ObDirectLoadExternalRow *&item)
{
  int ret = OB_SUCCESS;
  item = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDataBlockReader2 not init", KR(ret), KP(this));
  } else if (buf_pos_ == header_.size_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(curr_row_.deserialize(buf_, buf_size_, buf_pos_))) {
    LOG_WARN("fail to deserialize buffer", KR(ret), K(buf_size_), K(buf_pos_));
  } else {
    item = &curr_row_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDirectLoadDataBlockHeader)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, size_))) {
      RPC_WARN("encode object fail", "name", MSTR(size_), K(buf_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, occupy_size_))) {
      RPC_WARN("encode object fail", "name", MSTR(occupy_size_), K(buf_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(NS_::encode_i32(buf, buf_len, pos, last_row_offset_))) {
      RPC_WARN("encode object fail", "name", MSTR(last_row_offset_), K(buf_len), K(pos), K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDirectLoadDataBlockHeader)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &size_))) {
      RPC_WARN("decode object fail", "name", MSTR(size_), K(data_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &occupy_size_))) {
      RPC_WARN("decode object fail", "name", MSTR(occupy_size_), K(data_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(NS_::decode_i32(buf, data_len, pos, &last_row_offset_))) {
      RPC_WARN("decode object fail", "name", MSTR(last_row_offset_), K(data_len), K(pos), K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDirectLoadDataBlockHeader)
{
  int64_t len = 0;
  len += NS_::encoded_length_i32(size_);
  len += NS_::encoded_length_i32(occupy_size_);
  len += NS_::encoded_length_i32(last_row_offset_);
  return len;
}

OB_DEF_SERIALIZE(ObDirectLoadIndexBlockHeader)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(NS_::encode_i64(buf, buf_len, pos, start_offset_))) {
      RPC_WARN("encode object fail", "name", MSTR(start_offset_), K(buf_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(NS_::encode_i64(buf, buf_len, pos, row_count_))) {
      RPC_WARN("encode object fail", "name", MSTR(row_count_), K(buf_len), K(pos), K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDirectLoadIndexBlockHeader)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(NS_::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&start_offset_)))) {
      RPC_WARN("decode object fail", "name", MSTR(start_offset_), K(data_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(NS_::decode_i64(buf, data_len, pos, &row_count_))) {
      RPC_WARN("decode object fail", "name", MSTR(row_count_), K(data_len), K(pos), K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDirectLoadIndexBlockHeader)
{
  int64_t len = 0;
  len += NS_::encoded_length_i64(start_offset_);
  len += NS_::encoded_length_i64(row_count_);
  return len;
}

OB_DEF_SERIALIZE(ObDirectLoadIndexBlockItem)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(NS_::encode_i64(buf, buf_len, pos, end_offset_))) {
      RPC_WARN("encode object fail", "name", MSTR(end_offset_), K(buf_len), K(pos), K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDirectLoadIndexBlockItem)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(NS_::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&end_offset_)))) {
      RPC_WARN("decode object fail", "name", MSTR(end_offset_), K(data_len), K(pos), K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDirectLoadIndexBlockItem)
{
  int64_t len = 0;
  len += NS_::encoded_length_i64(end_offset_);
  return len;
}

} // namespace storage
} // namespace oceanbase
