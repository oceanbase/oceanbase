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

#include "storage/direct_load/ob_direct_load_sstable_scanner.h"
#include "observer/table_load/ob_table_load_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadSSTableScanner
 */

ObDirectLoadSSTableScanner::ObDirectLoadSSTableScanner()
  : buf_pos_(0),
    buf_size_(0),
    sstable_data_block_size_(0),
    rowkey_column_num_(0),
    column_count_(0),
    buf_(nullptr),
    large_buf_(nullptr),
    io_timeout_ms_(0),
    curr_idx_(0),
    start_idx_(0),
    end_idx_(0),
    start_fragment_idx_(0),
    end_fragment_idx_(0),
    curr_fragment_idx_(0),
    locate_start_offset_(0),
    locate_end_offset_(0),
    found_start_(false),
    found_end_(false),
    offset_(0),
    allocator_("TLD_SSTScanner"),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadSSTableScanner::~ObDirectLoadSSTableScanner() {}
void ObDirectLoadSSTableScanner::assign(const int64_t buf_cap, char *buf)
{
  buf_size_ = buf_cap;
  buf_ = buf;
}

int ObDirectLoadSSTableScanner::init(ObDirectLoadSSTable *sstable,
                                     const ObDirectLoadTableDataDesc &table_data_desc,
                                     const ObDatumRange &range,
                                     const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadSSTableScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid() || !range.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KPC(sstable), K(range), KP(datum_utils));
  } else {
    sstable_data_block_size_ = table_data_desc.sstable_data_block_size_;
    sstable_ = sstable;
    query_range_ = &range;
    datum_utils_ = datum_utils;
    const int64_t buf_size = sstable_data_block_size_;
    if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate buffer", KR(ret), K(buf_size));
    } else {
      assign(buf_size, buf_);
      rowkey_column_num_ = sstable->get_meta().rowkey_column_count_;
      column_count_ = sstable->get_meta().column_count_;
      io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
    }
    if (OB_SUCC(ret)) {
      if (!sstable_->is_empty()) {
        if (OB_FAIL(inner_open())) {
          LOG_WARN("fail to inner open", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(datum_row_.init(column_count_))) {
        LOG_WARN("fail to init datum row", KR(ret));
      } else {
        datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::locate_lower_bound(const ObDatumRowkey &rowkey, int64_t &logic_id)
{
  int ret = OB_SUCCESS;
  int64_t length = sstable_->get_meta().index_item_count_;
  int64_t left = 0;
  int64_t right = length - 1;
  int64_t ans = length;
  ObDatumRowkey start_key;
  int cmp_ret = 0;
  while (OB_SUCC(ret) && (left <= right)) {
    int64_t mid = left + (right - left) / 2;
    if (OB_FAIL(get_start_key(mid, start_key))) {
      LOG_WARN("fail to get start key", KR(ret));
    } else if (OB_FAIL(start_key.compare(rowkey, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare", KR(ret));
    }
    if (OB_SUCC(ret)) {
      if (cmp_ret >= 0) {
        ans = mid;
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }
  }
  if (OB_SUCC(ret)) {
    logic_id = ans;
  }
  return ret;
}

int ObDirectLoadSSTableScanner::locate_upper_bound(const ObDatumRowkey &rowkey, int64_t &logic_id)
{
  int ret = OB_SUCCESS;
  int64_t length = sstable_->get_meta().index_item_count_;
  int64_t left = 0;
  int64_t right = length - 1;
  int64_t ans = length;
  ObDatumRowkey start_key;
  int cmp_ret = 0;
  while (OB_SUCC(ret) && (left <= right)) {
    int64_t mid = left + (right - left) / 2;
    if (OB_FAIL(get_start_key(mid, start_key))) {
      LOG_WARN("fail to get start key", KR(ret));
    } else if (OB_FAIL(start_key.compare(rowkey, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare", KR(ret));
    }
    if (OB_SUCC(ret)) {
      if (cmp_ret > 0) {
        ans = mid;
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }
  }
  if (OB_SUCC(ret)) {
    logic_id = ans;
  }
  return ret;
}

int ObDirectLoadSSTableScanner::get_start_key(int64_t idx, ObDatumRowkey &startkey)
{
  int ret = OB_SUCCESS;
  ObDirectLoadIndexInfo info;
  const ObDirectLoadExternalRow *item = nullptr;
  int64_t locate_fragment_idx = 0;
  int64_t new_idx = 0;
  data_block_reader_.reset();
  ObDirectLoadSSTableFragment fragment;
  if (OB_FAIL(fragment_operator_.get_fragment_item_idx(idx, locate_fragment_idx, new_idx))) {
    LOG_WARN("fail to get fragment idx", KR(ret));
  } else if (OB_FAIL(fragment_operator_.get_fragment(locate_fragment_idx, fragment))) {
    LOG_WARN("fail to get fragment ", KR(ret));
  } else if (OB_FAIL(index_block_reader_.change_fragment(fragment.index_file_handle_))) {
    LOG_WARN("fail to change index file handle ", KR(ret));
  } else if (OB_FAIL(file_io_handle_.open(fragment.data_file_handle_))) {
    LOG_WARN("Fail to open file handle", K(ret));
  } else if (OB_FAIL(index_block_reader_.get_index_info(new_idx, info))) {
    LOG_WARN("fail to get index info", KR(ret));
  } else if (OB_FAIL(read_buffer(info.offset_, info.size_))) {
    LOG_WARN("fail to read buffer", KR(ret));
  } else {
    if (info.size_ > sstable_data_block_size_) {
      if (OB_FAIL(data_block_reader_.init(info.size_, large_buf_, column_count_))) {
        LOG_WARN("fail to init data block reader", KR(ret));
      }
    } else {
      if (OB_FAIL(data_block_reader_.init(info.size_, buf_, column_count_))) {
        LOG_WARN("fail to init data block reader", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_block_reader_.get_next_item(item))) {
        LOG_WARN("fail to read item", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObDatumRowkey key(item->rowkey_datum_array_.datums_, rowkey_column_num_);
    if (OB_FAIL(key.deep_copy(startkey, allocator_))) {
      LOG_WARN("fail to deep copy", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::inner_open()
{
  int ret = OB_SUCCESS;
  int64_t data_buf_size = sstable_data_block_size_;
  int64_t start_idx = 0;
  int64_t end_idx = 0;
  const uint64_t tenant_id = MTL_ID();
  int64_t index_size = sstable_->get_meta().index_block_size_;
  data_block_reader_.reset();
  if (OB_FAIL(fragment_operator_.init(sstable_))) {
    LOG_WARN("Fail to init fragment opeartor", K(ret));
  } else if (OB_FAIL(index_block_reader_.init(
               tenant_id, index_size, sstable_->get_fragment_array().at(0).index_file_handle_))) {
    LOG_WARN("Fail to init index_block_reader", K(ret));
  } else if (OB_FAIL(locate_lower_bound(query_range_->start_key_, start_idx))) {
    LOG_WARN("fail to local lower bound", KR(ret));
  } else if (OB_FAIL(locate_upper_bound(query_range_->end_key_, end_idx))) {
    LOG_WARN("fail to local upper bound", KR(ret));
  } else {
    if (start_idx == 0) {
      start_idx_ = 0;
      curr_idx_ = 0;
    } else {
      start_idx_ = start_idx - 1;
      curr_idx_ = start_idx - 1;
    }
    end_idx_ = end_idx - 1;
    // 如果定位的下界为第一块, 属于特殊情况，则直接跳过数据块扫描
    if (end_idx_ < 0) {
      end_idx_ = 0;
      found_end_ = true;
    }
    data_block_reader_.reset();
    if (nullptr != large_buf_) {
      ob_free(large_buf_);
      large_buf_ = nullptr;
    }
  }
  // locate start and end
  if (OB_SUCC(ret)) {
    int64_t start_fragment_idx = 0;
    int64_t new_start_idx = 0;
    int64_t end_fragment_idx = 0;
    int64_t new_end_idx = 0;
    ObDirectLoadSSTableFragment start_fragment;
    ObDirectLoadSSTableFragment end_fragment;
    if (OB_FAIL(fragment_operator_.get_fragment_item_idx(start_idx_, start_fragment_idx,
                                                         new_start_idx))) {
      LOG_WARN("fail to get start fragment idx", KR(ret));
    } else if (OB_FAIL(fragment_operator_.get_fragment(start_fragment_idx, start_fragment))) {
      LOG_WARN("fail to get start fragment ", KR(ret));
    } else if (OB_FAIL(fragment_operator_.get_fragment_item_idx(end_idx_, end_fragment_idx,
                                                                new_end_idx))) {
      LOG_WARN("fail to get end fragment idx", KR(ret));
    } else if (OB_FAIL(fragment_operator_.get_fragment(end_fragment_idx, end_fragment))) {
      LOG_WARN("fail to get end fragment ", KR(ret));
    } else {
      ObDirectLoadIndexInfo start_info;
      ObDirectLoadIndexInfo end_info;
      if (OB_FAIL(index_block_reader_.change_fragment(start_fragment.index_file_handle_))) {
        LOG_WARN("fail to change start fragment", KR(ret));
      } else if (OB_FAIL(index_block_reader_.get_index_info(new_start_idx, start_info))) {
        LOG_WARN("fail to get index start info", KR(ret));
      } else if (OB_FAIL(index_block_reader_.change_fragment(end_fragment.index_file_handle_))) {
        LOG_WARN("fail to change end fragment", KR(ret));
      } else if (OB_FAIL(index_block_reader_.get_index_info(new_end_idx, end_info))) {
        LOG_WARN("fail to get index end info", KR(ret));
      } else if (OB_FAIL(file_io_handle_.open(start_fragment.data_file_handle_))) {
        LOG_WARN("Fail to open file handle", K(ret));
      } else {
        start_fragment_idx_ = start_fragment_idx;
        end_fragment_idx_ = end_fragment_idx;
        curr_fragment_idx_ = start_fragment_idx_;
        locate_start_offset_ = start_info.offset_;
        locate_end_offset_ = end_info.offset_ + end_info.size_;
        offset_ = locate_start_offset_;
      }
    }
  }
  // read buffer
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_buffer())) {
      LOG_WARN("fail to read buffer", KR(ret));
    } else if (OB_FAIL(data_block_reader_.init(data_buf_size, buf_, column_count_))) {
      if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        LOG_WARN("fail to init data block reader", KR(ret));
      } else {
        int64_t data_block_size = data_block_reader_.get_header()->occupy_size_;
        if (OB_FAIL(get_large_buffer(data_block_size))) {
          LOG_WARN("fail to get large buffer", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::locate_next_buffer()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    LOG_WARN("ObDirectLoadSSTableScanner is not init", KR(ret));
  } else {
    if (OB_FAIL(change_buffer())) {
      if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        LOG_WARN("fail to switch next buffer", KR(ret));
      } else {
        if (nullptr != large_buf_) {
          ob_free(large_buf_);
          large_buf_ = nullptr;
        }
        if (OB_FAIL(get_next_buffer())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to switch next buffer", KR(ret));
          }
        } else if (OB_FAIL(data_block_reader_.init(buf_size_, buf_, column_count_))) {
          if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
            LOG_WARN("fail to init data block reader", KR(ret));
          } else {
            int64_t data_block_size = data_block_reader_.get_header()->occupy_size_;
            if (OB_FAIL(get_large_buffer(data_block_size))) {
              LOG_WARN("fail to get large buffer", KR(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      curr_idx_++;
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::change_buffer()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    LOG_WARN("ObDirectLoadSSTableScanner is not init", KR(ret));
  } else {
    int32_t data_block_size = 0;
    data_block_size = data_block_reader_.get_header()->occupy_size_;
    buf_pos_ += data_block_size;
    int64_t buf_size = buf_size_ - buf_pos_;
    abort_unless(large_buf_ == nullptr || buf_pos_ == buf_size_);
    char *buf = buf_ + buf_pos_;
    data_block_reader_.reset();
    if (buf_size == 0) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      if (OB_FAIL(data_block_reader_.init(buf_size, buf, column_count_))) {
        LOG_WARN("fail to init data block reader", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::read_buffer(uint64_t offset, uint64_t size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = size;
  if (size <= sstable_data_block_size_) {
    if (OB_FAIL(file_io_handle_.pread(buf_, read_size, offset))) {
      LOG_WARN("fail to do pread from data file", KR(ret));
    }
  } else {
    int64_t read_size = size;
    if (large_buf_ == nullptr) {
      int64_t large_buf_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
      ObMemAttr attr(MTL_ID(), "TLD_LargeBuf");
      if (OB_ISNULL(large_buf_ = static_cast<char *>(ob_malloc(large_buf_size, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate buffer", KR(ret), K(large_buf_size));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(file_io_handle_.pread(large_buf_, read_size, offset))) {
        LOG_WARN("fail to do pread from data file", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::get_large_buffer(int64_t buf_size)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "TLD_LargeBuf");
  if (OB_ISNULL(large_buf_ = static_cast<char *>(ob_malloc(buf_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", KR(ret), K(buf_size));
  } else {
    int64_t read_size = buf_size;
    data_block_reader_.reset();
    if (OB_FAIL(file_io_handle_.pread(large_buf_, read_size, offset_))) {
      LOG_WARN("fail to do pread from data file", KR(ret));
    } else if (OB_FAIL(data_block_reader_.init(buf_size, large_buf_, column_count_))) {
      LOG_WARN("fail to init data block reader", KR(ret));
    } else {
      buf_pos_ = 0;
      buf_size_ = buf_size;
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::get_next_buffer()
{
  int ret = OB_SUCCESS;
  offset_ += buf_pos_;
  buf_pos_ = 0;
  int64_t curr_fragment_end_offset = 0;
  if (curr_fragment_idx_ == end_fragment_idx_) {
    curr_fragment_end_offset = locate_end_offset_;
    if (curr_fragment_end_offset == offset_) {
      ret = OB_ITER_END;
    }
  } else {
    ObDirectLoadSSTableFragment fragment;
    buf_size_ = sstable_data_block_size_;
    curr_fragment_end_offset =
      sstable_->get_fragment_array().at(curr_fragment_idx_).meta_.occupy_size_;
    if (curr_fragment_end_offset == offset_) {
      if (OB_FAIL(fragment_operator_.get_next_fragment(curr_fragment_idx_, fragment))) {
        LOG_WARN("fail to get next fragment", KR(ret));
      } else if (OB_FAIL(file_io_handle_.open(fragment.data_file_handle_))) {
        LOG_WARN("Fail to open file handle", K(ret));
      } else {
        if (curr_fragment_idx_ == end_fragment_idx_) {
          curr_fragment_end_offset = locate_end_offset_;
        } else {
          curr_fragment_end_offset =
            sstable_->get_fragment_array().at(curr_fragment_idx_).meta_.occupy_size_;
        }
        offset_ = 0;
      }
    }
  }
  if (OB_SUCC(ret)) {
    buf_size_ = sstable_data_block_size_;
    int64_t size = MIN(buf_size_, curr_fragment_end_offset - offset_);
    if (OB_FAIL(read_buffer(offset_, size))) {
      LOG_WARN("fail to read buffer", KR(ret));
    }
    buf_size_ = size;
  }
  return ret;
}

int ObDirectLoadSSTableScanner::inner_get_next_row(const ObDirectLoadExternalRow *&external_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_block_reader_.get_next_item(external_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next item", KR(ret));
    } else {
      if (OB_FAIL(locate_next_buffer())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to locate next buffer", KR(ret));
        }
      } else if (OB_FAIL(data_block_reader_.get_next_item(external_row))) {
        LOG_WARN("fail to read item", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::compare(ObDatumRowkey cmp_key,
                                        const ObDirectLoadExternalRow &external_row, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(external_row.rowkey_datum_array_.count_ != rowkey_column_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey column num", KR(ret), K(external_row.rowkey_datum_array_.count_),
             K(rowkey_column_num_));
  } else {
    ObDatumRowkey key(external_row.rowkey_datum_array_.datums_, rowkey_column_num_);
    if (OB_FAIL(key.compare(cmp_key, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare key", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::get_next_row(const ObDirectLoadExternalRow *&external_row)
{
  int ret = OB_SUCCESS;
  external_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableScanner not init", KR(ret), KP(this));
  } else {
    int cmp_ret = 0;
    external_row = nullptr;
    if (curr_idx_ > end_idx_ || found_end_ || sstable_->is_empty()) {
      ret = OB_ITER_END;
    }
    while (OB_SUCC(ret) && nullptr == external_row) {
      if (OB_FAIL(inner_get_next_row(external_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to inner get next row", KR(ret));
        }
      }
      if (OB_SUCC(ret) && !found_start_) {
        if (OB_FAIL(compare(query_range_->start_key_, *external_row, cmp_ret))) {
          LOG_WARN("fail to compare start key", KR(ret));
        } else if (cmp_ret < 0 || (!query_range_->border_flag_.inclusive_start() && cmp_ret == 0)) {
          external_row = nullptr;
        } else {
          found_start_ = true;
        }
      }
      if (OB_SUCC(ret) && nullptr != external_row && curr_idx_ == end_idx_) {
        if (OB_FAIL(compare(query_range_->end_key_, *external_row, cmp_ret))) {
          LOG_WARN("fail to compare start key", KR(ret));
        } else if (cmp_ret > 0 || (!query_range_->border_flag_.inclusive_end() && cmp_ret == 0)) {
          external_row = nullptr;
          found_end_ = true;
          ret = OB_ITER_END;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableScanner::get_next_row(const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableScanner not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadExternalRow *external_row = nullptr;
    if (OB_FAIL(get_next_row(external_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else if (OB_FAIL(external_row->to_datums(datum_row_.storage_datums_, datum_row_.count_))) {
      LOG_WARN("fail to transfer datum row", KR(ret));
    } else {
      datum_row = &datum_row_;
    }
  }
  return ret;
}
/**
 * ObDirectLoadIndexBlockMetaIterator
 */
ObDirectLoadIndexBlockMetaIterator::ObDirectLoadIndexBlockMetaIterator()
  : buf_pos_(0),
    buf_size_(0),
    io_timeout_ms_(0),
    buf_(nullptr),
    curr_fragment_idx_(0),
    curr_block_idx_(0),
    rowkey_column_count_(0),
    total_index_block_count_(0),
    index_item_num_per_block_(0),
    allocator_("TLD_IDBMeta"),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadIndexBlockMetaIterator::~ObDirectLoadIndexBlockMetaIterator() {}

void ObDirectLoadIndexBlockMetaIterator::assign(const int64_t buf_pos, const int64_t buf_cap,
                                                char *buf)
{
  buf_pos_ = buf_pos;
  buf_size_ = buf_cap;
  buf_ = buf;
}

int ObDirectLoadIndexBlockMetaIterator::init(ObDirectLoadSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObDirectLoadIndexBlockMetaIterator has been inited", K(ret));
  } else if (OB_ISNULL(sstable) || !sstable->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KPC(sstable));
  } else {
    sstable_ = sstable;
    const uint64_t tenant_id = MTL_ID();
    int64_t index_size = sstable_->get_meta().index_block_size_;
    if (!sstable_->is_empty()) {
      if (OB_FAIL(fragment_operator_.init(sstable_))) {
        LOG_WARN("Failed to init fragment operator", K(ret));
      } else if (OB_FAIL(index_block_reader_.init(
                   tenant_id, index_size,
                   sstable_->get_fragment_array().at(0).index_file_handle_))) {
        LOG_WARN("Fail to init index_block_reader", K(ret));
      } else if (OB_FAIL(
                   file_io_handle_.open(sstable_->get_fragment_array().at(0).data_file_handle_))) {
        LOG_WARN("Fail to assign file handle", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t buf_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
      if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate buffer", KR(ret), K(buf_size));
      } else {
        rowkey_column_count_ = sstable_->get_meta().rowkey_column_count_;
        index_item_num_per_block_ = ObDirectLoadIndexBlock::get_item_num_per_block(
          sstable_->get_meta().index_block_size_);
        total_index_block_count_ = sstable->get_meta().index_block_count_;
        assign(0, buf_size, buf_);
        io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObDirectLoadIndexBlockMetaIterator::get_end_key(ObDirectLoadExternalRow &row,
                                                    ObDirectLoadIndexBlockMeta &index_block_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta iterator not inited", K(ret));
  } else {
    int64_t count = sstable_->get_meta().rowkey_column_count_;
    ObDatumRowkey key(row.rowkey_datum_array_.datums_, count);
    if (OB_FAIL(key.deep_copy(index_block_meta.end_key_, allocator_))) {
      LOG_WARN("fail to deep copy", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadIndexBlockMetaIterator::get_next(ObDirectLoadIndexBlockMeta &index_block_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta iterator not inited", K(ret));
  } else {
    if (curr_block_idx_ >= total_index_block_count_ || sstable_->is_empty()) {
      ret = OB_ITER_END;
    }
    int64_t row_count = 0;
    int64_t locate_fragment_idx = 0;
    int64_t new_idx = 0;
    ObDirectLoadSSTableFragment fragment;
    if (OB_SUCC(ret) && curr_block_idx_ < total_index_block_count_) {
      if (OB_FAIL(fragment_operator_.get_fragment_block_idx(curr_block_idx_, locate_fragment_idx,
                                                            new_idx))) {
        LOG_WARN("fail to get start fragment idx", KR(ret));
      } else if (OB_FAIL(fragment_operator_.get_fragment(locate_fragment_idx, fragment))) {
        LOG_WARN("fail to get start fragment ", KR(ret));
      } else {
        curr_fragment_idx_ = locate_fragment_idx;
        if (OB_FAIL(index_block_reader_.change_fragment(fragment.index_file_handle_))) {
          LOG_WARN("fail to change start fragment", KR(ret));
        } else if (OB_FAIL(file_io_handle_.open(fragment.data_file_handle_))) {
          LOG_WARN("Fail to open file handle", K(ret));
        } else if (OB_FAIL(get_row(new_idx, index_block_meta))) {
          LOG_WARN("Fail to get secondary meta row from block", K(ret));
        } else if (OB_FAIL(get_end_key(row_, index_block_meta))) {
          LOG_WARN("Fail to parse macro meta", K(ret));
        } else {
          curr_block_idx_++;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadIndexBlockMetaIterator::get_row(int64_t idx, ObDirectLoadIndexBlockMeta &meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta iterator not inited", K(ret));
  } else {
    int64_t count = 0;
    int64_t index_item_idx = (idx + 1) * index_item_num_per_block_ - 1;
    index_item_idx =
      MIN(index_item_idx,
          sstable_->get_fragment_array().at(curr_fragment_idx_).meta_.index_item_count_ - 1);
    ObDirectLoadIndexInfo info;
    ObDirectLoadDataBlockHeader header;
    if (OB_FAIL(index_block_reader_.get_index_info(index_item_idx, info))) {
      LOG_WARN("fail to get index info", KR(ret));
    } else if (OB_FAIL(read_buffer(info.offset_, info.size_))) {
      LOG_WARN("fail to read buffer", KR(ret));
    } else if (OB_FAIL(header.deserialize(buf_, buf_size_, buf_pos_))) {
      LOG_WARN("fail to deserialize header", KR(ret), K(buf_size_), K(buf_pos_));
    } else {
      meta.row_count_ = index_block_reader_.get_header()->row_count_;
      meta.rowkey_column_count_ = rowkey_column_count_;
      buf_pos_ = header.last_row_offset_;
      if (OB_FAIL(row_.deserialize(buf_, buf_size_, buf_pos_))) {
        LOG_WARN("fail to deserialize buffer", KR(ret), K(buf_size_), K(buf_pos_));
      }
    }
  }
  return ret;
}

int ObDirectLoadIndexBlockMetaIterator::read_buffer(uint64_t offset, uint64_t size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = size;
  if (OB_FAIL(file_io_handle_.pread(buf_, read_size, offset))) {
    LOG_WARN("fail to do pread from data file", KR(ret));
  } else {
    buf_pos_ = 0;
    buf_size_ = size;
  }
  return ret;
}

/**
 * ObDirectLoadIndexBlockEndKeyIterator
 */

ObDirectLoadIndexBlockEndKeyIterator::~ObDirectLoadIndexBlockEndKeyIterator()
{
  if (nullptr != index_block_meta_iter_) {
    index_block_meta_iter_->~ObDirectLoadIndexBlockMetaIterator();
    index_block_meta_iter_ = nullptr;
  }
}

int ObDirectLoadIndexBlockEndKeyIterator::init(
  ObDirectLoadIndexBlockMetaIterator *index_block_meta_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadIndexBlockEndKeyIterator init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(index_block_meta_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(index_block_meta_iter));
  } else {
    index_block_meta_iter_ = index_block_meta_iter;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadIndexBlockEndKeyIterator::get_next_rowkey(const ObDatumRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadIndexBlockEndKeyIterator not init", KR(ret), KP(this));
  } else {
    ObDirectLoadIndexBlockMeta index_block_meta;
    if (OB_FAIL(index_block_meta_iter_->get_next(index_block_meta))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next macro block meta", KR(ret));
      }
    } else if (OB_FAIL(rowkey_.assign(index_block_meta.end_key_.datums_,
                                      index_block_meta.rowkey_column_count_))) {
      LOG_WARN("fail to get datum rowkey", KR(ret));
    } else {
      rowkey = &rowkey_;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
