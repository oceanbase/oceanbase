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

#include "ob_imicro_block_writer.h"
#include "lib/utility/utility.h"
#include  "ob_row_writer.h"
namespace oceanbase
{
namespace blocksstable
{

/**
 * -------------------------------------------------------------------ObMicroBlockDesc-------------------------------------------------------------------
 */
bool ObMicroBlockDesc::is_valid() const
{
  return last_rowkey_.is_valid()
         && NULL != header_ && header_->is_valid()
         && NULL != buf_ && buf_size_ > 0
         && data_size_ > 0 && row_count_ > 0 && column_count_ > 0
         && max_merged_trans_version_  >= 0;
}

void ObMicroBlockDesc::reset()
{
  last_rowkey_.reset();
  buf_ = NULL;
  header_ = NULL;
  aggregated_row_ = NULL;
  buf_size_ = 0;
  data_size_ = 0;
  row_count_ = 0;
  column_count_ = 0;
  max_merged_trans_version_ = 0;
  macro_id_.reset();
  block_offset_ = 0;
  block_checksum_ = 0;
  row_count_delta_ = 0;
  contain_uncommitted_row_ = false;
  can_mark_deletion_ = false;
  has_string_out_row_ = false;
  has_lob_out_row_ = false;
  original_size_ = 0;
  is_last_row_last_flag_ = false;
}

 /**
 * -------------------------------------------------------------------ObMicroBufferWriter-------------------------------------------------------------------
 */
int ObMicroBufferWriter::init(const int64_t capacity, const int64_t reserve_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "micro buffer writer is inited", K(ret), K(capacity_));
  } else if (OB_UNLIKELY(reserve_size < 0 || capacity > MAX_DATA_BUFFER_SIZE
      || capacity < reserve_size)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(capacity), K(reserve_size));
  } else {
    capacity_ = capacity;
    len_ = 0;
    data_= nullptr;
    buffer_size_ = 0;
    reset_memory_threshold_ = DEFAULT_RESET_MEMORY_THRESHOLD;
    memory_reclaim_cnt_ = 0;
  }

  if (OB_SUCC(ret)) {
    if(OB_FAIL(reserve(reserve_size))) {
      STORAGE_LOG(WARN, "failed to reserve", K(ret), K(reserve_size));
    } else {
      default_reserve_ = reserve_size;
      is_inited_ = true;
    }
  }

  return ret;
}

void ObMicroBufferWriter::reset()
{
  if (data_ != nullptr) {
    allocator_.free(data_);
    data_ = nullptr;
  }
  has_expand_ = false;
  memory_reclaim_cnt_ = 0;
  reset_memory_threshold_ = 0;
  default_reserve_ = 0;
  len_ = 0;
  buffer_size_ = 0;
  capacity_ = 0;
  is_inited_ = false;
  allocator_.reset();
}

void ObMicroBufferWriter::reuse()
{
  if (buffer_size_ > default_reserve_ && len_ <= default_reserve_) {
    memory_reclaim_cnt_++;
    if (memory_reclaim_cnt_ >= reset_memory_threshold_) {
      reset_memory_threshold_ <<= 1;
      memory_reclaim_cnt_ = 0;
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(default_reserve_))) {
        int ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to reclaim memory", K(ret), K(default_reserve_));
      } else {
        allocator_.free(data_);
        buffer_size_ = default_reserve_;
        data_ = reinterpret_cast<char *>(buf);
      }
    }
  } else {
    memory_reclaim_cnt_ = 0;
  }
  has_expand_ = false;
  len_ = 0;
}

int ObMicroBufferWriter::expand(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(capacity_ <= buffer_size_ || size > capacity_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(size), K(buffer_size_), K(capacity_));
  } else {
    int64_t expand_size = buffer_size_ * 2;
    while (expand_size < size) {
      expand_size <<= 1;
    }
    expand_size = MIN(expand_size, capacity_);
    if (OB_FAIL(reserve(expand_size))) {
      STORAGE_LOG(WARN, "fail to reserve", K(ret), K(expand_size));
    }
  }

  return ret;
}

int ObMicroBufferWriter::reserve(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size < 0 || size > capacity_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(size), K(capacity_));
  } else if (size <= buffer_size_) {//do nothing
  } else {
    void* buf = nullptr;
    const int64_t alloc_size = MAX(size, MIN_BUFFER_SIZE);
    if (OB_ISNULL(buf = allocator_.alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(alloc_size));
    } else if (data_ != nullptr) {
      has_expand_ = true;
      MEMCPY(buf, data_, len_);
      allocator_.free(data_);
      data_ = nullptr;
    }
    if (OB_SUCC(ret)) {
      data_ = reinterpret_cast<char *>(buf);
      buffer_size_ = alloc_size;
    }
  }

  return ret;
}

int ObMicroBufferWriter::ensure_space(const int64_t append_size)
{
  int ret = OB_SUCCESS;

  if (len_ + append_size > capacity_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (len_ + append_size > buffer_size_) {
    if (OB_FAIL(expand(len_ + append_size))) {
      STORAGE_LOG(WARN, "failed to expand size", K(ret), K(len_), K(append_size));
    }
  }

  return ret;
}

int ObMicroBufferWriter::write_nop(const int64_t size, bool is_zero)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(size), K(len_), K(capacity_));
  } else if (OB_FAIL(ensure_space(size))) {
    if (ret != OB_BUF_NOT_ENOUGH) {
      STORAGE_LOG(WARN, "failed to ensure space", K(ret), K(size));
    }
  } else {
    if (is_zero) {
      MEMSET(data_ + len_, 0, size);
    }
    len_ += size;
  }

  return ret;
}

int ObMicroBufferWriter::write(const ObDatumRow &row, const int64_t rowkey_cnt, int64_t &size)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;

  if ((buffer_size_ == len_) && OB_FAIL(expand(buffer_size_))) {
    STORAGE_LOG(WARN, "failed to reserve", K(ret), K(buffer_size_));
  }

  while (OB_SUCC(ret)) {
    if (OB_SUCC(row_writer.write(rowkey_cnt, row, data_ + len_, buffer_size_ - len_, size))) {
      break;
    } else {
      if (OB_UNLIKELY(ret != OB_BUF_NOT_ENOUGH)) {
        STORAGE_LOG(WARN, "failed to write row", K(ret), K(buffer_size_), K(capacity_));
      } else if (buffer_size_ >= capacity_) { //break
      } else if (OB_FAIL(expand(buffer_size_))) {
        STORAGE_LOG(WARN, "failed to reserve", K(ret), K(buffer_size_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    len_ += size;
  }
  return ret;
}

int ObMicroBufferWriter::write(const void *buf, int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(buf == nullptr || size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(buf), K(size), K(len_), K(capacity_));
  } else if (OB_FAIL(ensure_space(size))) {
    if (ret != OB_BUF_NOT_ENOUGH) {
      STORAGE_LOG(WARN, "failed to ensure space", K(ret), K(size));
    }
  } else {
    MEMCPY(data_ + len_, buf, size);
    len_ += size;
  }

  return ret;
}

int ObMicroBufferWriter::advance(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size < 0 || len_ + size > buffer_size_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(size), K(len_), K(buffer_size_));
  } else {
    len_ += size;
  }
  return ret;
}

int ObMicroBufferWriter::set_length(const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(len > buffer_size_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(len), K(len_), K(buffer_size_));
  } else {
    len_ = len;
  }
  return ret;
}
 /**
 * -------------------------------------------------------------------ObIMicroBlockWriter-------------------------------------------------------------------
 */
int ObIMicroBlockWriter::build_micro_block_desc(ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;
  micro_block_desc.reset();

  char *block_buffer = NULL;
  int64_t block_size = 0;
  if (OB_FAIL(build_block(block_buffer, block_size))) {
    STORAGE_LOG(WARN, "failed to build micro block", K(ret));
  } else {
    ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader *>(block_buffer);
    micro_block_desc.header_ = micro_header;
    micro_block_desc.buf_ = block_buffer + micro_block_desc.header_->header_size_;
    micro_block_desc.buf_size_ = block_size - micro_block_desc.header_->header_size_;
    micro_block_desc.data_size_ = micro_block_desc.buf_size_;
    micro_block_desc.row_count_ = get_row_count();
    micro_block_desc.column_count_ = get_column_count();
    micro_block_desc.row_count_delta_ = get_row_count_delta();
    micro_block_desc.max_merged_trans_version_ = get_max_merged_trans_version();
    micro_block_desc.contain_uncommitted_row_ = is_contain_uncommitted_row();
    micro_block_desc.block_checksum_ = get_micro_block_checksum();
    micro_block_desc.has_string_out_row_ = has_string_out_row_;
    micro_block_desc.has_lob_out_row_ = has_lob_out_row_;
    micro_block_desc.original_size_ = get_original_size();
    micro_block_desc.is_last_row_last_flag_ = is_last_row_last_flag();
    // fill micro header for bugfix on micro block that bypass compression/encryption
    // since these fields will be only filled on compression in current implementation
    micro_header->data_length_ = micro_block_desc.buf_size_;
    micro_header->data_zlength_ = micro_block_desc.buf_size_;
    micro_header->data_checksum_ = ob_crc64_sse42(0, micro_block_desc.buf_, micro_block_desc.buf_size_);
    micro_header->original_length_ = micro_block_desc.original_size_;
    micro_header->set_header_checksum();
  }
  // do not reuse micro writer here
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase
