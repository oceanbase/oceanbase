/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_csv_prefetch_mgr.h"
#include "sql/engine/cmd/ob_load_data_file_reader.h"

namespace oceanbase
{
using namespace common;
namespace sql {

ObCSVPrefetchMgr::ObCSVPrefetchMgr()
  : file_access_(),
    slots_(nullptr),
    prefetch_buf_size_(0),
    prefetch_count_(0),
    cur_slot_idx_(0),
    pending_slot_count_(0),
    next_prefetch_offset_(0),
    file_size_(0),
    end_offset_(INT64_MAX),
    all_prefetched_(true),
    is_inited_(false),
    allocator_(nullptr),
    remain_buf_(nullptr),
    remain_data_size_(0),
    decompressor_(nullptr),
    compression_format_(ObCSVGeneralFormat::ObCSVCompression::NONE),
    compressed_data_(nullptr),
    compressed_data_capacity_(0),
    compress_data_size_(0),
    consumed_data_size_(0)
{}

ObCSVPrefetchMgr::~ObCSVPrefetchMgr()
{
  reset();
}

int ObCSVPrefetchMgr::init(common::ObIAllocator &allocator,
                           ObCSVGeneralFormat::ObCSVCompression compression_format,
                           const int64_t buf_size,
                           const int64_t prefetch_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[CSV PREFETCH] csv prefetch mgr already inited", K(ret));
  } else if (OB_UNLIKELY(buf_size <= 0 || prefetch_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[CSV PREFETCH] invalid argument", K(ret), K(buf_size), K(prefetch_count));
  } else {
    allocator_ = &allocator;
    prefetch_buf_size_ = buf_size;
    prefetch_count_ = prefetch_count;
    compression_format_ = compression_format;

    // slot
    void *slot_mem = allocator_->alloc(sizeof(PrefetchSlot) * prefetch_count_);
    if (OB_ISNULL(slot_mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[CSV PREFETCH] fail to alloc prefetch slots", K(ret), K(prefetch_count_));
    } else {
      slots_ = static_cast<PrefetchSlot *>(slot_mem);
      for (int64_t i = 0; i < prefetch_count_; i++) {
        new (&slots_[i]) PrefetchSlot();
      }
    }

    // slot.buffer
    for (int64_t i = 0; OB_SUCC(ret) && i < prefetch_count_; i++) {
      void *buf = allocator_->alloc(prefetch_buf_size_);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[CSV PREFETCH] fail to alloc prefetch buffer", K(ret), K(i), K(prefetch_buf_size_));
      } else {
        slots_[i].buf_ = static_cast<char *>(buf);
      }
    }

    // remain_buf_
    if (OB_SUCC(ret)) {
      void *rbuf = allocator_->alloc(prefetch_buf_size_);
      if (OB_ISNULL(rbuf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[CSV PREFETCH] fail to alloc remain buffer", K(ret), K(prefetch_buf_size_));
      } else {
        remain_buf_ = static_cast<char *>(rbuf);
        remain_data_size_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
      LOG_TRACE("[CSV PREFETCH] init success", K(prefetch_buf_size_), K(prefetch_count_),
                K(compression_format_));
    } else {
      reset();
    }
  }
  return ret;
}

int ObCSVPrefetchMgr::open(const ObExternalFileUrlInfo &file_info,
                           const ObExternalFileCacheOptions &cache_options,
                           const int64_t start_offset,
                           const int64_t end_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[CSV PREFETCH] csv prefetch mgr not inited", K(ret));
  } else if (OB_UNLIKELY(start_offset < 0 || end_offset < 0 || start_offset > end_offset)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[CSV PREFETCH] invalid offset", K(ret), K(start_offset), K(end_offset));
  } else {
    close();

    if (OB_FAIL(file_access_.open(file_info, cache_options))) {
      LOG_WARN("[CSV PREFETCH] fail to open file", K(ret), K(file_info));
    } else if (OB_FAIL(file_access_.get_file_size(file_size_))) {
      LOG_WARN("[CSV PREFETCH] fail to get file size", K(ret));
    } else {
      end_offset_ = (end_offset == INT64_MAX) ? file_size_ : MIN(end_offset, file_size_);
      cur_slot_idx_ = 0;
      pending_slot_count_ = 0;
      next_prefetch_offset_ = start_offset;
      all_prefetched_ = false;

      ObCSVGeneralFormat::ObCSVCompression this_file_compression = compression_format_;
      if (this_file_compression == ObCSVGeneralFormat::ObCSVCompression::AUTO) {
        if (OB_FAIL(compression_algorithm_from_suffix(file_info.url_, this_file_compression))) {
          LOG_WARN("[CSV PREFETCH] fail to detect compression format from filename",
                   K(ret), K(file_info.url_));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(create_decompressor(this_file_compression))) {
        LOG_WARN("[CSV PREFETCH] fail to create decompressor", K(ret), K(this_file_compression));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < prefetch_count_; i++) {
        OZ (submit_prefetch(i));
      }

      LOG_TRACE("[CSV PREFETCH] open file", K(ret), K(file_info),
               K(start_offset), K(end_offset_), K(file_size_), K(pending_slot_count_),
               K(this_file_compression));
    }
  }
  return ret;
}

int ObCSVPrefetchMgr::submit_prefetch(int64_t slot_idx)
{
  int ret = OB_SUCCESS;
  if (all_prefetched_) {
    // do nothing
  } else if (slot_idx < 0 || slot_idx >= prefetch_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[CSV PREFETCH] invalid slot index", K(ret), K(slot_idx), K(prefetch_count_));
  } else {
    PrefetchSlot &slot = slots_[slot_idx];
    if (OB_UNLIKELY(slot.state_ != SlotState::EMPTY)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[CSV PREFETCH] slot is not in EMPTY state", K(ret), K(slot_idx));
    } else {
      slot.reuse();
      int64_t remain = end_offset_ - next_prefetch_offset_;
      int64_t read_size = MIN(prefetch_buf_size_, remain);
      if (read_size <= 0) {
        all_prefetched_ = true;
      } else {
        ObExternalReadInfo read_info(next_prefetch_offset_,
                                    slot.buf_,
                                    read_size,
                                    DEFAULT_IO_TIMEOUT_MS);
        if (OB_FAIL(file_access_.async_read(read_info, slot.handle_))) {
          LOG_WARN("[CSV PREFETCH] fail to issue async read", K(ret), K(slot_idx),
                  K(next_prefetch_offset_), K(read_size));
        } else {
          slot.state_ = SlotState::LOADING;
          LOG_TRACE("[CSV PREFETCH] issue prefetch", K(slot_idx),
                  K(next_prefetch_offset_), K(read_size), K(pending_slot_count_));
          next_prefetch_offset_ += read_size;
          pending_slot_count_++;
        }
      }
    }
  }
  return ret;
}

int ObCSVPrefetchMgr::create_decompressor(ObCSVGeneralFormat::ObCSVCompression compression_format)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[CSV PREFETCH] allocator is null", K(ret));
  } else if (compression_format == ObCSVGeneralFormat::ObCSVCompression::NONE) {
    if (OB_NOT_NULL(decompressor_)) {
      ObDecompressor::destroy(decompressor_);
      decompressor_ = nullptr;
    }
  } else if (OB_NOT_NULL(decompressor_) && decompressor_->compression_format() == compression_format) {
    // reuse decompressor, snappy needs manual reset for new file
    if (compression_format == ObCSVGeneralFormat::ObCSVCompression::SNAPPY_BLOCK) {
      decompressor_->destroy();
      decompressor_->init();
    }
  } else {
    if (OB_NOT_NULL(decompressor_)) {
      ObDecompressor::destroy(decompressor_);
      decompressor_ = nullptr;
    }

    if (OB_FAIL(ObDecompressor::create(compression_format, *allocator_, decompressor_))) {
      LOG_WARN("[CSV PREFETCH] fail to create decompressor", K(ret), K(compression_format));
    } else if (OB_ISNULL(compressed_data_)) {
      compressed_data_capacity_ = DEFAULT_COMPRESSED_DATA_BUFFER_SIZE;
      if (OB_SUCC(ret) && OB_ISNULL(compressed_data_ = static_cast<char *>(allocator_->alloc(compressed_data_capacity_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[CSV PREFETCH] fail to allocate compressed data buffer",
                 K(ret), K(compressed_data_capacity_));
      }
    }
  }
  return ret;
}

int ObCSVPrefetchMgr::get_buffer(char *buf, const int64_t buf_size, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[CSV PREFETCH] csv prefetch mgr not inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[CSV PREFETCH] invalid buffer", K(ret), KP(buf), K(buf_size));
  } else if (OB_NOT_NULL(decompressor_)) {
    if (OB_FAIL(get_buffer_decompress(buf, buf_size, read_size))) {
      LOG_WARN("[CSV PREFETCH] fail to get buffer decompress", K(ret));
    }
  } else if (OB_FAIL(get_buffer_raw(buf, buf_size, read_size))) {
    LOG_WARN("[CSV PREFETCH] fail to get buffer raw", K(ret));
  }
  return ret;
}

int ObCSVPrefetchMgr::get_buffer_raw(char *buf, const int64_t buf_size, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (remain_data_size_ > 0) {
    int64_t copy_size = MIN(buf_size, remain_data_size_);
    MEMCPY(buf, remain_buf_, copy_size);
    read_size = copy_size;

    remain_data_size_ -= copy_size;
    if (remain_data_size_ > 0) {
      MEMMOVE(remain_buf_, remain_buf_ + copy_size, remain_data_size_);
    }
    LOG_TRACE("[CSV PREFETCH] get_buffer_raw from remain", K(copy_size), K_(remain_data_size));
  } else if (pending_slot_count_ <= 0) {
    // no more data
  } else {
    int64_t slot_idx = cur_slot_idx_ % prefetch_count_;
    PrefetchSlot &slot = slots_[slot_idx];

    if (OB_UNLIKELY(slot.state_ != SlotState::LOADING)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[CSV PREFETCH] slot is not in LOADING state", K(ret), K(slot_idx), K(cur_slot_idx_));
    } else if (OB_FAIL(slot.handle_.wait())) {
      LOG_WARN("[CSV PREFETCH] fail to wait for prefetch completion", K(ret), K(slot_idx));
    } else if (OB_FAIL(slot.handle_.get_user_buf_read_data_size(slot.data_size_))) {
      LOG_WARN("[CSV PREFETCH] fail to get read data size", K(ret), K(slot_idx));
    } else {
      read_size = MIN(buf_size, slot.data_size_);
      MEMCPY(buf, slot.buf_, read_size);

      // handle leftover data
      int64_t leftover = slot.data_size_ - read_size;
      if (leftover > 0) {
        if (OB_UNLIKELY(leftover > prefetch_buf_size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[CSV PREFETCH] leftover exceeds remain buffer size",
                   K(ret), K(leftover), K_(prefetch_buf_size));
        } else {
          MEMCPY(remain_buf_, slot.buf_ + read_size, leftover);
          remain_data_size_ = leftover;
        }
      }

      slot.state_ = SlotState::EMPTY;
      pending_slot_count_--;
      cur_slot_idx_++;

      OZ (submit_prefetch(slot_idx));

      LOG_TRACE("[CSV PREFETCH] get_buffer_raw from slot", K(slot_idx), K(read_size),
               "slot_data_size", slot.data_size_, K(leftover), K(pending_slot_count_));
    }
  }
  return ret;
}

int ObCSVPrefetchMgr::get_buffer_decompress(char *buf, const int64_t buf_size, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (OB_ISNULL(decompressor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[CSV PREFETCH] decompressor is null", K(ret));
  } else if ((consumed_data_size_ >= compress_data_size_ && !decompressor_->has_remain_data())
             || (decompressor_->need_more_data())) {
    // 需要读取更多压缩数据：
    // 1. 当前压缩数据已全部消费且解压器内部无残留数据
    // 2. 解压器表示当前数据不完整，需要更多数据
    if (pending_slot_count_ > 0 || !all_prefetched_) {
      ret = read_compressed_data();
    }
  }

  if (OB_SUCC(ret) && (compress_data_size_ > consumed_data_size_ || decompressor_->has_remain_data())) {
    int64_t consumed_size = 0;
    ret = decompressor_->decompress(compressed_data_ + consumed_data_size_,
                                    compress_data_size_ - consumed_data_size_,
                                    consumed_size,
                                    buf,
                                    buf_size,
                                    read_size);
    if (OB_FAIL(ret)) {
      LOG_WARN("[CSV PREFETCH] fail to decompress", K(ret));
    } else {
      consumed_data_size_ += consumed_size;
      LOG_TRACE("[CSV PREFETCH] decompress done", K(consumed_size), K(read_size),
               K_(compress_data_size), K_(consumed_data_size));
    }
  }

  return ret;
}

int ObCSVPrefetchMgr::read_compressed_data()
{
  int ret = OB_SUCCESS;
  char *read_buffer = compressed_data_;

  if (OB_UNLIKELY(consumed_data_size_ < compress_data_size_)) {
    // backup remaining data
    const int64_t last_data_size = compress_data_size_ - consumed_data_size_;
    MEMMOVE(compressed_data_, compressed_data_ + consumed_data_size_, last_data_size);
    read_buffer = compressed_data_ + last_data_size;
    consumed_data_size_ = 0;
    compress_data_size_ = last_data_size;
  } else if (consumed_data_size_ == compress_data_size_) {
    consumed_data_size_ = 0;
    compress_data_size_ = 0;
  }

  if (OB_SUCC(ret)) {
    // read data into compressed buffer
    int64_t capacity = compressed_data_capacity_ - compress_data_size_;
    int64_t read_size = 0;
    if (OB_FAIL(get_buffer_raw(read_buffer, capacity, read_size))) {
      LOG_WARN("[CSV PREFETCH] fail to get buffer raw", K(ret));
    } else {
      compress_data_size_ += read_size;
    }
  }
  return ret;
}

bool ObCSVPrefetchMgr::eof() const
{
  bool is_eof = all_prefetched_ && pending_slot_count_ <= 0 && remain_data_size_ <= 0;
  if (OB_NOT_NULL(decompressor_)) {
    is_eof = is_eof && consumed_data_size_ >= compress_data_size_
             && !decompressor_->has_remain_data();
  }
  return is_eof;
}

void ObCSVPrefetchMgr::close()
{
  if (nullptr != slots_) {
    for (int64_t i = 0; i < prefetch_count_; i++) {
      if (slots_[i].state_ == SlotState::LOADING) {
        (void) slots_[i].handle_.wait();
      }
      slots_[i].reuse();
    }
  }
  LOG_TRACE("[CSV PREFETCH] close", K_(cur_slot_idx), K_(pending_slot_count),
           K_(next_prefetch_offset), K_(file_size), K_(all_prefetched), K_(remain_data_size),
           K_(compress_data_size), K_(consumed_data_size));
  file_access_.close();
  cur_slot_idx_ = 0;
  pending_slot_count_ = 0;
  next_prefetch_offset_ = 0;
  file_size_ = 0;
  end_offset_ = INT64_MAX;
  all_prefetched_ = true;
  remain_data_size_ = 0;
  compress_data_size_ = 0;
  consumed_data_size_ = 0;
}

void ObCSVPrefetchMgr::reset()
{
  close();
  if (nullptr != allocator_) {
    if (nullptr != slots_) {
      for (int64_t i = 0; i < prefetch_count_; i++) {
        if (nullptr != slots_[i].buf_) {
          allocator_->free(slots_[i].buf_);
          slots_[i].buf_ = nullptr;
        }
        slots_[i].~PrefetchSlot();
      }
      allocator_->free(slots_);
      slots_ = nullptr;
    }
    if (nullptr != remain_buf_) {
      allocator_->free(remain_buf_);
      remain_buf_ = nullptr;
    }
    if (nullptr != compressed_data_) {
      allocator_->free(compressed_data_);
      compressed_data_ = nullptr;
    }
  }
  if (nullptr != decompressor_) {
    ObDecompressor::destroy(decompressor_);
    decompressor_ = nullptr;
  }
  file_access_.reset();
  prefetch_buf_size_ = 0;
  prefetch_count_ = 0;
  is_inited_ = false;
  allocator_ = nullptr;
  compression_format_ = ObCSVGeneralFormat::ObCSVCompression::NONE;
  compressed_data_capacity_ = 0;
  compress_data_size_ = 0;
  consumed_data_size_ = 0;
}

}
}
