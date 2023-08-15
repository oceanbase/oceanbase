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
#pragma once

#include "observer/table_load/ob_table_load_stat.h"
#include "share/config/ob_server_config.h"
#include "storage/direct_load/ob_direct_load_data_block_decoder.h"
#include "storage/direct_load/ob_direct_load_external_interface.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"

namespace oceanbase
{
namespace storage
{

template <typename Header, typename T>
class ObDirectLoadDataBlockReader : public ObDirectLoadExternalIterator<T>
{
public:
  ObDirectLoadDataBlockReader();
  virtual ~ObDirectLoadDataBlockReader();
  void reuse();
  void reset();
  int init(int64_t data_block_size, int64_t buf_size, common::ObCompressorType compressor_type);
  int open(const ObDirectLoadTmpFileHandle &file_handle, int64_t offset, int64_t size);
  int get_next_item(const T *&item) override;
  OB_INLINE int64_t get_block_count() const { return block_count_; }
protected:
  virtual int prepare_read_block() { return common::OB_SUCCESS; }
private:
  int read_next_buffer();
  int switch_next_block();
protected:
  common::ObArenaAllocator allocator_;
  int64_t data_block_size_;
  char *buf_;
  int64_t buf_capacity_;
  int64_t buf_size_;
  int64_t buf_pos_;
  int64_t io_timeout_ms_;
  ObDirectLoadDataBlockDecoder<Header> data_block_reader_;
  ObDirectLoadTmpFileIOHandle file_io_handle_;
  T curr_item_;
  int64_t offset_;
  int64_t read_size_;
  int64_t block_count_;
  bool is_opened_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadDataBlockReader);
};

template <typename Header, typename T>
ObDirectLoadDataBlockReader<Header, T>::ObDirectLoadDataBlockReader()
  : allocator_("TLD_DBReader"),
    data_block_size_(0),
    buf_(nullptr),
    buf_capacity_(0),
    buf_size_(0),
    buf_pos_(0),
    io_timeout_ms_(0),
    offset_(0),
    read_size_(0),
    block_count_(0),
    is_opened_(false),
    is_inited_(false)
{
}

template <typename Header, typename T>
ObDirectLoadDataBlockReader<Header, T>::~ObDirectLoadDataBlockReader()
{
  reset();
}

template <typename Header, typename T>
void ObDirectLoadDataBlockReader<Header, T>::reuse()
{
  buf_size_ = 0;
  buf_pos_ = 0;
  data_block_reader_.reuse();
  file_io_handle_.reset();
  curr_item_.reuse();
  offset_ = 0;
  read_size_ = 0;
  block_count_ = 0;
  is_opened_ = false;
}

template <typename Header, typename T>
void ObDirectLoadDataBlockReader<Header, T>::reset()
{
  data_block_size_ = 0;
  buf_ = nullptr;
  buf_capacity_ = 0;
  buf_size_ = 0;
  buf_pos_ = 0;
  io_timeout_ms_ = 0;
  allocator_.reset();
  data_block_reader_.reset();
  file_io_handle_.reset();
  curr_item_.reset();
  offset_ = 0;
  read_size_ = 0;
  block_count_ = 0;
  is_opened_ = false;
  is_inited_ = false;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockReader<Header, T>::init(int64_t data_block_size, int64_t buf_size,
                                                 common::ObCompressorType compressor_type)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockReader init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(data_block_size <= 0 || data_block_size % DIO_ALIGN_SIZE != 0 ||
                         buf_size <= 0 || buf_size % DIO_ALIGN_SIZE != 0 ||
                         data_block_size > buf_size ||
                         compressor_type <= common::ObCompressorType::INVALID_COMPRESSOR)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), K(data_block_size), K(buf_size), K(compressor_type));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(buf_size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", KR(ret), K(buf_size));
    } else if (OB_FAIL(data_block_reader_.init(buf_size, compressor_type))) {
      STORAGE_LOG(WARN, "fail to init data block reader", KR(ret));
    } else {
      data_block_size_ = data_block_size;
      buf_capacity_ = buf_size;
      io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockReader<Header, T>::open(const ObDirectLoadTmpFileHandle &file_handle,
                                                 int64_t offset, int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockReader not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_opened_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "external block reader already opened", KR(ret));
  } else if (OB_UNLIKELY(!file_handle.is_valid() || offset < 0 || size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), K(file_handle), K(offset), K(size));
  } else {
    reuse();
    offset_ = offset;
    read_size_ = size;
    if (OB_FAIL(file_io_handle_.open(file_handle))) {
      STORAGE_LOG(WARN, "fail to open file handle", KR(ret));
    } else if (OB_FAIL(switch_next_block())) {
      STORAGE_LOG(WARN, "fail to switch next block", KR(ret));
    } else {
      is_opened_ = true;
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockReader<Header, T>::read_next_buffer()
{
  int ret = common::OB_SUCCESS;
  if (0 == read_size_) {
    ret = common::OB_ITER_END;
  } else {
    // squash buf
    const int64_t data_size = buf_size_ - buf_pos_;
    if (data_size > 0) {
      MEMMOVE(buf_, buf_ + buf_pos_, data_size);
    }
    buf_pos_ = 0;
    buf_size_ = data_size;
    // read buffer
    const int64_t read_size = MIN(buf_capacity_ - buf_size_, read_size_);
    if (OB_FAIL(file_io_handle_.aio_pread(buf_ + buf_size_, read_size, offset_))) {
      STORAGE_LOG(WARN, "fail to do aio read from tmp file", KR(ret));
    } else if (OB_FAIL(file_io_handle_.wait(io_timeout_ms_))) {
      STORAGE_LOG(WARN, "fail to wait io finish", KR(ret), K(io_timeout_ms_));
    } else {
      buf_size_ += read_size;
      offset_ += read_size;
      read_size_ -= read_size;
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockReader<Header, T>::switch_next_block()
{
  int ret = common::OB_SUCCESS;
  int64_t data_size = 0;
  if (buf_size_ - buf_pos_ < data_block_size_ && OB_FAIL(read_next_buffer())) {
    if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "fail to read next buffer", KR(ret));
    }
  } else if (OB_FAIL(data_block_reader_.prepare_data_block(buf_ + buf_pos_, buf_size_ - buf_pos_,
                                                           data_size))) {
    if (OB_UNLIKELY(common::OB_BUF_NOT_ENOUGH != ret)) {
      STORAGE_LOG(WARN, "fail to prepare data block", KR(ret), K(buf_pos_), K(buf_size_));
    } else {
      if (OB_FAIL(read_next_buffer())) {
        STORAGE_LOG(WARN, "fail to read next buffer", KR(ret));
      } else if (OB_FAIL(data_block_reader_.prepare_data_block(buf_ + buf_pos_,
                                                               buf_size_ - buf_pos_, data_size))) {
        STORAGE_LOG(WARN, "fail to prepare data block", KR(ret), K(buf_pos_), K(buf_size_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t data_block_size = ALIGN_UP(data_size, DIO_ALIGN_SIZE);
    buf_pos_ += MAX(data_block_size_, data_block_size);
    ++block_count_;
    if (OB_FAIL(prepare_read_block())) {
      STORAGE_LOG(WARN, "fail to prepare read block", KR(ret));
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockReader<Header, T>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  item = nullptr;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockReader not init", KR(ret), KP(this));
  } else {
    curr_item_.reuse();
    if (OB_FAIL(data_block_reader_.read_next_item(curr_item_))) {
      if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "fail to read item", KR(ret));
      } else {
        if (OB_FAIL(switch_next_block())) {
          if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
            STORAGE_LOG(WARN, "fail to switch next block", KR(ret));
          }
        } else if (OB_FAIL(data_block_reader_.read_next_item(curr_item_))) {
          STORAGE_LOG(WARN, "fail to read item", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      item = &curr_item_;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
