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
#include "storage/direct_load/ob_direct_load_data_block_encoder.h"
#include "storage/direct_load/ob_direct_load_external_interface.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"

namespace oceanbase
{
namespace storage
{

class ObIDirectLoadDataBlockFlushCallback
{
public:
  virtual ~ObIDirectLoadDataBlockFlushCallback() = default;
  virtual int write(char *buf, int64_t buf_size, int64_t offset) = 0;
};

template <typename Header, typename T>
class ObDirectLoadDataBlockWriter : public ObDirectLoadExternalWriter<T>
{
public:
  ObDirectLoadDataBlockWriter();
  virtual ~ObDirectLoadDataBlockWriter();
  void reuse();
  void reset();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type, char *extra_buf,
           int64_t extra_buf_size, ObIDirectLoadDataBlockFlushCallback *callback);
  int open(const ObDirectLoadTmpFileHandle &file_handle) override;
  int write_item(const T &item) override;
  int close() override;
  OB_INLINE int64_t get_file_size() const { return offset_; }
  OB_INLINE int64_t get_block_count() const { return block_count_; }
  OB_INLINE int64_t get_max_block_size() const { return max_block_size_; }
protected:
  virtual int pre_write_item() { return common::OB_SUCCESS; }
  virtual int pre_flush_buffer() { return common::OB_SUCCESS; }
  int flush_buffer();
  int flush_extra_buffer(const T &item);
protected:
  int64_t data_block_size_;
  char *extra_buf_;
  int64_t extra_buf_size_;
  ObDirectLoadDataBlockEncoder<Header> data_block_writer_;
  int64_t io_timeout_ms_;
  ObDirectLoadTmpFileIOHandle file_io_handle_;
  int64_t offset_;
  int64_t block_count_;
  int64_t max_block_size_;
  ObIDirectLoadDataBlockFlushCallback *callback_;
  bool is_opened_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadDataBlockWriter);
};

template <typename Header, typename T>
ObDirectLoadDataBlockWriter<Header, T>::ObDirectLoadDataBlockWriter()
  : data_block_size_(0),
    extra_buf_(nullptr),
    extra_buf_size_(0),
    io_timeout_ms_(0),
    offset_(0),
    block_count_(0),
    max_block_size_(0),
    is_opened_(false),
    is_inited_(false)
{
}

template <typename Header, typename T>
ObDirectLoadDataBlockWriter<Header, T>::~ObDirectLoadDataBlockWriter()
{
  reset();
}

template <typename Header, typename T>
void ObDirectLoadDataBlockWriter<Header, T>::reuse()
{
  data_block_writer_.reuse();
  file_io_handle_.reset();
  offset_ = 0;
  block_count_ = 0;
  max_block_size_ = 0;
  is_opened_ = false;
}

template <typename Header, typename T>
void ObDirectLoadDataBlockWriter<Header, T>::reset()
{
  data_block_size_ = 0;
  extra_buf_ = nullptr;
  extra_buf_size_ = 0;
  data_block_writer_.reset();
  io_timeout_ms_ = 0;
  file_io_handle_.reset();
  offset_ = 0;
  block_count_ = 0;
  max_block_size_ = 0;
  callback_ = nullptr;
  is_opened_ = false;
  is_inited_ = false;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockWriter<Header, T>::init(int64_t data_block_size,
                                                 common::ObCompressorType compressor_type,
                                                 char *extra_buf, int64_t extra_buf_size,
                                                 ObIDirectLoadDataBlockFlushCallback *callback)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(data_block_size <= 0 || data_block_size % DIO_ALIGN_SIZE != 0 ||
                         compressor_type <= common::ObCompressorType::INVALID_COMPRESSOR)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), K(data_block_size), K(compressor_type));
  } else {
    if (OB_FAIL(data_block_writer_.init(data_block_size, compressor_type))) {
      STORAGE_LOG(WARN, "fail to init data block writer", KR(ret));
    } else {
      data_block_size_ = data_block_size;
      extra_buf_ = extra_buf;
      extra_buf_size_ = extra_buf_size;
      io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
      callback_ = callback;
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockWriter<Header, T>::open(const ObDirectLoadTmpFileHandle &file_handle)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_opened_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "external block writer already opened", KR(ret));
  } else if (OB_UNLIKELY(!file_handle.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(file_handle));
  } else {
    reuse();
    if (OB_FAIL(file_io_handle_.open(file_handle))) {
      STORAGE_LOG(WARN, "fail to assign file handle", KR(ret));
    } else {
      is_opened_ = true;
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockWriter<Header, T>::write_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "external block writer not open", KR(ret));
  } else {
    if (OB_FAIL(pre_write_item())) {
      STORAGE_LOG(WARN, "fail to pre write item", KR(ret));
    } else if (OB_FAIL(data_block_writer_.write_item(item))) {
      if (OB_LIKELY(common::OB_BUF_NOT_ENOUGH == ret)) {
        if (OB_FAIL(flush_buffer())) {
          STORAGE_LOG(WARN, "fail to flush buffer", KR(ret));
        } else if (OB_FAIL(pre_write_item())) {
          STORAGE_LOG(WARN, "fail to pre write item", KR(ret));
        } else if (OB_FAIL(data_block_writer_.write_item(item))) {
          STORAGE_LOG(WARN, "fail to write item", KR(ret));
        }
      } else if (common::OB_SIZE_OVERFLOW == ret && nullptr != extra_buf_) {
        if (data_block_writer_.has_item() && OB_FAIL(flush_buffer())) {
          STORAGE_LOG(WARN, "fail to flush buffer", KR(ret));
        } else if (OB_FAIL(pre_write_item())) {
          STORAGE_LOG(WARN, "fail to pre write item", KR(ret));
        } else if (OB_FAIL(flush_extra_buffer(item))) {
          STORAGE_LOG(WARN, "fail to flush extra buffer", KR(ret));
        }
      } else {
        STORAGE_LOG(WARN, "fail to write item", KR(ret));
      }
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockWriter<Header, T>::flush_buffer()
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, external_flush_buffer_time_us);
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(pre_flush_buffer())) {
    STORAGE_LOG(WARN, "fail to pre flush buffer", KR(ret));
  } else {
    char *buf = nullptr;
    int64_t buf_size = 0;
    if (OB_FAIL(data_block_writer_.build_data_block(buf, buf_size))) {
      STORAGE_LOG(WARN, "fail to build data block", KR(ret));
    } else if (OB_FAIL(file_io_handle_.aio_write(buf, data_block_size_))) {
      STORAGE_LOG(WARN, "fail to do aio write tmp file", KR(ret));
    } else if (nullptr != callback_ && OB_FAIL(callback_->write(buf, data_block_size_, offset_))) {
      STORAGE_LOG(WARN, "fail to callback write", KR(ret));
    } else {
      OB_TABLE_LOAD_STATISTICS_INC(external_write_bytes, data_block_size_);
      data_block_writer_.reuse();
      offset_ += data_block_size_;
      ++block_count_;
      max_block_size_ = MAX(max_block_size_, data_block_size_);
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockWriter<Header, T>::flush_extra_buffer(const T &item)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, external_flush_buffer_time_us);
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(pre_flush_buffer())) {
    STORAGE_LOG(WARN, "fail to pre flush buffer", KR(ret));
  } else {
    int64_t data_size = 0;
    int64_t data_block_size = 0;
    if (OB_FAIL(
          data_block_writer_.build_data_block(item, extra_buf_, extra_buf_size_, data_size))) {
      STORAGE_LOG(WARN, "fail to build data block", KR(ret));
    } else if (FALSE_IT(data_block_size = ALIGN_UP(data_size, DIO_ALIGN_SIZE))) {
    } else if (OB_FAIL(file_io_handle_.aio_write(extra_buf_, data_block_size))) {
      STORAGE_LOG(WARN, "fail to do aio write tmp file", KR(ret));
    } else if (nullptr != callback_ &&
               OB_FAIL(callback_->write(extra_buf_, data_block_size, offset_))) {
      STORAGE_LOG(WARN, "fail to callback write", KR(ret));
    } else {
      OB_TABLE_LOAD_STATISTICS_INC(external_write_bytes, data_block_size);
      data_block_writer_.reuse();
      offset_ += data_block_size;
      ++block_count_;
      max_block_size_ = MAX(max_block_size_, data_block_size);
    }
  }
  return ret;
}

template <typename Header, typename T>
int ObDirectLoadDataBlockWriter<Header, T>::close()
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "external block writer not open", KR(ret));
  } else {
    if (data_block_writer_.has_item() && OB_FAIL(flush_buffer())) {
      STORAGE_LOG(WARN, "fail to flush buffer", KR(ret));
    } else if (OB_FAIL(file_io_handle_.wait(io_timeout_ms_))) {
      STORAGE_LOG(WARN, "fail to wait io finish", KR(ret), K(io_timeout_ms_));
    } else {
      is_opened_ = false;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
