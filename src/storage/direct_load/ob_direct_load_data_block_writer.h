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

// align: 是否对齐写. 当前的索引文件必须对齐写, 数据文件可以不对齐写.
template <typename Header, typename T, bool align = false>
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
protected:
  int64_t data_block_size_;
  char *extra_buf_;
  int64_t extra_buf_size_;
  ObDirectLoadDataBlockEncoder<Header, align> data_block_writer_;
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

template <typename Header, typename T, bool align>
ObDirectLoadDataBlockWriter<Header, T, align>::ObDirectLoadDataBlockWriter()
  : data_block_size_(0),
    extra_buf_(nullptr),
    extra_buf_size_(0),
    io_timeout_ms_(0),
    offset_(0),
    block_count_(0),
    max_block_size_(0),
    callback_(nullptr),
    is_opened_(false),
    is_inited_(false)
{
}

template <typename Header, typename T, bool align>
ObDirectLoadDataBlockWriter<Header, T, align>::~ObDirectLoadDataBlockWriter()
{
  reset();
}

template <typename Header, typename T, bool align>
void ObDirectLoadDataBlockWriter<Header, T, align>::reuse()
{
  data_block_writer_.reuse();
  file_io_handle_.reset();
  offset_ = 0;
  block_count_ = 0;
  max_block_size_ = 0;
  is_opened_ = false;
}

template <typename Header, typename T, bool align>
void ObDirectLoadDataBlockWriter<Header, T, align>::reset()
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

template <typename Header, typename T, bool align>
int ObDirectLoadDataBlockWriter<Header, T, align>::init(
  int64_t data_block_size,
  common::ObCompressorType compressor_type,
  char *extra_buf,
  int64_t extra_buf_size,
  ObIDirectLoadDataBlockFlushCallback *callback)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
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

template <typename Header, typename T, bool align>
int ObDirectLoadDataBlockWriter<Header, T, align>::open(
  const ObDirectLoadTmpFileHandle &file_handle)
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

template <typename Header, typename T, bool align>
int ObDirectLoadDataBlockWriter<Header, T, align>::write_item(const T &item)
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
      } else {
        STORAGE_LOG(WARN, "fail to write item", KR(ret));
      }
    }
  }
  return ret;
}

template <typename Header, typename T, bool align>
int ObDirectLoadDataBlockWriter<Header, T, align>::flush_buffer()
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, external_flush_buffer_time_us);
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(pre_flush_buffer())) {
    STORAGE_LOG(WARN, "fail to pre flush buffer", KR(ret));
  } else {
    char *buf = nullptr;
    int64_t buf_size = 0;
    int64_t occupy_size = 0;
    if (OB_FAIL(data_block_writer_.build_data_block(buf, buf_size))) {
      STORAGE_LOG(WARN, "fail to build data block", KR(ret));
    } else if (FALSE_IT(occupy_size = align ? ALIGN_UP(buf_size, DIO_ALIGN_SIZE) : buf_size)) {
    } else if (OB_FAIL(file_io_handle_.write(buf, occupy_size))) {
      STORAGE_LOG(WARN, "fail to do write tmp file", KR(ret));
    } else if (nullptr != callback_ && OB_FAIL(callback_->write(buf, occupy_size, offset_))) {
      STORAGE_LOG(WARN, "fail to callback write", KR(ret));
    } else {
      OB_TABLE_LOAD_STATISTICS_INC(external_write_bytes, occupy_size);
      data_block_writer_.reuse();
      offset_ += occupy_size;
      ++block_count_;
      max_block_size_ = MAX(max_block_size_, occupy_size);
    }
  }
  return ret;
}

template <typename Header, typename T, bool align>
int ObDirectLoadDataBlockWriter<Header, T, align>::close()
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
    } else if (OB_FAIL(file_io_handle_.wait())) {
      STORAGE_LOG(WARN, "fail to wait io finish", KR(ret));
    } else {
      max_block_size_ = ALIGN_UP(max_block_size_, DIO_ALIGN_SIZE); // 这个值目前没什么用了, 这里是为了过参数检查
      is_opened_ = false;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
