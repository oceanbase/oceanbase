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

#include "lib/allocator/page_arena.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

template <typename Header, bool align = false>
class ObDirectLoadDataBlockEncoder
{
  static const int64_t APPLY_COMPRESSION_THRESHOLD = 90; // compression ratio to apply compression
public:
  ObDirectLoadDataBlockEncoder();
  ~ObDirectLoadDataBlockEncoder();
  void reuse();
  void reset();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type);
  template <typename T>
  int write_item(const T &item);
  bool has_item() const { return pos_ > header_size_; }
  int64_t get_pos() const { return pos_; }
  Header &get_header() { return header_; }
  int build_data_block(char *&buf, int64_t &buf_size);
  TO_STRING_KV(K_(header), K_(header_size), K_(compressor_type), KP_(compressor),
               K_(data_block_size), KP_(buf), K_(buf_size), K_(pos), KP_(compress_buf),
               K_(compress_buf_size));
protected:
  int realloc_bufs(const int64_t size);
protected:
  Header header_;
  int64_t header_size_;
  common::ObCompressorType compressor_type_;
  common::ObCompressor *compressor_;
  int64_t data_block_size_;
  char *buf_;
  int64_t buf_size_; // buf capacity
  int64_t pos_;
  char *compress_buf_;
  int64_t compress_buf_size_; // buf capacity
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadDataBlockEncoder);
};

template <typename Header, bool align>
ObDirectLoadDataBlockEncoder<Header, align>::ObDirectLoadDataBlockEncoder()
  : header_size_(0),
    compressor_type_(common::ObCompressorType::INVALID_COMPRESSOR),
    compressor_(nullptr),
    data_block_size_(0),
    buf_(nullptr),
    buf_size_(0),
    pos_(0),
    compress_buf_(nullptr),
    compress_buf_size_(0),
    is_inited_(false)
{
}

template <typename Header, bool align>
ObDirectLoadDataBlockEncoder<Header, align>::~ObDirectLoadDataBlockEncoder()
{
  reset();
}

template <typename Header, bool align>
void ObDirectLoadDataBlockEncoder<Header, align>::reuse()
{
  header_.reset();
  pos_ = header_size_;
}

template <typename Header, bool align>
void ObDirectLoadDataBlockEncoder<Header, align>::reset()
{
  header_.reset();
  header_size_ = 0;
  compressor_type_ = common::ObCompressorType::INVALID_COMPRESSOR;
  compressor_ = nullptr;
  data_block_size_ = 0;
  if (buf_ != nullptr) {
    ob_free(buf_);
    buf_ = nullptr;
  }
  buf_size_ = 0;
  pos_ = 0;
  if (compress_buf_ != nullptr) {
    ob_free(compress_buf_);
    compress_buf_ = nullptr;
  }
  compress_buf_size_ = 0;
  is_inited_ = false;
}

template <typename Header, bool align>
int ObDirectLoadDataBlockEncoder<Header, align>::realloc_bufs(const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = align ? ALIGN_UP(size, DIO_ALIGN_SIZE) : size;
  if (buf_size_ != buf_size) {
    char *tmp_buf = (char *)ob_malloc(buf_size, ObMemAttr(MTL_ID(), "TLD_DBEncoder"));
    if (tmp_buf == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc buf", K(buf_size), KR(ret));
    }
    if (OB_SUCC(ret) && buf_ != nullptr && pos_ > 0) {
      if (pos_ > buf_size) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "pos is bigger than buf buf size", K(pos_), K(buf_size), KR(ret));
      } else {
        MEMCPY(tmp_buf, buf_, pos_);
      }
    }
    if (OB_SUCC(ret)) {
      if (buf_ != nullptr) {
        ob_free(buf_);
        buf_ = nullptr;
      }
      buf_ = tmp_buf;
      buf_size_ = buf_size;
      // pos_不变
    }
  }

  if (compressor_ != nullptr) {
    int64_t max_overflow_size = 0;
    int64_t compress_buf_size = 0;
    if (OB_FAIL(compressor_->get_max_overflow_size(size, max_overflow_size))) {
      STORAGE_LOG(WARN, "fail to get max_overflow_size", KR(ret), K(size), K(max_overflow_size));
    } else {
      const int64_t compress_size = size + max_overflow_size;
      compress_buf_size = align ? ALIGN_UP(compress_size, DIO_ALIGN_SIZE) : compress_size;
    }
    if (OB_SUCC(ret) && compress_buf_size_ != compress_buf_size) {
      if (compress_buf_ != nullptr) {
        ob_free(compress_buf_);
        compress_buf_ = nullptr;
      }
      compress_buf_ = (char *)ob_malloc(compress_buf_size, ObMemAttr(MTL_ID(), "TLD_DBEncoder"));
      if (compress_buf_ == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc compress buf", K(compress_buf_size), KR(ret));
      } else {
        compress_buf_size_ = compress_buf_size;
      }
    }
  }
  return ret;
}

template <typename Header, bool align>
int ObDirectLoadDataBlockEncoder<Header, align>::init(int64_t data_block_size,
                                                      common::ObCompressorType compressor_type)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockEncoder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(data_block_size <= 0 || data_block_size % DIO_ALIGN_SIZE != 0 ||
                         compressor_type <= common::ObCompressorType::INVALID_COMPRESSOR)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), K(data_block_size), K(compressor_type));
  } else {
    if (common::ObCompressorType::NONE_COMPRESSOR != compressor_type &&
        OB_FAIL(
          common::ObCompressorPool::get_instance().get_compressor(compressor_type, compressor_))) {
      STORAGE_LOG(WARN, "fail to get compressor", KR(ret), K(compressor_type));
    } else if (OB_FAIL(realloc_bufs(data_block_size))) {
      STORAGE_LOG(WARN, "fail to alloc bufs", KR(ret), K(data_block_size));
    } else {
      header_size_ = header_.get_serialize_size();
      compressor_type_ = compressor_type;
      data_block_size_ = data_block_size;
      pos_ = header_size_;
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename Header, bool align>
template <typename T>
int ObDirectLoadDataBlockEncoder<Header, align>::write_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  const int64_t item_size = item.get_serialize_size();

  // 内存太大恢复到默认数据块大小
  if (item_size + pos_ < data_block_size_) {
    if (OB_FAIL(realloc_bufs(data_block_size_))) {
      STORAGE_LOG(WARN, "fail to realloc bufs", KR(ret));
    }
  }

  // 单行数据超过默认数据块大小, 且buf未扩容, 重新分配buf
  if (OB_SUCC(ret)) {
    if (item_size > data_block_size_ - header_size_ && item_size > buf_size_ - header_size_) {
      if (OB_FAIL(realloc_bufs(item_size + header_size_))) {
        STORAGE_LOG(WARN, "fail to realloc bufs", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (item_size + pos_ > buf_size_) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(item.serialize(buf_, buf_size_, pos_))) {
      STORAGE_LOG(WARN, "fail to serialize item", KR(ret));
    }
  }
  return ret;
}

template <typename Header, bool align>
int ObDirectLoadDataBlockEncoder<Header, align>::build_data_block(char *&buf, int64_t &buf_size)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockEncoder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!has_item())) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected build empty data block", KR(ret));
  } else {
    buf = buf_;
    buf_size = pos_;
    // do compression
    if (common::ObCompressorType::NONE_COMPRESSOR != compressor_type_) {
      const int64_t data_size = pos_ - header_size_;
      int64_t compress_size = 0;
      if (OB_FAIL(compressor_->compress(buf_ + header_size_, data_size,
                                        compress_buf_ + header_size_,
                                        compress_buf_size_ - header_size_, compress_size))) {
        ret = common::OB_SUCCESS; // give up compression
      } else if (compress_size * 100 < data_size * APPLY_COMPRESSION_THRESHOLD) {
        // apply compression
        buf = compress_buf_;
        buf_size = compress_size + header_size_;
      }
    }
    // serialize header
    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      header_.data_size_ = pos_;
      header_.occupy_size_ = buf_size;
      header_.checksum_ =
        ob_crc64_sse42(0, buf + header_size_, header_.occupy_size_ - header_size_);
      if (OB_FAIL(header_.serialize(buf, header_size_, pos))) {
        STORAGE_LOG(WARN, "fail to serialize header", KR(ret));
      } else if (header_size_ != pos) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "header_size must be equal pos", KR(ret), K(header_size_), K(pos));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
