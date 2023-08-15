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

template <typename Header>
class ObDirectLoadDataBlockDecoder
{
public:
  ObDirectLoadDataBlockDecoder();
  ~ObDirectLoadDataBlockDecoder();
  void reuse();
  void reset();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type);
  int prepare_data_block(char *buf, int64_t buf_size, int64_t &data_size);
  int set_pos(int64_t pos);
  template <typename T>
  int read_next_item(T &item);
  template <typename T>
  int read_item(int64_t pos, T &item);
  OB_INLINE const Header &get_header() const { return header_; }
  OB_INLINE int64_t get_end_pos() const { return buf_size_; }
  TO_STRING_KV(K_(header), K_(compressor_type), KP_(compressor), KP_(buf), K_(buf_size), K_(pos),
               KP_(decompress_buf), KP_(decompress_buf_size));
protected:
  Header header_;
  common::ObCompressorType compressor_type_;
  common::ObCompressor *compressor_;
  common::ObArenaAllocator allocator_;
  char *buf_;
  int64_t buf_size_;
  int64_t pos_;
  char *decompress_buf_;
  int64_t decompress_buf_size_; // buf capacity
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadDataBlockDecoder);
};

template <typename Header>
ObDirectLoadDataBlockDecoder<Header>::ObDirectLoadDataBlockDecoder()
  : compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    compressor_(nullptr),
    allocator_("TLD_DBDecoder"),
    buf_(nullptr),
    buf_size_(0),
    pos_(0),
    decompress_buf_(nullptr),
    decompress_buf_size_(0),
    is_inited_(false)
{
}

template <typename Header>
ObDirectLoadDataBlockDecoder<Header>::~ObDirectLoadDataBlockDecoder()
{
  reset();
}

template <typename Header>
void ObDirectLoadDataBlockDecoder<Header>::reuse()
{
  header_.reset();
  buf_ = nullptr;
  buf_size_ = 0;
  pos_ = 0;
}

template <typename Header>
void ObDirectLoadDataBlockDecoder<Header>::reset()
{
  header_.reset();
  compressor_type_ = common::ObCompressorType::INVALID_COMPRESSOR;
  compressor_ = nullptr;
  buf_ = nullptr;
  buf_size_ = 0;
  pos_ = 0;
  decompress_buf_ = nullptr;
  decompress_buf_size_ = 0;
  allocator_.reset();
  is_inited_ = false;
}

template <typename Header>
int ObDirectLoadDataBlockDecoder<Header>::init(int64_t data_block_size,
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
    allocator_.set_tenant_id(MTL_ID());
    if (common::ObCompressorType::NONE_COMPRESSOR != compressor_type) {
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(data_block_size)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc buf", KR(ret), K(data_block_size));
      } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type,
                                                                                 compressor_))) {
        STORAGE_LOG(WARN, "fail to get compressor, ", KR(ret), K(compressor_type));
      } else {
        decompress_buf_ = buf;
        decompress_buf_size_ = data_block_size;
      }
    }
    if (OB_SUCC(ret)) {
      compressor_type_ = compressor_type;
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename Header>
int ObDirectLoadDataBlockDecoder<Header>::prepare_data_block(char *buf, int64_t buf_size,
                                                             int64_t &data_size)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadDataBlockDecoder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), KP(buf), K(buf_size));
  } else if (buf_size <= header_.get_serialize_size()) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected buf size", KR(ret), K(buf_size));
  } else {
    pos_ = 0;
    // deserialize header
    if (OB_FAIL(header_.deserialize(buf, buf_size, pos_))) {
      STORAGE_LOG(WARN, "fail to deserialize header", KR(ret), K(buf_size), K(pos_));
    } else {
      data_size = header_.occupy_size_;
      if (OB_UNLIKELY(data_size > buf_size)) {
        ret = common::OB_BUF_NOT_ENOUGH;
      } else {
        buf_ = buf;
        buf_size_ = header_.occupy_size_;
      }
    }
    // valid checksum
    if (OB_SUCC(ret)) {
      const int64_t checksum = ob_crc64_sse42(0, buf + pos_, header_.occupy_size_ - pos_);
      if (OB_UNLIKELY(checksum != header_.checksum_)) {
        ret = common::OB_CHECKSUM_ERROR;
        STORAGE_LOG(WARN, "fail to valid checksum", KR(ret), K(header_), K(checksum));
      }
    }
    // do decompress
    if (OB_SUCC(ret) && header_.occupy_size_ != header_.data_size_) {
      int64_t decompress_size = 0;
      if (OB_UNLIKELY(common::ObCompressorType::NONE_COMPRESSOR == compressor_type_)) {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected compressor type", KR(ret));
      } else if (OB_FAIL(compressor_->decompress(buf + pos_, header_.occupy_size_ - pos_,
                                                 decompress_buf_ + pos_,
                                                 decompress_buf_size_ - pos_, decompress_size))) {
        STORAGE_LOG(WARN, "fail to decompress", KR(ret));
      } else if (OB_UNLIKELY(decompress_size + pos_ != header_.data_size_)) {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected decompress size", KR(ret), K(header_), K(decompress_size));
      } else {
        buf_ = decompress_buf_;
        buf_size_ = decompress_size + pos_;
      }
    }
  }
  return ret;
}

template <typename Header>
int ObDirectLoadDataBlockDecoder<Header>::set_pos(int64_t pos)
{
  int ret = common::OB_SUCCESS;
  const int64_t header_size = header_.get_serialize_size();
  if (OB_UNLIKELY(pos < header_size || pos > buf_size_)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), K(pos), K(header_size), K(buf_size_));
  } else {
    pos_ = pos;
  }
  return ret;
}

template <typename Header>
template <typename T>
int ObDirectLoadDataBlockDecoder<Header>::read_next_item(T &item)
{
  int ret = common::OB_SUCCESS;
  if (pos_ >= buf_size_) {
    ret = common::OB_ITER_END;
  } else if (OB_FAIL(item.deserialize(buf_, buf_size_, pos_))) {
    STORAGE_LOG(WARN, "fail to deserialize item", KR(ret), K(buf_size_), K(pos_));
  }
  return ret;
}

template <typename Header>
template <typename T>
int ObDirectLoadDataBlockDecoder<Header>::read_item(int64_t pos, T &item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(pos >= buf_size_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected read pos", KR(ret));
  } else if (OB_FAIL(item.deserialize(buf_, buf_size_, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize item", KR(ret), K(buf_size_), K(pos));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
