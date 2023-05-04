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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_MICRO_BLOCK_READER_HELPER_H_
#define OCEANBASE_BLOCKSSTABLE_OB_MICRO_BLOCK_READER_HELPER_H_

#include "encoding/ob_micro_block_decoder.h"
#include "ob_micro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{
class ObMicroBlockReaderHelper final
{
public:
  ObMicroBlockReaderHelper()
    : allocator_(nullptr), flat_reader_(nullptr), decoder_(nullptr) {}
  ~ObMicroBlockReaderHelper() { reset(); }

  inline int init(ObIAllocator &allocator);
  inline void reset();

  inline bool is_inited() { return nullptr != allocator_; };
  inline int get_reader(const ObRowStoreType store_type, ObIMicroBlockReader *&reader);
private:
  template <typename T>
  int init_reader(T *&cache_reader_ptr, ObIMicroBlockReader *&reader);
private:
  ObIAllocator *allocator_;
  ObMicroBlockReader *flat_reader_;
  ObMicroBlockDecoder *decoder_;
};

int ObMicroBlockReaderHelper::init(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(allocator_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "allocator is not null, might double init", KP(allocator_));
  } else {
    allocator_ = &allocator;
  }
  return ret;
}

void ObMicroBlockReaderHelper::reset()
{
  if (nullptr != allocator_) {
    if (nullptr != flat_reader_) {
      flat_reader_->~ObMicroBlockReader();
      allocator_->free(flat_reader_);
    }
    if (nullptr != decoder_) {
      decoder_->~ObMicroBlockDecoder();
      allocator_->free(decoder_);
    }
    allocator_ = nullptr;
  }
  flat_reader_ = nullptr;
  decoder_ = nullptr;
}

int ObMicroBlockReaderHelper::get_reader(
    const ObRowStoreType store_type,
    ObIMicroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  switch (store_type) {
  case FLAT_ROW_STORE: {
    if (OB_FAIL(init_reader(flat_reader_, reader))) {
      STORAGE_LOG(WARN, "Fail to initialize flat micro block reader", K(ret));
    }
    break;
  }
  case ENCODING_ROW_STORE:
  case SELECTIVE_ENCODING_ROW_STORE: {
    if (OB_FAIL(init_reader(decoder_, reader))) {
      STORAGE_LOG(WARN, "Fail to initialize micro block decoder", K(ret));
    }
    break;
  }
  default: {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Not supported row store type", K(ret), K(store_type));
  }
  }
  return ret;
}

template <typename T>
int ObMicroBlockReaderHelper::init_reader(T *&cache_reader_ptr, ObIMicroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null pointer for allocator", K(ret), KP(allocator_));
  } else if (nullptr != cache_reader_ptr) {
    reader = cache_reader_ptr;
  } else if (OB_ISNULL(cache_reader_ptr = OB_NEWx(T, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to construct a new micro block reader", K(ret));
  } else {
    reader = cache_reader_ptr;
  }
  return ret;
}


} // end namespace blocksstable
} // end namespace oceanbase
 #endif //OCEANBASE_BLOCKSSTABLE_OB_MICRO_BLOCK_READER_HELPER_H_
