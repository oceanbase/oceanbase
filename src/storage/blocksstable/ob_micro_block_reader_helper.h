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

#include "cs_encoding/ob_micro_block_cs_decoder.h"
#include "encoding/ob_micro_block_decoder.h"
#include "ob_micro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{
template <typename BaseReaderType,
          typename FlatReaderType,
          typename NewFlatReaderType,
          typename DecoderType,
          typename CSDecoderType>
class ObMicroBlockReaderHelperBase final
{
public:
  ObMicroBlockReaderHelperBase()
      : allocator_(nullptr), flat_reader_(nullptr), new_flat_reader_(nullptr), decoder_(nullptr),
        cs_decoder_(nullptr)
  {
  }

  ~ObMicroBlockReaderHelperBase() { reset(); }

  OB_INLINE int init(ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited()
                    && allocator_ != &allocator)) { // init the same helper instead of reuse
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "Fail to init reader helper", KP(allocator_));
    } else {
      allocator_ = &allocator;
    }
    return ret;
  }

  // don't provide reuse function because call reuse just do nothing
  // but reuse need the same allocator, the check is in init function

  OB_INLINE void reset()
  {
    if (nullptr != allocator_) {
      if (nullptr != flat_reader_) {
        flat_reader_->~FlatReaderType();
        allocator_->free(flat_reader_);
        flat_reader_ = nullptr;
      }
      if (nullptr != new_flat_reader_) {
        new_flat_reader_->~NewFlatReaderType();
        allocator_->free(new_flat_reader_);
        new_flat_reader_ = nullptr;
      }
      if (nullptr != decoder_) {
        decoder_->~DecoderType();
        allocator_->free(decoder_);
        decoder_ = nullptr;
      }
      if (nullptr != cs_decoder_) {
        cs_decoder_->~CSDecoderType();
        allocator_->free(cs_decoder_);
        cs_decoder_ = nullptr;
      }
      allocator_ = nullptr;
    }
  }

  OB_INLINE bool is_inited() { return nullptr != allocator_; };

  OB_INLINE int get_reader(const ObRowStoreType store_type, BaseReaderType *&reader)
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
    case FLAT_OPT_ROW_STORE: {
      if (OB_FAIL(init_reader(new_flat_reader_, reader))) {
        STORAGE_LOG(WARN, "Fail to initialize flat micro block new flat reader", K(ret));
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
    case CS_ENCODING_ROW_STORE: {
      if (OB_FAIL(init_reader(cs_decoder_, reader))) {
        STORAGE_LOG(WARN, "Fail to initialize micro block cs decoder", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "Not supported row store type", K(ret), K(store_type));
      break;
    }
    }
    return ret;
  }

private:
  template <typename T> int init_reader(T *&cache_reader_ptr, BaseReaderType *&reader)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_inited())) {
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

private:
  ObIAllocator *allocator_;
  FlatReaderType *flat_reader_;
  NewFlatReaderType *new_flat_reader_;
  DecoderType *decoder_;
  CSDecoderType *cs_decoder_;
};

using ObMicroBlockReaderHelper
    = ObMicroBlockReaderHelperBase<ObIMicroBlockReader,
                                   ObMicroBlockReader</* enable new flat reader */ false>,
                                   ObMicroBlockReader</* enable new flat reader */ true>,
                                   ObMicroBlockDecoder,
                                   ObMicroBlockCSDecoder>;

using ObMicroBlockGetReaderHelper
    = ObMicroBlockReaderHelperBase<ObIMicroBlockGetReader,
                                   ObMicroBlockGetReader</* enable new flat reader */ false>,
                                   ObMicroBlockGetReader</* enable new flat reader */ true>,
                                   ObEncodeBlockGetReader,
                                   ObCSEncodeBlockGetReader>;

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_MICRO_BLOCK_READER_HELPER_H_
