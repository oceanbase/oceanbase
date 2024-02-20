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

#include "storage/direct_load/ob_direct_load_external_block_reader.h"
#include "storage/direct_load/ob_direct_load_external_fragment.h"
#include "storage/direct_load/ob_direct_load_external_merger.h"

namespace oceanbase
{
namespace storage
{

/**
 * external sequential scanner
 */

template <typename T>
class ObDirectLoadExternalSequentialScanner : public ObDirectLoadExternalIterator<T>
{
  typedef ObDirectLoadExternalBlockReader<T> ExternalReader;
public:
  ObDirectLoadExternalSequentialScanner();
  virtual ~ObDirectLoadExternalSequentialScanner();
  void reuse();
  void reset();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type,
           const ObDirectLoadExternalFragmentArray &fragments);
  int get_next_item(const T *&item) override;
private:
  int switch_next_fragment();
private:
  ObDirectLoadExternalFragmentArray fragments_;
  ExternalReader reader_;
  int64_t next_pos_;
  bool is_inited_;
};

template <typename T>
ObDirectLoadExternalSequentialScanner<T>::ObDirectLoadExternalSequentialScanner()
  : next_pos_(0), is_inited_(false)
{
}

template <typename T>
ObDirectLoadExternalSequentialScanner<T>::~ObDirectLoadExternalSequentialScanner()
{
  reset();
}

template <typename T>
void ObDirectLoadExternalSequentialScanner<T>::reuse()
{
  reset();
}

template <typename T>
void ObDirectLoadExternalSequentialScanner<T>::reset()
{
  next_pos_ = 0;
  reader_.reset();
  fragments_.reset();
  is_inited_ = false;
}

template <typename T>
int ObDirectLoadExternalSequentialScanner<T>::init(
  int64_t data_block_size, common::ObCompressorType compressor_type,
  const ObDirectLoadExternalFragmentArray &fragments)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadExternalSequentialScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(data_block_size <= 0 || data_block_size % DIO_ALIGN_SIZE != 0 ||
                         compressor_type <= common::ObCompressorType::INVALID_COMPRESSOR ||
                         fragments.empty())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(compressor_type), K(fragments));
  } else {
    if (OB_FAIL(fragments_.assign(fragments))) {
      STORAGE_LOG(WARN, "fail to assign fragments", KR(ret));
    } else if (OB_FAIL(reader_.init(data_block_size, compressor_type))) {
      STORAGE_LOG(WARN, "fail to init fragment reader", KR(ret));
    } else if (OB_FAIL(switch_next_fragment())) {
      STORAGE_LOG(WARN, "fail to switch next fragment", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T>
int ObDirectLoadExternalSequentialScanner<T>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadExternalSequentialScanner not init", KR(ret), KP(this));
  } else {
    item = nullptr;
    while (OB_SUCC(ret) && nullptr == item) {
      if (OB_FAIL(reader_.get_next_item(item))) {
        if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "fail to get next item", KR(ret));
        } else {
          if (OB_FAIL(switch_next_fragment())) {
            if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
              STORAGE_LOG(WARN, "fail to switch next fragment", KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

template <typename T>
int ObDirectLoadExternalSequentialScanner<T>::switch_next_fragment()
{
  int ret = common::OB_SUCCESS;
  if (next_pos_ >= fragments_.count()) {
    ret = common::OB_ITER_END;
  } else {
    reader_.reuse();
    const ObDirectLoadExternalFragment &fragment = fragments_.at(next_pos_);
    if (OB_FAIL(reader_.open(fragment.file_handle_, 0, fragment.file_size_))) {
      STORAGE_LOG(WARN, "fail to open file", KR(ret));
    } else {
      next_pos_++;
    }
  }
  return ret;
}

/**
 * external sort scanner
 */

template <typename T, typename Compare>
class ObDirectLoadExternalSortScanner : public ObDirectLoadExternalIterator<T>
{
  typedef ObDirectLoadExternalIterator<T> ExternalIterator;
  typedef ObDirectLoadExternalBlockReader<T> ExternalReader;
public:
  ObDirectLoadExternalSortScanner();
  virtual ~ObDirectLoadExternalSortScanner();
  int init(int64_t data_block_size, common::ObCompressorType compressor_type,
           const ObDirectLoadExternalFragmentArray &fragments, Compare *compare);
  int get_next_item(const T *&item) override;
  void reuse();
private:
  common::ObArenaAllocator allocator_;
  common::ObArray<ExternalIterator *> iters_;
  ObDirectLoadExternalMerger<T, Compare> merger_;
  bool is_inited_;
};

template <typename T, typename Compare>
ObDirectLoadExternalSortScanner<T, Compare>::ObDirectLoadExternalSortScanner()
  : allocator_("TLD_ESScanner"), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  iters_.set_tenant_id(MTL_ID());
}

template <typename T, typename Compare>
ObDirectLoadExternalSortScanner<T, Compare>::~ObDirectLoadExternalSortScanner()
{
  reuse();
}

template <typename T, typename Compare>
void ObDirectLoadExternalSortScanner<T, Compare>::reuse()
{
  is_inited_ = false;
  merger_.reset();
  for (int64_t i = 0; i < iters_.count(); ++i) {
    ExternalIterator *iter = iters_[i];
    iter->~ExternalIterator();
    allocator_.free(iter);
  }
  iters_.reset();
  allocator_.reset();
}

template <typename T, typename Compare>
int ObDirectLoadExternalSortScanner<T, Compare>::init(
  int64_t data_block_size, common::ObCompressorType compressor_type,
  const ObDirectLoadExternalFragmentArray &fragments, Compare *compare)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadExternalSortScanner init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(data_block_size <= 0 || data_block_size % DIO_ALIGN_SIZE != 0 ||
                         compressor_type <= common::ObCompressorType::INVALID_COMPRESSOR ||
                         fragments.empty() || nullptr == compare)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(compressor_type), K(fragments),
                KP(compare));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fragments.count(); ++i) {
      const ObDirectLoadExternalFragment &fragment = fragments.at(i);
      ExternalReader *reader = nullptr;
      if (OB_ISNULL(reader = OB_NEWx(ExternalReader, (&allocator_)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to new fragment reader", KR(ret));
      } else if (OB_FAIL(reader->init(data_block_size, compressor_type))) {
        STORAGE_LOG(WARN, "fail to init fragment reader", KR(ret));
      } else if (OB_FAIL(reader->open(fragment.file_handle_, 0, fragment.file_size_))) {
        STORAGE_LOG(WARN, "fail to open fragment", KR(ret));
      } else if (OB_FAIL(iters_.push_back(reader))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != reader) {
          reader->~ExternalReader();
          allocator_.free(reader);
          reader = nullptr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(merger_.init(iters_, compare))) {
        STORAGE_LOG(WARN, "fail to init merger", KR(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObDirectLoadExternalSortScanner<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadExternalSortScanner not init", KR(ret), KP(this));
  } else {
    ret = merger_.get_next_item(item);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
