// Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX STORAGE

#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

template <typename T>
int ObMDSInfoArray<T>::init_for_first_creation(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret));
  } else {
    allocator_ = &allocator;
    src_ = ObMDSInfoSrc::SRC_MDS;
    stack_cached_array_idx_ = 0;
    is_inited_ = true;
  }

  return ret;
}

template <typename T>
int ObMDSInfoArray<T>::init_with_kv_cache_array(ObIAllocator &allocator,
                                                const ObArrayWrap<T> &input_array)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret));
  } else if (OB_UNLIKELY(input_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init mds info array", KR(ret), K(input_array));
  } else {
    allocator_ = &allocator;
    src_ = ObMDSInfoSrc::SRC_KV_CACHE;
    stack_cached_array_idx_ = 0;
    is_inited_ = true;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < input_array.count(); idx++) {
      if (OB_FAIL(inner_append_and_deep_copy(input_array.at(idx)))) {
        LOG_WARN("Fail to append and deep copy mds info", KR(ret), K(input_array.at(idx)));
      }
    }

    if (FAILEDx(sort_array())) {
      LOG_WARN("Fail to sort array", KR(ret));
    }

    if (OB_FAIL(ret)) {
      reset();
    }
  }

  return ret;
}

template <typename T>
int ObMDSInfoArray<T>::inner_allocate_and_append(T *&info)
{
  int ret = OB_SUCCESS;

  // private function, don't check is_inited_

  info = nullptr;

  bool using_stack_cache = stack_cached_array_idx_ < STACK_CACHED_ARRAY_SIZE;
  if (using_stack_cache
        ? OB_FALSE_IT(info = &stack_cached_array_[stack_cached_array_idx_++])
        : OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(*allocator_, info))) {
    // if fail in this, the info is allocated failed, don't need to free it
    LOG_WARN("Fail to allocate mds info", KR(ret));
  } else if (OB_FAIL(mds_info_array_.push_back(info))) {
    // if fail in this, we should free the info
    using_stack_cache ? info->destroy() : ObTabletObjLoadHelper::free(*allocator_, info);
    LOG_WARN("Fail to push back to array", KR(ret));
  }

  // now, the info must be in array struct, which will free in reset()

  return ret;
}

template <typename T>
int ObMDSInfoArray<T>::inner_append_and_deep_copy(const T &info)
{
  int ret = OB_SUCCESS;

  T *dst_info = nullptr;
  if (OB_FAIL(inner_allocate_and_append(dst_info))) {
    LOG_WARN("Fail to allocate and push back mds info", KR(ret));
  } else if (OB_FAIL(dst_info->assign(*allocator_, info))) {
    LOG_WARN("Fail to copy mds info", KR(ret), K(info));
  }

  return ret;
}

template <typename T>
int ObMDSInfoArray<T>::append_with_deep_copy(const T &mds_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", KR(ret));
  } else if (OB_FAIL(inner_append_and_deep_copy(mds_info))) {
    LOG_WARN("Fail to copy mds info", KR(ret), K(mds_info));
  } else if (OB_FAIL(sort_array())) {
    LOG_WARN("Fail to sort array", KR(ret));
  }

  return ret;
}

template <typename T>
int ObMDSInfoArray<T>::append_ptr(T &info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", KR(ret));
  } else if (OB_FAIL(mds_info_array_.push_back(&info))) {
    LOG_WARN("Fail to push back to array", KR(ret));
  } else if (OB_FAIL(sort_array())) {
    LOG_WARN("Fail to sort array", KR(ret));
  }

  return ret;
}

bool ObTruncateInfoArray::compare(const ObTruncateInfo *lhs, const ObTruncateInfo *rhs)
{
  bool bret = true;

  if (lhs->commit_version_ == rhs->commit_version_) {
    bret = lhs->key_ < rhs->key_;
  } else {
    bret = lhs->commit_version_ < rhs->commit_version_;
  }

  return bret;
}

template <typename T>
void ObMDSInfoArray<T>::reset()
{
  for (int64_t i = 0; i < mds_info_array_.count(); i++) {
    bool is_stack_cached = false;
    for (int64_t j = 0; j < STACK_CACHED_ARRAY_SIZE; j++) {
      if (mds_info_array_.at(i) == &stack_cached_array_[j]) {
        is_stack_cached = true;
        break;
      }
    }

    if (!is_stack_cached) {
      ObTabletObjLoadHelper::free(*allocator_, mds_info_array_.at(i));
    }
  }

  for (int64_t i = 0; i < STACK_CACHED_ARRAY_SIZE; i++) {
    stack_cached_array_[i].destroy();
  }

  mds_info_array_.reset();
  stack_cached_array_idx_ = 0;
  allocator_ = nullptr;
  is_inited_ = false;
}

template <typename T>
int ObMDSInfoArray<T>::assign(ObIAllocator &allocator, const ObMDSInfoArray &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();

    allocator_ = &allocator;

    for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); i++) {
      const T *src_info = other.mds_info_array_.at(i);
      if (OB_ISNULL(src_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected error, mds info is null", KR(ret), K(i), KP(src_info));
      } else if (OB_FAIL(inner_append_and_deep_copy(*src_info))) {
        LOG_WARN("Fail to assign mds info", KR(ret), K(i), KPC(src_info));
      }
    }

    if (FAILEDx(sort_array())) {
      LOG_WARN("Fail to sort array", KR(ret));
    } else {
      is_inited_ = true;
    }

    if (OB_FAIL(ret)) {
      reset();
    }
  }

  return ret;
}

template <typename T>
int ObMDSInfoArray<T>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  int64_t new_pos = pos;

  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid args", KR(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Mds info array is invalid", KR(ret), KPC(this));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, new_pos, count()))) {
    LOG_WARN("Fail to serialize mds info array count", KR(ret), K(buf_len));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count(); ++idx) {
      T *info = mds_info_array_.at(idx);
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null info", KR(ret), K(idx), K_(mds_info_array));
      } else if (OB_FAIL(info->serialize(buf, buf_len, new_pos))) {
        LOG_WARN("Fail to serialize mds info", KR(ret), K(buf), K(buf_len), K(new_pos), KPC(info));
      } else {
        LOG_DEBUG("Success to serialize mds info", K(ret), KPC(info));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }

  return ret;
}

template <typename T>
int ObMDSInfoArray<T>::deserialize(ObIAllocator &allocator,
                                   const char *buf,
                                   const int64_t data_len,
                                   int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t new_pos = pos;
  int64_t array_cnt = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid args", KR(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, new_pos, &array_cnt))) {
    LOG_WARN("Fail to deserialize array count", KR(ret), K(data_len));
  } else if (OB_UNLIKELY(array_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected array count", KR(ret), K(array_cnt));
  } else if (array_cnt > 0) {
    allocator_ = &allocator;
    for (int64_t i = 0; OB_SUCC(ret) && i < array_cnt; i++) {
      T *new_info = nullptr;
      if (OB_FAIL(inner_allocate_and_append(new_info))) {
        LOG_WARN("Fail to allocate and push back mds info", KR(ret));
      } else if (OB_FAIL(new_info->deserialize(allocator, buf, data_len, new_pos))) {
        LOG_WARN("Fail to deserialize mds info", KR(ret));
      } else {
        LOG_DEBUG("Success to deserialize mds info", KR(ret), K(new_info));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!inner_is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Mds info array is invalid", KR(ret), KPC(this));
  } else if (OB_FAIL(sort_array())) {
    LOG_WARN("Fail to sort array", KR(ret));
  } else {
    is_inited_ = true;
    pos = new_pos;
  }

  if (OB_FAIL(ret)) {
    reset();
  }

  return ret;
}

template <typename T>
int64_t ObMDSInfoArray<T>::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(count());
  for (int64_t idx = 0; idx < count(); ++idx) {
    const T *info = mds_info_array_.at(idx);
    if (OB_NOT_NULL(info)) {
      len += info->get_serialize_size();
    }
  }
  return len;
}

int ObTruncateInfoArray::sort_array()
{
  lib::ob_sort(mds_info_array_.begin(), mds_info_array_.end(), compare);
  return OB_SUCCESS;
}

template class ObMDSInfoArray<ObTruncateInfo>;
template class ObMDSInfoArray<ObTTLFilterInfo>;

} // namespace storage
} // namespace oceanbase
