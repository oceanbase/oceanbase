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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MEMBER_WRAPPER
#define OCEANBASE_STORAGE_OB_TABLET_MEMBER_WRAPPER

#include <type_traits>
#include "lib/oblog/ob_log.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "share/ob_tablet_autoincrement_param.h"

namespace oceanbase
{
namespace storage
{

// only allow such classes to specialize template class ObTabletMemberWrapper:
// ObTabletTableStore
// ObTabletAutoincSeq
// forbidden classes: ObStorageSchema, ObMediumCompactionInfoList
template <typename T,
          typename U = typename std::enable_if<std::is_same<T, ObTabletTableStore>::value || std::is_same<T, share::ObTabletAutoincSeq>::value, T>::type>
class ObTabletMemberWrapper final
{
public:
  friend class ObTablet;
  friend class ObTabletMdsData;
public:
  ObTabletMemberWrapper();
  ~ObTabletMemberWrapper() = default;
public:
  void reset();
  bool is_valid() const;
  int get_member(const T *&t) const;
  const T *get_member() const;
  const ObStorageMetaHandle &get_meta_handle() const { return secondary_meta_handle_; }
  TO_STRING_KV(KP_(ptr), KPC_(ptr), K_(secondary_meta_handle));
private:
  void set_member(T *t);
  int set_cache_handle(ObStorageMetaHandle &handle);
  int set_cache_handle(const ObStorageMetaValue &value);
private:
  const T *ptr_;
  ObStorageMetaHandle secondary_meta_handle_;
};

template <typename T, typename U>
ObTabletMemberWrapper<T, U>::ObTabletMemberWrapper()
  : ptr_(nullptr),
    secondary_meta_handle_()
{
}

template <typename T, typename U>
void ObTabletMemberWrapper<T, U>::reset()
{
  ptr_ = nullptr;
  secondary_meta_handle_.reset();
};

template <typename T, typename U>
bool ObTabletMemberWrapper<T, U>::is_valid() const
{
  return nullptr != ptr_ || secondary_meta_handle_.is_valid();
};

template <typename T, typename U>
void ObTabletMemberWrapper<T, U>::set_member(T *t)
{
  ptr_ = t;
};

template <typename T, typename U>
int ObTabletMemberWrapper<T, U>::set_cache_handle(ObStorageMetaHandle &handle)
{
  int ret = common::OB_SUCCESS;
  const ObStorageMetaValue *value = nullptr;
  if (OB_UNLIKELY(!handle.is_valid())) {
    STORAGE_LOG(WARN, "secondary meta handle is not valid", K(ret), K(handle));
  } else if (OB_FAIL(handle.get_value(value))) {
    STORAGE_LOG(WARN, "secondary meta handle get value failed", K(ret), K(handle));
  } else if (OB_ISNULL(value)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null secondary meta value", K(ret), K(handle));
  } else if (OB_FAIL(set_cache_handle(*value))) {
    STORAGE_LOG(WARN, "failed to set cache handle", K(ret));
  } else {
    secondary_meta_handle_ = handle;
  }
  return ret;
};

template <>
inline int ObTabletMemberWrapper<ObTabletTableStore>::set_cache_handle(const ObStorageMetaValue &value)
{
  int ret = OB_SUCCESS;
  const ObTabletTableStore *table_store = nullptr;
  if (OB_FAIL(value.get_table_store(table_store))) {
    STORAGE_LOG(WARN, "get table store failed", K(ret));
  } else {
    ptr_ = table_store;
  }
  return ret;
}

template <>
inline int ObTabletMemberWrapper<share::ObTabletAutoincSeq>::set_cache_handle(const ObStorageMetaValue &value)
{
  int ret = OB_SUCCESS;
  const share::ObTabletAutoincSeq *autoinc_seq = nullptr;
  if (OB_FAIL(value.get_autoinc_seq(autoinc_seq))) {
    STORAGE_LOG(WARN, "get auto inc seq failed", K(ret));
  } else {
    ptr_ = autoinc_seq;
  }
  return ret;
}

template <typename T, typename U>
int ObTabletMemberWrapper<T, U>::get_member(const T *&t) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ERROR;
    STORAGE_LOG(WARN, "tablet member wrapper is not valid");
  } else if (OB_ISNULL(ptr_)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "tablet member wrapper pointer is nullptr");
  } else {
    t = ptr_;
  }
  return ret;
}

template <typename T, typename U>
const T *ObTabletMemberWrapper<T, U>::get_member() const
{
  OB_ASSERT(is_valid());
  OB_ASSERT(nullptr != ptr_);
  return ptr_;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MEMBER_WRAPPER
