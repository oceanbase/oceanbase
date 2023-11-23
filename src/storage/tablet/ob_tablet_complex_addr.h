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

#ifndef OCEANBASE_STORAGE_OB_TABLET_COMPLEX_ADDR
#define OCEANBASE_STORAGE_OB_TABLET_COMPLEX_ADDR

#include <stdint.h>
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"

namespace oceanbase
{
namespace storage
{
template <typename T>
class ObTabletComplexAddr final
{
public:
  ObTabletComplexAddr();
  explicit ObTabletComplexAddr(T *ptr);
  ~ObTabletComplexAddr();
public:
  bool is_valid() const;
  bool is_memory_object() const;
  bool is_disk_object() const;
  bool is_none_object() const;
  void reset();
  T* get_ptr() const { return ptr_; }
public:
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  int64_t to_string(char *buf, const int64_t buf_len) const;
public:
  T *ptr_;
  ObMetaDiskAddr addr_;
};

template <typename T>
ObTabletComplexAddr<T>::ObTabletComplexAddr()
  : ptr_(nullptr),
    addr_()
{
}

template <typename T>
ObTabletComplexAddr<T>::ObTabletComplexAddr(T *ptr)
  : ptr_(ptr),
    addr_()
{
}

template <typename T>
ObTabletComplexAddr<T>::~ObTabletComplexAddr()
{
  reset();
}

template <typename T>
bool ObTabletComplexAddr<T>::is_valid() const
{
  return is_memory_object() || is_disk_object() || is_none_object();
}

template <typename T>
bool ObTabletComplexAddr<T>::is_memory_object() const
{
  return nullptr != ptr_;
}

template <typename T>
bool ObTabletComplexAddr<T>::is_disk_object() const
{
  return nullptr == ptr_ && addr_.is_block();
}

template <typename T>
bool ObTabletComplexAddr<T>::is_none_object() const
{
  return nullptr == ptr_ && addr_.is_none();
}

template <typename T>
void ObTabletComplexAddr<T>::reset()
{
  if (nullptr != ptr_) {
    ptr_->~T();
    ptr_ = nullptr;
  }
  addr_.reset();
}

template <typename T>
int ObTabletComplexAddr<T>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = common::OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      addr_);
  return ret;
}

template <typename T>
int ObTabletComplexAddr<T>::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      addr_);
  return ret;
}

template <typename T>
int64_t ObTabletComplexAddr<T>::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      addr_);
  return len;
}

template <typename T>
int64_t ObTabletComplexAddr<T>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(KP_(ptr), K_(addr));
    J_OBJ_END();
  }

  return pos;
}

template <>
int64_t ObTabletComplexAddr<mds::MdsDumpKV>::to_string(char *buf, const int64_t buf_len) const;

template <>
int64_t ObTabletComplexAddr<share::ObTabletAutoincSeq>::to_string(char *buf, const int64_t buf_len) const;

template <>
int64_t ObTabletComplexAddr<ObTabletDumpedMediumInfo>::to_string(char *buf, const int64_t buf_len) const;
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_COMPLEX_ADDR
