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
#ifndef OCEANBASE_STORAGE_OB_STORAGE_LEAK_CHECKER_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LEAK_CHECKER_H_

#include "share/cache/ob_kvcache_struct.h"
#include "share/io/ob_io_define.h"
#include "storage/ob_storage_checked_object_base.h"


namespace oceanbase
{
namespace storage
{

inline bool is_cache(ObStorageCheckID id) {
  return (int)id > OB_CACHE_INVALID && (int)id <= (int)ObStorageCheckID::ALL_CACHE;
}

inline bool is_io_handle(ObStorageCheckID id) {
  return id == ObStorageCheckID::IO_HANDLE;
}

inline bool is_storage_iter(ObStorageCheckID id) {
  return id == ObStorageCheckID::STORAGE_ITER;
}

inline bool is_valid_check_id(ObStorageCheckID id) {
  return is_cache(id) || is_io_handle(id) || is_storage_iter(id);
}

struct ObStorageCheckerKey
{
public:
  ObStorageCheckerKey();
  ObStorageCheckerKey(const void *handle);
  ~ObStorageCheckerKey() = default;
  int hash(uint64_t &hash_value) const;
  OB_INLINE bool is_valid() const { return nullptr != handle_; }
  bool operator== (const ObStorageCheckerKey &other) const;
  TO_STRING_KV(KP_(handle));
  const void *handle_;
};


struct ObStorageCheckerValue
{
public:
  ObStorageCheckerValue();
  ObStorageCheckerValue(const ObStorageCheckerValue &other);
  ~ObStorageCheckerValue() = default;
  int hash(uint64_t &hash_value) const;
  bool operator== (const ObStorageCheckerValue &other) const;
  ObStorageCheckerValue & operator= (const ObStorageCheckerValue &other);
  TO_STRING_KV(K_(tenant_id), K_(check_id), K_(bt));
  uint64_t tenant_id_;
  ObStorageCheckID check_id_;
  char bt_[512];
};

class ObStorageLeakChecker final
{
public:
  static const char ALL_CACHE_NAME[MAX_CACHE_NAME_LENGTH];
  static const char IO_HANDLE_CHECKER_NAME[MAX_CACHE_NAME_LENGTH];
  static const char ITER_CHECKER_NAME[MAX_CACHE_NAME_LENGTH];
  static constexpr int MEMORY_LIMIT = 128L << 20;
  static constexpr int MAP_SIZE_LIMIT = MEMORY_LIMIT / sizeof(ObStorageCheckerValue);

  static ObStorageLeakChecker &get_instance() { return instance_; }
  void reset();
  // return if is recorded
  template<typename T>
  bool handle_hold(T* handle, bool errsim_bypass = false);
  template<typename T>
  void handle_reset(T* handle);
  int get_aggregate_bt_info(hash::ObHashMap<ObStorageCheckerValue, int64_t> &bt_info);
private:
  void inner_handle_hold(ObStorageCheckedObjectBase* handle, const ObStorageCheckID type_id);
  void inner_handle_reset(ObStorageCheckedObjectBase* handle, const ObStorageCheckID type_id);
  static const int64_t HANDLE_BT_MAP_BUCKET_NUM = 10000;

  ObStorageLeakChecker();
  ~ObStorageLeakChecker();

  static ObStorageLeakChecker instance_;

  hash::ObHashMap<ObStorageCheckerKey, ObStorageCheckerValue> checker_info_;
};

template<typename T>
OB_INLINE bool ObStorageLeakChecker::handle_hold(T* handle, bool errsim_bypass)
{
  bool b_ret = false;
  if (OB_UNLIKELY(handle->need_trace() || errsim_bypass)) {
    inner_handle_hold(static_cast<ObStorageCheckedObjectBase*>(handle), handle->get_check_id());
    b_ret = true;
  }
  return b_ret;
}

template<typename T>
OB_INLINE void ObStorageLeakChecker::handle_reset(T* handle)
{
  if (OB_UNLIKELY(handle->is_traced())) {
    inner_handle_reset(static_cast<ObStorageCheckedObjectBase*>(handle), handle->get_check_id());
  }
}


}  // storage
}  // oceanbase
#endif  // OCEANBASE_STORAGE_OB_STORAGE_LEAK_CHECKER_H_
