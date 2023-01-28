// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <jianming.cjq@alipay.com>

#pragma once

#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/lock/ob_mutex.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace observer
{

template<class KeyType, class ValueType>
class ObTableLoadManager
{
public:
  int init();
  int put(const KeyType &key, ValueType *value);
  int get(const KeyType &key, ValueType *&value);
  int remove(const KeyType &key, ValueType *value);
  void free_value(ValueType *value);
  template<typename... Args>
  int new_and_insert(const KeyType &key, ValueType *&value, const Args&... args);
  template<typename... Args>
  int get_or_new(const KeyType &key, ValueType *&value, bool &is_new, const Args&... args);
  template <typename Function>
  int for_each(Function &fn) { return value_map_.for_each(fn); }
private:
  template<typename... Args>
  int new_value(const KeyType &key, ValueType *&value, const Args&... args);
private:
  common::ObConcurrentHashMap<KeyType, ValueType *> value_map_;
  lib::ObMutex mutex_;
};

template<class KeyType, class ValueType>
int ObTableLoadManager<KeyType, ValueType>::init()
{
  return value_map_.init();
}

template<class KeyType, class ValueType>
int ObTableLoadManager<KeyType, ValueType>::put(const KeyType &key, ValueType *value)
{
  return value_map_.put_refactored(key, value);
}

template<class KeyType, class ValueType>
int ObTableLoadManager<KeyType, ValueType>::get(const KeyType &key, ValueType *&value)
{
  return value_map_.get_refactored(key, value);
}

template<class KeyType, class ValueType>
int ObTableLoadManager<KeyType, ValueType>::remove(const KeyType &key, ValueType *value)
{
  return value_map_.remove_refactored(key);
}

template<class KeyType, class ValueType>
template<typename... Args>
int ObTableLoadManager<KeyType, ValueType>::new_value(const KeyType &key, ValueType *&value,
                                                      const Args&... args)
{
  int ret = common::OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ret = get(key, value);
  if (ret == common::OB_ENTRY_NOT_EXIST) {
    ret = common::OB_SUCCESS;
    if (OB_ISNULL(value = OB_NEW(ValueType, ObMemAttr(MTL_ID(), "TLD_MgrValue"), args...))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to new value", KR(ret), K(key));
    } else {
      if (OB_FAIL(value->init())) {
        OB_LOG(WARN, "fail to init value", KR(ret));
      } else if (OB_FAIL(put(key, value))) {
        OB_LOG(WARN, "fail to put value", KR(ret));
      }
      if (OB_FAIL(ret)) {
        free_value(value);
        value = nullptr;
      }
    }
  } else if (ret != common::OB_SUCCESS) {
    OB_LOG(WARN, "fail to get value", KR(ret));
  } else { // 已存在
    ret = common::OB_ENTRY_EXIST;
  }
  return ret;
}

template<class KeyType, class ValueType>
void ObTableLoadManager<KeyType, ValueType>::free_value(ValueType *value)
{
  OB_DELETE(ValueType, "TLD_MgrValue", value);
}

template<class KeyType, class ValueType>
template<typename... Args>
int ObTableLoadManager<KeyType, ValueType>::new_and_insert(const KeyType &key, ValueType *&value,
                                                           const Args&... args)
{
  int ret = common::OB_SUCCESS;
  ret = value_map_.contains_key(key);
  if (OB_LIKELY(common::OB_ENTRY_NOT_EXIST == ret)) {
    ret = new_value(key, value, args...);
  }
  if (OB_FAIL(ret)) {
    if (OB_LIKELY(common::OB_ENTRY_EXIST == ret)) {
      OB_LOG(WARN, "value already exist", KR(ret), K(key));
    } else {
      OB_LOG(WARN, "fail to call contains key", KR(ret), K(key));
    }
  }
  return ret;
}

template<class KeyType, class ValueType>
template<typename... Args>
int ObTableLoadManager<KeyType, ValueType>::get_or_new(const KeyType &key, ValueType *&value,
                                                       bool &is_new, const Args&... args)
{
  int ret = common::OB_SUCCESS;
  value = nullptr;
  is_new = false;
  ret = get(key, value);
  if (common::OB_ENTRY_NOT_EXIST == ret) {
    ret = common::OB_SUCCESS;
    if (OB_FAIL(new_value(key, value, args...))) {
      if (OB_UNLIKELY(common::OB_ENTRY_EXIST != ret)) {
        OB_LOG(WARN, "fail to new value", KR(ret));
      } else { // 已存在
        ret = common::OB_SUCCESS;
      }
    } else {
      is_new = true;
    }
  } else if (ret != common::OB_SUCCESS) {
    OB_LOG(WARN, "fail to get value", KR(ret));
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
