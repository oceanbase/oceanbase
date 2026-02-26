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

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadClientTask;
class ObTableLoadClientTaskBrief;

class ObTableLoadManager
{
public:
  ObTableLoadManager(const uint64_t tenant_id);
  ~ObTableLoadManager();
  int init();

#define DEFINE_OBJ_ALLOC_INTERFACE(ValueType, name)                \
  int acquire_##name(ValueType *&value);                           \
  void release_##name(ValueType *value);                           \
  void revert_##name(ValueType *value);                            \
  int acquire(ValueType *&value) { return acquire_##name(value); } \
  void release(ValueType *&value) { release_##name(value); }       \
  void revert(ValueType *value) { revert_##name(value); }

  DEFINE_OBJ_ALLOC_INTERFACE(ObTableLoadTableCtx, table_ctx);
  DEFINE_OBJ_ALLOC_INTERFACE(ObTableLoadClientTask, client_task);
  DEFINE_OBJ_ALLOC_INTERFACE(ObTableLoadClientTaskBrief, client_task_brief);

#undef DEFINE_OBJ_ALLOC_INTERFACE

#define DEFINE_OBJ_MAP_INTERFACE(ValueType, name)                       \
  int add_##name(const ObTableLoadUniqueKey &key, ValueType *value);    \
  int remove_##name(const ObTableLoadUniqueKey &key, ValueType *value); \
  int remove_all_##name();                                              \
  int remove_inactive_##name();                                         \
  int get_##name(const ObTableLoadUniqueKey &key, ValueType *&value);   \
  int get_all_##name(common::ObIArray<ValueType *> &list);              \
  int64_t get_##name##_cnt() const;

  DEFINE_OBJ_MAP_INTERFACE(ObTableLoadTableCtx, table_ctx);
  DEFINE_OBJ_MAP_INTERFACE(ObTableLoadClientTask, client_task);
  DEFINE_OBJ_MAP_INTERFACE(ObTableLoadClientTaskBrief, client_task_brief);

#undef DEFINE_OBJ_MAP_INTERFACE

#define DEFINE_OBJ_GC_INTERFACE(ValueType, name)    \
  int push_##name##_into_gc_list(ValueType *value); \
  int gc_##name##_in_list();                        \
  int64_t get_##name##_cnt_in_gc_list() const;

  DEFINE_OBJ_GC_INTERFACE(ObTableLoadTableCtx, table_ctx);
  DEFINE_OBJ_GC_INTERFACE(ObTableLoadClientTask, client_task);

#undef DEFINE_OBJ_GC_INTERFACE

  int check_all_obj_removed(bool &all_removed);
  int check_all_obj_released(bool &all_released);

private:
  template <typename Value>
  class ObjAllocator
  {
  public:
    ObjAllocator(const ObMemAttr &attr) : attr_(attr) {}
    ~ObjAllocator() { OB_ASSERT(used_list_.is_empty()); }
    int acquire(Value *&value)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(value = OB_NEW(Value, attr_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to alloc table ctx", KR(ret));
      } else {
        add_list(value);
      }
      return ret;
    }
    void release(Value *value)
    {
      if (OB_NOT_NULL(value)) {
        remove_list(value);
        OB_DELETE(Value, attr_, value);
      }
    }
    int64_t get_used_count() const
    {
      ObMutexGuard guard(mutex_);
      return used_list_.get_size();
    }

  private:
    inline void add_list(Value *value)
    {
      ObMutexGuard guard(mutex_);
      OB_ASSERT(used_list_.add_last(value));
    }
    inline void remove_list(Value *value)
    {
      ObMutexGuard guard(mutex_);
      OB_ASSERT(OB_NOT_NULL(used_list_.remove(value)));
    }

  private:
    ObMemAttr attr_;
    mutable lib::ObMutex mutex_;
    common::ObDList<Value> used_list_;
  };

private:
  template <typename Value>
  class ObjHandle
  {
  public:
    ObjHandle() : manager_(nullptr), value_(nullptr) {}
    ObjHandle(const ObjHandle &other) : manager_(nullptr), value_(nullptr) { *this = other; }
    ObjHandle &operator=(const ObjHandle &other)
    {
      if (this != &other) {
        reset();
        if (other.is_valid()) {
          manager_ = other.manager_;
          value_ = other.value_;
          value_->inc_ref_count();
        }
      }
      return *this;
    }
    ~ObjHandle() { reset(); }
    void reset()
    {
      if (nullptr != manager_ && nullptr != value_) {
        manager_->revert(value_);
      }
      manager_ = nullptr;
      value_ = nullptr;
    }
    int set_value(ObTableLoadManager *manager, Value *value)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(nullptr == manager || nullptr == value)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "invalid args", KR(ret), KP(manager), KP(value));
      } else {
        reset();
        manager_ = manager;
        value_ = value;
        value_->inc_ref_count();
      }
      return ret;
    }
    Value *get_value() const { return value_; }
    bool is_valid() const { return nullptr != manager_ && nullptr != value_; }
    TO_STRING_KV(KP_(manager), KP_(value));

  private:
    ObTableLoadManager *manager_;
    Value *value_;
  };
  typedef ObjHandle<ObTableLoadTableCtx> ObTableLoadTableCtxHandle;
  typedef ObjHandle<ObTableLoadClientTask> ObTableLoadClientTaskHandle;
  typedef ObjHandle<ObTableLoadClientTaskBrief> ObTableLoadClientTaskBriefHandle;

  typedef common::hash::ObHashMap<ObTableLoadUniqueKey, ObTableLoadTableCtxHandle>
    ObTableLoadTableCtxMap;
  typedef common::hash::ObHashMap<ObTableLoadUniqueKey, ObTableLoadClientTaskHandle>
    ObTableLoadClientTaskMap;
  typedef common::hash::ObHashMap<ObTableLoadUniqueKey, ObTableLoadClientTaskBriefHandle>
    ObTableLoadClientTaskBriefMap;

  template <typename Value>
  class HashMapEraseIfEqual
  {
    typedef ObjHandle<Value> Handle;

  public:
    HashMapEraseIfEqual(Value *value) : value_(value) {}
    bool operator()(common::hash::HashMapPair<ObTableLoadUniqueKey, Handle> &entry) const
    {
      return value_ == entry.second.get_value();
    }
    bool operator()(common::hash::HashMapPair<uint64_t, Handle> &entry) const
    {
      return value_ == entry.second.get_value();
    }

  public:
    Value *value_;
  };

  template <typename Value>
  class GetAllKeyFunc
  {
    typedef ObjHandle<Value> Handle;

  public:
    GetAllKeyFunc(common::ObIArray<ObTableLoadUniqueKey> &list) : list_(list) {}
    int operator()(common::hash::HashMapPair<ObTableLoadUniqueKey, Handle> &entry)
    {
      int ret = OB_SUCCESS;
      const ObTableLoadUniqueKey &key = entry.first;
      if (OB_FAIL(list_.push_back(key))) {
        SERVER_LOG(WARN, "fail to push back", KR(ret), K(key));
      }
      return ret;
    }

  private:
    common::ObIArray<ObTableLoadUniqueKey> &list_;
  };

  template <typename Value>
  class GetAllInactiveKeyFunc
  {
    typedef ObjHandle<Value> Handle;

  public:
    GetAllInactiveKeyFunc(common::ObIArray<ObTableLoadUniqueKey> &list) : list_(list) {}
    int operator()(common::hash::HashMapPair<ObTableLoadUniqueKey, Handle> &entry)
    {
      int ret = OB_SUCCESS;
      const ObTableLoadUniqueKey &key = entry.first;
      const Handle &handle = entry.second;
      if (OB_UNLIKELY(!handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected handle is invalid", KR(ret), K(key), K(handle));
      } else {
        Value *value = handle.get_value();
        if (value->get_ref_count() > 1) {
        } else if (OB_FAIL(list_.push_back(key))) {
          SERVER_LOG(WARN, "fail to push back", KR(ret));
        }
      }
      return ret;
    }

  private:
    common::ObIArray<ObTableLoadUniqueKey> &list_;
  };

  template <typename Value>
  class GetAllValueFunc
  {
    typedef ObjHandle<Value> Handle;

  public:
    GetAllValueFunc(common::ObIArray<Value *> &list) : list_(list) {}
    int operator()(common::hash::HashMapPair<ObTableLoadUniqueKey, Handle> &entry)
    {
      int ret = OB_SUCCESS;
      const ObTableLoadUniqueKey &key = entry.first;
      const Handle &handle = entry.second;
      if (OB_UNLIKELY(!handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected handle is invalid", KR(ret), K(key), K(handle));
      } else {
        Value *value = handle.get_value();
        if (OB_FAIL(list_.push_back(value))) {
          SERVER_LOG(WARN, "fail to push back", KR(ret), KP(value));
        } else {
          value->inc_ref_count();
        }
      }
      return ret;
    }

  private:
    common::ObIArray<Value *> &list_;
  };

private:
  const uint64_t tenant_id_;

  ObjAllocator<ObTableLoadTableCtx> table_ctx_alloc_;
  ObjAllocator<ObTableLoadClientTask> client_task_alloc_;
  ObjAllocator<ObTableLoadClientTaskBrief> client_task_brief_alloc_;

  // map会持有对象的引用计数
  ObTableLoadTableCtxMap table_ctx_map_;
  ObTableLoadClientTaskMap client_task_map_;
  ObTableLoadClientTaskBriefMap client_task_brief_map_;

  // 对象的引用计数归0后, 放入gc队列
  // client_task_brief的引用计数归0后直接释放
  mutable common::ObSpinLock gc_list_lock_;
  ObArray<ObTableLoadTableCtx *> table_ctx_gc_list_;
  ObArray<ObTableLoadClientTask *> client_task_gc_list_;

  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
