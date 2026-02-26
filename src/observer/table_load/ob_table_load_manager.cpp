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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_manager.h"
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace lib;
using namespace table;

/**
 * ObTableLoadManager
 */

ObTableLoadManager::ObTableLoadManager(const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    table_ctx_alloc_(ObMemAttr(tenant_id, "TLD_TblCtxVal")),
    client_task_alloc_(ObMemAttr(tenant_id, "TLD_CliTaskVal")),
    client_task_brief_alloc_(ObMemAttr(tenant_id, "TLD_CTBriefVal")),
    is_inited_(false)
{
}

ObTableLoadManager::~ObTableLoadManager() {}

int ObTableLoadManager::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadManager init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_ctx_map_.create(bucket_num, "TLD_TblCtxMap", "TLD_TblCtxMap", tenant_id_))) {
      LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
    } else if (OB_FAIL(client_task_map_.create(bucket_num, "TLD_CliTaskMap", "TLD_CliTaskMap",
                                               tenant_id_))) {
      LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
    } else if (OB_FAIL(client_task_brief_map_.create(bucket_num, "TLD_CTBriefMap", "TLD_CTBriefMap",
                                                     tenant_id_))) {
      LOG_WARN("fail to init create hashmap", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

// OBJ ALLOC IMPL

#define DEFINE_OBJ_ALLOC_ACQUIRE_IMPL(ValueType, name)            \
  int ObTableLoadManager::acquire_##name(ValueType *&value)       \
  {                                                               \
    int ret = OB_SUCCESS;                                         \
    if (IS_NOT_INIT) {                                            \
      ret = OB_NOT_INIT;                                          \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this)); \
    } else if (OB_FAIL(name##_alloc_.acquire(value))) {           \
      LOG_WARN("fail to acquire", KR(ret));                       \
    } else {                                                      \
      value->inc_ref_count();                                     \
      FLOG_INFO("acquire " #name " succeed", KP(value));          \
    }                                                             \
    return ret;                                                   \
  }

#define DEFINE_OBJ_ALLOC_RELEASE_IMPL(ValueType, name)                                      \
  void ObTableLoadManager::release_##name(ValueType *value)                                 \
  {                                                                                         \
    if (OB_NOT_NULL(value)) {                                                               \
      if (0 != value->get_ref_count()) {                                                    \
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, #name " reference count may be leak", KPC(value)); \
      } else {                                                                              \
        FLOG_INFO("release " #name " succeed", KP(value));                                  \
        name##_alloc_.release(value);                                                       \
      }                                                                                     \
    }                                                                                       \
  }

#define DEFINE_OBJ_ALLOC_REVERT_IMPL1(ValueType, name)                                          \
  void ObTableLoadManager::revert_##name(ValueType *value)                                      \
  {                                                                                             \
    if (OB_NOT_NULL(value)) {                                                                   \
      const int64_t ref_cnt = value->dec_ref_count();                                           \
      if (0 == ref_cnt) {                                                                       \
        FLOG_INFO("release " #name " succeed", KP(value));                                      \
        name##_alloc_.release(value);                                                           \
      } else if (OB_UNLIKELY(ref_cnt < 0)) {                                                    \
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "possible " #name " leak!!!", K(ref_cnt), KPC(value)); \
      }                                                                                         \
    }                                                                                           \
  }

#define DEFINE_OBJ_ALLOC_REVERT_IMPL2(ValueType, name)                                          \
  void ObTableLoadManager::revert_##name(ValueType *value)                                      \
  {                                                                                             \
    if (OB_NOT_NULL(value)) {                                                                   \
      const int64_t ref_cnt = value->dec_ref_count();                                           \
      if (0 == ref_cnt) {                                                                       \
        push_##name##_into_gc_list(value);                                                      \
      } else if (OB_UNLIKELY(ref_cnt < 0)) {                                                    \
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "possible " #name " leak!!!", K(ref_cnt), KPC(value)); \
      }                                                                                         \
    }                                                                                           \
  }

#define DEFINE_OBJ_ALLOC_IMPL1(ValueType, name)   \
  DEFINE_OBJ_ALLOC_ACQUIRE_IMPL(ValueType, name); \
  DEFINE_OBJ_ALLOC_RELEASE_IMPL(ValueType, name); \
  DEFINE_OBJ_ALLOC_REVERT_IMPL1(ValueType, name);

#define DEFINE_OBJ_ALLOC_IMPL2(ValueType, name)   \
  DEFINE_OBJ_ALLOC_ACQUIRE_IMPL(ValueType, name); \
  DEFINE_OBJ_ALLOC_RELEASE_IMPL(ValueType, name); \
  DEFINE_OBJ_ALLOC_REVERT_IMPL2(ValueType, name);

DEFINE_OBJ_ALLOC_IMPL2(ObTableLoadTableCtx, table_ctx);
DEFINE_OBJ_ALLOC_IMPL2(ObTableLoadClientTask, client_task);
DEFINE_OBJ_ALLOC_IMPL1(ObTableLoadClientTaskBrief, client_task_brief);

#undef DEFINE_OBJ_ALLOC_ACQUIRE_IMPL
#undef DEFINE_OBJ_ALLOC_RELEASE_IMPL
#undef DEFINE_OBJ_ALLOC_REVERT_IMPL
#undef DEFINE_OBJ_ALLOC_IMPL

// OBJ MAP IMPL

#define DEFINE_OBJ_MAP_ADD_IMPL(ValueType, name)                                        \
  int ObTableLoadManager::add_##name(const ObTableLoadUniqueKey &key, ValueType *value) \
  {                                                                                     \
    int ret = OB_SUCCESS;                                                               \
    if (IS_NOT_INIT) {                                                                  \
      ret = OB_NOT_INIT;                                                                \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));                       \
    } else if (OB_UNLIKELY(!key.is_valid() || nullptr == value)) {                      \
      ret = OB_INVALID_ARGUMENT;                                                        \
      LOG_WARN("invalid args", KR(ret), K(key), KP(value));                             \
    } else if (OB_UNLIKELY(value->is_in_map())) {                                       \
      ret = OB_ERR_UNEXPECTED;                                                          \
      LOG_WARN(#name "already in map", KR(ret), KPC(value));                            \
    } else {                                                                            \
      ValueType##Handle handle;                                                         \
      if (OB_FAIL(handle.set_value(this, value))) {                                     \
        LOG_WARN("fail to set value", KR(ret));                                         \
      } else if (OB_FAIL(name##_map_.set_refactored(key, handle))) {                    \
        if (OB_UNLIKELY(OB_HASH_EXIST != ret)) {                                        \
          LOG_WARN("fail to set refactored", KR(ret), K(key));                          \
        } else {                                                                        \
          ret = OB_ENTRY_EXIST;                                                         \
          LOG_WARN(#name "already exist", KR(ret), K(key));                             \
        }                                                                               \
      } else {                                                                          \
        value->set_is_in_map(true);                                                     \
        FLOG_INFO("add " #name " succeed", K(key), KP(value));                          \
      }                                                                                 \
    }                                                                                   \
    return ret;                                                                         \
  }

#define DEFINE_OBJ_MAP_REMOVE_IMPL(ValueType, name)                                        \
  int ObTableLoadManager::remove_##name(const ObTableLoadUniqueKey &key, ValueType *value) \
  {                                                                                        \
    int ret = OB_SUCCESS;                                                                  \
    if (IS_NOT_INIT) {                                                                     \
      ret = OB_NOT_INIT;                                                                   \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));                          \
    } else if (OB_UNLIKELY(!key.is_valid() || nullptr == value)) {                         \
      ret = OB_INVALID_ARGUMENT;                                                           \
      LOG_WARN("invalid args", KR(ret), K(key), KP(value));                                \
    } else if (OB_UNLIKELY(!value->is_in_map())) {                                         \
      ret = OB_ENTRY_NOT_EXIST;                                                            \
      LOG_WARN(#name "not in map", KR(ret), KPC(value));                                   \
    } else {                                                                               \
      bool is_erased = false;                                                              \
      HashMapEraseIfEqual<ValueType> erase_if_equal(value);                                \
      if (OB_FAIL(name##_map_.erase_if(key, erase_if_equal, is_erased))) {                 \
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {                                       \
          LOG_WARN("fail to erase refactored", KR(ret), K(key));                           \
        } else {                                                                           \
          ret = OB_ENTRY_NOT_EXIST;                                                        \
          LOG_WARN(#name "not exist in manager", KR(ret), K(key), KPC(value));             \
        }                                                                                  \
      } else if (OB_UNLIKELY(!is_erased)) {                                                \
        ret = OB_ERR_UNEXPECTED;                                                           \
        LOG_WARN("unexpected " #name " not in manager", KR(ret), K(key), KPC(value));      \
      } else {                                                                             \
        value->set_is_in_map(false);                                                       \
        FLOG_INFO("remove " #name " succeed", K(key), KP(value));                          \
      }                                                                                    \
    }                                                                                      \
    return ret;                                                                            \
  }

#define DEFINE_OBJ_MAP_REMOVE_ALL_IMPL(ValueType, name)                           \
  int ObTableLoadManager::remove_all_##name()                                     \
  {                                                                               \
    int ret = OB_SUCCESS;                                                         \
    if (IS_NOT_INIT) {                                                            \
      ret = OB_NOT_INIT;                                                          \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));                 \
    } else {                                                                      \
      ObArray<ObTableLoadUniqueKey> keys;                                         \
      while (OB_SUCC(ret) && !name##_map_.empty()) {                              \
        keys.reset();                                                             \
        GetAllKeyFunc<ValueType> func(keys);                                      \
        if (OB_FAIL(name##_map_.foreach_refactored(func))) {                      \
          LOG_WARN("fail to foreach hashmap", KR(ret));                           \
        }                                                                         \
        for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {              \
          const ObTableLoadUniqueKey &key = keys.at(i);                           \
          ValueType##Handle handle;                                               \
          if (OB_FAIL(name##_map_.erase_refactored(key, &handle))) {              \
            if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {                          \
              LOG_WARN("fail to erase refactored", KR(ret), K(key));              \
            } else {                                                              \
              ret = OB_SUCCESS;                                                   \
            }                                                                     \
          } else if (OB_UNLIKELY(!handle.is_valid())) {                           \
            ret = OB_ERR_UNEXPECTED;                                              \
            LOG_WARN("unexpected handle is invalid", KR(ret), K(key), K(handle)); \
          } else {                                                                \
            ValueType *value = handle.get_value();                                \
            value->set_is_in_map(false);                                          \
            FLOG_INFO("remove " #name " succeed", K(key), KP(value));             \
          }                                                                       \
        }                                                                         \
      }                                                                           \
    }                                                                             \
    return ret;                                                                   \
  }

#define DEFINE_OBJ_MAP_REMOVE_INACTIVE_IMPL(ValueType, name)                    \
  int ObTableLoadManager::remove_inactive_##name()                              \
  {                                                                             \
    int ret = OB_SUCCESS;                                                       \
    if (IS_NOT_INIT) {                                                          \
      ret = OB_NOT_INIT;                                                        \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));               \
    } else {                                                                    \
      ObArray<ObTableLoadUniqueKey> keys;                                       \
      GetAllInactiveKeyFunc<ValueType> func(keys);                              \
      if (OB_FAIL(name##_map_.foreach_refactored(func))) {                      \
        LOG_WARN("fail to foreach hashmap", KR(ret));                           \
      }                                                                         \
      for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {              \
        const ObTableLoadUniqueKey &key = keys.at(i);                           \
        ValueType##Handle handle;                                               \
        if (OB_FAIL(name##_map_.erase_refactored(key, &handle))) {              \
          if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {                          \
            LOG_WARN("fail to erase refactored", KR(ret), K(key));              \
          } else {                                                              \
            ret = OB_SUCCESS;                                                   \
          }                                                                     \
        } else if (OB_UNLIKELY(!handle.is_valid())) {                           \
          ret = OB_ERR_UNEXPECTED;                                              \
          LOG_WARN("unexpected handle is invalid", KR(ret), K(key), K(handle)); \
        } else {                                                                \
          ValueType *value = handle.get_value();                                \
          value->set_is_in_map(false);                                          \
          FLOG_INFO("remove " #name " succeed", K(key), KP(value));             \
        }                                                                       \
      }                                                                         \
    }                                                                           \
    return ret;                                                                 \
  }

#define DEFINE_OBJ_MAP_GET_IMPL(ValueType, name)                                         \
  int ObTableLoadManager::get_##name(const ObTableLoadUniqueKey &key, ValueType *&value) \
  {                                                                                      \
    int ret = OB_SUCCESS;                                                                \
    if (IS_NOT_INIT) {                                                                   \
      ret = OB_NOT_INIT;                                                                 \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));                        \
    } else if (OB_UNLIKELY(!key.is_valid())) {                                           \
      ret = OB_INVALID_ARGUMENT;                                                         \
      LOG_WARN("invalid args", KR(ret), K(key));                                         \
    } else {                                                                             \
      value = nullptr;                                                                   \
      ValueType##Handle handle;                                                          \
      if (OB_FAIL(name##_map_.get_refactored(key, handle))) {                            \
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {                                     \
          LOG_WARN("fail to get refactored", KR(ret), K(key));                           \
        } else {                                                                         \
          ret = OB_ENTRY_NOT_EXIST;                                                      \
          LOG_WARN(#name "not exist", KR(ret), K(key));                                  \
        }                                                                                \
      } else if (OB_UNLIKELY(!handle.is_valid())) {                                      \
        ret = OB_ERR_UNEXPECTED;                                                         \
        LOG_WARN("unexpected handle is invalid", KR(ret), K(key), K(handle));            \
      } else {                                                                           \
        value = handle.get_value();                                                      \
        value->inc_ref_count();                                                          \
      }                                                                                  \
    }                                                                                    \
    return ret;                                                                          \
  }

#define DEFINE_OBJ_MAP_GET_ALL_IMPL(ValueType, name)                  \
  int ObTableLoadManager::get_all_##name(ObIArray<ValueType *> &list) \
  {                                                                   \
    int ret = OB_SUCCESS;                                             \
    if (IS_NOT_INIT) {                                                \
      ret = OB_NOT_INIT;                                              \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));     \
    } else {                                                          \
      list.reset();                                                   \
      GetAllValueFunc<ValueType> func(list);                          \
      if (OB_FAIL(name##_map_.foreach_refactored(func))) {            \
        LOG_WARN("fail to foreach hashmap", KR(ret));                 \
      }                                                               \
      if (OB_FAIL(ret)) {                                             \
        for (int64_t i = 0; i < list.count(); ++i) {                  \
          ValueType *value = list.at(i);                              \
          revert_##name(value);                                       \
        }                                                             \
        list.reset();                                                 \
      }                                                               \
    }                                                                 \
    return ret;                                                       \
  }

#define DEFINE_OBJ_MAP_GET_CNT_IMPL(ValueType, name) \
  int64_t ObTableLoadManager::get_##name##_cnt() const { return name##_map_.size(); }

#define DEFINE_OBJ_MAP_IMPL(ValueType, name)            \
  DEFINE_OBJ_MAP_ADD_IMPL(ValueType, name);             \
  DEFINE_OBJ_MAP_REMOVE_IMPL(ValueType, name);          \
  DEFINE_OBJ_MAP_REMOVE_ALL_IMPL(ValueType, name);      \
  DEFINE_OBJ_MAP_REMOVE_INACTIVE_IMPL(ValueType, name); \
  DEFINE_OBJ_MAP_GET_IMPL(ValueType, name);             \
  DEFINE_OBJ_MAP_GET_ALL_IMPL(ValueType, name);         \
  DEFINE_OBJ_MAP_GET_CNT_IMPL(ValueType, name);

DEFINE_OBJ_MAP_IMPL(ObTableLoadTableCtx, table_ctx);
DEFINE_OBJ_MAP_IMPL(ObTableLoadClientTask, client_task);
DEFINE_OBJ_MAP_IMPL(ObTableLoadClientTaskBrief, client_task_brief);

#undef DEFINE_OBJ_MAP_ADD_IMPL
#undef DEFINE_OBJ_MAP_REMOVE_IMPL
#undef DEFINE_OBJ_MAP_REMOVE_ALL_IMPL
#undef DEFINE_OBJ_MAP_REMOVE_INACTIVE_IMPL
#undef DEFINE_OBJ_MAP_GET_IMPL
#undef DEFINE_OBJ_MAP_GET_ALL_IMPL
#undef DEFINE_OBJ_MAP_GET_CNT_IMPL
#undef DEFINE_OBJ_MAP_IMPL

// OBJ GC IMPL

#define DEFINE_OBJ_GC_PUSH_IMPL(ValueType, name)                                         \
  int ObTableLoadManager::push_##name##_into_gc_list(ValueType *value)                   \
  {                                                                                      \
    int ret = OB_SUCCESS;                                                                \
    if (IS_NOT_INIT) {                                                                   \
      ret = OB_NOT_INIT;                                                                 \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));                        \
    } else if (OB_ISNULL(value)) {                                                       \
      ret = OB_INVALID_ARGUMENT;                                                         \
      LOG_WARN("invalid args", KR(ret), KP(value));                                      \
    } else {                                                                             \
      static const int64_t MAX_RETRY_TIMES = 1000;                                       \
      int64_t retry_cnt = 0;                                                             \
      do {                                                                               \
        {                                                                                \
          lib::ObLockGuard<common::ObSpinLock> lock_guard(gc_list_lock_);                \
          if (OB_FAIL(name##_gc_list_.push_back(value))) {                               \
            LOG_WARN("fail to push back", KR(ret));                                      \
          } else {                                                                       \
            FLOG_INFO("push " #name " into gc list succeed", KP(value));                 \
          }                                                                              \
        }                                                                                \
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {                                          \
          ob_usleep(100_ms);                                                             \
          retry_cnt++;                                                                   \
          if (retry_cnt % 100 == 0) {                                                    \
            LOG_ERROR("push " #name " into gc list retry too many times", K(retry_cnt)); \
          }                                                                              \
        }                                                                                \
      } while (ret == OB_ALLOCATE_MEMORY_FAILED && retry_cnt < MAX_RETRY_TIMES);         \
    }                                                                                    \
    return ret;                                                                          \
  }

#define DEFINE_OBJ_GC_CLEAN_IMPL(ValueType, name)                       \
  int ObTableLoadManager::gc_##name##_in_list()                         \
                                                                        \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    if (IS_NOT_INIT) {                                                  \
      ret = OB_NOT_INIT;                                                \
      LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));       \
    } else {                                                            \
      ObArray<ValueType *> gc_list;                                     \
      {                                                                 \
        lib::ObLockGuard<common::ObSpinLock> lock_guard(gc_list_lock_); \
        if (OB_FAIL(gc_list.assign(name##_gc_list_))) {                 \
          LOG_WARN("fail to assign gc list", KR(ret));                  \
        } else {                                                        \
          name##_gc_list_.reset();                                      \
        }                                                               \
      }                                                                 \
      for (int64_t i = 0; i < gc_list.count(); ++i) {                   \
        ValueType *value = gc_list.at(i);                               \
        FLOG_INFO("release " #name " succeed", KP(value));              \
        name##_alloc_.release(value);                                   \
      }                                                                 \
    }                                                                   \
    return ret;                                                         \
  }

#define DEFINE_OBJ_GC_GET_CNT_IMPL(ValueType, name)                 \
  int64_t ObTableLoadManager::get_##name##_cnt_in_gc_list() const   \
  {                                                                 \
    lib::ObLockGuard<common::ObSpinLock> lock_guard(gc_list_lock_); \
    return name##_gc_list_.count();                                 \
  }

#define DEFINE_OBJ_GC_IMPL(ValueType, name)  \
  DEFINE_OBJ_GC_PUSH_IMPL(ValueType, name);  \
  DEFINE_OBJ_GC_CLEAN_IMPL(ValueType, name); \
  DEFINE_OBJ_GC_GET_CNT_IMPL(ValueType, name);

DEFINE_OBJ_GC_IMPL(ObTableLoadTableCtx, table_ctx);
DEFINE_OBJ_GC_IMPL(ObTableLoadClientTask, client_task);

#undef DEFINE_OBJ_GC_PUSH_IMPL
#undef DEFINE_OBJ_GC_CLEAN_IMPL
#undef DEFINE_OBJ_GC_GET_CNT_IMPL
#undef DEFINE_OBJ_GC_IMPL

int ObTableLoadManager::check_all_obj_removed(bool &all_removed)
{
  int ret = OB_SUCCESS;
  all_removed = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else {
    const int64_t table_ctx_cnt = table_ctx_map_.size();
    const int64_t client_task_cnt = client_task_map_.size();
    const int64_t client_task_brief_cnt = client_task_brief_map_.size();
    if (0 != table_ctx_cnt || 0 != client_task_cnt || 0 != client_task_brief_cnt) {
      all_removed = false;
    } else {
      all_removed = true;
    }
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("check all obj in map", K(table_ctx_cnt), K(client_task_cnt),
               K(client_task_brief_cnt));
    }
  }
  return ret;
}

int ObTableLoadManager::check_all_obj_released(bool &all_released)
{
  int ret = OB_SUCCESS;
  all_released = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else {
    const int64_t table_ctx_cnt = table_ctx_alloc_.get_used_count();
    const int64_t client_task_cnt = client_task_alloc_.get_used_count();
    const int64_t client_task_brief_cnt = client_task_brief_alloc_.get_used_count();
    if (0 != table_ctx_cnt || 0 != client_task_cnt || 0 != client_task_brief_cnt) {
      all_released = false;
    } else {
      all_released = true;
    }
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("check all obj in alloc", K(table_ctx_cnt), K(client_task_cnt),
               K(client_task_brief_cnt));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
