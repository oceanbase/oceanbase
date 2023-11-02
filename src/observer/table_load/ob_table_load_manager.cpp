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
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "share/rc/ob_tenant_base.h"

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

ObTableLoadManager::ObTableLoadManager() : is_inited_(false) {}

ObTableLoadManager::~ObTableLoadManager() {}

int ObTableLoadManager::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadManager init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(
          table_ctx_map_.create(bucket_num, "TLD_TableCtxMgr", "TLD_TableCtxMgr", MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
    } else if (OB_FAIL(table_ctx_index_map_.create(bucket_num, "TLD_TblCtxIMgr", "TLD_TblCtxIMgr",
                                                   MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadManager::add_table_ctx(const ObTableLoadUniqueKey &key,
                                      ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key.is_valid() || nullptr == table_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(key), KP(table_ctx));
  } else if (OB_UNLIKELY(table_ctx->is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dirty table ctx", KR(ret), KP(table_ctx));
  } else {
    const uint64_t table_id = key.table_id_;
    obsys::ObWLockGuard guard(rwlock_);
    if (OB_FAIL(table_ctx_map_.set_refactored(key, table_ctx))) {
      if (OB_UNLIKELY(OB_HASH_EXIST != ret)) {
        LOG_WARN("fail to set refactored", KR(ret), K(key));
      } else {
        ret = OB_ENTRY_EXIST;
      }
    }
    // force update table ctx index
    else if (OB_FAIL(table_ctx_index_map_.set_refactored(table_id, table_ctx, 1))) {
      LOG_WARN("fail to set refactored", KR(ret), K(table_id));
      // erase from table ctx map, avoid wild pointer is been use
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(table_ctx_map_.erase_refactored(key))) {
        LOG_WARN("fail to erase refactored", KR(tmp_ret), K(key));
      }
    } else {
      table_ctx->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadManager::remove_table_ctx(const ObTableLoadUniqueKey &key,
                                         ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key.is_valid() || nullptr == table_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(key), KP(table_ctx));
  } else {
    {
      const uint64_t table_id = key.table_id_;
      HashMapEraseIfEqual erase_if_equal(table_ctx);
      bool is_erased = false;
      obsys::ObWLockGuard guard(rwlock_);
      if (OB_FAIL(table_ctx_map_.erase_if(key, erase_if_equal, is_erased))) {
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
          LOG_WARN("fail to erase refactored", KR(ret), K(key));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("table ctx not exist in manager", KR(ret), K(key), KPC(table_ctx));
        }
      } else if (OB_UNLIKELY(!is_erased)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table ctx not in manager", KR(ret), K(key), KPC(table_ctx));
      }
      // try remove table ctx index
      else if (OB_FAIL(table_ctx_index_map_.erase_if(table_id, erase_if_equal, is_erased))) {
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
          LOG_WARN("fail to get refactored", KR(ret), K(table_id));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_dirty_list(table_ctx))) {
        LOG_WARN("fail to add dirty list", KR(ret), K(key), KP(table_ctx));
      }
    }
  }
  return ret;
}

int ObTableLoadManager::get_all_table_ctx(ObIArray<ObTableLoadTableCtx *> &table_ctx_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else {
    table_ctx_array.reset();
    obsys::ObRLockGuard guard(rwlock_);
    for (TableCtxMap::const_iterator iter = table_ctx_map_.begin();
         OB_SUCC(ret) && iter != table_ctx_map_.end(); ++iter) {
      const ObTableLoadUniqueKey &key = iter->first;
      ObTableLoadTableCtx *table_ctx = iter->second;
      if (OB_FAIL(table_ctx_array.push_back(table_ctx))) {
        LOG_WARN("fail to push back", KR(ret), K(key));
      } else {
        table_ctx->inc_ref_count();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < table_ctx_array.count(); ++i) {
        ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
        put_table_ctx(table_ctx);
      }
      table_ctx_array.reset();
    }
  }
  return ret;
}

int ObTableLoadManager::get_table_ctx(const ObTableLoadUniqueKey &key,
                                      ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else {
    table_ctx = nullptr;
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(table_ctx_map_.get_refactored(key, table_ctx))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(key));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      table_ctx->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadManager::get_table_ctx_by_table_id(uint64_t table_id,
                                                  ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else {
    table_ctx = nullptr;
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(table_ctx_index_map_.get_refactored(table_id, table_ctx))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(table_id));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      table_ctx->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadManager::get_inactive_table_ctx_list(
  ObIArray<ObTableLoadTableCtx *> &table_ctx_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else {
    table_ctx_array.reset();
    auto fn = [&ret, &table_ctx_array](
                const HashMapPair<ObTableLoadUniqueKey, ObTableLoadTableCtx *> &entry) -> int {
      ObTableLoadTableCtx *table_ctx = entry.second;
      OB_ASSERT(nullptr != table_ctx);
      if (table_ctx->get_ref_count() > 0) {
        // skip active table ctx
      } else if (OB_FAIL(table_ctx_array.push_back(table_ctx))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        table_ctx->inc_ref_count();
      }
      return ret;
    };
    {
      obsys::ObRLockGuard guard(rwlock_);
      if (OB_FAIL(table_ctx_map_.foreach_refactored(fn))) {
        LOG_WARN("fail to foreach map", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < table_ctx_array.count(); ++i) {
        ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
        put_table_ctx(table_ctx);
      }
      table_ctx_array.reset();
    }
  }
  return ret;
}

void ObTableLoadManager::put_table_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_ctx));
  } else {
    // not release here
    OB_ASSERT(table_ctx->dec_ref_count() >= 0);
  }
}

int64_t ObTableLoadManager::get_table_ctx_count() const
{
  obsys::ObRLockGuard guard(rwlock_);
  return table_ctx_map_.size();
}

int ObTableLoadManager::add_dirty_list(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_ctx));
  } else if (OB_UNLIKELY(table_ctx->is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dirty table ctx", KR(ret), KPC(table_ctx));
  } else {
    table_ctx->set_dirty();
    ObMutexGuard guard(mutex_);
    OB_ASSERT(dirty_list_.add_last(table_ctx));
  }
  return ret;
}

int ObTableLoadManager::get_releasable_table_ctx_list(
  ObIArray<ObTableLoadTableCtx *> &table_ctx_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else {
    table_ctx_array.reset();
    ObMutexGuard guard(mutex_);
    ObTableLoadTableCtx *table_ctx = nullptr;
    DLIST_FOREACH_REMOVESAFE(table_ctx, dirty_list_)
    {
      if (table_ctx->get_ref_count() > 0) {
        // wait all task exit
      } else if (OB_FAIL(table_ctx_array.push_back(table_ctx))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        OB_ASSERT(OB_NOT_NULL(dirty_list_.remove(table_ctx)));
      }
    }
  }
  return ret;
}

int64_t ObTableLoadManager::get_dirty_list_count() const
{
  ObMutexGuard guard(mutex_);
  return dirty_list_.get_size();
}

} // namespace observer
} // namespace oceanbase
