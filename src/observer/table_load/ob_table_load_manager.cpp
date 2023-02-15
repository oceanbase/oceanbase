// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

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
 * TableCtxHandle
 */

ObTableLoadManager::TableCtxHandle::TableCtxHandle()
  : table_ctx_(nullptr)
{
}

ObTableLoadManager::TableCtxHandle::TableCtxHandle(ObTableLoadTableCtx *table_ctx)
  : table_ctx_(table_ctx)
{
  if (OB_NOT_NULL(table_ctx_)) {
    table_ctx_->inc_ref_count();
  }
}

ObTableLoadManager::TableCtxHandle::TableCtxHandle(const TableCtxHandle &other)
  : table_ctx_(other.table_ctx_)
{
  if (OB_NOT_NULL(table_ctx_)) {
    table_ctx_->inc_ref_count();
  }
}

ObTableLoadManager::TableCtxHandle::~TableCtxHandle()
{
  reset();
}

void ObTableLoadManager::TableCtxHandle::reset()
{
  if (nullptr != table_ctx_) {
    table_ctx_->dec_ref_count();
    table_ctx_ = nullptr;
  }
}

void ObTableLoadManager::TableCtxHandle::set(ObTableLoadTableCtx *table_ctx)
{
  reset();
  if (OB_NOT_NULL(table_ctx)) {
    table_ctx_ = table_ctx;
    table_ctx_->inc_ref_count();
  }
}

ObTableLoadManager::TableCtxHandle &ObTableLoadManager::TableCtxHandle::operator=(
  const TableCtxHandle &other)
{
  if (this != &other) {
    set(other.table_ctx_);
  }
  return *this;
}

/**
 * ObTableLoadManager
 */

ObTableLoadManager::ObTableLoadManager()
  : is_inited_(false)
{
}

ObTableLoadManager::~ObTableLoadManager()
{
}

int ObTableLoadManager::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadManager init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(
          table_ctx_map_.create(bucket_num, "TLD_TableCtxMap", "TLD_TableCtxMap", MTL_ID()))) {
      LOG_WARN("fail to create table ctx map", KR(ret), K(bucket_num));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadManager::add_table_ctx(uint64_t table_id, ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || nullptr == table_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id), KP(table_ctx));
  } else if (OB_UNLIKELY(table_ctx->is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dirty table ctx", KR(ret), KP(table_ctx));
  } else {
    TableCtxHandle handle(table_ctx); // inc_ref_count
    if (OB_FAIL(table_ctx_map_.set_refactored(table_id, handle))) {
      if (OB_UNLIKELY(OB_HASH_EXIST != ret)) {
        LOG_WARN("fail to set refactored", KR(ret), K(table_id));
      } else {
        ret = OB_ENTRY_EXIST;
      }
    } else {
      handle.take(); // keep reference count
    }
  }
  return ret;
}

int ObTableLoadManager::remove_table_ctx(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_id));
  } else {
    TableCtxHandle handle;
    // remove from map
    if (OB_FAIL(table_ctx_map_.erase_refactored(table_id, &handle))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(table_id));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
    // add to dirty list
    else {
      ObTableLoadTableCtx *table_ctx = handle.get();
      table_ctx->set_dirty();
      ObMutexGuard guard(mutex_);
      OB_ASSERT(dirty_list_.add_last(table_ctx));
    }
  }
  return ret;
}

int ObTableLoadManager::get_table_ctx(uint64_t table_id, ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadManager not init", KR(ret), KP(this));
  } else {
    table_ctx = nullptr;
    TableCtxHandle handle;
    if (OB_FAIL(table_ctx_map_.get_refactored(table_id, handle))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(table_id));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      table_ctx = handle.take(); // keep reference count
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
    auto fn = [&ret, &table_ctx_array](HashMapPair<uint64_t, TableCtxHandle> &entry) -> int {
      TableCtxHandle handle = entry.second; // inc_ref_count
      ObTableLoadTableCtx *table_ctx = handle.get();
      OB_ASSERT(nullptr != table_ctx);
      if (table_ctx->get_ref_count() > 2) { // 2 = map + handle
        // skip active table ctx
      } else if (OB_FAIL(table_ctx_array.push_back(table_ctx))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        handle.take(); // keep reference count
      }
      return ret;
    };
    if (OB_FAIL(table_ctx_map_.foreach_refactored(fn))) {
      LOG_WARN("fail to foreach map", KR(ret));
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
    OB_ASSERT(table_ctx->dec_ref_count() >= 0);
  }
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

} // namespace observer
} // namespace oceanbase
