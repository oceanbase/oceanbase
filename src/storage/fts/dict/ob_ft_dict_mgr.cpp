/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{
namespace
{
struct ObTableIdsCollector
{
  ObTableIdsCollector(uint64_t *buf, const int64_t cap, int64_t &cnt)
      : buf_(buf), cap_(cap), cnt_(cnt) {}
  int operator()(const uint64_t &key) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(cnt_ >= cap_)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      buf_[cnt_++] = key;
    }
    return ret;
  }
  uint64_t *buf_;
  int64_t cap_;
  int64_t &cnt_;
};
}  // namespace

ObFTDictMgr::ObFTDictMgr()
    : cache_lock_(common::ObLatchIds::FT_DICT_ROW_SCN_CACHE_LOCK),
      row_scn_cache_(),
      is_started_(false),
      is_inited_(false)
{
}

ObFTDictMgr::~ObFTDictMgr()
{
  destroy();
}

int ObFTDictMgr::mtl_init(ObFTDictMgr *&mgr)
{
  return mgr->init();
}

int ObFTDictMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFTDictMgr init twice", K(ret));
  } else if (OB_FAIL(slot_mgr_.init())) {
    LOG_WARN("fail to init slot mgr", K(ret));
  } else if (OB_FAIL(refresh_task_mgr_.init())) {
    LOG_WARN("fail to init refresh task mgr", K(ret));
  } else if (OB_FAIL(access_cache_mgr_.init())) {
    LOG_WARN("fail to init access row scn cache mgr", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    access_cache_mgr_.destroy();
    refresh_task_mgr_.destroy();
    slot_mgr_.destroy();
  }
  return ret;
}

int ObFTDictMgr::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTDictMgr not inited", K(ret));
  } else if (is_started_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFTDictMgr already started", K(ret));
  } else if (is_meta_tenant(MTL_ID())) {
    is_started_ = true;
  } else if (OB_FAIL(refresh_task_mgr_.start())) {
    LOG_WARN("fail to start refresh task mgr", K(ret));
  } else if (OB_FAIL(access_cache_mgr_.start())) {
    LOG_WARN("fail to start access row scn cache mgr", K(ret));
  } else if (OB_FAIL(refresh_task_mgr_.schedule_task(ObFTDictRefreshTaskMgr::REFRESH_INTERVAL, true))) {
    LOG_WARN("fail to schedule refresh task", K(ret));
  } else if (OB_FAIL(access_cache_mgr_.schedule_task(ObFTAccessRowScnCacheTaskMgr::CONSUME_INTERVAL, true))) {
    LOG_WARN("fail to schedule access row scn cache task", K(ret));
  } else {
    is_started_ = true;
  }
  if (OB_FAIL(ret)) {
    refresh_task_mgr_.cancel_task();
    refresh_task_mgr_.stop();
    access_cache_mgr_.stop();
  }
  return ret;
}

void ObFTDictMgr::stop()
{
  if (is_started_) {
    refresh_task_mgr_.cancel_task();
    access_cache_mgr_.cancel_task();
    refresh_task_mgr_.stop();
    access_cache_mgr_.stop();
    is_started_ = false;
  }
}

void ObFTDictMgr::wait()
{
  refresh_task_mgr_.wait();
  access_cache_mgr_.wait();
}

void ObFTDictMgr::push_access_row_scn_cache_task(const uint64_t table_id)
{
  access_cache_mgr_.push(table_id);
}

void ObFTDictMgr::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    access_cache_mgr_.destroy();
    refresh_task_mgr_.destroy();
    slot_mgr_.destroy();
    row_scn_cache_.reset();
    is_started_ = false;
    is_inited_ = false;
  }
}

int ObFTDictMgr::get_table_snapshot(const uint64_t table_id, DictTableSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTDictMgr not inited", K(ret));
  } else {
    common::ObSpinLockGuard guard(cache_lock_);
    if (OB_FAIL(row_scn_cache_.get_handle(table_id, snapshot))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get handle from row_scn_cache failed", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObFTDictMgr::update_table_snapshot(const uint64_t table_id, DictTableSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTDictMgr not inited", K(ret));
  } else {
    common::ObSpinLockGuard guard(cache_lock_);
    if (OB_FAIL(row_scn_cache_.put_handle(table_id, snapshot))) {
      LOG_WARN("put handle to row_scn_cache failed", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObFTDictMgr::collect_table_ids(uint64_t *table_ids, int64_t &table_id_cnt) const
{
  int ret = OB_SUCCESS;
  table_id_cnt = 0;
  if (OB_ISNULL(table_ids)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_ids is null", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTDictMgr not inited", K(ret));
  } else {
    common::ObSpinLockGuard guard(cache_lock_);
    if (OB_FAIL(row_scn_cache_.for_each_key(
            ObTableIdsCollector(table_ids, ROW_SCN_CACHE_SIZE, table_id_cnt)))) {
      LOG_WARN("fail to iterate row_scn_cache", K(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
