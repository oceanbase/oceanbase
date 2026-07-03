/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_slot_mgr.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace storage
{

ObFTSlotMgr::ObFTSlotMgr()
  : slots_lock_(common::ObLatchIds::FT_DICT_BUILD_SLOTS_LOCK),
  is_inited_(false)
{
  memset(slots_, 0xFF, sizeof(slots_));
}

ObFTSlotMgr::~ObFTSlotMgr()
{
  destroy();
}

int ObFTSlotMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFTSlotMgr init twice", K(ret));
  } else {
    memset(slots_, 0xFF, sizeof(slots_));
    is_inited_ = true;
  }
  return ret;
}

void ObFTSlotMgr::destroy()
{
  if (is_inited_) {
    memset(slots_, 0xFF, sizeof(slots_));
    is_inited_ = false;
  }
}

int64_t ObFTSlotMgr::try_get_used_slot_fast(const uint64_t table_id)
{
  int64_t slot_idx = -1;
  for (int64_t i = 0; i < MAX_CONCURRENT_BUILD_COUNT; ++i) {
    if (table_id == ATOMIC_LOAD(&slots_[i])) {
      slot_idx = i;
      break;
    }
  }
  return slot_idx;
}

int ObFTSlotMgr::try_acquire_slot(const uint64_t table_id, int64_t &slot_idx)
{
  int ret = OB_SUCCESS;
  slot_idx = -1;
  common::ObSpinLockGuard guard(slots_lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_CONCURRENT_BUILD_COUNT; ++i) {
    uint64_t slot_value = ATOMIC_LOAD(&slots_[i]);
    if (table_id == slot_value) {
      slot_idx = i;
      ret = OB_EAGAIN;
    } else if (OB_INVALID_ID == slot_value && -1 == slot_idx) {
      slot_idx = i;
    }
  }
  if (OB_SUCC(ret) && -1 != slot_idx) {
    ATOMIC_STORE(&slots_[slot_idx], table_id);
  }
  return ret;
}

int ObFTSlotMgr::wait_one_round(const int64_t retry_count, const uint64_t table_id,
                                const bool wait_for_slot, int64_t &wait_interval)
{
  int ret = OB_SUCCESS;
  const char *log_prefix = wait_for_slot ? "available build slot" : "build completion";
  if (retry_count % 10 == 0) {
    LOG_WARN("still waiting for %s after multiple retries", K(log_prefix), K(ret), K(retry_count), K(table_id), K(wait_interval));
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("query cancelled while waiting for %s", K(log_prefix), K(ret), K(table_id), K(retry_count));
    }
  }
  if (OB_SUCC(ret)) {
    ob_usleep(wait_interval);
    wait_interval = OB_MIN(wait_interval * 2, WAIT_INTERVAL_MAX_US);
  }
  return ret;
}

int ObFTSlotMgr::acquire_slot(const uint64_t table_id, int64_t &slot_idx)
{
  int ret = OB_SUCCESS;
  slot_idx = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTSlotMgr not inited", K(ret));
  } else if (0 <= (slot_idx = try_get_used_slot_fast(table_id))) {
    ret = OB_EAGAIN;
  } else {
    int64_t retry_count = 0;
    int64_t wait_interval = WAIT_INTERVAL_MIN_US;
    while (OB_SUCC(ret) && -1 == slot_idx) {
      if (OB_FAIL(try_acquire_slot(table_id, slot_idx)) && OB_EAGAIN != ret) {
        LOG_WARN("fail to acquire slot", K(ret), K(table_id));
      } else if (-1 != slot_idx) {
        LOG_DEBUG("success to acquire a used or empty slot", K(ret), K(table_id), K(slot_idx));
      } else if (FALSE_IT(++retry_count)) {
      } else if (OB_FAIL(wait_one_round(retry_count, table_id, true, wait_interval))) {
        LOG_WARN("fail to wait for available build slot", K(ret), K(table_id), K(slot_idx), K(retry_count));
      }
    }
  }
  return ret;
}

void ObFTSlotMgr::release_slot(const int64_t slot_idx)
{
  if (OB_LIKELY(slot_idx >= 0 && slot_idx < MAX_CONCURRENT_BUILD_COUNT)) {
    ATOMIC_STORE(&slots_[slot_idx], OB_INVALID_ID);
  }
}

int ObFTSlotMgr::wait_build_complete(const uint64_t table_id, const int64_t slot_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTSlotMgr not inited", K(ret));
  } else {
    int64_t wait_interval = WAIT_INTERVAL_MIN_US;
    int64_t retry_count = 0;
    while (OB_SUCC(ret) && table_id == ATOMIC_LOAD(&slots_[slot_idx])) {
      ++retry_count;
      if (OB_FAIL(wait_one_round(retry_count, table_id, false, wait_interval))) {
        LOG_WARN("fail to wait for build completion", K(ret), K(table_id), K(slot_idx), K(retry_count));
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
