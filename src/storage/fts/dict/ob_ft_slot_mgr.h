/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_SLOT_MGR_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_SLOT_MGR_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace storage
{

class ObFTSlotMgr
{
public:
  ObFTSlotMgr();
  ~ObFTSlotMgr();

  int init();
  void destroy();

  int acquire_slot(const uint64_t table_id, int64_t &slot_idx);
  void release_slot(const int64_t slot_idx);
  int wait_build_complete(const uint64_t table_id, const int64_t slot_idx);

private:
  int64_t try_get_used_slot_fast(const uint64_t table_id);
  int try_acquire_slot(const uint64_t table_id, int64_t &slot_idx);
  int wait_one_round(const int64_t retry_count, const uint64_t table_id, const bool wait_for_slot, int64_t &wait_interval);

private:
  static constexpr int64_t WAIT_INTERVAL_MIN_US = 100;
  static constexpr int64_t WAIT_INTERVAL_MAX_US = 100 * 1000;
  static constexpr int64_t MAX_CONCURRENT_BUILD_COUNT = 11;

private:
  uint64_t slots_[MAX_CONCURRENT_BUILD_COUNT];
  common::ObSpinLock slots_lock_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObFTSlotMgr);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_SLOT_MGR_H_
