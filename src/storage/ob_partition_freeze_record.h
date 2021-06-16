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

#ifndef OCEANBASE_STORAGE_PARTITION_FREEZE_RECORD_
#define OCEANBASE_STORAGE_PARTITION_FREEZE_RECORD_

#include <stdint.h>

#include "lib/utility/ob_print_utils.h"
#include "storage/ob_saved_storage_info_v2.h"
#include "storage/ob_i_table.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
}

namespace memtable {
class ObMemtable;
class ObIMemtable;
}  // namespace memtable

namespace storage {
class ObIPartitionGroup;

class ObFreezeRecord {
public:
  // check whether the memtable is available to execute a new minor freeze
  // the function is used to guarantee there is only one ongoing minor freeze
  bool available() const;
  // return the snapshot version if a minor freeze is ongoing
  int get_snapshot_version(int64_t& snapshot_version) const;
  // return the saved storage_info
  const ObSavedStorageInfoV2& get_saved_storage_info() const
  {
    return storage_info_;
  }
  // return whether is emergency
  bool is_emergency() const
  {
    return emergency_;
  }
  // return the protection clock of the new memtable
  int get_active_protection_clock(int64_t& active_protection_clock) const;
  // clear the freeze record to notify the success or fail of the minor freeze
  void clear();
  int submit_freeze(memtable::ObMemtable& frozen_memtable, const int64_t freeze_ts);
  int submit_new_active_memtable(ObTableHandle& handle);
  bool need_raise_memstore(const int64_t trans_version) const;

  int set_freeze_upper_limit(const int64_t upper_limit);
  void clear_freeze_upper_limit()
  {
    upper_limit_ = INT64_MAX;
  }

  // <--------------------  3.0 merge uncommitted transactions ----------------------->

  // return the frozen memtable which need to mark dirty or the active memtable
  // which is newly created. both of the memtable returned may be NULL if the
  // minor freeze is not ready of finished
  // NB: the function inside hold reference counts.
  int get_pending_frozen_memtable(memtable::ObMemtable*& frozen_memtable, memtable::ObMemtable*& active_memtable);
  int get_pending_frozen_memtable(memtable::ObMemtable*& frozen_memtable);

  // callback when the transaction is marked dirty
  void dirty_trans_marked(memtable::ObMemtable* const memtable, const int64_t cb_cnt, const bool finish,
      const int64_t applied_log_ts, bool& cleared);
  int64_t get_freeze_ts() const;

  ObFreezeRecord();
  ~ObFreezeRecord();
  ObFreezeRecord(const ObFreezeRecord&) = delete;
  ObFreezeRecord& operator=(const ObFreezeRecord&) = delete;

  static const int64_t OB_INVALID_FREEZE_TS = INT64_MAX;

  TO_STRING_KV(K(snapshot_version_), K(emergency_), K(active_protection_clock_), K(storage_info_), K(is_valid_));

private:
  int64_t snapshot_version_;
  bool emergency_;
  int64_t active_protection_clock_;
  ObSavedStorageInfoV2 storage_info_;
  int64_t upper_limit_;
  bool is_valid_;
  ObTableHandle frozen_memtable_handle_;
  ObTableHandle new_memtable_handle_;
  common::ObSpinLock lock_;
  int64_t freeze_ts_;
};
}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_PARTITION_FREEZE_RECORD_ */
