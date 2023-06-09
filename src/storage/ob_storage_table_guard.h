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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_TABLE_GUARD
#define OCEANBASE_STORAGE_OB_STORAGE_TABLE_GUARD

#include <stdint.h>
#include "share/scn.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtable;
class ObIMemtable;
}

namespace storage
{
class ObTablet;
class ObITable;
struct ObStoreCtx;
class ObRelativeTable;
class ObTableStoreIterator;

class ObStorageTableGuard
{
public:
  ObStorageTableGuard(
      ObTablet *tablet,
      ObStoreCtx &store_ctx,
      const bool need_control_mem,
      const bool for_replay = false,
      const share::SCN replay_scn = share::SCN(),
      const bool for_multi_source_data = false);
  ~ObStorageTableGuard();

  ObStorageTableGuard(const ObStorageTableGuard&) = delete;
  ObStorageTableGuard &operator=(const ObStorageTableGuard&) = delete;
public:
  int refresh_and_protect_table(ObRelativeTable &relative_table);
  int refresh_and_protect_memtable();
  int get_memtable_for_replay(memtable::ObIMemtable *&memtable);
private:
  void reset();
  void double_check_inc_write_ref(
      const uint32_t old_freeze_flag,
      const bool is_tablet_freeze,
      memtable::ObIMemtable *memtable,
      bool &bool_ret);
  int check_freeze_to_inc_write_ref(ObITable *table, bool &bool_ret, bool &for_replace_tablet_meta);
  bool need_to_refresh_table(ObTableStoreIterator &iter);
  void check_if_need_log_(bool &need_log, bool &need_log_error);
private:
  static const int64_t LOG_INTERVAL_US = 10 * 1000 * 1000;  // 10s
  static const int64_t LOG_ERROR_INTERVAL_US = 60 * 1000 * 1000;  // 1min
  static const int64_t GET_TS_INTERVAL = 10 * 1000;
  static const int64_t SLEEP_INTERVAL_PER_TIME = 20 * 1000; // 20ms

  ObTablet *tablet_;
  ObStoreCtx &store_ctx_;
  bool need_control_mem_;
  memtable::ObIMemtable *memtable_;
  int64_t retry_count_;
  int64_t last_ts_;
  // record write latency
  int64_t init_ts_;
  bool for_replay_;
  share::SCN replay_scn_;
  bool for_multi_source_data_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_STORAGE_TABLE_GUARD
