// Copyright (c) 2025 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_TTL_MLOG_CACHE_H_
#define OB_STORAGE_COMPACTION_TTL_MLOG_CACHE_H_
#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
namespace oceanbase
{
namespace storage
{
using MlogPurgeScnMap = common::hash::ObHashMap<int64_t, int64_t>; // mlog id -> last purge scn

class ObTenantMlogPurgeScnMapCache final
{
public:
  static constexpr int64_t MIN_REFRESH_INTERVAL_US = 30 * 1000 * 1000L; // 30s
  ObTenantMlogPurgeScnMapCache()
    : map_(),
      read_snapshot_(-1)
  {}
  DISABLE_COPY_ASSIGN(ObTenantMlogPurgeScnMapCache);
  int refresh_or_init(const int64_t read_snapshot);
  int get_purge_scn(const uint64_t mlog_id, int64_t &purge_scn) const;
  int64_t get_read_snapshot() const { return read_snapshot_; }
  bool is_initialized() const { return map_.created(); }
private:
  MlogPurgeScnMap map_;
  int64_t read_snapshot_;
};

class ObMLogPurgeInfoHelper
{
public:
  static int get_mlog_purge_scn(
    const uint64_t mlog_id,
    const int64_t read_snapshot,
    int64_t &last_purge_scn);
  static int get_recent_tenant_mlog_purge_scns(
    const int64_t read_snapshot,
    MlogPurgeScnMap &mlog_purge_scn_map);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TTL_MLOG_CACHE_H_
