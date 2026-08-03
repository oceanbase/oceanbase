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

#ifndef OCEANBASE_LOGSERVICE_SERVER_PRIORITY_H_
#define OCEANBASE_LOGSERVICE_SERVER_PRIORITY_H_

#include "common/ob_region.h"               // ObRegion
#include "common/ob_zone.h"                 // ObZone
#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "lib/lock/ob_small_spin_lock.h"    // ObByteLock
#include "lib/ob_define.h"                  // MAX_ZONE_LENGTH
#include "share/ob_define.h"                // ObReplicaType
#include "share/ob_errno.h"

namespace oceanbase
{
namespace logservice
{
// Unified fetch priority for server ordering (region / zone_priority tier; int).
// The smaller the value, the higher the priority.
static constexpr int REGION_PRIORITY_UNKNOWN = 99999;

// Log-route / CDC server ordering priority (smaller = higher). Same representation as int.
using FetchPriority = int;

// Replica type priority
// The smaller the value, the higher the priority
//
// The priority is from highest to lowest as follows: L > R > F > OTHER
// where F is a fully functional copy, R is a read-only copy, L is a logged copy
// OTHER is the other type of replica, with the lowest default level
enum ReplicaPriority
{
  REPLICA_PRIORITY_UNKNOWN = -1,
  REPLICA_PRIORITY_LOGONLY = 0,
  REPLICA_PRIORITY_READONLY = 1,
  REPLICA_PRIORITY_FULL = 2,
  REPLICA_PRIORITY_OTHER = 3,
  REPLICA_PRIORITY_MAX
};
const char *print_replica_priority(ReplicaPriority type);

// Get replica priority based on replica type
int get_replica_priority(const common::ObReplicaType type,
    ReplicaPriority &priority);

// Unified CDC log fetch priority for server ordering:
// primary_zone-like zone_priority string when configured, else preferred region HIGH/LOW.
class ObCdcLogFetchPriority
{
public:
  ObCdcLogFetchPriority();
  ~ObCdcLogFetchPriority();
  void destroy();
  // Empty or whitespace-only str => not configured (OB_SUCCESS).
  int init_zone_priority(const char *zone_priority_str);
  bool is_configured() const { return configured_; }
  int64_t get_default_priority_level() const { return default_priority_level_; }
  // Smaller value = higher fetch priority (same as existing region HIGH/LOW ordering).
  FetchPriority get_zone_priority(const common::ObZone &zone) const;
  int set_assign_region(const common::ObRegion &prefer_region);
  int get_assign_region(common::ObRegion &prefer_region);
  // zone_priority tiers when configured, else region HIGH/LOW vs assign region (ignore case).
  int get_priority(const common::ObZone &zone, const common::ObRegion &region,
      FetchPriority &priority);

private:
  static bool is_space_(const char c);
  static const char *ltrim_(const char *p, const char *end);
  static const char *rtrim_(const char *p, const char *end);
  int add_zone_token_(const char *tok_start, const char *tok_end, const int64_t tier_idx);
  bool is_assign_region_(const common::ObRegion &region);
  bool is_default_zone_(const common::ObZone &zone);

private:
  bool configured_;
  int64_t default_priority_level_;
  bool map_inited_;
  common::ObLinearHashMap<common::ObZone, int64_t> zone_tier_map_;
  common::ObRegion assign_region_;
  common::ObByteLock assign_lock_;
};

} // namespace logservice
} // namespace oceanbase

#endif
