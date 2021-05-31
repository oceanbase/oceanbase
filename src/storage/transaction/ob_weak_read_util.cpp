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

#include "ob_weak_read_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"  // ObTenantConfigGuard
#include "share/ob_cluster_version.h"           // GET_MIN_CLUSTER_VERSION
#include <algorithm>
#include <stdarg.h>
#include <stdint.h>
namespace oceanbase {
using namespace common;
namespace transaction {
// 1. follower if readable depend on follower readable snapshot version and weak read cluster version
//    and correspond to clog keepalive msg interval and weak read version refresh interval respectively
// 2. meanwhile weak read cluster version depond on follower readable snapshot version and max stable time
// 3. so keepalive msg interval should bigger than weak read version refresh interval
// 4. keepalive msg interval set DEAULT  if weak read version refresh interval
int64_t ObWeakReadUtil::replica_keepalive_interval()
{
  int64_t interval = 0;
  int64_t weak_read_refresh_interval = GCONF.weak_read_version_refresh_interval;
  if (weak_read_refresh_interval <= 0 || weak_read_refresh_interval > DEFAULT_REPLICA_KEEPALIVE_INTERVAL) {
    interval = DEFAULT_REPLICA_KEEPALIVE_INTERVAL;
  } else {
    interval = weak_read_refresh_interval;
  }
  return interval;
}

// following bellow, weak read version calulate as 'now - max_stale_time', to support weak read version increase
// 1. no partitions in server
// 2. all partitions offline
// 3. all partitions delay too much or in invalid status
// 4. all partitions in migrating and readable snapshot version delay more than 500ms
int64_t ObWeakReadUtil::generate_min_weak_read_version(const uint64_t tenant_id)
{
  int64_t base_version_when_no_valid_partition = 0;
  int64_t max_stale_time = 0;
  bool tenant_config_exist = false;
  // generating min weak version version should statisfy following constraint
  // 1. not smaller than max_stale_time_for_weak_consistency - DEFAULT_MAX_STALE_BUFFER_TIME
  // 2. not bigger than readable snapshot version
  int64_t buffer_time = std::max(static_cast<int64_t>(GCONF.weak_read_version_refresh_interval),
      static_cast<int64_t>(DEFAULT_MAX_STALE_BUFFER_TIME));

  OTC_MGR.read_tenant_config(
      tenant_id,
      oceanbase::omt::ObTenantConfigMgr::default_fallback_tenant_id(),
      /* success */
      [buffer_time, &tenant_config_exist, &max_stale_time](const omt::ObTenantConfig& config) mutable {
        max_stale_time = config.max_stale_time_for_weak_consistency - buffer_time;
        tenant_config_exist = true;
      },
      /* failure */
      [buffer_time, &tenant_config_exist, &max_stale_time]() mutable {
        max_stale_time = DEFAULT_MAX_STALE_TIME_FOR_WEAK_CONSISTENCY - buffer_time;
        tenant_config_exist = false;
      });

  max_stale_time = std::max(max_stale_time, static_cast<int64_t>(DEFAULT_REPLICA_KEEPALIVE_INTERVAL));
  base_version_when_no_valid_partition = ObTimeUtility::current_time() - max_stale_time;

  if ((!tenant_config_exist) && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
    TRANS_LOG(WARN,
        "tenant not exist when generate min weak read version, use default max stale time instead",
        K(tenant_id),
        K(base_version_when_no_valid_partition),
        K(lbt()));
  }

  return base_version_when_no_valid_partition;
}

bool ObWeakReadUtil::enable_monotonic_weak_read(const uint64_t tenant_id)
{
  bool is_monotonic = false;

  OTC_MGR.read_tenant_config(
      tenant_id,
      oceanbase::omt::ObTenantConfigMgr::default_fallback_tenant_id(),
      /* success */
      [&is_monotonic](const omt::ObTenantConfig& config) mutable { is_monotonic = config.enable_monotonic_weak_read; },
      /* failure */
      [tenant_id, &is_monotonic]() mutable {
        is_monotonic = true;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
          TRANS_LOG(WARN,
              "tenant not exist when check enable monotonic weak read, use true as default instead",
              K(tenant_id),
              K(is_monotonic),
              K(lbt()));
        }
      });

  return is_monotonic;
}

int64_t ObWeakReadUtil::max_stale_time_for_weak_consistency(const uint64_t tenant_id, int64_t ignore_warn)
{
  int64_t max_stale_time = 0;

  OTC_MGR.read_tenant_config(
      tenant_id,
      oceanbase::omt::ObTenantConfigMgr::default_fallback_tenant_id(),
      /* success */
      [&max_stale_time](
          const omt::ObTenantConfig& config) mutable { max_stale_time = config.max_stale_time_for_weak_consistency; },
      /* failure */
      [tenant_id, ignore_warn, &max_stale_time]() mutable {
        max_stale_time = DEFAULT_MAX_STALE_TIME_FOR_WEAK_CONSISTENCY;
        if (IGNORE_TENANT_EXIST_WARN != ignore_warn && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
          TRANS_LOG(WARN,
              "tenant not exist when get max stale time for weak consistency,"
              " use default max stale time instead",
              K(tenant_id),
              K(max_stale_time),
              K(lbt()));
        }
      });

  return max_stale_time;
}

// weak_read_version_refresh_interval value range [0,)
// wrs stop if 0 is set
bool ObWeakReadUtil::check_weak_read_service_available()
{
  bool ret = true;
  int64_t weak_read_version_refresh_interval = GCONF.weak_read_version_refresh_interval;
  if (OB_UNLIKELY(weak_read_version_refresh_interval <= 0)) {
    ret = false;
  } else {
    ret = true;
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
