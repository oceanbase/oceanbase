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
#include <algorithm>
#include <stdarg.h>
#include <stdint.h>
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/ob_timestamp_service.h"
namespace oceanbase
{
using namespace common;
using namespace share;
namespace transaction
{
// 1. follower if readable depend on follower readable snapshot version and weak read cluster version
//    and correspond to clog keepalive msg interval and weak read version refresh interval respectively
// 2. meanwhile weak read cluster version depond on follower readable snapshot version and max stable time
// 3. so keepalive msg interval should bigger than weak read version refresh interval
// 4. keepalive msg interval set DEAULT  if weak read version refresh interval
int64_t ObWeakReadUtil::replica_keepalive_interval()
{
  int64_t interval = 0;
  int64_t weak_read_refresh_interval = GCONF.weak_read_version_refresh_interval;
  if (weak_read_refresh_interval <= 0
      || weak_read_refresh_interval > DEFAULT_REPLICA_KEEPALIVE_INTERVAL) {
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
int ObWeakReadUtil::generate_min_weak_read_version(const uint64_t tenant_id, SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t max_stale_time = 0;
  bool tenant_config_exist = false;
  // generating min weak version version should statisfy following constraint
  // 1. not smaller than max_stale_time_for_weak_consistency - DEFAULT_MAX_STALE_BUFFER_TIME
  // 2. not bigger than readable snapshot version
  int64_t buffer_time = std::max(
          std::min(static_cast<int64_t>(GCONF.weak_read_version_refresh_interval),
                   static_cast<int64_t>(DEFAULT_REPLICA_KEEPALIVE_INTERVAL)),
          static_cast<int64_t>(DEFAULT_MAX_STALE_BUFFER_TIME));

  OTC_MGR.read_tenant_config(
      tenant_id,
      oceanbase::omt::ObTenantConfigMgr::default_fallback_tenant_id(),
      /* success */ [buffer_time, &tenant_config_exist, &max_stale_time](const omt::ObTenantConfig &config) mutable {
      if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
        max_stale_time = config.max_stale_time_for_weak_consistency - buffer_time;
      } else {
      //standby, restore, invalid
        max_stale_time = config.max_stale_time_for_weak_consistency + transaction::ObTimestampService::PREALLOCATE_RANGE_FOR_SWITHOVER - buffer_time;
      }
        tenant_config_exist = true;
      },
      /* failure */ [buffer_time, &tenant_config_exist, &max_stale_time]() mutable {
        max_stale_time = DEFAULT_MAX_STALE_TIME_FOR_WEAK_CONSISTENCY - buffer_time;
        tenant_config_exist = false;
      }
  );

  max_stale_time = std::max(max_stale_time, static_cast<int64_t>(DEFAULT_REPLICA_KEEPALIVE_INTERVAL));
  SCN tmp_scn;
  if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, NULL, tmp_scn))) {
    TRANS_LOG(WARN, "get gts cache error", K(ret), K(tenant_id));
  } else {
    // the unit of max_stale_time is us，we should change to ns
    scn.convert_from_ts(tmp_scn.convert_to_ts() - max_stale_time);
  }

  if ((!tenant_config_exist) && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tenant not exist when generate min weak read version, use default max stale time instead",
        K(tenant_id), K(tmp_scn), K(lbt()));
  }

  return ret;
}

bool ObWeakReadUtil::enable_monotonic_weak_read(const uint64_t tenant_id)
{
  bool is_monotonic = false;

  OTC_MGR.read_tenant_config(
      tenant_id,
      oceanbase::omt::ObTenantConfigMgr::default_fallback_tenant_id(),
      /* success */ [&is_monotonic](const omt::ObTenantConfig &config) mutable {
        is_monotonic = config.enable_monotonic_weak_read;
      },
      /* failure */ [tenant_id, &is_monotonic]() mutable {
        is_monotonic = true;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
        TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tenant not exist when check enable monotonic weak read, use true as default instead",
                  K(tenant_id), K(is_monotonic), K(lbt()));
        }
      }
  );

  return is_monotonic;
}

int64_t ObWeakReadUtil::max_stale_time_for_weak_consistency(const uint64_t tenant_id, int64_t ignore_warn)
{
  int64_t max_stale_time = 0;

  OTC_MGR.read_tenant_config(
      tenant_id,
      oceanbase::omt::ObTenantConfigMgr::default_fallback_tenant_id(),
      /* success */ [&max_stale_time](const omt::ObTenantConfig &config) mutable {
        max_stale_time = config.max_stale_time_for_weak_consistency;
      },
      /* failure */ [tenant_id, ignore_warn, &max_stale_time]() mutable {
        max_stale_time = DEFAULT_MAX_STALE_TIME_FOR_WEAK_CONSISTENCY;
        if (IGNORE_TENANT_EXIST_WARN != ignore_warn && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
        TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tenant not exist when get max stale time for weak consistency,"
                  " use default max stale time instead",
                  K(tenant_id), K(max_stale_time), K(lbt()));
        }
      }
  );

  return max_stale_time;
}

bool ObWeakReadUtil::check_weak_read_service_available()
{
  return true;
}

}// transaction
}// oceanbase
