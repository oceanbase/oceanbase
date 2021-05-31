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

#define USING_LOG_PREFIX RS
#include "ob_root_major_freeze_v2.h"
#include "rootserver/ob_freeze_info_manager.h"
#include "rootserver/ob_zone_manager.h"
#include "lib/profile/ob_trace_id.h"
#include "share/config/ob_server_config.h"
#include "lib/net/ob_addr.h"

namespace oceanbase {
using namespace common;
using namespace rootserver;

int ObRootMajorFreezeV2::init(ObZoneManager& zone_manager)
{
  int ret = OB_SUCCESS;
  zone_manager_ = &zone_manager;
  inited_ = true;
  return ret;
}

// tgt_try_frozen_version = 0 : user triggered major freeze or RS periodically trigger major freeze
// tgt_try_frozen_version < 0 : invalid arguments
// tgt_try_frozen_version > 0 : OBS trigger major freeze, use versions to avoid trigger successively.
int ObRootMajorFreezeV2::global_major_freeze(
    int64_t tgt_try_frozen_version, const ObAddr& addr, const uint64_t tenant_id, const ObAddr& rs_addr)
{
  int ret = OB_SUCCESS;
  int64_t proposal_frozen_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootMajorFreeze not init", K(ret));
  } else if (tgt_try_frozen_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tgt_try_frozen_version));
  } else if (OB_FAIL(check_frozen_version(tgt_try_frozen_version, proposal_frozen_version))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check and set new version", K(ret), K(tgt_try_frozen_version));
    }
  } else if (OB_FAIL(launch_major_freeze(proposal_frozen_version, addr, tenant_id, rs_addr))) {
    LOG_WARN("fail to launch major freeze", K(ret));
  }
  return ret;
}

// choose one timestamp large than gc snapshot as major frozen snapshot
int ObRootMajorFreezeV2::launch_major_freeze(
    const int64_t frozen_version, const ObAddr& addr, const uint64_t tenant_id, const ObAddr& rs_addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(freeze_info_manager_.set_freeze_info(frozen_version, tenant_id, addr, rs_addr))) {
    LOG_WARN("fail to launch new major freeeze");
  }
  return ret;
}

int ObRootMajorFreezeV2::check_frozen_version(const int64_t tgt_try_frozen_version, int64_t& proposal_frozen_version)
{
  int ret = OB_SUCCESS;
  int64_t latest_frozen_version = 0;
  int64_t global_last_merged_version = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(freeze_info_manager_.get_max_frozen_version(latest_frozen_version))) {
    LOG_WARN("fail to get latest_frozen_version", K(ret));
  } else if (OB_FAIL(zone_manager_->get_global_last_merged_version(global_last_merged_version))) {
    LOG_WARN("fail to get global last merged version", K(ret));
  } else {
    if (0 == tgt_try_frozen_version) {
      if (latest_frozen_version - global_last_merged_version >= MAX_UNMERGED_VERSION_NUM) {
        ret = OB_MAJOR_FREEZE_NOT_ALLOW;
        LOG_WARN("cannot do major freeze now, out of version limit",
            K(ret),
            K(global_last_merged_version),
            K(tgt_try_frozen_version));
      } else {
        proposal_frozen_version = latest_frozen_version + 1;
      }
    } else if (tgt_try_frozen_version > latest_frozen_version + 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(tgt_try_frozen_version), K(latest_frozen_version), K(ret));
    } else if (tgt_try_frozen_version <= latest_frozen_version) {
      ret = OB_EAGAIN;
      LOG_WARN("global_version is bigger than try_frozen_version",
          K(tgt_try_frozen_version),
          K(latest_frozen_version),
          K(ret));
    } else if (tgt_try_frozen_version - global_last_merged_version > UNMERGED_VERSION_LIMIT) {
      ret = OB_MAJOR_FREEZE_NOT_ALLOW;
      LOG_WARN("cannot do major freeze triggered by cluster self, out of version limit",
          K(ret),
          K(tgt_try_frozen_version),
          K(global_last_merged_version));
    } else {
      proposal_frozen_version = tgt_try_frozen_version;
    }
  }
  return ret;
}
}  // namespace oceanbase
