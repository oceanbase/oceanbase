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
#include "rootserver/freeze/ob_tenant_major_merge_strategy.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "share/ob_zone_info.h"
#include "share/ob_errno.h"
#include "share/ob_freeze_info_proxy.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::share;

int ObTenantMajorMergeStrategy::init(
    const uint64_t tenant_id,
    ObZoneMergeManager *zone_merge_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init, not init again", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(zone_merge_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init, invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    zone_merge_mgr_ = zone_merge_mgr;
    is_inited_ = true;
  }

  return ret;
}

int ObTenantMajorMergeStrategy::filter_merging_zones(common::ObIArray<common::ObZone> &to_merge_zones)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    HEAP_VAR(ObZoneMergeInfo, merge_info) {
      SCN global_broadcast_scn;
      if (OB_FAIL(zone_merge_mgr_->get_global_broadcast_scn(global_broadcast_scn))) {
        LOG_WARN("fail to get get_global_broadcast_scn", KR(ret), K_(tenant_id));
      }
      for (int64_t i = 0; (i < to_merge_zones.count()) && OB_SUCC(ret); i++) {
        merge_info.zone_ = to_merge_zones.at(i);
        merge_info.tenant_id_ = tenant_id_;
        if (OB_FAIL(zone_merge_mgr_->get_zone_merge_info(merge_info))) {
          LOG_WARN("fail to get merge info", KR(ret), K(tenant_id_), "zone", to_merge_zones.at(i));
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_WARN("zone not exist, maybe dropped, treat as success");
          }
        } else if (merge_info.broadcast_scn() == global_broadcast_scn) {
          if (OB_FAIL(to_merge_zones.remove(i))) {
            LOG_WARN("fail to remove to merge zone", KR(ret), K_(tenant_id), K(i));
          } else {
            i--;
          }
        }
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
