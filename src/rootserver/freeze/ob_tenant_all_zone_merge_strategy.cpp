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

#define USING_LOG_PREFIX RS_COMPACTION
#include "rootserver/freeze/ob_tenant_all_zone_merge_strategy.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;

int ObTenantAllZoneMergeStrategy::get_next_zone(ObIArray<ObZone> &to_merge_zones)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_->get_zone(to_merge_zones))) {
    LOG_WARN("fail to get zone", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(filter_merging_zones(to_merge_zones))) {
    LOG_WARN("fail to filter merging zones", KR(ret), K(tenant_id_));
  } else {
    LOG_INFO("get_next_zone of merge strategy", K(tenant_id_),
             "to_merge_cnt", to_merge_zones.count(), K(to_merge_zones));
  }

  return ret;
}

} // namespace rootserver
} // namespace oceanbase
