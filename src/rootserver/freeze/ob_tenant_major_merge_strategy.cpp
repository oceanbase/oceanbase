/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS_COMPACTION
#include "rootserver/freeze/ob_tenant_major_merge_strategy.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"

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


} // namespace rootserver
} // namespace oceanbase
