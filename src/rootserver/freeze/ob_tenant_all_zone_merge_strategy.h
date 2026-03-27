/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_TENANT_ALL_ZONE_MERGE_STRATEGY_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_TENANT_ALL_ZONE_MERGE_STRATEGY_H_

#include "rootserver/freeze/ob_tenant_major_merge_strategy.h"

namespace oceanbase
{
namespace rootserver
{
class ObZoneManager;

class ObTenantAllZoneMergeStrategy : public ObTenantMajorMergeStrategy
{
public:
  friend class TestTenantAllZoneMergeStrategy_get_next_zone_Test;

  ObTenantAllZoneMergeStrategy() {}
  virtual ~ObTenantAllZoneMergeStrategy() {}
  virtual int get_next_zone(common::ObIArray<common::ObZone> &to_merge_zones) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantAllZoneMergeStrategy);
};

} // namespace rootserver
} // namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_FREEZE_OB_TENANT_ALL_ZONE_MERGE_STRATEGY_H_
