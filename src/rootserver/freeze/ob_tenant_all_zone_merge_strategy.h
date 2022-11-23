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
