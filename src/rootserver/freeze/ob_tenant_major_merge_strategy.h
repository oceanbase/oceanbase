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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_TENANT_MAJOR_MERGE_STRATEGY_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_TENANT_MAJOR_MERGE_STRATEGY_H_

#include "lib/container/ob_iarray.h"
#include "common/ob_zone.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace rootserver
{
class ObZoneMergeManager;

class ObTenantMajorMergeStrategy
{
public:
  ObTenantMajorMergeStrategy()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      zone_merge_mgr_(NULL)
  {}
  virtual ~ObTenantMajorMergeStrategy() {}

  int init(const uint64_t tenant_id,
           ObZoneMergeManager *zone_merge_mgr);
  virtual int get_next_zone(common::ObIArray<common::ObZone> &to_merge_zones) = 0;

protected:
  int filter_merging_zones(common::ObIArray<common::ObZone> &to_merge_zones);

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  ObZoneMergeManager *zone_merge_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantMajorMergeStrategy);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_TENANT_MAJOR_MERGE_STRATEGY_H_
