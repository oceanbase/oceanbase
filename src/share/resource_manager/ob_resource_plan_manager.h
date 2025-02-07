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

#ifndef _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_PLAN_MANAGER_H_
#define _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_PLAN_MANAGER_H_

#include "lib/utility/ob_macro_utils.h"
#include "common/data_buffer.h"
#include "lib/string/ob_string.h"
#include "share/ob_define.h"
#include "share/resource_manager/ob_resource_plan_info.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{
static constexpr int64_t OTHER_GROUPS_IOPS_WEIGHT = 100L;
static constexpr int64_t OTHER_GROUPS_NET_BANDWIDTH_WEIGHT = 100L;
class ObResourcePlanManager
{
public:
  ObResourcePlanManager() : tenant_plan_map_(), background_quota_(INT32_MAX)
  {}
  virtual ~ObResourcePlanManager() = default;
  int init();
  int refresh_resource_plan(const uint64_t tenant_id, common::ObString &plan_name);
  int refresh_global_background_cpu();
  int get_cur_plan(const uint64_t tenant_id, ObResMgrVarcharValue &plan_name);
  int64_t to_string(char *buf, const int64_t len) const;
private:
  /* functions */
  int switch_resource_plan(const uint64_t tenant_id, common::ObString &plan_name);
  int flush_directive_to_cgroup_fs(ObPlanDirectiveSet &directives);
  int flush_directive_to_iops_control(const uint64_t tenant_id,
                                      ObPlanDirectiveSet &directives,
                                      ObPlanDirective &other_group_directive);
  int clear_deleted_directives(const uint64_t tenant_id, ObPlanDirectiveSet &directives);
  int normalize_iops_directives(const uint64_t tenant_id,
                                ObPlanDirectiveSet &directives,
                                ObPlanDirective &other_group_directive);
  int normalize_net_bandwidth_directives(const uint64_t tenant_id,
                                         ObPlanDirectiveSet &directives,
                                         ObPlanDirective &other_group_directive);
  common::hash::ObHashMap<uint64_t, ObResMgrVarcharValue> tenant_plan_map_;
  int32_t background_quota_;
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObResourcePlanManager);
};
}
}
#endif /* _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_PLAN_MANAGER_H_ */
//// end of header file

