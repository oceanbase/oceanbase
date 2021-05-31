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

#define USING_LOG_PREFIX SHARE

#include "ob_resource_plan_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

const ObString ObPlanDirective::INTERNAL_GROUP_NAME[ObPlanDirective::INTERNAL_GROUP_NAME_COUNT] = {
    "OTHER_GROUPS", "SYS_GROUP"};

bool ObPlanDirective::is_internal_group_name(const ObString& name)
{
  bool bret = false;
  for (int64_t i = 0; i < INTERNAL_GROUP_NAME_COUNT; ++i) {
    if (0 == INTERNAL_GROUP_NAME[i].compare(name)) {
      bret = true;
      break;
    }
  }
  return bret;
}

int ObPlanDirective::assign(const ObPlanDirective& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  mgmt_p1_ = other.mgmt_p1_;
  utilization_limit_ = other.utilization_limit_;
  level_ = other.level_;
  ret = group_name_.assign(other.group_name_);
  return ret;
}

int ObResourceMappingRule::assign(const ObResourceMappingRule& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  OZ(set_attr(other.attr_), other);
  OZ(set_value(other.value_), other);
  OZ(set_group(other.group_), other);
  return ret;
}

int ObResourceUserMappingRule::assign(const ObResourceUserMappingRule& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  user_id_ = other.user_id_;
  group_id_ = other.group_id_;
  ret = group_name_.assign(other.group_name_);
  return ret;
}
