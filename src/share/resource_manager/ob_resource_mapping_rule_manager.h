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

#ifndef _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_MAPPING_RULE_MANAGER_H_
#define _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_MAPPING_RULE_MANAGER_H_

#include "lib/utility/ob_macro_utils.h"
#include "common/data_buffer.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_define.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#include "sql/session/ob_user_resource_mgr.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{

class ObResourceMappingRuleManager
{
public:
  typedef common::ObArray<ObResourceMappingRule> ObResourceMappingRuleSet;
  typedef common::ObArray<ObResourceUserMappingRule> ObResourceUserMappingRuleSet;
public:
  ObResourceMappingRuleManager() = default;
  virtual ~ObResourceMappingRuleManager() = default;
  int init();
  int refresh_resource_mapping_rule(uint64_t tenant_id, const common::ObString &plan);
  inline int64_t get_group_id_by_user(uint64_t tenant_id, uint64_t user_id, uint64_t &group_id)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(rule_map_.get_refactored(sql::ObTenantUserKey(tenant_id, user_id), group_id))) {
      if (common::OB_HASH_NOT_EXIST == ret) {
        group_id = 0; // 没有定义 mapping rule，默认为 0 (OTHER_GROUPS)
        ret = common::OB_SUCCESS;
      }
    }
    return ret;
  }
  inline int64_t get_group_name_by_id(uint64_t tenant_id,
                                      uint64_t group_id,
                                      ObGroupName &group_name)
  {
    int ret = group_map_.get_refactored(common::combine_two_ids(tenant_id, group_id), group_name);
    return ret;
  }
private:
  /* variables */
  // 将用户 id 映射到 group id，用于用户登录时快速确定登录用户所属 cgroup
  common::hash::ObHashMap<sql::ObTenantUserKey, uint64_t> rule_map_;
  // 将 group id 映射到 GroupName，用于快速更新 cgroup fs 目录
  common::hash::ObHashMap<uint64_t, ObGroupName> group_map_;
  DISALLOW_COPY_AND_ASSIGN(ObResourceMappingRuleManager);
};
}
}
#endif /* _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_MAPPING_RULE_MANAGER_H_ */
//// end of header file

