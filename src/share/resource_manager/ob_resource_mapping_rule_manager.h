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
class ObResourceManagerProxy;
class ObResourceMappingRuleManager
{
public:
  typedef common::ObArray<ObResourceMappingRule> ObResourceMappingRuleSet;
  typedef common::ObArray<ObResourceUserMappingRule> ObResourceUserMappingRuleSet;
  typedef common::ObArray<ObResourceIdNameMappingRule> ObResourceIdNameMappingRuleSet;
public:
  ObResourceMappingRuleManager() = default;
  virtual ~ObResourceMappingRuleManager() = default;
  int init();
  int refresh_group_mapping_rule(const uint64_t tenant_id, const common::ObString &plan);
  int clear_deleted_group(const uint64_t tenant_id, const ObResourceUserMappingRuleSet &rules);
  int refresh_resource_mapping_rule(const uint64_t tenant_id, const common::ObString &plan);
  int reset_mapping_rules();
  common::hash::ObHashMap<ObTenantGroupIdKey, ObGroupName> &get_group_id_name_map() { return group_id_name_map_; }

  class GetTenantGroupIdNameFunctor final
  {
  public:
    GetTenantGroupIdNameFunctor(uint64_t tenant_id, common::ObIArray<share::ObTenantGroupIdKey> &group_id_keys,
        common::ObIArray<ObGroupName> &group_names)
        : tenant_id_(tenant_id), group_id_keys_(group_id_keys), group_names_(group_names)
    {}
    ~GetTenantGroupIdNameFunctor() = default;

    int operator()(hash::HashMapPair<share::ObTenantGroupIdKey, ObGroupName> &kv)
    {
      int ret = OB_SUCCESS;
      if (kv.first.tenant_id_ == tenant_id_) {
        if (OB_FAIL(group_id_keys_.push_back(kv.first))) {
          LOG_WARN("fail to push back function keys", K(ret), K(kv.first));
        } else if (OB_FAIL(group_names_.push_back(kv.second))) {
          LOG_WARN("fail to push back group names", K(ret), K(kv.second));
        }
      }
      return ret;
    }

  private:
    uint64_t tenant_id_;
    common::ObIArray<share::ObTenantGroupIdKey> &group_id_keys_;
    common::ObIArray<ObGroupName> &group_names_;
  };

  class GetTenantFunctionRuleFunctor final
  {
  public:
    GetTenantFunctionRuleFunctor(uint64_t tenant_id, common::ObIArray<share::ObTenantFunctionKey> &func_keys,
        common::ObIArray<uint64_t> &group_ids)
        : tenant_id_(tenant_id), func_keys_(func_keys), group_ids_(group_ids)
    {}
    ~GetTenantFunctionRuleFunctor() = default;

    int operator()(hash::HashMapPair<share::ObTenantFunctionKey, uint64_t> &kv)
    {
      int ret = OB_SUCCESS;
      if (kv.first.tenant_id_ == tenant_id_) {
        if (OB_FAIL(func_keys_.push_back(kv.first))) {
          LOG_WARN("fail to push back function keys", K(ret), K(kv.first));
        } else if (OB_FAIL(group_ids_.push_back(kv.second))) {
          LOG_WARN("fail to push back group ids", K(ret), K(kv.second));
        }
      }
      return ret;
    }

  private:
    int64_t tenant_id_;
    common::ObIArray<share::ObTenantFunctionKey> &func_keys_;
    common::ObIArray<uint64_t> &group_ids_;
  };

  class GetTenantUserRuleFunctor final
  {
  public:
    GetTenantUserRuleFunctor(
        uint64_t tenant_id, common::ObIArray<sql::ObTenantUserKey> &user_keys, common::ObIArray<uint64_t> &group_ids)
        : tenant_id_(tenant_id), user_keys_(user_keys), group_ids_(group_ids)
    {}
    ~GetTenantUserRuleFunctor() = default;

    int operator()(hash::HashMapPair<sql::ObTenantUserKey, uint64_t> &kv)
    {
      int ret = OB_SUCCESS;
      if (kv.first.tenant_id_ == tenant_id_ && kv.second > 0) {
        if (OB_FAIL(user_keys_.push_back(kv.first))) {
          LOG_WARN("fail to push back user keys", K(ret), K(kv.first));
        } else if (OB_FAIL(group_ids_.push_back(kv.second))) {
          LOG_WARN("fail to push back group ids", K(ret), K(kv.second));
        }
      }
      return ret;
    }

  private:
    uint64_t tenant_id_;
    common::ObIArray<sql::ObTenantUserKey> &user_keys_;
    common::ObIArray<uint64_t> &group_ids_;
  };

  inline int get_group_id_by_user(const uint64_t tenant_id, uint64_t user_id, uint64_t &group_id)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_INVALID_TENANT_ID;
      LOG_WARN("invalid config", K(ret), K(tenant_id));
    } else if (OB_FAIL(user_rule_map_.get_refactored(sql::ObTenantUserKey(tenant_id, user_id), group_id))) {
      if (common::OB_HASH_NOT_EXIST == ret) {
        group_id = 0; // 没有定义 mapping rule，默认为 0 (OTHER_GROUPS)
        ret = common::OB_SUCCESS;
      } else {
        LOG_WARN("get group id by user fail", K(ret), K(user_id));
        group_id = 0; // 没有定义 mapping rule，默认为 0 (OTHER_GROUPS)
        ret = common::OB_SUCCESS;
      }
    }
    return ret;
  }

  inline int get_group_id_by_function_type(const uint64_t tenant_id,
                                           const uint8_t function_type,
                                           uint64_t &group_id)
  {
    int ret = common::OB_SUCCESS;
    const ObString &func_name = share::get_io_function_name(static_cast<ObFunctionType>(function_type));
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_INVALID_TENANT_ID;
      LOG_WARN("invalid config", K(ret), K(tenant_id));
    } else if (OB_FAIL(get_group_id_by_function(tenant_id, func_name, group_id))) {
      LOG_WARN("get group id by function fail", K(ret), K(function_type));
      // 没有拿到group_id，可能是没有建立映射关系、map未初始化(没有生效plan）等原因
      // 此时仍然需要返回一个group_id，使dag绑定到other上
      group_id = 0;
      ret = common::OB_SUCCESS;
    }
    return ret;
  }

  inline int get_group_id_by_function(const uint64_t tenant_id,
                                      const common::ObString &func,
                                      uint64_t &group_id)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_INVALID_TENANT_ID;
      LOG_WARN("invalid config", K(ret), K(tenant_id));
    } else if (OB_FAIL(function_rule_map_.get_refactored(
                share::ObTenantFunctionKey(tenant_id, func), group_id))) {
      if (common::OB_HASH_NOT_EXIST == ret) {
        group_id = 0; // 没有定义 mapping rule，默认为 0 (OTHER_GROUPS)
        ret = common::OB_SUCCESS;
      }
    }
    return ret;
  }

  inline int get_group_name_by_id(const uint64_t tenant_id,
                                  const uint64_t group_id,
                                  ObGroupName &group_name)
  {
    int ret = group_id_name_map_.get_refactored(share::ObTenantGroupIdKey(tenant_id, group_id), group_name);
    return ret;
  }

  inline int get_group_id_by_name(const uint64_t tenant_id,
                                  ObGroupName group_name,
                                  uint64_t &group_id)
  {
    int ret = group_name_id_map_.get_refactored(share::ObTenantGroupKey(tenant_id, group_name), group_id);
    return ret;
  }
  inline int reset_group_id_by_user(const uint64_t tenant_id,
                                    const uint64_t user_id)
  {
    int ret = user_rule_map_.set_refactored(sql::ObTenantUserKey(tenant_id, user_id), 0, 1/*overwrite*/);
    return ret;
  }
  inline int reset_group_id_by_function(const uint64_t tenant_id,
                                        ObResMgrVarcharValue &func)
  {
    int ret = function_rule_map_.set_refactored(share::ObTenantFunctionKey(tenant_id, func), 0, 1/*overwrite*/);
    return ret;
  }
  int64_t to_string(char *buf, const int64_t len) const;
private:
  int refresh_resource_function_mapping_rule(
      ObResourceManagerProxy &proxy,
      const uint64_t tenant_id,
      const ObString &plan);
  int clear_resource_function_mapping_rule(const uint64_t tenant_id, const ObResourceMappingRuleSet &rules);
  int refresh_resource_user_mapping_rule(
      ObResourceManagerProxy &proxy,
      const uint64_t tenant_id,
      const ObString &plan);
  int clear_resource_user_mapping_rule(const uint64_t tenant_id, const ObResourceUserMappingRuleSet &rules);
private:
  /* variables */
  // 将用户 id 映射到 group id，用于用户登录时快速确定登录用户所属 cgroup
  common::hash::ObHashMap<sql::ObTenantUserKey, uint64_t> user_rule_map_;
  // 将 function 映射到 group id，用于后台线程快速确定后台 session 所属 cgroup
  common::hash::ObHashMap<share::ObTenantFunctionKey, uint64_t> function_rule_map_;
  // 将 group_id 映射到 group_name, 用于快速更新 cgroup fs 目录(包括user和function使用的group)
  common::hash::ObHashMap<ObTenantGroupIdKey, ObGroupName> group_id_name_map_;
  // 将 group_name 映射到 group_id, 用于快速根据group_name找到id(主要是用于io控制)
  common::hash::ObHashMap<share::ObTenantGroupKey, uint64_t> group_name_id_map_;
  DISALLOW_COPY_AND_ASSIGN(ObResourceMappingRuleManager);
};
}
}
#endif /* _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_MAPPING_RULE_MANAGER_H_ */
//// end of header file

