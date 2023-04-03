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

#ifndef _OB_SHARE_RESOURCE_OB_RESOURCE_MANAGER_H_
#define _OB_SHARE_RESOURCE_OB_RESOURCE_MANAGER_H_

#include "share/ob_define.h"
#include "share/resource_manager/ob_resource_plan_manager.h"
#include "share/resource_manager/ob_resource_mapping_rule_manager.h"
#include "share/resource_manager/ob_resource_col_mapping_rule_manager.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{

class ObResourceManager
{
public:
  static ObResourceManager &get_instance();
  int init();
  ObResourcePlanManager &get_plan_mgr() { return res_plan_mgr_; }
  ObResourceMappingRuleManager &get_mapping_rule_mgr() { return res_mapping_rule_mgr_; }
  ObResourceColMappingRuleManager &get_col_mapping_rule_mgr() { return res_col_mapping_rule_mgr_; }
private:
  ObResourceManager() = default;
  virtual ~ObResourceManager() = default;
private:
  /* variables */
  ObResourcePlanManager res_plan_mgr_;
  ObResourceMappingRuleManager res_mapping_rule_mgr_;
  ObResourceColMappingRuleManager res_col_mapping_rule_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObResourceManager);
};

#define G_RES_MGR (::oceanbase::share::ObResourceManager::get_instance())

}
}
#endif /* _OB_SHARE_RESOURCE_OB_RESOURCE_MANAGER_H_ */
//// end of header file

