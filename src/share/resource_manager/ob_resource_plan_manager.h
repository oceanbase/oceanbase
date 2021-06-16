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

namespace oceanbase {
namespace common {
class ObString;
}
namespace share {

class ObResourcePlanManager {
public:
  typedef common::ObSEArray<ObPlanDirective, 2> ObPlanDirectiveSet;

public:
  ObResourcePlanManager() = default;
  virtual ~ObResourcePlanManager() = default;
  int init();
  int refresh_resource_plan(uint64_t tenant_id, common::ObString& plan_name);

private:
  /* functions */
  int flush_directive_to_cgroup_fs(ObPlanDirectiveSet& directives);
  int create_cgroup_dir_if_not_exist(const ObPlanDirectiveSet& directives);
  int normalize_directives(ObPlanDirectiveSet& directives);
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObResourcePlanManager);
};
}  // namespace share
}  // namespace oceanbase
#endif /* _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_PLAN_MANAGER_H_ */
//// end of header file
