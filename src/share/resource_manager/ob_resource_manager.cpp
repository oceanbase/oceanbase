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
#include "ob_resource_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

ObResourceManager &ObResourceManager::get_instance()
{
  static ObResourceManager THE_ONE;
  return THE_ONE;
}

int ObResourceManager::init()
{
  int ret = OB_SUCCESS;
  OZ(res_plan_mgr_.init());
  OZ(res_mapping_rule_mgr_.init());
  OZ(res_col_mapping_rule_mgr_.init());
  return ret;
}
