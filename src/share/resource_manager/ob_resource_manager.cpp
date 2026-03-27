/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
