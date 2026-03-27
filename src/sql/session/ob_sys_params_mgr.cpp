/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/session/ob_sys_params_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObSysParamsMgr::ObSysParamsMgr()
{
  // default values are set here
  sort_mem_size_limit_ = 500000;  //500M
  group_mem_size_limit_ = 500000;  //500M
}

ObSysParamsMgr::~ObSysParamsMgr()
{
}

int ObSysParamsMgr::parse_from_file(const char *file_name)
{
  // Fix me, parse system parameters from .ini file
  UNUSED(file_name);
  int ret = OB_SUCCESS;
  return ret;
}
}//end of ns sql
}//end of ns oceanbase
