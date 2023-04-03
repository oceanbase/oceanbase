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
