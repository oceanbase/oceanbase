/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "ob_anonymous_block_stmt.h"

namespace oceanbase
{
namespace sql
{
int ObAnonymousBlockStmt::add_param(const ObObjParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited array", K(ret));
  } else if (OB_FAIL(params_->push_back(param))) {
    LOG_WARN("fail to push back param", K(ret));
  }
  return ret;
}
}
}
