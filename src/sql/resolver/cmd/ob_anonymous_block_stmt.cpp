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
