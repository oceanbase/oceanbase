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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_get_path.h"
#include "lib/string/ob_string.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDataAccessPathExtraInfo::deep_copy(common::ObIAllocator &allocator,
                                         const ObExprOperatorType type,
                                         ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("Failed to allocate memory for ObExprOracleLRpadInfo", K(ret));
  } else if (OB_ISNULL(copied_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra_info should not be nullptr", K(ret));
  } else {
    ObDataAccessPathExtraInfo *other = static_cast<ObDataAccessPathExtraInfo *>(copied_info);
    if (OB_FAIL(ob_write_string(allocator, data_access_path_, other->data_access_path_))) {
      LOG_WARN("fail to write string", K(ret));
    }
  }
  return ret;
}


OB_SERIALIZE_MEMBER(ObDataAccessPathExtraInfo, data_access_path_);

}
}
