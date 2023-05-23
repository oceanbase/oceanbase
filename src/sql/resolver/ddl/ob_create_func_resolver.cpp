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
#include "sql/resolver/ddl/ob_create_func_resolver.h"
#include "sql/engine/user_defined_function/ob_udf_util.h"

namespace oceanbase {
using namespace share;
namespace sql {

ObCreateFuncResolver::ObCreateFuncResolver(ObResolverParams& params) : ObDDLResolver(params)
{}

ObCreateFuncResolver::~ObCreateFuncResolver()
{}

int ObCreateFuncResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  UNUSED(parse_tree);
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "creating loadable function");
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
