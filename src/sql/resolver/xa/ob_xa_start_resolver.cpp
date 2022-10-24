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
#include "sql/resolver/xa/ob_xa_start_resolver.h"
#include "sql/resolver/xa/ob_xa_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace sql
{
ObXaStartResolver::ObXaStartResolver(ObResolverParams &params)
    : ObStmtResolver(params)
{
}

ObXaStartResolver::~ObXaStartResolver()
{
}

int ObXaStartResolver::resolve(const ParseNode &parse_node)
{
  int ret = OB_SUCCESS;
  ObXaStartStmt *xa_start_stmt = NULL;
  if (OB_UNLIKELY(T_XA_START != parse_node.type_ || 1 !=parse_node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected val", K(parse_node.type_), K(parse_node.num_child_), K(ret));
  } else if (OB_UNLIKELY(NULL == (xa_start_stmt = create_stmt<ObXaStartStmt>()))) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create xa start stmt", K(ret));
  } else {
    ObString xa_string;
    if (OB_FAIL(ObResolverUtils::resolve_string(parse_node.children_[0], xa_string))) {
      LOG_WARN("resolve string failed", K(ret));
    } else {
      xa_start_stmt->set_xa_string(xa_string);
      LOG_DEBUG("xa start resolver", K(xa_string));
    }
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
