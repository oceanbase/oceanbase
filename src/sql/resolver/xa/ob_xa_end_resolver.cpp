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
#include "sql/resolver/xa/ob_xa_stmt.h"
#include "sql/resolver/xa/ob_xa_end_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObXaEndResolver::ObXaEndResolver(ObResolverParams &params) 
  : ObStmtResolver(params)
{
}

ObXaEndResolver::~ObXaEndResolver()
{
}

int ObXaEndResolver::resolve(const ParseNode &parse_node)
{
  int ret = OB_SUCCESS;
  ObXaEndStmt *xa_end_stmt = NULL;
  if (OB_UNLIKELY(T_XA_END != parse_node.type_ || 2 != parse_node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse node", K(parse_node.type_), K(parse_node.num_child_), K(ret));
  } else if (OB_UNLIKELY(NULL == (xa_end_stmt = create_stmt<ObXaEndStmt>()))) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create xa end stmt", K(ret));
  } else {
    ObString gtrid_string;
    ObString bqual_string;
    int64_t format_id = -1;
    int64_t flag = 0;
    if (OB_FAIL(ObResolverUtils::resolve_xid(parse_node.children_[0], gtrid_string, bqual_string, format_id))) {
      LOG_WARN("resolve xid failed in xa end", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_opt_suspend(parse_node.children_[1], flag))) {
      LOG_WARN("resolve xa end suspend failed", K(ret));
    } else {
      if(gtrid_string.length() <= 0) {
        ret = OB_TRANS_XA_INVAL;
        LOG_WARN("resolve xid failed, gtrid string can not be NULL", K(ret));
      } else {
        xa_end_stmt->set_xa_string(gtrid_string, bqual_string);
        if(format_id >= 0) {
          xa_end_stmt->set_format_id(format_id);
        }
      LOG_DEBUG("xa end resolver", K(gtrid_string), K(bqual_string), K(format_id), K(flag));
      }
    }
  }
  return ret;
}

} // end namesapce sql
} // end namesapce oceanbase
