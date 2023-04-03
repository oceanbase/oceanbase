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

#define USING_LOG_PREFIX  SQL_ENG
#include "sql/resolver/cmd/ob_empty_query_resolver.h"
#include "sql/resolver/cmd/ob_empty_query_stmt.h"

namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{
int ObEmptyQueryResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObEmptyQueryStmt *empty_query_stmt = NULL;
  if (T_EMPTY_QUERY != parse_tree.type_ 
      && T_LOCK_TABLE != parse_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("parser tree type is not T_EMPTY_QUERY", K(parse_tree.type_), K(ret));
  } else if (0 == parse_tree.value_) {
    //empty query with no comment
    ret = OB_ERR_EMPTY_QUERY;

  } else if (OB_ISNULL(empty_query_stmt = create_stmt<ObEmptyQueryStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create empty query stmt", K(ret));
  } else {}
  return ret;
}
} // sql
} // oceanbase
