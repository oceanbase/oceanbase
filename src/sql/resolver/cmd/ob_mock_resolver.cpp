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
#include "sql/resolver/cmd/ob_mock_resolver.h"
#include "sql/resolver/cmd/ob_mock_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{
int ObMockResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObMockStmt *mock_stmt = NULL;
  if (OB_UNLIKELY(NULL == (mock_stmt = create_stmt<ObMockStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create mock stmt");
  } else {
    switch (parse_tree.type_) {
      case T_FLUSH_PRIVILEGES:
        mock_stmt->set_stmt_type(stmt::T_FLUSH_PRIVILEGES);
        break;
      case T_REPAIR_TABLE:
        mock_stmt->set_stmt_type(stmt::T_REPAIR_TABLE);
        break;
      case T_CHECKSUM_TABLE:
        mock_stmt->set_stmt_type(stmt::T_CHECKSUM_TABLE);
        break;
      case T_CACHE_INDEX:
        mock_stmt->set_stmt_type(stmt::T_CACHE_INDEX);
        break;
      case T_LOAD_INDEX_INTO_CACHE:
        mock_stmt->set_stmt_type(stmt::T_LOAD_INDEX_INTO_CACHE);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected parse tree type", K(ret), K(parse_tree.type_));
        break;
    }
  }
  return ret;
}
}
}
