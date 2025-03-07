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

namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{
int ObMockResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  switch (parse_tree.type_) {
    case T_FLUSH_PRIVILEGES:
    case T_INSTALL_PLUGIN:
    case T_UNINSTALL_PLUGIN:
    case T_FLUSH_MOCK:
    case T_HANDLER_MOCK:
    case T_SHOW_PLUGINS:
    case T_REPAIR_TABLE:
    case T_CHECKSUM_TABLE:
    case T_CACHE_INDEX:
    case T_LOAD_INDEX_INTO_CACHE:
    case T_CREATE_SERVER:
    case T_ALTER_SERVER:
    case T_DROP_SERVER:
    case T_CREATE_LOGFILE_GROUP:
    case T_ALTER_LOGFILE_GROUP:
    case T_DROP_LOGFILE_GROUP:
    case T_GRANT_PROXY:
    case T_REVOKE_PROXY:
    {
      ObMockStmt *mock_stmt = NULL;
      if (OB_UNLIKELY(NULL == (mock_stmt = create_stmt<ObMockStmt>()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create mock stmt");
      } else {
        const stmt::StmtType stmt_code = type_convert(parse_tree.type_);
        if (stmt_code == stmt::T_NONE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stmt code is null", K(ret));
        } else {
          mock_stmt->set_stmt_type(stmt_code);
        }
      }
      break;
    }
    case T_FLUSH_MOCK_LIST:
    {
      ObMockStmt *mock_stmt = NULL;
      if (OB_UNLIKELY(NULL == (mock_stmt = create_stmt<ObMockStmt>()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create mock stmt");
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
          ParseNode *node = parse_tree.children_[i];
          if (OB_NOT_NULL(node)) {
            if (node->type_ != T_FLUSH_MOCK && node->type_ != T_FLUSH_PRIVILEGES) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected parse tree type", K(ret), K(node->type_));
            } else if (OB_FAIL(mock_stmt->add_stmt(type_convert(node->type_)))) {
              LOG_WARN("failed to add stmt", K(ret), K(node->type_));
            }
          }
        }
        if (OB_SUCC(ret)) {
          mock_stmt->set_stmt_type(stmt::T_FLUSH_MOCK_LIST);
        }
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected parse tree type", K(ret), K(parse_tree.type_));
  }
  return ret;
}
}
}
