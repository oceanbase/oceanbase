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
#include "ob_savepoint_resolver.h"
#include "ob_savepoint_stmt.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

int ObSavePointResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSavePointStmt *stmt = NULL;
  if (OB_FAIL(create_savepoint_stmt(parse_tree.type_, stmt)) || OB_ISNULL(stmt)) {
    LOG_WARN("failed to create savepoint stmt", K(ret));
  } else if (OB_FAIL(stmt->set_sp_name(parse_tree.str_value_, parse_tree.str_len_))) {
    LOG_WARN("failed to set savepoint name", K(ret));
  } else {
    stmt_ = stmt;
  }
  return ret;
}

int ObSavePointResolver::create_savepoint_stmt(ObItemType stmt_type, ObSavePointStmt *&stmt)
{
  int ret = OB_SUCCESS;
  switch (stmt_type) {
  case T_CREATE_SAVEPOINT:
    stmt = create_stmt<ObCreateSavePointStmt>();
    break;
  case T_ROLLBACK_SAVEPOINT:
    stmt = create_stmt<ObRollbackSavePointStmt>();
    break;
  case T_RELEASE_SAVEPOINT:
    stmt = create_stmt<ObReleaseSavePointStmt>();
    break;
  default:
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stmt type", K(ret), K(stmt_type));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

