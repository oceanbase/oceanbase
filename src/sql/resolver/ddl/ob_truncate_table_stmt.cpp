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

#include "sql/resolver/ddl/ob_truncate_table_stmt.h"

namespace oceanbase {

using namespace share::schema;

namespace sql {
int64_t ObTruncateTableStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(stmt_type), K_(truncate_table_arg));
  J_OBJ_END();
  return pos;
}

ObTruncateTableStmt::ObTruncateTableStmt(common::ObIAllocator* name_pool) : ObDDLStmt(name_pool, stmt::T_TRUNCATE_TABLE)
{}

ObTruncateTableStmt::ObTruncateTableStmt() : ObDDLStmt(stmt::T_TRUNCATE_TABLE)
{}

ObTruncateTableStmt::~ObTruncateTableStmt()
{}

void ObTruncateTableStmt::set_database_name(const common::ObString& database_name)
{
  truncate_table_arg_.database_name_ = database_name;
}

void ObTruncateTableStmt::set_table_name(const common::ObString& table_name)
{
  truncate_table_arg_.table_name_ = table_name;
}

}  // namespace sql
}  // namespace oceanbase
