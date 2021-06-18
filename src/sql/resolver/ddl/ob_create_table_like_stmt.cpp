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

#include "sql/resolver/ddl/ob_create_table_like_stmt.h"

namespace oceanbase {
namespace sql {
int64_t ObCreateTableLikeStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(stmt_type), K_(create_table_like_arg));
  J_OBJ_END();
  return pos;
}

ObCreateTableLikeStmt::ObCreateTableLikeStmt(common::ObIAllocator* name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_TABLE_LIKE)
{}

ObCreateTableLikeStmt::ObCreateTableLikeStmt() : ObDDLStmt(stmt::T_CREATE_TABLE_LIKE)
{}

ObCreateTableLikeStmt::~ObCreateTableLikeStmt()
{}

}  // namespace sql
}  // namespace oceanbase
