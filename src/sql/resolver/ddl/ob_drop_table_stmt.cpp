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

#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
using namespace common;
using obrpc::ObTableItem;
namespace sql {
int64_t ObDropTableStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(stmt_type), K_(drop_table_arg));
  J_OBJ_END();
  return pos;
}
ObDropTableStmt::ObDropTableStmt(ObIAllocator* name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_TABLE), drop_table_arg_(), is_view_stmt_(false)
{}

ObDropTableStmt::ObDropTableStmt() : ObDDLStmt(stmt::T_DROP_TABLE), drop_table_arg_(), is_view_stmt_(false)
{}

ObDropTableStmt::~ObDropTableStmt()
{}

int ObDropTableStmt::add_table_item(const ObTableItem& table_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(drop_table_arg_.tables_.push_back(table_item))) {
    LOG_WARN("failed to add table item!", K(table_item), K(ret));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
