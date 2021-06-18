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

#include "sql/resolver/ddl/ob_drop_dblink_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace sql {
int64_t ObDropDbLinkStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(drop_dblink_arg));
  J_OBJ_END();
  return pos;
}
ObDropDbLinkStmt::ObDropDbLinkStmt() : ObDDLStmt(stmt::T_DROP_DBLINK), drop_dblink_arg_()
{}

ObDropDbLinkStmt::ObDropDbLinkStmt(common::ObIAllocator* name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_DBLINK), drop_dblink_arg_()
{}

ObDropDbLinkStmt::~ObDropDbLinkStmt()
{}

}  // namespace sql
}  // namespace oceanbase
