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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObAlterUDTStmt : public ObDDLStmt
{
public:
  explicit ObAlterUDTStmt(common::ObIAllocator *name_pool)
      : ObDDLStmt(name_pool, stmt::T_ALTER_TYPE),
        alter_udt_arg_() {}
    ObAlterUDTStmt()
      : ObDDLStmt(stmt::T_ALTER_TYPE),
        alter_udt_arg_() {}
  virtual ~ObAlterUDTStmt() {}
  obrpc::ObAlterUDTArg &get_alter_udt_arg() { return alter_udt_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return alter_udt_arg_; }
private:
  obrpc::ObAlterUDTArg alter_udt_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterUDTStmt);
};
}
}

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_STMT_H_ */
