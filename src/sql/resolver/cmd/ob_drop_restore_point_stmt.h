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

#ifndef OCEANBASE_SQL_OB_DROP_RESTORE_POINT_STMT_H_
#define OCEANBASE_SQL_OB_DROP_RESTORE_POINT_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObDropRestorePointStmt : public ObSystemCmdStmt
{
public:
  explicit ObDropRestorePointStmt(common::ObIAllocator *name_pool)
    :  ObSystemCmdStmt(name_pool, stmt::T_DROP_RESTORE_POINT),
       drop_restore_point_arg_(),
       restore_point_name_()
       {}
  ObDropRestorePointStmt()
    :  ObSystemCmdStmt(stmt::T_DROP_RESTORE_POINT),
       drop_restore_point_arg_(),
       restore_point_name_()
       {}
  virtual ~ObDropRestorePointStmt() {}
  inline obrpc::ObDropRestorePointArg &get_drop_restore_point_arg()
  {
    return drop_restore_point_arg_;
  }
  void set_tenant_id(const int64_t tenant_id)
  {
    drop_restore_point_arg_.tenant_id_ = tenant_id;
  }
  void set_restore_point_name(const common::ObString &restore_point_name)
  {
    restore_point_name_ = restore_point_name;
    drop_restore_point_arg_.name_ = restore_point_name;
  }
  ObString get_restore_point_name() { return restore_point_name_; }
private:
  obrpc::ObDropRestorePointArg drop_restore_point_arg_;
  ObString restore_point_name_;
  DISALLOW_COPY_AND_ASSIGN(ObDropRestorePointStmt);
};

} /* sql */
} /* oceanbase */
#endif //OCEANBASE_SQL_OB_CREATE_TENANT_STMT_H_
