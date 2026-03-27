/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_DBLINK_STMT_H_
#define OCEANBASE_SQL_OB_DROP_DBLINK_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDropDbLinkStmt : public ObDDLStmt
{
public:
  ObDropDbLinkStmt();
  explicit ObDropDbLinkStmt(common::ObIAllocator *name_pool);
  virtual ~ObDropDbLinkStmt();

  inline void set_tenant_id(const uint64_t id) { drop_dblink_arg_.tenant_id_ =id; }
  inline void set_dblink_name(const common::ObString &name) { drop_dblink_arg_.dblink_name_ = name; }
  inline bool get_if_exist() { return drop_dblink_arg_.if_exist_; }
  inline void set_if_exist(bool value) { drop_dblink_arg_.if_exist_ = true; }
  obrpc::ObDropDbLinkArg &get_drop_dblink_arg() { return drop_dblink_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_dblink_arg_; }
  virtual bool cause_implicit_commit() const { return true; }

  TO_STRING_KV(K_(drop_dblink_arg));

private:
  obrpc::ObDropDbLinkArg drop_dblink_arg_;
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_DROP_DBLINK_STMT_H_
