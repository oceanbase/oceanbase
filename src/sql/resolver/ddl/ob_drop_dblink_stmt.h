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
