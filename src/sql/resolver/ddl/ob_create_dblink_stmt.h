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

#ifndef OCEANBASE_SQL_OB_CREATE_DBLINK_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_DBLINK_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase {
namespace sql {
class ObCreateDbLinkStmt : public ObDDLStmt {
public:
  ObCreateDbLinkStmt();
  explicit ObCreateDbLinkStmt(common::ObIAllocator* name_pool);
  virtual ~ObCreateDbLinkStmt();

  inline void set_tenant_id(const uint64_t id)
  {
    create_dblink_arg_.dblink_info_.set_tenant_id(id);
  }
  inline void set_user_id(const uint64_t id)
  {
    create_dblink_arg_.dblink_info_.set_owner_id(id);
  }
  inline int set_dblink_name(const common::ObString& name)
  {
    return create_dblink_arg_.dblink_info_.set_dblink_name(name);
  }
  inline int set_cluster_name(const common::ObString& name)
  {
    return create_dblink_arg_.dblink_info_.set_cluster_name(name);
  }
  inline int set_tenant_name(const common::ObString& name)
  {
    return create_dblink_arg_.dblink_info_.set_tenant_name(name);
  }
  inline int set_user_name(const common::ObString& name)
  {
    return create_dblink_arg_.dblink_info_.set_user_name(name);
  }
  inline int set_password(const common::ObString& pwd)
  {
    return create_dblink_arg_.dblink_info_.set_password(pwd);
  }
  inline void set_host_addr(const common::ObAddr& addr)
  {
    create_dblink_arg_.dblink_info_.set_host_addr(addr);
    ;
  }

  obrpc::ObCreateDbLinkArg& get_create_dblink_arg()
  {
    return create_dblink_arg_;
  }
  virtual obrpc::ObDDLArg& get_ddl_arg()
  {
    return create_dblink_arg_;
  }
  virtual bool cause_implicit_commit() const
  {
    return true;
  }

  TO_STRING_KV(K_(create_dblink_arg));

private:
  obrpc::ObCreateDbLinkArg create_dblink_arg_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_CREATE_DBLINK_STMT_H_
