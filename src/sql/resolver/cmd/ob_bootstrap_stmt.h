/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_BOOTSTRAP_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_BOOTSTRAP_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObBootstrapStmt : public ObSystemCmdStmt
{
public:
  ObBootstrapStmt() : ObSystemCmdStmt(stmt::T_BOOTSTRAP), bootstrap_arg_()  {}
  explicit ObBootstrapStmt(common::ObIAllocator *name_pool)
      : ObSystemCmdStmt(name_pool, stmt::T_BOOTSTRAP)
  {}
  virtual ~ObBootstrapStmt() {}

  void set_cluster_role(common::ObClusterRole type) { bootstrap_arg_.cluster_role_ = type; }
  void set_shared_storage_info(const ObString &shared_storage_info)
  {
    bootstrap_arg_.shared_storage_info_ = shared_storage_info;
  }
  void set_logservice_access_point(const ObString &logservice_access_point)
  {
    bootstrap_arg_.logservice_access_point_ = logservice_access_point;
  }
  common::ObClusterRole get_cluster_role() const { return bootstrap_arg_.cluster_role_; }
  obrpc::ObServerInfoList &get_server_info_list() { return bootstrap_arg_.server_list_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(bootstrap_arg));
public:
  obrpc::ObBootstrapArg bootstrap_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObBootstrapStmt);
};

}// namespace sql
}// namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_BOOTSTRAP_STMT_ */
