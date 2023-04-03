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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OPTIMIZE_TABLE_STMT_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OPTIMIZE_TABLE_STMT_H_


#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObOptimizeTableStmt : public ObDDLStmt
{
public:
  explicit ObOptimizeTableStmt(common::ObIAllocator *name_pool);
  ObOptimizeTableStmt();
  virtual ~ObOptimizeTableStmt() = default;

  const obrpc::ObOptimizeTableArg &get_optimize_table_arg() const { return optimize_table_arg_; }
  obrpc::ObOptimizeTableArg &get_optimize_table_arg() { return optimize_table_arg_; }
  virtual bool cause_implicit_commit() const { return true; }
  int add_table_item(const obrpc::ObTableItem &table_item);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return optimize_table_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id) { optimize_table_arg_.tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const { return optimize_table_arg_.tenant_id_; }
  TO_STRING_KV(K_(stmt_type),K_(optimize_table_arg));
private:
  obrpc::ObOptimizeTableArg optimize_table_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObOptimizeTableStmt);
};

class ObOptimizeTenantStmt : public ObDDLStmt
{
public:
  explicit ObOptimizeTenantStmt(common::ObIAllocator *name_pool);
  ObOptimizeTenantStmt();
  virtual ~ObOptimizeTenantStmt() = default;
  const obrpc::ObOptimizeTenantArg &get_optimize_tenant_arg() const { return optimize_tenant_arg_; }
  obrpc::ObOptimizeTenantArg &get_optimize_tenant_arg() { return optimize_tenant_arg_; }
  virtual bool cause_implicit_commit() const { return true; }
  void set_tenant_name(const common::ObString &tenant_name);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return optimize_tenant_arg_; }
private:
  obrpc::ObOptimizeTenantArg optimize_tenant_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObOptimizeTenantStmt);
};

class ObOptimizeAllStmt : public ObDDLStmt
{
public:
  explicit ObOptimizeAllStmt(common::ObIAllocator *name_pool);
  ObOptimizeAllStmt();
  virtual ~ObOptimizeAllStmt() = default;
  const obrpc::ObOptimizeAllArg &get_optimize_all_arg() const { return optimize_all_arg_; }
  obrpc::ObOptimizeAllArg &get_optimize_all_arg() { return optimize_all_arg_; }
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return optimize_all_arg_; }
private:
  obrpc::ObOptimizeAllArg optimize_all_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObOptimizeAllStmt);
};

}
}

#endif  // OCEANBASE_SQL_RESOLVER_DDL_OPTIMIZE_TABLE_STMT_H_
