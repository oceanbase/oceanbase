/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_OB_TABLEGROUP_STMT_H_
#define OCEANBASE_SQL_RESOLVER_OB_TABLEGROUP_STMT_H_ 1
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_partitioned_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObTablegroupStmt : public ObPartitionedStmt
{
  const static int OB_DEFAULT_ARRAY_SIZE = 16;
public:

  ObTablegroupStmt(common::ObIAllocator *name_pool, stmt::StmtType type)
    : ObPartitionedStmt(name_pool, type),
      part_func_expr_num_(OB_INVALID_INDEX),
      sub_part_func_expr_num_(OB_INVALID_INDEX)
  {
  }
  explicit ObTablegroupStmt(stmt::StmtType type)
    : ObPartitionedStmt(type),
      part_func_expr_num_(OB_INVALID_INDEX),
      sub_part_func_expr_num_(OB_INVALID_INDEX)
  {
  }
  virtual ~ObTablegroupStmt() {}

  virtual void set_tenant_id(const uint64_t tenant_id) = 0;
  virtual int set_primary_zone(const common::ObString &zone) = 0;
  virtual int set_locality(const common::ObString &locality) = 0;
  virtual int set_tablegroup_id(uint64_t tablegroup_id) = 0;
  virtual int set_tablegroup_sharding(const common::ObString &sharding) = 0;

  int64_t get_part_func_expr_num() { return part_func_expr_num_; }
  void set_part_func_expr_num(int64_t expr_num) { part_func_expr_num_ = expr_num; }

  int64_t get_sub_part_func_expr_num() { return sub_part_func_expr_num_; }
  void set_sub_part_func_expr_num(int64_t expr_num) { sub_part_func_expr_num_ = expr_num; }

private:
  int64_t part_func_expr_num_;
  int64_t sub_part_func_expr_num_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTablegroupStmt);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_OB_TABLEGROUP_STMT_H_ */
