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

#ifndef OCEANBASE_SQL_RESOLVER_OB_TABLE_STMT_H_
#define OCEANBASE_SQL_RESOLVER_OB_TABLE_STMT_H_ 1
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ddl/ob_partitioned_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObPartitionResolveResult
{
  const static int OB_DEFAULT_ARRAY_SIZE = 16;
  typedef common::ObSEArray<ObRawExpr *,
                            OB_DEFAULT_ARRAY_SIZE,
                            common::ModulePageAllocator,
                            true> array_t;
  typedef common::ObSEArray<array_t,
                            OB_DEFAULT_ARRAY_SIZE,
                            common::ModulePageAllocator,
                            true> array_array_t;
public:
  ObPartitionResolveResult() {}
  ~ObPartitionResolveResult() {}
  array_t &get_part_fun_exprs() { return part_fun_exprs_; }
  array_t &get_part_values_exprs() { return part_values_exprs_; }
  array_t &get_subpart_fun_exprs() { return subpart_fun_exprs_; }
  array_t &get_template_subpart_values_exprs() { return template_subpart_values_exprs_; }
  array_array_t &get_individual_subpart_values_exprs() { return individual_subpart_values_exprs_; }
  TO_STRING_KV(K_(part_fun_exprs),
               K_(part_values_exprs),
               K_(subpart_fun_exprs),
               K_(template_subpart_values_exprs),
               K_(individual_subpart_values_exprs));
private:
  array_t part_fun_exprs_;       // for part fun expr
  array_t part_values_exprs_;   // for part values expr
  array_t subpart_fun_exprs_;    // for subpart fun expr
  array_t template_subpart_values_exprs_;    // for template subpart fun expr
  array_array_t individual_subpart_values_exprs_; //for individual subpart values expr
};

class ObTableStmt : public ObPartitionedStmt
{
  const static int OB_DEFAULT_ARRAY_SIZE = 16;
public:

  ObTableStmt(common::ObIAllocator *name_pool, stmt::StmtType type)
      : ObPartitionedStmt(name_pool, type),
        part_type_(share::schema::PARTITION_FUNC_TYPE_MAX)
  {
  }
  explicit ObTableStmt(stmt::StmtType type)
      : ObPartitionedStmt(type),
        part_type_(share::schema::PARTITION_FUNC_TYPE_MAX)
  {
  }
  virtual ~ObTableStmt() {}

  share::schema::ObPartitionFuncType get_part_type() const
  {
    return part_type_;
  }
  common::ObSArray<ObPartitionResolveResult> &get_index_partition_resolve_results()
  {
    return index_partition_resolve_results_;
  }
  TO_STRING_KV(K_(part_type));
private:
  common::ObSArray<ObPartitionResolveResult> index_partition_resolve_results_;
  share::schema::ObPartitionFuncType part_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableStmt);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_OB_TABLE_STMT_H_ */
