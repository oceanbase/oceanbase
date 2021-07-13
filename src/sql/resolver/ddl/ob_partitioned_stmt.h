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

#ifndef OCEANBASE_SQL_RESOLVER_OB_PARTITIONED_STMT_H_
#define OCEANBASE_SQL_RESOLVER_OB_PARTITIONED_STMT_H_ 1
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase {
namespace sql {
class ObPartitionedStmt : public ObDDLStmt {
public:
  ObPartitionedStmt(common::ObIAllocator* name_pool, stmt::StmtType type) : ObDDLStmt(name_pool, type)
  {}
  explicit ObPartitionedStmt(stmt::StmtType type) : ObDDLStmt(type)
  {}
  virtual ~ObPartitionedStmt()
  {}

  array_t& get_part_fun_exprs()
  {
    return part_fun_exprs_;
  }
  array_t& get_part_values_exprs()
  {
    return part_values_exprs_;
  }
  array_t& get_subpart_fun_exprs()
  {
    return subpart_fun_exprs_;
  }
  array_t& get_template_subpart_values_exprs()
  {
    return template_subpart_values_exprs_;
  }
  array_array_t& get_individual_subpart_values_exprs()
  {
    return individual_subpart_values_exprs_;
  }

  TO_STRING_KV(K_(part_fun_exprs), K_(part_values_exprs), K_(subpart_fun_exprs), K_(template_subpart_values_exprs),
      K_(individual_subpart_values_exprs));

private:
  /**
   * part_values_exprs demo:
   * range: array of single value, e.g.
   * partition by range(c1) (partition p0 values less than (100), partition p1 values less than (200))
   * array = [100, 200]
   * partition by range columns (c1,c2) (partition p0 values less than (100, 200), partition p1 values less than (300,
   * 300)) array = [100, 200, 300, 400] list: array of row, each row store some values of a single range, e.g. partition
   * by list(c1) (partition p0 values in (1,2,3,4,5), partition p1 values less in (6,7,8,9,10)) array = [(1,2,3,4,5),
   * (6,7,8,9,10)] partition by list columns (c1,c2) (partition p0 values in ((1,1),(2,2),(3,3)), partition p1 values
   * less in ((6,6),(7,7),(8,8))) array = [(1,1,2,2,3,3), (6,6,7,7,8,8)]
   */
  array_t part_fun_exprs_;                         // for part fun expr
  array_t part_values_exprs_;                      // for part values expr
  array_t subpart_fun_exprs_;                      // for subpart fun expr
  array_t template_subpart_values_exprs_;          // for template subpart fun expr
  array_array_t individual_subpart_values_exprs_;  // for individual subpart values expr
private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionedStmt);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_OB_PARTITIONED_STMT_H_ */
