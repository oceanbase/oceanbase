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

#ifndef OCEANBASE_SQL_OB_MERGE_STMT_H_
#define OCEANBASE_SQL_OB_MERGE_STMT_H_
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
namespace oceanbase {
namespace sql {
class ObMergeStmt : public ObInsertStmt {
  typedef common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> RawExprArray;

public:
  ObMergeStmt();
  int deep_copy_stmt_struct(
      ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other) override;
  int assign(const ObMergeStmt& other);
  virtual ~ObMergeStmt();
  void set_target_table(uint64_t id)
  {
    target_table_id_ = id;
  }
  inline uint64_t get_target_table_id() const
  {
    return target_table_id_;
  }
  void set_source_table(uint64_t id)
  {
    source_table_id_ = id;
  }
  inline uint64_t get_source_table_id() const
  {
    return source_table_id_;
  }
  int add_rowkey_column_expr(ObColumnRefRawExpr* column_expr);
  common::ObIArray<ObRawExpr*>& get_match_condition_exprs()
  {
    return match_condition_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_match_condition_exprs() const
  {
    return match_condition_exprs_;
  }

  common::ObIArray<ObRawExpr*>& get_insert_condition_exprs()
  {
    return insert_condition_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_insert_condition_exprs() const
  {
    return insert_condition_exprs_;
  }

  common::ObIArray<ObRawExpr*>& get_update_condition_exprs()
  {
    return update_condition_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_update_condition_exprs() const
  {
    return update_condition_exprs_;
  }

  common::ObIArray<ObRawExpr*>& get_delete_condition_exprs()
  {
    return delete_condition_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_delete_condition_exprs() const
  {
    return delete_condition_exprs_;
  }

  common::ObIArray<ObRawExpr*>& get_rowkey_exprs()
  {
    return rowkey_exprs_;
  }
  void set_insert_clause(bool has)
  {
    has_insert_clause_ = has;
  }
  void set_update_clause(bool has)
  {
    has_update_clause_ = has;
  }
  bool has_insert_clause() const
  {
    return has_insert_clause_;
  }
  bool has_update_clause() const
  {
    return has_update_clause_;
  }
  virtual int replace_inner_stmt_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs) override;
  DECLARE_VIRTUAL_TO_STRING;

protected:
  virtual int inner_get_relation_exprs(RelExprCheckerBase& expr_checker);

private:
  uint64_t source_table_id_;
  uint64_t target_table_id_;
  RawExprArray match_condition_exprs_;
  RawExprArray insert_condition_exprs_;
  RawExprArray update_condition_exprs_;
  RawExprArray delete_condition_exprs_;
  RawExprArray rowkey_exprs_;
  bool has_insert_clause_;
  bool has_update_clause_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_MERGE_STMT_H_
