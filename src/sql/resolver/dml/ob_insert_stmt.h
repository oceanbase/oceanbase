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

#ifndef OCEANBASE_SQL_OB_INSERT_STMT_H_
#define OCEANBASE_SQL_OB_INSERT_STMT_H_
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
namespace oceanbase {
namespace sql {
// ObInsertStmt inherit ObDelUpStmt for supporting insert on duplicate key
// All function of ObDelUpStmt is needed.
// used to multi table insert
typedef common::ObSEArray<ObColumnRefRawExpr*, 16, common::ModulePageAllocator, true> ColRawExprArray;
typedef common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> RawExprArray;
class ObSelectStmt;

struct ObDupKeyScanInfo {
  ObDupKeyScanInfo()
      : table_id_(common::OB_INVALID_ID),
        index_tid_(common::OB_INVALID_ID),
        loc_table_id_(common::OB_INVALID_ID),
        table_name_(),
        only_data_table_(false),
        output_exprs_(),
        conflict_exprs_()
  {}

  TO_STRING_KV(K_(table_id), K_(index_tid), K_(loc_table_id), K_(table_name), K_(only_data_table), K_(output_exprs),
      K_(conflict_exprs));

  int deep_copy(ObRawExprFactory& expr_factory, const ObDupKeyScanInfo& other);
  int assign(const ObDupKeyScanInfo& other);

  uint64_t table_id_;
  uint64_t index_tid_;
  uint64_t loc_table_id_;
  common::ObString table_name_;
  bool only_data_table_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> output_exprs_;
  common::ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> conflict_exprs_;
  common::ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> access_exprs_;
};

struct ObUniqueConstraintInfo {
  ObUniqueConstraintInfo()
      : table_id_(common::OB_INVALID_ID), index_tid_(common::OB_INVALID_ID), constraint_name_(), constraint_columns_()
  {}
  TO_STRING_KV(K_(table_id), K_(index_tid), K_(constraint_name), K_(constraint_columns));
  inline void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    index_tid_ = common::OB_INVALID_ID;
    constraint_name_.reset();
    constraint_columns_.reset();
  }
  int assign(const ObUniqueConstraintInfo& other);
  uint64_t table_id_;
  uint64_t index_tid_;
  common::ObString constraint_name_;
  common::ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> constraint_columns_;
};

struct ObUniqueConstraintCheckStmt {
  ObUniqueConstraintCheckStmt() : gui_lookup_info_(), table_scan_info_(), gui_scan_infos_(), constraint_infos_()
  {}

  int deep_copy(ObRawExprFactory& expr_factory, const ObUniqueConstraintCheckStmt& other);
  int assign(const ObUniqueConstraintCheckStmt& other);

  ObDupKeyScanInfo gui_lookup_info_;
  ObDupKeyScanInfo table_scan_info_;
  common::ObSEArray<ObDupKeyScanInfo, 8, common::ModulePageAllocator, true> gui_scan_infos_;
  common::ObSEArray<ObUniqueConstraintInfo, 8, common::ModulePageAllocator, true> constraint_infos_;
};

/*used to save table info for multi table insert*/
struct ObInsertTableInfo {
  ObInsertTableInfo()
      : table_id_(common::OB_INVALID_ID), when_conds_idx_(-1), when_conds_expr_(), check_constraint_exprs_()
  {}

  int deep_copy(ObRawExprFactory& expr_factory, const ObInsertTableInfo& other);
  int assign(const ObInsertTableInfo& other);
  TO_STRING_KV(K_(table_id), K_(when_conds_idx), K_(check_constraint_exprs), K_(when_conds_expr));
  uint64_t table_id_;
  int64_t when_conds_idx_;
  RawExprArray when_conds_expr_;
  RawExprArray check_constraint_exprs_;
};

class ObInsertStmt : public ObDelUpdStmt {
public:
  ObInsertStmt();
  virtual ~ObInsertStmt();
  int deep_copy_stmt_struct(
      ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other) override;
  int assign(const ObInsertStmt& other);
  bool is_replace() const;
  void set_replace(bool is_replace);
  int add_value(ObRawExpr* expr);
  bool value_from_select() const
  {
    return !from_items_.empty();
  }
  ObTablesAssignments& get_table_assignments()
  {
    return table_assignments_;
  }
  const ObTablesAssignments& get_tables_assignments() const
  {
    return table_assignments_;
  }
  int get_tables_assignments_exprs(ObIArray<ObRawExpr*>& exprs);
  inline const common::ObIArray<ObColumnRefRawExpr*>& get_values_desc() const
  {
    return values_desc_;
  }
  inline common::ObIArray<ObColumnRefRawExpr*>& get_values_desc()
  {
    return values_desc_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_value_vectors() const
  {
    return value_vectors_;
  }
  inline common::ObIArray<ObRawExpr*>& get_value_vectors()
  {
    return value_vectors_;
  }
  common::ObIArray<uint64_t>& get_primary_key_ids()
  {
    return primary_key_ids_;
  }
  const common::ObIArray<ObColumnRefRawExpr*>* get_table_columns(uint64_t table_offset = 0)
  {
    common::ObIArray<ObColumnRefRawExpr*>* column_exprs = NULL;
    if (!all_table_columns_.empty() && table_offset < all_table_columns_.count() &&
        !all_table_columns_.at(table_offset).index_dml_infos_.empty()) {
      column_exprs = &(all_table_columns_.at(table_offset).index_dml_infos_.at(0).column_exprs_);
    }
    return column_exprs;
  }
  common::ObIArray<ObRawExpr*>& get_column_conv_functions()
  {
    return column_conv_functions_;
  }
  const common::ObIArray<ObRawExpr*>& get_column_conv_functions() const
  {
    return column_conv_functions_;
  }
  int get_insert_columns(common::ObIArray<ObRawExpr*>& columns);
  void set_only_one_unique_key(bool has_index)
  {
    only_one_unique_key_ = has_index;
  }
  bool is_only_one_unique_key() const
  {
    return only_one_unique_key_;
  }
  int64_t get_row_count() const
  {
    int64_t count = 0;
    if (values_desc_.count() == 0) {
      count = 0;  // for merge into without insert clause
    } else {
      count = value_vectors_.count() / values_desc_.count();
    }
    return count;
  }
  void set_insert_up(bool insert_up)
  {
    insert_up_ = insert_up;
  }
  bool get_insert_up() const
  {
    return insert_up_;
  }
  bool is_all_const_values()
  {
    return is_all_const_values_;
  }
  void set_is_all_const_values(bool is_all_const)
  {
    is_all_const_values_ = is_all_const;
  }
  // is used to replace all references to from expr in stmt
  virtual int replace_expr_in_stmt(ObRawExpr* from, ObRawExpr* to);
  int64_t get_value_index(uint64_t table_id, uint64_t column_id) const;
  const ObUniqueConstraintCheckStmt& get_constraint_check_stmt() const
  {
    return constraint_check_stmt_;
  }
  ObUniqueConstraintCheckStmt& get_constraint_check_stmt()
  {
    return constraint_check_stmt_;
  }
  virtual int replace_inner_stmt_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs) override;
  int part_key_has_rand_value(bool& has);
  int part_key_has_subquery(bool& has);
  int get_value_exprs(ObIArray<ObRawExpr*>& value_exprs);

  // if generated col is partition key in heap table, we need to store all dep cols,
  // eg:
  //  create table t1(c0 int, c1 int, c2 int as (c0 + c1)) partition by hash(c2);
  //  insert into t1(c0) values(1);
  // part_generated_col_dep_cols_ store c1.
  common::ObIArray<ObColumnRefRawExpr*>& get_part_generated_col_dep_cols()
  {
    return part_generated_col_dep_cols_;
  }
  // use this only when part_generated_col_dep_cols_.count() is not zero
  int get_values_desc_for_heap_table(common::ObIArray<ObColumnRefRawExpr*>& arr);
  int get_value_vectors_for_heap_table(common::ObIArray<ObRawExpr*>& arr);

  bool is_multi_insert_first() const
  {
    return is_multi_insert_first_;
  }
  void set_is_multi_insert_first(bool v)
  {
    is_multi_insert_first_ = v;
  }
  bool is_multi_conditions_insert() const
  {
    return is_multi_conditions_insert_;
  }
  void set_is_multi_conditions_insert(bool v)
  {
    is_multi_conditions_insert_ = v;
  }
  bool is_multi_insert_stmt() const
  {
    return is_multi_insert_stmt_;
  }
  void set_is_multi_insert_stmt(bool multi_insert)
  {
    is_multi_insert_stmt_ = multi_insert;
  }
  inline const common::ObIArray<ColRawExprArray>& get_multi_values_desc() const
  {
    return multi_values_desc_;
  }
  inline common::ObIArray<ColRawExprArray>& get_multi_values_desc()
  {
    return multi_values_desc_;
  }
  inline const common::ObIArray<RawExprArray>& get_multi_value_vectors() const
  {
    return multi_value_vectors_;
  }
  inline common::ObIArray<RawExprArray>& get_multi_value_vectors()
  {
    return multi_value_vectors_;
  }
  inline const common::ObIArray<RawExprArray>& get_multi_insert_col_conv_funcs() const
  {
    return multi_insert_col_conv_funcs_;
  }
  inline common::ObIArray<RawExprArray>& get_multi_insert_col_conv_funcs()
  {
    return multi_insert_col_conv_funcs_;
  }
  inline const common::ObIArray<common::ObSEArray<uint64_t, 16>>& get_multi_insert_primary_key_ids() const
  {
    return multi_insert_primary_key_ids_;
  }
  inline common::ObIArray<common::ObSEArray<uint64_t, 16>>& get_multi_insert_primary_key_ids()
  {
    return multi_insert_primary_key_ids_;
  }
  inline const common::ObIArray<ObInsertTableInfo>& get_multi_insert_table_info() const
  {
    return multi_insert_table_info_;
  }
  inline common::ObIArray<ObInsertTableInfo>& get_multi_insert_table_info()
  {
    return multi_insert_table_info_;
  }
  DECLARE_VIRTUAL_TO_STRING;

protected:
  // Get the root expr of all query-related expressions in
  // stmt (expression generated by the attributes specified in the query statement)
  virtual int inner_get_relation_exprs(RelExprCheckerBase& expr_checker);
  // get the root expr that needs to be processed by enum_set_wrapper in stmt
  virtual int inner_get_relation_exprs_for_wrapper(RelExprChecker& expr_checker);
  int replace_dupkey_exprs(const common::ObIArray<ObRawExpr*>& other_exprs,
      const common::ObIArray<ObRawExpr*>& new_exprs, ObDupKeyScanInfo& scan_info);

protected:
  bool is_replace_;  // replace semantic
  common::ObSEArray<ObColumnRefRawExpr*, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> values_desc_;
  common::ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> value_vectors_;
  /**
   * @note These fields are added for the compatibility of MySQL syntax and are not
   * supported at the moment.
   */
  bool low_priority_;
  bool high_priority_;
  bool delayed_;
  bool insert_up_;
  bool only_one_unique_key_;
  common::ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> column_conv_functions_;
  // replace is used to save rowkey column
  common::ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> primary_key_ids_;
  ObTablesAssignments table_assignments_;  // one table for insert statement
  bool is_all_const_values_;
  ObUniqueConstraintCheckStmt constraint_check_stmt_;
  common::ObSEArray<ObColumnRefRawExpr*, 16> part_generated_col_dep_cols_;

  // used to mark is multi table insert
  bool is_multi_insert_stmt_;
  // used to save multi table insert conditions
  common::ObSEArray<ColRawExprArray, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> multi_values_desc_;
  common::ObSEArray<RawExprArray, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> multi_value_vectors_;
  common::ObSEArray<RawExprArray, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true>
      multi_insert_col_conv_funcs_;
  common::ObSEArray<ObSEArray<uint64_t, 16>, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true>
      multi_insert_primary_key_ids_;
  bool is_multi_insert_first_;
  bool is_multi_conditions_insert_;
  // Used to save the basic information of all inserted tables when inserting multiple tables
  common::ObSEArray<ObInsertTableInfo, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true>
      multi_insert_table_info_;
};

inline void ObInsertStmt::set_replace(bool is_replace)
{
  is_replace_ = is_replace;
  if (is_replace_) {
    set_stmt_type(stmt::T_REPLACE);
  } else {
    set_stmt_type(stmt::T_INSERT);
  }
}

inline bool ObInsertStmt::is_replace() const
{
  return is_replace_;
}

inline int ObInsertStmt::add_value(ObRawExpr* expr)
{
  return value_vectors_.push_back(expr);
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_INSERT_STMT_H_
