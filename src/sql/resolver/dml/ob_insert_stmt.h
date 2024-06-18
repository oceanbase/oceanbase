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
namespace oceanbase
{
namespace sql
{

class ObInsertStmt : public ObDelUpdStmt
{
public:
  ObInsertStmt();
  virtual ~ObInsertStmt();
  int deep_copy_stmt_struct(ObIAllocator &allocator,
                            ObRawExprCopier &expr_copier,
                            const ObDMLStmt &other) override;
  int assign(const ObInsertStmt &other);
  virtual int check_table_be_modified(uint64_t ref_table_id, bool& is_modified) const override;
  ObInsertTableInfo &get_insert_table_info() { return table_info_; }
  const ObInsertTableInfo &get_insert_table_info() const { return table_info_; }
  void set_replace(bool is_replace)
  {
    table_info_.is_replace_ = is_replace;
    if (table_info_.is_replace_) {
      set_stmt_type(stmt::T_REPLACE);
    } else {
      set_stmt_type(stmt::T_INSERT);
    }
  }
  bool is_replace() const { return table_info_.is_replace_; }
  void set_overwrite(const bool is_overwrite) {
    table_info_.is_overwrite_ = is_overwrite;
    set_stmt_type(stmt::T_INSERT);
  }
  bool is_overwrite() const { return table_info_.is_overwrite_; }
  bool value_from_select() const { return !from_items_.empty(); }
  common::ObIArray<ObAssignment> &get_table_assignments() { return table_info_.assignments_; }
  const common::ObIArray<ObAssignment> &get_table_assignments() const { return table_info_.assignments_; }
  inline const common::ObIArray<ObColumnRefRawExpr*> &get_values_desc() const { return table_info_.values_desc_;}
  inline common::ObIArray<ObColumnRefRawExpr*> &get_values_desc() { return table_info_.values_desc_; }
  inline const common::ObIArray<ObRawExpr*> &get_values_vector() const { return table_info_.values_vector_; }
  inline common::ObIArray<ObRawExpr*> &get_values_vector() { return table_info_.values_vector_; }
  common::ObIArray<ObRawExpr*> &get_column_conv_exprs() { return table_info_.column_conv_exprs_;}
  const common::ObIArray<ObRawExpr*> &get_column_conv_exprs() const
  { return table_info_.column_conv_exprs_; }
  virtual int get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const override;
  int64_t get_insert_row_count() const
  {
    int64_t count = 0;
    if (table_info_.values_desc_.count() > 0) {
      count = table_info_.values_vector_.count() / table_info_.values_desc_.count();
    }
    return count;
  }
  bool is_insert_single_value() const
  {
    return !value_from_select() && (get_insert_row_count() == 1);
  }
  bool is_insert_up() const { return !table_info_.assignments_.empty(); }
  bool is_all_const_values() const { return is_all_const_values_; }
  void set_is_all_const_values(bool is_all_const) { is_all_const_values_ = is_all_const; }
  int part_key_has_rand_value(bool &has) const;
  int part_key_has_subquery(bool &has) const ;
  int part_key_has_auto_inc(bool &has) const;
  int part_key_is_updated(bool &is_updated) const;
  int check_pdml_disabled(const bool is_online_ddl, bool &disable_pdml, bool &is_pk_auto_inc) const;
  int find_first_auto_inc_expr(const ObRawExpr *expr, const ObRawExpr *&auto_inc) const;
  virtual int get_value_exprs(ObIArray<ObRawExpr *> &value_exprs) const override;
  // use this only when part_generated_col_dep_cols_.count() is not zero
  int get_values_desc_for_heap_table(common::ObIArray<ObColumnRefRawExpr*> &arr) const;
  int get_value_vectors_for_heap_table(common::ObIArray<ObRawExpr*> &arr) const;
  int get_ddl_sort_keys(common::ObIArray<OrderItem> &sort_keys) const;
  virtual int get_assignments_exprs(ObIArray<ObRawExpr*> &exprs) const override;
  virtual int get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info) override;
  virtual int get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const override;
  virtual uint64_t get_trigger_events() const override
  {
    uint64_t events = 0;
    events |= ObDmlEventType::DE_INSERTING;
    if (is_replace()) {
      events |= ObDmlEventType::DE_DELETING;
    } else if (is_insert_up()) {
      events |= ObDmlEventType::DE_UPDATING;
    }
    return events;
  }
  virtual int64_t get_instead_of_trigger_column_count() const override;
  DECLARE_VIRTUAL_TO_STRING;
private:
  bool is_all_const_values_;
  ObInsertTableInfo table_info_;
};

}//namespace sql
}//namespace oceanbase

#endif //OCEANBASE_SQL_OB_INSERT_STMT_H_
