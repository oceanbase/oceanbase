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
#include "sql/resolver/dml/ob_del_upd_stmt.h"
namespace oceanbase
{
namespace sql
{

class ObMergeStmt: public ObDelUpdStmt
{
public:
  ObMergeStmt();
  virtual ~ObMergeStmt();
  int deep_copy_stmt_struct(ObIAllocator &allocator,
                            ObRawExprCopier &expr_copier,
                            const ObDMLStmt &other) override;
  int assign(const ObMergeStmt &other);
  virtual int check_table_be_modified(uint64_t ref_table_id, bool& is_modified) const override;
  ObMergeTableInfo &get_merge_table_info() { return table_info_; }
  const ObMergeTableInfo &get_merge_table_info() const { return table_info_; }
  void set_target_table_id(uint64_t id) { table_info_.target_table_id_ = id; }
  inline uint64_t get_target_table_id() const { return table_info_.target_table_id_; }
  void set_source_table_id(uint64_t id) { table_info_.source_table_id_ = id; }
  inline uint64_t get_source_table_id() const { return table_info_.source_table_id_; }
  common::ObIArray<ObRawExpr*> &get_match_condition_exprs() { return table_info_.match_condition_exprs_; }
  const common::ObIArray<ObRawExpr*> &get_match_condition_exprs() const
  { return table_info_.match_condition_exprs_; }

  common::ObIArray<ObRawExpr*> &get_insert_condition_exprs() { return table_info_.insert_condition_exprs_; }
  const common::ObIArray<ObRawExpr*> &get_insert_condition_exprs() const
  { return table_info_.insert_condition_exprs_; }

  common::ObIArray<ObRawExpr*> &get_update_condition_exprs() { return table_info_.update_condition_exprs_; }
  const common::ObIArray<ObRawExpr*> &get_update_condition_exprs() const
  { return table_info_.update_condition_exprs_; }

  common::ObIArray<ObRawExpr*> &get_delete_condition_exprs() { return table_info_.delete_condition_exprs_; }
  const common::ObIArray<ObRawExpr*> &get_delete_condition_exprs() const
  { return table_info_.delete_condition_exprs_; }

  virtual uint64_t get_trigger_events() const override
  {
    uint64_t events = 0;
    if (has_insert_clause()) {
      events |= ObDmlEventType::DE_INSERTING;
    }
    if (has_update_clause()) {
      events |= ObDmlEventType::DE_UPDATING;
    }
    if (has_delete_clause()) {
      events |= ObDmlEventType::DE_DELETING;
    }
    return events;
  }
  bool has_insert_clause() const { return !table_info_.values_desc_.empty(); }
  bool has_update_clause() const { return !table_info_.assignments_.empty(); }
  bool has_delete_clause() const { return !table_info_.delete_condition_exprs_.empty(); }
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
  virtual int get_assignments_exprs(ObIArray<ObRawExpr*> &exprs) const override;
  virtual int get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info) override;
  virtual int get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const override;
  virtual int get_value_exprs(ObIArray<ObRawExpr *> &value_exprs) const override;
  bool value_from_select() const { return !from_items_.empty(); }
  int part_key_is_updated(bool &is_updated) const;
  int part_key_has_rand_value(bool &has) const;
  int part_key_has_subquery(bool &has) const ;
  int part_key_has_auto_inc(bool &has) const;
  DECLARE_VIRTUAL_TO_STRING;

private:
  ObMergeTableInfo table_info_;

};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_MERGE_STMT_H_
