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

#ifndef SRC_SQL_RESOLVER_DML_OB_INSERT_ALL_STMT_H_
#define SRC_SQL_RESOLVER_DML_OB_INSERT_ALL_STMT_H_

#include "sql/resolver/dml/ob_del_upd_stmt.h"

namespace oceanbase
{

namespace sql
{

class ObInsertAllStmt: public sql::ObDelUpdStmt
{
public:
  ObInsertAllStmt();
  virtual ~ObInsertAllStmt();
  int deep_copy_stmt_struct(ObIAllocator &allocator,
                            ObRawExprCopier &expr_copier,
                            const ObDMLStmt &other) override;
  int assign(const ObInsertAllStmt &other);
  virtual int check_table_be_modified(uint64_t ref_table_id, bool& is_modified) const override;
  common::ObIArray<ObInsertAllTableInfo*> &get_insert_all_table_info() { return table_info_; }
  const common::ObIArray<ObInsertAllTableInfo*> &get_insert_all_table_info() const { return table_info_; }

  inline bool get_is_multi_insert_first() const
  {
    return is_multi_insert_first_;
  }
  inline void set_is_multi_insert_first(bool is_multi_insert_first)
  {
    is_multi_insert_first_ = is_multi_insert_first;
  }
  inline bool get_is_multi_condition_insert() const
  {
    return is_multi_condition_insert_;
  }
  inline void set_is_multi_condition_insert(bool is_multi_condition_insert)
  {
    is_multi_condition_insert_ = is_multi_condition_insert;
  }
  virtual uint64_t get_trigger_events() const override
  {
    uint64_t events = 0;
    events |= ObDmlEventType::DE_INSERTING;
    return events;
  }
  virtual int get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info) override;
  virtual int get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const override;
  virtual int get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const override;
  bool value_from_select() const { return !from_items_.empty(); }
  int get_all_values_vector(ObIArray<ObRawExpr*> &all_values_vector) const;
  int get_all_when_cond_exprs(ObIArray<ObRawExpr*> &all_when_cond_exprs) const;

  DECLARE_VIRTUAL_TO_STRING;
private:
  bool is_multi_insert_first_;
  bool is_multi_condition_insert_;
  common::ObSEArray<ObInsertAllTableInfo*, 8, common::ModulePageAllocator, true> table_info_;
};

} // namespace sql
} /* namespace oceanbase */

#endif /* SRC_SQL_RESOLVER_DML_OB_INSERT_ALL_STMT_H_ */
