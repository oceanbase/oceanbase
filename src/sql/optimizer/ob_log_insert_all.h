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

#ifndef _OB_LOG_INSERT_ALL_H
#define _OB_LOG_INSERT_ALL_H 1
#include "ob_logical_operator.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "ob_log_insert.h"

namespace oceanbase
{
namespace sql
{

class ObLogInsertAll : public ObLogInsert
{
public:
  ObLogInsertAll(ObDelUpdLogPlan &plan)
      : ObLogInsert(plan),
        is_multi_table_insert_(false),
        is_multi_insert_first_(false),
        is_multi_conditions_insert_(false),
        insert_all_table_info_(NULL)
  {
  }

  virtual ~ObLogInsertAll()
  {
  }
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  const char* get_name() const;
  bool is_multi_table_insert() const { return is_multi_table_insert_; }
  void set_is_multi_table_insert(bool is_multi) { is_multi_table_insert_ = is_multi; }
  bool is_multi_insert_first() { return is_multi_insert_first_; }
  void set_is_multi_insert_first(bool v) { is_multi_insert_first_ = v; }
  bool is_multi_conditions_insert() { return is_multi_conditions_insert_; }
  void set_is_multi_conditions_insert(bool v) { is_multi_conditions_insert_ = v; }
  inline void set_insert_all_table_info(const common::ObIArray<ObInsertAllTableInfo*> *insert_all_table_info)
  { insert_all_table_info_ = insert_all_table_info; }
  inline const common::ObIArray<ObInsertAllTableInfo*> *get_insert_all_table_info() const
  { return insert_all_table_info_; }
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
protected:
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  // virtual int generate_rowid_expr_for_trigger() override;
  // virtual int generate_multi_part_partition_id_expr() override;
private:
  bool is_multi_table_insert_;//标记是否是multi table insert
  bool is_multi_insert_first_;//标记是否是multi table insert first
  bool is_multi_conditions_insert_;//标记是带条件的multi table insert

  //用于多表插入时保存所有的插入的表基本信息
  const common::ObIArray<ObInsertAllTableInfo*> *insert_all_table_info_;
};

}
}
#endif
