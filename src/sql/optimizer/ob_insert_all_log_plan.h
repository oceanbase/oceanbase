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

#ifndef _OB_INSERT_ALL_LOG_PLAN_H
#define _OB_INSERT_ALL_LOG_PLAN_H
#include "sql/optimizer/ob_del_upd_log_plan.h"

namespace oceanbase
{
namespace sql
{
class ObDMLStmt;
class ObInsertAllStmt;
typedef common::ObSEArray<common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true>, 1, common::ModulePageAllocator, true> RowParamMap;
class ObInsertAllLogPlan: public ObDelUpdLogPlan
{
public:
  ObInsertAllLogPlan(ObOptimizerContext &ctx, const ObInsertAllStmt *insert_all_stmt)
      : ObDelUpdLogPlan(ctx, insert_all_stmt)
  { }
  virtual ~ObInsertAllLogPlan()
  { }

  const ObInsertAllStmt *get_stmt() const override
  { return reinterpret_cast<const ObInsertAllStmt*>(stmt_); }

  virtual int prepare_dml_infos() override;

protected:
  virtual int generate_normal_raw_plan() override;
  int allocate_insert_values_as_top(ObLogicalOperator *&top);
  int candi_allocate_insert_all();
  int create_insert_all_plan(ObLogicalOperator *&top);
  int allocate_insert_all_as_top(ObLogicalOperator *&top);

  virtual int prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                               IndexDMLInfo* table_dml_info,
                                               ObIArray<IndexDMLInfo*> &index_dml_infos,
                                               ObIArray<IndexDMLInfo*> &all_index_dml_infos) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObInsertAllLogPlan);
};
}
}
#endif // _OB_INSERT_ALL_LOG_PLAN_H
