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

#ifndef OCEANBASE_SQL_OB_LOG_TEMP_TABLE_INSERT_H
#define OCEANBASE_SQL_OB_LOG_TEMP_TABLE_INSERT_H 1

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/resolver/dml/ob_sql_hint.h"

namespace oceanbase {
namespace sql {
class ObLogTempTableInsert : public ObLogicalOperator {
public:
  ObLogTempTableInsert(ObLogPlan& plan);
  virtual ~ObLogTempTableInsert();

  virtual int copy_without_child(ObLogicalOperator*& out)
  {
    return clone(out);
  }
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int est_cost() override;
  virtual bool is_block_op() const override
  {
    return true;
  }
  inline void set_ref_table_id(uint64_t ref_table_id)
  {
    ref_table_id_ = ref_table_id;
  }
  uint64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }
  inline common::ObString& get_table_name()
  {
    return temp_table_name_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTempTableInsert);
  uint64_t ref_table_id_;
  common::ObString temp_table_name_;
};
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_TEMP_TABLE_INSERT_H
