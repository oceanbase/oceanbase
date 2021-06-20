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

#ifndef OCEANBASE_SQL_OB_LOG_TEMP_TABLE_TRANSFORMATION_H
#define OCEANBASE_SQL_OB_LOG_TEMP_TABLE_TRANSFORMATION_H

#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase {
namespace sql {
class ObLogTempTableTransformation : public ObLogicalOperator {
public:
  ObLogTempTableTransformation(ObLogPlan& plan);
  virtual ~ObLogTempTableTransformation();

  virtual int copy_without_child(ObLogicalOperator*& out) override;
  virtual int transmit_op_ordering() override;
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int allocate_exchange(AllocExchContext* ctx, ObExchangeInfo& exch_info) override;
  virtual int compute_op_ordering() override;
  virtual int transmit_local_ordering() override;
  virtual bool is_consume_child_1by1() const
  {
    return true;
  }
  virtual int compute_fd_item_set() override;
  virtual int est_cost() override;

  int get_temp_table_exprs(ObIArray<ObRawExpr*>& set_exprs) const;
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_TEMP_TABLE_TRANSFORMATION_H
