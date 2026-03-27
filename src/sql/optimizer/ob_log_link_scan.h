/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_LOG_LINK_SCAN_H
#define OCEANBASE_SQL_OB_LOG_LINK_SCAN_H

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_link.h"

namespace oceanbase
{
namespace sql
{

typedef common::ObIArray<common::ObString> ObStringIArray;

class ObLogLinkScan : public ObLogLink
{
public:
  ObLogLinkScan(ObLogPlan &plan);
  virtual ~ObLogLinkScan() {}
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  const common::ObIArray<ObRawExpr*> &get_select_exprs() const { return select_exprs_; }
  common::ObIArray<ObRawExpr*> &get_select_exprs() { return select_exprs_; }
private:
  ObSqlArray<ObRawExpr*> select_exprs_;

};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_LINK_SCAN_H
