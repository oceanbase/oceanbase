/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
  virtual bool print_flashback_query() const override { return true; };
private:
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> select_exprs_;

};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_LINK_SCAN_H
