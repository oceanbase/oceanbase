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

#ifndef _OB_LOG_ERR_LOG_H
#define _OB_LOG_ERR_LOG_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/optimizer/ob_log_operator_factory.h"
namespace oceanbase
{
namespace sql
{
class ObLogErrLog : public ObLogicalOperator
{
public:
  ObLogErrLog(ObLogPlan &plan);
  virtual ~ObLogErrLog() {}
  virtual const char *get_name() const;
  virtual int est_cost() override;
  virtual uint64_t hash(uint64_t seed) const override;
  int extract_err_log_info();
  ObErrLogDefine &get_err_log_define() { return err_log_define_; }
  const ObErrLogDefine &get_err_log_define() const { return err_log_define_; }
  void set_del_upd_stmt(const ObDelUpdStmt *del_upd_stmt) { del_upd_stmt_ = del_upd_stmt; }
  const ObDelUpdStmt *get_del_upd_stmt() const { return del_upd_stmt_; }
  int get_err_log_type(stmt::StmtType &type);
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
private:
  ObErrLogDefine err_log_define_;
  const ObDelUpdStmt *del_upd_stmt_;
  DISALLOW_COPY_AND_ASSIGN(ObLogErrLog);
};
} // end of namespace sql
} // end of namespace oceanbase
#endif // _OB_LOG_ERR_LOG_H
