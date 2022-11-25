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

#ifndef _OB_LOG_EXPR_VALUES_H
#define _OB_LOG_EXPR_VALUES_H
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
class ObLogExprValues : public ObLogicalOperator
  {
  public:
    ObLogExprValues(ObLogPlan &plan)
        : ObLogicalOperator(plan),
          err_log_define_()

    {}
    virtual ~ObLogExprValues() {}
    int add_values_expr(const common::ObIArray<ObRawExpr *> &value_exprs);

    const common::ObIArray<ObRawExpr *> &get_value_exprs() const
    {
      return value_exprs_;
    }
    common::ObIArray<ObRawExpr *> &get_value_exprs()
    {
      return value_exprs_;
    }
    bool contain_array_binding_param() const;
    bool is_ins_values_batch_opt() const;

    // add for error logging
    ObErrLogDefine &get_err_log_define() { return err_log_define_; }
    const ObErrLogDefine &get_err_log_define() const { return err_log_define_; }

    virtual int est_cost() override;
    virtual int compute_op_ordering() override;
    virtual int compute_equal_set() override;
    virtual int compute_table_set() override;
    virtual int compute_fd_item_set() override;
    virtual int compute_one_row_info() override;
    virtual int compute_sharding_info() override;
    virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
    virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
    int extract_err_log_info();
    int mark_probably_local_exprs();
    int allocate_dummy_output();
  private:
    int construct_array_binding_values();
    int construct_sequence_values();
    virtual int print_my_plan_annotation(char *buf,
                                         int64_t &buf_len,
                                         int64_t &pos,
                                         ExplainType type);
  private:
    common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> value_exprs_;
    //add for error_logging
    ObErrLogDefine err_log_define_;

    DISALLOW_COPY_AND_ASSIGN(ObLogExprValues);
  };
}
}
#endif
