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

#ifndef SRC_SQL_OPTIMIZER_OB_LOG_SELECT_INTO_H_
#define SRC_SQL_OPTIMIZER_OB_LOG_SELECT_INTO_H_

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "objit/common/ob_item_type.h"

namespace oceanbase
{
namespace sql
{

class ObLogSelectInto : public ObLogicalOperator
{
public:
  ObLogSelectInto(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        into_type_(T_INTO_OUTFILE),
        outfile_name_(),
        filed_str_(),
        line_str_(),
        user_vars_(),
        closed_cht_(0),
        is_optional_(true)
  {}
  virtual ~ObLogSelectInto() {}
  inline void set_into_type(ObItemType &into_type)
  {
    into_type_ = into_type;
  }
  inline void set_outfile_name(common::ObObj &name_str)
  {
    outfile_name_ = name_str;
  }
  inline void set_filed_str(common::ObObj &filed_str)
  {
    filed_str_ = filed_str;
  }
  inline void set_line_str(common::ObObj &line_str)
  {
    line_str_ = line_str;
  }
  inline void set_user_vars(common::ObIArray<common::ObString> &user_vars)
  {
    int ret = common::OB_SUCCESS;
    for (int i = 0 ; i < user_vars.count() ; ++i) {
      if (OB_FAIL(user_vars_.push_back(user_vars.at(i)))) {
        SQL_OPT_LOG(ERROR, "push back failed", K(ret));
      }
    }
  }
  inline void set_is_optional(bool is_optional)
  {
    is_optional_ = is_optional;
  }
  inline void set_closed_cht(char closed_cht)
  {
    closed_cht_ = closed_cht;
  }
  inline ObItemType get_into_type() const
  {
    return into_type_;
  }
  inline common::ObObj get_outfile_name() const
  {
    return outfile_name_;
  }
  inline common::ObObj get_filed_str() const
  {
    return filed_str_;
  }
  inline common::ObObj get_line_str() const
  {
    return line_str_;
  }
  inline const common::ObIArray<common::ObString> &get_user_vars() const
  {
    return user_vars_;
  }
  inline char get_is_optional() const
  {
    return is_optional_;
  }
  inline char get_closed_cht() const
  {
    return closed_cht_;
  }
  const common::ObIArray<ObRawExpr*> &get_select_exprs() const { return select_exprs_; }
  common::ObIArray<ObRawExpr*> &get_select_exprs() { return select_exprs_; }
  virtual int est_cost() override;
  virtual int compute_plan_type() override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer);
private:
  ObItemType into_type_;
  common::ObObj outfile_name_;
  common::ObObj filed_str_;
  common::ObObj line_str_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> select_exprs_;
  common::ObSEArray<common::ObString, 16, common::ModulePageAllocator, true> user_vars_;
  char closed_cht_;
  bool is_optional_;
};
}
}


#endif /* SRC_SQL_OPTIMIZER_OB_LOG_SELECT_INTO_H_ */
