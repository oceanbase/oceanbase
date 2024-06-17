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
        field_str_(),
        line_str_(),
        user_vars_(),
        closed_cht_(),
        is_optional_(true),
        is_single_(true),
        max_file_size_(DEFAULT_MAX_FILE_SIZE),
        escaped_cht_()
  {
    cs_type_ = ObCharset::get_system_collation();
  }
  virtual ~ObLogSelectInto() {}
  inline void set_into_type(ObItemType &into_type)
  {
    into_type_ = into_type;
  }
  inline void set_outfile_name(common::ObObj &name_str)
  {
    outfile_name_ = name_str;
  }
  inline void set_field_str(common::ObObj &field_str)
  {
    field_str_ = field_str;
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
  inline void set_is_single(bool is_single)
  {
    is_single_ = is_single;
  }
  inline void set_max_file_size(int64_t max_file_size)
  {
    max_file_size_ = max_file_size;
  }
  inline void set_closed_cht(common::ObObj closed_cht)
  {
    closed_cht_ = closed_cht;
  }
  inline void set_escaped_cht(common::ObObj escaped_cht)
  {
    escaped_cht_ = escaped_cht;
  }
  inline void set_cs_type(common::ObCollationType cs_type)
  {
    cs_type_ = cs_type;
  }
  inline ObItemType get_into_type() const
  {
    return into_type_;
  }
  inline common::ObObj get_outfile_name() const
  {
    return outfile_name_;
  }
  inline common::ObObj get_field_str() const
  {
    return field_str_;
  }
  inline common::ObObj get_line_str() const
  {
    return line_str_;
  }
  inline const common::ObIArray<common::ObString> &get_user_vars() const
  {
    return user_vars_;
  }
  inline bool get_is_optional() const
  {
    return is_optional_;
  }
  inline bool get_is_single() const
  {
    return is_single_;
  }
  inline int64_t get_max_file_size() const
  {
    return max_file_size_;
  }
  inline common::ObObj get_closed_cht() const
  {
    return closed_cht_;
  }
  inline common::ObObj get_escaped_cht() const
  {
    return escaped_cht_;
  }
  inline common::ObCollationType get_cs_type() const
  {
    return cs_type_;
  }
  const common::ObIArray<ObRawExpr*> &get_select_exprs() const { return select_exprs_; }
  common::ObIArray<ObRawExpr*> &get_select_exprs() { return select_exprs_; }
  virtual int est_cost() override;
  virtual int compute_plan_type() override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer);
  static const int64_t DEFAULT_MAX_FILE_SIZE = 256*1024*1024;
private:
  ObItemType into_type_;
  common::ObObj outfile_name_;
  common::ObObj field_str_;
  common::ObObj line_str_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> select_exprs_;
  common::ObSEArray<common::ObString, 16, common::ModulePageAllocator, true> user_vars_;
  common::ObObj closed_cht_;
  bool is_optional_;
  bool is_single_;
  int64_t max_file_size_;
  common::ObObj escaped_cht_;
  common::ObCollationType cs_type_;
};
}
}


#endif /* SRC_SQL_OPTIMIZER_OB_LOG_SELECT_INTO_H_ */
