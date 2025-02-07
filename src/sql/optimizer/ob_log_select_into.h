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
        buffer_size_(DEFAULT_BUFFER_SIZE),
        escaped_cht_(),
        cs_type_(CS_TYPE_INVALID),
        file_partition_expr_(NULL),
        is_overwrite_(false),
        external_properties_(),
        external_partition_(),
        alias_names_()
  {
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
  inline void set_buffer_size(int64_t buffer_size)
  {
    buffer_size_ = buffer_size;
  }
  inline void set_closed_cht(common::ObObj &closed_cht)
  {
    closed_cht_ = closed_cht;
  }
  inline void set_escaped_cht(common::ObObj &escaped_cht)
  {
    escaped_cht_ = escaped_cht;
  }
  inline void set_cs_type(common::ObCollationType &cs_type)
  {
    cs_type_ = cs_type;
  }
  inline void set_file_partition_expr(sql::ObRawExpr* file_partition_expr)
  {
    file_partition_expr_ = file_partition_expr;
  }
  inline void set_is_overwrite(bool is_overwrite)
  {
    is_overwrite_ = is_overwrite;
  }
  inline void set_external_properties(const common::ObString &external_properties)
  {
    external_properties_.assign_ptr(external_properties.ptr(), external_properties.length());
  }
  inline void set_external_partition(const common::ObString &external_partition)
  {
    external_partition_.assign_ptr(external_partition.ptr(), external_partition.length());
  }
  inline void set_alias_names(common::ObIArray<common::ObString> &alias_names)
  {
    int ret = common::OB_SUCCESS;
    for (int i = 0 ; OB_SUCC(ret) && i < alias_names.count() ; ++i) {
      if (OB_FAIL(alias_names_.push_back(alias_names.at(i)))) {
        SQL_OPT_LOG(WARN, "push back failed", K(ret));
      }
    }
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
  inline int64_t get_buffer_size() const
  {
    return buffer_size_;
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
  inline sql::ObRawExpr* get_file_partition_expr() const
  {
    return file_partition_expr_;
  }
  inline bool get_is_overwrite() const
  {
    return is_overwrite_;
  }
  inline common::ObString get_external_properties() const
  {
    return external_properties_;
  }
  inline common::ObString get_external_partition() const
  {
    return external_partition_;
  }
  inline const common::ObIArray<common::ObString> &get_alias_names() const
  {
    return alias_names_;
  }
  inline common::ObIArray<common::ObString> &get_alias_names()
  {
    return alias_names_;
  }
  const common::ObIArray<ObRawExpr*> &get_select_exprs() const { return select_exprs_; }
  common::ObIArray<ObRawExpr*> &get_select_exprs() { return select_exprs_; }
  virtual int est_cost() override;
  virtual int compute_plan_type() override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer);
  static const int64_t DEFAULT_MAX_FILE_SIZE = 256*1024*1024;
  static const int64_t DEFAULT_BUFFER_SIZE = 1*1024*1024;
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
  int64_t buffer_size_;
  common::ObObj escaped_cht_;
  common::ObCollationType cs_type_;
  sql::ObRawExpr* file_partition_expr_;
  bool is_overwrite_;
  common::ObString external_properties_;
  common::ObString external_partition_;
  common::ObSEArray<common::ObString, 8, common::ModulePageAllocator, true> alias_names_;
};
}
}


#endif /* SRC_SQL_OPTIMIZER_OB_LOG_SELECT_INTO_H_ */
