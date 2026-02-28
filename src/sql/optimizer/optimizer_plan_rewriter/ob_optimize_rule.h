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

#ifndef OCEANBASE_SQL_OB_OPTIMIZE_RULE_H
#define OCEANBASE_SQL_OB_OPTIMIZE_RULE_H

#include <typeinfo>
#include "sql/ob_sql_define.h"

namespace oceanbase
{
namespace sql
{

// Forward declarations
class ObLogPlan;
class ObOptimizerContext;

struct ObRuleResult {
  ObRuleResult()
    : is_enabled_(false),
      is_applicable_(false),
      is_transformed_(false)
  {
  }
  virtual ~ObRuleResult()
  {
  }
  bool is_enabled() const { return is_enabled_; }
  bool is_applicable() const { return is_applicable_; }
  bool is_transformed() const { return is_transformed_; }
  void set_enabled(bool is_enabled) { is_enabled_ = is_enabled; }
  void set_applicable(bool is_applicable) { is_applicable_ = is_applicable; }
  void set_transformed(bool is_transformed) { is_transformed_ = is_transformed; }

  bool is_enabled_;
  bool is_applicable_;
  bool is_transformed_;

  TO_STRING_KV(K_(is_enabled), K_(is_applicable), K_(is_transformed));
};
class ObOptimizeRule
{
  public:
  ObOptimizeRule();
  virtual ~ObOptimizeRule();
  virtual int apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx);
  virtual int apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx, ObRuleResult& result);
  virtual bool is_enabled(const ObOptimizerContext& ctx) {
    return true;
  }

  virtual ObString get_name() const {
    // by default return class name
    return ObString::make_string(typeid(*this).name());
  }

  int64_t to_string(char *buf, const int64_t buf_len) const {
    ObString name = get_name();
    return name.to_string(buf, buf_len);
  }
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_OPTIMIZE_RULE_H
