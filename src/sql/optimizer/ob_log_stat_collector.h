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

#ifndef OCEANBASE_SQL_OB_LOG_STAT_COLLECTOR_H_
#define OCEANBASE_SQL_OB_LOG_STAT_COLLECTOR_H_
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/engine/px/ob_px_basic_info.h"

namespace oceanbase
{
namespace sql
{

class ObLogStatCollector : public ObLogicalOperator
{
public:
  ObLogStatCollector(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        sort_keys_(),
        is_none_partition_(),
        type_(ObStatCollectorType::NOT_INIT_TYPE)
        {}
  virtual ~ObLogStatCollector() {}
  virtual const char *get_name() const;

  int set_sort_keys(const common::ObIArray<OrderItem> &order_keys);
  common::ObIArray<OrderItem> &get_sort_keys() { return sort_keys_; }
  void set_stat_collector_type(ObStatCollectorType type) { type_ = type; }
  ObStatCollectorType get_stat_collector_type() { return type_; }
  void set_is_none_partition(bool flag) { is_none_partition_ = flag; }
  bool get_is_none_partition() { return is_none_partition_;   }
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
private:
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> sort_keys_;
  bool is_none_partition_;
  ObStatCollectorType type_;
};


}
}

#endif
