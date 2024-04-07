/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_STORAGE_COLUMN_STORE_OB_CO_WHERE_OPTIMIZER_H_
#define OB_STORAGE_COLUMN_STORE_OB_CO_WHERE_OPTIMIZER_H_
#include "sql/engine/basic/ob_pushdown_filter.h"

namespace oceanbase
{
namespace storage
{
class ObCOSSTableV2;

class ObCOWhereOptimizer
{
public:
  ObCOWhereOptimizer(
      ObCOSSTableV2 &co_sstable,
      sql::ObPushdownFilterExecutor &filter);
  ~ObCOWhereOptimizer() = default;
  int analyze();

private:
  struct ObFilterCondition
  {
    uint64_t idx_;
    uint64_t columns_size_;
    uint64_t columns_cnt_;
    uint64_t execution_cost_;

    bool operator< (const ObFilterCondition &filter_condition) const {
      bool ret = false;
      if (this->execution_cost_ < filter_condition.execution_cost_) {
        ret = true;
      } else if (this->execution_cost_ > filter_condition.execution_cost_) {
        ret = false;
      } else if (this->columns_size_ < filter_condition.columns_size_) {
        ret = true;
      } else if (this->columns_size_ > filter_condition.columns_size_) {
        ret = false;
      } else {
        ret = this->columns_cnt_ < filter_condition.columns_cnt_;
      }
      return ret;
    }

    TO_STRING_KV(K_(idx), K_(columns_size), K_(columns_cnt), K_(execution_cost));
  };

  using ObFilterConditions = common::ObSEArray<ObFilterCondition, 4>;

  int analyze_impl(sql::ObPushdownFilterExecutor &filter);
  int collect_filter_info(
      sql::ObPushdownFilterExecutor &filter,
      ObFilterCondition &filter_condition);
  uint64_t estimate_execution_cost(sql::ObPushdownFilterExecutor &filter);
  bool can_choose_best_filter(
      ObFilterCondition *best_filter_condition,
      sql::ObPushdownFilterExecutor &best_filter,
      sql::ObPushdownFilterExecutor &parent_filter);

private:
  ObCOSSTableV2 &co_sstable_;
  sql::ObPushdownFilterExecutor &filter_;
  ObFilterConditions filter_conditions_;
};

}
}

#endif
