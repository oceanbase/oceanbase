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
#ifndef OB_STORAGE_COLUMN_STORE_OB_WHERE_OPTIMIZER_H_
#define OB_STORAGE_COLUMN_STORE_OB_WHERE_OPTIMIZER_H_ 
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/column_store/ob_i_cg_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObWhereOptimizer
{
public:
  ObWhereOptimizer();
  virtual ~ObWhereOptimizer() { reset(); };
  int init(
    const ObTableIterParam *iter_param,
    sql::ObPushdownFilterExecutor *filter,
    ObSEArray<ObICGIterator*, 4> *filter_iters = nullptr,
    ObSEArray<sql::ObPushdownFilterExecutor*, 4> *iter_filter_node = nullptr);
  void reset();
  void reuse();
  OB_INLINE bool is_disable_bypass()  // Disable bypass in this batch and collect real-time statistics, then reorder filter before next batch.
  { return reorder_filter_times_ == reorder_filter_interval_; }
  OB_INLINE bool is_first_batch()
  { return batch_num_ == 1; }
  int reorder_co_filter();
  int reorder_row_filter();
  TO_STRING_KV(KP(this), KP_(iter_param), KP_(filter), KP_(filter_iters), KP_(iter_filter_node),
    K_(batch_num), K_(reorder_filter_times), K_(reorder_filter_interval), K_(disable_bypass), K_(is_inited));

private:
  struct ObFilterCondition
  {
    uint64_t idx_;
    uint64_t filter_cost_time_;
    uint64_t filtered_row_cnt_;
    uint64_t skip_index_skip_mb_cnt_;
    sql::ObPushdownFilterExecutor *filter_;
    ObICGIterator *filter_iter_;
    sql::ObPushdownFilterExecutor *filter_node_;
  
    bool operator< (const ObFilterCondition &filter_condition) const {
      bool ret = false;
      if (skip_index_skip_mb_cnt_ != filter_condition.skip_index_skip_mb_cnt_) {
        ret = skip_index_skip_mb_cnt_ > filter_condition.skip_index_skip_mb_cnt_;
      } else if (filter_cost_time_ == 0 || filter_condition.filter_cost_time_ == 0) {
        ret = !(filter_cost_time_ == 0);
      } else {
        float rank1 = - 1.0 * filtered_row_cnt_ / filter_cost_time_;
        float rank2 = - 1.0 * filter_condition.filtered_row_cnt_ / filter_condition.filter_cost_time_;
        ret = rank1 < rank2;
      }
      return ret;
    }

    TO_STRING_KV(K_(idx), K_(filter_cost_time), K_(filtered_row_cnt), K_(skip_index_skip_mb_cnt));
  };

  int analyze(bool &reordered);
  int analyze_impl(sql::ObPushdownFilterExecutor &filter, bool &reordered);
  void collect_filter_info(sql::ObPushdownFilterExecutor &filter, ObFilterCondition &filter_condition);
  void judge_filter_whether_enable_reorder(sql::ObPushdownFilterExecutor *filter);

private:
  const ObTableIterParam *iter_param_;
  sql::ObPushdownFilterExecutor *filter_;
  ObSEArray<ObICGIterator*, 4> *filter_iters_;
  ObSEArray<sql::ObPushdownFilterExecutor*, 4> *iter_filter_node_;
  ObSEArray<ObFilterCondition, 4> filter_conditions_;
  uint32_t batch_num_;
  uint32_t reorder_filter_times_;
  uint32_t reorder_filter_interval_;
  bool disable_bypass_;
  bool is_inited_;
};

}
}

#endif
