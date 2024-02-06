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

#ifndef OB_HYBRID_HIST_ESTIMATOR_H
#define OB_HYBRID_HIST_ESTIMATOR_H

#include "share/stat/ob_stats_estimator.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_table_stat.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
namespace oceanbase
{
namespace common
{

struct BucketDesc
{
  BucketDesc() : ep_count_(0), is_pop_(false) {}
  int64_t ep_count_;
  bool is_pop_;
};

struct BucketNode
{
  BucketNode() : ep_val_(), ep_count_(0), is_pop_(false) {}
  BucketNode(const ObObj &obj, int64_t ep_count) :
    ep_val_(obj), ep_count_(ep_count), is_pop_(false)
  {}

  TO_STRING_KV(K(ep_val_), K(ep_count_), K(is_pop_));

  ObObj ep_val_;
  int64_t ep_count_;
  bool is_pop_;
};

class ObHybridHistograms
{
  OB_UNIS_VERSION(1);
public:
  ObHybridHistograms()
  : total_count_(0),
    num_distinct_(0),
    pop_count_(0),
    pop_freq_(0),
    hybrid_buckets_()
  {}

  int read_result(const ObObj &result_obj);
  const ObIArray<ObHistBucket> &get_buckets() const { return hybrid_buckets_; }
  int64_t get_bucket_size() { return hybrid_buckets_.count(); }
  int add_hist_bucket(ObHistBucket &hist_bucket) {
    return hybrid_buckets_.push_back(hist_bucket);
  }
  int64_t get_total_count() { return total_count_; }
  int64_t get_num_distinct() { return num_distinct_; }
  int64_t get_pop_count() { return pop_count_; }
  int64_t get_pop_freq() { return pop_freq_; }
  int build_hybrid_hist(ObIArray<BucketNode> &bucket_pairs,
                        int64_t bucket_num,
                        int64_t total_count,
                        int64_t num_distinct);
  int build_hybrid_hist(sql::ObAggregateProcessor::HybridHistExtraResult *extra,
                        ObIAllocator *alloc,
                        int64_t bucket_num,
                        int64_t total_count,
                        int64_t num_distinct,
                        int64_t pop_count,
                        int64_t pop_freq,
                        const ObObjMeta &obj_meta);
  TO_STRING_KV(K_(total_count),
               K_(num_distinct),
               K_(pop_count),
               K_(pop_freq));
private:
  int64_t total_count_;
  int64_t num_distinct_;
  int64_t pop_count_;
  int64_t pop_freq_;
  common::ObSEArray<ObHistBucket, 16, common::ModulePageAllocator, true> hybrid_buckets_;
};


class ObHybridHistEstimator : public ObStatsEstimator
{
public:
  explicit ObHybridHistEstimator(ObExecContext &ctx, ObIAllocator &allocator);

  int estimate(const ObTableStatParam &param,
               ObExtraParam &extra,
               ObIArray<ObOptStat> &dst_opt_stats);

  template <class T>
  int add_stat_item(const T &item, ObIArray<ObStatItem *> &stat_items);

private:

  int try_build_hybrid_hist(const ObColumnStatParam &param,
                            ObOptColumnStat &col_stat,
                            ObOptTableStat &table_stat,
                            bool &is_done);

  int compute_estimate_percent(int64_t total_row_count,
                               int64_t max_num_buckets,
                               const ObAnalyzeSampleInfo &sample_info,
                               bool &need_sample,
                               double &est_percent,
                               bool &is_block_sample);

  int gen_union_all_sql(ObIAllocator &allocator,
                        const ObString &raw_sql1,
                        const ObString &raw_sql2,
                        ObString &merge_raw_sql);

  int refine_hybrid_hist(ObIAllocator &allocator,
                         const ObTableStatParam &param,
                         ObExtraParam &extra,
                         int64_t total_refine_cnt,
                         ObOptStat &src_opt_stat,
                         ObOptStat &dst_opt_stat,
                         ObIArray<ObOptStat> &dst_opt_stats,
                         ObIArray<int64_t> &refine_col_ids,
                         ObIArray<ObStatItem *> &refine_stat_items,
                         ObIArray<ObOptColumnStat *> &refine_col_stats,
                         ObString &refine_raw_sql);

  int gen_query_sql(ObIAllocator &allocator,
                    const ObTableStatParam &param,
                    ObExtraParam &extra,
                    const ObString &simple_hint,
                    int64_t part_cnt,
                    ObOptStat &src_opt_stat,
                    ObOptStat &dst_opt_stat,
                    ObIArray<ObOptStat> &dst_opt_stats,
                    ObIArray<ObStatItem *> &stat_items,
                    ObIArray<ObOptColumnStat *> &refine_col_stats,
                    ObString &raw_sql);

  int reset_histogram_for_refine_col_stats(ObIArray<ObOptColumnStat *> &refine_col_stats);

  int need_add_hybrid_hist_item(ObOptColumnStat *dst_col_stat,
                                ObOptTableStat *dst_tab_stat,
                                const ObColumnStatParam &col_param,
                                double est_percent,
                                bool need_sample,
                                bool &is_need,
                                bool &need_refine);

  void get_max_num_buckets(const ObIArray<ObColumnStatParam> &col_params,
                           int64_t &max_num_buckets);

private:
  /// a magic number
  static const int64_t AUTO_SAMPLE_SIZE;
};

}
}

#endif // OB_HYBRID_HIST_ESTIMATOR_H
