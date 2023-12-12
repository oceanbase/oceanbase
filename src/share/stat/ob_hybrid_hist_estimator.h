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
  static int build_prefix_str_datum_for_lob(ObIAllocator &allocator,
                                            const ObObjMeta &obj_meta,
                                            const ObDatum &old_datum,
                                            ObDatum &new_datum);
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


class ObHybridHistEstimator : public ObBasicStatsEstimator
{
public:
  explicit ObHybridHistEstimator(ObExecContext &ctx, ObIAllocator &allocator);

  int estimate(const ObOptStatGatherParam &param,
               ObOptStat &opt_stat);
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

  int reset_histogram_for_refine_col_stats(ObIArray<ObOptColumnStat *> &refine_col_stats);

  int extract_hybrid_hist_col_info(const ObOptStatGatherParam &param,
                                   ObOptStat &opt_stat,
                                   ObIArray<const ObColumnStatParam *> &hybrid_col_params,
                                   ObIArray<ObOptColumnStat *> &hybrid_col_stats,
                                   int64_t &max_num_buckets);

  int add_hybrid_hist_stat_items(ObIArray<const ObColumnStatParam *> &hybrid_col_params,
                                 ObIArray<ObOptColumnStat *> &hybrid_col_stats,
                                 const int64_t table_row_cnt,
                                 const bool need_sample,
                                 const double est_percent,
                                 ObIArray<int64_t> &no_sample_idx);

  int estimate_no_sample_col_hydrid_hist(ObIAllocator &allocator,
                                         const ObOptStatGatherParam &param,
                                         ObOptStat &opt_stat,
                                         ObIArray<const ObColumnStatParam *> &hybrid_col_params,
                                         ObIArray<ObOptColumnStat *> &hybrid_col_stats,
                                         ObIArray<int64_t> &no_sample_idx);

   int add_no_sample_hybrid_hist_stat_items(ObIArray<const ObColumnStatParam *> &hybrid_col_params,
                                            ObIArray<ObOptColumnStat *> &hybrid_col_stats,
                                            ObIArray<int64_t> &no_sample_idx);
};

}
}

#endif // OB_HYBRID_HIST_ESTIMATOR_H
