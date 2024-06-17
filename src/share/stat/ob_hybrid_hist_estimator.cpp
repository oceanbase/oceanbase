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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_hybrid_hist_estimator.h"
#include "observer/ob_sql_client_decorator.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

ObHybridHistEstimator::ObHybridHistEstimator(ObExecContext &ctx, ObIAllocator &allocator)
  : ObBasicStatsEstimator(ctx, allocator)
{}

int ObHybridHistEstimator::estimate(const ObOptStatGatherParam &param,
                                    ObOptStat &opt_stat)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObColumnStatParam *, 4> hybrid_col_params;
  ObSEArray<ObOptColumnStat *, 4> hybrid_col_stats;
  bool need_sample = false;
  bool is_block_sample = false;
  double est_percent = 0.0;
  int64_t max_num_buckets = 0;
  ObSEArray<int64_t, 4> no_sample_idx;
  ObArenaAllocator allocator("ObHybridHistEst", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  ObSqlString raw_sql;
  int64_t duration_time = -1;
  ObSEArray<ObOptStat, 1> tmp_opt_stats;
  if (OB_FAIL(extract_hybrid_hist_col_info(param, opt_stat,
                                           hybrid_col_params,
                                           hybrid_col_stats,
                                           max_num_buckets))) {
    LOG_WARN("failed to extract hybrid hist col info", K(ret));
  } else if (hybrid_col_params.empty() || hybrid_col_stats.empty()) {
    //do nothing
  } else if (OB_UNLIKELY(hybrid_col_params.count() != hybrid_col_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(hybrid_col_params.count()), K(hybrid_col_stats.count()));
  } else if (OB_FAIL(compute_estimate_percent(opt_stat.table_stat_->get_row_count(),
                                              max_num_buckets,
                                              param.sample_info_,
                                              need_sample,
                                              est_percent,
                                              is_block_sample))) {
    LOG_WARN("failed to compute estimate percent", K(ret));
  } else if (need_sample && OB_FAIL(fill_sample_info(allocator,
                                                     est_percent,
                                                     is_block_sample))) {
    LOG_WARN("failed to fill sample info", K(ret));
  } else if (OB_FAIL(add_hybrid_hist_stat_items(hybrid_col_params,
                                                hybrid_col_stats,
                                                opt_stat.table_stat_->get_row_count(),
                                                need_sample,
                                                est_percent,
                                                no_sample_idx))) {
    LOG_WARN("failed to add hybrid hist stat items", K(ret));
  } else if (OB_FAIL(fill_hints(allocator, param.tab_name_, param.gather_vectorize_, false))) {
    LOG_WARN("failed to fill hints", K(ret));
  } else if (OB_FAIL(add_from_table(param.db_name_, param.tab_name_))) {
    LOG_WARN("failed to add from table", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to fill parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(param.gather_start_time_,
                                                               param.max_duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (!param.partition_infos_.empty() &&
             OB_FAIL(fill_partition_info(allocator, param.partition_infos_.at(0).part_name_))) {
    LOG_WARN("failed to add partition info", K(ret));
  } else if (OB_FAIL(fill_specify_scn_info(allocator, param.sepcify_scn_))) {
    LOG_WARN("failed to fill specify scn info", K(ret));
  } else if (OB_FAIL(pack(raw_sql))) {
    LOG_WARN("failed to pack", K(ret));
  } else if (OB_FAIL(tmp_opt_stats.push_back(opt_stat))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (get_item_size() > 0 &&
             OB_FAIL(do_estimate(param.tenant_id_, raw_sql.string(), false,
                                 opt_stat, tmp_opt_stats))) {
    LOG_WARN("failed to do estimate", K(ret));
  } else if (!no_sample_idx.empty() &&
             OB_FAIL(estimate_no_sample_col_hydrid_hist(allocator, param, opt_stat,
                                                        hybrid_col_params, hybrid_col_stats,
                                                        no_sample_idx))) {
    LOG_WARN("failed to estimate no sample col hydrid_hist", K(ret));
  } else {
    LOG_TRACE("succeed to build hybrid histogram", K(hybrid_col_stats));
  }
  return ret;
}

int ObHybridHistEstimator::extract_hybrid_hist_col_info(const ObOptStatGatherParam &param,
                                                        ObOptStat &opt_stat,
                                                        ObIArray<const ObColumnStatParam *> &hybrid_col_params,
                                                        ObIArray<ObOptColumnStat *> &hybrid_col_stats,
                                                        int64_t &max_num_buckets)
{
  int ret = OB_SUCCESS;
  max_num_buckets = 0;
  if (OB_UNLIKELY(param.column_params_.count() != opt_stat.column_stats_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.column_params_), K(opt_stat.column_stats_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < opt_stat.column_stats_.count(); ++i) {
      bool is_done = false;
      if (OB_ISNULL(opt_stat.table_stat_) ||
          OB_ISNULL(opt_stat.column_stats_.at(i)) ||
          OB_UNLIKELY(opt_stat.column_stats_.at(i)->get_column_id() != param.column_params_.at(i).column_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(opt_stat.column_stats_.at(i)),
                                         K(opt_stat.table_stat_), K(param.column_params_.at(i)));
      } else if (opt_stat.table_stat_->get_row_count() <=0 ||
                 !opt_stat.column_stats_.at(i)->get_histogram().is_hybrid()) {
        //do nothing
      } else if (OB_FAIL(try_build_hybrid_hist(param.column_params_.at(i),
                                               *opt_stat.column_stats_.at(i),
                                               *opt_stat.table_stat_,
                                               is_done))) {
        LOG_WARN("failed to try build hybrid hist", K(ret));
      } else if (is_done) {
        //do nothing
      } else if (OB_FAIL(hybrid_col_params.push_back(&param.column_params_.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(hybrid_col_stats.push_back(opt_stat.column_stats_.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        opt_stat.column_stats_.at(i)->get_histogram().reset();
        opt_stat.column_stats_.at(i)->get_histogram().set_type(ObHistType::HYBIRD);
        max_num_buckets = std::max(max_num_buckets, param.column_params_.at(i).bucket_num_);
      }
    }
    LOG_TRACE("succeed to extract hybrid hist col info", K(param), K(hybrid_col_params), K(hybrid_col_stats));
  }
  return ret;
}

int ObHybridHistEstimator::add_hybrid_hist_stat_items(ObIArray<const ObColumnStatParam *> &hybrid_col_params,
                                                      ObIArray<ObOptColumnStat *> &hybrid_col_stats,
                                                      const int64_t table_row_cnt,
                                                      const bool need_sample,
                                                      const double est_percent,
                                                      ObIArray<int64_t> &no_sample_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(hybrid_col_params.count() != hybrid_col_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(hybrid_col_params), K(hybrid_col_stats));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < hybrid_col_stats.count(); ++i) {
      if (OB_ISNULL(hybrid_col_params.at(i)) || OB_ISNULL(hybrid_col_stats.at(i)) ||
          OB_UNLIKELY(hybrid_col_params.at(i)->column_id_ != hybrid_col_stats.at(i)->get_column_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(hybrid_col_stats.at(i)), KPC(hybrid_col_params.at(i)));
      } else if (!need_sample || hybrid_col_stats.at(i)->get_num_not_null() > table_row_cnt * est_percent / 100) {
        if (OB_FAIL(add_stat_item(ObStatHybridHist(hybrid_col_params.at(i),
                                                   hybrid_col_stats.at(i))))) {
          LOG_WARN("failed to add stat item", K(ret));
        }
      //if col not null cnt is less than expected sample row cnt, then no need sample.
      } else if (OB_FAIL(no_sample_idx.push_back(i))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObHybridHistEstimator::estimate_no_sample_col_hydrid_hist(ObIAllocator &allocator,
                                                              const ObOptStatGatherParam &param,
                                                              ObOptStat &opt_stat,
                                                              ObIArray<const ObColumnStatParam *> &hybrid_col_params,
                                                              ObIArray<ObOptColumnStat *> &hybrid_col_stats,
                                                              ObIArray<int64_t> &no_sample_idx)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  int64_t duration_time = -1;
  ObSEArray<ObOptStat, 1> tmp_opt_stats;
  if (no_sample_idx.empty()) {
    //do nothing
  //reset select item if exists;
  } else if (FALSE_IT(reset_select_items())) {
  //reset sample hint if exists;
  } else if (FALSE_IT(reset_sample_hint())) {
  //reset other hint
  } else if (FALSE_IT(reset_other_hint())) {
  //add stat items
  } else if (OB_FAIL(add_no_sample_hybrid_hist_stat_items(hybrid_col_params,
                                                          hybrid_col_stats,
                                                          no_sample_idx))) {
    LOG_WARN("failed to add no sample hybrid hist stat items", K(ret));
  } else if (OB_FAIL(fill_hints(allocator, param.tab_name_, param.gather_vectorize_, false))) {
    LOG_WARN("failed to fill hints", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(param.gather_start_time_,
                                                               param.max_duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(pack(raw_sql))) {
    LOG_WARN("failed to pack", K(ret));
  } else if (OB_FAIL(tmp_opt_stats.push_back(opt_stat))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(do_estimate(param.tenant_id_, raw_sql.string(), false,
                                 opt_stat, tmp_opt_stats))) {
    LOG_WARN("failed to do estimate", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObHybridHistEstimator::add_no_sample_hybrid_hist_stat_items(ObIArray<const ObColumnStatParam *> &hybrid_col_params,
                                                                ObIArray<ObOptColumnStat *> &hybrid_col_stats,
                                                                ObIArray<int64_t> &no_sample_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(no_sample_idx.empty() || hybrid_col_params.count() != hybrid_col_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(no_sample_idx), K(hybrid_col_params), K(hybrid_col_stats));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < no_sample_idx.count(); ++i) {
      int64_t idx = no_sample_idx.at(i);
      if (OB_UNLIKELY(idx >= hybrid_col_params.count()) ||
          OB_ISNULL(hybrid_col_params.at(idx)) || OB_ISNULL(hybrid_col_stats.at(idx)) ||
          OB_UNLIKELY(hybrid_col_params.at(idx)->column_id_ != hybrid_col_stats.at(idx)->get_column_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(idx), K(hybrid_col_stats), K(hybrid_col_params));
      } else if (OB_FAIL(add_stat_item(ObStatHybridHist(hybrid_col_params.at(idx),
                                                        hybrid_col_stats.at(idx))))) {
        LOG_WARN("failed to add stat item", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObDbmsStatsUtils::try_build_hybrid_hist
 * 计算频率直方图的时候会额外收集一些 bucket，如果 bucket 数量超过 ndv，那么可以直接构造混合直方图
 * @param col_stat
 * @return
 */
int ObHybridHistEstimator::try_build_hybrid_hist(const ObColumnStatParam &param,
                                                 ObOptColumnStat &col_stat,
                                                 ObOptTableStat &table_stat,
                                                 bool &is_done)
{
  int ret = OB_SUCCESS;
  is_done = false;
  int64_t num_distinct = col_stat.get_num_distinct();
  int64_t bucket_num = param.bucket_num_;
  if (bucket_num >= num_distinct && col_stat.get_histogram().get_bucket_size() > 0) {
    int64_t null_count = col_stat.get_num_null();
    int64_t total_count = table_stat.get_row_count() - null_count;

    ObSEArray<BucketNode, 4> pairs;

    for (int64_t i = 0; OB_SUCC(ret) && i < col_stat.get_histogram().get_bucket_size(); ++i) {
      const ObHistBucket &hist_bucket = col_stat.get_histogram().get(i);
      if (OB_FAIL(pairs.push_back(BucketNode(hist_bucket.endpoint_value_,
                                             hist_bucket.endpoint_repeat_count_)))) {
        LOG_WARN("failed to push back new entry", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      ObHybridHistograms hybrid_hist;
      if (OB_FAIL(hybrid_hist.build_hybrid_hist(pairs,
                                                bucket_num,
                                                total_count,
                                                num_distinct))) {
        LOG_WARN("failed to do build hybrid hist", K(ret));
      } else {
        col_stat.get_histogram().get_buckets().reset();
        if (OB_FAIL(col_stat.get_histogram().prepare_allocate_buckets(allocator_,
                                                                      hybrid_hist.get_buckets().count()))) {
          LOG_WARN("failed to prepare allocate buckets", K(ret));
        } else if (OB_FAIL(col_stat.get_histogram().assign_buckets(hybrid_hist.get_buckets()))) {
          LOG_WARN("failed to assign buckets", K(ret));
        } else {
          col_stat.get_histogram().set_type(ObHistType::HYBIRD);
          col_stat.get_histogram().set_sample_size(total_count);
          col_stat.get_histogram().calc_density(ObHistType::HYBIRD,
                                                total_count,
                                                hybrid_hist.get_pop_freq(),
                                                num_distinct,
                                                hybrid_hist.get_pop_count());
          is_done = true;
          LOG_TRACE("succeed to build hybrid hist", K(hybrid_hist), K(col_stat));
        }
      }
    }
  }
  return ret;
}

/*@brief ObHybridHistEstimator::compute_estimate_percent, compute estimate percent.
 * Base on Oracle 12c relation guide and test to set:
 * 1.not specify estimate percent:
 *  a. if total_row_count < MAGIC_MAX_AUTO_SAMPLE_SIZE then choosing full table scan;
 *  b. if total_row_count >= MAGIC_MAX_AUTO_SAMPLE_SIZE then:
 *      i: if max_num_bkts <= DEFAULT_HISTOGRAM_BUCKET_NUM then choosing MAGIC_SAMPLE_SIZE;
 *      ii: if max_num_bkts > DEFAULT_HISTOGRAM_BUCKET_NUM:
 *          (1): if max_num_bkts >= total_row_count * MAGIC_SAMPLE_CUT_RATIO then choosing full table scan;
 *          (2): if max_num_bkts <= total_row_count * MAGIC_SAMPLE_CUT_RATIO then choosing:
 *               sample_size = MAGIC_SAMPLE_SIZE + MAGIC_BASE_SAMPLE_SIZE + (max_num_bkts -
 *                               DEFAULT_HISTOGRAM_BUCKET_NUM) * MAGIC_MIN_SAMPLE_SIZE * 0.01;
 *
 * 2.specify estimate percent:
 *  a. if total_row_count * percent >= MAGIC_MIN_SAMPLE_SIZE then choose specify percent;
 *  b. if total_row_count * percent < MAGIC_MIN_SAMPLE_SIZE then:
 *     i: if total_row_count <= MAGIC_SAMPLE_SIZE then choosing full table scan;
 *    ii: if total_row_count > MAGIC_SAMPLE_SIZE then choosing MAGIC_SAMPLE_SIZE to sample;
 */
int ObHybridHistEstimator::compute_estimate_percent(int64_t total_row_count,
                                                    int64_t max_num_bkts,
                                                    const ObAnalyzeSampleInfo &sample_info,
                                                    bool &need_sample,
                                                    double &est_percent,
                                                    bool &is_block_sample)
{
  int ret = OB_SUCCESS;
  if (0 == total_row_count) {
    need_sample = false;
  } else if (sample_info.is_sample_) {
    need_sample = true;
    is_block_sample = sample_info.is_block_sample_;
    if (sample_info.sample_type_ == SampleType::RowSample) {
      if (sample_info.sample_value_ < total_row_count) {
        est_percent = (sample_info.sample_value_ * 100.0) / total_row_count;
      } else {
        need_sample = false;
      }
    } else if (sample_info.sample_type_ == SampleType::PercentSample) {
      est_percent = (sample_info.sample_value_ * 1.0);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sample type", K(ret));
    }
    if (OB_SUCC(ret) && need_sample) {
      if (total_row_count * est_percent / 100 >= MAGIC_MIN_SAMPLE_SIZE) {
        const int64_t MAGIC_MAX_SPECIFY_SAMPLE_SIZE = 1000000;
        is_block_sample = !is_block_sample ? total_row_count >= MAX_AUTO_GATHER_FULL_TABLE_ROWS : is_block_sample;
        int64_t max_allowed_multiple = max_num_bkts <= ObColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM ? 1 :
                                                    max_num_bkts / ObColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM;
        int64_t max_specify_sample_size = MAGIC_MAX_SPECIFY_SAMPLE_SIZE * max_allowed_multiple;
        if (total_row_count * est_percent / 100 >= max_specify_sample_size) {
          est_percent = max_specify_sample_size * 100.0 / total_row_count;
        }
      } else if (total_row_count <= MAGIC_SAMPLE_SIZE) {
        need_sample = false;
        est_percent = 0.0;
        is_block_sample = false;
      } else {
        is_block_sample = total_row_count >= MAX_AUTO_GATHER_FULL_TABLE_ROWS;
        est_percent = (MAGIC_SAMPLE_SIZE * 100.0) / total_row_count;
      }
    }
  } else if (total_row_count >= MAX_AUTO_GATHER_FULL_TABLE_ROWS) {
    need_sample = true;
    is_block_sample = true;
    const int64_t MAGIC_MAX_SAMPLE_SIZE = 100000;
    est_percent = MAGIC_MAX_SAMPLE_SIZE * 100.0 / total_row_count;
  } else if (total_row_count >= MAGIC_MAX_AUTO_SAMPLE_SIZE) {
    if (max_num_bkts <= ObColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM) {
      need_sample = true;
      is_block_sample = false;
      est_percent = (MAGIC_SAMPLE_SIZE * 100.0) / total_row_count;
    } else {
      int64_t num_bound_bkts = static_cast<int64_t>(std::round(total_row_count * MAGIC_SAMPLE_CUT_RATIO));
      if (max_num_bkts >= num_bound_bkts) {
        need_sample = false;
      } else {
        int64_t sample_size = MAGIC_SAMPLE_SIZE + MAGIC_BASE_SAMPLE_SIZE + (max_num_bkts -
                    ObColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM) * MAGIC_MIN_SAMPLE_SIZE * 0.01;
        need_sample = true;
        is_block_sample = false;
        est_percent = (sample_size * 100.0) / total_row_count;
      }
    }
  } else {
    need_sample = false;
  }
  if (OB_SUCC(ret)) {
    // refine est_percent
    est_percent = std::max(0.000001, est_percent);
    if (est_percent >= 100) {
      need_sample = false;
    }
  }

  LOG_TRACE("Succeed to compute estimate percent", K(ret), K(total_row_count), K(max_num_bkts),
                                                K(need_sample), K(est_percent), K(is_block_sample));
  return ret;
}

int ObHybridHistograms::read_result(const ObObj &result_obj)
{
  int ret = OB_SUCCESS;
  ObString result_str;
  int64_t pos = 0;
  if (result_obj.is_null()) {
    // do nothing
  } else if (OB_UNLIKELY(!result_obj.is_lob())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob is expected", K(ret));
  } else if (OB_FAIL(result_obj.get_string(result_str))) {
    LOG_WARN("failed to get string", K(ret));
  } else if (OB_FAIL(deserialize(result_str.ptr(), result_str.length(), pos))) {
    LOG_WARN("failed to deserialize histograms from buffer", K(ret));
  }
  return ret;
}

int ObHybridHistograms::build_hybrid_hist(ObIArray<BucketNode> &bucket_pairs,
                                          int64_t bucket_num,
                                          int64_t total_count,
                                          int64_t num_distinct)
{
  int ret = OB_SUCCESS;
  int64_t bucket_size = -1;
  int64_t pop_count = 0;
  int64_t pop_freq = 0;
  bool dynamic_size = false;
  int64_t dynamic_step = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < bucket_pairs.count(); ++i) {
    if (bucket_pairs.at(i).ep_count_ > total_count / bucket_num) {
      ++ pop_count;
      pop_freq += bucket_pairs.at(i).ep_count_;
      bucket_pairs.at(i).is_pop_ = true;
    }
  }

  // determine bucket size
  if (OB_SUCC(ret)) {
    if (num_distinct <= bucket_num + 2) {
      bucket_size = 1;
    } else if (bucket_num <= pop_count) {
      bucket_size = total_count / bucket_num;
    } else {
      dynamic_size = true;
      // first bucket always contain only one values. following code will handle first value is 
      // popular value or not.
      if (bucket_pairs.at(0).is_pop_ || bucket_num == pop_count + 1) {
        bucket_size = (total_count - pop_freq) / (bucket_num - pop_count);
      } else {
        bucket_size = (total_count - pop_freq - bucket_pairs.at(0).ep_count_)
                          / (bucket_num - pop_count - 1);
      }
    }
  }

  int64_t bucket_rows = 0;
  int64_t ep_num = 0;
  int64_t un_pop_count = 0;
  int64_t un_pop_bucket = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < bucket_pairs.count(); ++i) {
    bucket_rows += bucket_pairs.at(i).ep_count_;
    ep_num += bucket_pairs.at(i).ep_count_;
    if (!bucket_pairs.at(i).is_pop_) {
      un_pop_count += bucket_pairs.at(i).ep_count_;
    }
    if (bucket_rows > bucket_size || 0 == i || bucket_pairs.count() - 1 == i) {
      bucket_rows = 0;
      ObHistBucket bkt(bucket_pairs.at(i).ep_val_, bucket_pairs.at(i).ep_count_, ep_num);
      if (!bucket_pairs.at(i).is_pop_) {
        ++un_pop_bucket;
      }
      if (OB_FAIL(add_hist_bucket(bkt))) {
        LOG_WARN("failed add hist bucket", K(ret));
      }

      if (dynamic_size && bucket_num > pop_count + un_pop_bucket) {
        bucket_size = (total_count - pop_freq - un_pop_count)
                      / (bucket_num - pop_count - un_pop_bucket);
      }
    }
  }
  if (OB_SUCC(ret)) {
    total_count_ = total_count;
    num_distinct_ = num_distinct;
    pop_count_ = pop_count;
    pop_freq_ = pop_freq;
    LOG_TRACE("succeed to build hybrid histogram", K(bucket_num), K(bucket_size), K(total_count),
                            K(pop_count), K(pop_freq), K(num_distinct), K(hybrid_buckets_.count()));
  }
  return ret;
}
int ObHybridHistograms::build_hybrid_hist(ObAggregateProcessor::HybridHistExtraResult *extra,
                                          ObIAllocator *alloc,
                                          int64_t bucket_num,
                                          int64_t total_count,
                                          int64_t num_distinct,
                                          int64_t pop_count,
                                          int64_t pop_freq,
                                          const ObObjMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  int64_t bucket_size = -1;
  bool dynamic_size = false;
  int64_t dynamic_step = 0;
  const ObChunkDatumStore::StoredRow *row = nullptr;
  if (OB_ISNULL(extra) || OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(extra), K(alloc));
  } else if (num_distinct == 0) {
    // do nothing
  } else {
    // determine bucket size
    if (OB_FAIL(extra->get_next_row_from_material(row))) {
      LOG_WARN("failed to get next row from material");
    } else if (OB_ISNULL(row) || OB_UNLIKELY(row->cnt_ != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null stored row", K(row));
    } else if (num_distinct <= bucket_num + 2) {
      bucket_size = 1;
    } else if (bucket_num <= pop_count) {
      bucket_size = total_count / bucket_num;
    } else {
      dynamic_size = true;
      // first bucket always contain only one values. following code will handle first value is
      // popular value or not.
      BucketDesc *desc = reinterpret_cast<BucketDesc*>(row->get_extra_payload());
      if (desc->is_pop_ || bucket_num == pop_count + 1) {
        bucket_size = (total_count - pop_freq) / (bucket_num - pop_count);
      } else {
        bucket_size = (total_count - pop_freq - desc->ep_count_) / (bucket_num - pop_count - 1);
      }
    }

    int64_t bucket_rows = 0;
    int64_t ep_num = 0;
    int64_t un_pop_count = 0;
    int64_t un_pop_bucket = 0;
    int64_t i = 0;
    if (OB_SUCC(ret)) {
      do {
        if (OB_ISNULL(row) || OB_UNLIKELY(row->cnt_ != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null stored row", K(row));
        } else {
          BucketDesc *desc = reinterpret_cast<BucketDesc*>(row->get_extra_payload());
          int64_t ep_count = desc->ep_count_;
          bool is_pop = desc->is_pop_;
          bucket_rows += ep_count;
          ep_num += ep_count;
          if (!is_pop) {
            un_pop_count += ep_count;
          }
          if (bucket_rows > bucket_size || 0 == i || extra->get_material_row_count() - 1 == i) {
            bucket_rows = 0;
            ObObj ep_val;
            common::ObArenaAllocator tmp_alloctor("BulidHybridHist", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
            ObDatum new_datum = row->cells()[0];
            if (obj_meta.is_lob_storage() &&
                OB_FAIL(build_prefix_str_datum_for_lob(tmp_alloctor,
                                                       obj_meta, row->cells()[0],
                                                       new_datum))) {
              LOG_WARN("failed to build prefix str datum for lob", K(ret));
            } else if (OB_FAIL(new_datum.to_obj(ep_val, obj_meta))) {
              LOG_WARN("failed to obj", K(ret));
            } else if (OB_FAIL(ob_write_obj(*alloc, ep_val, ep_val))) {
              LOG_WARN("failed to write obj", K(ret), K(ep_val));
            } else {
              ObHistBucket bkt(ep_val, ep_count, ep_num);
              if (!is_pop) {
                ++un_pop_bucket;
              }
              if (OB_FAIL(add_hist_bucket(bkt))) {
                LOG_WARN("failed add hist bucket", K(ret));
              }
            }

            if (dynamic_size && bucket_num > pop_count + un_pop_bucket) {
              bucket_size = (total_count - pop_freq - un_pop_count)
                            / (bucket_num - pop_count - un_pop_bucket);
            }
          }
          ++i;
        }
        if (OB_SUCC(ret)) {
          ret = extra->get_next_row_from_material(row);
        }
      } while (OB_SUCC(ret));
    }

    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to build hybrid histogram");
    }
  }

  if (OB_SUCC(ret)) {
    total_count_ = total_count;
    num_distinct_ = num_distinct;
    pop_count_ = pop_count;
    pop_freq_ = pop_freq;
    LOG_TRACE("succeed to build hybrid histogram", K(bucket_num), K(bucket_size), K(total_count),
                            K(pop_count), K(pop_freq), K(num_distinct), K(hybrid_buckets_.count()));
  }
  return ret;
}

int ObHybridHistograms::build_prefix_str_datum_for_lob(ObIAllocator &allocator,
                                                       const ObObjMeta &obj_meta,
                                                       const ObDatum &old_datum,
                                                       ObDatum &new_datum)
{
  int ret = OB_SUCCESS;
  if (!obj_meta.is_lob_storage()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(obj_meta));
  } else {
    ObObj obj;
    ObString str;
    bool can_reuse = false;
    if (OB_FAIL(old_datum.to_obj(obj, obj_meta))) {
      LOG_WARN("failed to obj", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::check_text_can_reuse(obj, can_reuse))) {
      LOG_WARN("failed to check text obj can reuse", K(ret), K(obj));
    } else if (can_reuse) {
      new_datum = old_datum;
    } else if (OB_FAIL(sql::ObTextStringHelper::read_prefix_string_data(&allocator, obj, str, OPT_STATS_MAX_VALUE_CHAR_LEN))) {
      LOG_WARN("failed to read prefix string data", K(ret));
    } else {
      ObTextStringDatumResult text_result(obj_meta.get_type(), obj_meta.has_lob_header(), &new_datum);
      if (OB_FAIL(text_result.init(str.length(), &allocator))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(text_result.append(str.ptr(), str.length()))) {
        LOG_WARN("failed to append realdata", K(ret), K(str), K(text_result));
      } else {
        text_result.set_result();
        LOG_TRACE("Succeed to build_prefix_str_datum_for_lob", K(obj), K(str));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObHybridHistograms)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(total_count_);
  OB_UNIS_ENCODE(num_distinct_);
  OB_UNIS_ENCODE(pop_count_);
  OB_UNIS_ENCODE(pop_freq_);
  int64_t items_count = hybrid_buckets_.count();
  OB_UNIS_ENCODE(items_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < items_count; ++i) {
    const ObHistBucket &hist_bucket = hybrid_buckets_.at(i);
    OB_UNIS_ENCODE(hist_bucket.endpoint_value_);
    OB_UNIS_ENCODE(hist_bucket.endpoint_repeat_count_);
    OB_UNIS_ENCODE(hist_bucket.endpoint_num_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObHybridHistograms)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(total_count_);
  OB_UNIS_ADD_LEN(num_distinct_);
  OB_UNIS_ADD_LEN(pop_count_);
  OB_UNIS_ADD_LEN(pop_freq_);
  int64_t items_count = hybrid_buckets_.count();
  OB_UNIS_ADD_LEN(items_count);
  for (int64_t i = 0; i < items_count; ++i) {
    const ObHistBucket &hist_bucket = hybrid_buckets_.at(i);
    OB_UNIS_ADD_LEN(hist_bucket.endpoint_value_);
    OB_UNIS_ADD_LEN(hist_bucket.endpoint_repeat_count_);
    OB_UNIS_ADD_LEN(hist_bucket.endpoint_num_);
  }
  return len;
}

OB_DEF_DESERIALIZE(ObHybridHistograms)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(total_count_);
  OB_UNIS_DECODE(num_distinct_);
  OB_UNIS_DECODE(pop_count_);
  OB_UNIS_DECODE(pop_freq_);
  int64_t items_count = 0;
  OB_UNIS_DECODE(items_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < items_count; ++i) {
    ObHistBucket hist_bucket;
    OB_UNIS_DECODE(hist_bucket.endpoint_value_);
    OB_UNIS_DECODE(hist_bucket.endpoint_repeat_count_);
    OB_UNIS_DECODE(hist_bucket.endpoint_num_);
    if (OB_FAIL(hybrid_buckets_.push_back(hist_bucket))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

} // end of common
} // end of oceanbase

