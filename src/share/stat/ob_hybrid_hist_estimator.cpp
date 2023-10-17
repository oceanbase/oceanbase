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
  : ObStatsEstimator(ctx, allocator)
{}

template<class T>
int ObHybridHistEstimator::add_stat_item(const T &item, ObIArray<ObStatItem *> &stat_items)
{
  int ret = OB_SUCCESS;
  ObStatItem *cpy = NULL;
  if (!item.is_needed()) {
    // do nothing
  } else if (OB_ISNULL(cpy = copy_stat_item(allocator_, item))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to copy stat item", K(ret));
  } else if (OB_FAIL(stat_items.push_back(cpy))) {
    LOG_WARN("failed to push back stat item", K(ret));
  }
  return ret;
}

// select hybrid_hist(c1,20)，hybrid_hist(c2,20)，.... from t1 partition(p0) simple ...
// union all
// select hybrid_hist(c1,20)，hybrid_hist(c2,20)，.... from t1 partition(p1) simple ...
// union all
// select hybrid_hist(c1,20)，hybrid_hist(c2,20)，.... from t1 partition(p2) simple ...
// no need to hit plan cache or rewrite
int ObHybridHistEstimator::estimate(const ObTableStatParam &param,
                                    ObExtraParam &extra,
                                    ObIArray<ObOptStat> &dst_opt_stats)
{
  int ret = OB_SUCCESS;
  ObOptTableStat tab_stat;
  ObOptStat src_opt_stat;
  src_opt_stat.table_stat_ = &tab_stat;
  ObIArray<ObOptColumnStat*> &src_col_stats = src_opt_stat.column_stats_;
  ObArenaAllocator allocator("ObHybridHist", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  ObString raw_sql;
  ObString refine_raw_sql;
  int64_t refine_cnt = 0;
  ObSEArray<ObStatItem *, 4> stat_items;
  ObSEArray<ObStatItem *, 4> refine_stat_items;
  ObSEArray<ObOptColumnStat *, 4> refine_col_stats;
  int64_t max_num_buckets = 0;
  if (OB_FAIL(ObDbmsStatsUtils::init_col_stats(allocator,
                                               param.column_params_.count(),
                                               src_col_stats))) {
    LOG_WARN("failed init col stats", K(ret));
  } else if (FALSE_IT(get_max_num_buckets(param.column_params_, max_num_buckets))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_opt_stats.count(); ++i) {
      ObOptStat &dst_opt_stat = dst_opt_stats.at(i);
      ObSEArray<int64_t, 4> refine_col_ids;
      ObString sample_hint;
      if (OB_ISNULL(dst_opt_stat.table_stat_) ||
        OB_UNLIKELY(dst_opt_stat.column_stats_.count() != src_col_stats.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column stat is null", K(ret), K(dst_opt_stat.table_stat_),
                                          K(src_col_stats.count()), K(src_col_stats.count()),
                                          K(dst_opt_stat.column_stats_.count()));
      } else {
        ObSEArray<ObStatItem *, 4> tmp_stat_items;
        //1.calc sample size
        bool need_sample = false;
        bool is_block_sample = false;
        double est_percent = 0.0;
        if (OB_FAIL(compute_estimate_percent(dst_opt_stat.table_stat_->get_row_count(),
                                             max_num_buckets,
                                             param.sample_info_,
                                             need_sample,
                                             est_percent,
                                             is_block_sample))) {
            LOG_WARN("failed to compute estimate percent", K(ret));
        } else if (need_sample && OB_FAIL(fill_sample_info(allocator,
                                                           est_percent,
                                                           is_block_sample,
                                                           sample_hint))) {
          LOG_WARN("failed to fill sample info", K(ret));
        } else {/*do nothing*/}
        //2.add select item
        bool need_gather_hybrid = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < param.column_params_.count(); ++j) {
          const ObColumnStatParam &col_param = param.column_params_.at(j);
          ObOptColumnStat *src_col_stat = src_col_stats.at(j);
          ObOptColumnStat *dst_col_stat = dst_opt_stat.column_stats_.at(j);
          bool is_need = false;
          bool need_refine = false;
          if (OB_FAIL(need_add_hybrid_hist_item(dst_col_stat,
                                                dst_opt_stat.table_stat_,
                                                col_param,
                                                est_percent,
                                                need_sample,
                                                is_need,
                                                need_refine))) {
            LOG_WARN("failed to need add truly hybrid hist item", K(ret));
          } else if (is_need) {
            if (OB_FAIL(add_stat_item(ObStatHybridHist(&col_param, src_col_stat),
                                      tmp_stat_items))) {
              LOG_WARN("failed to add statistic item", K(ret));
            } else {
              dst_col_stat->get_histogram().reset();
              dst_col_stat->get_histogram().set_type(ObHistType::HYBIRD);
              need_gather_hybrid = true;
            }
          } else if (OB_FAIL(add_stat_item(ObStatHybridHist(&col_param, src_col_stat, true),
                                           tmp_stat_items))) {
            LOG_WARN("failed to add statistic item", K(ret));
          } else if (need_refine) {
            if (OB_FAIL(refine_col_ids.push_back(j))) {
              LOG_WARN("failed to add statistic item", K(ret));
            } else {
              dst_col_stat->get_histogram().reset();
            }
          }
        }
        //3.add other base info
        if (OB_SUCC(ret) && need_gather_hybrid) {
          extra.nth_part_ = i;
          ObSEArray<ObOptColumnStat *, 4> dummy_col_stats;
          if (OB_FAIL(gen_query_sql(allocator, param, extra, sample_hint, i + 1, src_opt_stat,
                                    dst_opt_stat, dst_opt_stats, tmp_stat_items, dummy_col_stats,
                                    raw_sql))) {
            LOG_WARN("failed to gen query sql", K(ret));
          } else if (OB_FAIL(stat_items.assign(tmp_stat_items))) {
            LOG_WARN("failed to assign", K(ret));
          } else {/*do nothing*/}
        }
        //4.refine hybrid hist
        if (OB_SUCC(ret) && !refine_col_ids.empty()) {
          if (OB_FAIL(refine_hybrid_hist(allocator, param, extra, refine_cnt + 1, src_opt_stat,
                                         dst_opt_stat, dst_opt_stats, refine_col_ids,
                                         refine_stat_items, refine_col_stats, refine_raw_sql))) {
            LOG_WARN("failed to refine hybrid hist", K(ret));
          } else {
            ++ refine_cnt;
          }
        }
      }
    }
    //5.do estimate
    if (OB_SUCC(ret) && !raw_sql.empty()) {
      stat_items_.reset();
      if (OB_FAIL(append(stat_items_, stat_items))) {
        LOG_WARN("failed to append stat items", K(ret));
      } else if (OB_FAIL(do_estimate(param.tenant_id_, raw_sql, COPY_HYBRID_HIST_STAT,
                                     src_opt_stat, dst_opt_stats))) {
        LOG_WARN("failed to evaluate basic stats", K(ret));
      } else {
        LOG_TRACE("succeed to build hybrid histogram");
      }
    }
    if (OB_SUCC(ret) && !refine_raw_sql.empty()) {
      stat_items_.reset();
      if (OB_FAIL(append(stat_items_, refine_stat_items))) {
        LOG_WARN("failed to append stat items", K(ret));
      } else if (OB_FAIL(reset_histogram_for_refine_col_stats(refine_col_stats))) {
        LOG_WARN("failed to reset histogram for refine col stats", K(ret));
      } else if (OB_FAIL(do_estimate(param.tenant_id_, refine_raw_sql, COPY_HYBRID_HIST_STAT,
                                     src_opt_stat, dst_opt_stats))) {
        LOG_WARN("failed to evaluate basic stats", K(ret));
      } else {
        refine_col_stats.reset();
        LOG_TRACE("succeed to build hybrid histogram");
      }
    }
  }
  return ret;
}

void ObHybridHistEstimator::get_max_num_buckets(const ObIArray<ObColumnStatParam> &col_params,
                                                int64_t &max_num_buckets)
{
  max_num_buckets = 0;
  for (int64_t i = 0; i < col_params.count(); ++i) {
    max_num_buckets = std::max(max_num_buckets, col_params.at(i).bucket_num_);
  }
}

int ObHybridHistEstimator::need_add_hybrid_hist_item(ObOptColumnStat *dst_col_stat,
                                                     ObOptTableStat *dst_tab_stat,
                                                     const ObColumnStatParam &col_param,
                                                     double est_percent,
                                                     bool need_sample,
                                                     bool &is_need,
                                                     bool &need_refine)
{
  int ret = OB_SUCCESS;
  is_need = false;
  need_refine = false;
  bool is_done = false;
  if (OB_ISNULL(dst_col_stat) || OB_ISNULL(dst_tab_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(dst_col_stat), K(dst_tab_stat), K(ret));
  } else if (!dst_col_stat->get_histogram().is_hybrid()) {
    /*do nothing*/
  } else if (OB_FAIL(try_build_hybrid_hist(col_param,
                                           *dst_col_stat,
                                           *dst_tab_stat,
                                           is_done))) {
     LOG_WARN("failed to build hybrid hist from topk hist", K(ret));
  } else if (is_done) {
    /*do nothing*/
  //if col not null cnt is less than expected sample row cnt, then choose full table scan.
  } else if (need_sample &&
            dst_col_stat->get_num_not_null() <= dst_tab_stat->get_row_count() * est_percent / 100) {
    need_refine = true;
  } else {
    is_need = true;
  }
  return ret;
}

int ObHybridHistEstimator::refine_hybrid_hist(ObIAllocator &allocator,
                                              const ObTableStatParam &param,
                                              ObExtraParam &extra,
                                              int64_t total_refine_cnt,
                                              ObOptStat &src_opt_stat,
                                              ObOptStat &dst_opt_stat,
                                              ObIArray<ObOptStat> &dst_opt_stats,
                                              ObIArray<int64_t> &refine_col_ids,
                                              ObIArray<ObStatItem *> &refine_stat_items,
                                              ObIArray<ObOptColumnStat *> &refine_col_stats,
                                              ObString &refine_raw_sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(refine_col_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty", K(ret), K(refine_col_ids.empty()));
  } else {
    //1.add select item
    int64_t refine_id = 0;
    refine_stat_items.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
      const ObColumnStatParam &col_param = param.column_params_.at(i);
      ObOptColumnStat *src_col_stat = src_opt_stat.column_stats_.at(i);
      ObOptColumnStat *dst_col_stat = dst_opt_stat.column_stats_.at(i);
      if (refine_id < refine_col_ids.count() && i == refine_col_ids.at(refine_id)) {
        if (OB_FAIL(add_stat_item(ObStatHybridHist(&col_param, src_col_stat), refine_stat_items))) {
          LOG_WARN("failed to add statistic item", K(ret));
        } else if (OB_FAIL(refine_col_stats.push_back(dst_col_stat))) {
          LOG_WARN("failed to push back col stat", K(ret));
        } else {
          ++ refine_id;
        }
      } else if (OB_FAIL(add_stat_item(ObStatHybridHist(&col_param, src_col_stat, true),
                                       refine_stat_items))) {
        LOG_WARN("failed to add statistic item", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      ObString dummy_simple_hint;
      if (OB_FAIL(gen_query_sql(allocator, param, extra, dummy_simple_hint, total_refine_cnt,
                                src_opt_stat, dst_opt_stat, dst_opt_stats, refine_stat_items,
                                refine_col_stats, refine_raw_sql))) {
        LOG_WARN("failed to gen query sql", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObHybridHistEstimator::reset_histogram_for_refine_col_stats(
                                                      ObIArray<ObOptColumnStat *> &refine_col_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < refine_col_stats.count(); ++i) {
    if (OB_ISNULL(refine_col_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(refine_col_stats.at(i)));
    } else {
      refine_col_stats.at(i)->get_histogram().reset();
      refine_col_stats.at(i)->get_histogram().set_type(ObHistType::HYBIRD);
    }
  }
  return ret;
}

int ObHybridHistEstimator::gen_query_sql(ObIAllocator &allocator,
                                         const ObTableStatParam &param,
                                         ObExtraParam &extra,
                                         const ObString &simple_hint,
                                         int64_t part_cnt,
                                         ObOptStat &src_opt_stat,
                                         ObOptStat &dst_opt_stat,
                                         ObIArray<ObOptStat> &dst_opt_stats,
                                         ObIArray<ObStatItem *> &stat_items,
                                         ObIArray<ObOptColumnStat *> &refine_col_stats,
                                         ObString &raw_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString single_raw_sql;
  ObString empty_str;
  reset_select_items();
  sample_hint_ = simple_hint;
  int64_t duration_time = -1;
  ObString hint_str("NO_REWRITE USE_PLAN_CACHE(NONE) DBMS_STATS OPT_PARAM('ROWSETS_MAX_ROWS', 256)");
  //add select items
  if (OB_ISNULL(dst_opt_stat.table_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(dst_opt_stat.table_stat_));
  } else if (OB_FAIL(add_hint(hint_str, allocator))) {
    LOG_WARN("failed to add hint");
  } else if (OB_FAIL(add_stat_item(ObPartitionId(&param, src_opt_stat.table_stat_, empty_str,
                                                 dst_opt_stat.table_stat_->get_partition_id()),
                                   stat_items))) {
    LOG_WARN("failed to add partition id item", K(ret));
  } else if (OB_FAIL(append(stat_items_, stat_items))) {
    LOG_WARN("failed to append stat items", K(ret));
  } else if (OB_FAIL(add_from_table(param.db_name_, param.tab_name_))) {
    LOG_WARN("failed to add from table", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to fill parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(extra.start_time_,
                                                               param.duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(fill_partition_info(allocator, param, extra))) {
    LOG_WARN("failed to fill partition info", K(ret));
  } else if (OB_FAIL(pack(single_raw_sql))) {
    LOG_WARN("failed to pack sql", K(ret), K(single_raw_sql));
  } else if (raw_sql.empty()) {
    char *ptr = NULL;
    if (OB_ISNULL(ptr = static_cast<char*>(allocator.alloc(single_raw_sql.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      MEMSET(ptr, 0, single_raw_sql.length() + 1);
      MEMCPY(ptr, single_raw_sql.ptr(), single_raw_sql.length());
      raw_sql.assign_ptr(ptr, single_raw_sql.length());
    }
  } else if (OB_FAIL(gen_union_all_sql(allocator, single_raw_sql.string(), raw_sql, raw_sql))) {
    LOG_WARN("failed to gen union all sql", K(ret));
  //Currently union all sql has 1000 queries at most each time, So do estimate.
  } else if (part_cnt % 1000 == 0) {
    if (OB_FAIL(reset_histogram_for_refine_col_stats(refine_col_stats))) {
        LOG_WARN("failed to reset histogram for refine col stats", K(ret));
    } else if (OB_FAIL(do_estimate(param.tenant_id_, raw_sql, COPY_HYBRID_HIST_STAT,
                                   src_opt_stat, dst_opt_stats))) {
      LOG_WARN("failed to evaluate basic stats", K(ret));
    } else {
      LOG_TRACE("succeed to build hybrid histogram", K(raw_sql));
      raw_sql.reset();
      stat_items.reset();
      refine_col_stats.reset();
      sample_hint_.reset();
    }
  }
  if (OB_SUCC(ret)) {
    reset_select_items();
    other_hints_.reset();
  }
  return ret;
}

int ObHybridHistEstimator::gen_union_all_sql(ObIAllocator &allocator,
                                             const ObString &raw_sql1,
                                             const ObString &raw_sql2,
                                             ObString &merge_raw_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_sql_str;
  char *ptr = NULL;
  if (OB_FAIL(tmp_sql_str.append_fmt("%.*s union all %.*s",
                                      raw_sql1.length(),
                                      raw_sql1.ptr(),
                                      raw_sql2.length(),
                                      raw_sql2.ptr()))) {
    LOG_WARN("failed to build query sql stmt", K(ret));
  } else if (OB_ISNULL(ptr = static_cast<char*>(allocator.alloc(tmp_sql_str.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory is not enough", K(ret), K(ptr));
  } else {
    MEMSET(ptr, 0, tmp_sql_str.length() + 1);
    MEMCPY(ptr, tmp_sql_str.ptr(), tmp_sql_str.length());
    merge_raw_sql.assign_ptr(ptr, tmp_sql_str.length());
    LOG_TRACE("Succeed to gen union all sql", K(merge_raw_sql));
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
        /*do nothing*/
      } else if (total_row_count <= MAGIC_SAMPLE_SIZE) {
        need_sample = false;
        est_percent = 0.0;
        is_block_sample = false;
      } else {
        is_block_sample = false;
        est_percent = (MAGIC_SAMPLE_SIZE * 100.0) / total_row_count;
      }
    }
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
            if (OB_FAIL(row->cells()[0].to_obj(ep_val, obj_meta))) {
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

