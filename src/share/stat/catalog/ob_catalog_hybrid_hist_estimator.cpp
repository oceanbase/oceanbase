/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "share/stat/catalog/ob_catalog_hybrid_hist_estimator.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase
{
namespace common
{

ObCatalogHybridHistEstimator::ObCatalogHybridHistEstimator(
    sql::ObExecContext &ctx, ObIAllocator &allocator)
    : ObCatalogStatsEstimator(ctx, allocator)
{
}

int ObCatalogHybridHistEstimator::estimate(
    const ObOptCatalogStatGatherParam &param,
    ObOptCatalogStat &opt_stat)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(param);
  UNUSED(opt_stat);
  LOG_WARN("catalog hybrid histogram estimation is not supported yet", K(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog hybrid histogram estimation");
  return ret;
}

int ObCatalogHybridHistEstimator::fill_hints(
    common::ObIAllocator &alloc,
    const ObString &table_name,
    int64_t gather_vectorize)
{
  int ret = OB_SUCCESS;
  ObSqlString default_hints;
  if (OB_UNLIKELY(table_name.empty() || gather_vectorize < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_name), K(gather_vectorize));
  } else if (OB_FAIL(default_hints.append_fmt(
                 "NO_REWRITE DBMS_STATS OPT_PARAM('ROWSETS_MAX_ROWS', %ld)",
                 gather_vectorize))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(add_hint(default_hints.string(), alloc))) {
    LOG_WARN("failed to add hint", K(ret));
  }
  return ret;
}

int ObCatalogHybridHistEstimator::extract_hybrid_hist_col_info(
    const ObOptCatalogStatGatherParam &param,
    ObOptCatalogStat &opt_stat,
    ObIArray<const ObCatalogColumnStatParam *> &hybrid_col_params,
    ObIArray<share::ObOptCatalogColumnStat *> &hybrid_col_stats,
    int64_t &max_num_buckets)
{
  int ret = OB_SUCCESS;
  // TODO: histogram not yet supported for catalog tables
  max_num_buckets = 0;
  UNUSED(param);
  UNUSED(opt_stat);
  UNUSED(hybrid_col_params);
  UNUSED(hybrid_col_stats);
  return ret;
}

int ObCatalogHybridHistEstimator::try_build_hybrid_hist(
    const ObCatalogColumnStatParam &param,
    share::ObOptCatalogColumnStat &col_stat,
    share::ObOptCatalogTableStat &table_stat,
    bool &is_done)
{
  int ret = OB_SUCCESS;
  // TODO: histogram not yet supported for catalog tables
  UNUSED(param);
  UNUSED(col_stat);
  UNUSED(table_stat);
  is_done = false;
  return ret;
}

// Catalog tables: simplified sampling logic (no block sample, no micro_block_num)
int ObCatalogHybridHistEstimator::compute_estimate_percent(
    int64_t total_row_count,
    int64_t max_num_bkts,
    const ObCatalogAnalyzeSampleInfo &sample_info,
    bool &need_sample,
    double &est_percent)
{
  int ret = OB_SUCCESS;
  if (0 == total_row_count) {
    need_sample = false;
  } else if (sample_info.is_sample_) {
    need_sample = true;
    est_percent = sample_info.percent_;
    if (OB_SUCC(ret) && need_sample) {
      if (total_row_count * est_percent / 100 >= MAGIC_MIN_SAMPLE_SIZE) {
        // ok
      } else if (total_row_count <= MAGIC_SAMPLE_SIZE) {
        need_sample = false;
        est_percent = 0.0;
      } else {
        est_percent = (MAGIC_SAMPLE_SIZE * 100.0) / total_row_count;
      }
    }
  } else if (total_row_count > MAGIC_MAX_AUTO_SAMPLE_SIZE) {
    // Auto sampling for catalog tables (always row-based, no block sampling)
    if (max_num_bkts <= ObColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM) {
      need_sample = true;
      est_percent = (MAGIC_MAX_AUTO_SAMPLE_SIZE * 100.0) / total_row_count;
    } else {
      int64_t num_bound_bkts = static_cast<int64_t>(
          std::round(total_row_count * MAGIC_SAMPLE_CUT_RATIO));
      if (max_num_bkts >= num_bound_bkts) {
        need_sample = false;
      } else {
        int64_t sample_size = MAGIC_SAMPLE_SIZE + MAGIC_BASE_SAMPLE_SIZE
            + (max_num_bkts - ObColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM)
              * MAGIC_MIN_SAMPLE_SIZE * 0.01;
        need_sample = true;
        est_percent = (sample_size * 100.0) / total_row_count;
      }
    }
  } else {
    need_sample = false;
  }
  if (OB_SUCC(ret) && need_sample) {
    est_percent = std::max(0.000001, est_percent);
    if (est_percent >= 100) {
      need_sample = false;
    }
  }
  return ret;
}

int ObCatalogHybridHistEstimator::add_hybrid_hist_stat_items(
    ObIArray<const ObCatalogColumnStatParam *> &hybrid_col_params,
    ObIArray<share::ObOptCatalogColumnStat *> &hybrid_col_stats,
    int64_t table_row_cnt,
    bool need_sample,
    double est_percent,
    ObIArray<int64_t> &no_sample_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(hybrid_col_params.count() != hybrid_col_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(hybrid_col_params.count()),
             K(hybrid_col_stats.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < hybrid_col_stats.count(); ++i) {
      if (OB_ISNULL(hybrid_col_params.at(i)) || OB_ISNULL(hybrid_col_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (!need_sample
                 || hybrid_col_stats.at(i)->get_num_not_null()
                    > table_row_cnt * est_percent / 100) {
        if (OB_FAIL(add_stat_item(ObCatalogStatHybridHist(
                hybrid_col_params.at(i), hybrid_col_stats.at(i))))) {
          LOG_WARN("failed to add stat item", K(ret));
        }
      } else if (OB_FAIL(no_sample_idx.push_back(i))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObCatalogHybridHistEstimator::estimate_no_sample_col_hybrid_hist(
    ObIAllocator &allocator,
    const ObOptCatalogStatGatherParam &param,
    ObOptCatalogStat &opt_stat,
    ObIArray<const ObCatalogColumnStatParam *> &hybrid_col_params,
    ObIArray<share::ObOptCatalogColumnStat *> &hybrid_col_stats,
    ObIArray<int64_t> &no_sample_idx)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  int64_t duration_time = -1;
  ObSEArray<ObOptCatalogStat, 1> tmp_opt_stats;
  if (no_sample_idx.empty()) {
    // nothing to do
  } else if (FALSE_IT(reset_select_items())) {
  } else if (FALSE_IT(reset_sample_hint())) {
  } else if (FALSE_IT(reset_other_hint())) {
  } else if (OB_FAIL(add_no_sample_hybrid_hist_stat_items(
                 hybrid_col_params, hybrid_col_stats, no_sample_idx))) {
    LOG_WARN("failed to add no sample hybrid hist stat items", K(ret));
  } else if (OB_FAIL(fill_hints(allocator, from_table_,
                                param.gather_vectorize_))) {
    LOG_WARN("failed to fill hints", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to fill parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(
                 param.gather_start_time_,
                 param.max_duration_time_,
                 duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(pack(raw_sql))) {
    LOG_WARN("failed to pack", K(ret));
  } else if (OB_FAIL(tmp_opt_stats.push_back(opt_stat))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(do_estimate(param, raw_sql.string(), false,
                                 opt_stat, tmp_opt_stats))) {
    LOG_WARN("failed to do estimate", K(ret));
  }
  return ret;
}

int ObCatalogHybridHistEstimator::add_no_sample_hybrid_hist_stat_items(
    ObIArray<const ObCatalogColumnStatParam *> &hybrid_col_params,
    ObIArray<share::ObOptCatalogColumnStat *> &hybrid_col_stats,
    ObIArray<int64_t> &no_sample_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(no_sample_idx.empty()
                  || hybrid_col_params.count() != hybrid_col_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(no_sample_idx),
             K(hybrid_col_params.count()), K(hybrid_col_stats.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < no_sample_idx.count(); ++i) {
      int64_t idx = no_sample_idx.at(i);
      if (OB_UNLIKELY(idx >= hybrid_col_params.count())
          || OB_ISNULL(hybrid_col_params.at(idx))
          || OB_ISNULL(hybrid_col_stats.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(idx));
      } else if (OB_FAIL(add_stat_item(ObCatalogStatHybridHist(
              hybrid_col_params.at(idx), hybrid_col_stats.at(idx))))) {
        LOG_WARN("failed to add stat item", K(ret));
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
