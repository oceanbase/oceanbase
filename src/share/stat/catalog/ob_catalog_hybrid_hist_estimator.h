/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_CATALOG_HYBRID_HIST_ESTIMATOR_H
#define OB_CATALOG_HYBRID_HIST_ESTIMATOR_H

#include "share/stat/catalog/ob_catalog_stats_estimator.h"

namespace oceanbase
{
namespace common
{

/**
 * @brief ObCatalogHybridHistEstimator
 * Hybrid histogram estimator for catalog (external) tables.
 * Mirrors internal table ObHybridHistEstimator; not implemented yet.
 *
 * Key differences from internal table version (when implemented):
 * - No micro_block_num / sstable_row_count (external tables don't have these)
 * - Always uses row sampling (no block sampling for external tables)
 */
class ObCatalogHybridHistEstimator : public ObCatalogStatsEstimator
{
public:
  explicit ObCatalogHybridHistEstimator(sql::ObExecContext &ctx, ObIAllocator &allocator);

  int estimate(const ObOptCatalogStatGatherParam &param,
               ObOptCatalogStat &opt_stat);

private:
  int fill_hints(common::ObIAllocator &alloc,
                 const ObString &table_name,
                 int64_t gather_vectorize);

  int extract_hybrid_hist_col_info(
      const ObOptCatalogStatGatherParam &param,
      ObOptCatalogStat &opt_stat,
      ObIArray<const ObCatalogColumnStatParam *> &hybrid_col_params,
      ObIArray<share::ObOptCatalogColumnStat *> &hybrid_col_stats,
      int64_t &max_num_buckets);

  int try_build_hybrid_hist(const ObCatalogColumnStatParam &param,
                            share::ObOptCatalogColumnStat &col_stat,
                            share::ObOptCatalogTableStat &table_stat,
                            bool &is_done);

  int compute_estimate_percent(int64_t total_row_count,
                               int64_t max_num_buckets,
                               const ObCatalogAnalyzeSampleInfo &sample_info,
                               bool &need_sample,
                               double &est_percent);

  int add_hybrid_hist_stat_items(
      ObIArray<const ObCatalogColumnStatParam *> &hybrid_col_params,
      ObIArray<share::ObOptCatalogColumnStat *> &hybrid_col_stats,
      int64_t table_row_cnt,
      bool need_sample,
      double est_percent,
      ObIArray<int64_t> &no_sample_idx);

  int estimate_no_sample_col_hybrid_hist(
      ObIAllocator &allocator,
      const ObOptCatalogStatGatherParam &param,
      ObOptCatalogStat &opt_stat,
      ObIArray<const ObCatalogColumnStatParam *> &hybrid_col_params,
      ObIArray<share::ObOptCatalogColumnStat *> &hybrid_col_stats,
      ObIArray<int64_t> &no_sample_idx);

  int add_no_sample_hybrid_hist_stat_items(
      ObIArray<const ObCatalogColumnStatParam *> &hybrid_col_params,
      ObIArray<share::ObOptCatalogColumnStat *> &hybrid_col_stats,
      ObIArray<int64_t> &no_sample_idx);
};

} // namespace common
} // namespace oceanbase

#endif // OB_CATALOG_HYBRID_HIST_ESTIMATOR_H
