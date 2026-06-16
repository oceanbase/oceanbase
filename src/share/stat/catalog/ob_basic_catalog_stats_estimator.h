/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_BASIC_CATALOG_STATS_ESTIMATOR_H
#define OB_BASIC_CATALOG_STATS_ESTIMATOR_H

#include "share/stat/catalog/ob_catalog_stats_estimator.h"
#include "share/stat/ob_stat_item.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/catalog/ob_catalog_stat_item.h"

namespace oceanbase
{
namespace common
{

/**
 * Catalog table statistics estimator
 * Independent from internal table statistics estimator (ObBasicStatsEstimator)
 * Used exclusively for catalog table statistics estimation
 *
 * Key differences from internal table estimator:
 * - Uses ObOptCatalogStat instead of ObOptStat
 * - Uses ObOptCatalogTableStat instead of ObOptTableStat
 * - Uses ObOptCatalogColumnStat instead of ObOptColumnStat
 * - No block count estimation (catalog tables don't have blocks)
 * - Directly sets catalog_id, db_name, table_name, partition_value, column_name
 */
class ObBasicCatalogStatsEstimator : public ObCatalogStatsEstimator
{
public:
  explicit ObBasicCatalogStatsEstimator(sql::ObExecContext &ctx,
                                         ObIAllocator &allocator,
                                         bool can_re_estimate = true);

  int estimate(const ObOptCatalogStatGatherParam &param,
               ObIArray<ObOptCatalogStat> &dst_external_stats);

private:
  int fill_hints(common::ObIAllocator &alloc,
                 const ObString &table_name,
                 const int64_t part_cnt,
                 const int64_t gather_vectorize);

  int fill_group_by_partition_columns(const ObOptCatalogStatGatherParam &param,
                                      ObIAllocator &allocator,
                                      ObString &partition_cols_str);

  int check_stat_need_re_estimate(const ObOptCatalogStatGatherParam &origin_param,
                                  ObOptCatalogStat &catalog_stat,
                                  bool &need_re_estimate,
                                  ObOptCatalogStatGatherParam &new_param);

  int refine_basic_stats(const ObOptCatalogStatGatherParam &param,
                         ObIArray<ObOptCatalogStat> &dst_opt_catalog_stats);

  // Fetch external table file statistics and set file_count, data_size, last_analyzed
  static int fetch_and_set_external_table_file_stats(
      const ObOptCatalogStatGatherParam &param,
      ObIArray<ObOptCatalogStat> &dst_opt_catalog_stats);

private:
  int init_external_opt_stat(const ObOptCatalogStatGatherParam &param,
                              const int64_t part_id,
                              const int64_t part_stattype,
                              ObIAllocator &allocator,
                              ObOptCatalogStat &stat);

private:
  bool can_re_estimate_;
};

} // namespace common
} // namespace oceanbase

#endif // OB_BASIC_CATALOG_STATS_ESTIMATOR_H
