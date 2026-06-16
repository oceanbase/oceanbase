/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_CATALOG_INCREMENTAL_STAT_ESTIMATOR_H
#define OB_CATALOG_INCREMENTAL_STAT_ESTIMATOR_H

#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"
#include "share/stat/ob_opt_stat_gather_stat.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

/**
 * @brief ObCatalogIncrementalStatEstimator
 * Derive global statistics from partition statistics for catalog (external) tables.
 *
 * This is a simplified version of ObIncrementalStatEstimator for catalog tables.
 * Key differences:
 * - No histogram derivation (catalog tables don't support histograms yet)
 * - Uses llc_bitmap for accurate NDV estimation via ObGlobalNdvEval
 * - No macro/micro block handling (not applicable for external tables)
 */
class ObCatalogIncrementalStatEstimator
{
public:
  static int derive_split_gather_stats(
      sql::ObExecContext &ctx,
      ObMySQLTransaction &trans,
      const ObCatalogTableStatParam &param,
      ObOptStatGatherAudit *audit,
      bool derive_part_stat,
      bool is_all_columns_gather,
      ObIArray<share::ObOptCatalogTableStat *> &all_tstats);

  static int do_derive_catalog_global_stat(
      const ObCatalogTableStatParam &param,
      const ObIArray<ObOptCatalogStat> &part_opt_stats,
      ObOptCatalogStat &out_stat);

private:
  static int derive_global_tbl_stat(
      const ObCatalogTableStatParam &param,
      const ObIArray<ObOptCatalogStat> &part_stats,
      ObOptCatalogStat &global_stat);

  static int derive_global_col_stat(
      const ObCatalogTableStatParam &param,
      const ObIArray<ObOptCatalogStat> &part_stats,
      ObOptCatalogStat &global_stat);

  static int prepare_catalog_get_opt_stats_param(
      const ObCatalogTableStatParam &param,
      const bool derive_part_stat,
      ObCatalogTableStatParam &new_param);

  static int generate_all_catalog_opt_stat(
      const ObIArray<share::ObOptCatalogTableStat *> &table_stats,
      const ObIArray<share::ObOptCatalogColumnStat *> &column_stats,
      const int64_t col_cnt,
      ObIArray<ObOptCatalogStat> &all_opt_stats);
};

} // namespace common
} // namespace oceanbase

#endif // OB_CATALOG_INCREMENTAL_STAT_ESTIMATOR_H
