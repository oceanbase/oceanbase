/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DBMS_CATALOG_STATS_EXECUTOR_H
#define OB_DBMS_CATALOG_STATS_EXECUTOR_H

#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/ob_opt_stat_gather_stat.h"
#include "sql/engine/ob_exec_context.h"
#include "share/external_table/ob_external_table_file_mgr.h"

namespace oceanbase
{
namespace common
{

// Default batch size for catalog table statistics gathering.
const int64_t DEFAULT_CATALOG_GATHER_PART_BATCH_SIZE = 32;

/**
 * @brief CatalogGatherHelper
 * Helper structure for catalog table statistics gathering
 * Simplified version of GatherHelper for internal tables
 *
 * Key differences from internal table GatherHelper:
 * - No sepcify_scn_ (catalog tables don't support snapshot read)
 * - No use_column_store_ (catalog tables don't support column store)
 */
struct CatalogGatherHelper
{
  explicit CatalogGatherHelper(ObOptStatRunningMonitor &running_monitor)
      : is_split_gather_(false),
        maximum_gather_part_cnt_(1),
        maximum_gather_col_cnt_(1),
        is_approx_gather_(false),
        gather_vectorize_(DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE),
        running_monitor_(running_monitor),
        is_split_column_(false),
        is_all_col_gathered_(false)
  {}

  bool is_split_gather_;           // Whether to split gather due to resource limits
  int64_t maximum_gather_part_cnt_; // Maximum partitions per batch
  int64_t maximum_gather_col_cnt_;  // Maximum columns per batch
  bool is_approx_gather_;          // Whether to use approx gather for global stats
  int64_t gather_vectorize_;        // Vectorization batch size
  ObOptStatRunningMonitor &running_monitor_;  // Running monitor for progress tracking
  bool is_split_column_;            // Whether columns are split across batches
  bool is_all_col_gathered_;        // Whether all columns gathered in current batch

  TO_STRING_KV(K(is_split_gather_),
               K(maximum_gather_part_cnt_),
               K(maximum_gather_col_cnt_),
               K(is_approx_gather_),
               K(gather_vectorize_),
               K(running_monitor_),
               K(is_split_column_),
               K(is_all_col_gathered_));
};

/**
 * @brief CatalogGatherPartInfos
 * Partition batch information for catalog table statistics gathering
 * Simplified version of GatherPartInfos for internal tables
 *
 * Key differences from internal table GatherPartInfos:
 * - No sub_part_infos_ (catalog tables only have one partition level)
 * - No approx_gather_ (catalog tables don't need to derive partition stats from subpartitions)
 */
struct CatalogGatherPartInfos
{
  CatalogGatherPartInfos()
      : part_infos_(),
        gather_global_(false)
  {}

  ObSEArray<ObCatalogExtPartitionInfo, 4> part_infos_; // Partitions to gather in this batch
  bool gather_global_; // Whether to gather global stats in this batch

  bool is_invalid_task() const
  {
    return part_infos_.empty() && !gather_global_;
  }

  TO_STRING_KV(K(gather_global_), K(part_infos_));
};

/**
 * @brief CatalogTaskColumnParam
 * Column batch information for catalog table statistics gathering
 * Similar to TaskColumnParam for internal tables
 */
struct CatalogTaskColumnParam
{
  CatalogTaskColumnParam()
      : column_params_(NULL),
        start_(0),
        end_(0)
  {}

  // [start_, end_) range of columns to gather
  const ObIArray<ObCatalogColumnStatParam> *column_params_;
  int32_t start_;
  int32_t end_;

  TO_STRING_KV(KP(column_params_), K(start_), K(end_));
};

class ObDbmsCatalogStatsExecutor
{
public:
  ObDbmsCatalogStatsExecutor();

  static int gather_table_stats(const ObCatalogTableStatParam &param,
                                sql::ObExecContext &ctx,
                                ObOptStatRunningMonitor &running_monitor);

private:
  static int get_catalog_stats_collect_batch_size(ObMySQLProxy *mysql_proxy,
                                                  const ObCatalogTableIdentity &table_identity,
                                                  int64_t &batch_part_size);

  // Prepare gather helper with batch size configuration
  static int prepare_gather_stats(sql::ObExecContext &ctx,
                                  ObCatalogTableStatParam &param,
                                  CatalogGatherHelper &gather_helper);

  // Core partition statistics gathering function
  static int gather_partition_stats(sql::ObExecContext &ctx,
                                    const ObCatalogTableStatParam &param,
                                    CatalogGatherHelper &gather_helper,
                                    ObIArray<ObString> &failed_partition_names);

  // Split partition param into batches
  static int split_catalog_part_param(const ObCatalogTableStatParam &param,
                                      const CatalogGatherHelper &gather_helper,
                                      ObIArray<CatalogGatherPartInfos> &batch_part_infos);

  // Split column param into batches
  static int split_catalog_column_param(const ObCatalogTableStatParam &param,
                                        const CatalogGatherHelper &gather_helper,
                                        ObIArray<CatalogTaskColumnParam> &batch_col_infos);

  // Execute split gather for one batch (writes immediately to database)
  // Using do_split_part_gather_stats and do_split_global_gather_stats instead of do_split_gather_stats.
  static int do_split_part_gather_stats(ObExecContext &ctx,
                                        ObMySQLTransaction &trans,
                                        const CatalogGatherPartInfos &task_info,
                                        const ObIArray<CatalogTaskColumnParam> &batch_col_infos,
                                        ObCatalogTableStatParam &derived_param,
                                        CatalogGatherHelper &gather_helper,
                                        int64_t &global_file_count,
                                        int64_t &global_data_size,
                                        int64_t &global_last_analyzed,
                                        int64_t &global_schema_version);
  static int do_split_global_gather_stats(ObExecContext &ctx,
                                          ObMySQLTransaction &trans,
                                          const CatalogGatherPartInfos &task_info,
                                          const ObIArray<CatalogTaskColumnParam> &batch_col_infos,
                                          const int64_t &global_file_count,
                                          const int64_t &global_data_size,
                                          const int64_t &global_last_analyzed,
                                          const int64_t &global_schema_version,
                                          ObCatalogTableStatParam &derived_param,
                                          CatalogGatherHelper &gather_helper);

  // Similar to internal table's do_gather_stats_with_retry method.
  // Note: catalog tables don't support snapshot read, so no snapshot retry
  static int do_gather_catalog_stats(sql::ObExecContext &ctx,
                                     ObMySQLTransaction &trans,
                                     StatLevel stat_level,
                                     const ObIArray<ObCatalogExtPartitionInfo> &gather_partition_infos,
                                     CatalogGatherHelper &gather_helper,
                                     ObCatalogTableStatParam &param,
                                     ObIArray<share::ObOptCatalogTableStat *> &all_tstats);

  // Core gather logic (similar to internal table's do_gather_stats)
  static int do_gather_catalog_stats(
      sql::ObExecContext &ctx,
      ObMySQLTransaction &trans,
      ObOptCatalogStatGatherParam &gather_param,
      const ObIArray<ObCatalogExtPartitionInfo> &gather_partition_infos,
      const ObIArray<ObCatalogColumnStatParam> &gather_column_params,
      bool is_all_columns_gather,
      ObOptStatGatherAudit &audit,
      ObIArray<ObOptCatalogStat> &opt_stats,
      ObIArray<share::ObOptCatalogTableStat *> &all_tstats,
      ObIArray<share::ObOptCatalogColumnStat *> &all_cstats);

  // Collect failed partition names for error reporting
  static int collect_failed_partition_names(const ObCatalogTableStatParam &param,
                                            ObIArray<ObString> &failed_partition_names);

  // Handle timeout - derive global stats from already-collected partition stats
  static int derive_global_stats_on_timeout(
      sql::ObExecContext &ctx,
      const ObCatalogTableStatParam &origin_param,
      CatalogGatherHelper &gather_helper);
};

} // namespace common
} // namespace oceanbase

#endif // OB_DBMS_CATALOG_STATS_EXECUTOR_H
