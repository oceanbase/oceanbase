/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_CATALOG_MIN_MAX_ESTIMATOR_H
#define OB_CATALOG_MIN_MAX_ESTIMATOR_H

#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/catalog/ob_catalog_stats_estimator.h"
#include "share/stat/catalog/ob_catalog_stat_item.h"

namespace oceanbase
{
namespace common
{

class ObCatalogStatMinMaxAgg : public ObCatalogStatColItem
{
public:
  ObCatalogStatMinMaxAgg() : is_min_(true) {}
  ObCatalogStatMinMaxAgg(const ObCatalogColumnStatParam *param,
                         share::ObOptCatalogColumnStat *stat,
                         bool is_min)
      : ObCatalogStatColItem(param, stat), is_min_(is_min)
  {
  }
  virtual ~ObCatalogStatMinMaxAgg() {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
  virtual bool is_needed() const override
  {
    return col_param_ != NULL && col_param_->need_refine_min_max();
  }

private:
  bool is_min_;
};

// Min/max estimator for catalog tables.
// Supports both TABLE_LEVEL and PARTITION_LEVEL modes:
//
// TABLE_LEVEL (single SQL, uses stat_items + do_estimate from base class):
//   SELECT /*+ USE_PX PARALLEL(N) QUERY_TIMEOUT(T) */
//     sys_ext_min(c1), sys_ext_max(c1), ...
//   FROM `catalog`.`db`.`table`
//
// PARTITION_LEVEL (batch GROUP BY SQL, custom multi-row routing):
//   SELECT /*+ NO_REWRITE DBMS_STATS USE_PX PARALLEL(N) ... */
//     CONCAT('k1=', k1, '/', 'k2=', k2),
//     sys_ext_min(c1), sys_ext_max(c1), ...
//   FROM `catalog`.`db`.`table`
//   WHERE (k1, k2) IN (("v1a","v2a"), ("v1b","v2b"), ...)
//   GROUP BY k1, k2
class ObCatalogMinMaxEstimator : public ObCatalogStatsEstimator
{
public:
  explicit ObCatalogMinMaxEstimator(sql::ObExecContext &ctx, ObIAllocator &allocator);

  int estimate(const ObOptCatalogStatGatherParam &param,
               ObIArray<ObOptCatalogStat> &opt_catalog_stats);

private:
  // ---- shared ----
  int add_min_max_stat_items(ObIAllocator &allocator,
                             const ObIArray<ObCatalogColumnStatParam> &column_params,
                             ObOptCatalogStat &opt_catalog_stat);
  // Assemble final SQL from base class member variables
  // (other_hints_, select_fields_, catalog/db/table, where_string_, group_by_string_).
  // Caller must populate select_fields_ (via gen_select_filed) before calling.
  int pack_sql(ObSqlString &raw_sql_str);

  // ---- TABLE_LEVEL ----
  int estimate_table_level(ObIAllocator &allocator,
                           const ObOptCatalogStatGatherParam &param,
                           ObOptCatalogStat &opt_catalog_stat);

  // ---- PARTITION_LEVEL (batch) ----
  int estimate_partition_level(ObIAllocator &allocator,
                               const ObOptCatalogStatGatherParam &param,
                               ObIArray<ObOptCatalogStat> &opt_catalog_stats);
  int collect_eligible_partitions(
      const ObIArray<ObOptCatalogStat> &opt_catalog_stats,
      ObIArray<int64_t> &eligible_indices);
  int collect_refine_columns(
      const ObIArray<ObCatalogColumnStatParam> &column_params,
      ObIArray<int64_t> &refine_col_indices);
  // Prepend CONCAT('k1=', k1, '/', 'k2=', k2) to select_fields_
  int prepend_partition_key_concat(const ObIArray<ObString> &part_cols);
  int fill_batch_hints(ObIAllocator &allocator,
                       const ObOptCatalogStatGatherParam &param,
                       int64_t eligible_part_cnt);
  int fill_batch_where_clause(ObIAllocator &allocator,
                              const ObOptCatalogStatGatherParam &param,
                              const ObIArray<int64_t> &eligible_indices);
  int fill_batch_group_by(ObIAllocator &allocator,
                          const ObIArray<ObString> &part_cols);
  // Batch execution: multi-row result routing by partition key hash map.
  // Unlike base class do_estimate() which decodes single-row via stat_items_,
  // batch SQL returns one row per partition (via GROUP BY) with column 0 being
  // the CONCAT partition key. Results are routed to matching ObOptCatalogStat
  // via hash lookup.
  int execute_and_route_results(
      const ObOptCatalogStatGatherParam &param,
      const ObString &raw_sql,
      const ObIArray<int64_t> &refine_col_indices,
      ObIArray<ObOptCatalogStat> &opt_catalog_stats);
};

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_CATALOG_MIN_MAX_ESTIMATOR_H
