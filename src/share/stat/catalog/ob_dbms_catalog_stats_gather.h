/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DBMS_CATALOG_STATS_GATHER_H
#define OB_DBMS_CATALOG_STATS_GATHER_H

#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase
{
namespace common
{

/**
 * Catalog table statistics gatherer
 * Independent from internal table statistics gatherer (ObDbmsStatsGather)
 * Used exclusively for catalog table statistics gathering
 *
 * Key differences from internal table gatherer:
 * - Uses ObOptCatalogStat instead of ObOptStat
 * - No init_opt_stat() that requires partition_id_block_map_
 * - No block count information setup
 * - Directly sets catalog_id, db_name, table_name, partition_value, column_name
 */
class ObDbmsCatalogStatsGather
{
public:
  ObDbmsCatalogStatsGather();

  static int gather_stats(const ObOptCatalogStatGatherParam &param,
                          sql::ObExecContext &ctx,
                          ObOptStatGatherAudit &audit,
                          ObIArray<ObOptCatalogStat> &catalog_stats);

  static int init_opt_catalog_stats(ObIAllocator &allocator,
                                    const ObOptCatalogStatGatherParam &param,
                                    ObIArray<ObOptCatalogStat> &opt_catalog_stats);

  static int init_opt_catalog_stat(ObIAllocator &allocator,
                                   const ObOptCatalogStatGatherParam &param,
                                   const ObString &part_name,
                                   const int64_t part_stattype,
                                   ObOptCatalogStat &stat);
  static int adjust_sample_param(const ObIArray<ObOptCatalogStat> &opt_catalog_stats,
                                 ObOptCatalogStatGatherParam &param);
  static int classfy_catalog_column_histogram(
      const ObOptCatalogStatGatherParam &param,
      ObOptCatalogStat &opt_stat);
};

} // namespace common
} // namespace oceanbase

#endif // OB_DBMS_CATALOG_STATS_GATHER_H
