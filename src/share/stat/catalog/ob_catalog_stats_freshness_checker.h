/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_CATALOG_STATS_FRESHNESS_CHECKER_H
#define OB_CATALOG_STATS_FRESHNESS_CHECKER_H

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{
class ObOptCatalogTableStat;
} // namespace share

namespace common
{
struct ObCatalogExtPartitionInfo;

// Base class for catalog statistics freshness checking.
// filter_partitions_need_collect / build_existing_stats_map have default
// implementations; subclasses only need to override check_table_schema_changed
// and optionally build_existing_stats_map if extra filtering is required.
class ObICatalogStatsFreshnessChecker
{
public:
  virtual ~ObICatalogStatsFreshnessChecker() = default;

  virtual bool check_table_schema_changed(
      const ObCatalogExtPartitionInfo &new_part,
      const share::ObOptCatalogTableStat *existing_stat) = 0;

  virtual int filter_partitions_need_collect(
      const ObIArray<ObCatalogExtPartitionInfo> &new_all_partitions,
      const ObIArray<share::ObOptCatalogTableStat *> &existing_partition_stats,
      ObArray<ObCatalogExtPartitionInfo> &partitions_to_collect,
      ObArray<ObString> &partitions_to_delete,
      ObIArray<share::ObOptCatalogTableStat *> &unchanged_partition_stats);

protected:
  virtual int build_existing_stats_map(
      const ObIArray<share::ObOptCatalogTableStat *> &existing_partition_stats,
      hash::ObHashMap<ObString, share::ObOptCatalogTableStat *> &stats_map);
};

} // namespace common
} // namespace oceanbase

#endif // OB_CATALOG_STATS_FRESHNESS_CHECKER_H
