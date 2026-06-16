/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "share/stat/catalog/ob_catalog_stats_freshness_checker.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"

namespace oceanbase
{
namespace common
{

int ObICatalogStatsFreshnessChecker::filter_partitions_need_collect(
    const ObIArray<ObCatalogExtPartitionInfo> &new_all_partitions,
    const ObIArray<share::ObOptCatalogTableStat *> &existing_partition_stats,
    ObArray<ObCatalogExtPartitionInfo> &partitions_to_collect,
    ObArray<ObString> &partitions_to_delete,
    ObIArray<share::ObOptCatalogTableStat *> &unchanged_partition_stats)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObString, share::ObOptCatalogTableStat *> existing_map;

  if (OB_FAIL(build_existing_stats_map(existing_partition_stats, existing_map))) {
    LOG_WARN("failed to build existing stats map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < new_all_partitions.count(); ++i) {
    const ObCatalogExtPartitionInfo &new_part = new_all_partitions.at(i);
    share::ObOptCatalogTableStat *existing_stat = NULL;

    if (new_part.partition_.empty()) {
      LOG_TRACE("skip empty partition value", K(i));
    } else if (OB_FAIL(existing_map.get_refactored(new_part.partition_,
                                                    existing_stat))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(partitions_to_collect.push_back(new_part))) {
          LOG_WARN("failed to push back partition to collect",
                   K(ret), K(new_part));
        } else {
          LOG_TRACE("new partition needs collection",
                    K(new_part.partition_));
        }
      } else {
        LOG_WARN("failed to get partition stats",
                 K(ret), K(new_part.partition_));
      }
    } else {
      if (check_table_schema_changed(new_part, existing_stat)) {
        if (OB_FAIL(partitions_to_collect.push_back(new_part))) {
          LOG_WARN("failed to push back changed partition",
                   K(ret), K(new_part));
        } else {
          LOG_TRACE("partition metadata changed, needs re-collection",
                    K(new_part.partition_));
        }
      } else if (OB_FAIL(unchanged_partition_stats.push_back(
                     existing_stat))) {
        LOG_WARN("failed to push back unchanged stat", K(ret));
      } else {
        LOG_TRACE("partition unchanged, skip collection",
                  K(new_part.partition_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(existing_map.erase_refactored(
                     new_part.partition_))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to erase from map",
                   K(ret), K(new_part.partition_));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    hash::ObHashMap<ObString, share::ObOptCatalogTableStat *>::iterator iter;
    for (iter = existing_map.begin();
         OB_SUCC(ret) && iter != existing_map.end(); ++iter) {
      if (OB_FAIL(partitions_to_delete.push_back(iter->first))) {
        LOG_WARN("failed to push back partition to delete",
                 K(ret), K(iter->first));
      } else {
        LOG_TRACE("partition deleted from remote, needs deletion",
                  K(iter->first));
      }
    }
  }

  LOG_TRACE("filter partitions result",
            K(ret),
            K(new_all_partitions.count()),
            K(existing_partition_stats.count()),
            K(partitions_to_collect.count()),
            K(partitions_to_delete.count()));

  if (existing_map.created()) {
    existing_map.destroy();
  }

  return ret;
}

int ObICatalogStatsFreshnessChecker::build_existing_stats_map(
    const ObIArray<share::ObOptCatalogTableStat *> &existing_partition_stats,
    hash::ObHashMap<ObString, share::ObOptCatalogTableStat *> &stats_map)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num =
      existing_partition_stats.count() > 0
          ? existing_partition_stats.count() * 2 : 64;

  if (OB_FAIL(stats_map.create(bucket_num,
                               lib::ObLabel("CatStatFresh"),
                               lib::ObLabel("CatStatFresh")))) {
    LOG_WARN("failed to create hashmap", K(ret), K(bucket_num));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < existing_partition_stats.count();
       ++i) {
    share::ObOptCatalogTableStat *stat = existing_partition_stats.at(i);
    if (OB_ISNULL(stat) || stat->get_partition_value().empty()) {
    } else {
      const ObString &partition_value = stat->get_partition_value();
      if (OB_FAIL(stats_map.set_refactored(partition_value, stat))) {
        if (OB_HASH_EXIST == ret) {
          LOG_WARN("duplicate partition value in existing stats",
                   K(partition_value));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to set hashmap entry",
                   K(ret), K(partition_value));
        }
      }
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase
