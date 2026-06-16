/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "share/stat/catalog/hms/ob_hms_catalog_stats_freshness_checker.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"

namespace oceanbase
{
namespace common
{

bool ObHMSCatalogStatsFreshnessChecker::check_table_schema_changed(
    const ObCatalogExtPartitionInfo &new_part,
    const share::ObOptCatalogTableStat *existing_stat)
{
  bool changed = false;
  if (OB_ISNULL(existing_stat)) {
    changed = true;
  } else if (new_part.schema_version_ != existing_stat->get_schema_version()) {
    changed = true;
    LOG_TRACE("partition schema version changed",
              K(new_part.partition_),
              K(new_part.schema_version_),
              K(existing_stat->get_schema_version()));
  } else {
    bool modify_ts_changed =
        (new_part.modify_ts_ != existing_stat->get_last_analyzed());
    bool file_num_changed =
        (new_part.file_num_ != existing_stat->get_file_num());
    bool data_size_changed =
        (new_part.data_size_ != existing_stat->get_data_size());

    changed = modify_ts_changed || file_num_changed || data_size_changed;

    if (changed) {
      LOG_TRACE("partition metadata difference detected",
                K(new_part.partition_),
                K(modify_ts_changed),
                K(file_num_changed),
                K(data_size_changed),
                K(new_part.modify_ts_),
                K(existing_stat->get_last_analyzed()),
                K(new_part.file_num_),
                K(existing_stat->get_file_num()),
                K(new_part.data_size_),
                K(existing_stat->get_data_size()));
    } else {
      LOG_TRACE("partition metadata not changed",
                K(new_part.partition_),
                K(new_part.modify_ts_),
                K(existing_stat->get_last_analyzed()),
                K(new_part.file_num_),
                K(existing_stat->get_file_num()),
                K(new_part.data_size_),
                K(existing_stat->get_data_size()));
    }
  }
  return changed;
}

int ObHMSCatalogStatsFreshnessChecker::build_existing_stats_map(
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
    } else if (stat->get_last_analyzed() == 0) {
      // TODO(bitao): impl as same as internal table.
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
