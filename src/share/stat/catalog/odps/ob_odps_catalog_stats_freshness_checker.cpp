/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "share/stat/catalog/odps/ob_odps_catalog_stats_freshness_checker.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"

namespace oceanbase
{
namespace common
{

bool ObOdpsCatalogStatsFreshnessChecker::check_table_schema_changed(
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
    changed = (new_part.modify_ts_ != existing_stat->get_last_analyzed());
  }
  return changed;
}

} // namespace common
} // namespace oceanbase
