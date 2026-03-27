/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/diagnosis/ob_sql_monitor_statname.h"
#include <stdint.h>
#include "share/diagnosis/ob_runtime_metrics.h"

namespace oceanbase
{
namespace sql
{
ObSqlMonitorStatIds::ObSqlMonitorStatIds()
{
#define SQL_MONITOR_STATNAME_DEF(def, unit, name, desc, agg_type, level)                           \
  static_assert(sizeof(name) <= MAX_MONITOR_STAT_NAME_LENGTH,                                      \
                "metric name length is bigger than MAX_MONITOR_STAT_NAME_LENGTH");
#include "share/diagnosis/ob_sql_monitor_statname.h"
#undef SQL_MONITOR_STATNAME_DEF

  // notice, if ObSqlMonitorStatEnum define more than UINT8_MAX, you should modify
  // ObOpProfile::metrics_id_map_ from uint8_t to uint16_t
  static_assert(
      ObSqlMonitorStatEnum::MONITOR_STATNAME_END < UINT8_MAX,
      "metric define more than UINT8_MAX, please modify ObOpProfile::metrics_id_map_ also");
}

const ObMonitorStat OB_MONITOR_STATS[] = {
#define SQL_MONITOR_STATNAME_DEF(def, unit, name, desc, agg_type, level)                           \
  {{static_cast<int>(unit)}, name, desc, agg_type, static_cast<int>(level)},
#include "share/diagnosis/ob_sql_monitor_statname.h"
#undef SQL_MONITOR_STATNAME_DEF
};
} // namespace sql
} // namespace oceanbase
