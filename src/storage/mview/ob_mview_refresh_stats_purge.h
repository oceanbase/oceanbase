/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_mview_refresh_stats.h"

namespace oceanbase
{
namespace storage
{
class ObMViewRefreshStatsPurgeUtil final
{
public:
  static int purge_refresh_stats(
    ObISQLClient &sql_client, uint64_t tenant_id,
    const share::schema::ObMViewRefreshStats::FilterParam &filter_param, int64_t &affected_rows,
    int64_t limit = -1);
};

} // namespace storage
} // namespace oceanbase
