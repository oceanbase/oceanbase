/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
