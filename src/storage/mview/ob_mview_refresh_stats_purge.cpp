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

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/ob_mview_refresh_stats_purge.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace common::hash;
using namespace share;
using namespace share::schema;

int ObMViewRefreshStatsPurgeUtil::purge_refresh_stats(
  ObISQLClient &sql_client, uint64_t tenant_id,
  const ObMViewRefreshStats::FilterParam &filter_param, int64_t &affected_rows, int64_t limit)
{
  int ret = OB_SUCCESS;
  affected_rows = -1;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !filter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(filter_param));
  } else {
    ObArray<ObMViewRefreshStatsRecordId> record_ids;
    ObHashMap<int64_t /*refresh_id*/, int64_t /*dec_val*/, NoPthreadDefendMode> dec_val_map;
    if (OB_FAIL(dec_val_map.create(1024, "MVStatsPurge", "MVStatsPurge", tenant_id))) {
      LOG_WARN("fail to init hashmap", KR(ret));
    } else if (OB_FAIL(ObMViewRefreshStats::collect_record_ids(sql_client, tenant_id, filter_param,
                                                               record_ids, limit))) {
      LOG_WARN("fail to collect record ids", KR(ret), K(tenant_id), K(filter_param));
    }
    ARRAY_FOREACH_N(record_ids, i, cnt)
    {
      ObMViewRefreshStatsRecordId &record_id = record_ids.at(i);
      int64_t *dec_val = nullptr;
      if (OB_FAIL(
            ObMViewRefreshStats::drop_refresh_stats_record(sql_client, tenant_id, record_id))) {
        LOG_WARN("fail to drop refresh stats record", KR(ret), K(tenant_id), K(record_id));
      } else if (OB_FAIL(ObMViewRefreshChangeStats::drop_change_stats_record(sql_client, tenant_id,
                                                                             record_id))) {
        LOG_WARN("fail to drop change stats record", KR(ret), K(tenant_id), K(record_id));
      } else if (OB_FAIL(ObMViewRefreshStmtStats::drop_stmt_stats_record(sql_client, tenant_id,
                                                                         record_id))) {
        LOG_WARN("fail to drop stmt stats record", KR(ret), K(tenant_id), K(record_id));
      } else if (OB_ISNULL(dec_val = dec_val_map.get(record_id.refresh_id_))) {
        if (OB_FAIL(dec_val_map.set_refactored(record_id.refresh_id_, 1))) {
          LOG_WARN("fail to create", KR(ret));
        }
      } else {
        *dec_val += 1;
      }
    }
    // purge run stats
    FOREACH_X(iter, dec_val_map, OB_SUCC(ret))
    {
      const int64_t refresh_id = iter->first;
      const int64_t dec_val = iter->second;
      if (OB_FAIL(ObMViewRefreshRunStats::dec_num_mvs_current(sql_client, tenant_id, refresh_id,
                                                              dec_val))) {
        LOG_WARN("fail to drop empty run stats", KR(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
            ObMViewRefreshRunStats::drop_empty_run_stats(sql_client, tenant_id, affected_rows))) {
        LOG_WARN("fail to drop empty run stats", KR(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      affected_rows = record_ids.count();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
