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

#ifndef OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_STATS_UTILS_H_
#define OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_STATS_UTILS_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace storage
{
class ObMViewRefreshStatsUtils
{
public:
  // Deletes refresh stats of the whole tenant created more than retention_period days ago. There is no
  // mview_id condition, so mview level retention is never applied.
  // enable_retention_reload: re-read the sys default retention every 10 successful DELETEs and apply it
  // to subsequent DELETEs. A longer retention may finish a table's purge earlier, and -1 stops the whole
  // purge. Without it, retention_period is used as days as is, so -1 or any other non-positive value
  // deletes all existing stats (the PURGE_REFRESH_STATS path).
  // See "Semantics of retention_period" in share/schema/ob_mview_refresh_stats_params.h.
  static int purge_refresh_stats(common::ObMySQLProxy *sql_proxy,
                                 uint64_t tenant_id,
                                 int64_t retention_period,
                                 int64_t &affected_rows,
                                 bool enable_retention_reload = false);

private:
  static const int64_t PURGE_BATCH_COUNT = 100000;
  static const int64_t RETENTION_CHECK_INTERVAL = 10;
  static const int64_t RETENTION_NEVER_PURGE = -1;

  struct PurgeState
  {
    int64_t retention_period_;
    int64_t delete_rounds_;  // successful DELETEs of all tables, shared by the retention check
    int64_t affected_rows_;
    bool enable_retention_reload_;

    bool is_stopped() const
    {
      return enable_retention_reload_ && RETENTION_NEVER_PURGE == retention_period_;
    }
  };

  static int purge_table(common::ObMySQLProxy *sql_proxy,
                         uint64_t tenant_id,
                         const char *table_name,
                         PurgeState &state);
  static int fetch_refresh_id_range(common::ObMySQLProxy *sql_proxy,
                                    uint64_t tenant_id,
                                    const char *table_name,
                                    int64_t lower_id,
                                    int64_t &max_id);
  static int record_delete(common::ObMySQLProxy *sql_proxy,
                           uint64_t tenant_id,
                           int64_t deleted_rows,
                           PurgeState &state);
  static int execute_trans_write(common::ObMySQLProxy *sql_proxy,
                                 uint64_t tenant_id,
                                 const common::ObSqlString &sql,
                                 int64_t &affected_rows);
};

} // namespace storage
} // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MVIEW_OB_MVIEW_REFRESH_STATS_UTILS_H_
