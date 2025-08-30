/**
 * Copyright (c) 2024 OceanBase
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

#include "lib/task/ob_timer.h"
#include "rootserver/mview/ob_mview_timer_task.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewTrimMLogTask : public ObMViewTimerTask
{
public:
  ObMViewTrimMLogTask();
  virtual ~ObMViewTrimMLogTask();
  DISABLE_COPY_ASSIGN(ObMViewTrimMLogTask);
  // for Service
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  // for TimerTask
  void runTimerTask() override;
  static const int64_t MVIEW_TRIM_MLOG_INTERVAL = 5LL * 1000 * 1000; // 5s
private:
  int trim_mlog_impl(const uint64_t mlog_id, 
                     share::schema::ObSchemaGetterGuard &schema_guard,
                     common::ObISQLClient *sql_proxy,
                     sql::ObSQLSessionInfo *session);
  int drop_mlog(share::schema::ObSchemaGetterGuard &schema_guard,
                const share::schema::ObTableSchema *mlog_schema,
                const share::schema::ObTableSchema *base_table_schema,
                const share::schema::ObDatabaseSchema *db_schema);
  int replace_mlog(const ObIArray<uint64_t> &relevent_mviews,
                   share::schema::ObSchemaGetterGuard &schema_guard,
                   const share::schema::ObTableSchema *base_table_schema,
                   const share::schema::ObTableSchema *mlog_schema,
                   sql::ObSQLSessionInfo *session);
  int check_has_build_mview_task(bool &has_build_mview_task);
  int get_relevent_mviews(common::ObISQLClient *sql_proxy,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          const share::schema::ObTableSchema *base_table_schema,
                          ObIArray<uint64_t> &relevent_mviews);

private:
  bool is_inited_;
  bool in_sched_;
  bool is_stop_;
  uint64_t tenant_id_;
  int64_t last_trim_time_;
};

} // namespace rootserver
} // namespace oceanbase
