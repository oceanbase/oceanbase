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
#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashmap.h" //ObHashMap
#include "src/sql/session/ob_sql_session_info.h" // ObSQLSessionInfo
#include "src/sql/engine/expr/ob_expr_last_refresh_scn.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{

namespace rootserver
{
class ObMViewMaintenanceService;

class ObMviewUpdateCacheTask : public ObMViewTimerTask
{
public:
  static const uint64_t TaskDelay = 5 * 1000 * 1000; // 5s
public:
  ObMviewUpdateCacheTask();
  virtual ~ObMviewUpdateCacheTask();
  int get_mview_refresh_scn_sql(const int refresh_mode, ObSqlString &sql);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  void clean_up();
  void runTimerTask() override;
  DISABLE_COPY_ASSIGN(ObMviewUpdateCacheTask);
  int extract_sql_result(sqlclient::ObMySQLResult *mysql_result,
                         ObIArray<uint64_t> &mview_ids,
                         ObIArray<uint64_t> &last_refresh_scns,
                         ObIArray<uint64_t> &mview_refresh_modes);
private:
  bool is_inited_;
  bool is_stop_;
  bool in_sched_;
};


} // namespace rootserver
} // namespace oceanbase
