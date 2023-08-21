/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/ob_stat_define.h"
#include "share/stat/ob_dbms_stats_executor.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "sql/engine/cmd/ob_analyze_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_dbms_stats_lock_unlock.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"

//#define COMPUTE_FREQUENCY_HISTOGRAM
//   "SELECT /*+NO_USE_PX*/ col, sum(val) over (order by col rows between unbounded preceding and current row) "
//   "FROM (SELECT %.*s as col, count(*) as val FROM %s WHERE %.*s IS NOT NULL GROUP BY %.*s) temp;"
//
//#define COMPUTE_TOP_FREQUENCY_HISTOGRAM
//   "SELECT /*+NO_USE_PX*/ col, sum(val) over (order by col rows between unbounded preceding and current row) "
//   "FROM (SELECT * FROM (SELECT %.*s as col, count(*) as val FROM %s WHERE %.*s IS NOT NULL GROUP BY %.*s ORDER BY val LIMIT %ld) temp) temp2;"
//
//#define COMPUTE_HEIGHT_BASED_HISTOGRAM
//   "SELECT /*+NO_USE_PX*/ endpoint_value, sum(endpoint_num) over (order by endpoint_value rows between unbounded preceding and current row) "
//   "FROM (SELECT endpoint_value, count(*) as endpoint_num "
//   "FROM (SELECT MAX(col) as endpoint_value "
//   "FROM (SELECT %.*s as col, ntile(%ld) over (order by %.*s) as bucket FROM %s WHERE %.*s IS NOT NULL) temp GROUP BY bucket) temp2 group by endpoint_value) temp3;"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObAnalyzeExecutor::execute(ObExecContext &ctx, ObAnalyzeStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTableStatParam param;
  param.allocator_ = &ctx.get_allocator();
  share::schema::ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  ObSQLSessionInfo *session = ctx.get_my_session();
  bool in_restore = false;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard), K(session));
  } else if (OB_FAIL(schema_guard->check_tenant_is_restore(session->get_effective_tenant_id(),
                                                           in_restore))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_UNLIKELY(in_restore) ||
             GCTX.is_standby_cluster()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "analyze table during restore or standby cluster");
  } else if (OB_FAIL(stmt.fill_table_stat_param(ctx, param))) {
    LOG_WARN("failed to fill table stat param", K(ret));
  } else if (OB_FAIL(pl::ObDbmsStats::process_not_size_manual_column(ctx, param))) {
    LOG_WARN("failed to process not size_manual column", K(ret));
  } else if (!stmt.is_delete_histogram()) {
    int64_t task_cnt = 1;
    int64_t seq_id = 1;
    int64_t start_time = ObTimeUtility::current_time();
    ObOptStatTaskInfo task_info;
    if (OB_FAIL(pl::ObDbmsStats::init_gather_task_info(ctx, ObOptStatGatherType::MANUAL_GATHER,
                                                       start_time, task_cnt, task_info))) {
      LOG_WARN("failed to init gather task info", K(ret));
    } else {
      ObOptStatGatherStat gather_stat(task_info);
      ObOptStatGatherStatList::instance().push(gather_stat);
      ObOptStatRunningMonitor running_monitor(ctx.get_allocator(), start_time, param.allocator_->used(), gather_stat);
      if (OB_FAIL(running_monitor.add_table_info(param))) {
        LOG_WARN("failed to add table info", K(ret));
      } else if (OB_FAIL(ObDbmsStatsLockUnlock::check_stat_locked(ctx, param))) {
        LOG_WARN("failed check stat locked", K(ret));
      } else if (OB_FAIL(ObOptStatMonitorManager::flush_database_monitoring_info(ctx, false, true))) {
        LOG_WARN("failed to do flush database monitoring info", K(ret));
      } else if (OB_FAIL(ObDbmsStatsExecutor::gather_table_stats(ctx, param))) {
        LOG_WARN("failed to gather table stats", K(ret));
      } else if (OB_FAIL(pl::ObDbmsStats::update_stat_cache(session->get_rpc_tenant_id(), param))) {
        LOG_WARN("failed to update stat cache", K(ret));
      } else {
        LOG_TRACE("succeed to gather table stats", K(param));
      }
      running_monitor.set_monitor_result(ret, ObTimeUtility::current_time(), param.allocator_->used());
      ObOptStatGatherStatList::instance().remove(gather_stat);
      task_info.task_end_time_ = ObTimeUtility::current_time();
      task_info.ret_code_ = ret;
      task_info.failed_count_ = ret == OB_SUCCESS ? 0 : 1;
      ObOptStatManager::get_instance().update_opt_stat_task_stat(task_info);
      ObOptStatManager::get_instance().update_opt_stat_gather_stat(gather_stat);
    }
  } else {
    if (OB_FAIL(ObDbmsStatsExecutor::delete_table_stats(ctx, param, true))) {
      LOG_WARN("failed to drop table stats", K(ret));
    } else {
      LOG_TRACE("succeed to drop table stats", K(param));
    }
  }
  return ret;
}

} // end of SQL
} // end of OceanBase
