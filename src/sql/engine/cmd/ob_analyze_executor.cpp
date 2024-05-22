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
  ObSEArray<ObTableStatParam,1> params;
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
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (OB_FAIL(stmt.fill_table_stat_params(ctx, params))) {
    LOG_WARN("failed to fill table stat param", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      if (OB_FAIL(pl::ObDbmsStats::process_not_size_manual_column(ctx, params.at(i)))) {
        LOG_WARN("failed to process not size_manual column", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (stmt.is_delete_histogram()) {
        bool cascade_columns = true;
        bool cascade_indexes = true;
        if (OB_UNLIKELY(params.count() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(params));
        } else {
          ObArenaAllocator tmp_alloc("DeleteStats", OB_MALLOC_NORMAL_BLOCK_SIZE, params.at(0).tenant_id_);
          params.at(0).allocator_ = &tmp_alloc;//use the temp allocator to free memory after delete stats.
          if (OB_FAIL(ObDbmsStatsLockUnlock::check_stat_locked(ctx, params.at(0)))) {
            LOG_WARN("failed to check stat locked", K(ret));
          } else if (OB_FAIL(ObDbmsStatsExecutor::delete_table_stats(ctx, params.at(0), cascade_columns))) {
            LOG_WARN("failed to delete table stats", K(ret));
          } else if (OB_FAIL(pl::ObDbmsStats::update_stat_cache(session->get_rpc_tenant_id(), params.at(0)))) {
            LOG_WARN("failed to update stat cache", K(ret));
          } else if (cascade_indexes && params.at(0).part_name_.empty()) {
            if (OB_FAIL(pl::ObDbmsStats::delete_table_index_stats(ctx, params.at(0)))) {
              LOG_WARN("failed to delete index stats", K(ret));
            } else {/*do nothing*/}
          }
        }
        LOG_TRACE("succeed to drop table stats", K(params));
      } else {
        int64_t task_cnt = params.count();
        int64_t start_time = ObTimeUtility::current_time();
        ObOptStatTaskInfo task_info;
        if (OB_FAIL(pl::ObDbmsStats::init_gather_task_info(ctx, ObOptStatGatherType::MANUAL_GATHER,
                                                          start_time, task_cnt, task_info))) {
          LOG_WARN("failed to init gather task info", K(ret));
        } else {
          int64_t i = 0;
          for (; OB_SUCC(ret) && i < params.count(); ++i) {
            ObTableStatParam &param = params.at(i);
            ObArenaAllocator tmp_alloc("OptStatGather", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
            param.allocator_ = &tmp_alloc;//use the temp allocator to free memory after gather stats.
            start_time = ObTimeUtility::current_time();
            ObOptStatGatherStat gather_stat(task_info);
            ObOptStatGatherStatList::instance().push(gather_stat);
            ObOptStatRunningMonitor running_monitor(ctx.get_allocator(), start_time, param.allocator_->used(), gather_stat);
            if (OB_FAIL(running_monitor.add_monitor_info(ObOptStatRunningPhase::GATHER_PREPARE))) {
              LOG_WARN("failed to add add monitor info", K(ret));
            } else if (OB_FAIL(running_monitor.add_table_info(param))) {
              LOG_WARN("failed to add table info", K(ret));
            } else if (OB_FAIL(ObDbmsStatsLockUnlock::check_stat_locked(ctx, param))) {
              LOG_WARN("failed check stat locked", K(ret));
            } else if (OB_FAIL(ObOptStatMonitorManager::flush_database_monitoring_info(ctx, false, true))) {
              LOG_WARN("failed to do flush database monitoring info", K(ret));
            } else if (OB_FAIL(ObDbmsStatsExecutor::gather_table_stats(ctx, param, running_monitor))) {
              LOG_WARN("failed to gather table stats", K(ret));
            } else if (OB_FAIL(pl::ObDbmsStats::update_stat_cache(session->get_rpc_tenant_id(), param))) {
              LOG_WARN("failed to update stat cache", K(ret));
            } else {
              LOG_TRACE("succeed to gather table stats", K(param));
            }
            running_monitor.set_monitor_result(ret, ObTimeUtility::current_time(), param.allocator_->used());
            pl::ObDbmsStats::update_optimizer_gather_stat_info(NULL, &gather_stat);
            ObOptStatGatherStatList::instance().remove(gather_stat);
            task_info.completed_table_count_ ++;
          }
          task_info.task_end_time_ = ObTimeUtility::current_time();
          task_info.ret_code_ = ret;
          task_info.failed_count_ = ret == OB_SUCCESS ? 0 : params.count() - i + 1;
          pl::ObDbmsStats::update_optimizer_gather_stat_info(&task_info, NULL);
        }
      }
    }
  }
  return ret;
}

} // end of SQL
} // end of OceanBase
