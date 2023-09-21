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
#include "ob_dbms_stats_utils.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_osg_column_stat.h"
#include "share/object/ob_obj_cast.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"
#include "share/schema/ob_part_mgr_util.h"

namespace oceanbase
{
namespace common
{
int ObDbmsStatsUtils::get_part_info(const ObTableStatParam &param,
                                    const ObExtraParam &extra,
                                    PartInfo &part_info)
{
  int ret = OB_SUCCESS;
  if (extra.type_ == TABLE_LEVEL) {
    part_info.part_name_ = param.tab_name_;
    part_info.part_stattype_ = param.stattype_;
    part_info.part_id_ = param.global_part_id_;
    part_info.tablet_id_ = param.global_part_id_;
  } else if (extra.type_ == PARTITION_LEVEL) {
    if (OB_UNLIKELY(extra.nth_part_ >= param.part_infos_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition index",
               K(ret), K(extra.nth_part_), K(param.part_infos_.count()));
    } else {
      part_info = param.part_infos_.at(extra.nth_part_);
    }
  } else if (extra.type_ == SUBPARTITION_LEVEL) {
    if (OB_UNLIKELY(extra.nth_part_ >= param.subpart_infos_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid subpartition index",
               K(ret), K(extra.nth_part_), K(param.subpart_infos_.count()));
    } else {
      part_info = param.subpart_infos_.at(extra.nth_part_);
    }
  }
  return ret;
}

int ObDbmsStatsUtils::init_col_stats(ObIAllocator &allocator,
                                     int64_t col_cnt,
                                     ObIArray<ObOptColumnStat*> &col_stats)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error, expected specify column cnt is great 0", K(ret), K(col_cnt));
  } else if (OB_FAIL(col_stats.prepare_allocate(col_cnt))) {
    LOG_WARN("failed to prepare allocate column stat", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      ObOptColumnStat *&col_stat = col_stats.at(i);
      if (OB_ISNULL(col_stat = ObOptColumnStat::malloc_new_column_stat(allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(col_stat));
      }
    }
  }
  return ret;
}

/* @brief ObDbmsStatsUtils::check_range_skew, check data is skewed or not
 * Based on Oracle 12c:
 * 1.for frequency histogram:
 *  if the number of value in all buckets is the same, then it's even distributed, Otherwise, it's
 *  skewed.
 * 2.for hybrid histogram: ==> refine it, TODO@jiangxiu.wt
 *  if the repeat count of value in all buckets is less than total_not_null_row_count / bucket_num,
 *  then it's even distributed, Otherwise, it's skewed.
 */
int ObDbmsStatsUtils::check_range_skew(ObHistType hist_type,
                                       const ObHistogram::Buckets &bkts,
                                       int64_t standard_cnt,
                                       bool &is_even_distributed)
{
  int ret = OB_SUCCESS;
  is_even_distributed = false;
  if (hist_type == ObHistType::FREQUENCY) {
    is_even_distributed = true;
    for (int64_t i = 0; is_even_distributed && i < bkts.count(); ++i) {
      if (i == 0) {
        is_even_distributed = standard_cnt == bkts.at(i).endpoint_num_;
      } else {
        is_even_distributed = standard_cnt == bkts.at(i).endpoint_num_ -
                                                                       bkts.at(i - 1).endpoint_num_;
      }
    }
  } else if (hist_type == ObHistType::HYBIRD) {
    is_even_distributed = true;
    for (int64_t i = 0; is_even_distributed && i < bkts.count(); ++i) {
      is_even_distributed = bkts.at(i).endpoint_repeat_count_ <= standard_cnt;
    }
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsUtils::batch_write(share::schema::ObSchemaGetterGuard *schema_guard,
                                  const uint64_t tenant_id,
                                  ObMySQLTransaction &trans,
                                  ObIArray<ObOptTableStat *> &table_stats,
                                  ObIArray<ObOptColumnStat*> &column_stats,
                                  const int64_t current_time,
                                  const bool is_index_stat,
                                  const bool is_history_stat,
                                  const bool is_online_stat,
                                  const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptStatManager::get_instance().batch_write(schema_guard,
                                                           tenant_id,
                                                           trans,
                                                           table_stats,
                                                           column_stats,
                                                           current_time,
                                                           is_index_stat,
                                                           is_history_stat,
                                                           print_params))) {
    LOG_WARN("failed to batch write stats", K(ret));
  //histroy stat is from cache no need free.
  } else if (!is_history_stat && !is_online_stat) {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); ++i) {
      if (NULL != table_stats.at(i)) {
        table_stats.at(i)->~ObOptTableStat();
        table_stats.at(i) = NULL;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); ++i) {
      if (NULL != column_stats.at(i)) {
        column_stats.at(i)->~ObOptColumnStat();
        column_stats.at(i) = NULL;
      }
    }
  }
  return ret;
}

int ObDbmsStatsUtils::cast_number_to_double(const number::ObNumber &src_val, double &dst_val)
{
  int ret = OB_SUCCESS;
  ObObj src_obj;
  ObObj dest_obj;
  src_obj.set_number(src_val);
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_PARSER);
  ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
  if (OB_FAIL(ObObjCaster::to_type(ObDoubleType, cast_ctx, src_obj, dest_obj))) {
    LOG_WARN("failed to cast number to double type", K(ret));
  } else if (OB_FAIL(dest_obj.get_double(dst_val))) {
    LOG_WARN("failed to get double", K(ret));
  } else {
    LOG_TRACE("succeed to cast number to double", K(src_val), K(dst_val));
  }
  return ret;
}

// gather statistic related inner table should not read or write during tenant restore or on 
// standby cluster.
int ObDbmsStatsUtils::check_table_read_write_valid(const uint64_t tenant_id, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  bool in_restore = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->check_tenant_is_restore(NULL, tenant_id, in_restore))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_UNLIKELY(in_restore) || GCTX.is_standby_cluster()) {
    is_valid = false;
  }
  return ret;
}

//now we support user table、sys table and virtual table to gather and use optimizer stats.
int ObDbmsStatsUtils::check_is_stat_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                          const uint64_t tenant_id,
                                          const int64_t table_id,
                                          bool &is_valid)
{
  bool ret = OB_SUCCESS;
  is_valid = false;
  const ObTableSchema *table_schema = NULL;
  if (is_sys_table(table_id)) {//check sys table
    if (OB_FAIL(check_is_sys_table(schema_guard, tenant_id, table_id, is_valid))) {
      LOG_WARN("failed to check is sys table", K(ret));
    }
  } else if (is_virtual_table(table_id)) {//check virtual table
    is_valid = !is_no_stat_virtual_table(table_id);
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    //do nothing
  } else {//check user table
    is_valid = table_schema->is_user_table() || table_schema->is_external_table();
  }
  return ret;
}

int ObDbmsStatsUtils::check_is_sys_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                         const uint64_t tenant_id,
                                         const int64_t table_id,
                                         bool &is_valid)
{
  bool ret = OB_SUCCESS;
  const ObSimpleTenantSchema *tenant = NULL;
  is_valid = false;
  if (!is_sys_table(table_id) ||
      ObSysTableChecker::is_sys_table_index_tid(table_id) ||
      is_sys_lob_table(table_id) ||
      table_id == share::OB_ALL_TABLE_STAT_TID ||
      table_id == share::OB_ALL_COLUMN_STAT_TID ||
      table_id == share::OB_ALL_HISTOGRAM_STAT_TID ||
      table_id == share::OB_ALL_TABLE_STAT_HISTORY_TID ||
      table_id == share::OB_ALL_COLUMN_STAT_HISTORY_TID ||
      table_id == share::OB_ALL_HISTOGRAM_STAT_HISTORY_TID ||
      table_id == share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TID ||//circular dependency
      table_id == share::OB_ALL_OPTSTAT_USER_PREFS_TID ||
//bug:
      table_id == share::OB_ALL_SYS_VARIABLE_TID ||//circular dependency
      table_id == share::OB_ALL_SYS_VARIABLE_HISTORY_TID ||//circular dependency
      table_id == share::OB_ALL_MONITOR_MODIFIED_TID) {
    is_valid = false;
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
  //now sys table stat only use and gather in normal tenant
  } else if (OB_ISNULL(tenant) || !tenant->is_normal()) {
    is_valid = false;
  } else {
    is_valid = true;
  }
  return ret;
}

//following virtual table access error, we temporarily disable gather table stats.
bool ObDbmsStatsUtils::is_no_stat_virtual_table(const int64_t table_id)
{
  return is_virtual_index_table(table_id) ||
         table_id == share::OB_TENANT_VIRTUAL_ALL_TABLE_TID ||
         table_id == share::OB_TENANT_VIRTUAL_TABLE_COLUMN_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID ||
         table_id == share::OB_TENANT_VIRTUAL_CURRENT_TENANT_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_TABLES_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSTAT_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_SCHEMA_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_PARTITION_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID ||
         table_id == share::OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_TID ||
         table_id == share::OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID ||
         table_id == share::OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TID ||
         table_id == share::OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSION_EVENT_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_ROUTINE_TID ||
         table_id == share::OB_ALL_VIRTUAL_TX_DATA_TID ||
         table_id == share::OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID ||
         table_id == share::OB_ALL_VIRTUAL_TRANS_SCHEDULER_TID ||
         table_id == share::OB_ALL_VIRTUAL_SQL_AUDIT_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSTAT_ORA_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORA_TID ||
         table_id == share::OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TID ||
         table_id == share::OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_AUTO_INCREMENT_REAL_AGENT_ORA_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_SCHEMA_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_PARTITION_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_TRACE_SPAN_INFO_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_TRANS_STAT_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TID ||
         table_id == share::OB_ALL_VIRTUAL_TRANS_SCHEDULER_ORA_TID;
}

bool ObDbmsStatsUtils::is_virtual_index_table(const int64_t table_id)
{
  return table_id == share::OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSION_EVENT_ALL_VIRTUAL_SESSION_EVENT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSION_WAIT_ALL_VIRTUAL_SESSION_WAIT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SYSTEM_EVENT_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSTAT_ALL_VIRTUAL_SESSTAT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SYSSTAT_ALL_VIRTUAL_SYSSTAT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SQL_AUDIT_ALL_VIRTUAL_SQL_AUDIT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SQL_AUDIT_ORA_ALL_VIRTUAL_SQL_AUDIT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSION_WAIT_ORA_ALL_VIRTUAL_SESSION_WAIT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SESSTAT_ORA_ALL_VIRTUAL_SESSTAT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SYSSTAT_ORA_ALL_VIRTUAL_SYSSTAT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_ASH_ALL_VIRTUAL_ASH_I1_TID ||
         table_id == share::OB_ALL_VIRTUAL_ASH_ORA_ALL_VIRTUAL_ASH_I1_TID;
}

int ObDbmsStatsUtils::parse_granularity(const ObString &granularity, ObGranularityType &granu_type)
{
  int ret = OB_SUCCESS;
  if (0 == granularity.case_compare("all")) {
    granu_type = ObGranularityType::GRANULARITY_ALL;
  } else if (0 == granularity.case_compare("auto") ||
             0 == granularity.case_compare("Z")) {
    granu_type = ObGranularityType::GRANULARITY_AUTO;
  } else if (0 == granularity.case_compare("default") ||
             0 == granularity.case_compare("global and partition")) {
    granu_type = ObGranularityType::GRANULARITY_GLOBAL_AND_PARTITION;
  } else if (0 == granularity.case_compare("approx_global and partition")) {
    granu_type = ObGranularityType::GRANULARITY_APPROX_GLOBAL_AND_PARTITION;
  } else if (0 == granularity.case_compare("global")) {
    granu_type = ObGranularityType::GRANULARITY_GLOBAL;
  } else if (0 == granularity.case_compare("partition")) {
    granu_type = ObGranularityType::GRANULARITY_PARTITION;
  } else if (0 == granularity.case_compare("subpartition")) {
    granu_type = ObGranularityType::GRANULARITY_SUBPARTITION;
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal granularity : must be AUTO | ALL | GLOBAL | PARTITION | SUBPARTITION" \
             "| GLOBAL AND PARTITION | APPROX_GLOBAL AND PARTITION", K(ret));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal granularity : must be AUTO | ALL | GLOBAL |" \
             " PARTITION | SUBPARTITION | GLOBAL AND PARTITION | APPROX_GLOBAL AND PARTITION");
  }
  return ret;
}

int ObDbmsStatsUtils::split_batch_write(sql::ObExecContext &ctx,
                                        ObIArray<ObOptTableStat*> &table_stats,
                                        ObIArray<ObOptColumnStat*> &column_stats,
                                        const bool is_index_stat/*default false*/,
                                        const bool is_history_stat/*default false*/,
                                        const bool is_online_stat /*default false*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(split_batch_write(ctx.get_virtual_table_ctx().schema_guard_,
                                ctx.get_my_session(),
                                ctx.get_sql_proxy(),
                                table_stats,
                                column_stats,
                                is_index_stat,
                                is_history_stat,
                                is_online_stat))) {
    LOG_WARN("failed to split batch write", K(ret));
  }
  return ret;
}

int ObDbmsStatsUtils::split_batch_write(share::schema::ObSchemaGetterGuard *schema_guard,
                                        sql::ObSQLSessionInfo *session_info,
                                        common::ObMySQLProxy *sql_proxy,
                                        ObIArray<ObOptTableStat*> &table_stats,
                                        ObIArray<ObOptColumnStat*> &column_stats,
                                        const bool is_index_stat/*default false*/,
                                        const bool is_history_stat/*default false*/,
                                        const bool is_online_stat /*default false*/)
{
  int ret = OB_SUCCESS;
  int64_t idx_tab_stat = 0;
  int64_t idx_col_stat = 0;
  //avoid the write stat sql is too long, we split write table stats and column stats:
  //  write 2000 tables and 2000 columns every time.
  LOG_DEBUG("dbms stats write stats", K(table_stats), K(column_stats));
  const int64_t MAX_NUM_OF_WRITE_STATS = 2000;
  int64_t current_time = ObTimeUtility::current_time();
  ObMySQLTransaction trans;
  //begin trans before writing stats
  if (OB_ISNULL(session_info) || OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info), K(sql_proxy));
  } else if (OB_FAIL(trans.start(sql_proxy, session_info->get_effective_tenant_id()))) {
    LOG_WARN("fail to start transaction", K(ret));
  }
  while (OB_SUCC(ret) &&
        (idx_tab_stat < table_stats.count() || idx_col_stat < column_stats.count())) {
    ObSEArray<ObOptTableStat*, 4> write_table_stats;
    ObSEArray<ObOptColumnStat*, 4> write_column_stats;
    if (OB_UNLIKELY(idx_tab_stat > table_stats.count() || idx_col_stat > column_stats.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpectd error", K(ret), K(idx_tab_stat), K(table_stats.count()),
                                      K(idx_col_stat), K(column_stats.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX_NUM_OF_WRITE_STATS && idx_tab_stat < table_stats.count(); ++i) {
        if (OB_FAIL(write_table_stats.push_back(table_stats.at(idx_tab_stat++)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
      int64_t col_stat_cnt = 0;
      int64_t hist_stat_cnt = 0;
      while (OB_SUCC(ret) &&
             col_stat_cnt < MAX_NUM_OF_WRITE_STATS &&
             hist_stat_cnt < MAX_NUM_OF_WRITE_STATS &&
             idx_col_stat < column_stats.count()) {
        ObOptColumnStat *cur_opt_col_stat = column_stats.at(idx_col_stat);
        if (OB_ISNULL(cur_opt_col_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(cur_opt_col_stat));
        } else if (OB_FAIL(write_column_stats.push_back(cur_opt_col_stat))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          ++ col_stat_cnt;
          ++ idx_col_stat;
          if (cur_opt_col_stat->get_histogram().is_valid()) {
            hist_stat_cnt += cur_opt_col_stat->get_histogram().get_bucket_size();
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDbmsStatsUtils::batch_write(schema_guard,
                                                session_info->get_effective_tenant_id(),
                                                trans,
                                                write_table_stats,
                                                write_column_stats,
                                                current_time,
                                                is_index_stat,
                                                is_history_stat,
                                                is_online_stat,
                                                CREATE_OBJ_PRINT_PARAM(session_info)))) {
        LOG_WARN("failed to batch write stats", K(ret), K(idx_tab_stat), K(idx_col_stat));
      } else {/*do nothing*/}
    }
  }
  //end trans after writing stats.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      LOG_WARN("fail to commit transaction", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("fail to roll back transaction", K(tmp_ret));
    }
  }
  return ret;
}

int ObDbmsStatsUtils::batch_write_history_stats(sql::ObExecContext &ctx,
                                                ObIArray<ObOptTableStatHandle> &history_tab_handles,
                                                ObIArray<ObOptColumnStatHandle> &history_col_handles)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStat*, 4> table_stats;
  ObSEArray<ObOptColumnStat*, 4> column_stats;
  for (int64_t i = 0; OB_SUCC(ret) && i < history_tab_handles.count(); ++i) {
    if (OB_FAIL(table_stats.push_back(const_cast<ObOptTableStat*>(history_tab_handles.at(i).stat_)))) {
      LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < history_col_handles.count(); ++i) {
    if (OB_FAIL(column_stats.push_back(const_cast<ObOptColumnStat*>(history_col_handles.at(i).stat_)))) {
      LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(split_batch_write(ctx, table_stats, column_stats, false, true))) {
      LOG_WARN("failed to split batch write", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

bool ObDbmsStatsUtils::is_subpart_id(const ObIArray<PartInfo> &partition_infos,
                                     const int64_t partition_id,
                                     int64_t &part_id)
{
  bool is_true = false;
  part_id = OB_INVALID_ID;
  for (int64_t i = 0; !is_true && i < partition_infos.count(); ++i) {
    is_true = (partition_infos.at(i).first_part_id_ != OB_INVALID_ID &&
               partition_id == partition_infos.at(i).part_id_);
    if (is_true) {
      part_id = partition_infos.at(i).first_part_id_;
    }
  }
  return is_true;
}

int ObDbmsStatsUtils::get_valid_duration_time(const int64_t start_time,
                                              const int64_t max_duration_time,
                                              int64_t &valid_duration_time)
{
  int ret = OB_SUCCESS;
  const int64_t current_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(start_time <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(start_time), K(ret));
  } else if (max_duration_time == -1) {
    //do nothing
  } else if (OB_UNLIKELY(current_time - start_time >= max_duration_time)) {
    ret = OB_TIMEOUT;
    LOG_WARN("reach the duration time", K(ret), K(current_time), K(start_time), K(max_duration_time));
  } else {
    valid_duration_time = max_duration_time - (current_time - start_time);
  }
  return ret;
}

int ObDbmsStatsUtils::get_dst_partition_by_tablet_id(sql::ObExecContext &ctx,
                                                     const uint64_t tablet_id,
                                                     const ObIArray<PartInfo> &partition_infos,
                                                     int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  partition_id = -1;
  ObTabletID tmp_tablet_id(tablet_id);
  for (int64_t i = 0; partition_id == -1 && i < partition_infos.count(); ++i) {
    if (partition_infos.at(i).tablet_id_ == tmp_tablet_id) {
      partition_id = partition_infos.at(i).part_id_;
    }
  }
  if (OB_UNLIKELY(partition_id == -1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(tablet_id), K(partition_infos));
  } else {
    LOG_TRACE("succeed to get dst partition by tablet id", K(tablet_id), K(partition_infos), K(partition_id));
  }
  return ret;
}

int ObDbmsStatsUtils::calssify_opt_stat(const ObIArray<ObOptStat> &opt_stats,
                                        ObIArray<ObOptTableStat*> &table_stats,
                                        ObIArray<ObOptColumnStat*> &column_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < opt_stats.count(); ++i) {
    if (OB_FAIL(table_stats.push_back(opt_stats.at(i).table_stat_))) {
      LOG_WARN("failed to push back table stat", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < opt_stats.at(i).column_stats_.count(); ++j) {
        if (OB_ISNULL(opt_stats.at(i).column_stats_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(opt_stats.at(i).column_stats_.at(j)));
        } else if (opt_stats.at(i).column_stats_.at(j)->is_valid()) {
          ret = column_stats.push_back(opt_stats.at(i).column_stats_.at(j));
        }
      }
    }
  }
  return ret;
}

// merge history stats and online stats
// for each stats in history, first search wether is in online_stats(map).
//  if not exists, set_refactored.
//  else merge old and new stats;
int ObDbmsStatsUtils::merge_tab_stats(const ObTableStatParam &param,
                                      const TabStatIndMap &online_table_stats,
                                      common::ObIArray<ObOptTableStatHandle> &old_tab_handles,
                                      common::ObIArray<ObOptTableStat *> &dst_tab_stats)
{
  int ret = OB_SUCCESS;
  ObOptTableStat *tmp_tab_stat = NULL;

  // the map is faster than array traversal
  for (int64_t i = 0; OB_SUCC(ret) && i < old_tab_handles.count(); i++) {
    ObOptTableStat * old_tab_stat = const_cast<ObOptTableStat *>(old_tab_handles.at(i).stat_);
    ObOptTableStat::Key key(param.tenant_id_, old_tab_stat->get_table_id(),
                                 old_tab_stat->get_partition_id());
    if (OB_FAIL(online_table_stats.get_refactored(key, tmp_tab_stat))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to find in hashmap", K(ret));
      } else {
        if (OB_FAIL(dst_tab_stats.push_back(old_tab_stat))) {
          LOG_WARN("fail to push back table stats", K(ret));
        }
      }
    } else if (OB_FAIL(tmp_tab_stat->merge_table_stat(*old_tab_stat))) {
      //merge
      LOG_WARN("fail to merge new table stat with old table stat", K(ret));
    }
  }

  // put all stats into array for future use.
  FOREACH_X(it, online_table_stats, OB_SUCC(ret)) {
    if (is_part_id_valid(param, it->second->get_partition_id())) {
      if (OB_FAIL(dst_tab_stats.push_back(it->second))) {
        LOG_WARN("fail to push back table stats", K(ret));
      }
    }
  }
  LOG_DEBUG("OSG debug", K(dst_tab_stats));

  return ret;
}

int ObDbmsStatsUtils::merge_col_stats(const ObTableStatParam &param,
                                      const ColStatIndMap &online_column_stats,
                                      common::ObIArray<ObOptColumnStatHandle> &old_col_handles,
                                      common::ObIArray<ObOptColumnStat*> &dst_col_stats)
{
  int ret = OB_SUCCESS;
  ObOptColumnStat *tmp_col_stat = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < old_col_handles.count(); i++) {
    ObOptColumnStat *old_col_stat = const_cast<ObOptColumnStat *>(old_col_handles.at(i).stat_);
    ObOptColumnStat::Key key(param.tenant_id_,
                             old_col_stat->get_table_id(),
                             old_col_stat->get_partition_id(),
                             old_col_stat->get_column_id());
    if (OB_FAIL(online_column_stats.get_refactored(key, tmp_col_stat))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("failed to find in hashmap", K(ret));
      } else if (OB_FAIL(dst_col_stats.push_back(old_col_stat))) {
        LOG_WARN("fail to push back table stats", K(ret));
      }
    } else if (OB_FAIL(tmp_col_stat->merge_column_stat(*old_col_stat))) {
      LOG_WARN("fail to merge new table stat with old table stat", K(ret));
    }
  }

  FOREACH_X(it, online_column_stats, OB_SUCC(ret)) {
    // after merge, we need to re-calc ndv from llc
    bool is_valid = false;
    ObOptColumnStat *col_stat = NULL;
    if (OB_ISNULL(col_stat = it->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", K(ret));
    } else if (is_part_id_valid(param, col_stat->get_partition_id())) {
      col_stat->set_num_distinct(ObGlobalNdvEval::get_ndv_from_llc(col_stat->get_llc_bitmap()));
      if (OB_FAIL(dst_col_stats.push_back(col_stat))) {
        LOG_WARN("fail to push back table stats", K(ret));
      }
    }
  }
  LOG_DEBUG("OSG debug", K(dst_col_stats));
  return ret;
}

bool ObDbmsStatsUtils::is_part_id_valid(const ObTableStatParam &param,
                                           const ObObjectID part_id)
{
  bool is_valid = false;
  if (param.global_stat_param_.need_modify_) {
    if (part_id == param.global_part_id_) {
      is_valid = true;
    }
  }
  if (!is_valid && param.part_stat_param_.need_modify_) {
    for (int64_t i = 0; !is_valid && i < param.part_infos_.count(); i++) {
      if (part_id == param.part_infos_.at(i).part_id_) {
        is_valid = true;
      }
    }
  }
  if (!is_valid && param.subpart_stat_param_.need_modify_) {
    for (int64_t i = 0; !is_valid && i < param.subpart_infos_.count(); i++) {
      if (part_id == param.subpart_infos_.at(i).part_id_) {
        is_valid = true;
      }
    }
  }
  return is_valid;
}

int ObDbmsStatsUtils::get_part_ids_from_param(const ObTableStatParam &param,
                                              common::ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  if (param.global_stat_param_.need_modify_) {
    if (OB_FAIL(part_ids.push_back(param.global_part_id_))) {
      LOG_WARN("failed to push back partition id", K(ret));
    }
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (OB_FAIL(part_ids.push_back(param.part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back partition id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      if (OB_FAIL(part_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back partition id", K(ret));
      }
    }
  }
  return ret;
}

int ObDbmsStatsUtils::get_part_infos(const ObTableSchema &table_schema,
                                     ObIArray<PartInfo> &part_infos,
                                     ObIArray<PartInfo> &subpart_infos,
                                     ObIArray<int64_t> &part_ids,
                                     ObIArray<int64_t> &subpart_ids,
                                     OSGPartMap *part_map/*default null*/)
{
  int ret = OB_SUCCESS;
  const ObPartition *part = NULL;
  const bool is_twopart = (table_schema.get_part_level() == share::schema::PARTITION_LEVEL_TWO);
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  if (table_schema.is_partitioned_table()) {
    ObPartIterator iter(table_schema, check_partition_mode);
    while (OB_SUCC(ret) && OB_SUCC(iter.next(part))) {
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null partition", K(ret), K(part));
      } else {
        PartInfo part_info;
        part_info.part_name_ = part->get_part_name();
        part_info.part_id_ = part->get_part_id();
        part_info.tablet_id_ = part->get_tablet_id();
        if (OB_NOT_NULL(part_map)) {
          OSGPartInfo part_info;
          part_info.part_id_ = part->get_part_id();
          part_info.tablet_id_ = part->get_tablet_id();
          if (OB_FAIL(part_map->set_refactored(part->get_part_id(), part_info))) {
            LOG_WARN("fail to add part info to hashmap", K(ret), K(part_info), K(part->get_part_id()));
          }
        }
        int64_t origin_cnt = subpart_infos.count();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(part_infos.push_back(part_info))) {
          LOG_WARN("failed to push back part info", K(ret));
        } else if (OB_FAIL(part_ids.push_back(part_info.part_id_))) {
          LOG_WARN("failed to push back part id", K(ret));
        } else if (is_twopart &&
                   OB_FAIL(get_subpart_infos(table_schema, part, subpart_infos, subpart_ids, part_map))) {
          LOG_WARN("failed to get subpart info", K(ret));
        } else {
          part_infos.at(part_infos.count() - 1).subpart_cnt_ = subpart_infos.count() - origin_cnt;
          LOG_TRACE("succeed to get table part infos", K(part_info));
        }
      }
    }
    ret = (ret == OB_ITER_END ? OB_SUCCESS : ret);
  }
  return ret;
}

int ObDbmsStatsUtils::get_subpart_infos(const ObTableSchema &table_schema,
                                        const ObPartition *part,
                                        ObIArray<PartInfo> &subpart_infos,
                                        ObIArray<int64_t> &subpart_ids,
                                        OSGPartMap *part_map/*default NULL*/)
{
  int ret = OB_SUCCESS;
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  if (OB_ISNULL(part) ||
      OB_UNLIKELY(table_schema.get_part_level() != share::schema::PARTITION_LEVEL_TWO)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is null", K(ret), K(table_schema.get_part_level()));
  } else {
    const ObSubPartition *subpart = NULL;
    ObSubPartIterator sub_iter(table_schema, *part, check_partition_mode);
    while (OB_SUCC(ret) && OB_SUCC(sub_iter.next(subpart))) {
      if (OB_ISNULL(subpart)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null subpartition", K(ret));
      } else {
        PartInfo subpart_info;
        subpart_info.part_name_ = subpart->get_part_name();
        subpart_info.part_id_ = subpart->get_sub_part_id(); // means object_id
        subpart_info.tablet_id_ = subpart->get_tablet_id();
        subpart_info.first_part_id_ = part->get_part_id();
        if (OB_NOT_NULL(part_map)) {
          OSGPartInfo part_info;
          part_info.part_id_ = part->get_part_id();
          part_info.tablet_id_ = subpart->get_tablet_id();
          if (OB_FAIL(part_map->set_refactored(subpart->get_sub_part_id(), part_info))) {
            LOG_WARN("fail to add part info to hashmap", K(ret), K(part_info), K(subpart->get_sub_part_id()));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(subpart_infos.push_back(subpart_info))) {
          LOG_WARN("failed to push back subpart_info", K(ret));
        } else if (OB_FAIL(subpart_ids.push_back(subpart_info.part_id_))) {
          LOG_WARN("failed to push back part id", K(ret));
        } else {
          LOG_TRACE("succeed to get table part infos", K(subpart_info)) ;
        }
      }
    }
    ret = (ret == OB_ITER_END ? OB_SUCCESS : ret);
  }
  return ret;
}

int ObDbmsStatsUtils::truncate_string_for_opt_stats(const ObObj *old_obj,
                                                    ObIAllocator &alloc,
                                                    ObObj *&new_obj)
{
  int ret = OB_SUCCESS;
  bool is_truncated = false;
  if (OB_ISNULL(old_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (ObColumnStatParam::is_valid_opt_col_type(old_obj->get_type()) && old_obj->is_string_type()) {
    ObString str;
    if (OB_FAIL(old_obj->get_string(str))) {
      LOG_WARN("failed to get string", K(ret), K(str));
    } else {
      ObObj *tmp_obj = NULL;
      int64_t truncated_str_len = get_truncated_str_len(str, old_obj->get_collation_type());
      if (truncated_str_len == str.length()) {
        //do nothing
      } else if (OB_UNLIKELY(truncated_str_len < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(old_obj), K(str), K(truncated_str_len));
      } else if (OB_ISNULL(tmp_obj = static_cast<ObObj*>(alloc.alloc(sizeof(ObObj))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc buf", K(ret));
      } else {
        tmp_obj->set_varchar(str.ptr(), static_cast<int32_t>(truncated_str_len));
        tmp_obj->set_meta_type(old_obj->get_meta());
        new_obj = tmp_obj;
        is_truncated = true;
      }
    }
  }
  if (OB_SUCC(ret) && !is_truncated) {
    new_obj = const_cast<ObObj *>(old_obj);
  }
  LOG_TRACE("Succeed to truncate string obj for opt stats", KPC(old_obj), KPC(new_obj), K(is_truncated));
  return ret;
}


int ObDbmsStatsUtils::shadow_truncate_string_for_opt_stats(ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (ObColumnStatParam::is_valid_opt_col_type(obj.get_type()) && obj.is_string_type()) {
    ObString str;
    if (OB_FAIL(obj.get_string(str))) {
      LOG_WARN("failed to get string", K(ret), K(str), K(obj));
    } else {
      int64_t truncated_str_len = get_truncated_str_len(str, obj.get_collation_type());
      if (OB_UNLIKELY(truncated_str_len < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(obj), K(str), K(truncated_str_len));
      } else {
        str.assign_ptr(str.ptr(), static_cast<int32_t>(truncated_str_len));
        obj.set_common_value(str);
        LOG_TRACE("Succeed to shadow truncate string obj for opt stats", K(obj));
      }
    }
  }
  return ret;
}

int64_t ObDbmsStatsUtils::get_truncated_str_len(const ObString &str, const ObCollationType cs_type)
{
  int64_t truncated_str_len = 0;
  int64_t mb_len = ObCharset::strlen_char(cs_type, str.ptr(), str.length());
  if (mb_len <= OPT_STATS_MAX_VALUE_CHAR_LEN) {//keep origin str
    truncated_str_len = str.length();
  } else {//get max truncate str len
    truncated_str_len = ObCharset::charpos(cs_type, str.ptr(), str.length(), OPT_STATS_MAX_VALUE_CHAR_LEN);
  }
  LOG_TRACE("Succeed to get truncated str len", K(str), K(cs_type), K(truncated_str_len));
  return truncated_str_len;
}

int ObDbmsStatsUtils::get_current_opt_stats(const ObTableStatParam &param,
                                            ObIArray<ObOptTableStatHandle> &cur_tab_handles,
                                            ObIArray<ObOptColumnStatHandle> &cur_col_handles)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> part_ids;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_FAIL(get_part_ids_and_column_ids(param, part_ids, column_ids))) {
    LOG_WARN("failed to get part ids and column ids", K(ret));
  } else if (OB_FAIL(erase_stat_cache(param.tenant_id_, param.table_id_,
                                                        part_ids, column_ids))) {
    LOG_WARN("failed to erase stat cache", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_table_stat(param.tenant_id_,
                                                                     param.table_id_,
                                                                     part_ids,
                                                                     cur_tab_handles))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_column_stat(param.tenant_id_,
                                                                      param.table_id_,
                                                                      part_ids,
                                                                      column_ids,
                                                                      cur_col_handles))) {
    LOG_WARN("failed to get column stat", K(ret));
  }
  return ret;
}

int ObDbmsStatsUtils::get_part_ids_and_column_ids(const ObTableStatParam &param,
                                                  ObIArray<int64_t> &part_ids,
                                                  ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  //get part ids
  if (param.global_stat_param_.need_modify_) {
    int64_t part_id = param.global_part_id_;
    if (OB_FAIL(part_ids.push_back(part_id))) {
        LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  }

  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (OB_FAIL(part_ids.push_back(param.part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
  }

  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      if (OB_FAIL(part_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
  }
  //get column ids
  for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
    if (OB_FAIL(column_ids.push_back(param.column_params_.at(i).column_id_))) {
      LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDbmsStatsUtils::erase_stat_cache(const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       const ObIArray<int64_t> &part_ids,
                                       const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptStatManager::get_instance().erase_table_stat(tenant_id, table_id, part_ids))) {
    LOG_WARN("failed to erase table stats", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().erase_column_stat(tenant_id,
                                                                        table_id,
                                                                        part_ids,
                                                                        column_ids))) {
    LOG_WARN("failed to erase column stats", K(ret));
  } else {/*do nothing*/}
  return ret;
}

}
}
