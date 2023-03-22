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
#include "share/object/ob_obj_cast.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"

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
  void *ptr = NULL;
  if (OB_UNLIKELY(col_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error, expected specify column cnt is great 0", K(ret), K(col_cnt));
  } else if (OB_FAIL(col_stats.prepare_allocate(col_cnt))) {
    LOG_WARN("failed to prepare allocate column stat", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      ObOptColumnStat *&col_stat = col_stats.at(i);
      if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptColumnStat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(ptr));
      } else {
        col_stat = new (ptr) ObOptColumnStat(allocator);
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
    is_valid = table_schema->is_user_table();
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
      table_id == share::OB_ALL_HISTOGRAM_STAT_HISTORY_TID) {
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
         table_id == share::OB_ALL_VIRTUAL_PROXY_SCHEMA_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_PARTITION_TID ||
         table_id == share::OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID ||
         table_id == share::OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID ||
         table_id == share::OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_TID ||
         table_id == share::OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID ||
         table_id == share::OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TID ||
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
         table_id == share::OB_ALL_VIRTUAL_TRANS_STAT_ORA_TID;
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
         table_id == share::OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID;
}

/**
 * @brief ObDbmsStats::parse_granularity
 * @param ctx
 * @param granularity
 * possible values are:
 *  ALL: Gather all (subpartition, partition, and global)
 *  AUTO: Oracle recommends setting granularity to the default value of AUTO to gather subpartition,
 *        partition, or global statistics, depending on partition type. oracle 12c auto is same as
 *        ALL, compatible it.
 *  DEFAULT: Gathers global and partition-level
 *  GLOBAL: Gather global only
 *  GLOBAL AND PARTITION: Gather global and partition-level
 *  APPROX_GLOBAL AND PARTITION: similar to 'GLOBAL AND PARTITION' but in this case the global
                                 statistics are aggregated from partition level statistics.
 *  PARTITION: Gather partition-level
 *  SUBPARTITION: Gather subpartition-level
 *  Oracle granularity actual behavior survey:
 *
 * @return
 */
int ObDbmsStatsUtils::parse_granularity(const ObString &granularity,
                                        bool &need_global,
                                        bool &need_approx_global,
                                        bool &need_part,
                                        bool &need_subpart)
{
  int ret = OB_SUCCESS;
  // first check the table is partitioned;
  if (0 == granularity.case_compare("all")) {
    need_global = true;
    need_part = true;
    need_subpart = true;
    need_approx_global = false;
  } else if (0 == granularity.case_compare("auto") ||
             0 == granularity.case_compare("Z")) {
    /*do nothing, use default value*/
  } else if (0 == granularity.case_compare("default") ||
             0 == granularity.case_compare("global and partition")) {
    need_global = true;
    need_part = true;
    need_subpart = false;
    need_approx_global = false;
  } else if (0 == granularity.case_compare("approx_global and partition")) {
    need_global = false;
    need_approx_global = true;
    need_part = true;
    need_subpart = false;
  } else if (0 == granularity.case_compare("global")) {
    need_global = true;
    need_part = false;
    need_subpart = false;
    need_approx_global = false;
  } else if (0 == granularity.case_compare("partition")) {
    need_global = false;
    need_part = true;
    need_subpart = false;
    need_approx_global = false;
  } else if (0 == granularity.case_compare("subpartition")) {
    need_global = false;
    need_part = false;
    need_subpart = true;
    need_approx_global = false;
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal granularity : must be AUTO | ALL | GLOBAL | PARTITION | SUBPARTITION" \
             "| GLOBAL AND PARTITION | APPROX_GLOBAL AND PARTITION", K(ret));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal granularity : must be AUTO | ALL | GLOBAL |" \
             " PARTITION | SUBPARTITION | GLOBAL AND PARTITION | APPROX_GLOBAL AND PARTITION");
  }
  LOG_TRACE("succeed to parse granularity", K(need_global), K(need_part), K(need_subpart));
  return ret;
}

int ObDbmsStatsUtils::split_batch_write(sql::ObExecContext &ctx,
                                        ObIArray<ObOptTableStat*> &table_stats,
                                        ObIArray<ObOptColumnStat*> &column_stats,
                                        const bool is_index_stat /*default false*/,
                                        const bool is_history_stat /*default false*/,
                                        const bool is_online_stat /*default false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_my_session()));
  } else if (OB_FAIL(split_batch_write(ctx.get_virtual_table_ctx().schema_guard_,
                                       ctx.get_my_session()->get_effective_tenant_id(),
                                       table_stats,
                                       column_stats,
                                       is_index_stat,
                                       is_history_stat,
                                       is_online_stat,
                                       CREATE_OBJ_PRINT_PARAM(ctx.get_my_session())))) {
    LOG_WARN("failed to split batch write", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsUtils::split_batch_write(share::schema::ObSchemaGetterGuard *schema_guard,
                                        const uint64_t tenant_id,
                                        ObIArray<ObOptTableStat*> &table_stats,
                                        ObIArray<ObOptColumnStat*> &column_stats,
                                        const bool is_index_stat/*default false*/,
                                        const bool is_history_stat/*default false*/,
                                        const bool is_online_stat /*default false*/,
                                        const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  int64_t idx_tab_stat = 0;
  int64_t idx_col_stat = 0;
  //avoid the write stat sql is too long, we split write table stats and column stats:
  //  write 2000 tables and 2000 columns every time.
  LOG_DEBUG("dbms stats write stats", K(table_stats), K(column_stats));
  const int64_t MAX_NUM_OF_WRITE_STATS = 2000;
  int64_t current_time = ObTimeUtility::current_time();
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
                                                tenant_id,
                                                write_table_stats,
                                                write_column_stats,
                                                current_time,
                                                is_index_stat,
                                                is_history_stat,
                                                is_online_stat,
                                                print_params))) {
        LOG_WARN("failed to batch write stats", K(ret), K(idx_tab_stat), K(idx_col_stat));
      } else {/*do nothing*/}
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
    bool is_valid = false;
    if (OB_FAIL(check_part_id_valid(param, it->second->get_partition_id(), is_valid))) {
      // if partition is locked, shouldn't gather.
      LOG_WARN("fail to check part id valid", K(ret));
    } else if (is_valid) {
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
  ObOptColumnStat *tmp_col_stat;

  for (int64_t i = 0; OB_SUCC(ret) && i < old_col_handles.count(); i++) {
    ObOptColumnStat * old_col_stat = const_cast<ObOptColumnStat *>(old_col_handles.at(i).stat_);
    ObOptColumnStat::Key key(param.tenant_id_, old_col_stat->get_table_id(),
                            old_col_stat->get_partition_id(), old_col_stat->get_column_id());
    if (OB_FAIL(online_column_stats.get_refactored(key, tmp_col_stat))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to find in hashmap", K(ret));
      } else {
        if (OB_FAIL(dst_col_stats.push_back(old_col_stat))) {
          LOG_WARN("fail to push back table stats", K(ret));
        }
      }
    } else if (OB_FAIL(tmp_col_stat->merge_column_stat(*old_col_stat))) {
      //merge
      LOG_WARN("fail to merge new table stat with old table stat", K(ret));
    }
  }

  FOREACH_X(it, online_column_stats, OB_SUCC(ret)) {
    // after merge, we need to re-calc ndv from llc.
    bool is_valid = false;
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", K(ret));
    } else if (OB_FAIL(check_part_id_valid(param, it->second->get_partition_id(), is_valid))) {
      // if partition is locked, shouldn't gather.
      LOG_WARN("fail to check part id valid", K(ret));
    } else if (is_valid) {
      it->second->set_num_distinct(ObGlobalNdvEval::get_ndv_from_llc(it->second->get_llc_bitmap()));
      if (OB_FAIL(dst_col_stats.push_back(it->second))) {
        LOG_WARN("fail to push back table stats", K(ret));
      }
    }
  }
  LOG_DEBUG("OSG debug", K(dst_col_stats));

  return ret;
}

int ObDbmsStatsUtils::check_part_id_valid(const ObTableStatParam &param,
                                          const ObObjectID part_id,
                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool found = false;
  LOG_DEBUG("check part_id valid", K(param), K(part_id));
  is_valid = false;
  if (param.need_global_) {
    if (part_id == param.global_part_id_) {
      is_valid = true;
    }
  }
  if (!is_valid && param.need_part_) {
    for (int64_t i = 0; !found && i < param.part_infos_.count(); i++) {
      if (part_id == param.part_infos_.at(i).part_id_) {
        found = true;
        is_valid = true;
      }
    }
  }
  if (!is_valid && param.need_subpart_) {
    for (int64_t i = 0; !found && i < param.subpart_infos_.count(); i++) {
      if (part_id == param.subpart_infos_.at(i).part_id_) {
        found = true;
        is_valid = true;
      }
    }
  }
  return ret;
}

int ObDbmsStatsUtils::get_part_ids_from_param(const ObTableStatParam &param,
                                              common::ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  if (param.need_global_) {
    if (OB_FAIL(part_ids.push_back(param.global_part_id_))) {
      LOG_WARN("failed to push back partition id", K(ret));
    }
  }
  if (OB_SUCC(ret) && param.need_part_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (OB_FAIL(part_ids.push_back(param.part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back partition id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && param.need_subpart_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      if (OB_FAIL(part_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back partition id", K(ret));
      }
    }
  }
  return ret;
}

}
}
