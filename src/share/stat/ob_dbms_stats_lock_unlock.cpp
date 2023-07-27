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
#include "share/stat/ob_dbms_stats_lock_unlock.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/ob_dbms_stats_history_manager.h"
#include "share/stat/ob_incremental_stat_estimator.h"
#include "share/stat/ob_opt_table_stat_cache.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase {
using namespace sql;
namespace common {

#define UPDATE_STAT_STATTYPE_LOCKED "UPDATE __all_table_stat set stattype_locked = stattype_locked %c %u \
                                     where tenant_id = %lu and table_id = %lu %s%s;"

#define INSERT_TABLE_STAT_SQL "INSERT INTO __all_table_stat(tenant_id, \
                                                            table_id, \
                                                            partition_id, \
                                                            index_type,\
                                                            object_type, \
                                                            last_analyzed, \
                                                            sstable_row_cnt, \
                                                            sstable_avg_row_len, \
                                                            macro_blk_cnt, \
                                                            micro_blk_cnt, \
                                                            memtable_row_cnt, \
                                                            memtable_avg_row_len, \
                                                            row_cnt, \
                                                            avg_row_len, \
                                                            stattype_locked) VALUES"

#define CHECK_TABLE_STAT_EXISTS "select partition_id from __all_table_stat where \
                                 tenant_id = %lu and table_id = %lu;"

#define GET_LOCKED_PARTITION_STAT "select partition_id, stattype_locked from __all_table_stat \
                                  where %s and tenant_id = %lu and table_id = %lu;"

// for index, should get the date table's locked partition
#define GET_INDEX_LOCKED_PARTITION_STAT "select partition_id, stattype_locked from __all_table_stat \
                                  where %s and tenant_id = %lu and table_id in (%lu, %lu);"

int ObDbmsStatsLockUnlock::set_table_stats_lock(ObExecContext &ctx,
                                                const ObTableStatParam &param,
                                                bool set_locked)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString partition_list;
  ObSqlString table_list;
  ObSqlString raw_sql;
  ObSqlString insert_sql;
  bool specify_part = !param.part_name_.empty();
  bool need_update_lock = false;
  ObMySQLTransaction trans;
  ObSEArray<int64_t, 4> no_stats_partition_ids;//used to save partition which have no stats
  ObSEArray<uint64_t, 4> part_stattypes;
  ObSEArray<int64_t, 4> dummy_array;
  ObSEArray<ObOptTableStatHandle, 4> history_tab_handles;
  ObSEArray<ObOptColumnStatHandle, 4> history_col_handles;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, param.table_id_);
  if (OB_ISNULL(mysql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy));
  } else if (OB_FAIL(get_stats_history_sql(ctx, param, set_locked,
                                           need_update_lock, no_stats_partition_ids,
                                           part_stattypes,
                                           history_tab_handles,
                                           history_col_handles))) {
    LOG_WARN("failed to get stats history sql", K(ret));
  } else if (!need_update_lock) {
    LOG_TRACE("no need update lock", K(need_update_lock), K(param), K(set_locked));
  } else if (OB_FAIL(gen_partition_list(param, partition_list, dummy_array))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(raw_sql.append_fmt(UPDATE_STAT_STATTYPE_LOCKED,
                                        set_locked ? '|' : '^',
                                        param.stattype_,
                                        ext_tenant_id,
                                        pure_table_id,
                                        specify_part ? "and partition_id in" : "",
                                        specify_part ? partition_list.ptr() : ""))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(get_insert_locked_type_sql(param, no_stats_partition_ids,
                                                part_stattypes, insert_sql))) {
    LOG_WARN("failed to get insert locked type sql", K(ret));
  } else if (OB_FAIL(trans.start(mysql_proxy, param.tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(trans.write(param.tenant_id_, raw_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
  } else if (!insert_sql.empty() &&
             OB_FAIL(trans.write(param.tenant_id_, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
  } else {
    LOG_TRACE("Succeed to lock table stats", K(raw_sql), K(insert_sql));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      OB_LOG(WARN, "failed to commit", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::batch_write_history_stats(ctx,
                                                                   history_tab_handles,
                                                                   history_col_handles))) {
      LOG_WARN("failed to batch write history stats", K(ret));
    } else {/*do nothing*/}
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
    }
  }
  return ret;
}

int ObDbmsStatsLockUnlock::get_stats_history_sql(ObExecContext &ctx,
                                                 const ObTableStatParam &param,
                                                 bool set_locked,
                                                 bool &need_update_lock,
                                                 ObIArray<int64_t> &no_stats_partition_ids,
                                                 ObIArray<uint64_t> &part_stattypes,
                                                 ObIArray<ObOptTableStatHandle> &history_tab_handles,
                                                 ObIArray<ObOptColumnStatHandle> &history_col_handles)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString lock_str;
  ObSqlString unlock_str;
  ObSqlString partition_list;
  bool get_result = false;
  bool specify_part = !param.part_name_.empty();
  ObSEArray<int64_t, 4> all_partition_ids;
  ObSEArray<int64_t, 4> stat_partition_ids;
  ObSEArray<int64_t, 4> stattype_locked;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, param.table_id_);
  need_update_lock = false;
  if (OB_FAIL(gen_partition_list(param, partition_list, all_partition_ids))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(lock_str.append_fmt("(%s%s)",
                                 specify_part ? "partition_id in" : "1",
                                 specify_part ? partition_list.ptr() : ""))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(unlock_str.append_fmt("(stattype_locked & %u %s%s)",
                                    param.stattype_,
                                    specify_part ? "and partition_id in" : "",
                                    specify_part ? partition_list.ptr() : ""))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {//specify table
    if (OB_FAIL(raw_sql.append_fmt(GET_LOCKED_PARTITION_STAT,
                                   set_locked ? lock_str.ptr() : unlock_str.ptr(),
                                   ext_tenant_id,
                                   pure_table_id))) {
      LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
    } else if (OB_FAIL(get_stat_locked_partition_ids(ctx, param.tenant_id_, raw_sql,
                                                     stat_partition_ids,
                                                     stattype_locked))) {
      LOG_WARN("failed to get stat locked partition ids", K(ret));
    } else if (set_locked) {//lock
      if (OB_FAIL(get_no_stats_partition_ids(param.stattype_,
                                             all_partition_ids,
                                             stat_partition_ids,
                                             stattype_locked,
                                             no_stats_partition_ids,
                                             part_stattypes,
                                             need_update_lock))) {
        LOG_WARN("failed to get no stats partition ids", K(ret));
      } else {/*do nothing*/}
    } else {//unlock
      if (stat_partition_ids.empty()) {
        need_update_lock = false;
      } else {
        need_update_lock = true;
      }
    }

    if (OB_SUCC(ret) && need_update_lock) {
      //before lock, we need record history stats.
      if (OB_FAIL(ObDbmsStatsHistoryManager::get_history_stat_handles(ctx, param,
                                                                      history_tab_handles,
                                                                      history_col_handles))) {
        LOG_WARN("failed to get history stats", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObDbmsStatsLockUnlock::check_stat_locked(ObExecContext &ctx,
                                             ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSEArray<int64_t, 4> locked_partition_ids;
  ObSEArray<int64_t, 4> dummy_array;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, param.table_id_);
  int64_t dummy_idx = -1;
  if (!param.is_index_param() &&
      OB_FAIL(raw_sql.append_fmt(GET_LOCKED_PARTITION_STAT,
                                 "stattype_locked > 0",
                                 ext_tenant_id,
                                 pure_table_id))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (param.is_index_param() &&
             OB_FAIL(raw_sql.append_fmt(GET_INDEX_LOCKED_PARTITION_STAT,
                                        "stattype_locked > 0",
                                        ext_tenant_id,
                                        pure_table_id,
                                        share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, param.data_table_id_)))) {
    LOG_WARN("failed to append sql stmt", K(ret));
  } else if (OB_FAIL(get_stat_locked_partition_ids(ctx,
                                                   param.tenant_id_,
                                                   raw_sql,
                                                   locked_partition_ids,
                                                   dummy_array))) {
    LOG_WARN("failed to get stat locked partition ids", K(ret));
  } else if (locked_partition_ids.empty()) {//no locked table
    /*do nothing*/
  } else if (!param.is_index_param()
             ? param.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO
             : is_partition_id_locked(param.global_data_part_id_, locked_partition_ids, dummy_idx)) {
    //check the data table is locked for gather_index_stats
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("object statistics are locked", K(ret), K(locked_partition_ids));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"object statistics are locked");
  } else if (OB_FAIL(adjust_table_stat_param(locked_partition_ids, param))) {
    LOG_WARN("failed adjust gather table param", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsLockUnlock::fill_stat_locked(ObExecContext &ctx,
                                            ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSEArray<int64_t, 4> locked_partition_ids;
  ObSEArray<int64_t, 4> stattype_locked_array;
  uint64_t tenant_id = param.tenant_id_;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, param.table_id_);
  if (OB_FAIL(raw_sql.append_fmt(GET_LOCKED_PARTITION_STAT,
                                 "stattype_locked > 0",
                                 ext_tenant_id,
                                 pure_table_id))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(get_stat_locked_partition_ids(ctx,
                                                   param.tenant_id_,
                                                   raw_sql,
                                                   locked_partition_ids,
                                                   stattype_locked_array))) {
    LOG_WARN("failed to get stat locked partition ids", K(ret));
  } else if (locked_partition_ids.empty()) {//no locked table
    /*do nothing*/
  } else if (OB_FAIL(adjust_table_stat_locked(locked_partition_ids,
                                              stattype_locked_array,
                                              param))) {
    LOG_WARN("failed to adjust table stat locked", K(ret));
  } else {
    LOG_TRACE("succeed to adjust table stat locked", K(locked_partition_ids),
                                                     K(stattype_locked_array));
  }
  return ret;
}

int ObDbmsStatsLockUnlock::get_stat_locked_partition_ids(ObExecContext &ctx,
                                                         uint64_t tenant_id,
                                                         const ObSqlString &raw_sql,
                                                         ObIArray<int64_t> &partition_ids,
                                                         ObIArray<int64_t> &stattype_locked_array)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  if (OB_ISNULL(mysql_proxy) || OB_UNLIKELY(raw_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(raw_sql));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      const bool did_retry_weak = false;
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy, did_retry_weak);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          ObObj tmp;
          int64_t int_val = -1;
          int64_t idx1 = 0;
          int64_t idx2 = 1;
          if (OB_FAIL(client_result->get_obj(idx1, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tmp.get_int(int_val))) {
            LOG_WARN("failed to get int", K(ret), K(tmp));
          } else if (OB_FAIL(partition_ids.push_back(int_val))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(client_result->get_obj(idx2, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tmp.get_int(int_val))) {
            LOG_WARN("failed to get int", K(ret), K(tmp));
          } else if (OB_FAIL(stattype_locked_array.push_back(int_val))) {
            LOG_WARN("failed to push back", K(ret));
          } else {/*do nothing*/}
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("Succeed to get stat locked partition ids", K(partition_ids), K(raw_sql),
                                                          K(stattype_locked_array));
  }
  return ret;
}

int ObDbmsStatsLockUnlock::gen_partition_list(const ObTableStatParam &param,
                                              ObSqlString &partition_list,
                                              ObIArray<int64_t> &all_partition_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> partition_ids;
  if (param.part_name_.empty()) {
    int64_t part_id = param.global_part_id_;
    if (OB_FAIL(all_partition_ids.push_back(part_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (OB_FAIL(all_partition_ids.push_back(param.part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret), K(param.part_infos_.count()));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      if (OB_FAIL(all_partition_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret), K(param.subpart_infos_.count()));
      }
    }
  } else if (param.is_subpart_name_) {//specify subpart name
    if (OB_UNLIKELY(param.subpart_infos_.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param.subpart_infos_.count()));
    } else if (OB_FAIL(partition_ids.push_back(param.subpart_infos_.at(0).part_id_))) {
      LOG_WARN("failed to push back", K(ret), K(param.subpart_infos_.count()));
    } else if (OB_FAIL(all_partition_ids.push_back(param.subpart_infos_.at(0).part_id_))) {
      LOG_WARN("failed to push back", K(ret), K(param.subpart_infos_.count()));
    } else {/*do nothing*/}
  } else {//specify part name
    if (OB_UNLIKELY(param.part_infos_.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param.part_infos_.count()));
    } else if (OB_FAIL(partition_ids.push_back(param.part_infos_.at(0).part_id_))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
        if (OB_FAIL(partition_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing */}
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append(all_partition_ids, partition_ids))) {
          LOG_WARN("failed to append", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
      char prefix = (i == 0 ? '(' : ' ');
      char suffix = (i == partition_ids.count() - 1 ? ')' : ',');
      if (OB_FAIL(partition_list.append_fmt("%c%ld%c", prefix, partition_ids.at(i), suffix))) {
        LOG_WARN("failed to append sql", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObDbmsStatsLockUnlock::adjust_table_stat_param(const ObIArray<int64_t> &locked_partition_ids,
                                                   ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  bool has_valid_partition_id = false;
  ObSEArray<PartInfo, 4> new_part_infos;
  ObSEArray<PartInfo, 4> new_subpart_infos;
  int64_t idx = -1;
  if (locked_partition_ids.empty()) {
    /*do nothing*/
  } else {
    if (param.global_stat_param_.need_modify_) {
      int64_t part_id = param.global_part_id_;
      if (is_partition_id_locked(part_id, locked_partition_ids, idx)) {
        param.global_stat_param_.reset_gather_stat();
      } else {
        has_valid_partition_id = true;
      }
    }
    if (param.subpart_stat_param_.need_modify_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
        if (!is_partition_id_locked(param.subpart_infos_.at(i).part_id_, locked_partition_ids, idx)) {
          if (OB_FAIL(new_subpart_infos.push_back(param.subpart_infos_.at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          } else {
            has_valid_partition_id = true;
          }
        } else if (OB_FAIL(param.no_regather_partition_ids_.push_back(param.subpart_infos_.at(i).part_id_))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
    }
    if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
        if (!is_partition_id_locked(param.part_infos_.at(i).part_id_, locked_partition_ids, idx)) {
          if (ObIncrementalStatEstimator::is_part_can_incremental_gather(
                                                               param,
                                                               param.part_infos_.at(i).part_id_,
                                                               param.part_infos_.at(i).subpart_cnt_,
                                                               true)) {
            if (OB_FAIL(param.approx_part_infos_.push_back(param.part_infos_.at(i)))) {
              LOG_WARN("failed to push back", K(ret));
            } else {/*do nothing*/}
          } else if (OB_FAIL(new_part_infos.push_back(param.part_infos_.at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          } else {
            has_valid_partition_id = true;
          }
        } else if (ObIncrementalStatEstimator::is_part_can_incremental_gather(
                                                               param,
                                                               param.part_infos_.at(i).part_id_,
                                                               param.part_infos_.at(i).subpart_cnt_,
                                                               false)) {
          if (OB_FAIL(param.approx_part_infos_.push_back(param.part_infos_.at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          } else {/*do nothing*/}
        } else if (OB_FAIL(param.no_regather_partition_ids_.push_back(param.part_infos_.at(i).part_id_))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
    }
    if (OB_SUCC(ret)) {
      //1.lock all partition
      //2.specify partition name and the partition is be locked;
      if (!has_valid_partition_id ||
          (param.is_subpart_name_ && new_subpart_infos.empty()) ||
          (!param.part_name_.empty() && !param.is_subpart_name_ &&
           new_part_infos.empty() && param.approx_part_infos_.empty())) {
        ret = OB_ERR_DBMS_STATS_PL;
        LOG_WARN("object statistics are locked", K(ret), K(locked_partition_ids), K(param),
                                                 K(new_subpart_infos), K(new_part_infos));
        LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"object statistics are locked");
      } else if (OB_FAIL(param.subpart_infos_.assign(new_subpart_infos))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (OB_FAIL(param.part_infos_.assign(new_part_infos))) {
        LOG_WARN("failed to assign", K(ret));
      } else {
        param.subpart_stat_param_.need_modify_ = !new_subpart_infos.empty();
        param.part_stat_param_.need_modify_ = !new_part_infos.empty();
        if (param.global_stat_param_.need_modify_ &&
            param.global_stat_param_.gather_approx_ &&
            !param.part_stat_param_.need_modify_) {
          //if take approx global and partition to gather, but all partition are locked, should adjust
          //approx global gather to global gather.
          if (!param.subpart_stat_param_.need_modify_) {
            param.global_stat_param_.gather_approx_ = false;
          } else { //incremental partition gather stats from subpart stats.
            param.part_stat_param_.need_modify_ = true;
          }
        }
        LOG_TRACE("Succeed to adjust table stat param", K(param), K(locked_partition_ids));
      }
    }
  }
  return ret;
}

int ObDbmsStatsLockUnlock::adjust_table_stat_locked(const ObIArray<int64_t> &locked_partition_ids,
                                                    const ObIArray<int64_t> &stattype_locked_array,
                                                    ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (param.global_stat_param_.need_modify_) {
    int64_t part_id = param.global_part_id_;
    if (is_partition_id_locked(part_id, locked_partition_ids, idx)) {
      if (OB_UNLIKELY(idx < 0 || idx >= stattype_locked_array.count() ||
                      stattype_locked_array.at(idx) <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(idx), K(stattype_locked_array));
      } else {
        param.stattype_ = static_cast<StatTypeLocked>(stattype_locked_array.at(idx));
      }
    }
  }
  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      if (is_partition_id_locked(param.subpart_infos_.at(i).part_id_, locked_partition_ids, idx)) {
        if (OB_UNLIKELY(idx < 0 || idx >= stattype_locked_array.count() ||
                        stattype_locked_array.at(idx) <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(idx), K(stattype_locked_array));
        } else {
          param.subpart_infos_.at(i).part_stattype_ = stattype_locked_array.at(idx);
        }
      }
    }
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (is_partition_id_locked(param.part_infos_.at(i).part_id_, locked_partition_ids, idx)) {
        if (OB_UNLIKELY(idx < 0 || idx >= stattype_locked_array.count() ||
                        stattype_locked_array.at(idx) <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(idx), K(stattype_locked_array));
        } else {
          param.part_infos_.at(i).part_stattype_ = stattype_locked_array.at(idx);
        }
      }
    }
  }
  return ret;
}

bool ObDbmsStatsLockUnlock::is_partition_id_locked(int64_t partition_id,
                                                   const ObIArray<int64_t> &locked_partition_ids,
                                                   int64_t &idx)
{
  bool find_it = false;
  for (int64_t i = 0; !find_it && i < locked_partition_ids.count(); ++i) {
    if (locked_partition_ids.at(i) == partition_id) {
      find_it = true;
      idx = i;
    } else {/*do nothing*/}
  }
  return find_it;
}

int ObDbmsStatsLockUnlock::get_no_stats_partition_ids(const StatTypeLocked stattype,
                                                      const ObIArray<int64_t> &all_partition_ids,
                                                      const ObIArray<int64_t> &stat_partition_ids,
                                                      const ObIArray<int64_t> &stattype_locked,
                                                      ObIArray<int64_t> &no_stats_partition_ids,
                                                      ObIArray<uint64_t> &part_stattypes,
                                                      bool &need_update_lock)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> locked_partition_ids;
  need_update_lock = false;
  if (OB_UNLIKELY(stat_partition_ids.count() != stattype_locked.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(stat_partition_ids.count()),
                                     K(stattype_locked.count()));
  } else {
    bool need_reset_array = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_partition_ids.count(); ++i) {
      bool is_record = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_record && j < stat_partition_ids.count(); ++j) {
        if (all_partition_ids.at(i) == stat_partition_ids.at(j)) {
          is_record = true;
          if (!(stattype & stattype_locked.at(j))) {
            need_update_lock = true;
          } else {
            need_update_lock = false;
          }
        }
      }
      if (!is_record) {
        if (OB_FAIL(no_stats_partition_ids.push_back(all_partition_ids.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(part_stattypes.push_back(stattype))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          need_update_lock = true;
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsLockUnlock::get_insert_locked_type_sql(const ObTableStatParam &param,
                                                      const ObIArray<int64_t> &no_stats_partition_ids,
                                                      const ObIArray<uint64_t> &part_stattypes,
                                                      ObSqlString &insert_sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(no_stats_partition_ids.count() != part_stattypes.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(no_stats_partition_ids.count()),
                                     K(part_stattypes.count()));
  } else if (no_stats_partition_ids.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(insert_sql.append(INSERT_TABLE_STAT_SQL))) {
    LOG_WARN("failed to append", K(ret));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
    uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, param.table_id_);
    int64_t current_time = ObTimeUtility::current_time();
    StatLevel stat_level = INVALID_LEVEL;
    int64_t cur_part_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < no_stats_partition_ids.count(); ++i) {
      if (param.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO ||
          no_stats_partition_ids.at(i) == -1) {
        stat_level = TABLE_LEVEL;
      } else if (ObDbmsStatsUtils::is_subpart_id(param.all_subpart_infos_,
                                                 no_stats_partition_ids.at(i),
                                                 cur_part_id)) {
        stat_level = SUBPARTITION_LEVEL;
      } else {
        stat_level = PARTITION_LEVEL;
      }
      char suffix = (i == no_stats_partition_ids.count() - 1 ? ';' : ',');
      if (OB_FAIL(insert_sql.append_fmt("(%lu, %ld, %ld, %d, %u, usec_to_time('%ld'), -1, -1, 0, 0, -1,\
                                        -1, 0, 0, %ld)%c",
                                        ext_tenant_id,
                                        pure_table_id,
                                        no_stats_partition_ids.at(i),
                                        param.is_index_stat_,
                                        stat_level,
                                        current_time,
                                        part_stattypes.at(i),
                                        suffix))) {
        LOG_WARN("failed to append fmt", K(ret));
      }
    }
  }
  return ret;
}


} // namespace common
} // namespace oceanbase

#undef UPDATE_STAT_STATTYPE_LOCKED
#undef INSERT_TABLE_STAT_SQL
#undef CHECK_TABLE_STAT_EXISTS
#undef GET_LOCKED_PARTITION_STAT
