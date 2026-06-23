/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_dbms_catalog_stats_lock_unlock.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase {
using namespace sql;
using namespace sqlclient;
namespace common {

// Note: string values (db_name/table_name/partition_value) are appended via
// sql_append_hex_escape_str to avoid special-character issues.
#define UPDATE_CATALOG_STAT_STATTYPE_LOCKED_PREFIX                                                 \
  "UPDATE __all_catalog_table_stat set stattype_locked = stattype_locked %c %u \
                                              where tenant_id = %lu and catalog_id = %lu and db_name = "

#define INSERT_CATALOG_TABLE_STAT_SQL                                                              \
  "INSERT INTO __all_catalog_table_stat(tenant_id, \
                                                                             catalog_id, \
                                                                             db_name, \
                                                                             table_name, \
                                                                             partition_value, \
                                                                             schema_version, \
                                                                             last_analyzed, \
                                                                             last_modified, \
                                                                             file_count, \
                                                                             data_size, \
                                                                             row_cnt, \
                                                                             avg_row_len, \
                                                                             stattype_locked, \
                                                                             sample_size) VALUES"

#define GET_LOCKED_CATALOG_PARTITION_STAT_PREFIX                                                   \
  "select partition_value, stattype_locked from __all_catalog_table_stat \
                                           where %s and tenant_id = %lu and catalog_id = %lu and db_name = "

int ObDbmsCatalogStatsLockUnlock::set_catalog_table_stats_lock(const ObCatalogTableStatParam &param,
                                                               sql::ObExecContext &ctx,
                                                               bool set_locked)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString partition_value_list;
  ObSqlString raw_sql;
  ObSqlString insert_sql;
  bool specify_part = !param.part_name_.empty();
  bool need_update_lock = false;
  ObMySQLTransaction trans;
  ObSEArray<ObString, 4> no_stats_partition_values; // used to save partitions which have no stats
  ObSEArray<uint64_t, 4> part_stattypes;
  ObSEArray<ObString, 4> dummy_array;

  if (OB_ISNULL(mysql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy));
  } else if (OB_FAIL(trans.start(mysql_proxy, param.table_identity_.tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(get_catalog_stats_history_sql(param,
                                                   ctx,
                                                   trans,
                                                   set_locked,
                                                   need_update_lock,
                                                   no_stats_partition_values,
                                                   part_stattypes))) {
    LOG_WARN("failed to get stats history sql", K(ret));
  } else if (!need_update_lock) {
    LOG_TRACE("no need update lock", K(need_update_lock), K(param), K(set_locked));
  } else if (OB_FAIL(gen_partition_value_list(param, partition_value_list, dummy_array))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(raw_sql.append_fmt(UPDATE_CATALOG_STAT_STATTYPE_LOCKED_PREFIX,
                                        set_locked ? '|' : '^',
                                        param.gather_options_.stattype_,
                                        share::schema::ObSchemaUtils::get_extract_tenant_id(
                                            share::schema::ObSchemaUtils::get_exec_tenant_id(
                                                param.table_identity_.tenant_id_),
                                            param.table_identity_.tenant_id_),
                                        param.table_identity_.catalog_id_))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.db_name_, raw_sql))) {
    LOG_WARN("failed to append db name", K(ret), K(param.table_identity_.db_name_));
  } else if (OB_FAIL(raw_sql.append(" and table_name = "))) {
    LOG_WARN("failed to append table name", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.tab_name_, raw_sql))) {
    LOG_WARN("failed to append table name", K(ret), K(param.table_identity_.tab_name_));
  } else if (specify_part
             && OB_FAIL(raw_sql.append_fmt(" and partition_value in %s", partition_value_list.ptr()))) {
    LOG_WARN("failed to append partition filter", K(ret));
  } else if (OB_FAIL(raw_sql.append(";"))) {
    LOG_WARN("failed to append semicolon", K(ret));
  } else if (OB_FAIL(get_catalog_insert_locked_type_sql(param,
                                                        no_stats_partition_values,
                                                        part_stattypes,
                                                        insert_sql))) {
    LOG_WARN("failed to get insert locked type sql", K(ret));
  } else if (OB_FAIL(trans.write(param.table_identity_.tenant_id_, raw_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
  } else if (!insert_sql.empty()
             && OB_FAIL(trans.write(param.table_identity_.tenant_id_, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
  } else {
    LOG_TRACE("Succeed to lock catalog table stats", K(raw_sql), K(insert_sql));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      LOG_WARN("failed to commit", K(ret));
    } else { /*do nothing*/
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      LOG_WARN("failed to rollback trans", K(tmp_ret));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsLockUnlock::get_catalog_stats_history_sql(
    const ObCatalogTableStatParam &param,
    sql::ObExecContext &ctx,
    ObMySQLTransaction &trans,
    bool set_locked,
    bool &need_update_lock,
    ObIArray<ObString> &no_stats_partition_values,
    ObIArray<uint64_t> &part_stattypes)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString lock_str;
  ObSqlString unlock_str;
  ObSqlString partition_value_list;
  bool specify_part = !param.part_name_.empty();
  ObSEArray<ObString, 4> all_partition_values;
  ObSEArray<ObString, 4> stat_partition_values;
  ObSEArray<uint64_t, 4> stattype_locked;
  uint64_t tenant_id = param.table_identity_.tenant_id_;
  ObArenaAllocator allocator("ExtStatLock", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);

  need_update_lock = false;
  if (OB_FAIL(gen_partition_value_list(param, partition_value_list, all_partition_values))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(lock_str.append_fmt("(%s%s)",
                                         specify_part ? "partition_value in" : "1",
                                         specify_part ? partition_value_list.ptr() : ""))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(unlock_str.append_fmt("(stattype_locked & %u %s%s)",
                                           param.gather_options_.stattype_,
                                           specify_part ? "and partition_value in" : "",
                                           specify_part ? partition_value_list.ptr() : ""))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    if (OB_FAIL(raw_sql.append_fmt(GET_LOCKED_CATALOG_PARTITION_STAT_PREFIX,
                                   set_locked ? lock_str.ptr() : unlock_str.ptr(),
                                   share::schema::ObSchemaUtils::get_extract_tenant_id(
                                       share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id),
                                       tenant_id),
                                   param.table_identity_.catalog_id_))) {
      LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
    } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.db_name_, raw_sql))) {
      LOG_WARN("failed to append db name", K(ret), K(param.table_identity_.db_name_));
    } else if (OB_FAIL(raw_sql.append(" and table_name = "))) {
      LOG_WARN("failed to append table name", K(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.tab_name_, raw_sql))) {
      LOG_WARN("failed to append table name", K(ret), K(param.table_identity_.tab_name_));
    } else if (OB_FAIL(raw_sql.append(";"))) {
      LOG_WARN("failed to append semicolon", K(ret));
    } else if (OB_FAIL(get_catalog_stat_locked_partition_values(raw_sql,
                                                                ctx,
                                                                param.table_identity_.tenant_id_,
                                                                allocator,
                                                                stat_partition_values,
                                                                stattype_locked))) {
      LOG_WARN("failed to get stat locked partition values", K(ret));
    } else if (set_locked) { // lock
      // part_infos_ is always populated by parse_table_part_info now
      // (both partitioned and non-partitioned tables have part_infos_)
      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_catalog_no_stats_partition_values(param.gather_options_.stattype_,
                                                          all_partition_values,
                                                          stat_partition_values,
                                                          stattype_locked,
                                                          no_stats_partition_values,
                                                          part_stattypes,
                                                          need_update_lock))) {
          LOG_WARN("failed to get no stats partition values", K(ret));
        } else { /*do nothing*/
        }
      }
    } else { // unlock
      if (stat_partition_values.empty()) {
        need_update_lock = false;
      } else {
        need_update_lock = true;
      }
    }

    if (OB_SUCC(ret) && need_update_lock) {
      // Note: Catalog tables don't support history statistics backup
      // Unlike internal tables which call ObDbmsStatsHistoryManager::backup_opt_stats()
      // before locking, catalog tables skip this step as they don't have history tables
      // This is consistent with the design documented in ob_dbms_catalog_stats_executor.h
      /*do nothing*/
    }
  }
  return ret;
}

int ObDbmsCatalogStatsLockUnlock::check_catalog_stat_locked(sql::ObExecContext &ctx,
                                                            ObCatalogTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSEArray<ObString, 4> locked_partition_values;
  ObSEArray<uint64_t, 4> dummy_array;
  uint64_t tenant_id = param.table_identity_.tenant_id_;
  const bool has_part_name = !param.part_name_.empty();
  ObArenaAllocator allocator("ExtStatLock", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);

  if (OB_FAIL(raw_sql.append_fmt(GET_LOCKED_CATALOG_PARTITION_STAT_PREFIX,
                                 "stattype_locked > 0",
                                 share::schema::ObSchemaUtils::get_extract_tenant_id(
                                     share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id),
                                     tenant_id),
                                 param.table_identity_.catalog_id_))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.db_name_, raw_sql))) {
    LOG_WARN("failed to append db name", K(ret), K(param.table_identity_.db_name_));
  } else if (OB_FAIL(raw_sql.append(" and table_name = "))) {
    LOG_WARN("failed to append table name", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.tab_name_, raw_sql))) {
    LOG_WARN("failed to append table name", K(ret), K(param.table_identity_.tab_name_));
  } else if (has_part_name && OB_FAIL(raw_sql.append(" and partition_value = "))) {
    LOG_WARN("failed to append partition filter", K(ret), K(param.part_name_));
  } else if (has_part_name && OB_FAIL(sql_append_hex_escape_str(param.part_name_, raw_sql))) {
    LOG_WARN("failed to append partition value", K(ret), K(param.part_name_));
  } else if (OB_FAIL(raw_sql.append(";"))) {
    LOG_WARN("failed to append semicolon", K(ret));
  } else if (OB_FAIL(get_catalog_stat_locked_partition_values(raw_sql,
                                                              ctx,
                                                              param.table_identity_.tenant_id_,
                                                              allocator,
                                                              locked_partition_values,
                                                              dummy_array))) {
    LOG_WARN("failed to get stat locked partition values", K(ret));
  } else if (!locked_partition_values.empty()) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("catalog table statistics are locked", K(ret), K(locked_partition_values));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "catalog table statistics are locked");
  }
  return ret;
}

int ObDbmsCatalogStatsLockUnlock::fill_catalog_stat_locked(sql::ObExecContext &ctx,
                                                           ObCatalogTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSEArray<ObString, 4> locked_partition_values;
  ObSEArray<uint64_t, 4> stattype_locked_array;
  uint64_t tenant_id = param.table_identity_.tenant_id_;
  ObArenaAllocator allocator("ExtStatLock", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);

  if (OB_FAIL(raw_sql.append_fmt(GET_LOCKED_CATALOG_PARTITION_STAT_PREFIX,
                                 "stattype_locked > 0",
                                 share::schema::ObSchemaUtils::get_extract_tenant_id(
                                     share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id),
                                     tenant_id),
                                 param.table_identity_.catalog_id_))) {
    LOG_WARN("failed to append sql stmt", K(ret), K(raw_sql));
  } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.db_name_, raw_sql))) {
    LOG_WARN("failed to append db name", K(ret), K(param.table_identity_.db_name_));
  } else if (OB_FAIL(raw_sql.append(" and table_name = "))) {
    LOG_WARN("failed to append table name", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.tab_name_, raw_sql))) {
    LOG_WARN("failed to append table name", K(ret), K(param.table_identity_.tab_name_));
  } else if (OB_FAIL(raw_sql.append(";"))) {
    LOG_WARN("failed to append semicolon", K(ret));
  } else if (OB_FAIL(get_catalog_stat_locked_partition_values(raw_sql,
                                                              ctx,
                                                              param.table_identity_.tenant_id_,
                                                              allocator,
                                                              locked_partition_values,
                                                              stattype_locked_array))) {
    LOG_WARN("failed to get stat locked partition values", K(ret));
  } else if (locked_partition_values.empty()) { // no locked table
    /*do nothing*/
  } else {
    // Set part_stattype_ for each partition in partition_infos_ (aligned with internal table)
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      int64_t idx = -1;
      if (is_partition_value_locked(param.part_infos_.at(i).partition_,
                                    locked_partition_values,
                                    idx)) {
        if (OB_UNLIKELY(idx < 0 || idx >= stattype_locked_array.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(idx), K(stattype_locked_array));
        } else {
          param.part_infos_.at(i).part_stattype_ = stattype_locked_array.at(idx);
          if (param.part_infos_.at(i).partition_.empty()) {
            param.gather_options_.stattype_ =
                static_cast<StatTypeLocked>(stattype_locked_array.at(idx));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("catalog table failed to lock partitions", K(ret));
    } else {
      LOG_TRACE("catalog table has locked partitions",
                K(locked_partition_values),
                K(stattype_locked_array));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsLockUnlock::get_catalog_stat_locked_partition_values(
    const ObSqlString &raw_sql,
    sql::ObExecContext &ctx,
    uint64_t tenant_id,
    ObIAllocator &allocator,
    ObIArray<ObString> &partition_values,
    ObIArray<uint64_t> &stattype_locked_array)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = ctx.get_sql_proxy();
  if (OB_ISNULL(mysql_proxy) || OB_UNLIKELY(raw_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy), K(raw_sql));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result)
    {
      sqlclient::ObMySQLResult *client_result = NULL;
      const bool did_retry_weak = false;
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy, did_retry_weak);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        ObObj tmp;
        ObString partition_value;
        int64_t int_val = -1;
        int64_t idx1 = 0;
        int64_t idx2 = 1;
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          if (OB_FAIL(client_result->get_obj(idx1, tmp))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(tmp.get_varchar(partition_value))) {
            LOG_WARN("failed to get varchar", K(ret), K(tmp));
          } else {
            ObString stored_partition_value;
            if (OB_FAIL(ob_write_string(allocator, partition_value, stored_partition_value))) {
              LOG_WARN("failed to write string", K(ret));
            } else if (OB_FAIL(partition_values.push_back(stored_partition_value))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (OB_FAIL(client_result->get_obj(idx2, tmp))) {
              LOG_WARN("failed to get object", K(ret));
            } else if (OB_FAIL(tmp.get_int(int_val))) {
              LOG_WARN("failed to get int", K(ret), K(tmp));
            } else if (OB_FAIL(stattype_locked_array.push_back(int_val))) {
              LOG_WARN("failed to push back", K(ret));
            } else { /*do nothing*/
            }
          }
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
    LOG_TRACE("Succeed to get stat locked partition values",
              K(partition_values),
              K(raw_sql),
              K(stattype_locked_array));
  }
  return ret;
}

int ObDbmsCatalogStatsLockUnlock::gen_partition_value_list(const ObCatalogTableStatParam &param,
                                                           ObSqlString &partition_value_list,
                                                           ObIArray<ObString> &all_partition_values)
{
  int ret = OB_SUCCESS;
  ObString part_value;
  if (param.part_name_.empty()) {
    // Table-level operation: need to get all partition values from part_infos_
    // For non-partitioned table (PARTITION_LEVEL_ZERO), part_infos_ has one element with empty partition_
    // For partitioned table (PARTITION_LEVEL_ONE), part_infos_ has all partition values
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      part_value = param.part_infos_.at(i).partition_;
      if (OB_FAIL(all_partition_values.push_back(part_value))) {
        LOG_WARN("failed to push back", K(ret), K(param.part_infos_.count()));
      }
    }
  } else {
    // Partition-level operation: use specified partition name as partition_value
    part_value = param.part_name_;
    if (OB_FAIL(all_partition_values.push_back(part_value))) {
      LOG_WARN("failed to push back", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_partition_values.count(); i++) {
      if (i == 0) {
        if (OB_FAIL(partition_value_list.append("("))) {
          LOG_WARN("failed to append open paren", K(ret));
        }
      } else {
        if (OB_FAIL(partition_value_list.append(", "))) {
          LOG_WARN("failed to append comma", K(ret));
        }
      }
      if (OB_SUCC(ret)
          && OB_FAIL(sql_append_hex_escape_str(all_partition_values.at(i), partition_value_list))) {
        LOG_WARN("failed to append partition value", K(ret));
      }
      if (OB_SUCC(ret) && i == all_partition_values.count() - 1) {
        if (OB_FAIL(partition_value_list.append(")"))) {
          LOG_WARN("failed to append close paren", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsLockUnlock::get_catalog_no_stats_partition_values(
    const StatTypeLocked stattype,
    const ObIArray<ObString> &all_partition_values,
    const ObIArray<ObString> &stat_partition_values,
    const ObIArray<uint64_t> &stattype_locked,
    ObIArray<ObString> &no_stats_partition_values,
    ObIArray<uint64_t> &part_stattypes,
    bool &need_update_lock)
{
  int ret = OB_SUCCESS;
  need_update_lock = false;
  if (OB_UNLIKELY(stat_partition_values.count() != stattype_locked.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error",
             K(ret),
             K(stat_partition_values.count()),
             K(stattype_locked.count()));
  } else {
    // Check if any partition needs locking (align with internal table implementation)
    for (int64_t i = 0; OB_SUCC(ret) && i < all_partition_values.count(); ++i) {
      bool is_record = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_record && j < stat_partition_values.count(); ++j) {
        if (all_partition_values.at(i) == stat_partition_values.at(j)) {
          is_record = true;
          // If this partition is not locked with the target stattype, need to update
          if (!(stattype & stattype_locked.at(j))) {
            need_update_lock = true;
          }
          // If already locked, don't reset need_update_lock as other partitions might need updating
        }
      }
      // If partition doesn't exist in stats table, need to insert it
      if (!is_record) {
        if (OB_FAIL(no_stats_partition_values.push_back(all_partition_values.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(part_stattypes.push_back(stattype))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          need_update_lock = true;
        }
      }
    }
    if (OB_SUCC(ret) && !need_update_lock && !stat_partition_values.empty()) {
      // Check if any existing partition has different stattype_locked value
      for (int64_t j = 0; OB_SUCC(ret) && j < stat_partition_values.count(); ++j) {
        if (!(stattype & stattype_locked.at(j))) {
          need_update_lock = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStatsLockUnlock::get_catalog_insert_locked_type_sql(
    const ObCatalogTableStatParam &param,
    const ObIArray<ObString> &no_stats_partition_values,
    const ObIArray<uint64_t> &part_stattypes,
    ObSqlString &insert_sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(no_stats_partition_values.count() != part_stattypes.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error",
             K(ret),
             K(no_stats_partition_values.count()),
             K(part_stattypes.count()));
  } else if (no_stats_partition_values.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(insert_sql.append(INSERT_CATALOG_TABLE_STAT_SQL))) {
    LOG_WARN("failed to append", K(ret));
  } else {
    // For catalog external tables, last_analyzed should be 0 for partitions without stats
    // (indicating never analyzed). The actual last_analyzed will be set from file system
    // modification time when statistics are gathered via fill_file_and_data_stats
    int64_t last_analyzed = 0;
    int64_t last_modified = ObTimeUtility::current_time();
    const uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(
        share::schema::ObSchemaUtils::get_exec_tenant_id(param.table_identity_.tenant_id_),
        param.table_identity_.tenant_id_);
    for (int64_t i = 0; OB_SUCC(ret) && i < no_stats_partition_values.count(); ++i) {
      const char suffix = (i == no_stats_partition_values.count() - 1 ? ';' : ',');
      if (OB_FAIL(insert_sql.append_fmt("(%lu, %lu, ",
                                        ext_tenant_id,
                                        param.table_identity_.catalog_id_))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.db_name_, insert_sql))) {
        LOG_WARN("failed to append db name", K(ret), K(param.table_identity_.db_name_));
      } else if (OB_FAIL(insert_sql.append(", "))) {
        LOG_WARN("failed to append comma", K(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(param.table_identity_.tab_name_, insert_sql))) {
        LOG_WARN("failed to append table name", K(ret), K(param.table_identity_.tab_name_));
      } else if (OB_FAIL(insert_sql.append(", "))) {
        LOG_WARN("failed to append comma", K(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(no_stats_partition_values.at(i), insert_sql))) {
        LOG_WARN("failed to append partition value", K(ret));
      } else if (OB_FAIL(insert_sql.append_fmt(", 0, %ld, usec_to_time(%ld), 0, 0, 0, 0, %lu, 0)%c",
                                               last_analyzed,
                                               last_modified,
                                               part_stattypes.at(i),
                                               suffix))) {
        LOG_WARN("failed to append fmt", K(ret));
      }
    }
  }
  return ret;
}

bool ObDbmsCatalogStatsLockUnlock::is_partition_value_locked(
    const ObString &partition_value,
    const ObIArray<ObString> &locked_partition_values,
    int64_t &idx)
{
  bool find_it = false;
  for (int64_t i = 0; !find_it && i < locked_partition_values.count(); ++i) {
    if (locked_partition_values.at(i) == partition_value) {
      find_it = true;
      idx = i;
    } else { /*do nothing*/
    }
  }
  return find_it;
}

} // namespace common
} // namespace oceanbase

#undef UPDATE_CATALOG_STAT_STATTYPE_LOCKED_PREFIX
#undef INSERT_CATALOG_TABLE_STAT_SQL
#undef GET_LOCKED_CATALOG_PARTITION_STAT_PREFIX