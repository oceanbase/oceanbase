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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_table_sql_service.h"
#include "share/ob_global_stat_proxy.h"
#include "share/schema/ob_partition_sql_helper.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "src/share/vector_index/ob_vector_index_util.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/storage_cache_policy/ob_storage_cache_partition_sql_helper.h"
#include "share/ob_mview_args.h"

namespace oceanbase
{
using namespace common;
namespace rootserver
{
class ObRootService;
}
namespace share
{
namespace schema
{

int ObTableSqlService::exec_update(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const char *table_name,
    ObDMLSqlSplicer &dml,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (is_core_table(table_id)) {
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    ObCoreTableProxy kv(table_name, sql_client, tenant_id);
    if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("failed to load kv for update", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("splice core cells failed", K(ret));
    } else if (OB_FAIL(kv.update_row(cells, affected_rows))) {
      LOG_WARN("failed to update row", K(ret));
    }
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_update(table_name, dml, affected_rows))) {
      LOG_WARN("execute update failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::exec_insert(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const char *table_name,
    ObDMLSqlSplicer &dml,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (is_core_table(table_id)) {
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    ObCoreTableProxy kv(table_name, sql_client, tenant_id);
    if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("failed to load kv for insert", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("splice core cells failed", K(ret));
    } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
      LOG_WARN("failed to replace row", K(ret));
    }
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_insert(table_name, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::exec_delete(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const char *table_name,
    ObDMLSqlSplicer &dml,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (is_core_table(table_id)) {
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    ObCoreTableProxy kv(table_name, sql_client, tenant_id);
    if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("failed to load kv for delete", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("splice core cells failed", K(ret));
    } else if (OB_FAIL(kv.delete_row(cells, affected_rows))) {
      LOG_WARN("failed to delete row", K(ret));
    }
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_delete(table_name, dml, affected_rows))) {
      LOG_WARN("execute delete failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::exec_dml(common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const char *table_name,
    const ObDMLSqlSplicer &dml,
    const int64_t target_affected_row_count,
    const bool insert_ignore)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_name));
  } else if (dml.empty()) {
    if (target_affected_row_count > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml is empty", KR(ret), K(target_affected_row_count));
    }
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_batch_insert(table_name, dml, affected_rows,
            insert_ignore ? "INSERT IGNORE" : "INSERT"))) {
      LOG_WARN("failed to exec batch insert", KR(ret), K(table_name), K(insert_ignore));
    } else if (target_affected_row_count >= 0 && affected_rows != target_affected_row_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected rows not match", KR(ret), K(target_affected_row_count), K(affected_rows));
    }
  }
  return ret;
}

int ObTableSqlService::delete_table_part_info(const ObTableSchema &table_schema,
                                              const int64_t new_schema_version,
                                              ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  /*
   * Because __all_part(_history)/__all_sub_part(_history)/__all_def_sub_part(_history) is not core table,
   * to avoid cyclic dependence while refresh schema, all inner tables won't record any partition related schema in tables.
   * As compensation, schema module mock inner table's partition schema while refresh schema.
   * So far, we only support to define hash-like inner table.
   */
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else if (!is_inner_table(table_schema.get_table_id())) {
    if (table_schema.get_part_level() > 0 &&
        !table_schema.is_vir_table() &&
        !table_schema.is_view_table()) {
      int64_t tenant_id = table_schema.get_tenant_id();
      bool is_two_level = PARTITION_LEVEL_TWO == table_schema.get_part_level() ? true : false;
      const char *tname[] = {OB_ALL_PART_INFO_TNAME, OB_ALL_PART_TNAME, OB_ALL_SUB_PART_TNAME,
                             OB_ALL_DEF_SUB_PART_TNAME};
      ObSqlString sql;
      int64_t affected_rows = 0;
      //drop data in __all_part_info, __all_part, __all_subpart, __all_def_subpart,
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
        if (!is_two_level && (0 == STRCMP(tname[i], OB_ALL_SUB_PART_TNAME) ||
              0 == STRCMP(tname[i], OB_ALL_DEF_SUB_PART_TNAME))) {
          continue;
        }
        if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu",
                                   tname[i],
                                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schema.get_table_id())))) {
          LOG_WARN("append_fmt failed", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
        } else {}
      }
      if (OB_SUCC(ret)) {
        ObTableSchema new_table;
        if (OB_FAIL(new_table.assign(table_schema))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else {
          new_table.set_schema_version(new_schema_version);
          const ObPartitionSchema *new_table_schema = &new_table;
          ObDropPartInfoHelper part_helper(sql_client, new_table.get_tenant_id());
          if (OB_FAIL(part_helper.init(new_table_schema))) {
            LOG_WARN("failed to init part_helper", KR(ret), KPC(new_table_schema));
          } else if (OB_FAIL(part_helper.delete_partition_info())) {
            LOG_WARN("delete partition info failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::drop_inc_partition_add_extra_str(const ObTableSchema &inc_table,
                                                        ObSqlString &sql,
                                                        ObSqlString &condition_str,
                                                        ObSqlString &dml_info_cond_str)
{
  int ret = OB_SUCCESS;
  ObPartition **part_array = inc_table.get_part_array();
  const int64_t inc_part_num = inc_table.get_partition_num();
  if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition array is null", KR(ret), K(inc_table));
  } else if (OB_FAIL(condition_str.assign_fmt(" (0 = 1"))) {
    LOG_WARN("assign sql str fail", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(" AND (0 = 1"))) {
    LOG_WARN("append sql str fail", KR(ret));
  } else if (OB_FAIL(dml_info_cond_str.append_fmt(" (0 = 1"))) {
    LOG_WARN("append sql str fail", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
    ObPartition *part = part_array[i];
    if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
    } else if (OB_FAIL(sql.append_fmt(" OR part_id = %lu", part->get_part_id()))) {
      LOG_WARN("append_fmt failed", KR(ret));
    } else if (OB_FAIL(condition_str.append_fmt(" OR partition_id = %lu",
                                                part->get_part_id()))) {
      LOG_WARN("fail to append fmt", KR(ret));
    } else if (inc_table.get_part_level() == PARTITION_LEVEL_ONE &&
               OB_FAIL(dml_info_cond_str.append_fmt(" OR tablet_id = %lu",
                                                      part->get_tablet_id().id()))) {
      LOG_WARN("fail to append fmt", K(ret));
    } else {
      // get subpartition info
      for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
        ObSubPartition *subpart = part->get_subpart_array()[j];
        if (OB_ISNULL(subpart)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpartition is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
        } else if (OB_FAIL(condition_str.append_fmt(" OR partition_id = %lu",
                                                    subpart->get_sub_part_id()))) {
          LOG_WARN("append_fmt failed", KR(ret));
        } else if (OB_FAIL(dml_info_cond_str.append_fmt(" OR tablet_id = %lu",
                                                          subpart->get_tablet_id().id()))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(condition_str.append_fmt(" )"))) {
      LOG_WARN("append_fmt failed", KR(ret));
    } else if (OB_FAIL(sql.append_fmt(" )"))) {
      LOG_WARN("append_fmt failed", KR(ret));
    } else if (OB_FAIL(dml_info_cond_str.append_fmt(" )"))) {
      LOG_WARN("failed to append fmt", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::drop_inc_partition(common::ObISQLClient &sql_client,
                                           const ObTableSchema &ori_table,
                                           const ObTableSchema &inc_table,
                                           bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ori_table.get_tenant_id();
  const uint64_t table_id = ori_table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (!is_inner_table(ori_table.get_table_id())
      && 0 < ori_table.get_part_level()
      && !ori_table.is_vir_table()
      && !ori_table.is_view_table()) {
    int64_t tenant_id = ori_table.get_tenant_id();
    ObSqlString sql;
    // used to sync partition level info.
    ObSqlString condition_str;
    // used to sync partition dml info.
    ObSqlString dml_info_cond_str;
    const int64_t inc_part_num = inc_table.get_partition_num();
    ObPartition **part_array = inc_table.get_part_array();

    // build sql string
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", K(ret), K(inc_table));
    } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu",
                                      OB_ALL_PART_TNAME,
                                      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (!is_truncate_table) {
      if (OB_FAIL(drop_inc_partition_add_extra_str(inc_table, sql, condition_str, dml_info_cond_str))) {
        LOG_WARN("fail to add extra str when drop inc partition", K(ret), K(inc_table));
      }
    }
    // delete stat info here.
    if (OB_SUCC(ret)) {
      ObSqlString *extra_str = condition_str.empty() ? NULL : &condition_str;
      ObSqlString *extra_str2 = dml_info_cond_str.empty() ? NULL : &dml_info_cond_str;
      if (OB_FAIL(delete_from_all_table_stat(sql_client, tenant_id, table_id, extra_str))) {
        LOG_WARN("delete from all table stat failed", K(ret));
      } else if (OB_FAIL(delete_from_all_column_stat(sql_client, tenant_id, table_id, extra_str))) {
        LOG_WARN("failed to delete all column stat", K(table_id),
                "column count", ori_table.get_column_count(), K(ret));
      } else if (OB_FAIL(delete_from_all_histogram_stat(sql_client, tenant_id, table_id, extra_str))) {
        LOG_WARN("failed to delete all histogram_stat", K(table_id), K(ret));
      } else if (OB_FAIL(delete_from_all_monitor_modified(sql_client, tenant_id, table_id, extra_str2))) {
        LOG_WARN("failed to delete from all monitor modified", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
      } else if (affected_rows != inc_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(inc_part_num), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObTableSqlService::drop_inc_sub_partition(common::ObISQLClient &sql_client,
                                              const ObTableSchema &ori_table,
                                              const ObTableSchema &inc_table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ori_table.get_tenant_id();
  const uint64_t table_id = ori_table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (!ori_table.has_tablet()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has not tablet", KR(ret));
  } else {
    ObSqlString sql;
    ObSqlString condition_str;
    ObSqlString dml_info_cond_str;
    const int64_t inc_part_num = inc_table.get_partition_num();
    int64_t inc_subpart_num = 0;
    ObPartition **part_array = inc_table.get_part_array();

    // build sql string
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", K(ret), K(inc_table));
    } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu AND (0 = 1",
                                      OB_ALL_SUB_PART_TNAME,
                                      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(condition_str.assign_fmt("( 1 = 0"))) {
      LOG_WARN("assign fmt failed", K(ret));
    } else if (OB_FAIL(dml_info_cond_str.assign_fmt("( 1 = 0"))) {
      LOG_WARN("assign fmt failed", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      ObPartition *part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret), K(i), K(inc_part_num), K(inc_table));
      } else if (OB_ISNULL(part->get_subpart_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartitions is null", K(ret), K(i), K(inc_part_num), K(inc_table));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
          inc_subpart_num++;
          ObSubPartition *subpart = part->get_subpart_array()[j];
          if (OB_ISNULL(subpart)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpartition is null", K(ret), K(i), K(inc_part_num), K(inc_table));
          } else if (OB_FAIL(sql.append_fmt(" OR (part_id = %lu AND sub_part_id = %lu)",
                     subpart->get_part_id(), subpart->get_sub_part_id()))) {
            LOG_WARN("append_fmt failed", K(ret));
          } else if (OB_FAIL(condition_str.append_fmt(" OR partition_id = %lu", subpart->get_sub_part_id()))) {
            LOG_WARN("append_fmt failed", K(ret));
          } else if (OB_FAIL(dml_info_cond_str.append_fmt(" OR tablet_id = %lu", subpart->get_tablet_id().id()))) {
            LOG_WARN("append_fmt failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(" )"))) {
        LOG_WARN("append_fmt failed", K(ret));
      } else if (OB_FAIL(condition_str.append_fmt(" )"))) {
        LOG_WARN("append_fmt failed", K(ret));
      } else if (OB_FAIL(dml_info_cond_str.append_fmt(" )"))) {
        LOG_WARN("append_fmt failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(delete_from_all_table_stat(sql_client, tenant_id, table_id, &condition_str))) {
        LOG_WARN("delete from all table stat failed", K(ret));
      } else if (OB_FAIL(delete_from_all_column_stat(sql_client, tenant_id, table_id, &condition_str))) {
        LOG_WARN("failed to delete all column stat", K(table_id),
                "column count", ori_table.get_column_count(), K(ret));
      } else if (OB_FAIL(delete_from_all_histogram_stat(sql_client, tenant_id, table_id, &condition_str))) {
        LOG_WARN("failed to delete all histogram_stat", K(table_id), K(ret));
      } else if (OB_FAIL(delete_from_all_monitor_modified(sql_client, tenant_id, table_id, &dml_info_cond_str))) {
        LOG_WARN("failed to delete from all monitor modified", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
      } else if (affected_rows != inc_subpart_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(inc_subpart_num), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObTableSqlService::drop_inc_all_sub_partition_add_extra_str(const ObTableSchema &inc_table,
                                                                ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObPartition **part_array = inc_table.get_part_array();
  const int64_t inc_part_num = inc_table.get_partition_num();
  if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition array is null", KR(ret), K(inc_table));
  } else if (OB_FAIL(sql.append(" AND (0 = 1"))) {
    LOG_WARN("append failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
    ObPartition *part = part_array[i];
    if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
    } else if (OB_FAIL(sql.append_fmt(" OR (part_id = %lu)", part->get_part_id()))) {
      LOG_WARN("append_fmt failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append_fmt(" )"))) {
      LOG_WARN("append_fmt failed", KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::drop_inc_all_sub_partition(common::ObISQLClient &sql_client,
                                                  const ObTableSchema &ori_table,
                                                  const ObTableSchema &inc_table,
                                                  bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ori_table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (!ori_table.has_tablet()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has not tablet", KR(ret));
  } else {
    int64_t tenant_id = ori_table.get_tenant_id();
    ObSqlString sql;
    const int64_t inc_part_num = inc_table.get_partition_num();
    ObPartition **part_array = inc_table.get_part_array();

    // build sql string
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", K(ret), K(inc_table));
    } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu",
                                      OB_ALL_SUB_PART_TNAME,
                                      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, ori_table.get_table_id())))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (!is_truncate_table) {
      if (OB_FAIL(drop_inc_all_sub_partition_add_extra_str(inc_table, sql))) {
        LOG_WARN("fail to drop inc all sub_partition add extra str", K(ret), K(inc_table));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
      }
    }
  }
  return ret;
}

int ObTableSqlService::rename_inc_part_info(
    ObISQLClient &sql_client,
    const ObTableSchema &table_schema,
    const ObTableSchema &inc_table_schema,
    const int64_t new_schema_version,
    const bool update_part_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowed failed", KR(ret), K(table_schema));
  } else if (!table_schema.is_user_table() && !table_schema.is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(table_schema.is_external_table()),K(table_schema));
  } else {
    const ObPartitionSchema *table_schema_ptr = &table_schema;
    const ObPartitionSchema *inc_table_schema_ptr = &inc_table_schema;
    ObRenameIncPartHelper rename_part_helper(table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client);
    if (OB_FAIL(rename_part_helper.rename_partition_info(update_part_idx))) {
      LOG_WARN("fail to rename partition", KR(ret), KPC(table_schema_ptr), KPC(inc_table_schema_ptr));
    }
  }
  return ret;
}

int ObTableSqlService::rename_inc_subpart_info(
    ObISQLClient &sql_client,
    const ObTableSchema &table_schema,
    const ObTableSchema &inc_table_schema,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowed failed", KR(ret), K(table_schema));
  } else if (!table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(table_schema));
  } else {
    const ObPartitionSchema *table_schema_ptr = &table_schema;
    const ObPartitionSchema *inc_table_schema_ptr = &inc_table_schema;
    ObRenameIncSubpartHelper rename_subpart_helper(table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client);
    if (OB_FAIL(rename_subpart_helper.rename_subpartition_info())) {
      LOG_WARN("fail to rename partition", KR(ret), KPC(table_schema_ptr), KPC(inc_table_schema_ptr));
    }
  }
  return ret;
}

int ObTableSqlService::alter_inc_part_policy(
    ObISQLClient &sql_client,
    const ObTableSchema &table_schema,
    const ObTableSchema &inc_table_schema,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("alter inc part policy", K(table_schema), K(inc_table_schema), K(new_schema_version));
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowed failed", KR(ret), K(table_schema));
  } else if (!table_schema.is_user_table() || table_schema.is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(table_schema.is_external_table()),K(table_schema));
  } else {
    FLOG_INFO("alter inc part policy progress", K(table_schema), K(inc_table_schema), K(new_schema_version));
    const ObPartitionSchema *table_schema_ptr = &table_schema;
    const ObPartitionSchema *inc_table_schema_ptr = &inc_table_schema;
    ObAlterIncPartPolicyHelper alter_part_policy_helper(table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client);
    if (OB_FAIL(alter_part_policy_helper.alter_partition_policy())) {
      LOG_WARN("fail to alter partition policy", KR(ret), KPC(table_schema_ptr), KPC(inc_table_schema_ptr));
    }
  }
  FLOG_INFO("alter inc part policy success", K(table_schema), K(inc_table_schema), K(new_schema_version));
  return ret;
}

int ObTableSqlService::alter_inc_subpart_policy(
    ObISQLClient &sql_client,
    const ObTableSchema &table_schema,
    const ObTableSchema &inc_table_schema,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("alter inc subpart policy", K(table_schema), K(inc_table_schema), K(new_schema_version));
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowed failed", KR(ret), K(table_schema));
  } else if (!table_schema.is_user_table() && !table_schema.is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(table_schema.is_external_table()),K(table_schema));
  } else {
    FLOG_INFO("alter inc subpart policy progress", K(table_schema), K(inc_table_schema), K(new_schema_version));
    const ObPartitionSchema *table_schema_ptr = &table_schema;
    const ObPartitionSchema *inc_table_schema_ptr = &inc_table_schema;
    ObAlterIncSubpartPolicyHelper alter_subpart_policy_helper(table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client);
    if (OB_FAIL(alter_subpart_policy_helper.alter_subpartition_policy())) {
      LOG_WARN("fail to alter subpartition policy", KR(ret), KPC(table_schema_ptr), KPC(inc_table_schema_ptr));
    }
  }
  FLOG_INFO("alter inc subpart policy success", K(table_schema), K(inc_table_schema), K(new_schema_version));
  return ret;
}

int ObTableSqlService::drop_inc_part_info(
    ObISQLClient &sql_client,
    const ObTableSchema &table_schema,
    const ObTableSchema &inc_table_schema,
    const int64_t new_schema_version,
    bool is_truncate_partition,
    bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else if (!is_inner_table(table_schema.get_table_id())
      && table_schema.get_part_level() > 0
      && !table_schema.is_vir_table()
      && !table_schema.is_view_table()) {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(drop_inc_partition(sql_client, table_schema, inc_table_schema, is_truncate_table))) {
      // remove from __all_part
      LOG_WARN("failed to drop partition", K(ret), K(inc_table_schema));
    } else if (!table_schema.is_external_table() && OB_FAIL(drop_inc_all_sub_partition(sql_client, table_schema, inc_table_schema, is_truncate_table))) {
      // remove from __all_sub_part
      LOG_WARN("failed to drop subpartition", K(ret), K(inc_table_schema));
    } else {
      const ObPartitionSchema *table_schema_ptr = &table_schema;
      const ObPartitionSchema *inc_table_schema_ptr = &inc_table_schema;
      ObDropIncPartHelper drop_part_helper(table_schema_ptr, inc_table_schema_ptr,
                                           new_schema_version, sql_client);
      if (OB_SUCC(ret) && OB_FAIL(drop_part_helper.drop_partition_info())) {
        LOG_WARN("drop increment partition info failed", K(table_schema),
                 KPC(inc_table_schema_ptr), K(new_schema_version), K(ret));
      } else if (!(is_truncate_partition || is_truncate_table)
                && OB_INVALID_ID == table_schema.get_tablegroup_id()) {
        /*
         * While table is in tablegroup, we only support to modify partition schema by tablegroup.
         * For tablegroup's partition modification, we only record tablegroup's ddl operation.
         * When refresh tablegroup's schema, we also refresh tables schema in tablegroup
         * if related tablegroup once modify partition schema.
         */
        ObSchemaOperation opt;
        opt.tenant_id_ = table_schema.get_tenant_id();
        opt.database_id_ = table_schema.get_database_id();
        opt.tablegroup_id_ = table_schema.get_tablegroup_id();
        opt.table_id_ = table_schema.get_table_id();
        opt.op_type_ = OB_DDL_DROP_PARTITION;
        opt.schema_version_ = new_schema_version;
        if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
          LOG_WARN("log operation failed", K(opt), K(ret));
        }
      }
    }
  }
  return ret;
}

/*
 * drop subpartition in non-template secondary partition table
 * [@input]:
 * 1. inc_table_schema: only following members are useful.
 *    - partition_num_ : first partition num
 *      (Truncate subpartitions in different first partitions in one ddl stmt is supported.)
 *    - partition_array_ : related first partition schema array
 * 2. For each partition in partition_array_: only following members are useful.
 *    - part_id_ : physical first partition_id
 *    - subpartition_num_ : (to be deleted) secondary partition num in first partition
 *    - subpartition_array_ : related secondary partition schema array
 * 3. For each subpartition in subpartition_array_: only following members are useful.
 *    - subpart_id_ : physical secondary partition_id
 */
int ObTableSqlService::drop_inc_subpart_info(
    ObISQLClient &sql_client,
    const ObTableSchema &table_schema,
    const ObTableSchema &inc_table_schema,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else if (!table_schema.has_tablet()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has not tablet", KR(ret));
  // remove from __all_sub_part
  } else if (OB_FAIL(drop_inc_sub_partition(sql_client, table_schema, inc_table_schema))) {
    LOG_WARN("failed to drop subpartition", KR(ret), K(inc_table_schema));
  } else {
    const ObPartitionSchema *table_schema_ptr = &table_schema;
    const ObPartitionSchema *inc_table_schema_ptr = &inc_table_schema;
    ObDropIncSubPartHelper drop_subpart_helper(table_schema_ptr, inc_table_schema_ptr,
                                               new_schema_version, sql_client);
    if (OB_FAIL(drop_subpart_helper.drop_subpartition_info())) {
      LOG_WARN("drop increment partition info failed", K(table_schema),
               KPC(inc_table_schema_ptr), K(new_schema_version), KR(ret));
    }
  }
  return ret;
}

// truncate first partition of partitioned table
int ObTableSqlService::truncate_part_info(
    common::ObISQLClient &sql_client,
    const ObTableSchema &ori_table,
    ObTableSchema &inc_table,
    ObTableSchema &del_table,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  bool is_truncate_table = false;
  bool is_truncate_partition = true;
  // drop first partitions
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (OB_FAIL(drop_inc_part_info(sql_client,
                                 ori_table,
                                 del_table,
                                 schema_version,
                                 is_truncate_partition,
                                 is_truncate_table))) {
    LOG_WARN("delete inc part info failed", KR(ret));
  } else if (OB_FAIL(add_inc_partition_info(sql_client,
                                            ori_table,
                                            inc_table,
                                            schema_version,
                                            true,
                                            false))) {
    LOG_WARN("add inc part info failed", KR(ret));
  } else {
    ObSchemaOperation opt;
    opt.tenant_id_ = ori_table.get_tenant_id();
    opt.database_id_ = ori_table.get_database_id();
    opt.tablegroup_id_ = ori_table.get_tablegroup_id();
    opt.table_id_ = ori_table.get_table_id();
    opt.op_type_ = OB_DDL_TRUNCATE_PARTITION;
    opt.schema_version_ = schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::truncate_subpart_info(
    common::ObISQLClient &sql_client,
    const ObTableSchema &ori_table,
    ObTableSchema &inc_table,
    ObTableSchema &del_table,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const ObPartition *part = NULL;
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (OB_FAIL(drop_inc_subpart_info(sql_client, ori_table, del_table,
                                           schema_version))) {
    LOG_WARN("failed to drop partition", KR(ret), K(del_table));
  } else if (OB_FAIL(add_inc_partition_info(sql_client,
                                            ori_table,
                                            inc_table,
                                            schema_version,
                                            true,
                                            true))) {
    LOG_WARN("add partition info failed", KR(ret));
  } else {
    ObSchemaOperation opt;
    opt.tenant_id_ = ori_table.get_tenant_id();
    opt.database_id_ = ori_table.get_database_id();
    opt.tablegroup_id_ = ori_table.get_tablegroup_id();
    opt.table_id_ = ori_table.get_table_id();
    opt.op_type_ = OB_DDL_TRUNCATE_SUB_PARTITION;

    opt.schema_version_ = schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::exchange_part_info(
    common::ObISQLClient &sql_client,
    const ObTableSchema &ori_table,
    ObTableSchema &inc_table,
    ObTableSchema &del_table,
    const int64_t drop_schema_version,
    const int64_t add_schema_version)
{
  int ret = OB_SUCCESS;
  bool is_truncate_table = false;
  bool is_truncate_partition = true;
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (OB_FAIL(drop_inc_part_info(sql_client,
                                        ori_table,
                                        del_table,
                                        drop_schema_version,
                                        is_truncate_partition,
                                        is_truncate_table))) {
    LOG_WARN("delete inc part info failed", K(ret));
  } else if (OB_FAIL(add_inc_partition_info(sql_client,
                                            ori_table,
                                            inc_table,
                                            add_schema_version,
                                            true/*is_truncate_table*/,
                                            false/*is_subpart*/))) {
    LOG_WARN("add inc part info failed", K(ret));
  }
  return ret;
}

int ObTableSqlService::exchange_subpart_info(
    common::ObISQLClient &sql_client,
    const ObTableSchema &ori_table,
    ObTableSchema &inc_table,
    ObTableSchema &del_table,
    const int64_t drop_schema_version,
    const int64_t add_schema_version,
    const bool is_subpart_idx_specified)
{
  int ret = OB_SUCCESS;
  const ObPartition *part = NULL;
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (OB_FAIL(drop_inc_subpart_info(sql_client, ori_table, del_table,
                                           drop_schema_version))) {
    LOG_WARN("failed to drop partition", K(ret), K(del_table));
  } else if (OB_FAIL(add_inc_partition_info(sql_client,
                                            ori_table,
                                            inc_table,
                                            add_schema_version,
                                            true/*is_truncate_table*/,
                                            true/*is_subpart*/,
                                            is_subpart_idx_specified))) {
    LOG_WARN("add partition info failed", K(ret));
  }
  return ret;
}

int ObTableSqlService::drop_table(const ObTableSchema &table_schema,
                                  const int64_t new_schema_version,
                                  ObISQLClient &sql_client,
                                  const ObString *ddl_stmt_str/*=NULL*/,
                                  bool is_truncate_table /*false*/,
                                  bool is_drop_db /*false*/,
                                  bool is_force_drop_lonely_lob_aux_table /*false*/,
                                  ObSchemaGetterGuard *schema_guard /*=NULL*/,
                                  DropTableIdHashSet *drop_table_set/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else {
    // delete from __all_table_history
    if (OB_FAIL(delete_from_all_table_history(
                sql_client, table_schema, new_schema_version))) {
      LOG_WARN("delete_from_all_table_history failed", K(table_schema), K(ret));
    }
  }
  bool need_drop_column = (!table_schema.is_view_table() || table_schema.view_column_filled());
  // delete from __all_column_history
  if (OB_FAIL(ret)) {
  } else if (need_drop_column && OB_FAIL(delete_from_all_column_history(
      sql_client, table_schema, new_schema_version))) {
    LOG_WARN("delete_from_column_history_table failed", K(table_schema), K(ret));
  }
  // delete from __all_table and __all_column
  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_from_all_table(sql_client, tenant_id, table_id))) {
      LOG_WARN("delete from all table failed", K(ret));
    } else if (OB_FAIL(delete_from_all_column(
                       sql_client, tenant_id, table_id,
                       table_schema.get_column_count(),
                       table_schema.get_table_type() != MATERIALIZED_VIEW))) {
      LOG_WARN("failed to delete columns", K(table_id),
               "column count", table_schema.get_column_count(), K(ret));
    }
  }

  // delete from __all_table_stat, __all_monitor_modified, __all_column_usage, __all_optstat_user_prefs
  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_from_all_table_stat(sql_client, tenant_id, table_id))) {
      LOG_WARN("delete from all table stat failed", K(ret));
    //column stat and histogram stat will be delete asynchronously by optimizer auto task, not delete here, avoid cost too much time.
    } else if (OB_FAIL(delete_from_all_column_usage(sql_client, tenant_id, table_id))) {
      LOG_WARN("failed to delete from all column usage", K(ret));
    } else if (OB_FAIL(delete_from_all_monitor_modified(sql_client, tenant_id, table_id))) {
      LOG_WARN("failed to delete from all monitor modified", K(ret));
    } else if (OB_FAIL(delete_from_all_optstat_user_prefs(sql_client, tenant_id, table_id))) {
      LOG_WARN("failed to delete all optstat user prefs", K(ret));
    }
  }

  //delete from __all_temp_table if it is a temporary table or ctas temporary table
  if (OB_SUCC(ret) && (table_schema.is_ctas_tmp_table() || table_schema.is_tmp_table())) {
    if (OB_FAIL(delete_from_all_temp_table(sql_client, tenant_id, table_id))) {
      LOG_WARN("delete from all temp table failed", K(ret));
    }
  }

  // delete from __all_part_info, __all_part and __all_sub_part
  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_table_part_info(table_schema, new_schema_version, sql_client))) {
      LOG_WARN("delete partition info failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // For oracle compatibility, foreign key should be dropped while drop table.
    if (OB_FAIL(delete_foreign_key(sql_client, table_schema, new_schema_version, is_truncate_table))) {
      LOG_WARN("failed to delete foreign key", K(ret));
    }
  }

  // delete constraint here
  if (OB_SUCC(ret)) {
    // Drop all constraints while drop table and recyclebin is off.
    if (OB_FAIL(delete_constraint(sql_client, table_schema, new_schema_version))) {
      LOG_WARN("failed to delete constraint", K(ret));
    }
  }

  // delete column group
  if (OB_SUCC(ret)) {
    if (!table_schema.is_column_store_supported()) {
    } else if (OB_FAIL(delete_column_group(sql_client, table_schema, new_schema_version))) {
      LOG_WARN("fail to delete column group", K(ret));
    }
  }

  // log operations
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = table_schema.get_database_id();
    opt.tablegroup_id_ = table_schema.get_tablegroup_id();
    opt.table_id_ = table_schema.get_table_id();
    if (is_truncate_table) {
      opt.op_type_ = OB_DDL_TRUNCATE_TABLE_DROP;
    } else {
      if (table_schema.is_index_table()) {
        opt.op_type_ = table_schema.is_global_index_table() ? OB_DDL_DROP_GLOBAL_INDEX : OB_DDL_DROP_INDEX;
      } else if (table_schema.is_view_table()){
        opt.op_type_ = OB_DDL_DROP_VIEW;
      } else {
        opt.op_type_ = OB_DDL_DROP_TABLE;
      }
    }
    opt.schema_version_ = new_schema_version;
    opt.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    } else {
      if (is_force_drop_lonely_lob_aux_table) {
        // Due to some bugs, the lob aux table was not deleted along with the main table。
        // which would affect some features, such as load balancing.
        // Therefore, a way needs to be provided to force the deletion of the lob aux table.
        // However, since the main table corresponding to the lob aux table has already been deleted
        // the schema_version of the main table cannot be updated. Thus, this step needs to be skipped.
        // But for safety, we try to update it, but the expectation was to fail.
        if (table_schema.is_aux_lob_table()) {
          ret = update_data_table_schema_version(sql_client, tenant_id, table_schema.get_data_table_id(), table_schema.get_in_offline_ddl_white_list());
          if (OB_TABLE_NOT_EXIST != ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("is_force_drop_lonely_lob_aux_table is true, but update_data_table_schema_version success ", K(table_schema));
          } else {
            ret = OB_SUCCESS;
            LOG_ERROR("is_force_drop_lonely_lob_aux_table, skip update data table schema version", K(table_schema));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("is_force_drop_lonely_lob_aux_table is true, but not drop lob aux table", K(table_schema));
        }
      } else if (table_schema.is_index_table()
          || table_schema.is_aux_vp_table()
          || table_schema.is_aux_lob_table()
          || table_schema.is_mlog_table()) {
        if (OB_FAIL(update_data_table_schema_version(sql_client, tenant_id,
            table_schema.get_data_table_id(), table_schema.get_in_offline_ddl_white_list()))) {
          LOG_WARN("update_data_table_schema_version failed", "data_table_id",
              table_schema.get_data_table_id(), K(ret));
        }
      } else if (table_schema.get_foreign_key_real_count() > 0) {
        // We use "else" here since create foreign key on index/materialized_view is not supported.
        const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema.get_foreign_key_infos();
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
          const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
          const ObSimpleTableSchemaV2 *external_db_table_schema = NULL;
          const uint64_t update_table_id = table_schema.get_table_id() == foreign_key_info.parent_table_id_
              ? foreign_key_info.child_table_id_
              : foreign_key_info.parent_table_id_;
          if (is_drop_db) {
            if (NULL == schema_guard) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema guard is null when to drop database", K(ret));
            } else {
              // int tmp_ret = OB_SUCCESS;
              tmp_ret = schema_guard->get_simple_table_schema(
                        table_schema.get_tenant_id(),
                        update_table_id,
                        external_db_table_schema);
              if (OB_SUCCESS != tmp_ret) { // do-nothing
                LOG_WARN("get_table_schema failed", K(update_table_id), K(tmp_ret));
              } else if (NULL == external_db_table_schema) {
                // do-nothing
              } else if (table_schema.get_database_id() != external_db_table_schema->get_database_id()) {
                // FIXME: foreign key should not be across dbs.
                tmp_ret = update_data_table_schema_version(sql_client, tenant_id,
                          update_table_id, table_schema.get_in_offline_ddl_white_list());
                LOG_WARN("update child table schema version", K(tmp_ret));
              }
            }
          } else if ((NULL != drop_table_set) && (OB_HASH_EXIST == drop_table_set->exist_refactored(update_table_id))) {
            // no need to update data table schema version since table has been dropped.
          } else if (update_table_id == table_schema.get_table_id()) {
            // no need to update data table schema version since data table is itself.
          } else if (OB_FAIL(update_data_table_schema_version(sql_client, tenant_id, update_table_id,
                             table_schema.get_in_offline_ddl_white_list()))) {
            LOG_WARN("failed to update parent table schema version", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::insert_single_constraint(ObISQLClient &sql_client,
                                            const ObTableSchema &new_table_schema,
                                            const ObConstraint &new_constraint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(new_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(new_table_schema));
  } else if (OB_FAIL(add_single_constraint(sql_client, new_constraint,
                                          false, /* only_history */
                                          true, /* need_to_deal_with_cst_cols */
                                          false /* do_cst_revise */))) {
    SHARE_SCHEMA_LOG(WARN, "add_single_constraint failed", K(new_constraint), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = new_table_schema.get_tenant_id();
    opt.database_id_ = new_table_schema.get_database_id();
    opt.tablegroup_id_ = new_table_schema.get_tablegroup_id();
    opt.table_id_ = new_table_schema.get_table_id();
    opt.op_type_ = OB_DDL_ADD_CONSTRAINT;
    opt.schema_version_ = new_constraint.get_schema_version();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::revise_check_cst_column_info(
    common::ObISQLClient &sql_client,
    const ObTableSchema &table_schema,
    const ObIArray<ObConstraint> &csts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else if (0 == csts.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of csts is zero", K(ret));
  } else {
    ObDMLSqlSplicer dml;
    int64_t affected_rows = 0;
    const uint64_t tenant_id = csts.at(0).get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    for (int64_t i = 0; OB_SUCC(ret) && i < csts.count(); ++i) {
      dml.reset();
      for (ObConstraint::const_cst_col_iterator iter = csts.at(i).cst_col_begin();
           OB_SUCC(ret) && (iter != csts.at(i).cst_col_end());
           ++iter) {
        dml.reset();
        if (OB_FAIL(gen_constraint_column_dml(exec_tenant_id, csts.at(i), *iter, dml))) {
          LOG_WARN("failed to gen constraint column dml", K(ret));
        } else if (OB_FAIL(exec.exec_insert(
            OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME, dml, affected_rows))) {
          LOG_WARN("failed to insert constraint column", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be one", K(ret), K(affected_rows));
        }
        if (OB_SUCC(ret)) {
          const int64_t is_deleted = 0;
          if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
            LOG_WARN("add column failed", K(ret));
          } else if (OB_FAIL(exec.exec_insert(
              OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
            LOG_WARN("execute insert failed", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(ret), K(affected_rows));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::insert_single_column(
    ObISQLClient &sql_client,
    const ObTableSchema &new_table_schema,
    const ObColumnSchemaV2 &new_column_schema,
    const bool record_ddl_operation)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(new_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(new_table_schema));
  } else if (OB_FAIL(add_single_column(sql_client, new_column_schema))) {
    SHARE_SCHEMA_LOG(WARN, "add_single_column failed", K(new_column_schema), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (new_column_schema.is_autoincrement()) {
      if (OB_FAIL(add_sequence(sql_client,
                               new_table_schema.get_tenant_id(),
                               new_table_schema.get_table_id(),
                               new_column_schema.get_column_id(),
                               new_table_schema.get_auto_increment(),
                               new_table_schema.get_truncate_version()))) {
        SHARE_SCHEMA_LOG(WARN, "insert sequence record failed",
                         K(ret), K(new_column_schema));
      }
    }
  }
  if (OB_SUCC(ret) && record_ddl_operation) {
    ObSchemaOperation opt;
    opt.tenant_id_ = new_table_schema.get_tenant_id();
    opt.database_id_ = new_table_schema.get_database_id();
    opt.tablegroup_id_ = new_table_schema.get_tablegroup_id();
    opt.table_id_ = new_table_schema.get_table_id();
    opt.op_type_ = OB_DDL_ADD_COLUMN;
    opt.schema_version_ = new_column_schema.get_schema_version();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_single_column(
    ObISQLClient &sql_client,
    const ObTableSchema &origin_table_schema,
    const ObTableSchema &new_table_schema,
    const ObColumnSchemaV2 &new_column_schema,
    const bool record_ddl_operation,
    const bool need_del_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t table_id = new_column_schema.get_table_id();

  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(new_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(new_table_schema));
  } else if (OB_FAIL(gen_column_dml(exec_tenant_id, new_column_schema, dml))) {
    LOG_WARN("gen column dml failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_update(sql_client, tenant_id, table_id,
                            OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(ret), K(affected_rows));
    }
  }

  // change from non-auto-increment to auto-increment, add sequence record
  // or
  // change from auto-increment to non-auto-increment, do not delete sequence record, ignore it
  ObSqlString sql;
  if (OB_SUCC(ret)) {
    if (new_column_schema.is_autoincrement()) {
      if (origin_table_schema.get_autoinc_column_id() == 0 &&
          new_table_schema.get_autoinc_column_id() == new_column_schema.get_column_id()) {
        if (OB_FAIL(add_sequence(sql_client,
                                 tenant_id,
                                 new_table_schema.get_table_id(),
                                 new_column_schema.get_column_id(),
                                 new_table_schema.get_auto_increment(),
                                 new_table_schema.get_truncate_version()))) {
          LOG_WARN("insert sequence record failed", K(ret), K(new_table_schema));
        }
      } else if (new_table_schema.get_autoinc_column_id() == new_column_schema.get_column_id()) {
        // do nothing; auto-increment column does not change
      } else {
        const ObColumnSchemaV2 *origin_inc_column_schema = origin_table_schema.get_column_schema(new_column_schema.get_column_id());
        if (OB_ISNULL(origin_inc_column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The origin column is null", K(ret));
        } else if (new_column_schema.get_prev_column_id() != origin_inc_column_schema->get_prev_column_id()) {
          // update previous column id
        } else {
          // should check in alter_table_column
          ret = OB_ERR_UNEXPECTED;
          RS_LOG(WARN, "only one auto-increment column permitted", K(ret));
        }
      }
    }
  }
  // for drop column online, delete column stat.
  if (OB_SUCC(ret) && need_del_stats) {
    if (OB_FAIL(delete_column_stat(sql_client, tenant_id, table_id, new_column_schema.get_column_id()))) {
      LOG_WARN("fail to delete column stat", K(ret));
    }
  }

  // insert updated column to __all_column_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_single_column(sql_client, new_column_schema, only_history))) {
      LOG_WARN("add_single_column failed", K(new_column_schema), K(ret));
    } else if (record_ddl_operation) {
      // log operation
      ObSchemaOperation opt;
      opt.tenant_id_ = new_table_schema.get_tenant_id();
      opt.database_id_ = new_table_schema.get_database_id();
      opt.tablegroup_id_ = new_table_schema.get_tablegroup_id();
      opt.table_id_ = new_table_schema.get_table_id();
      opt.op_type_ = OB_DDL_MODIFY_COLUMN;
      opt.schema_version_ = new_column_schema.get_schema_version();
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  }

  return ret;
}

int ObTableSqlService::add_columns(ObISQLClient &sql_client,
                                  const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else if (is_core_table(table.get_table_id())) {
    ret = add_columns_for_core(sql_client, table);
  } else {
    ret = add_columns_for_not_core(sql_client, table);
  }
  return ret;
}

int ObTableSqlService::add_columns_for_core(ObISQLClient &sql_client, const ObTableSchema &table)
{
  int ret = OB_SUCCESS;

  const int64_t tenant_id = table.get_tenant_id();

  ObDMLSqlSplicer dml;
  ObCoreTableProxy kv(OB_ALL_COLUMN_TNAME, sql_client, tenant_id);
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else if (OB_FAIL(kv.load_for_update())) {
    LOG_WARN("failed to load kv for update", K(ret));
  }
  // for batch sql query
  bool enable_stash_query = false;
  ObSqlTransQueryStashDesc *stash_desc = NULL;
  ObMySQLTransaction* trans = dynamic_cast<ObMySQLTransaction*>(&sql_client);
  if (OB_SUCC(ret) && trans != nullptr && trans->get_enable_query_stash()) {
    enable_stash_query = true;
    if (OB_FAIL(trans->get_stash_query(tenant_id, OB_ALL_COLUMN_HISTORY_TNAME, stash_desc))) {
      LOG_WARN("get_stash_query fail", K(ret), K(tenant_id));
    }
  }
  for (ObTableSchema::const_column_iterator iter = table.column_begin();
      OB_SUCC(ret) && iter != table.column_end(); ++iter) {
    dml.reset();
    cells.reuse();
    int64_t affected_rows = 0;
    ObColumnSchemaV2 column;
    if (OB_FAIL(column.assign(**iter))) {
      LOG_WARN("fail to assign column", KR(ret), KPC(*iter));
    } else {
      column.set_schema_version(table.get_schema_version());
      column.set_tenant_id(table.get_tenant_id());
      column.set_table_id(table.get_table_id());
    }
    if (FAILEDx(gen_column_dml(tenant_id, column, dml))) {
      LOG_WARN("gen column dml failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("splice core cells failed", K(ret));
    } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
      LOG_WARN("failed to replace row", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (enable_stash_query) {
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (stash_desc->get_stash_query().empty()) {
        if (OB_FAIL(dml.splice_insert_sql_without_plancache(OB_ALL_COLUMN_HISTORY_TNAME, stash_desc->get_stash_query()))) {
          LOG_WARN("dml splice_insert_sql fail", K(ret));
        }
      } else {
        ObSqlString value_str;
        if (OB_FAIL(dml.splice_values(value_str))) {
          LOG_WARN("splice_values failed", K(ret));
        } else if (OB_FAIL(stash_desc->get_stash_query().append_fmt(", (%s)", value_str.ptr()))) {
          LOG_WARN("append_fmt failed", K(value_str), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        stash_desc->add_row_cnt(1);
      }
    } else {
      ObDMLExecHelper exec(sql_client, tenant_id);
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && enable_stash_query) {
    if (OB_FAIL(trans->do_stash_query_batch())) {
      LOG_WARN("do_stash_query_batch fail", K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::add_columns_dml(
    const ObTableSchema &table,
    share::ObDMLSqlSplicer &all_column_dml,
    int64_t &column_count)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  const int64_t schema_version = table.get_schema_version();
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  column_count = 0;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else {
    for (ObTableSchema::const_column_iterator iter = table.column_begin();
        OB_SUCCESS == ret && iter != table.column_end(); ++iter) {
      if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is NULL", KR(ret));
      } else {
        ObColumnSchemaV2 &column = **iter;
        const int64_t raw_schema_version = column.get_schema_version();
        const uint64_t raw_tenant_id = column.get_tenant_id();
        const uint64_t raw_table_id = column.get_table_id();
        column.set_schema_version(schema_version);
        column.set_tenant_id(tenant_id);
        column.set_table_id(table.get_table_id());
        if (OB_FAIL(gen_column_dml(exec_tenant_id, column, all_column_dml))) {
          LOG_WARN("failed to gen_column_dml", KR(ret), K(exec_tenant_id), K(column));
        } else if (OB_FAIL(all_column_dml.finish_row())) {
          LOG_WARN("failed to finish row", KR(ret));
        } else {
          column_count++;
        }
        column.set_schema_version(raw_schema_version);
        column.set_tenant_id(raw_tenant_id);
        column.set_table_id(raw_table_id);
      }
    }
  }
  LOG_INFO("add_columns_dml finish", KR(ret), K(column_count), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObTableSqlService::batch_add_columns_for_create_table(common::ObISQLClient &sql_client,
    const ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (tables.empty()) {
  } else {
    ObDMLSqlSplicer dml;
    common::ObTimeGuard time_guard("batch_add_columns_for_create_table", 1_ms);
    const uint64_t tenant_id = tables.at(0).get_tenant_id();
    int64_t column_count = 0;
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); i++) {
      const ObTableSchema &table = tables.at(i);
      int64_t tmp = 0;
      if (table.get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id not same", KR(ret), K(table), K(tenant_id));
      } else if (table.is_view_table() && !table.view_column_filled()) {
      } else if (OB_FAIL(add_columns_dml(table, dml, tmp))) {
        ObCStringHelper helper;
        LOG_WARN("insert table schema failed, ", KR(ret), "table", helper.convert(table));
      } else {
        column_count += tmp;
      }
    }
    time_guard.click("generate_dml") ;
    if (FAILEDx(exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_TNAME, dml, column_count))) {
      LOG_WARN("failed to insert all_column", KR(ret), K(tenant_id), K(column_count));
    } else if (FALSE_IT(time_guard.click("insert_all_column"))) {
    } else if (OB_FAIL(dml.set_default_columns("is_deleted", "0"))) {
      LOG_WARN("failed to set default columns", KR(ret));
    } else if (OB_FAIL(exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_HISTORY_TNAME, dml, column_count))) {
      LOG_WARN("failed to insert all_column_history", KR(ret), K(tenant_id), K(column_count));
    } else if (FALSE_IT(time_guard.click("insert_all_column_history"))) {
    }
  }
  return ret;
}


int ObTableSqlService::add_columns_for_not_core(ObISQLClient &sql_client,
                                                const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t column_count = 0;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (OB_FAIL(add_columns_dml(table, dml, column_count))) {
    LOG_WARN("failed to add columns dml", KR(ret), K(table));
  } else if (column_count != table.get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count not match", KR(ret), K(column_count), K(table.get_column_count()));
  } else if (OB_FAIL(exec_dml(sql_client, exec_tenant_id, OB_ALL_COLUMN_TNAME, dml, column_count))) {
    LOG_WARN("failed to exec dml", KR(ret), K(column_count));
  } else if (OB_FAIL(dml.set_default_columns("is_deleted", "0"))) {
    LOG_WARN("failed to set default columns", KR(ret));
  } else if (OB_FAIL(exec_dml(sql_client, exec_tenant_id, OB_ALL_COLUMN_HISTORY_TNAME, dml, column_count))) {
    LOG_WARN("failed to exec dml", KR(ret), K(column_count));
  }
  return ret;
}

int ObTableSqlService::add_constraints(ObISQLClient &sql_client,
                                       const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (is_core_table(table.get_table_id())) {
    // ignore
  } else if (table.get_constraint_count() > 0) {
    if (OB_FAIL(add_constraints_for_not_core(sql_client, table))) {
      LOG_WARN("add_constraints_for_not_core failed", KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::add_constraints_dml(
    const ObTableSchema &table,
    ObDMLSqlSplicer &cst_dml,
    ObDMLSqlSplicer &cst_col_dml,
    int64_t &cst_col_count)
{
  int ret = OB_SUCCESS;
  const int64_t new_schema_version = table.get_schema_version();
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (is_inner_table(table.get_table_id())) {
    // To avoid cyclic dependence
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("should not be here", KR(ret), K(table));
  }
  for (ObTableSchema::constraint_iterator cst_iter = table.constraint_begin_for_non_const_iter();
       OB_SUCC(ret) && cst_iter != table.constraint_end_for_non_const_iter();
       ++cst_iter) {
    if (OB_ISNULL(*cst_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", KR(ret));
    } else {
      // generate sql of 'insert into __all_constraint_history' and 'insert into __all_constraint'
      (*cst_iter)->set_schema_version(new_schema_version);
      (*cst_iter)->set_tenant_id(tenant_id);
      (*cst_iter)->set_table_id(table.get_table_id());
      if (OB_FAIL(gen_constraint_dml(exec_tenant_id, **cst_iter, cst_dml))) {
        LOG_WARN("gen_constraint_dml failed", KR(ret), K(**cst_iter));
      } else if (OB_FAIL(cst_dml.finish_row())) {
        LOG_WARN("failed to finish row", KR(ret));
      }
      for (ObConstraint::const_cst_col_iterator cst_col_iter = (*cst_iter)->cst_col_begin();
            OB_SUCC(ret) && (cst_col_iter != (*cst_iter)->cst_col_end());
            ++cst_col_iter, ++cst_col_count) {
        if (OB_FAIL(gen_constraint_column_dml(exec_tenant_id, **cst_iter, *cst_col_iter, cst_col_dml))) {
          LOG_WARN("failed to gen constraint column dml", KR(ret));
        } else if (OB_FAIL(cst_col_dml.finish_row())) {
          LOG_WARN("failed to finish row", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::batch_add_constraints_for_create_table(
    common::ObISQLClient &sql_client, const ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (tables.empty()) {
  } else {
    ObDMLSqlSplicer cst_dml;
    ObDMLSqlSplicer cst_col_dml;
    common::ObTimeGuard time_guard("batch_add_constraints_for_create_table", 1_ms);
    const uint64_t tenant_id = tables.at(0).get_tenant_id();
    int64_t cst_col_count = 0;
    int64_t cst_count = 0;
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); i++) {
      const ObTableSchema &table = tables.at(i);
      int64_t tmp = 0;
      if (table.get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id not same", KR(ret), K(table), K(tenant_id));
      } else if (table.is_view_table() || table.get_constraint_count() <= 0) {
      } else if (OB_FAIL(add_constraints_dml(table, cst_dml, cst_col_dml, tmp))) {
        ObCStringHelper helper;
        LOG_WARN("insert table schema failed, ", KR(ret), "table", helper.convert(table));
      } else {
        cst_count += table.get_constraint_count();
        cst_col_count += tmp;
      }
    }
    time_guard.click("generate_dml") ;
    if (FAILEDx(exec_dml(sql_client, tenant_id, OB_ALL_CONSTRAINT_TNAME, cst_dml, cst_count))) {
      LOG_WARN("failed to insert all_cst", KR(ret), K(tenant_id), K(cst_count));
    } else if (FALSE_IT(time_guard.click("insert_all_cst"))) {
    } else if (OB_FAIL(cst_dml.set_default_columns("is_deleted", "0"))) {
      LOG_WARN("failed to set default columns", KR(ret));
    } else if (OB_FAIL(exec_dml(sql_client, tenant_id, OB_ALL_CONSTRAINT_HISTORY_TNAME, cst_dml, cst_count))) {
      LOG_WARN("failed to insert all_cst_history", KR(ret), K(tenant_id), K(cst_count));
    } else if (FALSE_IT(time_guard.click("insert_all_cst_history"))) {
    } else if (OB_FAIL(exec_dml(sql_client, tenant_id, OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME,
            cst_col_dml, cst_col_count))) {
      LOG_WARN("failed to insert all_cst_column", KR(ret), K(tenant_id), K(cst_col_count));
    } else if (FALSE_IT(time_guard.click("insert_all_cst_col"))) {
    } else if (OB_FAIL(cst_col_dml.set_default_columns("is_deleted", "0"))) {
      LOG_WARN("failed to set default columns", KR(ret));
    } else if (OB_FAIL(exec_dml(sql_client, tenant_id, OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME,
            cst_col_dml, cst_col_count))) {
      LOG_WARN("failed to insert all_cst_column_history", KR(ret), K(tenant_id), K(cst_col_count));
    } else if (FALSE_IT(time_guard.click("insert_all_cst_col_history"))) {
    }
  }
  return ret;
}

// create table add constraint
int ObTableSqlService::add_constraints_for_not_core(ObISQLClient &sql_client,
                                                    const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t cst_cols_num_in_table = 0;
  ObDMLSqlSplicer cst_dml;
  ObDMLSqlSplicer cst_col_dml;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (is_inner_table(table.get_table_id())) {
    // To avoid cyclic dependence
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("should not be here", KR(ret), K(table));
  } else if (OB_FAIL(add_constraints_dml(table, cst_dml, cst_col_dml, cst_cols_num_in_table))) {
    LOG_WARN("failed to add constraints dml", KR(ret), K(table));
  } else if (OB_FAIL(exec_dml(sql_client, exec_tenant_id, OB_ALL_CONSTRAINT_TNAME, cst_dml,
          table.get_constraint_count()))) {
    LOG_WARN("failed to insert all constraint table", KR(ret));
  } else if (OB_FAIL(cst_dml.set_default_columns("is_deleted", "0"))) {
    LOG_WARN("failed to set default columns", KR(ret));
  } else if (OB_FAIL(exec_dml(sql_client, exec_tenant_id, OB_ALL_CONSTRAINT_HISTORY_TNAME, cst_dml,
          table.get_constraint_count()))) {
    LOG_WARN("failed to insert all constraint history table", KR(ret));
  } else if (cst_col_dml.empty()) {
  } else if (OB_FAIL(exec_dml(sql_client, exec_tenant_id, OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME,
          cst_col_dml, cst_cols_num_in_table))) {
    LOG_WARN("failed to insert all column constraint history table", KR(ret));
  } else if (OB_FAIL(cst_col_dml.set_default_columns("is_deleted", "0"))) {
    LOG_WARN("failed to set default columns", KR(ret));
  } else if (OB_FAIL(exec_dml(sql_client, exec_tenant_id, OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME,
          cst_col_dml, cst_cols_num_in_table))) {
    LOG_WARN("failed to insert all column constraint history table", KR(ret));
  }
  return ret;
}

int ObTableSqlService::rename_csts_in_inner_table(common::ObISQLClient &sql_client,
                                                  const ObTableSchema &table_schema,
                                                  const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObString new_cst_name;
  ObSqlString constraint_history_sql;
  uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  ObDMLSqlSplicer dml_for_update;
  ObDMLSqlSplicer dml_for_insert;
  bool is_oracle_mode = false;
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check if tenant mode is oracle mode", K(ret));
  }
  for (; OB_SUCC(ret) && iter != table_schema.constraint_end(); ++iter) {
    dml_for_update.reuse();
    dml_for_insert.reuse();
    int64_t affected_rows = 0;
    // `drop table` modify constraint_name but do not modify name_generated_type
    const ObNameGeneratedType name_generated_type = (*iter)->get_name_generated_type();
    if (OB_FAIL(ObTableSchema::create_cons_name_automatically(new_cst_name, table_schema.get_table_name_str(), allocator, (*iter)->get_constraint_type(), is_oracle_mode))) {
      SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
    } else if (OB_FAIL(gen_constraint_update_name_dml(exec_tenant_id, new_cst_name, name_generated_type, new_schema_version, **iter, dml_for_update))) {
      LOG_WARN("failed to delete from __all_constraint or __all_constraint_history", K(ret), K(new_cst_name), K(**iter), K(table_schema));
    } else if (OB_FAIL(exec_update(sql_client, tenant_id, table_schema.get_table_id(),
                                   OB_ALL_CONSTRAINT_TNAME, dml_for_update, affected_rows))) {
      LOG_WARN("execute update sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert succeeded but affected_rows is not one", K(ret), K(affected_rows));
    } else if (OB_FAIL(gen_constraint_insert_new_name_row_dml(exec_tenant_id, new_cst_name, name_generated_type, new_schema_version, **iter, dml_for_insert))) {
      LOG_WARN("failed to delete from __all_constraint or __all_constraint_history", K(ret), K(new_cst_name), K(**iter), K(table_schema));
    } else if (OB_FAIL(exec_insert(sql_client, tenant_id, table_schema.get_table_id(),
                                   OB_ALL_CONSTRAINT_HISTORY_TNAME, dml_for_insert, affected_rows))) {
      LOG_WARN("execute insert sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", K(ret), K(affected_rows));
    }
  }

  return ret;
}

int ObTableSqlService::delete_constraint(common::ObISQLClient &sql_client,
                                            const ObTableSchema &table_schema,
                                            const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const int64_t is_deleted = 1;
  int64_t cst_cols_num_in_table = 0;
  int64_t affected_rows = 0;
  ObSqlString constraint_sql;
  ObSqlString constraint_history_sql;
  ObSqlString constraint_column_sql;
  ObSqlString constraint_column_history_sql;
  ObTableSchema::const_constraint_iterator cst_iter = table_schema.constraint_begin();

  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  }
  for (; OB_SUCC(ret) && cst_iter != table_schema.constraint_end(); ++cst_iter) {
    // generate sql of 'insert into __all_constraint_history' and 'delete from __all_constraint'
    if (OB_ISNULL(*cst_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", K(ret));
    } else if (cst_iter == table_schema.constraint_begin()) {
      if (OB_FAIL(constraint_history_sql.assign_fmt(
          "INSERT INTO %s(tenant_id, table_id, constraint_id, schema_version, is_deleted)"
          " VALUES(%lu, %lu, %lu, %ld, %ld)",
          OB_ALL_CONSTRAINT_HISTORY_TNAME,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*cst_iter)->get_table_id()),
          (*cst_iter)->get_constraint_id(), new_schema_version, is_deleted))) {
        LOG_WARN("assign insert into __all_constraint_history fail",
            K(ret), K(tenant_id), K((*cst_iter)->get_table_id()), K((*cst_iter)->get_constraint_id()), K(new_schema_version));
      } else if (OB_FAIL(constraint_sql.assign_fmt(
            "DELETE FROM %s WHERE (tenant_id, table_id, constraint_id)"
            " IN ((%lu, %lu, %lu)",
            OB_ALL_CONSTRAINT_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*cst_iter)->get_table_id()),
            (*cst_iter)->get_constraint_id()))) {
        LOG_WARN("assign_fmt failed", K(ret));
      }
    } else {
      if (OB_FAIL(constraint_history_sql.append_fmt(
          ", (%lu, %lu, %lu, %ld, %ld)",
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*cst_iter)->get_table_id()),
          (*cst_iter)->get_constraint_id(), new_schema_version, is_deleted))) {
        LOG_WARN("assign insert into __all_constraint_history fail", K(ret), K(tenant_id),
                 K((*cst_iter)->get_table_id()), K((*cst_iter)->get_constraint_id()), K(new_schema_version));
      } else if (OB_FAIL(constraint_sql.append_fmt(
          ", (%lu, %lu, %lu)",
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*cst_iter)->get_table_id()),
          (*cst_iter)->get_constraint_id()))) {
        LOG_WARN("assign_fmt failed", K(ret));
      }
    }
    // generate sql of 'insert into __all_constraint_column_history' and 'delete from __all_constraint_column'
    if (OB_SUCC(ret)) {
      for (ObConstraint::const_cst_col_iterator cst_col_iter = (*cst_iter)->cst_col_begin();
           OB_SUCC(ret) && (cst_col_iter != (*cst_iter)->cst_col_end());
           ++cst_col_iter, ++cst_cols_num_in_table) {
        if (constraint_column_sql.empty()) {
          if (OB_FAIL(constraint_column_history_sql.assign_fmt(
              "INSERT INTO %s(tenant_id, table_id, constraint_id, column_id, schema_version, is_deleted)"
              " VALUES(%lu, %lu, %lu, %lu, %ld, %ld)",
              OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME,
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*cst_iter)->get_table_id()),
              (*cst_iter)->get_constraint_id(), *cst_col_iter, new_schema_version, is_deleted))) {
            LOG_WARN("assign insert into __all_constraint_column_history fail",
                     K(ret), K(tenant_id), K((*cst_iter)->get_table_id()), K((*cst_iter)->get_constraint_id()),
                     K(*cst_col_iter), K(new_schema_version), K(constraint_column_history_sql));
          } else if (OB_FAIL(constraint_column_sql.assign_fmt(
              "DELETE FROM %s WHERE (tenant_id, table_id, constraint_id, column_id)"
              " IN ((%lu, %lu, %lu, %lu)",
              OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME,
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*cst_iter)->get_table_id()),
              (*cst_iter)->get_constraint_id(),
              *cst_col_iter))) {
            LOG_WARN("assign_fmt failed", K(ret), K(constraint_column_sql));
          }
        } else {
          if (OB_FAIL(constraint_column_history_sql.append_fmt(
              ", (%lu, %lu, %lu, %lu, %ld, %ld)",
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*cst_iter)->get_table_id()),
              (*cst_iter)->get_constraint_id(), *cst_col_iter, new_schema_version, is_deleted))) {
            LOG_WARN("assign insert into __all_constraint_column_history fail",
                     K(ret), K(tenant_id), K((*cst_iter)->get_table_id()), K((*cst_iter)->get_constraint_id()),
                     K(*cst_col_iter), K(new_schema_version), K(constraint_column_history_sql));
          } else if (OB_FAIL(constraint_column_sql.append_fmt(
              ", (%lu, %lu, %lu, %lu)",
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*cst_iter)->get_table_id()),
              (*cst_iter)->get_constraint_id(),
              *cst_col_iter))) {
            LOG_WARN("assign_fmt failed", K(ret), K(constraint_column_sql));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!constraint_sql.empty() && OB_FAIL(constraint_sql.append_fmt(")"))) {
    LOG_WARN("assign_fmt assign ) to end failed", K(ret), K(constraint_sql));
  } else if (!constraint_column_sql.empty() && OB_FAIL(constraint_column_sql.append_fmt(")"))) {
    LOG_WARN("assign_fmt assign ) to end failed", K(ret), K(constraint_column_sql));
  }
  // execute constraint_sql and constraint_history_sql
  if (OB_SUCC(ret)) {
    if (table_schema.get_constraint_count() > 0) {
      if (OB_FAIL(sql_client.write(exec_tenant_id, constraint_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql fail", K(constraint_sql));
      } else if (table_schema.get_constraint_count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row counts has deleted", K(ret), K(constraint_history_sql),
                 K(table_schema.get_constraint_count()), K(affected_rows), K(table_schema));
      } else if (OB_FAIL(sql_client.write(
                         exec_tenant_id, constraint_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(constraint_history_sql));
      } else if (table_schema.get_constraint_count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row counts has inserted", K(ret), K(constraint_history_sql),
                 K(table_schema.get_constraint_count()), K(affected_rows), K(table_schema));
      }
    }
  }
  // execute constraint_column_sql and constraint_column_history_sql
  if (OB_SUCC(ret)) {
    if (!constraint_column_sql.empty()) {
      if (OB_FAIL(sql_client.write(exec_tenant_id, constraint_column_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql fail", K(constraint_sql));
      } else if (cst_cols_num_in_table != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row counts has deleted", K(ret), K(cst_cols_num_in_table),
                 K(affected_rows), K(table_schema), K(constraint_column_sql));
      } else if (OB_FAIL(sql_client.write(
                 exec_tenant_id, constraint_column_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(constraint_history_sql));
      } else if (cst_cols_num_in_table != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row counts has inserted", K(ret), K(cst_cols_num_in_table),
                 K(affected_rows), K(table_schema), K(constraint_column_history_sql));
      }
    }
  }

  return ret;
}

int ObTableSqlService::supplement_for_core_table(ObISQLClient &sql_client,
                                                 const bool is_all_table,
                                                 const ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;

  int64_t orig_default_value_len = 0;
  const int64_t value_buf_len = 2 * OB_MAX_DEFAULT_VALUE_LENGTH + 3;
  char *orig_default_value_buf = NULL;
  orig_default_value_buf = static_cast<char *>(
      malloc(value_buf_len));
  if (OB_ISNULL(orig_default_value_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMSET(orig_default_value_buf, 0, value_buf_len);
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    if (!ob_is_string_type(column.get_data_type()) && !ob_is_json(column.get_data_type())
        && !ob_is_geometry(column.get_data_type()) && !ob_is_roaringbitmap(column.get_data_type())) {
      if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(
          column.get_tenant_id(), column.get_table_id(), compat_mode))) {
        LOG_WARN("fail to get tenant mode", K(ret), K(column));
      } else {
        lib::CompatModeGuard compat_mode_guard(compat_mode);
        ObTimeZoneInfo tz_info;
        if (OB_FAIL(column.get_orig_default_value().print_plain_str_literal(
                orig_default_value_buf, value_buf_len, orig_default_value_len, &tz_info))) {
          LOG_WARN("failed to print orig default value", K(ret),
              K(value_buf_len), K(orig_default_value_len));
        }
      }
    }
  }
  ObString orig_default_value;
  if (OB_SUCC(ret)) {
    if (ob_is_string_type(column.get_data_type()) || ob_is_json(column.get_data_type())
        || ob_is_geometry(column.get_data_type()) || ob_is_roaringbitmap(column.get_data_type())) {
      ObString orig_default_value_str = column.get_orig_default_value().get_string();
      orig_default_value.assign_ptr(orig_default_value_str.ptr(), orig_default_value_str.length());
    } else {
      orig_default_value.assign_ptr(orig_default_value_buf,
                                    static_cast<int32_t>(orig_default_value_len));
    }
    if (column.get_orig_default_value().is_null()) {
      orig_default_value.reset();
    }
  }
  const char *supplement_tbl_name = NULL;
  if (OB_FAIL(ret)) {
  } else if (is_all_table) {
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(OB_SYS_TENANT_ID, supplement_tbl_name))) {
      LOG_WARN("fail to get all table name", K(ret));
    }
  } else {
    supplement_tbl_name = OB_ALL_COLUMN_TNAME;
  }
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = column.get_tenant_id();
    ObCoreTableProxy kv(supplement_tbl_name, sql_client, tenant_id);
    if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("failed to load kv for update", K(ret));
    } else {
      ObCoreTableProxy::UpdateCell ucell;
      ucell.is_filter_cell_ = false;
      ObCoreTableProxy::Cell cell;
      cell.name_ = column.get_column_name_str();
      ObSqlString sql_string;
      if (column.get_orig_default_value().is_null()) {
        cell.value_.reset();
      } else {
        if (OB_FAIL(sql_append_hex_escape_str(ObHexEscapeSqlStr(orig_default_value).str(),
                                              sql_string))) {
          LOG_WARN("sql_append_hex_escape_str failed", K(ret));
        } else {
          cell.value_ = sql_string.string();
        }
      }
      if (OB_SUCC(ret)) {
        cell.is_hex_value_ = true;
        if (OB_FAIL(kv.store_cell(cell, ucell.cell_))) {
          LOG_WARN("store cell failed");
        } else if (OB_FAIL(kv.supplement_cell(ucell))) {
          LOG_WARN("supplement_cell failed", K(ucell));
        }
      }
    }
  }
  if (NULL != orig_default_value_buf) {
    free(orig_default_value_buf);
    orig_default_value_buf = NULL;
  }

  return ret;
}

// alter table add check constraint
int ObTableSqlService::add_single_constraint(ObISQLClient &sql_client,
                                             const ObConstraint &constraint,
                                             const bool only_history,
                                             const bool need_to_deal_with_cst_cols,
                                             const bool do_cst_revise)
{
  int ret = OB_SUCCESS;
  UNUSED(do_cst_revise);
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = constraint.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;

  if (OB_FAIL(gen_constraint_dml(exec_tenant_id, constraint, dml))) {
    LOG_WARN("gen constraint dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      const int64_t table_id = constraint.get_table_id();
      if (OB_FAIL(exec_insert(sql_client, tenant_id, table_id,
                              OB_ALL_CONSTRAINT_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add constraint failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_CONSTRAINT_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }

  // __all_constraint_column and __all_constraint_column_history
  if (OB_SUCC(ret)
      && need_to_deal_with_cst_cols) {
    // Because column schema won't change while alter table modify constraint states,
    // it's no need to modify constraint_column.
    for (ObConstraint::const_cst_col_iterator iter = constraint.cst_col_begin();
         OB_SUCC(ret) && (iter != constraint.cst_col_end());
         ++iter) {
      dml.reset();
      if (OB_FAIL(gen_constraint_column_dml(exec_tenant_id, constraint, *iter, dml))) {
        LOG_WARN("failed to gen constraint column dml", K(ret));
      } else {
        if (!only_history) {
          if (OB_FAIL(exec.exec_insert(
              OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME, dml, affected_rows))) {
            LOG_WARN("failed to insert foreign key column", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t is_deleted = 0;
          if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
            LOG_WARN("add column failed", K(ret));
          } else if (OB_FAIL(exec.exec_insert(
              OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
            LOG_WARN("execute insert failed", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableSqlService::add_single_column(ObISQLClient &sql_client,
                                         const ObColumnSchemaV2 &column,
                                         const bool only_history)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = column.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(gen_column_dml(exec_tenant_id, column, dml))) {
    LOG_WARN("gen column dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      const int64_t table_id = column.get_table_id();
      if (OB_FAIL(exec_insert(sql_client, tenant_id, table_id,
                              OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      } else {
        bool is_all_table = OB_ALL_TABLE_TID == table_id;
        bool is_all_column = OB_ALL_COLUMN_TID == table_id;
        if (is_all_table || is_all_column) {
          if (OB_FAIL(supplement_for_core_table(sql_client, is_all_table, column))) {
            LOG_WARN("supplement_for_core_table failed", K(is_all_table), K(column), K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }

  return ret;
}

int ObTableSqlService::add_table(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const bool update_object_status_ignore_version,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = table.get_tenant_id();
  const int64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(add_table_dml(table, update_object_status_ignore_version, dml))) {
    LOG_WARN("failed to add table dml", KR(ret), K(table));
  } else if (OB_FAIL(dml.finish_row())) {
    LOG_WARN("failed to finish_row", KR(ret));
  } else {
    if (!only_history) {
      int64_t affected_rows = 0;
      if (OB_FAIL(exec_insert(sql_client, exec_tenant_id, table_id, OB_ALL_TABLE_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert failed", KR(ret), K(exec_tenant_id));
      } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.set_default_columns("is_deleted", "0"))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(exec_dml(sql_client, exec_tenant_id, OB_ALL_TABLE_HISTORY_TNAME, dml,
              1/*target_affected_row_count*/))) {
        LOG_WARN("execute insert failed", KR(ret), K(exec_tenant_id));
      }
    }
    if (OB_SUCC(ret) && only_history) {
      if (OB_FAIL(check_table_history_matched_(
          sql_client, tenant_id, table_id, table.get_schema_version()))) {
        LOG_WARN("fail to check if table history matched", KR(ret),
                 K(tenant_id), K(table_id), "schema_version", table.get_schema_version());
      }
    }
  }

  return ret;
}

int ObTableSqlService::add_table_dml(const ObTableSchema &table,
    const bool update_object_status_ignore_version,
    ObDMLSqlSplicer &all_table_dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (OB_FAIL(gen_table_dml(exec_tenant_id, table,
          update_object_status_ignore_version, all_table_dml))) {
    LOG_WARN("gen table dml failed", KR(ret));
  }
  return ret;
}

int ObTableSqlService::batch_add_table_for_create_table(common::ObISQLClient &sql_client,
    const ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (tables.empty()) {
  } else {
    ObDMLSqlSplicer dml;
    common::ObTimeGuard time_guard("batch_add_table_for_create_table", 1_ms);
    const uint64_t tenant_id = tables.at(0).get_tenant_id();
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); i++) {
      const ObTableSchema &table = tables.at(i);
      if (table.get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id not same", KR(ret), K(table), K(tenant_id));
      } else if (OB_FAIL(add_table_dml(table, false/*update_object_status_ignore_version*/, dml))) {
        ObCStringHelper helper;
        LOG_WARN("insert table schema failed, ", KR(ret), "table", helper.convert(table));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish_row", KR(ret));
      }
    }
    time_guard.click("generate_dml");
    if (FAILEDx(exec_dml(sql_client, tenant_id, OB_ALL_TABLE_TNAME, dml, tables.count()))) {
      LOG_WARN("failed to insert all_table", KR(ret), K(tenant_id), K(tables.count()));
    } else if (FALSE_IT(time_guard.click("insert_all_table"))) {
    } else if (OB_FAIL(dml.set_default_columns("is_deleted", "0"))) {
      LOG_WARN("failed to set default columns", KR(ret));
    } else if (OB_FAIL(exec_dml(sql_client, tenant_id, OB_ALL_TABLE_HISTORY_TNAME, dml, tables.count()))) {
      LOG_WARN("failed to insert all_table_history", KR(ret), K(tenant_id), K(tables.count()));
    } else if (FALSE_IT(time_guard.click("insert_all_table_history"))) {
    }
  }
  return ret;
}

// check table's latest history is matched with related record in __all_table.
int ObTableSqlService::check_table_history_matched_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == table_id
      || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id/table_id/schema_version",
             KR(ret), K(tenant_id), K(table_id), K(schema_version));
  } else if (is_core_table(table_id)) {
    // TODO:(yanmu.ztl) core tables don't record in __all_table, we should check __all_core_table instead.
  } else {
    // use __all_table_history's columns for compatibility because of the following factors:
    // 1. __all_table_history's definition is generated by __all_table's.
    // 2. __all_table's schema will be upgraded before __all_table_history's schema.
    ObSchemaGetterGuard guard;
    const ObTableSchema *table_schema = NULL;
    ObSqlString column_sql;
    if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(tenant_id, guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(guard.get_table_schema(
               tenant_id, OB_ALL_TABLE_HISTORY_TID, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema not exist", KR(ret), K(tenant_id), "table_id", OB_ALL_TABLE_HISTORY_TID);
    } else {
      bool first_flag = true;
      ObString column_name;
      for (auto col = table_schema->column_begin();
           OB_SUCC(ret) && col != table_schema->column_end(); col++) {
        if (OB_ISNULL(*col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("col is null", KR(ret));
        } else if (FALSE_IT(column_name = (*col)->get_column_name_str())) {
        } else if (0 == column_name.case_compare("gmt_create")
                   || 0 == column_name.case_compare("gmt_modified")
                   || 0 == column_name.case_compare("is_deleted")) {
          // skip
        } else if (OB_FAIL(column_sql.append_fmt("%s%.*s",
                   first_flag ? "" : ",", column_name.length(), column_name.ptr()))) {
          LOG_WARN("fail to append fmt", KR(ret), K(column_name));
        } else {
          first_flag = false;
        }
      } // end for

      if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObSqlString sql;
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql.assign_fmt(
            "SELECT %s FROM %s WHERE tenant_id = 0 AND table_id = %lu AND schema_version = %ld"
            " EXCEPT SELECT %s FROM %s WHERE tenant_id = 0 AND table_id = %lu",
            column_sql.ptr(), OB_ALL_TABLE_HISTORY_TNAME, table_id, schema_version,
            column_sql.ptr(), OB_ALL_TABLE_TNAME, table_id))) {
          LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(table_id), K(schema_version));
        } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get result", KR(ret));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get next", KR(ret));
          }
        } else {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("__all_table_history's row not match with __all_table's",
                   KR(ret), K(tenant_id), K(table_id), K(schema_version));
        }
      } // end SMART_VAR
      }
    }
  }
  return ret;
}

/**
 * @operation_type
 * operation_type can be OB_DDL_ALTER_TABLE or OB_DDL_TABLE_RENAME
 * alter table stmt is OB_DDL_ALTER_TABLE
 * rename table stmt is OB_DDL_TABLE_RENAME
 */
int ObTableSqlService::update_table_options(ObISQLClient &sql_client,
                                            const ObTableSchema &table_schema,
                                            ObTableSchema &new_table_schema,
                                            share::schema::ObSchemaOperationType operation_type,
                                            const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  uint64_t table_id = table_schema.get_table_id();
  if (OB_FAIL(inner_update_table_options_(sql_client, new_table_schema))) {
    LOG_WARN("fail to do inner update table option", KR(ret), KPC(ddl_stmt_str));
  }

  if (OB_SUCC(ret)) {
    if ((OB_DDL_DROP_TABLE_TO_RECYCLEBIN == operation_type)
        || (OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN == operation_type)) {
      // 1. For oracle compatibility, drop foreign key while drop table.
      // 2. Foreign key will be rebuilded while truncate table.
      if (OB_FAIL(delete_foreign_key(sql_client, new_table_schema, new_table_schema.get_schema_version(), OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN == operation_type))) {
        LOG_WARN("failed to delete foreign key", K(ret));
      }
    }
  }

  // rename constraint name while drop/truncate table and recyclebin is on.
  if (OB_SUCC(ret)) {
    if (OB_DDL_DROP_TABLE_TO_RECYCLEBIN == operation_type){
      // 这里遍历表上所有约束，逐一改掉内部表信息, update __all_constraint 里面改表中的各个约束名，并在 __all_constraint_history 里面增加一条记录
      // TODO:@xiaofeng.lby, this interface is independent of 'truncate table', modify it later.
      if (OB_FAIL(rename_csts_in_inner_table(sql_client, new_table_schema, new_table_schema.get_schema_version()))) {
        LOG_WARN("failed to delete constraint", K(ret));
      }
    } else if (OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN == operation_type) {
      // Constraint will be rebuilded while truncate table.
      if (OB_FAIL(delete_constraint(sql_client, new_table_schema, new_table_schema.get_schema_version()))) {
        LOG_WARN("failed to delete constraint", K(ret));
      }
    }
  }
  // delete from __all_table_stat, __all_monitor_modified, __all_column_usage, __all_optstat_user_prefs
  if (OB_SUCC(ret)) {
    if (operation_type != OB_DDL_DROP_TABLE_TO_RECYCLEBIN) {
      // do nothing
    } else if (OB_FAIL(delete_from_all_table_stat(sql_client, tenant_id, table_id))) {
      LOG_WARN("delete from all table stat failed", K(ret));
    //column stat and histogram stat will be delete asynchronously by optimizer auto task, not delete here, avoid cost too much time.
    } else if (OB_FAIL(delete_from_all_column_usage(sql_client, tenant_id, table_id))) {
      LOG_WARN("failed to delete from all column usage", K(ret));
    } else if (OB_FAIL(delete_from_all_monitor_modified(sql_client, tenant_id, table_id))) {
      LOG_WARN("failed to delete from all monitor modified", K(ret));
    } else if (OB_FAIL(delete_from_all_optstat_user_prefs(sql_client, tenant_id, table_id))) {
      LOG_WARN("failed to delete all optstat user prefs", K(ret));
    }
  }

  // log operation
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    if (NULL != ddl_stmt_str) {
      opt.ddl_stmt_str_ = *ddl_stmt_str;
    }
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = new_table_schema.get_database_id();
    opt.tablegroup_id_ = new_table_schema.get_tablegroup_id();
    opt.table_id_ = new_table_schema.get_table_id();
    opt.op_type_ = operation_type;
    opt.schema_version_ = new_table_schema.get_schema_version();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (table_schema.get_foreign_key_real_count() > 0) {
      const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema.get_foreign_key_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
        const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
        const uint64_t update_table_id = table_schema.get_table_id() == foreign_key_info.parent_table_id_
            ? foreign_key_info.child_table_id_
            : foreign_key_info.parent_table_id_;
        if (OB_FAIL(update_data_table_schema_version(sql_client, tenant_id, update_table_id,
                    table_schema.get_in_offline_ddl_white_list()))) {
          LOG_WARN("failed to update parent table schema version", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("alter table", "table type", table_schema.get_table_type(),
              "index type", table_schema.get_index_type());
    if (new_table_schema.is_index_table()
        || new_table_schema.is_aux_vp_table()
        || new_table_schema.is_aux_lob_table()
        || new_table_schema.is_mlog_table()) {
      // use new_table_schema.get_in_offline_ddl_white_list() here for drop index when offline ddl failed, there is no foreign key on index table.
      if (OB_FAIL(update_data_table_schema_version(sql_client, tenant_id,
                  new_table_schema.get_data_table_id(), new_table_schema.get_in_offline_ddl_white_list()))) {
        LOG_WARN("update data table schema version failed", K(ret));
      }
    }
  }

  return ret;
}

// alter table drop constraint
int ObTableSqlService::delete_single_constraint(
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    const ObTableSchema &new_table_schema,
    const ObConstraint &orig_constraint)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t table_id = new_table_schema.get_table_id();
  const uint64_t constraint_id = orig_constraint.get_constraint_id();

  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(new_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(new_table_schema));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, table_id)))
      || OB_FAIL(dml.add_pk_column("constraint_id", constraint_id))) {
    LOG_WARN("add constraint failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                            OB_ALL_CONSTRAINT_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(affected_rows), K(ret));
    } else if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
               OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    }
  }

  ObSqlString sql;
  int64_t affected_rows = 0;
  // mark delete in __all_constraint_history
  if (OB_SUCC(ret)) {
    const int64_t is_deleted = 1;
    if (OB_FAIL(sql.assign_fmt(
        "INSERT INTO %s (TENANT_ID, TABLE_ID, CONSTRAINT_ID, SCHEMA_VERSION, IS_DELETED) values "
        "(%lu, %lu, %lu, %ld, %ld)",
        OB_ALL_CONSTRAINT_HISTORY_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, orig_constraint.get_table_id()),
        orig_constraint.get_constraint_id(),
        new_schema_version,
        is_deleted))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affect_rows expected to be one", K(affected_rows), K(ret));
    }
  }
  // mark delete in __all_constraint_column_history
  if (OB_SUCC(ret)) {
    const int64_t is_deleted = 1;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    const ObConstraint *constraint =
        const_cast<ObTableSchema &>(new_table_schema).get_constraint(
                                                      orig_constraint.get_constraint_id());
    for (ObConstraint::const_cst_col_iterator cst_col_iter = constraint->cst_col_begin();
         OB_SUCC(ret) && (cst_col_iter != constraint->cst_col_end());
         ++cst_col_iter) {
      dml.reset();
      if (OB_FAIL(dml.add_pk_column("tenant_id",
                  ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("table_id",
             ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                  orig_constraint.get_table_id())))
          || OB_FAIL(dml.add_pk_column("constraint_id", orig_constraint.get_constraint_id()))
          || OB_FAIL(dml.add_column("column_id", *cst_col_iter))
          || OB_FAIL(dml.add_column("schema_version", new_schema_version))
          || OB_FAIL(dml.add_column("is_deleted", is_deleted))
          || OB_FAIL(dml.add_gmt_create())
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("dml add constraint column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME,
                                          dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }

  // log delete constraint
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = new_table_schema.get_tenant_id();
    opt.database_id_ = new_table_schema.get_database_id();
    opt.tablegroup_id_ = new_table_schema.get_tablegroup_id();
    opt.table_id_ = new_table_schema.get_table_id();
    opt.op_type_ = OB_DDL_DROP_CONSTRAINT;
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_single_column(
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    const ObTableSchema &new_table_schema,
    const ObColumnSchemaV2 &orig_column_schema,
    const bool record_ddl_operation)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t table_id = new_table_schema.get_table_id();
  const uint64_t column_id = orig_column_schema.get_column_id();

  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(new_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(new_table_schema));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, table_id)))
      || OB_FAIL(dml.add_pk_column("column_id", column_id))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                            OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(affected_rows), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_column_stat(sql_client, tenant_id, table_id, column_id))) {
      LOG_WARN("fail to delete column stat", K(ret));
    }
  }

  ObSqlString sql;
  int64_t affected_rows = 0;
  // xiyu: TODO
  // mark delete in __all_column_history
  if (OB_SUCC(ret)) {
    const int64_t is_deleted = 1;
    if (OB_FAIL(sql.assign_fmt(
        "INSERT INTO %s (TENANT_ID, TABLE_ID, COLUMN_ID, SCHEMA_VERSION, IS_DELETED) values "
        "(%lu, %lu, %lu, %ld, %ld)",
        OB_ALL_COLUMN_HISTORY_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, orig_column_schema.get_table_id()),
        orig_column_schema.get_column_id(),
        new_schema_version,
        is_deleted))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affect_rows expected to be one", K(affected_rows), K(ret));
    }
  }

  // log delete column
  if (OB_SUCC(ret) && record_ddl_operation) {
    ObSchemaOperation opt;
    opt.tenant_id_ = new_table_schema.get_tenant_id();
    opt.database_id_ = new_table_schema.get_database_id();
    opt.tablegroup_id_ = new_table_schema.get_tablegroup_id();
    opt.table_id_ = new_table_schema.get_table_id();
    opt.op_type_ = OB_DDL_DROP_COLUMN;
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }

  return ret;
}

bool ObTableSqlService::table_need_sync_schema_version(const ObTableSchema &table)
{
  return (table.is_index_table() || table.is_mlog_table()
          || table.is_aux_vp_table() || table.is_aux_lob_table());
}

int ObTableSqlService::inner_create_sys_table(ObTableSchema &table,
    ObSchemaOperation &opt,
    const bool need_sync_schema_version,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard time_guard("inner_create_sys_table", 1_ms);
  const bool update_object_status_ignore_version = false;
  const bool only_history = false;
  // add __all_table/__all_column with its history, __all_ddl_operation/__all_core_table
  if (!is_sys_table(table.get_table_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this function should only be called with sys table", KR(ret), K(table), K(lbt()));
  } else if (OB_FAIL(add_table(sql_client, table, update_object_status_ignore_version, only_history))) {
    LOG_WARN("failed to add table", KR(ret));
  } else if (FALSE_IT(time_guard.click("add_table"))) {
  } else if (OB_FAIL(add_columns(sql_client, table))) {
    LOG_WARN("failed to add columns", KR(ret));
  } else if (FALSE_IT(time_guard.click("add_columns"))) {
  } else if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
    LOG_WARN("failed to add ddl operation", KR(ret), K(opt));
  } else if (FALSE_IT(time_guard.click("add_ddl_operation"))) {
  } else if (need_sync_schema_version && table_need_sync_schema_version(table)) {
    if (OB_FAIL(update_data_table_schema_version(sql_client, table.get_tenant_id(),
            table.get_data_table_id(), table.get_in_offline_ddl_white_list()))) {
      LOG_WARN("fail to update schema_version", KR(ret));
    }
    time_guard.click("sync_schema_version");
  }
  return ret;
}

int ObTableSqlService::batch_create_table(ObIArray<ObTableSchema> &tables,
    common::ObISQLClient &sql_client,
    const common::ObString *ddl_stmt_str,
    const bool sync_schema_version_for_last_table,
    const bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard time_guard("batch_create_table", 10_ms);
  int64_t start_usec = ObTimeUtility::current_time();
  int64_t end_usec = 0;
  int64_t cost_usec = 0;
  if (tables.empty()) {
  } else {
    ObDMLSqlSplicer ddl_operation_dml;
    ObDMLSqlSplicer ddl_id_dml;
    const uint64_t tenant_id = tables.at(0).get_tenant_id();
    const bool has_sys_table = is_sys_table(tables.at(0).get_table_id());
    const bool update_object_status_ignore_version = false;
    // generate dmls
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); i++) {
      ObTableSchema &table = tables.at(i);
      int64_t tmp = 0;
      if (OB_FAIL(table.check_valid(true/*for_create*/))) {
        LOG_WARN("invalid create table argument, ", KR(ret), K(table));
      } else if (table.get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id not equal", KR(ret), K(tenant_id), K(table));
      } else if (OB_FAIL(check_ddl_allowed(table))) {
        LOG_WARN("check ddl allowd failed", KR(ret), K(table));
      } else if (table.get_storage_cache_policy().empty() && set_default_storage_cache_policy_for_table(tenant_id, table)) {
        LOG_WARN("failed to set default storage cache policy for table", K(ret));
      } else if (table.is_view_table() && !table.is_sys_view()
          && !table.is_force_view() && table.get_column_count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get wrong view schema", KR(ret), K(table));
      } else if (has_sys_table != is_sys_table(tables.at(i).get_table_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys table should not be created with user table", KR(ret), K(table));
      } else if (table.is_force_view()
          && table.get_column_count() <= 0
          && FALSE_IT(table.set_object_status(ObObjectStatus::INVALID))) {
      } else if (table.is_view_table() && table.get_column_count() > 0
          && FALSE_IT(table.set_view_column_filled_flag(ObViewColumnFilledFlag::FILLED))) {
      }
      // add ddl operation
      ObSchemaOperation opt;
      opt.tenant_id_ = tenant_id;
      opt.database_id_ = table.get_database_id();
      opt.tablegroup_id_ = table.get_tablegroup_id();
      opt.table_id_ = table.get_table_id();
      if (is_truncate_table) {
        opt.op_type_ = OB_DDL_TRUNCATE_TABLE_CREATE;
      } else {
        if (table.is_index_table()) {
          opt.op_type_ = table.is_global_index_table() ? OB_DDL_CREATE_GLOBAL_INDEX : OB_DDL_CREATE_INDEX;
        } else if (table.is_view_table()){
          opt.op_type_ = OB_DDL_CREATE_VIEW;
        } else {
          opt.op_type_ = OB_DDL_CREATE_TABLE;
        }
      }
      opt.schema_version_ = table.get_schema_version();
      opt.ddl_stmt_str_ = (i == 0 && OB_NOT_NULL(ddl_stmt_str)) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(ret)) {
      } else if (is_sys_table(table.get_table_id())) {
        if (OB_FAIL(inner_create_sys_table(table, opt,
                (sync_schema_version_for_last_table && i + 1 == tables.count()), sql_client))) {
          LOG_WARN("failed to inner create core table", KR(ret));
        }
      } else {
        // for user table
        // add ddl operation
        if (OB_FAIL(log_operation_dml(opt, ddl_operation_dml, ddl_id_dml))) {
          LOG_WARN("log operation failed", K(opt), KR(ret));
        }
      }
    }
    time_guard.click("log_operation");
    if (OB_FAIL(ret) || has_sys_table) {
    } else if (OB_FAIL(batch_add_sequence_for_create_table(sql_client, tables))) {
      LOG_WARN("failed to batch add sequence for create table", KR(ret), K(tables));
    } else if (FALSE_IT(time_guard.click("insert_auto_increment"))) {
    } else if (OB_FAIL(batch_add_table_for_create_table(sql_client, tables))) {
      LOG_WARN("failed to batch add table for create table", KR(ret), K(tables));
    } else if (FALSE_IT(time_guard.click("insert_all_table"))) {
    } else if (OB_FAIL(batch_add_columns_for_create_table(sql_client, tables))) {
      LOG_WARN("failed to batch add columns for create table", KR(ret), K(tables));
    } else if (FALSE_IT(time_guard.click("insert_all_column"))) {
    } else if (OB_FAIL(batch_add_column_groups_for_create_table(sql_client, tables))) {
      LOG_WARN("failed to batch add column groups for create table", KR(ret), K(tables));
    } else if (FALSE_IT(time_guard.click("insert_all_column_group"))) {
    } else if (OB_FAIL(batch_add_constraints_for_create_table(sql_client, tables))) {
      LOG_WARN("failed to batch add constraints for create table", KR(ret), K(tables));
    } else if (FALSE_IT(time_guard.click("insert_all_cst"))) {
    } else if (OB_FAIL(batch_add_table_part_info(sql_client, tables))) {
      LOG_WARN("failed to add table part info", KR(ret));
    } else if (FALSE_IT(time_guard.click("add_table_part_info"))) {
    } else if (OB_FAIL(exec_dml(sql_client, tenant_id, OB_ALL_DDL_OPERATION_TNAME, ddl_operation_dml,
            tables.count()))) {
      LOG_WARN("log operation failed", KR(ret), K(tenant_id));
    } else if (FALSE_IT(time_guard.click("insert_all_ddl_operation"))) {
    } else if (OB_FAIL(exec_dml(sql_client, tenant_id, OB_ALL_DDL_ID_TNAME, ddl_id_dml))) {
      LOG_WARN("log operation failed", KR(ret), K(tenant_id));
    } else if (FALSE_IT(time_guard.click("insert_all_ddl_id"))) {
    } else {
      ObTableSchema &last_table = tables.at(tables.count() - 1);
      for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); i++) {
        if (is_inner_table(tables.at(i).get_table_id())) {
        } else if (OB_FAIL(add_foreign_key(sql_client, tables.at(i), false/*only_history*/))) {
          LOG_WARN("failed to add foreign key", KR(ret), K(tables.at(i)));
        }
      }
      time_guard.click("add_foreign_key");
      if (OB_FAIL(ret)) {
      } else if (sync_schema_version_for_last_table && table_need_sync_schema_version(last_table)) {
        if (OB_FAIL(update_data_table_schema_version(sql_client, tenant_id,
            last_table.get_data_table_id(), last_table.get_in_offline_ddl_white_list()))) {
          LOG_WARN("fail to update schema_version", KR(ret));
        }
        time_guard.click("update_data_table_schema_version");
      }
    }
  }
  return ret;
}

int ObTableSqlService::create_table(ObTableSchema &table,
                                    ObISQLClient &sql_client,
                                    const ObString *ddl_stmt_str/*=NULL*/,
                                    const bool need_sync_schema_version,
                                    const bool is_truncate_table /*false*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableSchema, 1> tables;
  if (OB_FAIL(tables.push_back(table))) {
    LOG_WARN("failed to push_back tables", KR(ret), K(table));
  } else if (OB_FAIL(batch_create_table(tables, sql_client, ddl_stmt_str, need_sync_schema_version, is_truncate_table))) {
    LOG_WARN("failed to batch create table", KR(ret), K(tables), K(ddl_stmt_str),
        K(need_sync_schema_version), K(is_truncate_table));
  }
  return ret;
}

int ObTableSqlService::update_index_status(
    const ObTableSchema &data_table_schema,
    const uint64_t index_table_id,
    const ObIndexStatus status,
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTableSchema index_schema;
  const uint64_t data_table_id = data_table_schema.get_table_id();
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_ddl_allowed(data_table_schema))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(data_table_schema));
  } else if (OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id
      || status <= INDEX_STATUS_NOT_FOUND || status >= INDEX_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_table_id), K(index_table_id), K(status));
  } else if (OB_FAIL(update_data_table_schema_version(sql_client, tenant_id, data_table_id,
                     data_table_schema.get_in_offline_ddl_white_list()))) {
    LOG_WARN("update data table schema version failed", K(ret));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, index_table_id)))
        || OB_FAIL(dml.add_column("schema_version", new_schema_version))
        || OB_FAIL(dml.add_column("index_status", status))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    } else {
      int64_t affected_rows = 0;
      const char *table_name = NULL;
      if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
        LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
      } else if (OB_FAIL(exec_update(sql_client, tenant_id, index_table_id, table_name, dml, affected_rows))) {
        LOG_WARN("exec update failed", K(ret));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(affected_rows), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;
    const bool update_object_status_ignore_version = false;
    if (OB_FAIL(schema_service_.get_table_schema_from_inner_table(schema_status, index_table_id, sql_client, index_schema))) {
      LOG_WARN("get_table_schema failed", K(index_table_id), K(ret));
    } else {
      const bool only_history = true;
      index_schema.set_index_status(status);
      index_schema.set_schema_version(new_schema_version);
      index_schema.set_in_offline_ddl_white_list(data_table_schema.get_in_offline_ddl_white_list());
      if (OB_FAIL(add_table(sql_client, index_schema, update_object_status_ignore_version, only_history))) {
        LOG_WARN("add_table failed", K(index_schema), K(only_history), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = index_schema.get_database_id();
    opt.tablegroup_id_ = index_schema.get_tablegroup_id();
    opt.table_id_ = index_table_id;
    opt.op_type_ = index_schema.is_global_index_table() ? OB_DDL_MODIFY_GLOBAL_INDEX_STATUS : OB_DDL_MODIFY_INDEX_STATUS;
    opt.schema_version_ = new_schema_version;
    opt.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}


int ObTableSqlService::update_index_type(const ObTableSchema &data_table_schema,
                                         const uint64_t index_table_id,
                                         const ObIndexType index_type,
                                         const int64_t new_schema_version,
                                         const common::ObString *ddl_stmt_str,
                                         common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const char *table_name = NULL;

  if (OB_INVALID_ID == index_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_table_id));
  } else if (OB_FAIL(check_ddl_allowed(data_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(data_table_schema));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, tenant_id)))
            || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                    exec_tenant_id, index_table_id)))
            || OB_FAIL(dml.add_column("schema_version", new_schema_version))
            || OB_FAIL(dml.add_column("index_type", index_type))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
    LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
  } else if (OB_FAIL(exec_update(sql_client, tenant_id, index_table_id, table_name, dml, affected_rows))) {
    LOG_WARN("exec update failed", K(ret));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(affected_rows), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObTableSchema index_schema;
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;
    if (OB_FAIL(schema_service_.get_table_schema_from_inner_table(schema_status, index_table_id, sql_client, index_schema))) {
      LOG_WARN("get_table_schema failed", K(index_table_id), K(ret));
    } else {
      const bool update_object_status_ignore_version = false;
      const bool only_history = true;
      index_schema.set_index_type(index_type);
      index_schema.set_schema_version(new_schema_version);
      index_schema.set_in_offline_ddl_white_list(data_table_schema.get_in_offline_ddl_white_list());
      if (OB_FAIL(add_table(sql_client, index_schema, update_object_status_ignore_version, only_history))) {
        LOG_WARN("add_table failed", K(index_schema), K(only_history), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = 0;
    opt.tablegroup_id_ = 0;
    opt.table_id_ = index_table_id;
    opt.op_type_ = OB_DDL_MODIFY_INDEX_TYPE;
    opt.schema_version_ = new_schema_version;
    opt.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_mlog_status(const ObTableSchema &data_table_schema,
                                          const uint64_t mlog_table_id,
                                          const char *new_name,
                                          const int64_t new_schema_version,
                                          common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const char *table_name = NULL;

  if (OB_FAIL(check_ddl_allowed(data_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(data_table_schema));
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
    LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
  } else if (OB_FAIL(dml.add_pk_column(
                 "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                       exec_tenant_id, mlog_table_id))) ||
             OB_FAIL(dml.add_column("schema_version", new_schema_version)) ||
             OB_FAIL(dml.add_column("table_name", new_name))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec_update(sql_client, tenant_id, mlog_table_id, table_name, dml,
                                 affected_rows))) {
    LOG_WARN("exec update failed", K(ret));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(affected_rows), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObTableSchema index_schema;
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;
    if (OB_FAIL(schema_service_.get_table_schema_from_inner_table(schema_status, mlog_table_id,
                                                                  sql_client, index_schema))) {
      LOG_WARN("get_table_schema failed", K(mlog_table_id), K(ret));
    } else {
      const bool update_object_status_ignore_version = false;
      const bool only_history = true;
      index_schema.set_table_name(new_name);
      index_schema.set_schema_version(new_schema_version);
      index_schema.set_in_offline_ddl_white_list(data_table_schema.get_in_offline_ddl_white_list());
      if (OB_FAIL(add_table(sql_client, index_schema, update_object_status_ignore_version,
                            only_history))) {
        LOG_WARN("add_table failed", K(index_schema), K(only_history), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = 0;
    opt.tablegroup_id_ = 0;
    opt.table_id_ = mlog_table_id;
    opt.op_type_ = OB_DDL_MODIFY_MLOG_STATUS;
    opt.schema_version_ = new_schema_version;
    opt.ddl_stmt_str_ = ObString();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::update_mview_status(
    const ObTableSchema &mview_table_schema,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = mview_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  share::schema::ObTableMode table_mode_struct = mview_table_schema.get_table_mode_struct();
  uint64_t mview_table_id = mview_table_schema.get_table_id();
  int64_t new_schema_version = mview_table_schema.get_schema_version();
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, mview_table_id)))
      || OB_FAIL(dml.add_column("schema_version", new_schema_version))
      || OB_FAIL(dml.add_column("table_mode", table_mode_struct.mode_))) {
    LOG_WARN("failed to add column", KR(ret), K(exec_tenant_id), K(tenant_id));
  } else {
    int64_t affected_rows = 0;
    const char *table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client,
        tenant_id, mview_table_id, table_name, dml, affected_rows))) {
      LOG_WARN("failed to exec update", KR(ret),
          K(tenant_id), K(mview_table_id), K(table_name));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(affected_rows));
    }
  }

  if (OB_SUCC(ret)) {
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;
    const bool update_object_status_ignore_version = false;
    SMART_VAR(ObTableSchema, mview_schema) {
      if (OB_FAIL(schema_service_.get_table_schema_from_inner_table(
          schema_status, mview_table_id, sql_client, mview_schema))) {
        LOG_WARN("failed to get table schema for inner table", KR(ret), K(mview_table_id));
      } else {
        const bool only_history = true;
        mview_schema.set_mv_available(ObTableMode::get_mv_available_flag(table_mode_struct.mode_));
        mview_schema.set_schema_version(new_schema_version);
        mview_schema.set_in_offline_ddl_white_list(mview_table_schema.get_in_offline_ddl_white_list());
        if (OB_FAIL(add_table(sql_client,
                              mview_schema,
                              update_object_status_ignore_version,
                              only_history))) {
          LOG_WARN("failed to add_table", KR(ret), K(mview_schema), K(only_history));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = 0;
    opt.tablegroup_id_ = 0;
    opt.table_id_ = mview_table_id;
    opt.op_type_ = OB_DDL_MODIFY_MATERIALIZED_VIEW_STATUS;
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(ret), K(opt));
    }
  }
  return ret;
}

int ObTableSqlService::update_mview_reference_table_status(
    const ObTableSchema &table_schema,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  share::schema::ObTableMode table_mode_struct = table_schema.get_table_mode_struct();
  uint64_t table_id = table_schema.get_table_id();
  int64_t new_schema_version = table_schema.get_schema_version();
  int64_t mv_mode = table_schema.get_mv_mode();
  ObDMLSqlSplicer dml;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column(
                 "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column(
                 "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
             OB_FAIL(dml.add_column("schema_version", new_schema_version)) ||
             OB_FAIL(dml.add_column("table_mode", table_mode_struct.mode_)) ||
             (data_version >= DATA_VERSION_4_3_4_0 &&
              OB_FAIL(dml.add_column("mv_mode", mv_mode)))) {
    LOG_WARN("failed to add column", KR(ret), K(exec_tenant_id), K(tenant_id));
  } else {
    int64_t affected_rows = 0;
    const char *table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client,
        tenant_id, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("failed to exec update", KR(ret),
          K(tenant_id), K(table_id), K(table_name));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(affected_rows));
    }
  }

  if (OB_SUCC(ret)) {
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;
    const bool update_object_status_ignore_version = false;
    SMART_VAR(ObTableSchema, new_schema) {
      if (OB_FAIL(schema_service_.get_table_schema_from_inner_table(
          schema_status, table_id, sql_client, new_schema))) {
        LOG_WARN("failed to get table schema for inner table", KR(ret), K(table_id));
      } else {
        const bool only_history = true;
        new_schema.set_table_referenced_by_mv(ObTableMode::get_table_referenced_by_mv_flag(table_mode_struct.mode_));
        new_schema.set_mv_mode(mv_mode);
        new_schema.set_schema_version(new_schema_version);
        new_schema.set_in_offline_ddl_white_list(table_schema.get_in_offline_ddl_white_list());
        if (OB_FAIL(add_table(sql_client,
                              new_schema,
                              update_object_status_ignore_version,
                              only_history))) {
          LOG_WARN("failed to add_table", KR(ret), K(new_schema), K(only_history));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = 0;
    opt.tablegroup_id_ = 0;
    opt.table_id_ = table_id;
    opt.op_type_ = OB_DDL_MODIFY_MVIEW_REFERENCE_TABLE_STATUS;
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(ret), K(opt));
    }
  }
  return ret;
}

int ObTableSqlService::add_transition_point_val(ObDMLSqlSplicer &dml,
                                                const ObTableSchema &table) {
  int ret = OB_SUCCESS;
  const ObRowkey &transition_point = table.get_transition_point();
  char *transition_point_str = NULL;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  transition_point_str = static_cast<char *>(allocator.alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH));
  if (OB_ISNULL(transition_point_str)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("transition point is null", K(ret), K(transition_point_str));
  } else {
    MEMSET(transition_point_str, 0, OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    int64_t pos = 0;
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0);
    bool is_oracle_mode = false;
    if (OB_FAIL(table.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle compat mode", KR(ret), K(table));
    } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(table.get_tenant_id(), tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(table.get_tenant_id()));
    } else if (transition_point.is_valid() && OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
               is_oracle_mode, transition_point, transition_point_str,
               OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos, false, &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), K(transition_point), K(ret));
    } else if (OB_FAIL(dml.add_column("transition_point",
                                      ObHexEscapeSqlStr(ObString(pos, transition_point_str))))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (FALSE_IT(pos = 0)) {
    } else if (transition_point.is_valid() && OB_FAIL(ObPartitionUtils::convert_rowkey_to_hex(
        transition_point, transition_point_str,
        OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos))) {
      LOG_WARN("Failed to convert rowkey to hex", K(ret));
    } else if (OB_FAIL(dml.add_column("b_transition_point", ObString(pos, transition_point_str)))) {
      LOG_WARN("Failed to add column b_transition_point", K(ret));
    } else {
      LOG_DEBUG("transition point info", "transition_point", ObString(pos, transition_point_str).ptr(), K(pos));
    } //do nothing
  }
  return ret;
}

int ObTableSqlService::add_interval_range_val(ObDMLSqlSplicer &dml,
                                                const ObTableSchema &table) {
  int ret = OB_SUCCESS;
  const ObRowkey &interval_range = table.get_interval_range();
  char *interval_range_str = NULL;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  interval_range_str = static_cast<char *>(allocator.alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH));
  if (OB_ISNULL(interval_range_str)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("interval range is null", K(ret), K(interval_range_str));
  } else {
    MEMSET(interval_range_str, 0, OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    int64_t pos = 0;
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0);
    bool is_oracle_mode = false;
    if (OB_FAIL(table.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle compat mode", KR(ret), K(table));
    } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(table.get_tenant_id(), tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(table.get_tenant_id()));
    } else if (interval_range.is_valid() && OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
            is_oracle_mode, interval_range, interval_range_str,
            OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos, false, &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), K(interval_range), K(ret));
    } else if (OB_FAIL(
        dml.add_column("interval_range", ObHexEscapeSqlStr(ObString(pos, interval_range_str))))) {
      LOG_WARN("dml add part info failed", K(ret));
    } else if (FALSE_IT(pos = 0)) {
    } else if (interval_range.is_valid() && OB_FAIL(ObPartitionUtils::convert_rowkey_to_hex(
            interval_range, interval_range_str,
            OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos))) {
      LOG_WARN("Failed to convert rowkey to hex", K(ret));
    } else if (OB_FAIL(dml.add_column("b_interval_range", ObString(pos, interval_range_str)))) {
      LOG_WARN("Failed to add column b_interval_range", K(ret));
    } else {
      LOG_DEBUG(
          "interval range info",
          "interval_range",
          ObString(pos, interval_range_str).ptr(),
          K(pos));
    } //do nothing
  }
  return ret;
}

int ObTableSqlService::gen_mview_dml(
    const uint64_t exec_tenant_id,
    const ObTableSchema &table,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(table.get_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("mview is not support before 4.3", KR(ret), K(table));
  } else {
    const ObViewSchema &view_schema = table.get_view_schema();
    const obrpc::ObMVAdditionalInfo *mv_additional_info = view_schema.get_mv_additional_info();
    const obrpc::ObMVRefreshInfo *mv_refresh_info = nullptr;

    if (OB_ISNULL(mv_additional_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv_additional_info is null", KR(ret));
    } else if (OB_ISNULL(mv_refresh_info = &(mv_additional_info->mv_refresh_info_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv_refresh_info is null", KR(ret));
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table.get_tenant_id())))
        || OB_FAIL(dml.add_pk_column("mview_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, table.get_table_id())))
        || OB_FAIL(dml.add_column("refresh_mode", mv_refresh_info->refresh_mode_))
        || OB_FAIL(dml.add_column("refresh_method", mv_refresh_info->refresh_method_))
        || OB_FAIL(dml.add_column("build_mode", ObMViewBuildMode::IMMEDIATE))
        || OB_FAIL(dml.add_column("last_refresh_scn", 0))) {
      LOG_WARN("add column failed", KR(ret));
    }

    if (OB_SUCC(ret) && mv_refresh_info->start_time_.is_datetime()) {
      if (OB_FAIL(dml.add_time_column("refresh_start", mv_refresh_info->start_time_.get_datetime()))) {
        LOG_WARN("add column failed", KR(ret));
      }
    }
    if (OB_SUCC(ret) && mv_refresh_info->next_time_expr_.length() != 0) {
      if (OB_FAIL(dml.add_column("refresh_next", ObHexEscapeSqlStr(mv_refresh_info->next_time_expr_)))) {
        LOG_WARN("add column failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableSqlService::gen_table_dml_without_check(
    const uint64_t exec_tenant_id,
    const ObTableSchema &table,
    const bool update_object_status_ignore_version,
    const int64_t data_version,
    share::ObDMLSqlSplicer &dml,
    const bool used_for_unittest /*= false*/)
{
  int ret = OB_SUCCESS;
  ObString empty_str("");
  const ObPartitionOption &part_option = table.get_part_option();
  const ObPartitionOption &sub_part_option = table.get_sub_part_option();
  const char *expire_info = table.get_expire_info().length() <= 0 ?
    "" : table.get_expire_info().ptr();
  const char *part_func_expr = part_option.get_part_func_expr_str().length() <= 0 ?
    "" : part_option.get_part_func_expr_str().ptr();
  const char *sub_part_func_expr = sub_part_option.get_part_func_expr_str().length() <= 0 ?
    "" : sub_part_option.get_part_func_expr_str().ptr();
  const char *encryption = table.get_encryption_str().empty() ?
    "" : table.get_encryption_str().ptr();
  const int64_t INVALID_REPLICA_NUM = -1;
  const int64_t part_num = part_option.get_part_num();
  const int64_t sub_part_num = PARTITION_LEVEL_TWO == table.get_part_level()
    && table.has_sub_part_template_def() ?
    sub_part_option.get_part_num() : 0;
  const char *ttl_definition = table.get_ttl_definition().empty() ?
    "" : table.get_ttl_definition().ptr();
  const char *kv_attributes = table.get_kv_attributes().empty() ?
    "" : table.get_kv_attributes().ptr();
  const char *storage_cache_policy = table.get_storage_cache_policy().empty() ?
      OB_DEFAULT_STORAGE_CACHE_POLICY_STR : table.get_storage_cache_policy().ptr();
  ObString index_params = table.get_index_params().empty() ? empty_str : table.get_index_params();
  const ObString parser_properties = table.get_parser_property_str().empty() ? empty_str : table.get_parser_property_str();
  const char *dynamic_partition_policy = table.get_dynamic_partition_policy().empty() ?
      "" : table.get_dynamic_partition_policy().ptr();
  const char *semistruct_properties = table.get_semistruct_properties().empty() ?
      "" : table.get_semistruct_properties().ptr();
  ObString local_session_var;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  if (table.is_materialized_view() && data_version >= DATA_VERSION_4_3_3_0
      && OB_FAIL(table.get_local_session_var().gen_local_session_var_str(allocator, local_session_var))) {
    LOG_WARN("fail to gen local session var str", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
            exec_tenant_id, table.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
            exec_tenant_id, table.get_table_id())))
      || OB_FAIL(dml.add_column("table_name", ObHexEscapeSqlStr(table.get_table_name())))
      || OB_FAIL(dml.add_column("database_id", ObSchemaUtils::get_extract_schema_id(
            exec_tenant_id, table.get_database_id())))
      || OB_FAIL(dml.add_column("table_type", table.get_table_type()))
      || OB_FAIL(dml.add_column("load_type", table.get_load_type()))
      || OB_FAIL(dml.add_column("def_type", table.get_def_type()))
      || OB_FAIL(dml.add_column("rowkey_column_num", table.get_rowkey_column_num()))
      || OB_FAIL(dml.add_column("index_column_num", table.get_index_column_num()))
      || OB_FAIL(dml.add_column("max_used_column_id", table.get_max_used_column_id()))
      || OB_FAIL(dml.add_column("session_id", table.get_session_id()))
      || OB_FAIL(dml.add_column("sess_active_time", table.get_sess_active_time()))
      //|| OB_FAIL(dml.add_column("create_host", table.get_create_host()))
      || OB_FAIL(dml.add_column("tablet_size", table.get_tablet_size()))
      || OB_FAIL(dml.add_column("pctfree", table.get_pctfree()))
      || OB_FAIL(dml.add_column("autoinc_column_id", table.get_autoinc_column_id()))
      || OB_FAIL(dml.add_column("auto_increment", share::ObRealUInt64(table.get_auto_increment())))
      || OB_FAIL(dml.add_column("read_only", table.is_read_only()))
      || OB_FAIL(dml.add_column("rowkey_split_pos", table.get_rowkey_split_pos()))
      || OB_FAIL(dml.add_column("compress_func_name", ObHexEscapeSqlStr(table.get_compress_func_name())))
      || OB_FAIL(dml.add_column("expire_condition", ObHexEscapeSqlStr(expire_info)))
      || OB_FAIL(dml.add_column("is_use_bloomfilter", table.is_use_bloomfilter()))
      || OB_FAIL(dml.add_column("index_attributes_set", table.get_index_attributes_set()))
      || OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(table.get_comment())))
      || OB_FAIL(dml.add_column("block_size", table.get_block_size()))
      || OB_FAIL(dml.add_column("collation_type", table.get_collation_type()))
      || OB_FAIL(dml.add_column("data_table_id", ObSchemaUtils::get_extract_schema_id(
              exec_tenant_id, table.get_data_table_id())))
      || OB_FAIL(dml.add_column("index_status", table.get_index_status()))
      || OB_FAIL(dml.add_column("tablegroup_id", ObSchemaUtils::get_extract_schema_id(
              exec_tenant_id, table.get_tablegroup_id())))
      || OB_FAIL(dml.add_column("progressive_merge_num", table.get_progressive_merge_num()))
      || OB_FAIL(dml.add_column("index_type", table.get_index_type()))
      || OB_FAIL(dml.add_column("index_using_type", table.get_index_using_type()))
      || OB_FAIL(dml.add_column("part_level", table.get_part_level()))
      || OB_FAIL(dml.add_column("part_func_type", part_option.get_part_func_type()))
      || OB_FAIL(dml.add_column("part_func_expr", ObHexEscapeSqlStr(part_func_expr)))
      || OB_FAIL(dml.add_column("part_num", part_num))
      || OB_FAIL(dml.add_column("sub_part_func_type", sub_part_option.get_part_func_type()))
      || OB_FAIL(dml.add_column("sub_part_func_expr", ObHexEscapeSqlStr(sub_part_func_expr)))
      || OB_FAIL(dml.add_column("sub_part_num", sub_part_num))
      || (OB_LIKELY(!used_for_unittest) && OB_FAIL(dml.add_column("schema_version", table.get_schema_version())))
      || OB_FAIL(dml.add_column("view_definition", ObHexEscapeSqlStr(table.get_view_schema().get_view_definition())))
      || OB_FAIL(dml.add_column("view_check_option", table.get_view_schema().get_view_check_option()))
      || OB_FAIL(dml.add_column("view_is_updatable", table.get_view_schema().get_view_is_updatable()))
      || OB_FAIL(dml.add_column("parser_name", ObHexEscapeSqlStr(table.get_parser_name_str())))
      || OB_FAIL(dml.add_column("partition_status", table.get_partition_status()))
      || OB_FAIL(dml.add_column("partition_schema_version", table.get_partition_schema_version()))
      || OB_FAIL(dml.add_column("pk_comment", ObHexEscapeSqlStr(table.get_pk_comment())))
      || OB_FAIL(dml.add_column("row_store_type",
            ObHexEscapeSqlStr(ObStoreFormat::get_row_store_name(table.get_row_store_type()))))
      || OB_FAIL(dml.add_column("store_format",
            ObHexEscapeSqlStr(ObStoreFormat::get_store_format_name(table.get_store_format()))))
      || OB_FAIL(dml.add_column("duplicate_scope", table.get_duplicate_scope()))
      || OB_FAIL(dml.add_column("progressive_merge_round", table.get_progressive_merge_round()))
      || OB_FAIL(dml.add_column("storage_format_version", table.get_storage_format_version()))
      || OB_FAIL(dml.add_column("table_mode", table.get_table_mode()))
      || OB_FAIL(dml.add_column("encryption", ObHexEscapeSqlStr(encryption)))
      || OB_FAIL(dml.add_column("tablespace_id", ObSchemaUtils::get_extract_schema_id(
              exec_tenant_id, table.get_tablespace_id())))
      // To avoid compatibility problems (such as error while upgrade virtual schema) in upgrade post stage,
      // cluster version judgement is needed if columns are added in upgrade post stage.
      || OB_FAIL(dml.add_column("sub_part_template_flags", table.get_sub_part_template_flags()))
      || OB_FAIL(dml.add_column("dop", table.get_dop()))
      || OB_FAIL(dml.add_column("character_set_client", table.get_view_schema().get_character_set_client()))
      || OB_FAIL(dml.add_column("collation_connection", table.get_view_schema().get_collation_connection()))
      || OB_FAIL(dml.add_column("auto_part", table.get_part_option().get_auto_part()))
      || OB_FAIL(dml.add_column("auto_part_size", table.get_part_option().get_auto_part_size()))
      || OB_FAIL(dml.add_column("association_table_id", ObSchemaUtils::get_extract_schema_id(
              exec_tenant_id, table.get_association_table_id())))
      || OB_FAIL(dml.add_column("define_user_id", ObSchemaUtils::get_extract_schema_id(
              exec_tenant_id, table.get_define_user_id())))
      || OB_FAIL(dml.add_column("max_dependency_version", table.get_max_dependency_version()))
      || (table.is_interval_part() && OB_FAIL(add_transition_point_val(dml, table)))
      || (table.is_interval_part() && OB_FAIL(add_interval_range_val(dml, table)))
      || (OB_FAIL(dml.add_column("tablet_id", table.get_tablet_id().id())))
      || ((data_version >= DATA_VERSION_4_1_0_0 || update_object_status_ignore_version)
          && OB_FAIL(dml.add_column("object_status", static_cast<int64_t> (table.get_object_status()))))
      || (data_version >= DATA_VERSION_4_1_0_0
          && OB_FAIL(dml.add_column("table_flags", table.get_table_flags())))
      || (data_version >= DATA_VERSION_4_1_0_0
          && OB_FAIL(dml.add_column("truncate_version", table.get_truncate_version())))
      || (data_version >= DATA_VERSION_4_2_0_0
          && OB_FAIL(dml.add_column("external_file_location", ObHexEscapeSqlStr(table.get_external_file_location()))))
      || (data_version >= DATA_VERSION_4_2_0_0
          && OB_FAIL(dml.add_column("external_file_location_access_info", ObHexEscapeSqlStr(table.get_external_file_location_access_info()))))
      || (data_version >= DATA_VERSION_4_2_0_0
          && OB_FAIL(dml.add_column("external_file_format", ObHexEscapeSqlStr(table.get_external_file_format()))))
      || (data_version >= DATA_VERSION_4_2_0_0
          && OB_FAIL(dml.add_column("external_file_pattern", ObHexEscapeSqlStr(table.get_external_file_pattern()))))
      || (data_version >= DATA_VERSION_4_2_1_0
          && OB_FAIL(dml.add_column("ttl_definition", ObHexEscapeSqlStr(ttl_definition))))
      || (data_version >= DATA_VERSION_4_2_1_0
          && OB_FAIL(dml.add_column("kv_attributes", ObHexEscapeSqlStr(kv_attributes))))
      || (data_version >= DATA_VERSION_4_2_1_0
          && OB_FAIL(dml.add_column("name_generated_type", table.get_name_generated_type())))
      || (data_version >= DATA_VERSION_4_2_1_2
          && OB_FAIL(dml.add_column("lob_inrow_threshold", table.get_lob_inrow_threshold())))
      || (data_version >= DATA_VERSION_4_3_0_0
          && OB_FAIL(dml.add_column("max_used_column_group_id", table.get_max_used_column_group_id())))
      || (data_version >= DATA_VERSION_4_3_0_0
          && OB_FAIL(dml.add_column("column_store", table.is_column_store_supported())))
      || ((data_version >= DATA_VERSION_4_3_2_0 || (data_version < DATA_VERSION_4_3_0_0 && data_version >= MOCK_DATA_VERSION_4_2_3_0))
          && OB_FAIL(dml.add_column("auto_increment_cache_size", table.get_auto_increment_cache_size())))
      || ((data_version >= DATA_VERSION_4_3_4_0)
          && OB_FAIL(dml.add_column("duplicate_read_consistency", table.get_duplicate_read_consistency())))
      || ((data_version >= DATA_VERSION_4_3_4_0)
          && OB_FAIL(dml.add_column("mv_mode", table.get_mv_mode())))
      || (data_version >= DATA_VERSION_4_3_2_1 &&
          OB_FAIL(dml.add_column("external_properties", ObHexEscapeSqlStr(table.get_external_properties()))))
      || (data_version >= DATA_VERSION_4_3_3_0
          && OB_FAIL(dml.add_column("index_params", ObHexEscapeSqlStr(index_params))))
      || (data_version >= DATA_VERSION_4_3_3_0
          && OB_FAIL(dml.add_column("micro_index_clustered", table.get_micro_index_clustered())))
      || (data_version >= DATA_VERSION_4_3_3_0
          && OB_FAIL(dml.add_column("local_session_vars", ObHexEscapeSqlStr(local_session_var))))
      || (data_version >= DATA_VERSION_4_3_5_1
          && OB_FAIL(dml.add_column("parser_properties", ObHexEscapeSqlStr(parser_properties))))
      || (data_version >= DATA_VERSION_4_3_5_1
          && OB_FAIL(dml.add_column("enable_macro_block_bloom_filter", table.get_enable_macro_block_bloom_filter())))
      || (data_version >= DATA_VERSION_4_3_5_2
          && OB_FAIL(dml.add_column("storage_cache_policy", ObHexEscapeSqlStr(storage_cache_policy))))
      || (data_version >= DATA_VERSION_4_3_5_2
          && OB_FAIL(dml.add_column("semistruct_encoding_type", table.get_semistruct_encoding_flags())))
      || (data_version >= DATA_VERSION_4_3_5_2
          && OB_FAIL(dml.add_column("dynamic_partition_policy", ObHexEscapeSqlStr(dynamic_partition_policy))))
      || (data_version >= DATA_VERSION_4_3_5_2
          && OB_FAIL(dml.add_column("merge_engine_type", table.get_merge_engine_type())))
      || (data_version >= DATA_VERSION_4_4_0_0
          && OB_FAIL(dml.add_column("external_location_id", table.get_external_location_id())))
      || (data_version >= DATA_VERSION_4_4_0_0
          && OB_FAIL(dml.add_column("external_sub_path", ObHexEscapeSqlStr(table.get_external_sub_path()))))
      || (data_version >= DATA_VERSION_4_4_1_0
        && OB_FAIL(dml.add_column("semistruct_properties", ObHexEscapeSqlStr(semistruct_properties))))
      || (data_version >= DATA_VERSION_4_4_1_0
          && OB_FAIL(dml.add_column("micro_block_format_version", table.get_micro_block_format_version())))
      || (((data_version >= MOCK_DATA_VERSION_4_3_5_5 && data_version < DATA_VERSION_4_4_0_0)
           || (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0)
           || (data_version >= DATA_VERSION_4_5_1_0))
          && OB_FAIL(dml.add_column("mview_expand_definition", ObHexEscapeSqlStr(table.get_view_schema().get_expand_view_definition_for_mv()))))
        ) {
        LOG_WARN("add column failed", K(ret));
      }
  return ret;
}

int ObTableSqlService::gen_table_dml(
    const uint64_t exec_tenant_id,
    const ObTableSchema &table,
    const bool update_object_status_ignore_version,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(table.get_charset_type(),
                                                                    exec_tenant_id))) {
    LOG_WARN("failed to check charset data version valid", K(table.get_charset_type()), K(ret));
  } else if (OB_FAIL(sql::ObSQLUtils::is_collation_data_version_valid(table.get_collation_type(),
                                                                      exec_tenant_id))) {
    LOG_WARN("failed to check collation data version valid", K(table.get_collation_type()), K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(table.get_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0
             && OB_UNLIKELY((!update_object_status_ignore_version && ObObjectStatus::VALID != table.get_object_status())
                            || table.view_column_filled())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("option is not support before 4.1", K(ret), K(table));
  } else if (data_version < DATA_VERSION_4_1_0_0
             && OB_UNLIKELY((OB_INVALID_VERSION != table.get_truncate_version()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("truncate version is not support before 4.1", K(ret), K(table));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_2_0_0
                         && (table.is_external_table()
                             || !table.get_external_file_location().empty()
                             || !table.get_external_file_format().empty()
                             || !table.get_external_properties().empty()
                             || !table.get_external_file_location_access_info().empty()
                             || !table.get_external_file_pattern().empty()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external table is not support before 4.2", K(ret), K(table));
  } else if (data_version < DATA_VERSION_4_3_1_0 && table.get_table_flags() > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("some table flag is not support before 4.3.1", K(ret), K(table));
  } else if (data_version < DATA_VERSION_4_2_1_2
             && OB_UNLIKELY(OB_DEFAULT_LOB_INROW_THRESHOLD != table.get_lob_inrow_threshold())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("lob_inrow_threshold not support before 4.2.1.2", K(ret), K(table));
  } else if (data_version < DATA_VERSION_4_3_0_0
             && OB_UNLIKELY(ObRowStoreType::CS_ENCODING_ROW_STORE == table.get_row_store_type() ||
                 ObStoreFormatType::OB_STORE_FORMAT_ARCHIVE_HIGH_ORACLE == table.get_store_format())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("CS_ENCODING_ROW_STORE and OB_STORE_FORMAT_ARCHIVE_HIGH_ORACLE not support before 4.3", K(ret), K(table));

  } else if (data_version < DATA_VERSION_4_3_0_0
             && (table.get_compressor_type() == ObCompressorType::ZLIB_LITE_COMPRESSOR
                 || 0 == strcasecmp(table.get_compress_func_name(), all_compressor_name[ObCompressorType::ZLIB_LITE_COMPRESSOR]))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("zlib_lite_1.0 not support before 4.3", K(ret), K(table));
  } else if ((data_version < MOCK_DATA_VERSION_4_2_3_0 ||
                (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_2_0))
             && OB_UNLIKELY(0 != table.get_auto_increment_cache_size())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("auto increment cache size not support before 4.2.3", K(ret), K(table));
  } else if (data_version < DATA_VERSION_4_3_3_0
      && !table.get_index_params().empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index params setting is not support in version less than 433");
  } else if (data_version < DATA_VERSION_4_2_1_0
      && (!table.get_ttl_definition().empty() || !table.get_kv_attributes().empty())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ttl definition and kv attributes is not supported in version less than 4.2.1",
        "ttl_definition", table.get_ttl_definition().empty(),
        "kv_attributes", table.get_kv_attributes().empty());
  } else if (data_version < DATA_VERSION_4_3_5_2 &&
            !is_storage_cache_policy_default(table.get_storage_cache_policy())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("storage cache policy is not supported in version less than 4.3.5.2",
        "storage_cache_policy", table.get_storage_cache_policy());
  } else if (data_version < DATA_VERSION_4_3_5_2 && table.get_semistruct_encoding_flags() != 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("semistruct encodin is not supported in version less than 4.3.5.2",
        "semistruct_encoding_flags", table.get_semistruct_encoding_flags(),
        "semistruct_encoding_type", table.get_semistruct_encoding_type());
  } else if (data_version < DATA_VERSION_4_4_1_0 &&
              !table.get_semistruct_properties().empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("semistruct properties is not supported in version less than 4.4.1.0", K(table.get_semistruct_properties()));
  } else if (not_compat_for_queuing_mode(data_version) && table.is_new_queuing_table_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN(QUEUING_MODE_NOT_COMPAT_WARN_STR, K(ret), K(table));
  } else if (data_version < DATA_VERSION_4_1_0_0 && 0 != table.get_table_flags()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table flags is not supported when tenant's data version is below 4.1.0.0", KR(ret),
        K(table));
  } else if (data_version < DATA_VERSION_4_3_5_1 && !table.get_parser_property_str().empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parser properities for full-text search index is not supported "
        "when tenant's data version is below 4.3.5.1", KP(ret), K(table));
  } else if (data_version < DATA_VERSION_4_3_4_0 &&
      (table.get_part_option().get_auto_part() == true ||
       table.get_part_option().get_auto_part_size() >= ObPartitionOption::MIN_AUTO_PART_SIZE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("auto partition is not supported "
        "when tenant's data version is below 4.4.0.0", KR(ret), K(table));
  }  else if (data_version < DATA_VERSION_4_3_3_0 && table.is_vec_index()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.3, vector index is not supported", K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.3, vector index");
  } else if (data_version < DATA_VERSION_4_3_0_0 && table.is_materialized_view()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("mview is not support before 4.3", KR(ret), K(table));
  } else if (data_version < DATA_VERSION_4_3_1_0 && (table.is_fts_index() || table.is_multivalue_index())) {
    ret = OB_NOT_SUPPORTED;
    if (table.is_multivalue_index()) {
      LOG_WARN("tenant data version is less than 4.3.1, multivalue index is not supported", K(ret), K(data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, multivalue index");
    } else {
      LOG_WARN("tenant data version is less than 4.3.1, fulltext index is not supported", K(ret), K(data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, fulltext index");
    }
  } else if (OB_FAIL(check_column_store_valid(table, data_version))) {
    LOG_WARN("fail to check column store valid", KR(ret), K(table), K(data_version));
  } else if (OB_FAIL(check_table_options(table))) {
    LOG_WARN("fail to check table option", K(ret), K(table));
  } else if (data_version < DATA_VERSION_4_3_3_0 && table.get_micro_index_clustered()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't set micro_index_clustered in current version", KR(ret), K(table));
  } else if (data_version < DATA_VERSION_4_3_5_1 && table.get_enable_macro_block_bloom_filter()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't set enable_macro_block_bloom_filter in current version", KR(ret), K(table));
  } else if (data_version < DATA_VERSION_4_3_5_2 && table.is_delete_insert_merge_engine()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't set merge_engine_type in current version", KR(ret), K(table));
  } else if (!ObMicroBlockFormatVersionHelper::check_version_valid(table.get_micro_block_format_version(), data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't set micro block format version in current version", KR(ret), K(table));
  } else if (OB_FAIL(gen_table_dml_without_check(exec_tenant_id, table,
          update_object_status_ignore_version, data_version, dml))) {
    LOG_WARN("failed to gen_table_dml_with_data_version", KR(ret), KDV(data_version));
  }
  return ret;
}

int ObTableSqlService::gen_table_options_dml(
    const uint64_t exec_tenant_id,
    const ObTableSchema &table,
    const bool update_object_status_ignore_version,
    ObDMLSqlSplicer &dml)
{
  return gen_table_dml(exec_tenant_id, table, update_object_status_ignore_version, dml);
}

int ObTableSqlService::update_table_attribute(ObISQLClient &sql_client,
                                              const ObTableSchema &new_table_schema,
                                              const ObSchemaOperationType operation_type,
                                              const bool update_object_status_ignore_version,
                                              const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t table_id = new_table_schema.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(new_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(new_table_schema));
  } else if (OB_FAIL(gen_table_dml(exec_tenant_id, new_table_schema,
          update_object_status_ignore_version, dml))) {
    LOG_WARN("failed to gen_table_dml", KR(ret), K(exec_tenant_id), K(new_table_schema),
        K(update_object_status_ignore_version));
  } else if (!new_table_schema.is_interval_part()) {
    bool is_null = true; // maybe unset transition_point
    if (OB_FAIL(dml.add_column(is_null, "transition_point"))
        || OB_FAIL(dml.add_column(is_null, "b_transition_point"))
        || OB_FAIL(dml.add_column(is_null, "interval_range"))
        || OB_FAIL(dml.add_column(is_null, "b_interval_range"))) {
      LOG_WARN("fail to reset interval column info", KR(ret), K(new_table_schema));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t affected_rows = 0;
    const char *table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, tenant_id, table_id,
            table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }

  // add updated table to __all_table_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, new_table_schema, update_object_status_ignore_version, only_history))) {
      LOG_WARN("add_table failed", K(new_table_schema), K(only_history));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = new_table_schema.get_tenant_id();
      opt.database_id_ = new_table_schema.get_database_id();
      opt.tablegroup_id_ = new_table_schema.get_tablegroup_id();
      opt.table_id_ = new_table_schema.get_table_id();
      opt.op_type_ = operation_type;
      opt.schema_version_ = new_table_schema.get_schema_version();
      opt.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObTableSqlService::gen_partition_option_dml(const ObTableSchema &table, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table.get_tenant_id());
  const ObPartitionOption &part_option = table.get_part_option();
  const ObPartitionOption &sub_part_option = table.get_sub_part_option();
  const char *part_func_expr = part_option.get_part_func_expr_str().length() <= 0 ?
  "" : part_option.get_part_func_expr_str().ptr();
  const char *sub_part_func_expr = sub_part_option.get_part_func_expr_str().length() <= 0 ?
  "" : sub_part_option.get_part_func_expr_str().ptr();
  const int64_t part_num = part_option.get_part_num();
  const int64_t sub_part_num = PARTITION_LEVEL_TWO == table.get_part_level()
                               && table.has_sub_part_template_def() ?
                               sub_part_option.get_part_num() : 0;
  uint64_t data_version = 0;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(table.get_tenant_id(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0 && 0 != table.get_table_flags()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table flags is not supported when tenant's data version is below 4.1.0.0", KR(ret),
             K(table));
  } else if (data_version < DATA_VERSION_4_3_4_0 &&
               (table.get_part_option().get_auto_part() == true ||
                table.get_part_option().get_auto_part_size() >= ObPartitionOption::MIN_AUTO_PART_SIZE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("auto partition is not supported "
             "when tenant's data version is below 4.4.0.0", KR(ret), K(table));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, table_id)))
      || OB_FAIL(dml.add_column("part_level", table.get_part_level()))
      || OB_FAIL(dml.add_column("part_func_type", part_option.get_part_func_type()))
      || OB_FAIL(dml.add_column("part_func_expr", ObHexEscapeSqlStr(part_func_expr)))
      || OB_FAIL(dml.add_column("part_num", part_num))
      || OB_FAIL(dml.add_column("sub_part_func_type", sub_part_option.get_part_func_type()))
      || OB_FAIL(dml.add_column("sub_part_func_expr", ObHexEscapeSqlStr(sub_part_func_expr)))
      || OB_FAIL(dml.add_column("sub_part_num", sub_part_num))
      || OB_FAIL(dml.add_column("schema_version", table.get_schema_version()))
      || OB_FAIL(dml.add_column("partition_status", table.get_partition_status()))
      || OB_FAIL(dml.add_column("partition_schema_version", table.get_partition_schema_version()))
      || OB_FAIL(dml.add_column("sub_part_template_flags", table.get_sub_part_template_flags()))
      || OB_FAIL(dml.add_column("auto_part", table.get_part_option().get_auto_part()))
      || OB_FAIL(dml.add_column("auto_part_size", table.get_part_option().get_auto_part_size()))
      || OB_FAIL(dml.add_gmt_create())
      || (table.is_interval_part() && OB_FAIL(add_transition_point_val(dml, table)))
      || (table.is_interval_part() && OB_FAIL(add_interval_range_val(dml, table)))
      || (data_version >= DATA_VERSION_4_1_0_0
          && OB_FAIL(dml.add_column("table_flags", table.get_table_flags())))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObTableSqlService::update_partition_option(ObISQLClient &sql_client,
                                               const ObTableSchema &table,
                                               const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (OB_FAIL(gen_partition_option_dml(table, dml))) {
    LOG_WARN("fail to gen dml", KR(ret));
  } else if (OB_FAIL(update_partition_option_(sql_client, table, dml))) {
    LOG_WARN("fail to update partition option", KR(ret), K(table));
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = table.get_tenant_id();
    opt.database_id_ = table.get_database_id();
    opt.tablegroup_id_ = table.get_tablegroup_id();
    opt.table_id_ = table.get_table_id();
    opt.op_type_ = OB_DDL_ALTER_TABLE;
    opt.schema_version_ = table.get_schema_version();
    opt.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_partition_option(ObISQLClient &sql_client,
                                               ObTableSchema &table,
                                               const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  table.set_schema_version(new_schema_version);
  ObDMLSqlSplicer dml;

  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (OB_FAIL(gen_partition_option_dml(table, dml))) {
    LOG_WARN("fail to gen dml", KR(ret));
  } else if (OB_FAIL(update_partition_option_(sql_client, table, dml))) {
    LOG_WARN("fail to update partition option", KR(ret), K(table));
  }
  return ret;
}

int ObTableSqlService::update_splitting_partition_option(ObISQLClient &sql_client,
                                                         const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;

  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (OB_FAIL(gen_partition_option_dml(table, dml))) {
    LOG_WARN("fail to gen dml", KR(ret));
  } else if (OB_FAIL(dml.add_column("tablet_id", table.get_tablet_id().id()))) {
    // when first auto partitions non-partitioned table,
    // the tablet_id record should be set as INVALID_TABLET_ID like partitioned_table
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(update_partition_option_(sql_client, table, dml))) {
    LOG_WARN("fail to update partition option", KR(ret), K(table));
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = table.get_tenant_id();
    opt.database_id_ = table.get_database_id();
    opt.tablegroup_id_ = table.get_tablegroup_id();
    opt.table_id_ = table.get_table_id();
    opt.op_type_ = OB_DDL_SPLIT_PARTITION;
    opt.schema_version_ = table.get_schema_version();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_partition_option_(ObISQLClient &sql_client,
                                                const ObTableSchema &table,
                                                ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t table_id = table.get_table_id();
  int64_t affected_rows = 0;
  const char *table_name = NULL;

  if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
    LOG_WARN("fail to get all table name", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(exec_update(sql_client, tenant_id, table_id,
                                 table_name, dml, affected_rows))) {
    LOG_WARN("exec update failed", KR(ret));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(affected_rows), KR(ret));
  }

  // add updated table to __all_table_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    const bool update_object_status_ignore_version = false;
    if (OB_FAIL(add_table(sql_client, table, update_object_status_ignore_version, only_history))) {
      LOG_WARN("add_table failed", KR(ret), K(table), K(only_history));
    } else {}
  }
  return ret;
}

int ObTableSqlService::update_all_part_for_subpart(ObISQLClient &sql_client,
                                                   const ObTableSchema &table,
                                                   const ObIArray<ObPartition*> &update_part_array)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < update_part_array.count(); i++) {
      ObPartition *inc_part = update_part_array.at(i);
      if (OB_ISNULL(inc_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc_part_array[i] is NULL", K(ret), K(i));
      } else {
        if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                   exec_tenant_id, tenant_id)))
            || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                     exec_tenant_id, table_id)))
            || OB_FAIL(dml.add_pk_column("part_id", inc_part->get_part_id()))
            || OB_FAIL(dml.add_column("sub_part_num", inc_part->get_sub_part_num()))
            || OB_FAIL(dml.add_column("schema_version", table.get_schema_version()))
            || OB_FAIL(dml.add_gmt_modified())) {
          LOG_WARN("add column failed", K(ret));
        } else {
          int64_t affected_rows = 0;
          if (OB_FAIL(exec_update(sql_client, tenant_id, table_id,
                                  OB_ALL_PART_TNAME, dml, affected_rows))) {
            LOG_WARN("exec update failed", K(ret));
          } else if (affected_rows > 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(affected_rows), KR(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) || update_part_array.count() == 0) {
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table.get_tenant_id());
    ObDMLSqlSplicer history_dml;
    for (int64_t i = 0; OB_SUCC(ret) && i < update_part_array.count(); i++) {
      ObPartition *inc_part = update_part_array.at(i);
      if (OB_ISNULL(inc_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc_part_array[i] is NULL", K(ret), K(i));
      } else {
        HEAP_VAR(ObAddIncPartDMLGenerator, part_dml_gen,
                 &table, *inc_part, -1, -1, table.get_schema_version()) {
          if (OB_FAIL(part_dml_gen.gen_dml(history_dml))) {
            LOG_WARN("gen dml failed", K(ret));
          } else if (OB_FAIL(history_dml.add_column("is_deleted", false))) {
            LOG_WARN("add column failed", K(ret));
          } else if (OB_FAIL(history_dml.finish_row())) {
            LOG_WARN("failed to finish row", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && update_part_array.count() != 0) {
      int64_t affected_rows = 0;
      ObSqlString part_history_sql;
      if (OB_FAIL(history_dml.splice_batch_insert_sql(
                  share::OB_ALL_PART_HISTORY_TNAME,
                  part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql",
                 K(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id,
                                      part_history_sql.ptr(),
                                      affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != update_part_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret),
                 K(update_part_array.count()), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_subpartition_option(ObISQLClient &sql_client,
                                                  const ObTableSchema &table,
                                                  const ObIArray<ObPartition*> &update_part_array)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else if (!table.has_tablet()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has not tablet", KR(ret));
  } else if (OB_FAIL(update_all_part_for_subpart(sql_client, table, update_part_array))) {
    LOG_WARN("fail to update all part", K(ret));
  }

  return ret;
}

int ObTableSqlService::delete_from_all_table(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char *table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                                   table_name, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret), K(table_id), K(tenant_id));
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_temp_table(ObISQLClient &sql_client,
                                                  const uint64_t tenant_id,
                                                  const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                            OB_ALL_TEMP_TABLE_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::delete_from_all_table_stat(ObISQLClient &sql_client,
                                                  const uint64_t tenant_id,
                                                  const uint64_t table_id,
                                                  ObSqlString *extra_condition)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                  exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                     exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_NOT_NULL(extra_condition) && OB_FAIL(dml.get_extra_condition().assign(*extra_condition))) {
      LOG_WARN("fail to assign extra condition", K(ret));
    } else if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                            OB_ALL_TABLE_STAT_TNAME,
                            dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    }
  }

  return ret;
}
int ObTableSqlService::delete_from_all_histogram_stat(ObISQLClient &sql_client,
                                                      const uint64_t tenant_id,
                                                      const uint64_t table_id,
                                                      ObSqlString *extra_condition)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                  exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                     exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_NOT_NULL(extra_condition) && OB_FAIL(dml.get_extra_condition().assign(*extra_condition))) {
      LOG_WARN("fail to assign extra condition", K(ret));
    } else if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                            OB_ALL_HISTOGRAM_STAT_TNAME,
                            dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_column(ObISQLClient &sql_client,
                                              const uint64_t tenant_id,
                                              const uint64_t table_id,
                                              int64_t column_count,
                                              bool check_affect_rows)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                            OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (check_affect_rows && affected_rows < column_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not all row deleted, ", K(column_count), K(affected_rows), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_column_stat(ObISQLClient &sql_client,
                                                   const uint64_t tenant_id,
                                                   const uint64_t table_id,
                                                   ObSqlString *extra_condition)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                  exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                     exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_NOT_NULL(extra_condition) && OB_FAIL(dml.get_extra_condition().assign(*extra_condition))) {
      LOG_WARN("fail to assign extra condition", K(ret));
    } else if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                            OB_ALL_COLUMN_STAT_TNAME,
                            dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_column_stat(ObISQLClient &sql_client,
                                          const uint64_t tenant_id,
                                          const uint64_t table_id,
                                          const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer del_stat_dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(del_stat_dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                            exec_tenant_id, tenant_id)))
      || OB_FAIL(del_stat_dml.add_pk_column("table_id", table_id))
      || OB_FAIL(del_stat_dml.add_pk_column("column_id", column_id))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id, OB_ALL_COLUMN_STAT_TNAME,
                            del_stat_dml, affected_rows))) {
      LOG_WARN("exec delete column stat failed", K(ret));
    } else if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id, OB_ALL_HISTOGRAM_STAT_TNAME,
                                    del_stat_dml, affected_rows))) {
      LOG_WARN("exec delete histogram stat failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::delete_from_all_table_history(ObISQLClient &sql_client,
                                                     const ObTableSchema &table_schema,
                                                     const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  // insert into __all_table_history
  const char *table_name = NULL;
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(exec_tenant_id, table_name))) {
    LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "INSERT INTO %s(tenant_id,table_id,schema_version,data_table_id,is_deleted)"
      " VALUES(%lu,%lu,%ld,%lu,%ld)",
      table_name,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table_schema.get_tenant_id()),
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schema.get_table_id()),
      new_schema_version,
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schema.get_data_table_id()),
      is_deleted))) {
    LOG_WARN("assign insert into all talbe history fail",
             K(table_schema), K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql fail", K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no row has inserted", K(ret));
  }
  return ret;
}

int ObTableSqlService::delete_from_all_column_history(ObISQLClient &sql_client,
                                                      const ObTableSchema &table_schema,
                                                      const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else if (OB_FAIL(sql.append_fmt("INSERT /*+use_plan_cache(none)*/ INTO %s "
      "(TENANT_ID, TABLE_ID, COLUMN_ID, SCHEMA_VERSION, IS_DELETED) VALUES ",
      OB_ALL_COLUMN_HISTORY_TNAME))) {
    LOG_WARN("append_fmt failed", K(ret));
  }
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
      OB_SUCCESS == ret && iter != table_schema.column_end(); ++iter) {
    if (OB_FAIL(sql.append_fmt("%s(%lu, %lu, %lu, %ld, %ld)",
        (iter == table_schema.column_begin()) ? "" : ",",
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, (*iter)->get_tenant_id()),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*iter)->get_table_id()),
        (*iter)->get_column_id(),
        new_schema_version, is_deleted))) {
      LOG_WARN("append_fmt failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (table_schema.get_column_count() != affected_rows) {
      LOG_WARN("affected_rows not same with column_count", K(affected_rows),
          "column_count", table_schema.get_column_count(), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::delete_from_all_optstat_user_prefs(ObISQLClient &sql_client,
                                                          const uint64_t tenant_id,
                                                          const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                       exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                          exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                            OB_ALL_OPTSTAT_USER_PREFS_TNAME,
                            dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_data_table_schema_version(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const bool in_offline_ddl_white_list,
    int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = tenant_id;
  ObDMLSqlSplicer dml;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id;
  ObTableSchema table_schema;
  if (OB_INVALID_VERSION == new_schema_version
      && OB_FAIL(schema_service_.gen_new_schema_version(
                 tenant_id, OB_INVALID_VERSION, new_schema_version))) {
    // for generating different schema version for the same table in one trans
    LOG_WARN("fail to gen new schema version", K(ret), K(tenant_id));
  } else if (OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data table id", K(data_table_id));
  } else if (OB_FAIL(schema_service_.get_table_schema_from_inner_table(
                     schema_status, data_table_id, sql_client, table_schema))) {
    LOG_WARN("get_table_schema failed", K(data_table_id), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (FALSE_IT(table_schema.set_in_offline_ddl_white_list(in_offline_ddl_white_list))) {
    } else if (OB_FAIL(check_ddl_allowed(table_schema))) {
      LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                      exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, data_table_id)))
          || OB_FAIL(dml.add_column("schema_version", new_schema_version))
          || OB_FAIL(dml.add_column("rowkey_column_num", table_schema.get_rowkey_column_num()))
          || OB_FAIL(dml.add_column("index_column_num", table_schema.get_index_column_num()))
          || OB_FAIL(dml.add_column("max_used_column_id", table_schema.get_max_used_column_id()))
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("add column failed", K(ret));
    } else {
      int64_t affected_rows = 0;
      const char *table_name = NULL;
      if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
        LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
      } else if (OB_FAIL(exec_update(sql_client, tenant_id, data_table_id,
                                     table_name, dml, affected_rows))) {
        LOG_WARN("exec update failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error",
                 K(affected_rows),
                 K(ret),
                 K(tenant_id),
                 K(data_table_id));
      }
    }
    // add new table_schema to __all_table_history
    if (OB_SUCC(ret)) {
      const bool only_history = true;
      const bool update_object_status_ignore_version = false;
      table_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(add_table(sql_client, table_schema, update_object_status_ignore_version, only_history))) {
        LOG_WARN("add_table failed", K(table_schema), K(only_history), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = tenant_id;
      opt.database_id_ = table_schema.get_database_id();
      opt.tablegroup_id_ = table_schema.get_tablegroup_id();
      opt.table_id_ = data_table_id;
      opt.op_type_ = OB_DDL_MODIFY_TABLE_SCHEMA_VERSION;
      opt.schema_version_ = new_schema_version;
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  } else if (OB_TABLE_NOT_EXIST == ret) { // need to check if mock fk parent table exist
    ObMockFKParentTableSchema mock_fk_parent_table_schema;
    if (OB_FAIL(schema_service_.get_mock_fk_parent_table_schema_from_inner_table(
                schema_status, data_table_id, sql_client, mock_fk_parent_table_schema))) {
      LOG_WARN("get_mock_fk_parent_table_schema_from_inner_table failed", K(ret), K(data_table_id));
    } else if (OB_FAIL(update_mock_fk_parent_table_schema_version(&sql_client, mock_fk_parent_table_schema))) {
      LOG_WARN("get_table_schema failed", K(ret), K(mock_fk_parent_table_schema));
    }
  }
  return ret;
}

int ObTableSqlService::add_sequence_dml(share::ObDMLSqlSplicer &dml,
                                    const uint64_t tenant_id,
                                    const uint64_t table_id,
                                    const uint64_t column_id,
                                    const uint64_t auto_increment,
                                    const int64_t truncate_version)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = tenant_id;
  if (OB_FAIL(dml.add_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                      exec_tenant_id, tenant_id)))) {
    LOG_WARN("failed to add tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("sequence_key", ObSchemaUtils::get_extract_schema_id(
                                      exec_tenant_id, table_id)))) {
    LOG_WARN("failed to add sequence_key", KR(ret), K(table_id));
  } else if (OB_FAIL(dml.add_pk_column("column_id", column_id))) {
    LOG_WARN("failed to add column_id", KR(ret), K(column_id));
  } else if (OB_FAIL(dml.add_column("sequence_value", 0 == auto_increment ? 1 : share::ObRealUInt64(auto_increment)))) {
    LOG_WARN("failed to add sequence_value", KR(ret), K(auto_increment));
  } else if (OB_FAIL(dml.add_column("sync_value", 0 == auto_increment ? 0 : share::ObRealUInt64(auto_increment - 1)))) {
    LOG_WARN("failed to add sync_value", KR(ret), K(auto_increment));
  } else if (OB_FAIL(dml.add_column("truncate_version", truncate_version))) {
    LOG_WARN("failed to add truncate_version", KR(ret), K(truncate_version));
  } else if (OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("failed to add gmt_modified", KR(ret));
  }
  return ret;
}

int ObTableSqlService::batch_add_sequence_for_create_table(
    common::ObISQLClient &sql_client,
    const ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (tables.empty()) {
  } else {
    common::ObTimeGuard time_guard("batch_add_sequence_for_create_table", 1_ms);
    ObDMLSqlSplicer dml;
    const uint64_t tenant_id = tables.at(0).get_tenant_id();
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); i++) {
      const ObTableSchema &table = tables.at(i);
      if (table.get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id not same", KR(ret), K(table), K(tenant_id));
      } else if (0 == table.get_autoinc_column_id()) {
      } else if (OB_FAIL(add_sequence_dml(dml, table.get_tenant_id(), table.get_table_id(),
              table.get_autoinc_column_id(), table.get_auto_increment(),
              table.get_truncate_version()))) {
        LOG_WARN("failed to add sequence dml", KR(ret), K(table));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish_row", KR(ret), K(table));
      }
    }
    time_guard.click("generate_dml");
    if (FAILEDx(exec_dml(sql_client, tenant_id, OB_ALL_AUTO_INCREMENT_TNAME, dml,
            -1/*affected_rows -1 means not check*/, true/*insert_ignore*/))) {
      LOG_WARN("failed to insert all_auto_increment", KR(ret), K(tenant_id));
    }
    time_guard.click("exec_sql");
  }
  return ret;
}

int ObTableSqlService::add_sequence(ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const uint64_t table_id,
                                    const uint64_t column_id,
                                    const uint64_t auto_increment,
                                    const int64_t truncate_version)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(sql_client, tenant_id);
  // FIXME:__all_time_zone contains auto increment column. Cyclic dependence may occur.
  if (OB_FAIL(add_sequence_dml(dml, tenant_id, table_id, column_id, auto_increment, truncate_version))) {
    LOG_WARN("failed to add sequence dml", KR(ret), K(tenant_id), K(table_id), K(column_id), K(auto_increment), K(truncate_version));
  } else if (OB_FAIL(dml.finish_row())) {
    LOG_WARN("failed to finish_row", KR(ret));
  } else if (OB_FAIL(exec_dml(sql_client, tenant_id, OB_ALL_AUTO_INCREMENT_TNAME, dml,
          -1/*target_affected_row_count*/, true/*insert_ignore*/))) {
    LOG_WARN("fail to execute", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTableSqlService::sync_aux_schema_version_for_history(
  ObISQLClient &sql_client,
  const ObTableSchema &aux_schema1,
  const uint64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObTableSchema, aux_schema) {
  if (OB_FAIL(check_ddl_allowed(aux_schema1))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(aux_schema1));
  } else if (OB_FAIL(aux_schema.assign(aux_schema1))) {
    LOG_WARN("fail to assign schema", KR(ret));
  } else if (OB_FAIL(sync_schema_version_for_history(sql_client, aux_schema, new_schema_version))) {
    LOG_WARN("fail to sync schema version for history", KR(ret), K(aux_schema), K(new_schema_version));
  }
  } // end HEAP_VAR
  return ret;
}


int ObTableSqlService::sync_schema_version_for_history(
  ObISQLClient &sql_client,
  ObTableSchema &schema,
  uint64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  schema.set_schema_version(new_schema_version);
  if (OB_FAIL(check_ddl_allowed(schema))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(schema));
  } else {
    ObDMLSqlSplicer dml;
    const uint64_t tenant_id = schema.get_tenant_id();
    const uint64_t table_id = schema.get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    int64_t affected_rows = 0;
    const char *table_name = NULL;
    const bool only_history = true;
    const bool update_object_status_ignore_version = false;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, table_id)))
        || OB_FAIL(dml.add_column("schema_version", new_schema_version))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, tenant_id, table_id,
                                   table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", KR(ret));
    } else if (OB_UNLIKELY(affected_rows > 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(affected_rows));
    } else if (OB_FAIL(add_table(sql_client, schema, update_object_status_ignore_version, only_history))) {
      LOG_WARN("fail to add table for history", KR(ret), K(schema));
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = schema.get_tenant_id();
    opt.database_id_ = schema.get_database_id();
    opt.tablegroup_id_ = schema.get_tablegroup_id();
    opt.table_id_ = schema.get_table_id();
    opt.op_type_ = OB_DDL_MODIFY_TABLE_SCHEMA_VERSION;
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", KR(ret), K(opt));
    }
  }
  return ret;
}

int ObTableSqlService::update_tablegroup(ObSchemaGetterGuard &schema_guard,
                                         ObTableSchema &new_table_schema,
                                         common::ObISQLClient &sql_client,
                                         const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const bool update_object_status_ignore_version = false;
  if (OB_FAIL(check_ddl_allowed(new_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(new_table_schema));
  } else {
    if (OB_FAIL(check_table_options(new_table_schema))) {
      LOG_WARN("fail to check table option", K(ret), K(new_table_schema));
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, new_table_schema.get_table_id())))
        || OB_FAIL(dml.add_column("schema_version", new_table_schema.get_schema_version()))
        || OB_FAIL(dml.add_column("tablegroup_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, new_table_schema.get_tablegroup_id())))) {
      LOG_WARN("add column failed", K(ret));
    } else {
      int64_t affected_rows = 0;
      const char *table_name = NULL;
      if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
        LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
      } else if (OB_FAIL(exec_update(sql_client, tenant_id, new_table_schema.get_table_id(),
                                     table_name, dml, affected_rows))) {
        LOG_WARN("exec update failed", K(ret));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(affected_rows), K(ret));
      }
    }
    // insert __all_table_history
    const bool only_history = true;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_table(sql_client, new_table_schema, update_object_status_ignore_version, only_history))) {
      LOG_WARN("add_table failed", K(new_table_schema), K(only_history), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation update_tg_op;
    update_tg_op.tenant_id_ = new_table_schema.get_tenant_id();
    update_tg_op.database_id_ = new_table_schema.get_database_id();
    update_tg_op.tablegroup_id_ = new_table_schema.get_tablegroup_id();
    update_tg_op.table_id_ = new_table_schema.get_table_id();
    update_tg_op.op_type_ = OB_DDL_ALTER_TABLEGROUP_ADD_TABLE;
    update_tg_op.schema_version_ = new_table_schema.get_schema_version();
    update_tg_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation_wrapper(update_tg_op, sql_client))) {
      LOG_WARN("log operation failed", K(update_tg_op), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::gen_column_dml_without_check(
    const uint64_t exec_tenant_id,
    const ObColumnSchemaV2 &column,
    const uint64_t tenant_data_version,
    const lib::Worker::CompatMode compat_mode,
    share::ObDMLSqlSplicer &dml,
    const bool used_for_unittest /*= false*/)
{
  int ret = OB_SUCCESS;
  ObString orig_default_value;
  ObString cur_default_value;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  char *extended_type_info_buf = NULL;
  if (column.is_generated_column() ||
      column.is_identity_column() ||
      ob_is_string_type(column.get_data_type()) ||
      ob_is_json(column.get_data_type()) ||
      ob_is_geometry(column.get_data_type()) ||
      ob_is_roaringbitmap(column.get_data_type())) {
    //The default value of the generated column is the expression definition of the generated column
    ObString orig_default_value_str = column.get_orig_default_value().get_string();
    ObString cur_default_value_str = column.get_cur_default_value().get_string();
    orig_default_value.assign_ptr(orig_default_value_str.ptr(), orig_default_value_str.length());
    cur_default_value.assign_ptr(cur_default_value_str.ptr(), cur_default_value_str.length());
    if (!column.get_orig_default_value().is_null() && OB_ISNULL(orig_default_value.ptr())) {
      orig_default_value.assign_ptr("", 0);
    }
    if (!column.get_cur_default_value().is_null() && OB_ISNULL(cur_default_value.ptr())) {
      cur_default_value.assign_ptr("", 0);
    }
  } else {
    const int64_t value_buf_len = 2 * OB_MAX_DEFAULT_VALUE_LENGTH + 3;
    char *orig_default_value_buf = NULL;
    char *cur_default_value_buf = NULL;
    orig_default_value_buf = static_cast<char *>(allocator.alloc(value_buf_len));
    cur_default_value_buf = static_cast<char *>(allocator.alloc(value_buf_len));
    extended_type_info_buf = static_cast<char *>(allocator.alloc(OB_MAX_VARBINARY_LENGTH));
    if (OB_ISNULL(orig_default_value_buf)
        || OB_ISNULL(cur_default_value_buf)
        || OB_ISNULL(extended_type_info_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for default value buffer failed");
    } else {
      MEMSET(orig_default_value_buf, 0, value_buf_len);
      MEMSET(cur_default_value_buf, 0, value_buf_len);
      MEMSET(extended_type_info_buf, 0, OB_MAX_VARBINARY_LENGTH);

      int64_t orig_default_value_len = 0;
      int64_t cur_default_value_len = 0;
      lib::CompatModeGuard compat_mode_guard(compat_mode);
      ObTimeZoneInfo tz_info;
      if (OB_FAIL(OTTZ_MGR.get_tenant_tz(exec_tenant_id, tz_info.get_tz_map_wrap()))) {
        LOG_WARN("get tenant timezone failed", K(ret));
      } else if (OB_FAIL(column.get_orig_default_value().print_plain_str_literal(
                      orig_default_value_buf, value_buf_len, orig_default_value_len, &tz_info))) {
        LOG_WARN("failed to print orig default value", K(ret),
                 K(value_buf_len), K(orig_default_value_len));
      } else if (OB_FAIL(column.get_cur_default_value().print_plain_str_literal(
                             cur_default_value_buf, value_buf_len, cur_default_value_len, &tz_info))) {
        LOG_WARN("failed to print cur default value",
                 K(ret), K(value_buf_len), K(cur_default_value_len));
      } else {
        orig_default_value.assign_ptr(orig_default_value_buf, static_cast<int32_t>(orig_default_value_len));
        cur_default_value.assign_ptr(cur_default_value_buf, static_cast<int32_t>(cur_default_value_len));
      }
      LOG_TRACE("begin gen_column_dml", K(ret), K(compat_mode), K(orig_default_value), K(cur_default_value),  K(orig_default_value_len), K(cur_default_value_len));
    }
  }
  LOG_TRACE("begin gen_column_dml", K(ret), K(orig_default_value), K(cur_default_value), K(column));
  if (OB_SUCC(ret)) {
    ObString cur_default_value_v1;
    if (column.get_orig_default_value().is_null()) {
      orig_default_value.reset();
    }
    if (column.get_cur_default_value().is_null()) {
      cur_default_value.reset();
    }
    ObString bin_extended_type_info;
    if (OB_SUCC(ret) && (column.is_enum_or_set() || column.is_collection())) {
      int64_t pos = 0;
      extended_type_info_buf = static_cast<char *>(allocator.alloc(OB_MAX_VARBINARY_LENGTH));
      if (OB_ISNULL(extended_type_info_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for default value buffer failed", K(ret));
      } else if (OB_FAIL(column.serialize_extended_type_info(extended_type_info_buf, OB_MAX_VARBINARY_LENGTH, pos))) {
        LOG_WARN("fail to serialize_extended_type_info", K(ret));
      } else {
        bin_extended_type_info.assign_ptr(extended_type_info_buf, static_cast<int32_t>(pos));
      }
    }
    ObString local_session_var;
    if (OB_SUCC(ret) && column.is_generated_column() && tenant_data_version >= DATA_VERSION_4_2_2_0
        && OB_FAIL(column.get_local_session_var().gen_local_session_var_str(allocator, local_session_var))) {
      LOG_WARN("fail to gen local session var str", K(ret));
    }
    if (OB_SUCC(ret) && (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, column.get_tenant_id())))
                         || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                    exec_tenant_id, column.get_table_id())))
                         || OB_FAIL(dml.add_pk_column("column_id", column.get_column_id()))
                         || OB_FAIL(dml.add_column("column_name", ObHexEscapeSqlStr(column.get_column_name())))
                         || OB_FAIL(dml.add_column("rowkey_position", column.get_rowkey_position()))
                         || OB_FAIL(dml.add_column("index_position", column.get_index_position()))
                         || OB_FAIL(dml.add_column("partition_key_position", column.get_tbl_part_key_pos()))
                         || OB_FAIL(dml.add_column("data_type", column.get_data_type()))
                         || OB_FAIL(dml.add_column("data_length", column.get_data_length()))
                         || OB_FAIL(dml.add_column("data_precision", column.get_data_precision()))
                         || OB_FAIL(dml.add_column("data_scale", column.get_data_scale()))
                         || OB_FAIL(dml.add_column("zero_fill", column.is_zero_fill()))
                         || OB_FAIL(dml.add_column("nullable", column.is_nullable()))
                         || OB_FAIL(dml.add_column("autoincrement", column.is_autoincrement()))
                         || OB_FAIL(dml.add_column("is_hidden", column.is_hidden()))
                         || OB_FAIL(dml.add_column("on_update_current_timestamp", column.is_on_update_current_timestamp()))
                         || OB_FAIL(dml.add_column("orig_default_value_v2", ObHexEscapeSqlStr(orig_default_value)))
                         || OB_FAIL(dml.add_column("cur_default_value_v2", ObHexEscapeSqlStr(cur_default_value)))
                         || OB_FAIL(dml.add_column("cur_default_value", ObHexEscapeSqlStr(cur_default_value_v1)))
                         || OB_FAIL(dml.add_column("order_in_rowkey", column.get_order_in_rowkey()))
                         || OB_FAIL(dml.add_column("collation_type", column.get_collation_type()))
                         || OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(column.get_comment())))
                         || (OB_LIKELY(!used_for_unittest) &&
                             OB_FAIL(dml.add_column("schema_version", column.get_schema_version())))
                         || OB_FAIL(dml.add_column("column_flags", column.get_stored_column_flags()))
                         || OB_FAIL(dml.add_column("extended_type_info", ObHexEscapeSqlStr(bin_extended_type_info)))
                         || OB_FAIL(dml.add_column("prev_column_id", column.get_prev_column_id()))
                         || (tenant_data_version >= DATA_VERSION_4_1_0_0 && OB_FAIL(dml.add_column("srs_id", column.get_srs_id())))
                         || (tenant_data_version >= DATA_VERSION_4_2_0_0 && OB_FAIL(dml.add_column("udt_set_id", column.get_udt_set_id())))
                         || (tenant_data_version >= DATA_VERSION_4_2_0_0 &&OB_FAIL(dml.add_column("sub_data_type", column.get_sub_data_type())))
                         || (tenant_data_version >= DATA_VERSION_4_3_0_0
                            && OB_FAIL(dml.add_column("skip_index_attr", column.get_skip_index_attr().get_packed_value())))
                         || (((DATA_VERSION_4_2_2_0 <= tenant_data_version && tenant_data_version < DATA_VERSION_4_3_0_0) || tenant_data_version >= DATA_VERSION_4_3_1_0)
                            && OB_FAIL(dml.add_column("lob_chunk_size", column.get_lob_chunk_size())))
                         || (tenant_data_version >= DATA_VERSION_4_2_2_0 &&OB_FAIL(dml.add_column("local_session_vars", ObHexEscapeSqlStr(local_session_var))))
                         || OB_FAIL(dml.add_gmt_create())
                         || OB_FAIL(dml.add_gmt_modified()))) {
      LOG_WARN("dml add column failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::gen_column_dml(
    const uint64_t exec_tenant_id,
    const ObColumnSchemaV2 &column,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (OB_FAIL(GET_MIN_DATA_VERSION(exec_tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (! ((DATA_VERSION_4_2_2_0 <= tenant_data_version && tenant_data_version < DATA_VERSION_4_3_0_0) || tenant_data_version >= DATA_VERSION_4_3_1_0)
      && column.get_lob_chunk_size() != OB_DEFAULT_LOB_CHUNK_SIZE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.1, lob chunk size is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, lob chunk size");
  } else if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(
               column.get_tenant_id(), column.get_table_id(), compat_mode))) {
      LOG_WARN("fail to get tenant mode", K(ret), K(column));
  } else if (tenant_data_version < DATA_VERSION_4_3_3_0 && column.is_collection()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.3, array type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.3, array");
  } else if ((tenant_data_version < DATA_VERSION_4_2_2_0
             || (tenant_data_version >= DATA_VERSION_4_3_0_0 && tenant_data_version < DATA_VERSION_4_3_1_0)) &&
             column.is_geometry() && compat_mode ==lib::Worker::CompatMode::ORACLE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sdo_geometry is not supported when data_version is below 4.2.2.0 or data version is above 4.3.0.0 but below 4.3.1.0", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2.2 or data version is above 4.3.0.0 but below 4.3.1.0, sdo_geometry");
  } else if (tenant_data_version < DATA_VERSION_4_2_0_0 &&
             (column.is_xmltype() || column.get_udt_set_id() != 0 || column.get_sub_data_type() != 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.2, xmltype type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2, xmltype");
  } else if (tenant_data_version < DATA_VERSION_4_1_0_0 && ob_is_json(column.get_data_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.1, json type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, json type");
  } else if (tenant_data_version < DATA_VERSION_4_1_0_0 &&
             (ob_is_geometry(column.get_data_type()) ||
             column.get_srs_id() != OB_DEFAULT_COLUMN_SRS_ID ||
             column.is_spatial_generated_column())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.1, geometry type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, geometry type");
  } else if (tenant_data_version < DATA_VERSION_4_3_0_0 && column.get_skip_index_attr().has_skip_index()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.2, skip index feature is not supported",
        K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2, skip index");
  } else if (tenant_data_version < DATA_VERSION_4_3_2_0 &&
             (ob_is_roaringbitmap(column.get_data_type()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.2, roaringbitmap type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.2, roaringbitmap type");
  } else if (tenant_data_version < DATA_VERSION_4_3_5_1 && column.is_string_lob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.5.1, string type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.5.1, string type");
  } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(column.get_charset_type(),
                                                                    exec_tenant_id))) {
    LOG_WARN("failed to check charset data version valid",  K(column.get_charset_type()), K(ret));
  } else if (OB_FAIL(sql::ObSQLUtils::is_collation_data_version_valid(column.get_collation_type(),
                                                                      exec_tenant_id))) {
    LOG_WARN("failed to check collation data version valid",  K(column.get_collation_type()), K(ret));
  } else if (OB_FAIL(gen_column_dml_without_check(exec_tenant_id, column, tenant_data_version, compat_mode, dml))) {
    LOG_WARN("failed to gen_column_dml_without_check", KR(ret), KDV(tenant_data_version), K(compat_mode));
  }
  LOG_DEBUG("gen column dml", K(exec_tenant_id), K(column.get_tenant_id()), K(column.get_table_id()), K(column.get_column_id()),
            K(column.is_nullable()), K(column.get_stored_column_flags()), K(column.get_column_flags()));
  return ret;
}

int ObTableSqlService::gen_constraint_dml(
    const uint64_t exec_tenant_id,
    const ObConstraint &constraint,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(constraint.get_tenant_id(), data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, constraint.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, constraint.get_table_id())))
      || OB_FAIL(dml.add_pk_column("constraint_id", constraint.get_constraint_id()))
      || OB_FAIL(dml.add_column("schema_version", constraint.get_schema_version()))
      || OB_FAIL(dml.add_column("constraint_type", constraint.get_constraint_type()))
      || OB_FAIL(dml.add_column("constraint_name", ObHexEscapeSqlStr(constraint.get_constraint_name())))
      || OB_FAIL(dml.add_column("check_expr", ObHexEscapeSqlStr(constraint.get_check_expr())))
      || OB_FAIL(dml.add_column("rely_flag", constraint.get_rely_flag()))
      || OB_FAIL(dml.add_column("enable_flag", constraint.get_enable_flag()))
      || OB_FAIL(dml.add_column("validate_flag", constraint.get_validate_flag()))
      || (data_version >= DATA_VERSION_4_2_1_0
            && OB_FAIL(dml.add_column("name_generated_type", constraint.get_name_generated_type())))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("dml add constraint failed", K(ret));
  }

  return ret;
}

int ObTableSqlService::gen_constraint_column_dml(const uint64_t exec_tenant_id,
                                                 const ObConstraint &constraint,
                                                 uint64_t column_id,
                                                 share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, constraint.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, constraint.get_table_id())))
      || OB_FAIL(dml.add_pk_column("constraint_id", constraint.get_constraint_id()))
      || OB_FAIL(dml.add_pk_column("column_id", column_id))
      || OB_FAIL(dml.add_column("schema_version", constraint.get_schema_version()))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("dml add constraint column failed", K(ret));
  }

  return ret;
}

// Generate new constraint name while drop table to recyclebin.
int ObTableSqlService::gen_constraint_update_name_dml(
    const uint64_t exec_tenant_id,
    const ObString &cst_name,
    const ObNameGeneratedType name_generated_type,
    const int64_t new_schema_version,
    const ObConstraint &constraint,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;

  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(constraint.get_tenant_id(), data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, constraint.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, constraint.get_table_id())))
      || OB_FAIL(dml.add_pk_column("constraint_id", constraint.get_constraint_id()))
      || OB_FAIL(dml.add_column("schema_version", new_schema_version))
      || OB_FAIL(dml.add_column("constraint_name", ObHexEscapeSqlStr(cst_name)))
      || (data_version >= DATA_VERSION_4_2_1_0
            && OB_FAIL(dml.add_column("name_generated_type", name_generated_type)))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("dml add constraint failed", K(ret));
  }

  return ret;
}

// Generate new constraint name while drop table to recyclebin.
int ObTableSqlService::gen_constraint_insert_new_name_row_dml(
    const uint64_t exec_tenant_id,
    const ObString &cst_name,
    const ObNameGeneratedType name_generated_type,
    const int64_t new_schema_version,
    const ObConstraint &constraint,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  const int64_t is_deleted = 0;

  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(constraint.get_tenant_id(), data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, constraint.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, constraint.get_table_id())))
      || OB_FAIL(dml.add_pk_column("constraint_id", constraint.get_constraint_id()))
      || OB_FAIL(dml.add_column("schema_version", new_schema_version))
      || OB_FAIL(dml.add_column("constraint_type", constraint.get_constraint_type()))
      || OB_FAIL(dml.add_column("constraint_name", ObHexEscapeSqlStr(cst_name)))
      || OB_FAIL(dml.add_column("check_expr", ObHexEscapeSqlStr(constraint.get_check_expr())))
      || (data_version >= DATA_VERSION_4_2_1_0
            && OB_FAIL(dml.add_column("name_generated_type", name_generated_type)))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())
      || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
    LOG_WARN("dml add constraint failed", K(ret));
  }

  return ret;
}

int ObTableSqlService::log_core_operation(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatProxy proxy(sql_client, tenant_id);
  if (OB_FAIL(proxy.set_core_schema_version(schema_version))) {
    LOG_WARN("set_core_schema_version failed", KR(ret), K(tenant_id),
             "core_schema_version", schema_version);
  }
  return ret;
}

int ObTableSqlService::log_sys_operation(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatProxy proxy(sql_client, tenant_id);
  if (OB_FAIL(proxy.set_sys_schema_version(schema_version))) {
    LOG_WARN("set_sys_schema_version failed", KR(ret), K(tenant_id),
             "sys_schema_version", schema_version);
  }
  return ret;
}

int ObTableSqlService::add_table_part_info(ObISQLClient &sql_client,
                                           const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(table));
  } else if (is_core_table(table.get_table_id())) {
    //do nothing
  } else if (is_inner_table(table.get_table_id())) {
    //do nothing
  } else {
    const ObPartitionSchema *table_schema = &table;
    ObAddPartInfoHelper part_helper(sql_client, table.get_tenant_id());
    if (OB_FAIL(part_helper.init(table_schema))) {
      LOG_WARN("failed to init part_helper", KR(ret), KPC(table_schema));
    } else if (OB_FAIL(part_helper.add_partition_info())) {
      LOG_WARN("add partition info failed", KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::batch_add_table_part_info(ObISQLClient &sql_client,
                                           const ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  ObArray<const ObPartitionSchema *> partitions;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); i++) {
    const ObTableSchema &table = tables.at(i);
    if (OB_FAIL(check_ddl_allowed(table))) {
      LOG_WARN("check ddl allowd failed", KR(ret), K(table));
    } else if (is_core_table(table.get_table_id())) {
      //do nothing
    } else if (is_inner_table(table.get_table_id())) {
      //do nothing
    } else if (!is_valid_tenant_id(table.get_tenant_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tenant id", KR(ret), K(table));
    } else if (is_valid_tenant_id(tenant_id) && tenant_id != table.get_tenant_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_id not match", KR(ret), K(table), K(tenant_id));
    } else if (OB_FAIL(partitions.push_back(&table))) {
      LOG_WARN("failed to push_back table", KR(ret), K(table));
    } else {
      tenant_id = table.get_tenant_id();
    }
  }
  if (OB_SUCC(ret) && !partitions.empty()) {
    ObAddPartInfoHelper part_helper(sql_client, tenant_id);
    if (OB_FAIL(part_helper.init(partitions))) {
      LOG_WARN("failed to init part_helper", KR(ret), K(partitions));
    } else if (OB_FAIL(part_helper.add_partition_info())) {
      LOG_WARN("add partition info failed", KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::add_inc_partition_info(
                       ObISQLClient &sql_client,
                       const ObTableSchema &ori_table,
                       ObTableSchema &inc_table,
                       const int64_t schema_version,
                       bool ignore_log_operation,
                       bool is_subpart,
                       const bool is_subpart_idx_specified)
{
  int ret = OB_SUCCESS;
  if (is_subpart) {
    if (OB_FAIL(add_inc_subpart_info(sql_client,
                                     ori_table,
                                     inc_table,
                                     schema_version,
                                     is_subpart_idx_specified))) {
      LOG_WARN("add inc part info failed", KR(ret));
    }
  } else {
    if (OB_FAIL(add_inc_part_info(sql_client,
                                  ori_table,
                                  inc_table,
                                  schema_version,
                                  ignore_log_operation))) {
      LOG_WARN("add inc subpart info failed", KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::add_inc_part_info(ObISQLClient &sql_client,
                                         const ObTableSchema &ori_table,
                                         const ObTableSchema &inc_table,
                                         const int64_t schema_version,
                                         bool ignore_log_operation)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (is_core_table(ori_table.get_table_id())) {
    //do nothing
  } else if (is_inner_table(ori_table.get_table_id())) {
    //do nothing
  } else {
    const ObPartitionSchema *ori_table_schema = &ori_table;
    const ObPartitionSchema *inc_table_schema = &inc_table;
    ObAddIncPartHelper part_helper(ori_table_schema, inc_table_schema, schema_version,
                                   sql_client);
    if (OB_FAIL(part_helper.add_partition_info())) {
      LOG_WARN("add partition info failed", K(ret));
    } else if (!ignore_log_operation && OB_INVALID_ID == ori_table.get_tablegroup_id()) {
      /*
       * While table is in tablegroup, we only support to modify partition schema by tablegroup.
       * For tablegroup's partition modification, we only record tablegroup's ddl operation.
       * When refresh tablegroup's schema, we also refresh tables schema in tablegroup
       * if related tablegroup once modify partition schema.
       */
      ObSchemaOperation opt;
      opt.tenant_id_ = ori_table.get_tenant_id();
      opt.database_id_ = ori_table.get_database_id();
      opt.tablegroup_id_ = ori_table.get_tablegroup_id();
      opt.table_id_ = ori_table.get_table_id();
      opt.op_type_ = OB_DDL_ADD_PARTITION;
      opt.schema_version_ = schema_version;
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObTableSqlService::add_split_inc_part_info(ObISQLClient &sql_client,
                                               const ObTableSchema &ori_table,
                                               const ObTableSchema &inc_table,
                                               const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", KR(ret), K(ori_table));
  } else if (OB_UNLIKELY(is_inner_table(ori_table.get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table type", KR(ret), K(ori_table));
  } else {
    ObAddSplitIncPartHelper part_helper(&ori_table,
                                        &inc_table,
                                        schema_version,
                                        sql_client);
    if (OB_FAIL(part_helper.add_split_partition_info())) {
      LOG_WARN("add split partition info failed", KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_part_info(ObISQLClient &sql_client,
                                        const ObTableSchema &ori_table,
                                        const ObTableSchema &upd_table,
                                        const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (is_inner_table(ori_table.get_table_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to update split partition info of a inner table", K(ret), K(ori_table.get_table_id()));
  } else {
    const ObPartitionSchema *ori_table_schema = &ori_table;
    const ObPartitionSchema *upd_table_schema = &upd_table;
    ObUpdatePartHelper part_helper(ori_table_schema, upd_table_schema,
                                   schema_version, sql_client);
    if (OB_FAIL(part_helper.update_partition_info())) {
      LOG_WARN("update split partition info failed", K(ret));
    }
  }
  return ret;
}

/*
 * add subpartition in non-template secondary partition table
 * [@input]:
 * 1. inc_table_schema: only following members are useful.
 *    - partition_num_ : first partition num
 *      (Truncate subpartitions in different first partitions in one ddl stmt is supported.)
 *    - partition_array_ : related first partition schema array
 * 2. For each partition in partition_array_: only following members are useful.
 *    - part_id_ : physical first partition_id
 *    - subpartition_num_ : (to be added) secondary partition num in first partition
 *    - subpartition_array_ : related secondary partition schema array
 * 3. For each subpartition in subpartition_array_: only following members are useful.
 *    - subpart_id_ : should be valid
 */
int ObTableSqlService::add_inc_subpart_info(ObISQLClient &sql_client,
                                         const ObTableSchema &ori_table,
                                         const ObTableSchema &inc_table,
                                         const int64_t schema_version,
                                         const bool is_subpart_idx_specified)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ddl_allowed(ori_table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(ori_table));
  } else if (!ori_table.has_tablet()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has not tablet", KR(ret));
  } else {
    const ObPartitionSchema *ori_table_schema = &ori_table;
    const ObPartitionSchema *inc_table_schema = &inc_table;
    ObAddIncSubPartHelper subpart_helper(ori_table_schema, inc_table_schema, schema_version,
                                   sql_client);
    if (OB_FAIL(subpart_helper.add_subpartition_info(is_subpart_idx_specified))) {
      LOG_WARN("add subpartition info failed", KR(ret), K(is_subpart_idx_specified));
    }
  }
  return ret;
}

int ObTableSqlService::log_operation_wrapper(
    ObSchemaOperation &opt,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;

  const ObSchemaOperationType type = opt.op_type_;
  const uint64_t table_id = opt.table_id_;
  const int64_t schema_version = opt.schema_version_;
  const uint64_t exec_tenant_id = opt.tenant_id_;
  if (type <= OB_DDL_TABLE_OPERATION_BEGIN || type >= OB_DDL_TABLE_OPERATION_END) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected operation type", K(ret), K(type));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(table_id));
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema_version", K(ret), K(schema_version));
  } else if (OB_FAIL(log_operation(opt, sql_client))) {
    LOG_WARN("failed to log operation", K(opt), K(ret));
  } else {
    // if core schema(core table and its index/lob table) changed, core_schema_version and sys_schema_version will both be modified
    // if sys schema(sys table and its index/lob table) changed, sys_schema_version will be modified
    if (is_core_table(table_id) && OB_FAIL(log_core_operation(sql_client, exec_tenant_id, schema_version))) {
      LOG_WARN("log_core_version failed", K(ret), K(exec_tenant_id), K(schema_version));
    }
    if (OB_FAIL(ret)) {
    } else if (is_sys_table(table_id) && OB_FAIL(log_sys_operation(sql_client, exec_tenant_id, schema_version))) {
      LOG_WARN("log_sys_version failed", K(ret), K(exec_tenant_id), K(schema_version));
    }
  }
  return ret;
}

int ObTableSqlService::batch_insert_ori_schema_version(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObIArray<uint64_t> &table_ids,
    const int64_t &ori_schema_version)
{
  int ret = OB_SUCCESS;

  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  int64_t row_count = 0;
  ObSqlString insert_sql_string;
  for (int64_t i = 0; i < table_ids.count() && OB_SUCC(ret); i++) {
    const uint64_t table_id = table_ids.at(i);
    if (is_inner_table(table_id)) {
      // To avoid cyclic dependence, inner table won't record ori schema version
    } else {
      if (0 == row_count) {
        if (OB_FAIL(insert_sql_string.append_fmt(
              "INSERT INTO %s (TENANT_ID, TABLE_ID, ORI_SCHEMA_VERSION, gmt_create, gmt_modified) VALUES ",
              OB_ALL_ORI_SCHEMA_VERSION_TNAME))) {
          LOG_WARN("failed to append_fmt", KR(ret));
        }
      } else {
        if (OB_FAIL(insert_sql_string.append(", "))) {
          LOG_WARN("failed to append", KR(ret));
        }
      }
      if (FAILEDx(insert_sql_string.append_fmt("(%lu, %lu, %ld, now(6), now(6))",
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
              ori_schema_version))) {
        LOG_WARN("sql string append format string failed, ", KR(ret));
      } else {
        row_count++;
      }
    }
  }
  if (0 == row_count || OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, insert_sql_string.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed,  ", "sql", insert_sql_string.ptr(), KR(ret));
  } else if (row_count != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expect to 1, ", K(affected_rows), KR(ret));
  }
  return ret;
}

int ObTableSqlService::insert_ori_schema_version(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t &ori_schema_version)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> table_ids;
  if (OB_FAIL(table_ids.push_back(table_id))) {
    LOG_WARN("failed to push_back", KR(ret), K(table_id));
  } else if (OB_FAIL(batch_insert_ori_schema_version(sql_client, tenant_id, table_ids, ori_schema_version))) {
    LOG_WARN("failed to batch insert ori schema version", KR(ret), K(tenant_id), K(table_ids),
        K(ori_schema_version));
  }
  return ret;
}

int ObTableSqlService::batch_insert_temp_table_info(
    common::ObISQLClient &sql_client,
    const ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tables.empty() ? OB_INVALID_TENANT_ID : tables.at(0).get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  int64_t row_count = 0;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("get tenant data version failed", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); i++) {
    const ObTableSchema &table_schema = tables.at(i);
    const uint64_t table_id = table_schema.get_table_id();
    if (OB_FAIL(check_ddl_allowed(table_schema))) {
      LOG_WARN("check ddl allowd failed", KR(ret), K(table_schema));
    } else if (table_schema.is_ctas_tmp_table() || table_schema.is_tmp_table()) {
      if (OB_FAIL(dml.add_pk_column("tenant_id",
                  ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("table_id", table_id))
          || OB_FAIL(dml.add_column("create_host", table_schema.get_create_host_str()))
          || OB_FAIL(!(data_version < MOCK_DATA_VERSION_4_3_5_4
                       || (data_version < MOCK_DATA_VERSION_4_4_2_0
                           && data_version >= DATA_VERSION_4_4_0_0)
                       || (data_version < DATA_VERSION_4_5_1_0
                           && data_version >= DATA_VERSION_4_5_0_0))
                     && dml.add_uint64_column("table_session_id", table_schema.get_session_id()))) {
        LOG_WARN("fail to add dml", KR(ret));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish_row", KR(ret));
      } else {
        row_count++;
      }
    }
  }
  if (OB_FAIL(ret) || 0 == row_count) {
  } else if (OB_FAIL(exec_dml(sql_client, exec_tenant_id, OB_ALL_TEMP_TABLE_TNAME, dml, row_count))) {
    LOG_WARN("execute sql failed", KR(ret));
  }
  return ret;
}

int ObTableSqlService::insert_temp_table_info(ObISQLClient &sql_client, const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableSchema, 1> tables;
  if (OB_FAIL(tables.push_back(table_schema))) {
    LOG_WARN("failed to push_back table", KR(ret), K(table_schema));
  } else if (OB_FAIL(batch_insert_temp_table_info(sql_client, tables))) {
    LOG_WARN("failed to batch insert temp table info", KR(ret));
  }
  return ret;
}

int ObTableSqlService::gen_foreign_key_dml(const uint64_t exec_tenant_id,
                                           uint64_t tenant_id,
                                           const ObForeignKeyInfo &foreign_key_info,
                                           ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  dml.reset();
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("foreign_key_id", ObSchemaUtils::get_extract_schema_id(
                                                     exec_tenant_id, foreign_key_info.foreign_key_id_)))
      || OB_FAIL(dml.add_column("foreign_key_name", ObHexEscapeSqlStr(foreign_key_info.foreign_key_name_)))
      || OB_FAIL(dml.add_column("child_table_id",  ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, foreign_key_info.child_table_id_)))
      || OB_FAIL(dml.add_column("parent_table_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, foreign_key_info.parent_table_id_)))
      || OB_FAIL(dml.add_column("update_action", foreign_key_info.update_action_))
      || OB_FAIL(dml.add_column("delete_action", foreign_key_info.delete_action_))
      || OB_FAIL(dml.add_column("enable_flag", foreign_key_info.enable_flag_))
      || OB_FAIL(dml.add_column("validate_flag", foreign_key_info.validate_flag_))
      || OB_FAIL(dml.add_column("rely_flag", foreign_key_info.rely_flag_))
      || OB_FAIL(dml.add_column("ref_cst_type", foreign_key_info.fk_ref_type_))
      || OB_FAIL(dml.add_column("ref_cst_id", foreign_key_info.ref_cst_id_))
      || OB_FAIL(dml.add_column("is_parent_table_mock", foreign_key_info.is_parent_table_mock_))
      || (data_version >= DATA_VERSION_4_2_1_0
            && OB_FAIL(dml.add_column("name_generated_type", foreign_key_info.name_generated_type_)))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())
      ) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObTableSqlService::gen_foreign_key_column_dml(
    const uint64_t exec_tenant_id,
    uint64_t tenant_id,
    uint64_t foreign_key_id,
    uint64_t child_column_id,
    uint64_t parent_column_id,
    int64_t position,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  dml.reset();
  if ( OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                              exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("foreign_key_id", ObSchemaUtils::get_extract_schema_id(
                                                     exec_tenant_id, foreign_key_id)))
      || OB_FAIL(dml.add_pk_column("child_column_id", child_column_id))
      || OB_FAIL(dml.add_pk_column("parent_column_id", parent_column_id))
      || OB_FAIL(dml.add_column("position", position))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())
      ) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObTableSqlService::delete_from_all_foreign_key(ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t new_schema_version,
    const ObForeignKeyInfo &foreign_key_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  // insert into __all_foreign_key_history
  if (OB_FAIL(sql.assign_fmt(
      "INSERT INTO %s(tenant_id,foreign_key_id,schema_version,is_deleted,child_table_id,parent_table_id)"
      " VALUES(%lu,%lu,%ld,%ld,%lu,%lu)",
      OB_ALL_FOREIGN_KEY_HISTORY_TNAME,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_info.foreign_key_id_),
      new_schema_version, is_deleted,
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_info.child_table_id_),
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_info.parent_table_id_)))) {
    LOG_WARN("assign insert into __all_foreign_key_history fail", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql fail", K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no row has inserted", K(ret));
  }
  // delete from __all_foreign_key
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND foreign_key_id = %lu",
                              OB_ALL_FOREIGN_KEY_TNAME,
                              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_info.foreign_key_id_)))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      // Checking affected rows here is unnecessary because
      // record maybe be deleted before in the same trans.
    }
  }
  return ret;
}

int ObTableSqlService::delete_from_all_foreign_key_column(ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t foreign_key_id,
    const uint64_t child_column_id,
    const uint64_t parent_column_id,
    const uint64_t fk_column_pos,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  // insert into __all_foreign_key_column_history
  if (OB_FAIL(sql.assign_fmt(
      "INSERT INTO %s(tenant_id,foreign_key_id,child_column_id,parent_column_id,schema_version,is_deleted,position)"
      " VALUES(%lu,%lu,%lu,%lu,%ld,%ld,%lu)",
      OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TNAME,
      ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_id),
      child_column_id, parent_column_id, new_schema_version, is_deleted, fk_column_pos))) {
    LOG_WARN("assign insert into __all_foreign_key_column_history fail", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql fail", K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no row has inserted", K(ret));
  }
  if (OB_SUCC(ret)) {
    // delete from __all_foreign_key_column
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND foreign_key_id = %lu AND child_column_id = %lu AND parent_column_id = %lu",
                              OB_ALL_FOREIGN_KEY_COLUMN_TNAME,
                              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_id),
                              child_column_id, parent_column_id))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      // Checking affected rows here is unnecessary because
      // record maybe be deleted before in the same trans.
    }
  }
  return ret;
}

// drop fk child table or drop fk child table into recyclebin will come here
int ObTableSqlService::delete_foreign_key(
    common::ObISQLClient &sql_client, const ObTableSchema &table_schema,
    const int64_t new_schema_version, const bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema.get_foreign_key_infos();
  uint64_t tenant_id = table_schema.get_tenant_id();
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
    const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
    /*When the parent table or child table is deleted,
     *the foreign key relationship should be deleted.
     *If the parent table is deleted under foreign_key_checks=off,
     *the foreign key relationship will be mocked by the child table
     */
    uint64_t foreign_key_id = foreign_key_info.foreign_key_id_;
    if (OB_FAIL(delete_from_all_foreign_key(sql_client, tenant_id, new_schema_version, foreign_key_info))) {
      LOG_WARN("failed to delete __all_foreign_key_history", K(table_schema), K(ret));
    } else if (OB_UNLIKELY(foreign_key_info.child_column_ids_.count() != foreign_key_info.parent_column_ids_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("child column num and parent column num should be equal", K(ret),
                K(foreign_key_info.child_column_ids_.count()),
                K(foreign_key_info.parent_column_ids_.count()));
    } else if (table_schema.get_table_id() == foreign_key_info.child_table_id_) {
      //The column table needs to be updated only when the child table is deleted
      for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.child_column_ids_.count(); j++) {
        // delete from __all_foreign_key_column_history
        uint64_t child_column_id = foreign_key_info.child_column_ids_.at(j);
        uint64_t parent_column_id = foreign_key_info.parent_column_ids_.at(j);
        if (OB_FAIL(delete_from_all_foreign_key_column(
            sql_client, tenant_id, foreign_key_id, child_column_id, parent_column_id, j + 1 /* fk_column_pos */, new_schema_version))) {
          LOG_WARN("failed to delete __all_foreign_key_column_history", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_check_constraint_state(
    common::ObISQLClient &sql_client,
    const ObTableSchema &table,
    const ObConstraint &cst)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);

  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else if (is_inner_table(table.get_table_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys table doesn't have check constraints", K(ret), K(table));
  } else {
    dml.reset();
    if (!cst.get_is_modify_rely_flag() // not modify rely attribute
        && !cst.get_is_modify_enable_flag() // nor enable attribute
        && !cst.get_is_modify_validate_flag() // nor validate attribute
        && !cst.get_is_modify_check_expr()) { // nor check constraint expr
      // do nothing
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, cst.get_table_id())))
        || OB_FAIL(dml.add_pk_column("constraint_id", cst.get_constraint_id()))
        || OB_FAIL(dml.add_column("schema_version", cst.get_schema_version()))
        || OB_FAIL(dml.add_column("check_expr", ObHexEscapeSqlStr(cst.get_check_expr())))
        || OB_FAIL(dml.add_gmt_modified())
        ) {
      LOG_WARN("failed to add column", K(ret));
    } else if (cst.get_is_modify_rely_flag() && OB_FAIL(dml.add_column("rely_flag", cst.get_rely_flag()))) {
      LOG_WARN("failed to add rely_flag column", K(ret));
    } else if (cst.get_is_modify_enable_flag() && OB_FAIL(dml.add_column("enable_flag", cst.get_enable_flag()))) {
      LOG_WARN("failed to add enable_flag column", K(ret));
    } else if (cst.get_is_modify_validate_flag() && OB_FAIL(dml.add_column("validate_flag", cst.get_validate_flag()))) {
      LOG_WARN("failed to add validate_flag column", K(ret));
    } else {
      int64_t affected_rows = 0;
      uint64_t table_id = cst.get_table_id();
      if (OB_FAIL(exec_update(sql_client, tenant_id, table_id,
                              OB_ALL_CONSTRAINT_TNAME, dml, affected_rows))) {
        LOG_WARN("exec update failed", K(ret));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(affected_rows), K(ret));
      } else if (OB_FAIL(add_single_constraint(sql_client, cst,
                                        true, /* only_history */
                                        false, /* need_to_deal_with_cst_cols */
                                        false /* do_cst_revise */))) {
        SHARE_SCHEMA_LOG(WARN, "add_single_constraint failed", K(ret), K(cst));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = tenant_id;
      opt.database_id_ = table.get_database_id();
      opt.tablegroup_id_ = table.get_tablegroup_id();
      opt.table_id_ = table.get_table_id();
      opt.op_type_ = OB_DDL_ALTER_TABLE;
      opt.schema_version_ = cst.get_schema_version();
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  }

  return ret;
}

// foreign_key_columns will not be updated unless replacing the mock fk parent table with a real fk parent table
int ObTableSqlService::update_foreign_key_columns(
    common::ObISQLClient &sql_client, const uint64_t tenant_id,
    const ObForeignKeyInfo &ori_foreign_key_info, const ObForeignKeyInfo &new_foreign_key_info,
    const int64_t new_schema_version_1, const int64_t new_schema_version_2)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(drop_foreign_key_columns(sql_client, tenant_id, ori_foreign_key_info, new_schema_version_1))) {
      LOG_WARN("failed to drop_foreign_key_columns", K(ret), K(tenant_id), K(ori_foreign_key_info), K(new_schema_version_1));
    } else if (OB_FAIL(add_foreign_key_columns(sql_client, tenant_id, new_foreign_key_info, new_schema_version_2, false))) {
      LOG_WARN("failed to add_foreign_key_columns", K(ret), K(tenant_id), K(new_foreign_key_info), K(new_schema_version_2));
    }
  }
  return ret;
}

int ObTableSqlService::update_foreign_key_state(common::ObISQLClient &sql_client, const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table.get_foreign_key_infos();
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); ++i) {
    const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
    if (!foreign_key_info.is_modify_fk_state_) {
      // skip
    } else {
      dml.reset();
      int64_t affected_rows = 0;
      //UPDATE is not used to update __all_foreign_key because the parent table may have deleted the record in advance
      if (OB_FAIL(gen_foreign_key_dml(exec_tenant_id, tenant_id, foreign_key_info, dml))) {
          LOG_WARN("failed to gen foreign key dml", K(ret));
      } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_FOREIGN_KEY_TNAME, dml, affected_rows))) {
        LOG_WARN("exec update failed", K(ret));
      } else if (affected_rows > 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(affected_rows), K(ret));
      } else if (OB_FAIL(dml.add_column("schema_version", table.get_schema_version()))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(dml.add_column("is_deleted", false))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_FOREIGN_KEY_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(update_data_table_schema_version(sql_client, tenant_id,
                    foreign_key_info.parent_table_id_, table.get_in_offline_ddl_white_list()))) {
          LOG_WARN("failed to update parent table schema version", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableSqlService::add_foreign_key(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table.get_foreign_key_infos();
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else if (is_inner_table(table.get_table_id())) {
    // To avoid cyclic dependence
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("should not be here", K(ret), K(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
    const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
    const int64_t is_deleted = only_history ? 1 : 0;
    if (foreign_key_info.is_modify_fk_state_) {
      continue;
    } else if (!foreign_key_info.is_parent_table_mock_) {
      // If parent table is mock, it may not exist. And we will deal with mock fk parent table after add_foreign_key.
      if (OB_FAIL(update_data_table_schema_version(sql_client, tenant_id,
          table.get_table_id() == foreign_key_info.child_table_id_ ? foreign_key_info.parent_table_id_ : foreign_key_info.child_table_id_, table.get_in_offline_ddl_white_list()))) {
        LOG_WARN("failed to update parent table schema version", K(ret), K(foreign_key_info));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(gen_foreign_key_dml(exec_tenant_id, tenant_id, foreign_key_info, dml))) {
       LOG_WARN("failed to gen foreign key dml", K(ret));
    } else {
      if (!only_history) {
        if (OB_FAIL(exec.exec_insert(OB_ALL_FOREIGN_KEY_TNAME, dml, affected_rows))) {
          LOG_WARN("failed to insert foreign key", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(dml.add_column("schema_version", table.get_schema_version()))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(exec.exec_insert(OB_ALL_FOREIGN_KEY_HISTORY_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_foreign_key_columns(sql_client, tenant_id, foreign_key_info, table.get_schema_version(), only_history))) {
          LOG_WARN("fail to add_foreign_key_columns", K(ret), K(foreign_key_info), K(only_history));
        }
      }
    }
  }

  return ret;
}

int ObTableSqlService::add_foreign_key_columns(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObForeignKeyInfo &foreign_key_info,
    const int64_t new_schema_version,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  const int64_t is_deleted = only_history ? 1 : 0;
  if (OB_UNLIKELY(foreign_key_info.child_column_ids_.count() != foreign_key_info.parent_column_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("child column num and parent column num should be equal", K(ret),
             K(foreign_key_info.child_column_ids_.count()),
             K(foreign_key_info.parent_column_ids_.count()));
  } else {
    uint64_t foreign_key_id = foreign_key_info.foreign_key_id_;
    for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.child_column_ids_.count(); j++) {
      uint64_t child_column_id = foreign_key_info.child_column_ids_.at(j);
      uint64_t parent_column_id = foreign_key_info.parent_column_ids_.at(j);
      if (OB_FAIL(gen_foreign_key_column_dml(exec_tenant_id, tenant_id, foreign_key_id,
                                             child_column_id, parent_column_id, j + 1, dml))) {
        LOG_WARN("failed to gen foreign key column dml", K(ret));
      } else {
        if (!only_history) {
          if (OB_FAIL(exec.exec_insert(OB_ALL_FOREIGN_KEY_COLUMN_TNAME, dml, affected_rows))) {
            LOG_WARN("failed to insert foreign key column", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(dml.add_column("schema_version", new_schema_version))) {
            LOG_WARN("add column failed", K(ret));
          } else if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
            LOG_WARN("add column failed", K(ret));
          } else if (OB_FAIL(exec.exec_insert(OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
            LOG_WARN("execute insert failed", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

// description: alter table drop foreign key
// @param [in] sql_client : Normally, sql_client here is ObDDLSQLTransaction which is inherited from ObISQLClient.
// @param [in] table_schema
// @param [in] foreign_key_name
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObTableSqlService::drop_foreign_key(
    const int64_t new_schema_version,
    ObISQLClient &sql_client,
    const ObTableSchema &table_schema,
    const ObForeignKeyInfo *foreign_key_info,
    const bool parent_table_in_offline_ddl_white_list)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(foreign_key_info)) {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    uint64_t tenant_id = table_schema.get_tenant_id();
    int64_t affected_rows = 0;
    if (OB_FAIL(gen_foreign_key_dml(exec_tenant_id, tenant_id, *foreign_key_info, dml))) {
      LOG_WARN("failed to gen foreign key dml", K(ret));
    } else if (OB_FAIL(exec.exec_delete(OB_ALL_FOREIGN_KEY_TNAME, dml, affected_rows))) {
      LOG_WARN("failed to delete foreign key", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
    } else if (OB_FAIL(dml.add_column("schema_version", new_schema_version))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.add_column("is_deleted", 1))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_FOREIGN_KEY_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
    } else if (OB_FAIL(drop_foreign_key_columns(sql_client, tenant_id, *foreign_key_info, new_schema_version))) {
      LOG_WARN("fail to drop_foreign_key_columns", K(affected_rows), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_data_table_schema_version(sql_client,
          tenant_id, foreign_key_info->child_table_id_, table_schema.get_in_offline_ddl_white_list()))) {
        LOG_WARN("failed to update child table schema version", K(ret));
      } else if (OB_FAIL(update_data_table_schema_version(sql_client,
                 tenant_id, foreign_key_info->parent_table_id_, parent_table_in_offline_ddl_white_list))) {
        LOG_WARN("failed to update parent table schema version", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSqlService::drop_foreign_key_columns(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObForeignKeyInfo &foreign_key_info,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.child_column_ids_.count(); j++) {
    if (OB_FAIL(delete_from_all_foreign_key_column(sql_client, tenant_id, foreign_key_info.foreign_key_id_,
        foreign_key_info.child_column_ids_.at(j), foreign_key_info.parent_column_ids_.at(j), j + 1 /* fk_column_pos */, new_schema_version))) {
      LOG_WARN("failed to delete __all_foreign_key_column_history", K(ret), K(foreign_key_info));
    }
  }
  return ret;
}

int ObTableSqlService::check_table_options(const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  rootserver::ObRootService *root_service = GCTX.root_service_;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  }
  return ret;
}

// this interface have been used by parallel set comment and set kv_attribute(for hbase)
// since parallel ddl have to allocate schema version previously
// any modification of this interface should think the times of generate schema version carefully
int ObTableSqlService::only_update_table_options(ObISQLClient &sql_client,
                                                 ObTableSchema &new_table_schema,
                                                 share::schema::ObSchemaOperationType operation_type,
                                                 const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_update_table_options_(sql_client, new_table_schema))) {
    LOG_WARN("fail to do inner update table", KR(ret), KPC(ddl_stmt_str));
  } else {
    ObSchemaOperation opt;
    if (nullptr != ddl_stmt_str) {
      opt.ddl_stmt_str_ = *ddl_stmt_str;
    }
    opt.tenant_id_ = new_table_schema.get_tenant_id();
    opt.database_id_ = new_table_schema.get_database_id();
    opt.tablegroup_id_ = new_table_schema.get_tablegroup_id();
    opt.table_id_ = new_table_schema.get_table_id();
    opt.op_type_ = operation_type;
    opt.schema_version_ = new_table_schema.get_schema_version();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", KR(ret), K(opt));
    }
  }
  return ret;
}

int ObTableSqlService::inner_update_table_options_(ObISQLClient &sql_client,
                                       const ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  uint64_t table_id = new_table_schema.get_table_id();
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const bool update_object_status_ignore_version = false;
  if (OB_FAIL(check_ddl_allowed(new_table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(new_table_schema));
  } else if (OB_FAIL(gen_table_options_dml(exec_tenant_id, new_table_schema, update_object_status_ignore_version, dml))) {
    LOG_WARN("gen table options dml failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char *table_name = nullptr;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, tenant_id, table_id,
                                   table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", KR(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", KR(ret), K(affected_rows));
    }
  }
  // add to __all_table_history table
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, new_table_schema, update_object_status_ignore_version, only_history))) {
      LOG_WARN("add table failed", KR(ret), K(new_table_schema), K(only_history));
    }
  }
  return ret;
}

int ObTableSqlService::update_table_schema_version(ObISQLClient &sql_client,
                                                   const ObTableSchema &table_schema,
                                                   share::schema::ObSchemaOperationType operation_type,
                                                   const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = table_schema.get_table_id();
  uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table_schema.get_tenant_id());
  const bool update_object_status_ignore_version = false;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_ddl_allowed(table_schema))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table_schema));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, table_id)))
        || OB_FAIL(dml.add_column("schema_version", table_schema.get_schema_version()))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char *table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, tenant_id, table_id,
                                   table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(ret), K(affected_rows));
    }
  }
  // add to __all_table_history table
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, table_schema, update_object_status_ignore_version, only_history))) {
      LOG_WARN("add_table failed", K(table_schema), K(only_history), K(ret));
    }
  }
  // log operation
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    if (NULL != ddl_stmt_str) {
      opt.ddl_stmt_str_ = *ddl_stmt_str;
    }
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = table_schema.get_database_id();
    opt.tablegroup_id_ = table_schema.get_tablegroup_id();
    opt.table_id_ = table_schema.get_table_id();
    opt.op_type_ = operation_type;
    opt.schema_version_ = table_schema.get_schema_version();
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::check_ddl_allowed(const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  if (!table_schema.check_can_do_ddl()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table_sql_service", K(table_schema.get_table_mode_struct()),
        K(table_schema.get_in_offline_ddl_white_list()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "execute ddl while table is executing offline ddl");
  }
  return ret;
}

int ObTableSqlService::delete_from_all_column_usage(ObISQLClient &sql_client,
                                                    const uint64_t tenant_id,
                                                    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                  exec_tenant_id, tenant_id))) ||
            OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                  exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                                 OB_ALL_COLUMN_USAGE_TNAME,
                                 dml, affected_rows))) {
    LOG_WARN("exec delete failed", K(ret));
  }
  return ret;
}

int ObTableSqlService::delete_from_all_monitor_modified(ObISQLClient &sql_client,
                                                        const uint64_t tenant_id,
                                                        const uint64_t table_id,
                                                        const ObSqlString *extra_condition)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                  exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(
                                  exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_NOT_NULL(extra_condition) &&
             OB_FAIL(dml.get_extra_condition().assign(*extra_condition))) {
    LOG_WARN("fail to assign extra condition", K(ret));
  } else if (OB_FAIL(exec_delete(sql_client, tenant_id, table_id,
                                 OB_ALL_MONITOR_MODIFIED_TNAME,
                                 dml, affected_rows))) {
    LOG_WARN("exec delete failed", K(ret));
  }
  return ret;
}

int ObTableSqlService::batch_add_column_groups_for_create_table(ObISQLClient &sql_client,
                                                                const ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (tables.empty()) {
  } else {
    common::ObTimeGuard time_guard("batch_add_column_groups_for_create_table", 1_ms);
    const uint64_t tenant_id = tables.at(0).get_tenant_id();
    ObDMLSqlSplicer cg_dml;
    ObDMLSqlSplicer cg_history_dml;
    ObDMLSqlSplicer mapping_dml;
    ObDMLSqlSplicer mapping_history_dml;
    int64_t column_group_cnt = 0;
    int64_t mapping_cnt = 0;
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObTableSchema &table = tables.at(i);
      if (table.get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id not same", KR(ret), K(table), K(tenant_id));
      } else if (OB_FAIL(append_column_group_dml_for_create_table(table,
                  data_version, cg_dml, cg_history_dml, mapping_dml, mapping_history_dml,
                  column_group_cnt, mapping_cnt))) {
        ObCStringHelper helper;
        LOG_WARN("append column group dml failed", KR(ret), "table", helper.convert(table));
      }
    }
    if (OB_SUCC(ret)) {
      time_guard.click("generate_dml");
      if (FAILEDx(exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_GROUP_TNAME, cg_dml, column_group_cnt))) {
        LOG_WARN("failed to insert all_column_group", KR(ret), K(tenant_id), K(column_group_cnt));
      } else if (FALSE_IT(time_guard.click("insert_all_column_group"))) {
      } else if (FAILEDx(exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_GROUP_HISTORY_TNAME, cg_history_dml, column_group_cnt))) {
        LOG_WARN("failed to insert all_column_group_history", KR(ret), K(tenant_id), K(column_group_cnt));
      } else if (FALSE_IT(time_guard.click("insert_all_column_group_history"))) {
      } else if (FAILEDx(exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_GROUP_MAPPING_TNAME, mapping_dml, mapping_cnt))) {
        LOG_WARN("failed to insert all_column_group_mapping", KR(ret), K(tenant_id), K(mapping_cnt));
      } else if (FALSE_IT(time_guard.click("insert_all_column_group_mapping"))) {
      } else if (FAILEDx(exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TNAME, mapping_history_dml, mapping_cnt))) {
        LOG_WARN("failed to insert all_column_group_mapping_history", KR(ret), K(tenant_id), K(mapping_cnt));
      } else if (FALSE_IT(time_guard.click("insert_all_column_group_mapping_history"))) {
      }
    }
  }
  return ret;
}

int ObTableSqlService::append_column_group_dml_for_create_table(const ObTableSchema &table,
                                                                const uint64_t data_version,
                                                                ObDMLSqlSplicer &cg_dml,
                                                                ObDMLSqlSplicer &cg_history_dml,
                                                                ObDMLSqlSplicer &mapping_dml,
                                                                ObDMLSqlSplicer &mapping_history_dml,
                                                                int64_t &column_group_cnt,
                                                                int64_t &mapping_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_column_store_valid(table, data_version))) {
    LOG_WARN("fail to check column store valid", KR(ret), K(table), K(data_version));
  } else if ((data_version < DATA_VERSION_4_3_0_0) || (table.get_column_group_count() < 1)) {
    // skip, no need to persist column_group
  } else if (!table.is_column_store_supported()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("we can persist column_group info only when column_store=true", KR(ret), K(table));
  } else {
    const int64_t schema_version = table.get_schema_version();
    ObTableSchema::const_column_group_iterator it_begin = table.column_group_begin();
    ObTableSchema::const_column_group_iterator it_end = table.column_group_end();
    for (; OB_SUCC(ret) && it_begin != it_end; ++it_begin) {
      const ObColumnGroupSchema *column_group = *it_begin;
      if (OB_ISNULL(column_group)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("column_group schema should not be null", KR(ret), K(table));
      } else if (data_version < DATA_VERSION_4_3_0_0
                 && column_group->get_compressor_type() == ObCompressorType::ZLIB_LITE_COMPRESSOR) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("zlib_lite_1.0 not support before 4.3", KR(ret), K(table), KPC(column_group));
      } else if (OB_FAIL(gen_column_group_dml(table, *column_group, false /*not history*/,
                                              false /*not deleted*/, schema_version, cg_dml))) {
        LOG_WARN("fail to gen column_group_dml", KR(ret), K(table), KPC(column_group));
      } else if (OB_FAIL(gen_column_group_dml(table, *column_group, true /*history*/,
                                              false /*not deleted*/, schema_version, cg_history_dml))) {
        LOG_WARN("fail to gen column_group_history_dml", KR(ret), K(table), KPC(column_group));
      } else {
        ++column_group_cnt;
        const int64_t column_id_cnt = column_group->get_column_id_count();
        if (column_id_cnt > 0) {
          for (int64_t idx = 0; OB_SUCC(ret) && idx < column_id_cnt; ++idx) {
            uint64_t tmp_column_id = UINT64_MAX;
            if (OB_FAIL(column_group->get_column_id(idx, tmp_column_id))) {
              LOG_WARN("fail to get column id", KR(ret), K(idx), KPC(column_group));
            } else {
              const int64_t column_id = static_cast<int64_t>(tmp_column_id);
              if (OB_FAIL(gen_column_group_mapping_dml(table, *column_group,
                      column_id, false /*not history*/,
                      false /*not deleted*/, schema_version, mapping_dml))) {
                LOG_WARN("fail to gen column_group_mapping_dml", KR(ret), K(table), K(column_id));
              } else if (OB_FAIL(gen_column_group_mapping_dml(table, *column_group,
                      column_id, true /*history*/,
                      false /*not deleted*/, schema_version, mapping_history_dml))) {
                LOG_WARN("fail to gen column_group_mapping_history_dml", KR(ret), K(table), K(column_id));
              } else {
                ++mapping_cnt;
              }
            }
          }
        } else if (column_group->get_column_group_type() != ObColumnGroupType::DEFAULT_COLUMN_GROUP) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid column group type without column ids", KR(ret), KPC(column_group));
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::add_column_groups(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(table.get_tenant_id(), data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(table));
  } else if (OB_FAIL(check_column_store_valid(table, data_version))) {
    LOG_WARN("fail to check column store valid", KR(ret));
  } else if ((data_version < DATA_VERSION_4_3_0_0) || (table.get_column_group_count() < 1)) {
    // skip, no need to persist column_group
  } else if (!table.is_column_store_supported()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("we can persist column_group info only when column_store=true", KR(ret), K(table));
  } else if ((!only_history) && OB_FAIL(exec_insert_column_group(sql_client, table, schema_version, false))) {
    LOG_WARN("fail to insert column group", KR(ret), K(table));
  } else if (OB_FAIL(exec_insert_column_group(sql_client, table, schema_version, true))) {
    LOG_WARN("fail to insert column group history", KR(ret), K(table));
  } else if ((!only_history) && OB_FAIL(exec_insert_column_group_mapping(sql_client, table, schema_version, false))) {
    LOG_WARN("fail to insert column group mapping", KR(ret), K(table));
  } else if (OB_FAIL(exec_insert_column_group_mapping(sql_client, table, schema_version, true))) {
    LOG_WARN("fail to insert column group mapping", KR(ret), K(table));
  }
  return ret;
}

int ObTableSqlService::insert_column_ids_into_column_group(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    const ObIArray<uint64_t> &column_ids,
    const ObColumnGroupSchema &column_group,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (!table.is_column_store_supported()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table not support column store", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(table.get_tenant_id(), data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(table));
  } else if (OB_FAIL(check_column_store_valid(table, data_version))) {
    LOG_WARN("fail to check column store valid", KR(ret));
  } else if (!only_history && OB_FAIL(exec_insert_column_group_mapping(sql_client, table, schema_version, column_group, column_ids, true))) {
    LOG_WARN("fail to exec_insert_column_group_mapping", K(ret), K(column_ids));
  } else if (OB_FAIL(exec_insert_column_group_mapping(sql_client, table, schema_version, column_group, column_ids, false))) {
    LOG_WARN("fail to exec_insert_column_group_mapping", K(ret), K(column_ids));
  }

  return ret;
}

int ObTableSqlService::check_column_store_valid(const ObTableSchema &table, const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  if (data_version < DATA_VERSION_4_3_0_0) {
    if (table.is_column_store_supported()
        || table.get_max_used_column_group_id() > COLUMN_GROUP_START_ID
        || table.get_column_group_count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("can't set column_store or add column_group in current version", KR(ret), K(table));
    } else if (table.get_column_group_count() == 1) {
      const ObColumnGroupSchema *cg = *(table.column_group_begin());
      if (OB_NOT_NULL(cg) && (cg->get_column_group_type() != ObColumnGroupType::DEFAULT_COLUMN_GROUP)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can't support other type column_group in current_version", KR(ret), K(table));
      }
    }
  }
  return ret;
}

int ObTableSqlService::exec_insert_column_group(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    bool is_history)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  const int64_t cg_cnt = table.get_column_group_count();
  if (cg_cnt > 0) {
    ObDMLSqlSplicer dml;
    ObSqlString sql;
    const char* tname = is_history ? OB_ALL_COLUMN_GROUP_HISTORY_TNAME : OB_ALL_COLUMN_GROUP_TNAME;

    const uint64_t tenant_id = table.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObTableSchema::const_column_group_iterator it_begin = table.column_group_begin();
    ObTableSchema::const_column_group_iterator it_end = table.column_group_end();
    const ObColumnGroupSchema *column_group = NULL;

    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("failed to get data version", K(ret));
    }

    for (; OB_SUCC(ret) && (it_begin != it_end); ++it_begin) {
      column_group = *it_begin;
      if (OB_ISNULL(column_group)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("column_group schema should not be null", KR(ret));
      } else if (data_version < DATA_VERSION_4_3_0_0
                 && column_group->get_compressor_type() == ObCompressorType::ZLIB_LITE_COMPRESSOR) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("zlib_lite_1.0 not support before 4.3", K(ret), K(table));
      } else if (OB_FAIL(gen_column_group_dml(table, *column_group, is_history,
                                              false /*not deleted*/, schema_version, dml))){
        LOG_WARN("fail to gen column_group_dml", K(ret));
      }
    }

    int64_t affected_rows = 0;
    if (FAILEDx(dml.splice_batch_insert_sql(tname, sql))) {
      LOG_WARN("fail to splice batch update sql", KR(ret), K(sql));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
    } else if (affected_rows != cg_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(cg_cnt));
    }
  }
  return ret;
}

int ObTableSqlService::exec_insert_column_group_mapping(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    const ObColumnGroupSchema &column_group,
    const ObIArray<uint64_t> &column_ids,
    const bool is_history)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  const char* tname = is_history ? OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TNAME : OB_ALL_COLUMN_GROUP_MAPPING_TNAME;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  const uint64_t tmp_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
  const uint64_t tmp_table_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id);
  const uint64_t tmp_cg_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_group.get_column_group_id());

  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
    const int64_t IS_DELETED = 0;
    uint64_t tmp_column_id = column_ids.at(i);
    if (OB_FAIL(gen_column_group_mapping_dml(table, column_group, tmp_column_id, is_history,
                                             false /*not delete*/, schema_version, dml))) {
      LOG_WARN("fail to finish row", K(ret), K(i), K(column_group));
    }
  }

  int64_t affected_rows = 0;
  if (FAILEDx(dml.splice_batch_insert_sql(tname, sql))) {
    LOG_WARN("fail to splice batch update sql", KR(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
  } else if (affected_rows != column_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(column_ids.count()));
  }

  return ret;
}

int ObTableSqlService::exec_insert_column_group_mapping(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    bool is_history)
{
  int ret = OB_SUCCESS;

  ObTableSchema::const_column_group_iterator it_begin = table.column_group_begin();
  ObTableSchema::const_column_group_iterator it_end = table.column_group_end();
  const ObColumnGroupSchema *column_group = NULL;
  for (; OB_SUCC(ret) && (it_begin != it_end); ++it_begin) {
    column_group = *it_begin;
    if (OB_ISNULL(column_group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column_group schema should not be null", KR(ret));
    } else {
      const ObColumnGroupType cg_type = column_group->get_column_group_type();
      const int64_t column_id_cnt = column_group->get_column_id_count();
      // only default type column_group can have empty column_ids.
      if (column_id_cnt > 0) {
        ObArray<uint64_t> column_ids;
        for (int64_t i = 0; OB_SUCC(ret) && (i < column_id_cnt); ++i) {
          uint64_t tmp_column_id = UINT64_MAX;
          if (OB_FAIL(column_group->get_column_id(i, tmp_column_id))) {
            LOG_WARN("fail to get column id", KR(ret), K(i), K(column_id_cnt));
          } else if (OB_FAIL(column_ids.push_back(tmp_column_id))) {
            LOG_WARN("fail to push back column id", KR(ret), K(i), K(column_ids));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(exec_insert_column_group_mapping(sql_client, table, schema_version, *column_group, column_ids, is_history))) {
          LOG_WARN("fail to exec_insert_column_group_mapping", KR(ret), K(table), KPC(column_group), K(column_ids));
        }
      } else if (cg_type != ObColumnGroupType::DEFAULT_COLUMN_GROUP) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(column_id_cnt), KPC(column_group));
      }
    }
  }

  return ret;
}

int ObTableSqlService::gen_column_group_dml(const ObTableSchema &table_schema,
                                            const ObColumnGroupSchema &column_group_schema,
                                            const bool is_history,
                                            const bool is_deleted,
                                            const int64_t schema_version,
                                            ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || !column_group_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(column_group_schema));
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = table_schema.get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    const uint64_t tmp_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
    const uint64_t tmp_table_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id);
    const uint64_t tmp_cg_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_group_schema.get_column_group_id());

    if (OB_FAIL(dml.add_pk_column("tenant_id", tmp_tenant_id))
      || OB_FAIL(dml.add_pk_column("table_id", tmp_table_id))
      || OB_FAIL(dml.add_pk_column("column_group_id", tmp_cg_id))
      || (!(is_history && is_deleted) && OB_FAIL(dml.add_column("column_group_name", ObHexEscapeSqlStr(column_group_schema.get_column_group_name().ptr()))))
      || OB_FAIL(dml.add_column("column_group_type", column_group_schema.get_column_group_type()))
      || OB_FAIL(dml.add_column("block_size", column_group_schema.get_block_size()))
      || OB_FAIL(dml.add_column("compressor_type", column_group_schema.get_compressor_type()))
      || OB_FAIL(dml.add_column("row_store_type", column_group_schema.get_row_store_type()))
      || (is_history && OB_FAIL(dml.add_column("is_deleted", is_deleted)))
      || (is_history && OB_FAIL(dml.add_column("schema_version", schema_version)))) {
      LOG_WARN("fail to build column column dml", K(ret));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("fail to finish column group dml row", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::gen_column_group_mapping_dml(const ObTableSchema &table_schema,
                                                    const ObColumnGroupSchema &column_group_schema,
                                                    const int64_t column_id,
                                                    const bool is_history,
                                                    const bool is_deleted,
                                                    const int64_t schema_version,
                                                    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || !column_group_schema.is_valid() || schema_version == OB_INVALID_SCHEMA_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(column_group_schema), K(schema_version));
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = table_schema.get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

    const uint64_t tmp_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
    const uint64_t tmp_table_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id);
    const uint64_t tmp_cg_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_group_schema.get_column_group_id());
    if (OB_FAIL(dml.add_pk_column("tenant_id", tmp_tenant_id))
        || OB_FAIL(dml.add_pk_column("table_id", tmp_table_id))
        || OB_FAIL(dml.add_pk_column("column_group_id", tmp_cg_id))
        || OB_FAIL(dml.add_pk_column("column_id", column_id))
        || (is_history && OB_FAIL(dml.add_column("is_deleted", is_deleted ? 1: 0)))
        || (is_history && OB_FAIL(dml.add_column("schema_version", schema_version)))) {
      LOG_WARN("fail to add info to column group mapping dml", K(ret), K(table_schema), K(column_group_schema));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("dml splicer fail to finish row", K(ret));
    }
  }
  return ret;
}


int ObTableSqlService::delete_from_column_group(ObISQLClient &sql_client,
                                                const ObTableSchema &table_schema,
                                                const int64_t new_schema_version,
                                                const bool is_history)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || new_schema_version == OB_INVALID_SCHEMA_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", K(ret), K(table_schema), K(new_schema_version));
  } else {
    ObDMLSqlSplicer dml;
    int64_t affect_rows = 0;
    ObSqlString sql;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table_schema.get_tenant_id());
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObTableSchema::const_column_group_iterator iter_begin = table_schema.column_group_begin();
    ObTableSchema::const_column_group_iterator iter_end = table_schema.column_group_end();
    if (table_schema.get_column_group_count() == 0) {
      /* skip table has not column*/
    } else if (is_history) { /* remove from __all_column_group_history*/
      for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
        const ObColumnGroupSchema *column_group = *iter_begin;
        if (OB_ISNULL(column_group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column group should not be null", K(ret), K(table_schema));
        } else if (OB_FAIL(gen_column_group_dml(table_schema, *column_group, is_history,
                                              true /* is_delete */, new_schema_version, dml))) {
          LOG_WARN("fail to write dml for __all_column_group", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_COLUMN_GROUP_HISTORY_TNAME, sql))) {
        LOG_WARN("fail to splice batch insert sql", K(ret), K(sql), K(table_schema));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affect_rows))) {
        LOG_WARN("fail to insert deleted record to all column group history", K(ret));
      } else if (table_schema.get_column_group_count() != affect_rows){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to drop all column group columns", K(ret), K(affect_rows), K(table_schema));
      }
    } else {
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table_schema.get_tenant_id())))
          || OB_FAIL(dml.add_pk_column("table_id", table_schema.get_table_id()))) {
        LOG_WARN("fail to gen dml to delete from __all_column_group", K(ret));
      } else if (OB_FAIL(exec.exec_delete(OB_ALL_COLUMN_GROUP_TNAME, dml, affect_rows))) {
        LOG_WARN("fail to insert deleted record to all column group history", K(ret));
      } else if (table_schema.get_column_group_count() != affect_rows && affect_rows != 0) {
        /*the table upgrade from 4.2 or less has not record in inner table, allow 0 row affected*/
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("fail to drop all column group columns", K(ret), K(affect_rows), K(table_schema));
      }
    }
  }
  return ret;
}

int ObTableSqlService::delete_from_column_group_mapping(ObISQLClient &sql_client,
                                                        const ObTableSchema &table_schema,
                                                        const int64_t schema_version,
                                                        const bool is_history)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", K(ret), K(table_schema));
  } else {
    ObDMLSqlSplicer dml;
    int64_t cg_mapping_cnt = 0;
    int64_t affect_rows = 0;
    ObSqlString sql;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table_schema.get_tenant_id());
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObTableSchema::const_column_group_iterator iter_begin = table_schema.column_group_begin();
    ObTableSchema::const_column_group_iterator iter_end = table_schema.column_group_end();
    /* count affect rows & form dml for history table*/
    for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
      const ObColumnGroupSchema *column_group = *iter_begin;
      cg_mapping_cnt += column_group->get_column_id_count();
      if (OB_ISNULL(column_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column group should not be null", K(ret), K(table_schema));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && is_history && i < column_group->get_column_id_count(); i++) {
          if (OB_FAIL(gen_column_group_mapping_dml(table_schema, *column_group, column_group->get_column_ids()[i],
                                                  is_history, true /* is_deleted*/, schema_version, dml))) {
            LOG_WARN("fail to write column group mapping dml", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (cg_mapping_cnt == 0) {
        /*skip table don't have column group like view*/
    } else if (is_history) {
      if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TNAME, sql))) {
        LOG_WARN("fail to splice batch insert_sql", K(ret), K(sql), K(table_schema));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affect_rows))) {
        LOG_WARN("fail to wirte rows into __all_column_group_mapping_history", K(ret), K(sql));
      } else if (cg_mapping_cnt != affect_rows){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to drop in __all_column_group_mapping_history", K(ret), K(affect_rows), K(table_schema));
      }
    } else {
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                 exec_tenant_id, table_schema.get_tenant_id())))
          || OB_FAIL(dml.add_pk_column("table_id", table_schema.get_table_id()))) {
        LOG_WARN("fail to gen dml to delete from __all_column_group", K(ret));
      } else if (OB_FAIL(exec.exec_delete(OB_ALL_COLUMN_GROUP_MAPPING_TNAME, dml, affect_rows))) {
        LOG_WARN("fail to insert deleted record to all column group", K(ret));
      } else if (cg_mapping_cnt != affect_rows  && 0 != affect_rows) {
        /*the table upgrade from 4.2 or less has not record in inner table, allow 0 row affected*/
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to drop in __all_column_group_mapping", K(ret), K(affect_rows), K(table_schema));
      }
    }
  }
  return ret;
}


int ObTableSqlService::delete_column_group(ObISQLClient &sql_client,
                                           const ObTableSchema &table_schema,
                                           const int64_t schema_version)
{

  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || !table_schema.is_column_store_supported()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table", K(ret), K(table_schema));
  } else if (OB_FAIL(delete_from_column_group(sql_client, table_schema, schema_version))) {
    LOG_WARN("fail to delete from table __all_column_group", K(ret));
  } else if (OB_FAIL(delete_from_column_group(sql_client, table_schema, schema_version, true /*history table*/))) {
    LOG_WARN("fail to delete from table __all_column_group_history", K(ret));
  } else if (OB_FAIL(delete_from_column_group_mapping(sql_client, table_schema, schema_version))) {
    LOG_WARN("fail to delete from table __all_column_group_mapping", K(ret));
  } else if (OB_FAIL(delete_from_column_group_mapping(sql_client, table_schema, schema_version, true /*history*/))) {
    LOG_WARN("fail to delete from talbe __all_column_group_mapping_history", K(ret));
  }
  return ret;
}

int ObTableSqlService::update_single_column_group(ObISQLClient &sql_client,
                                                  const ObTableSchema &new_table_schema,
                                                  const ObColumnGroupSchema &ori_cg_schema,
                                                  const ObColumnGroupSchema &new_cg_schema)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affect_rows = 0;
  uint64_t compat_version = 0;
  if (!sql_client.is_active() || !new_table_schema.is_valid() ||
      !ori_cg_schema.is_valid() || !new_cg_schema.is_valid() || !new_table_schema.is_column_store_supported()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_table_schema), K(ori_cg_schema), K(new_cg_schema));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(new_table_schema.get_tenant_id(), compat_version))) {
    LOG_WARN("fail to check min data_version", K(ret), K(new_table_schema));
  } else if (OB_FAIL(check_column_store_valid(new_table_schema, compat_version))) {
    LOG_WARN("fail to check column store valid", KR(ret), K(new_table_schema), K(compat_version));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(new_table_schema.get_tenant_id());
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    /* write into __all_column_group*/
    if (OB_FAIL(gen_column_group_dml(new_table_schema, new_cg_schema, false, /* not history*/
                                     false /* not deleted*/, new_cg_schema.get_schema_version(), dml))) {
      LOG_WARN("fail to gen column group dml", K(ret));
    } else if (OB_FAIL(exec.exec_update(OB_ALL_COLUMN_GROUP_TNAME, dml, affect_rows))) {
      LOG_WARN("fail to update all column group", K(ret));
    } else if (affect_rows != (ori_cg_schema.get_column_group_name() != new_cg_schema.get_column_group_name())) {
      /* for some ddl don't change propertype, affect rows should be 0, since all_column_group has no schema version*/
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to update single row in all column group ", K(ret), K(affect_rows), K(ori_cg_schema), K(new_cg_schema));
    }

    /* write into __all_column_group_history*/
    dml.reset();
    affect_rows = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(gen_column_group_dml(new_table_schema, new_cg_schema, true, /* history table*/
                                            false/*not delete*/, new_cg_schema.get_schema_version(), dml))) {
      LOG_WARN("fail to gen column group dml", K(ret));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_COLUMN_GROUP_HISTORY_TNAME, dml, affect_rows))) {
      LOG_WARN("fail to exec dml on history table", K(ret));
    } else if (1 != affect_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affect row not equal to 1", K(ret), K(affect_rows), K(ori_cg_schema), K(new_cg_schema));
    }
  }
  return ret;
}

int ObTableSqlService::update_origin_column_group_with_new_schema(ObISQLClient &sql_client,
                                                                  const int64_t delete_schema_version,
                                                                  const int64_t insert_schema_version,
                                                                  const ObTableSchema &origin_table_schema,
                                                                  const ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = OB_INVALID_VERSION;
  uint64_t origin_tenant_id = origin_table_schema.get_tenant_id();
  uint64_t new_tenant_id = new_table_schema.get_tenant_id();
  if (OB_UNLIKELY(!sql_client.is_active()
                  || !origin_table_schema.is_valid()
                  || !new_table_schema.is_valid()
                  || origin_tenant_id != new_tenant_id
                  || !new_table_schema.is_column_store_supported())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(origin_table_schema), K(new_table_schema));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(origin_tenant_id, data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(origin_table_schema));
  } else if (OB_FAIL(check_column_store_valid(origin_table_schema, data_version))) {
    LOG_WARN("fail to check column store valid for origin table schema", KR(ret), K(origin_table_schema));
  } else if (OB_FAIL(check_column_store_valid(new_table_schema, data_version))) {
    LOG_WARN("fail to check column store valid for new table schema", KR(ret), K(new_table_schema));
  } else if (origin_table_schema.is_column_store_supported() && OB_FAIL(delete_column_group(sql_client, origin_table_schema, delete_schema_version))) {
    LOG_WARN("fail to delete column group for origin table schema", KR(ret), K(origin_table_schema));
  } else if (OB_FAIL(add_column_groups(sql_client, new_table_schema, insert_schema_version, false/*only_history*/))) {
    LOG_WARN("fail to add column groups from new table schema", KR(ret), K(new_table_schema), K(insert_schema_version));
  }
  return ret;
}

// Three scenes :
// 1. drop fk parent table
// 2. create child table with a fk references a mock fk parent table not exist
// 3. alter child table add fk references a mock fk parent table not exist
int ObTableSqlService::add_mock_fk_parent_table(
    common::ObISQLClient *sql_client,
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const bool need_update_foreign_key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL", K(ret));
  } else {
    if (OB_FAIL(insert_mock_fk_parent_table(*sql_client, mock_fk_parent_table_schema, false))) {
      LOG_WARN("failed to add mock_fk_parent_table", K(ret));
    } else if (OB_FAIL(insert_mock_fk_parent_table_column(*sql_client, mock_fk_parent_table_schema, false))) {
      LOG_WARN("failed to add mock_fk_parent_table", K(ret));
    } else if (need_update_foreign_key
               && OB_FAIL(update_foreign_key_in_mock_fk_parent_table(sql_client, mock_fk_parent_table_schema, NULL, false))) {
      // need to update fk info (such as parent table id) when drop fk parent table
      // no need to update fk info when alter child table add fk references a mock fk parent table or create child table with a fk references a mock fk parent table
      LOG_WARN("failed to update_foreign_key_in_mock_fk_parent_table", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = mock_fk_parent_table_schema.get_tenant_id();
      opt.database_id_ = mock_fk_parent_table_schema.get_database_id();
      opt.mock_fk_parent_table_id_ = mock_fk_parent_table_schema.get_mock_fk_parent_table_id();
      opt.mock_fk_parent_table_name_ = mock_fk_parent_table_schema.get_mock_fk_parent_table_name();
      opt.op_type_ = OB_DDL_CREATE_MOCK_FK_PARENT_TABLE;
      opt.schema_version_ = mock_fk_parent_table_schema.get_schema_version();
      opt.ddl_stmt_str_ = ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

// Three scenes :
// 1. create child table with a fk references a mock fk parent table existed
// 2. alter child table add fk references a mock fk parent table existed
// 3. drop fk from a child table with a fk references a mock fk parent table existed
int ObTableSqlService::alter_mock_fk_parent_table(
    common::ObISQLClient *sql_client,
    ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL", K(ret));
  } else if (MOCK_FK_PARENT_TABLE_OP_ADD_COLUMN == mock_fk_parent_table_schema.get_operation_type()) {
    if (OB_FAIL(insert_mock_fk_parent_table_column(*sql_client, mock_fk_parent_table_schema, false))) {
      LOG_WARN("failed to add columns of mock_fk_parent_table", K(ret), K(mock_fk_parent_table_schema));
    }
  } else if (MOCK_FK_PARENT_TABLE_OP_DROP_COLUMN == mock_fk_parent_table_schema.get_operation_type()) {
    if (OB_FAIL(delete_mock_fk_parent_table_column(*sql_client, mock_fk_parent_table_schema, false))) {
      LOG_WARN("failed to drop columns of mock_fk_parent_table", K(ret), K(mock_fk_parent_table_schema));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_mock_fk_parent_table_schema_version(sql_client, mock_fk_parent_table_schema))) {
      LOG_WARN("failed to add update_mock_fk_parent_table_schema_version", K(ret));
    }
  }
  return ret;
}

// Three scenes :
// 1. drop child table with a fk references a mock fk parent table existed
// 2. drop fk from a child table with a fk references a mock fk parent table existed
// 3. drop database
int ObTableSqlService::drop_mock_fk_parent_table(
    common::ObISQLClient *sql_client,
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(delete_mock_fk_parent_table_column(*sql_client, mock_fk_parent_table_schema, false))) {
    LOG_WARN("failed to drop columns of mock_fk_parent_table", K(ret), K(mock_fk_parent_table_schema));
  } else if (OB_FAIL(delete_mock_fk_parent_table(*sql_client, mock_fk_parent_table_schema, false))) {
    LOG_WARN("failed to drop the mock_fk_parent_table", K(ret), K(mock_fk_parent_table_schema));
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = mock_fk_parent_table_schema.get_tenant_id();
    opt.database_id_ = mock_fk_parent_table_schema.get_database_id();
    opt.mock_fk_parent_table_id_ = mock_fk_parent_table_schema.get_mock_fk_parent_table_id();
    opt.mock_fk_parent_table_name_ = mock_fk_parent_table_schema.get_mock_fk_parent_table_name();
    opt.op_type_ = OB_DDL_DROP_MOCK_FK_PARENT_TABLE;
    opt.schema_version_ = mock_fk_parent_table_schema.get_schema_version();
    opt.ddl_stmt_str_ = ObString();
    if (OB_FAIL(log_operation(opt, *sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

// replace mock_fk_parent_table with a real parent table
// Five scenes :
// 1. create table (as select)
// 2. create table like
// 3. rename table
// 4. alter table rename to
// 5. flashback table to before drop
// will drop mock_fk_parent_table, update foreign_key info and update foreign_key_column info
int ObTableSqlService::replace_mock_fk_parent_table(
    common::ObISQLClient *sql_client,
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const ObMockFKParentTableSchema *ori_mock_fk_parent_table_schema_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(delete_mock_fk_parent_table_column(*sql_client, mock_fk_parent_table_schema, false))) {
    LOG_WARN("failed to drop columns of mock_fk_parent_table", K(ret), K(mock_fk_parent_table_schema));
  } else if (OB_FAIL(delete_mock_fk_parent_table(*sql_client, mock_fk_parent_table_schema, false))) {
    LOG_WARN("failed to drop the mock_fk_parent_table", K(ret), K(mock_fk_parent_table_schema));
  } else if (OB_FAIL(update_foreign_key_in_mock_fk_parent_table(sql_client, mock_fk_parent_table_schema, ori_mock_fk_parent_table_schema_ptr, true))) {
    LOG_WARN("failed to add mock_fk_parent_table", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = mock_fk_parent_table_schema.get_tenant_id();
    opt.database_id_ = mock_fk_parent_table_schema.get_database_id();
    opt.mock_fk_parent_table_id_ = mock_fk_parent_table_schema.get_mock_fk_parent_table_id();
    opt.mock_fk_parent_table_name_ = mock_fk_parent_table_schema.get_mock_fk_parent_table_name();
    opt.op_type_ = OB_DDL_DROP_MOCK_FK_PARENT_TABLE;
    opt.schema_version_ = mock_fk_parent_table_schema.get_schema_version();
    opt.ddl_stmt_str_ = ObString();
    if (OB_FAIL(log_operation(opt, *sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

// will come here when alter_mock_fk_parent_table
int ObTableSqlService::update_mock_fk_parent_table_schema_version(
    common::ObISQLClient *sql_client,
    ObMockFKParentTableSchema &mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(*sql_client, exec_tenant_id);
  dml.reset();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, OB_INVALID_VERSION, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", K(ret), K(tenant_id));
  } else if (FALSE_IT(mock_fk_parent_table_schema.set_schema_version(new_schema_version))) {
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("mock_fk_parent_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_mock_fk_parent_table_id())))
        || OB_FAIL(dml.add_column("schema_version", mock_fk_parent_table_schema.get_schema_version()))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_update(OB_ALL_MOCK_FK_PARENT_TABLE_TNAME, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", K(ret), K(affected_rows));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(insert_mock_fk_parent_table(*sql_client, mock_fk_parent_table_schema, true))) {
      LOG_WARN("failed to add mock_fk_parent_table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = mock_fk_parent_table_schema.get_tenant_id();
    opt.database_id_ = mock_fk_parent_table_schema.get_database_id();
    opt.mock_fk_parent_table_id_ = mock_fk_parent_table_schema.get_mock_fk_parent_table_id();
    opt.mock_fk_parent_table_name_ = mock_fk_parent_table_schema.get_mock_fk_parent_table_name();
    opt.op_type_ = OB_DDL_ALTER_MOCK_FK_PARENT_TABLE;
    opt.schema_version_ = mock_fk_parent_table_schema.get_schema_version();
    opt.ddl_stmt_str_ = ObString();
    if (OB_FAIL(log_operation(opt, *sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_foreign_key_in_mock_fk_parent_table(
    common::ObISQLClient *sql_client,
    const ObMockFKParentTableSchema &new_mock_fk_parent_table_schema,
    const ObMockFKParentTableSchema *ori_mock_fk_parent_table_schema_ptr,
    const bool need_update_foreign_key_columns)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = new_mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(*sql_client, exec_tenant_id);
  const ObIArray<ObForeignKeyInfo> &foreign_key_infos = new_mock_fk_parent_table_schema.get_foreign_key_infos();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_SUCC(ret) && need_update_foreign_key_columns) {
    if (OB_ISNULL(ori_mock_fk_parent_table_schema_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ori_mock_fk_parent_table_schema_ptr is null", K(ret), K(ori_mock_fk_parent_table_schema_ptr), K(new_mock_fk_parent_table_schema));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, OB_INVALID_VERSION, new_schema_version))) {
      LOG_WARN("fail to gen new schema version", K(ret), K(tenant_id));
    } else {
      const ObIArray<ObForeignKeyInfo> &ori_foreign_key_infos = ori_mock_fk_parent_table_schema_ptr->get_foreign_key_infos();
      if (ori_foreign_key_infos.count() != foreign_key_infos.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the count of foreign_key_infos is not equal", K(ret), K(ori_foreign_key_infos.count()), K(foreign_key_infos.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ori_foreign_key_infos.count(); ++i) {
          if (OB_FAIL(update_foreign_key_columns(
              *sql_client, new_mock_fk_parent_table_schema.get_tenant_id(), ori_foreign_key_infos.at(i),
              foreign_key_infos.at(i), new_mock_fk_parent_table_schema.get_schema_version(), new_schema_version))) {
            LOG_WARN("update_foreign_key_columns failed", K(ret));
          }
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); ++i) {
    const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
    dml.reset();
    int64_t affected_rows = 0;
    //UPDATE is not used to update __all_foreign_key because the parent table may have deleted the record in advance
    if (OB_FAIL(gen_foreign_key_dml(exec_tenant_id, tenant_id, foreign_key_info, dml))) {
        LOG_WARN("failed to gen foreign key dml", K(ret));
    } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_FOREIGN_KEY_TNAME, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(affected_rows));
    } else if (OB_FAIL(dml.add_column("schema_version", need_update_foreign_key_columns ? new_schema_version : new_mock_fk_parent_table_schema.get_schema_version()))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.add_column("is_deleted", false))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_FOREIGN_KEY_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::insert_mock_fk_parent_table(
    common::ObISQLClient &sql_client,
     const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
     const bool only_history)
{
  int ret = OB_SUCCESS;
  const char *tname[] = {OB_ALL_MOCK_FK_PARENT_TABLE_TNAME, OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TNAME};
  const uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    ObDMLSqlSplicer dml;
    bool is_history = (0 == STRCMP(tname[i], OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TNAME));
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_MOCK_FK_PARENT_TABLE_TNAME)) {
      continue;
    } else if (OB_FAIL(format_insert_mock_table_dml_sql(mock_fk_parent_table_schema, dml, is_history))) {
      LOG_WARN("failed to format dml sql", K(ret));
    } else if (OB_FAIL(exec.exec_insert(tname[i], dml, affected_rows))) {
      LOG_WARN("failed to exec insert", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::delete_mock_fk_parent_table(
    common::ObISQLClient &sql_client,
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  const char *tname[] = {OB_ALL_MOCK_FK_PARENT_TABLE_TNAME, OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TNAME};
  const uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); ++i) {
    ObSqlString delete_mock_table_dml_sql;
    bool is_history = (0 == STRCMP(tname[i], OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TNAME));
    int64_t affected_rows = 0;
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TNAME)) {
      continue;
    } else if (OB_FAIL(format_delete_mock_table_dml_sql(mock_fk_parent_table_schema, is_history, delete_mock_table_dml_sql))) {
      LOG_WARN("failed to format column_sql and column_history_sql", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, delete_mock_table_dml_sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(delete_mock_table_dml_sql));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows is not single row", K(ret),
               K(affected_rows), K(delete_mock_table_dml_sql), K(mock_fk_parent_table_schema));
    }
  }
  return ret;
}

int ObTableSqlService::insert_mock_fk_parent_table_column(
    common::ObISQLClient &sql_client,
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  const char *tname[] = {OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TNAME, OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TNAME};
  const uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); ++i) {
    ObSqlString column_sql;
    bool is_history = (0 == STRCMP(tname[i], OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TNAME));
    int64_t affected_rows = 0;
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TNAME)) {
      continue;
    } else if (OB_FAIL(format_insert_mock_table_column_dml_sql(mock_fk_parent_table_schema, is_history, column_sql))) {
      LOG_WARN("failed to format column_sql and column_history_sql", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, column_sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(column_sql));
    } else if (affected_rows != mock_fk_parent_table_schema.get_column_array().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not equal to col count in table", K(ret),
               K(affected_rows), K(mock_fk_parent_table_schema.get_column_array().count()));
    }
  }
  return ret;
}

int ObTableSqlService::delete_mock_fk_parent_table_column(
    common::ObISQLClient &sql_client,
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  const char *tname[] = {OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TNAME, OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TNAME};
  const uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); ++i) {
    ObSqlString column_sql;
    bool is_history = (0 == STRCMP(tname[i], OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TNAME));
    int64_t affected_rows = 0;
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TNAME)) {
      continue;
    } else if (OB_FAIL(format_delete_mock_table_column_dml_sql(mock_fk_parent_table_schema, is_history, column_sql))) {
      LOG_WARN("failed to format column_sql and column_history_sql", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, column_sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(column_sql));
    } else if (affected_rows != mock_fk_parent_table_schema.get_column_array().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not equal to col count in table", K(ret),
               K(affected_rows), K(column_sql), K(mock_fk_parent_table_schema));
    }
  }
  return ret;
}

int ObTableSqlService::format_insert_mock_table_dml_sql(
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    ObDMLSqlSplicer &dml,
    bool &is_history)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("mock_fk_parent_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_mock_fk_parent_table_id())))
      || OB_FAIL(dml.add_column("database_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_database_id())))
      || OB_FAIL(dml.add_column("mock_fk_parent_table_name", mock_fk_parent_table_schema.get_mock_fk_parent_table_name()))
      || OB_FAIL(dml.add_column("schema_version", mock_fk_parent_table_schema.get_schema_version()))
      || OB_FAIL(dml.add_gmt_modified())
      || (is_history && OB_FAIL(dml.add_column("is_deleted", 0)))) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObTableSqlService::format_delete_mock_table_dml_sql(
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const bool is_history,
    ObSqlString &delete_mock_table_dml_sql)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const int64_t IS_DELETED = 1;
  ObDMLSqlSplicer dml;
  if (is_history) {
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("mock_fk_parent_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_mock_fk_parent_table_id())))
        || OB_FAIL(dml.add_column("database_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_database_id())))
        || OB_FAIL(dml.add_column("schema_version", mock_fk_parent_table_schema.get_schema_version()))
        || OB_FAIL(dml.add_gmt_modified())
        || (is_history && OB_FAIL(dml.add_column("is_deleted", IS_DELETED)))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_insert_sql_without_plancache(OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TNAME, delete_mock_table_dml_sql))) {
      LOG_WARN("splice_insert_sql failed", K(ret));
    }
  } else {
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("mock_fk_parent_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_mock_fk_parent_table_id())))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_MOCK_FK_PARENT_TABLE_TNAME, delete_mock_table_dml_sql))) {
      LOG_WARN("splice_insert_sql failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::format_insert_mock_table_column_dml_sql(
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const bool is_history,
    ObSqlString &column_sql)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table_schema.get_column_array().count(); ++i) {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("mock_fk_parent_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_mock_fk_parent_table_id())))
        || OB_FAIL(dml.add_pk_column("parent_column_id", mock_fk_parent_table_schema.get_column_array().at(i).first))
        || OB_FAIL(dml.add_column("parent_column_name", mock_fk_parent_table_schema.get_column_array().at(i).second))
        || OB_FAIL(dml.add_column("schema_version", mock_fk_parent_table_schema.get_schema_version()))
        || OB_FAIL(dml.add_gmt_modified())
        || (is_history && OB_FAIL(dml.add_column("is_deleted", 0)))) {
      LOG_WARN("add column failed", K(ret));
    } else if (0 == i) { // 0 == i or column_sql.empty() means the first column in fk info
      if (OB_FAIL(dml.splice_insert_sql_without_plancache(
          is_history ? OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TNAME : OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TNAME, column_sql))) {
        LOG_WARN("splice_insert_sql failed", K(ret), K(is_history));
      }
    } else { // the following columns in fk info
      ObSqlString value_str;
      if (OB_FAIL(dml.splice_values(value_str))) {
        LOG_WARN("splice_values failed", K(ret));
      } else if (OB_FAIL(column_sql.append_fmt(", (%s)", value_str.ptr()))) {
        LOG_WARN("append_fmt failed", K(ret), K(value_str));
      }
    }
  }
  return ret;
}

int ObTableSqlService::format_delete_mock_table_column_dml_sql(
    const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
    const bool is_history,
    ObSqlString &column_sql)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = mock_fk_parent_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const int64_t IS_DELETED = 1;

  if (is_history) {
    for (int64_t i = 0; OB_SUCC(ret) && i < mock_fk_parent_table_schema.get_column_array().count(); ++i) {
      ObDMLSqlSplicer dml;
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("mock_fk_parent_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_mock_fk_parent_table_id())))
          || OB_FAIL(dml.add_pk_column("parent_column_id", mock_fk_parent_table_schema.get_column_array().at(i).first))
          || OB_FAIL(dml.add_column("schema_version", mock_fk_parent_table_schema.get_schema_version()))
          || OB_FAIL(dml.add_gmt_modified())
          || OB_FAIL(dml.add_column("is_deleted", IS_DELETED))) {
        LOG_WARN("add column failed", K(ret));
      } else if (0 == i) { // 0 == i or column_sql.empty() means the first column in fk info
        if (OB_FAIL(dml.splice_insert_sql_without_plancache(OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TNAME, column_sql))) {
          LOG_WARN("splice_insert_sql failed", K(ret), K(is_history));
        }
      } else { // the following columns in fk info
        ObSqlString value_str;
        if (OB_FAIL(dml.splice_values(value_str))) {
          LOG_WARN("splice_values failed", K(ret));
        } else if (OB_FAIL(column_sql.append_fmt(", (%s)", value_str.ptr()))) {
          LOG_WARN("append_fmt failed", K(ret), K(value_str));
        }
      }
    }
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("mock_fk_parent_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, mock_fk_parent_table_schema.get_mock_fk_parent_table_id())))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TNAME, column_sql))) {
      LOG_WARN("splice_insert_sql failed", K(ret));
    } else if (OB_FAIL(column_sql.append_fmt(" AND parent_column_id in (%lu", mock_fk_parent_table_schema.get_column_array().at(0).first))) {
      LOG_WARN("append_fmt failed", K(ret), K(column_sql));
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < mock_fk_parent_table_schema.get_column_array().count(); ++i) {
        if (OB_FAIL(column_sql.append_fmt(", %lu", mock_fk_parent_table_schema.get_column_array().at(i).first))) {
          LOG_WARN("append_fmt failed", K(ret), K(column_sql));
        }
      }
      if (FAILEDx(column_sql.append_fmt(")"))) {
        LOG_WARN("append_fmt failed", K(ret), K(column_sql));
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_view_columns(ObISQLClient &sql_client,
                                           const ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  const int64_t new_schema_version = table.get_schema_version();
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t data_version = 0;
  if (OB_FAIL(check_ddl_allowed(table))) {
    LOG_WARN("check ddl allowd failed", K(ret), K(table));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version and feature mismatch", K(ret));
  }
  ObSqlString column_sql_obj;
  ObSqlString column_history_sql_obj;
  ObSqlString *column_sql_ptr = &column_sql_obj;
  ObSqlString *column_history_sql_ptr = &column_history_sql_obj;
  ObSqlString &column_sql = *column_sql_ptr;
  ObSqlString &column_history_sql = *column_history_sql_ptr;
  int64_t affected_rows = 0;
  for (ObTableSchema::const_column_iterator iter = table.column_begin();
      OB_SUCCESS == ret && iter != table.column_end(); ++iter) {
    if (OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", K(ret));
    } else {
      ObColumnSchemaV2 column;
      if (OB_FAIL(column.assign(**iter))) {
        LOG_WARN("fail to assign column", KR(ret), KPC(*iter));
      } else {
        column.set_schema_version(new_schema_version);
        column.set_tenant_id(table.get_tenant_id());
        column.set_table_id(table.get_table_id());
      }
      ObDMLSqlSplicer dml;
      if (FAILEDx(gen_column_dml(exec_tenant_id, column, dml))) {
        LOG_WARN("gen_column_dml failed", K(column), K(ret));
      } else if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_COLUMN_TNAME, column_sql))) {
        LOG_WARN("splice_insert_sql failed", "table_name", OB_ALL_COLUMN_TNAME, K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, column_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(column_sql), K(ret));
      } else if (affected_rows > 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows not equal to column count", K(affected_rows), K(ret));
      } else if (column_history_sql.empty()) {
        const int64_t is_deleted = 0;
        if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
          LOG_WARN("dml add column failed", K(ret));
        } else if (OB_FAIL(dml.splice_insert_sql_without_plancache(
                OB_ALL_COLUMN_HISTORY_TNAME, column_history_sql))) {
          LOG_WARN("splice_insert_sql failed", "table_name", OB_ALL_COLUMN_HISTORY_TNAME, K(ret));
        }
      } else {
        ObSqlString value_str;
        const int64_t is_deleted = 0;
        if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(dml.splice_values(value_str))) {
          LOG_WARN("splice_values failed", K(ret));
        } else if (OB_FAIL(column_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
          LOG_WARN("append_fmt failed", K(value_str), K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, column_history_sql.ptr(), affected_rows))) {
    LOG_WARN("execute_sql failed", K(column_history_sql), K(ret));
  } else if (affected_rows != table.get_column_count()) {
    LOG_WARN("affected_rows not equal to column count", K(affected_rows),
        "column_count", table.get_column_count(), K(ret));
  }
  return ret;
}

} //end of schema
} //end of share
} //end of oceanbase
