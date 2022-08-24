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
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_store_format.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_partition_sql_helper.h"
#include "share/ob_partition_modify.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_time_zone_info_manager.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "rootserver/ob_root_service.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"

namespace oceanbase {
using namespace common;
namespace rootserver {
class ObRootService;
}
namespace share {
namespace schema {

int ObTableSqlService::exec_update(ObISQLClient& sql_client, const uint64_t table_id, const char* table_name,
    ObDMLSqlSplicer& dml, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (is_core_table(table_id)) {
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    ObCoreTableProxy kv(table_name, sql_client);
    if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("failed to load kv for update", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("splice core cells failed", K(ret));
    } else if (OB_FAIL(kv.update_row(cells, affected_rows))) {
      LOG_WARN("failed to update row", K(ret));
    }
  } else {
    const uint64_t tenant_id = extract_tenant_id(table_id);
    // For compatibility, system table's schema should be recorded in system tenant.
    const uint64_t exec_tenant_id =
        is_inner_table(table_id) ? OB_SYS_TENANT_ID : ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_update(table_name, dml, affected_rows))) {
      LOG_WARN("execute update failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::exec_insert(ObISQLClient& sql_client, const uint64_t table_id, const char* table_name,
    ObDMLSqlSplicer& dml, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (is_core_table(table_id)) {
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    ObCoreTableProxy kv(table_name, sql_client);
    if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("failed to load kv for insert", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("splice core cells failed", K(ret));
    } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
      LOG_WARN("failed to replace row", K(ret));
    }
  } else {
    const uint64_t tenant_id = extract_tenant_id(table_id);
    // For compatibility, system table's schema should be recorded in system tenant.
    const uint64_t exec_tenant_id =
        is_inner_table(table_id) ? OB_SYS_TENANT_ID : ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_insert(table_name, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::exec_delete(ObISQLClient& sql_client, const uint64_t table_id, const char* table_name,
    ObDMLSqlSplicer& dml, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (is_core_table(table_id)) {
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    ObCoreTableProxy kv(table_name, sql_client);
    if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("failed to load kv for delete", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("splice core cells failed", K(ret));
    } else if (OB_FAIL(kv.delete_row(cells, affected_rows))) {
      LOG_WARN("failed to delete row", K(ret));
    }
  } else {
    const uint64_t tenant_id = extract_tenant_id(table_id);
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_delete(table_name, dml, affected_rows))) {
      LOG_WARN("execute delete failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::delete_table_part_info(
    const ObTableSchema& table_schema, const int64_t new_schema_version, ObISQLClient& sql_client, bool is_delay_delete)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  /*
   * Because __all_part(_history)/__all_sub_part(_history)/__all_def_sub_part(_history) is not core table,
   * to avoid cyclic dependence while refresh schema, all inner tables won't record any partition related schema in
   * tables. As compensation, schema module mock inner table's partition schema while refresh schema. So far, we only
   * support to define hash-like inner table.
   */
  if (!is_inner_table(table_schema.get_table_id())) {
    if (table_schema.get_part_level() > 0 && !table_schema.is_vir_table() && !table_schema.is_view_table()) {
      int64_t tenant_id = table_schema.get_tenant_id();
      bool is_two_level = PARTITION_LEVEL_TWO == table_schema.get_part_level() ? true : false;
      const char* tname[] = {
          OB_ALL_PART_INFO_TNAME, OB_ALL_PART_TNAME, OB_ALL_SUB_PART_TNAME, OB_ALL_DEF_SUB_PART_TNAME};
      ObSqlString sql;
      int64_t affected_rows = 0;
      // drop data in __all_part_info, __all_part, __all_subpart, __all_def_subpart,
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
        if (!is_two_level &&
            (0 == STRCMP(tname[i], OB_ALL_SUB_PART_TNAME) || 0 == STRCMP(tname[i], OB_ALL_DEF_SUB_PART_TNAME))) {
          continue;
        }
        if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu",
                tname[i],
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schema.get_table_id())))) {
          LOG_WARN("append_fmt failed", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
        } else {
        }
      }
      // While table should be delay-deleted, we remove its content from __all_table and
      // remain its content in __all_table_history.
      if (OB_SUCC(ret) && !is_delay_delete) {
        ObTableSchema new_table;
        if (OB_FAIL(new_table.assign(table_schema))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else {
          new_table.set_schema_version(new_schema_version);
          const bool is_tablegroup_def = false;
          const ObPartitionSchema* new_table_schema = &new_table;
          ObDropPartInfoHelper part_helper(new_table_schema, sql_client, is_tablegroup_def);
          if (OB_FAIL(part_helper.delete_partition_info())) {
            LOG_WARN("delete partition info failed", K(ret));
          } else if (OB_FAIL(part_helper.delete_dropped_partition_info())) {
            LOG_WARN("delete delay deleted partition info failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::drop_inc_partition(
    common::ObISQLClient& sql_client, const ObTableSchema& ori_table, const ObTableSchema& inc_table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ori_table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!is_inner_table(ori_table.get_table_id()) && 0 < ori_table.get_part_level() && !ori_table.is_vir_table() &&
      !ori_table.is_view_table()) {
    int64_t tenant_id = ori_table.get_tenant_id();
    ObSqlString sql;
    const int64_t inc_part_num = inc_table.get_partition_num();
    ObPartition** part_array = inc_table.get_part_array();

    // build sql string
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", K(ret), K(inc_table));
    } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu AND (0 = 1",
                   OB_ALL_PART_TNAME,
                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, ori_table.get_table_id())))) {
      LOG_WARN("append_fmt failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      ObPartition* part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret), K(i), K(inc_part_num), K(inc_table));
      } else if (OB_FAIL(sql.append_fmt(" OR part_id = %lu", part->get_part_id()))) {
        LOG_WARN("append_fmt failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(" )"))) {
        LOG_WARN("append_fmt failed", K(ret));
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

int ObTableSqlService::drop_inc_sub_partition(
    common::ObISQLClient& sql_client, const ObTableSchema& ori_table, const ObTableSchema& inc_table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ori_table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (ori_table.is_sub_part_template()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("template cannot drop subpart", KR(ret));
  } else if (!ori_table.has_self_partition()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has not self partition", KR(ret));
  } else {
    int64_t tenant_id = ori_table.get_tenant_id();
    ObSqlString sql;
    const int64_t inc_part_num = inc_table.get_partition_num();
    int64_t inc_subpart_num = 0;
    ObPartition** part_array = inc_table.get_part_array();

    // build sql string
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", K(ret), K(inc_table));
    } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu AND (0 = 1",
                   OB_ALL_SUB_PART_TNAME,
                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, ori_table.get_table_id())))) {
      LOG_WARN("append_fmt failed", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      ObPartition* part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret), K(i), K(inc_part_num), K(inc_table));
      } else if (OB_ISNULL(part->get_subpart_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartitions is null", K(ret), K(i), K(inc_part_num), K(inc_table));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
          inc_subpart_num++;
          ObSubPartition* subpart = part->get_subpart_array()[j];
          if (OB_ISNULL(subpart)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpartition is null", K(ret), K(i), K(inc_part_num), K(inc_table));
          } else if (OB_FAIL(sql.append_fmt(" OR (part_id = %lu AND sub_part_id = %lu)",
                         subpart->get_part_id(),
                         subpart->get_sub_part_id()))) {
            LOG_WARN("append_fmt failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(" )"))) {
        LOG_WARN("append_fmt failed", K(ret));
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

int ObTableSqlService::drop_inc_all_sub_partition(
    common::ObISQLClient& sql_client, const ObTableSchema& ori_table, const ObTableSchema& inc_table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ori_table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (ori_table.is_sub_part_template()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("template cannot drop subpart", KR(ret));
  } else if (!ori_table.has_self_partition()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has not self partition", KR(ret));
  } else {
    int64_t tenant_id = ori_table.get_tenant_id();
    ObSqlString sql;
    const int64_t inc_part_num = inc_table.get_partition_num();
    ObPartition** part_array = inc_table.get_part_array();

    // build sql string
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", K(ret), K(inc_table));
    } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu AND (0 = 1",
                   OB_ALL_SUB_PART_TNAME,
                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, ori_table.get_table_id())))) {
      LOG_WARN("append_fmt failed", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      ObPartition* part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret), K(i), K(inc_part_num), K(inc_table));
      } else if (OB_FAIL(sql.append_fmt(" OR (part_id = %lu)", part->get_part_id()))) {
        LOG_WARN("append_fmt failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(" )"))) {
        LOG_WARN("append_fmt failed", K(ret));
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

// For force drop schema only
int ObTableSqlService::drop_part_info_for_inspection(ObISQLClient& sql_client, const ObTableSchema& table_schema,
    const ObTableSchema& inc_table_schema, const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  if (!is_inner_table(table_schema.get_table_id()) && table_schema.get_part_level() > 0 &&
      !table_schema.is_vir_table() && !table_schema.is_view_table()) {
    const bool is_tablegroup_def = false;
    const ObPartitionSchema* table_schema_ptr = &table_schema;
    const ObPartitionSchema* inc_table_schema_ptr = &inc_table_schema;

    if (!table_schema.is_sub_part_template()) {
      ObDropIncSubPartHelper drop_subpart_helper(
          table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client, is_tablegroup_def);
      if (OB_FAIL(drop_subpart_helper.drop_dropped_subpartition_array())) {
        LOG_WARN("drop increment dropped subpartition info failed",
            K(table_schema),
            KPC(inc_table_schema_ptr),
            K(new_schema_version),
            K(ret));
      }
    }

    ObDropIncPartHelper drop_part_helper(
        table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client, is_tablegroup_def);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(drop_part_helper.drop_partition_info())) {
      LOG_WARN("drop increment partition info failed",
          K(table_schema),
          KPC(inc_table_schema_ptr),
          K(new_schema_version),
          K(ret));
    } else {
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

  return ret;
}

int ObTableSqlService::drop_subpart_info_for_inspection(ObISQLClient& sql_client, const ObTableSchema& table_schema,
    const ObTableSchema& inc_table_schema, const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_sub_part_template()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("template cannot drop subpart", KR(ret));
  } else if (!table_schema.has_self_partition()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table has not self partition", KR(ret));
  } else {
    const bool is_tablegroup_def = false;
    const ObPartitionSchema* table_schema_ptr = &table_schema;
    const ObPartitionSchema* inc_table_schema_ptr = &inc_table_schema;
    ObDropIncSubPartHelper drop_subpart_helper(
        table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client, is_tablegroup_def);
    if (OB_FAIL(drop_subpart_helper.drop_subpartition_info())) {
      LOG_WARN("drop increment partition info failed",
          K(table_schema),
          KPC(inc_table_schema_ptr),
          K(new_schema_version),
          K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = table_schema.get_tenant_id();
      opt.database_id_ = table_schema.get_database_id();
      opt.tablegroup_id_ = table_schema.get_tablegroup_id();
      opt.table_id_ = table_schema.get_table_id();
      opt.op_type_ = OB_DDL_DROP_SUB_PARTITION;
      opt.schema_version_ = new_schema_version;
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  }

  return ret;
}

int ObTableSqlService::drop_inc_part_info(ObISQLClient& sql_client, const ObTableSchema& table_schema,
    const ObTableSchema& inc_table_schema, const int64_t new_schema_version, bool is_delay_delete,
    bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  if (!is_inner_table(table_schema.get_table_id()) && table_schema.get_part_level() > 0 &&
      !table_schema.is_vir_table() && !table_schema.is_view_table()) {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(drop_inc_partition(sql_client, table_schema, inc_table_schema))) {
      // remove from __all_part
      LOG_WARN("failed to drop partition", K(ret), K(inc_table_schema));
    } else if (!table_schema.is_sub_part_template() &&
               OB_FAIL(drop_inc_all_sub_partition(sql_client, table_schema, inc_table_schema))) {
      // remove from __all_sub_part
      LOG_WARN("failed to drop subpartition", K(ret), K(inc_table_schema));
    } else {
      const bool is_tablegroup_def = false;
      const ObPartitionSchema* table_schema_ptr = &table_schema;
      const ObPartitionSchema* inc_table_schema_ptr = &inc_table_schema;
      if (is_delay_delete) {
        // add delay-deleted record
        ObAddIncPartHelper add_part_helper(
            table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client, is_tablegroup_def);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(add_part_helper.add_partition_info(true))) {
          LOG_WARN("drop increment partition info failed",
              K(ret),
              K(table_schema),
              KPC(inc_table_schema_ptr),
              K(new_schema_version));
        }
      } else {
        // add deleted record
        if (!table_schema.is_sub_part_template()) {
          ObDropIncSubPartHelper drop_subpart_helper(
              table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client, is_tablegroup_def);
          if (OB_FAIL(drop_subpart_helper.drop_dropped_subpartition_array())) {
            LOG_WARN("drop increment dropped subpartition info failed",
                K(table_schema),
                KPC(inc_table_schema_ptr),
                K(new_schema_version),
                K(ret));
          }
        }

        ObDropIncPartHelper drop_part_helper(
            table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client, is_tablegroup_def);
        if (OB_SUCC(ret) && OB_FAIL(drop_part_helper.drop_partition_info())) {
          LOG_WARN("drop increment partition info failed",
              K(table_schema),
              KPC(inc_table_schema_ptr),
              K(new_schema_version),
              K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!is_truncate_table && OB_INVALID_ID == table_schema.get_tablegroup_id()) {
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
        if (is_delay_delete) {
          opt.op_type_ = OB_DDL_DELAY_DELETE_TABLE_PARTITION;
        } else {
          opt.op_type_ = OB_DDL_DROP_PARTITION;
        }

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
int ObTableSqlService::drop_inc_subpart_info(ObISQLClient& sql_client, const ObTableSchema& table_schema,
    const ObTableSchema& inc_table_schema, const int64_t new_schema_version, bool is_delay_delete)
{
  int ret = OB_SUCCESS;

  if (table_schema.is_sub_part_template()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("template cannot drop subpart", KR(ret));
  } else if (!table_schema.has_self_partition()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table has not self partition", KR(ret));
    // remove from __all_sub_part
  } else if (OB_FAIL(drop_inc_sub_partition(sql_client, table_schema, inc_table_schema))) {
    LOG_WARN("failed to drop subpartition", KR(ret), K(inc_table_schema));
  } else {
    const bool is_tablegroup_def = false;
    const ObPartitionSchema* table_schema_ptr = &table_schema;
    const ObPartitionSchema* inc_table_schema_ptr = &inc_table_schema;
    if (is_delay_delete) {
      ObAddIncSubPartHelper add_subpart_helper(
          table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client, is_tablegroup_def);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(add_subpart_helper.add_subpartition_info(true))) {
        LOG_WARN("drop increment partition info failed",
            KR(ret),
            K(table_schema),
            KPC(inc_table_schema_ptr),
            K(new_schema_version));
      }
    } else {
      ObDropIncSubPartHelper drop_subpart_helper(
          table_schema_ptr, inc_table_schema_ptr, new_schema_version, sql_client, is_tablegroup_def);
      if (OB_FAIL(drop_subpart_helper.drop_subpartition_info())) {
        LOG_WARN("drop increment partition info failed",
            K(table_schema),
            KPC(inc_table_schema_ptr),
            K(new_schema_version),
            KR(ret));
      }
    }
  }
  return ret;
}

// truncate first partition of partitioned table
int ObTableSqlService::truncate_part_info(common::ObISQLClient& sql_client, const ObTableSchema& ori_table,
    ObTableSchema& inc_table, const int64_t schema_version, bool is_delay_delete)
{
  int ret = OB_SUCCESS;
  // drop first partitions
  if (OB_FAIL(drop_inc_part_info(sql_client, ori_table, inc_table, schema_version, is_delay_delete, true))) {
    LOG_WARN("delete inc part info failed", KR(ret));
  } else {
    if (!ori_table.is_sub_part_template()) {
      // For non-template subpartitioned table, part_id should be -1 and sub_part_is should be valid.
      const int64_t inc_part_num = inc_table.get_part_option().get_part_num();
      ObPartition** inc_part_array = inc_table.get_part_array();
      if (OB_ISNULL(inc_part_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition array is null", KR(ret), K(inc_table));
      } else {
        for (int64_t i = 0; i < inc_part_num && OB_SUCC(ret); ++i) {
          const ObPartition* part = NULL;
          ObPartition* inc_part = inc_part_array[i];
          if (OB_ISNULL(inc_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
          } else if (OB_ISNULL(inc_part->get_subpart_array())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpartitions is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
          } else if (OB_FAIL(ori_table.get_partition_by_part_id(inc_part->get_part_id(), false, part))) {
            LOG_WARN("fail to get partition", KR(ret), K(inc_part->get_part_id()));
          } else if (OB_ISNULL(part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", KR(ret), K(i), K(inc_part));
          } else {
            // sub_part_num should be same with subpartition_num
            inc_part->set_sub_part_num(inc_part->get_subpartition_num());
            inc_part->set_max_used_sub_part_id(inc_part->get_subpartition_num() - 1);
            inc_part->set_part_id(-1);
            for (int64_t j = 0; OB_SUCC(ret) && j < inc_part->get_subpartition_num(); j++) {
              ObSubPartition* subpart = inc_part->get_subpart_array()[j];
              if (OB_ISNULL(subpart)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("subpartition is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
              } else {
                subpart->set_sub_part_id(j);
              }
            }
          }
        }
      }
    }
    // add first partitions
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_inc_part_info(sql_client, ori_table, inc_table, schema_version, true))) {
      LOG_WARN("add inc part info failed", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
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

int ObTableSqlService::truncate_subpart_info(common::ObISQLClient& sql_client, const ObTableSchema& ori_table,
    ObTableSchema& inc_table, const int64_t schema_version, bool is_delay_delete)
{
  int ret = OB_SUCCESS;
  const ObPartition* part = NULL;
  if (OB_FAIL(drop_inc_subpart_info(sql_client, ori_table, inc_table, schema_version, is_delay_delete))) {
    LOG_WARN("failed to drop partition", KR(ret), K(inc_table));
  } else {
    const int64_t inc_part_num = inc_table.get_partition_num();
    ObPartition** inc_part_array = inc_table.get_part_array();
    if (OB_ISNULL(inc_part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", KR(ret), K(inc_table));
    } else {
      // Reset related sub_part_id to generate new sub_part_id by max_used_sub_part_id in add_inc_subpart_info.
      for (int64_t i = 0; i < inc_part_num && OB_SUCC(ret); ++i) {
        ObPartition* inc_part = inc_part_array[i];
        if (OB_ISNULL(inc_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
        } else if (OB_ISNULL(inc_part->get_subpart_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpartitions is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
        } else if (OB_FAIL(ori_table.get_partition_by_part_id(inc_part->get_part_id(), false, part))) {
          LOG_WARN("fail to get partition", KR(ret), K(inc_part->get_part_id()));
        } else if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", KR(ret), K(i), K(inc_part));
        } else {
          inc_part->set_max_used_sub_part_id(part->get_max_used_sub_part_id());
          // make sure sub_part_num is same with subpartition_num
          inc_part->set_sub_part_num(inc_part->get_subpartition_num());
          for (int64_t j = 0; OB_SUCC(ret) && j < inc_part->get_subpartition_num(); j++) {
            ObSubPartition* subpart = inc_part->get_subpart_array()[j];
            if (OB_ISNULL(subpart)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("subpartition is null", KR(ret), K(i), K(inc_part_num), K(inc_table));
            } else {
              subpart->set_sub_part_id(-1);
            }
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_inc_subpart_info(sql_client, ori_table, inc_table, schema_version))) {
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

int ObTableSqlService::drop_table_for_inspection(
    ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(delete_from_all_table_history(sql_client, table_schema, new_schema_version))) {
    LOG_WARN("delete from __all_table_history failed", K(ret), K(table_schema));
  } else if (OB_FAIL(delete_table_part_info(table_schema, new_schema_version, sql_client, false))) {
    LOG_WARN("delete partition info failed", K(ret), K(table_schema));
  } else if (!table_schema.is_view_table() &&
             OB_FAIL(delete_from_all_column_history(sql_client, table_schema, new_schema_version))) {
    LOG_WARN("delete from __all_column_history_table failed", K(ret), K(table_schema));
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = table_schema.get_tenant_id();
    opt.database_id_ = table_schema.get_database_id();
    opt.tablegroup_id_ = table_schema.get_tablegroup_id();
    opt.table_id_ = table_schema.get_table_id();
    opt.op_type_ = OB_DDL_DROP_TABLE;
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::drop_table(const ObTableSchema& table_schema, const int64_t new_schema_version,
    ObISQLClient& sql_client, const ObString* ddl_stmt_str /*=NULL*/, bool is_truncate_table /*false*/,
    bool is_drop_db /*false*/, ObSchemaGetterGuard* schema_guard /*=NULL*/,
    DropTableIdHashSet* drop_table_set /*=NULL*/, const bool is_delay_delete, const ObString* delay_deleted_name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  const uint64_t table_id = table_schema.get_table_id();
  if (is_inner_table(table_id) && OB_SYS_TENANT_ID != table_schema.get_tenant_id()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not drop sys table", K(ret), K(table_id), K(table_schema));
  } else if (is_delay_delete) {
    ObTableSchema new_table_schema;
    if (OB_ISNULL(delay_deleted_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delay_deleted_name is null", K(ret));
    } else if (OB_FAIL(new_table_schema.assign(table_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (!new_table_schema.is_in_recyclebin() && OB_FAIL(new_table_schema.set_table_name(*delay_deleted_name))) {
      LOG_WARN("fail to set table name", K(ret), K(*delay_deleted_name));
    } else if (FALSE_IT(new_table_schema.set_schema_version(new_schema_version))) {
    } else if (FALSE_IT(new_table_schema.set_drop_schema_version(new_schema_version))) {
    } else if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null", K(ret));
    } else {
      if (OB_FAIL(add_table(sql_client, new_table_schema, true))) {
        LOG_WARN("add_table failed", K(ret), K(new_table_schema));
      }
    }
  } else {
    // delete from __all_table_history
    if (OB_FAIL(delete_from_all_table_history(sql_client, table_schema, new_schema_version))) {
      LOG_WARN("delete_from_all_table_history failed", K(table_schema), K(ret));
    }
  }

  // delete from __all_column_history
  if (OB_FAIL(ret)) {
  } else if (is_delay_delete) {
    // Column schema won't be modified while delay delete table.
  } else if (!table_schema.is_view_table()) {
    // view don't preserve column schema
    if (OB_FAIL(delete_from_all_column_history(sql_client, table_schema, new_schema_version))) {
      LOG_WARN("delete_from_column_history_table failed", K(table_schema), K(ret));
    }
  }
  // delete from __all_table and __all_column
  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_from_all_table(sql_client, table_id))) {
      LOG_WARN("delete from all table failed", K(ret));
    } else if (OB_FAIL(delete_from_all_column(sql_client,
                   table_id,
                   table_schema.get_column_count(),
                   table_schema.get_table_type() != MATERIALIZED_VIEW))) {
      LOG_WARN("failed to delete columns", K(table_id), "column count", table_schema.get_column_count(), K(ret));
    }
  }

  // delete from __all_table_stat, __all_column_stat and __all_histogram_stat
  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_from_all_table_stat(sql_client, table_id))) {
      LOG_WARN("delete from all table stat failed", K(ret));
    } else if (OB_FAIL(delete_from_all_column_stat(sql_client, table_id, table_schema.get_column_count(), false))) {
      LOG_WARN(
          "failed to delete all column stat", K(table_id), "column count", table_schema.get_column_count(), K(ret));
    } else if (OB_FAIL(delete_from_all_histogram_stat(sql_client, table_id))) {
      LOG_WARN("failed to delete all histogram_stat", K(table_id), K(ret));
    }
  }

  // delete from __all_temp_table if it is a temporary table or ctas temporary table
  if (OB_SUCC(ret) && (table_schema.is_ctas_tmp_table() || table_schema.is_tmp_table())) {
    if (OB_FAIL(delete_from_all_temp_table(sql_client, table_schema.get_tenant_id(), table_id))) {
      LOG_WARN("delete from all temp table failed", K(ret));
    }
  }

  // Partition schema won't be modified while delay delete table.
  // delete from __all_part_info, __all_part and __all_sub_part
  if (OB_SUCC(ret)) {
    if (OB_FAIL(delete_table_part_info(table_schema, new_schema_version, sql_client, is_delay_delete))) {
      LOG_WARN("delete partition info failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // For oracle compatibility, foreign key should be dropped while drop table.
    if (OB_FAIL(delete_foreign_key(sql_client, table_schema, new_schema_version))) {
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

  // log operations
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = table_schema.get_tenant_id();
    opt.database_id_ = table_schema.get_database_id();
    opt.tablegroup_id_ = table_schema.get_tablegroup_id();
    opt.table_id_ = table_schema.get_table_id();
    if (is_delay_delete) {
      opt.op_type_ = OB_DDL_DELAY_DELETE_TABLE;
    } else if (is_truncate_table) {
      opt.op_type_ = OB_DDL_TRUNCATE_TABLE_DROP;
    } else {
      if (table_schema.is_index_table()) {
        opt.op_type_ = table_schema.is_global_index_table() ? OB_DDL_DROP_GLOBAL_INDEX : OB_DDL_DROP_INDEX;
      } else if (table_schema.is_view_table()) {
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
      if (table_schema.is_index_table() || table_schema.is_materialized_view()) {
        if (OB_FAIL(update_data_table_schema_version(sql_client, table_schema.get_data_table_id()))) {
          LOG_WARN(
              "update_data_table_schema_version failed", "data_table_id", table_schema.get_data_table_id(), K(ret));
        }
      } else if (table_schema.get_foreign_key_real_count() > 0) {
        // We use "else" here since create foreign key on index/materialized_view is not supported.
        const ObIArray<ObForeignKeyInfo>& foreign_key_infos = table_schema.get_foreign_key_infos();
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
          const ObForeignKeyInfo& foreign_key_info = foreign_key_infos.at(i);
          const ObSimpleTableSchemaV2* external_db_table_schema = NULL;
          const uint64_t update_table_id = table_schema.get_table_id() == foreign_key_info.parent_table_id_
                                               ? foreign_key_info.child_table_id_
                                               : foreign_key_info.parent_table_id_;
          if (is_drop_db) {
            if (NULL == schema_guard) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema guard is null when to drop database", K(ret));
            } else {
              // int tmp_ret = OB_SUCCESS;
              tmp_ret = schema_guard->get_table_schema(update_table_id, external_db_table_schema);
              if (OB_SUCCESS != tmp_ret) {  // do-nothing
                LOG_WARN("get_table_schema failed", K(update_table_id), K(tmp_ret));
              } else if (NULL == external_db_table_schema) {
                // do-nothing
              } else if (table_schema.get_database_id() != external_db_table_schema->get_database_id()) {
                // FIXME: foreign key should not be across dbs.
                tmp_ret = update_data_table_schema_version(sql_client, update_table_id);
                LOG_WARN("update child table schema version", K(tmp_ret));
              }
            }
          } else if ((NULL != drop_table_set) && (OB_HASH_EXIST == drop_table_set->exist_refactored(update_table_id))) {
            // no need to update data table schema version since table has been dropped.
          } else if (update_table_id == table_schema.get_table_id()) {
            // no need to update data table schema version since data table is itself.
          } else if (OB_FAIL(update_data_table_schema_version(sql_client, update_table_id))) {
            LOG_WARN("failed to update parent table schema version", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::insert_single_constraint(
    ObISQLClient& sql_client, const ObTableSchema& new_table_schema, const ObConstraint& new_constraint)
{
  int ret = OB_SUCCESS;
  if (is_inner_table(new_table_schema.get_table_id()) && OB_SYS_TENANT_ID != new_table_schema.get_tenant_id()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(new_table_schema));
  } else if (OB_FAIL(add_single_constraint(sql_client, new_constraint))) {
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

int ObTableSqlService::insert_single_column(
    ObISQLClient& sql_client, const ObTableSchema& new_table_schema, const ObColumnSchemaV2& new_column_schema)
{
  int ret = OB_SUCCESS;
  if (is_inner_table(new_table_schema.get_table_id()) && OB_SYS_TENANT_ID != new_table_schema.get_tenant_id()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(new_table_schema));
  } else if (OB_FAIL(add_single_column(sql_client, new_column_schema))) {
    SHARE_SCHEMA_LOG(WARN, "add_single_column failed", K(new_column_schema), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (new_column_schema.is_autoincrement()) {
      if (OB_FAIL(add_sequence(new_table_schema.get_tenant_id(),
              new_table_schema.get_table_id(),
              new_column_schema.get_column_id(),
              new_table_schema.get_auto_increment()))) {
        SHARE_SCHEMA_LOG(WARN, "insert sequence record failed", K(ret), K(new_column_schema));
      }
    }
  }
  if (OB_SUCC(ret)) {
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

int ObTableSqlService::update_single_column(ObISQLClient& sql_client, const ObTableSchema& origin_table_schema,
    const ObTableSchema& new_table_schema, const ObColumnSchemaV2& new_column_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t table_id = new_column_schema.get_table_id();

  ObDMLSqlSplicer dml;
  if (is_inner_table(table_id) && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(new_table_schema));
  } else if (OB_FAIL(gen_column_dml(exec_tenant_id, new_column_schema, dml))) {
    LOG_WARN("gen column dml failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_update(sql_client, table_id, OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
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
        if (OB_FAIL(add_sequence(tenant_id,
                new_table_schema.get_table_id(),
                new_column_schema.get_column_id(),
                new_table_schema.get_auto_increment()))) {
          LOG_WARN("insert sequence record failed", K(ret), K(new_table_schema));
        }
      } else if (new_table_schema.get_autoinc_column_id() == new_column_schema.get_column_id()) {
        // do nothing; auto-increment column does not change
      } else {
        const ObColumnSchemaV2* origin_inc_column_schema =
            origin_table_schema.get_column_schema(new_column_schema.get_column_id());
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

  // insert updated column to __all_column_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_single_column(sql_client, new_column_schema, only_history))) {
      LOG_WARN("add_single_column failed", K(new_column_schema), K(ret));
    } else {
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

int ObTableSqlService::add_columns(ObISQLClient& sql_client, const ObTableSchema& table)
{
  return is_core_table(table.get_table_id()) ? add_columns_for_core(sql_client, table)
                                             : add_columns_for_not_core(sql_client, table);
}

int ObTableSqlService::add_columns_for_core(ObISQLClient& sql_client, const ObTableSchema& table)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  ObCoreTableProxy kv(OB_ALL_COLUMN_TNAME, sql_client);
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  if (OB_FAIL(kv.load_for_update())) {
    LOG_WARN("failed to load kv for update", K(ret));
  }
  for (ObTableSchema::const_column_iterator iter = table.column_begin();
       OB_SUCCESS == ret && iter != table.column_end();
       ++iter) {
    ObColumnSchemaV2 column = (**iter);
    column.set_schema_version(table.get_schema_version());
    column.set_tenant_id(table.get_tenant_id());
    column.set_table_id(table.get_table_id());
    dml.reset();
    cells.reuse();
    int64_t affected_rows = 0;
    if (OB_FAIL(gen_column_dml(OB_SYS_TENANT_ID, column, dml))) {
      LOG_WARN("gen column dml failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("splice core cells failed", K(ret));
    } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
      LOG_WARN("failed to replace row", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);
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

int ObTableSqlService::add_columns_for_not_core(ObISQLClient& sql_client, const ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  ObSqlString column_sql;
  ObSqlString column_history_sql;
  const int64_t new_schema_version = table.get_schema_version();
  const uint64_t tenant_id = table.get_tenant_id();
  // For compatibility, system table's schema should be recorded in system tenant.
  const uint64_t exec_tenant_id =
      is_inner_table(table.get_table_id()) ? OB_SYS_TENANT_ID : ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (ObTableSchema::const_column_iterator iter = table.column_begin();
       OB_SUCCESS == ret && iter != table.column_end();
       ++iter) {
    if (OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", K(ret));
    } else {
      ObColumnSchemaV2 column = (**iter);
      column.set_schema_version(new_schema_version);
      column.set_tenant_id(table.get_tenant_id());
      column.set_table_id(table.get_table_id());
      ObDMLSqlSplicer dml;
      if (OB_FAIL(gen_column_dml(exec_tenant_id, column, dml))) {
        LOG_WARN("gen_column_dml failed", K(column), K(ret));
      } else if (iter == table.column_begin()) {
        if (OB_FAIL(dml.splice_insert_sql_without_plancache(OB_ALL_COLUMN_TNAME, column_sql))) {
          LOG_WARN("splice_insert_sql failed", "table_name", OB_ALL_COLUMN_TNAME, K(ret));
        } else {
          const int64_t is_deleted = 0;
          if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
            LOG_WARN("dml add column failed", K(ret));
          } else if (OB_FAIL(
                         dml.splice_insert_sql_without_plancache(OB_ALL_COLUMN_HISTORY_TNAME, column_history_sql))) {
            LOG_WARN("splice_insert_sql failed", "table_name", OB_ALL_COLUMN_HISTORY_TNAME, K(ret));
          }
        }
      } else {
        ObSqlString value_str;
        if (OB_FAIL(dml.splice_values(value_str))) {
          LOG_WARN("splice_values failed", K(ret));
        } else if (OB_FAIL(column_sql.append_fmt(", (%s)", value_str.ptr()))) {
          LOG_WARN("append_fmt failed", K(value_str), K(ret));
        } else {
          value_str.reset();
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
  }
  int64_t affected_rows = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, column_sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(column_sql), K(ret));
  } else if (affected_rows != table.get_column_count()) {
    LOG_WARN(
        "affected_rows not equal to column count", K(affected_rows), "column_count", table.get_column_count(), K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, column_history_sql.ptr(), affected_rows))) {
    LOG_WARN("execute_sql failed", K(column_history_sql), K(ret));
  } else if (affected_rows != table.get_column_count()) {
    LOG_WARN(
        "affected_rows not equal to column count", K(affected_rows), "column_count", table.get_column_count(), K(ret));
  }
  return ret;
}

int ObTableSqlService::add_constraints(ObISQLClient& sql_client, const ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  if (is_core_table(table.get_table_id())) {
    // ignore
  } else if (table.get_constraint_count() > 0) {
    ret = add_constraints_for_not_core(sql_client, table);
  }
  return ret;
}

// create table add constraint
int ObTableSqlService::add_constraints_for_not_core(ObISQLClient& sql_client, const ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  ObSqlString constraint_sql;
  ObSqlString constraint_history_sql;
  const int64_t new_schema_version = table.get_schema_version();
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  if (is_inner_table(table.get_table_id())) {
    // To avoid cyclic dependence
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("should not be here", K(ret), K(table));
  }
  for (ObTableSchema::const_constraint_iterator iter = table.constraint_begin();
       OB_SUCCESS == ret && iter != table.constraint_end();
       ++iter) {
    if (OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", K(ret));
    } else {
      ObConstraint constraint;
      if (OB_FAIL(constraint.assign(**iter))) {
        SQL_RESV_LOG(WARN, "Fail to assign constraint", K(ret));
      } else {
        constraint.set_schema_version(new_schema_version);
        constraint.set_tenant_id(table.get_tenant_id());
        constraint.set_table_id(table.get_table_id());
        ObDMLSqlSplicer dml;
        if (OB_FAIL(gen_constraint_dml(exec_tenant_id, constraint, dml))) {
          LOG_WARN("gen_constraint_dml failed", K(constraint), K(ret));
        } else if (iter == table.constraint_begin()) {
          if (OB_FAIL(dml.splice_insert_sql_without_plancache(OB_ALL_CONSTRAINT_TNAME, constraint_sql))) {
            LOG_WARN("splice_insert_sql failed", "table_name", OB_ALL_CONSTRAINT_TNAME, K(ret));
          } else {
            const int64_t is_deleted = 0;
            if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
              LOG_WARN("dml add constraint failed", K(ret));
            } else if (OB_FAIL(dml.splice_insert_sql_without_plancache(
                           OB_ALL_CONSTRAINT_HISTORY_TNAME, constraint_history_sql))) {
              LOG_WARN("splice_insert_sql failed", "table_name", OB_ALL_CONSTRAINT_HISTORY_TNAME, K(ret));
            }
          }
        } else {
          ObSqlString value_str;
          if (OB_FAIL(dml.splice_values(value_str))) {
            LOG_WARN("splice_values failed", K(ret));
          } else if (OB_FAIL(constraint_sql.append_fmt(", (%s)", value_str.ptr()))) {
            LOG_WARN("append_fmt failed", K(value_str), K(ret));
          } else {
            value_str.reset();
            const int64_t is_deleted = 0;
            if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
              LOG_WARN("add constraint failed", K(ret));
            } else if (OB_FAIL(dml.splice_values(value_str))) {
              LOG_WARN("splice_values failed", K(ret));
            } else if (OB_FAIL(constraint_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
              LOG_WARN("append_fmt failed", K(value_str), K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100)) {
          if (CONSTRAINT_TYPE_CHECK == constraint.get_constraint_type()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "do ddl with check constraint before cluster upgrade to 310");
          }
        }
        if (OB_SUCC(ret) && (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100)) {
          for (ObConstraint::const_cst_col_iterator iter = constraint.cst_col_begin();
               OB_SUCC(ret) && (iter != constraint.cst_col_end());
               ++iter) {
            dml.reset();
            if (OB_FAIL(gen_constraint_column_dml(exec_tenant_id, constraint, *iter, dml))) {
              LOG_WARN("failed to gen constraint column dml", K(ret));
            } else if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME, dml, affected_rows))) {
              LOG_WARN("failed to insert foreign key column", K(ret));
            } else if (!is_single_row(affected_rows)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
            }
            if (OB_SUCC(ret)) {
              const int64_t is_deleted = 0;
              if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
                LOG_WARN("add column failed", K(ret));
              } else if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
                LOG_WARN("execute insert failed", K(ret));
              } else if (!is_single_row(affected_rows)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
              }
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, constraint_sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(constraint_sql), K(ret));
  } else if (affected_rows != table.get_constraint_count()) {
    LOG_WARN("affected_rows not equal to constraint count",
        K(affected_rows),
        "constraint_count",
        table.get_constraint_count(),
        K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, constraint_history_sql.ptr(), affected_rows))) {
    LOG_WARN("execute_sql failed", K(constraint_history_sql), K(ret));
  } else if (affected_rows != table.get_constraint_count()) {
    LOG_WARN("affected_rows not equal to constraint count",
        K(affected_rows),
        "constraint_count",
        table.get_constraint_count(),
        K(ret));
  }
  return ret;
}

int ObTableSqlService::rename_csts_in_inner_table(
    common::ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version)
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

  for (; OB_SUCC(ret) && iter != table_schema.constraint_end(); ++iter) {
    dml_for_update.reuse();
    dml_for_insert.reuse();
    int64_t affected_rows = 0;
    if (OB_FAIL(ObTableSchema::create_cons_name_automatically(
            new_cst_name, table_schema.get_table_name_str(), allocator, (*iter)->get_constraint_type()))) {
      SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
    } else if (OB_FAIL(gen_constraint_update_name_dml(
                   exec_tenant_id, new_cst_name, new_schema_version, **iter, dml_for_update))) {
      LOG_WARN("failed to delete from __all_constraint or __all_constraint_history",
          K(ret),
          K(new_cst_name),
          K(**iter),
          K(table_schema));
    } else if (OB_FAIL(exec_update(
                   sql_client, table_schema.get_table_id(), OB_ALL_CONSTRAINT_TNAME, dml_for_update, affected_rows))) {
      LOG_WARN("execute update sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert succeeded but affected_rows is not one", K(ret), K(affected_rows));
    } else if (OB_FAIL(gen_constraint_insert_new_name_row_dml(
                   exec_tenant_id, new_cst_name, new_schema_version, **iter, dml_for_insert))) {
      LOG_WARN("failed to delete from __all_constraint or __all_constraint_history",
          K(ret),
          K(new_cst_name),
          K(**iter),
          K(table_schema));
    } else if (OB_FAIL(exec_insert(sql_client,
                   table_schema.get_table_id(),
                   OB_ALL_CONSTRAINT_HISTORY_TNAME,
                   dml_for_insert,
                   affected_rows))) {
      LOG_WARN("execute insert sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", K(ret), K(affected_rows));
    }
  }

  return ret;
}

int ObTableSqlService::delete_constraint(
    common::ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema.get_tenant_id();
  ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();

  for (; OB_SUCC(ret) && iter != table_schema.constraint_end(); ++iter) {
    if (OB_FAIL(delete_from_all_constraint(sql_client, tenant_id, new_schema_version, **iter))) {
      LOG_WARN(
          "failed to delete from __all_constraint or __all_constraint_history", K(ret), K(**iter), K(table_schema));
    } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100) {
      // delete from all constraint column
      for (ObConstraint::const_cst_col_iterator cst_col_iter = (*iter)->cst_col_begin();
           OB_SUCC(ret) && (cst_col_iter != (*iter)->cst_col_end());
           ++cst_col_iter) {
        if (OB_FAIL(
                delete_from_all_constraint_column(sql_client, tenant_id, new_schema_version, **iter, *cst_col_iter))) {
          LOG_WARN("failed to delete constraint_column info from inner table", K(ret), K(**iter), K(table_schema));
        }
      }
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_constraint(
    ObISQLClient& sql_client, const uint64_t tenant_id, const int64_t new_schema_version, const ObConstraint& cst)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  // insert into __all_constraint_history
  if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id, table_id, constraint_id, schema_version, is_deleted)"
                             " VALUES(%lu, %lu, %lu, %ld, %ld)",
          OB_ALL_CONSTRAINT_HISTORY_TNAME,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, cst.get_table_id()),
          cst.get_constraint_id(),
          new_schema_version,
          is_deleted))) {
    LOG_WARN("assign insert into __all_constraint_history fail",
        K(ret),
        K(tenant_id),
        K(cst.get_table_id()),
        K(cst.get_constraint_id()),
        K(new_schema_version));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql fail", K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no row has inserted", K(ret));
  }
  // delete from __all_constraint
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND table_id = %lu AND constraint_id = %lu",
            OB_ALL_CONSTRAINT_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, cst.get_table_id()),
            cst.get_constraint_id()))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows is expected to one", K(affected_rows), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_constraint_column(ObISQLClient& sql_client, const uint64_t tenant_id,
    const int64_t new_schema_version, const ObConstraint& cst, const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id, table_id, constraint_id, column_id, schema_version, is_deleted)"
                             " VALUES(%lu, %lu, %lu, %lu, %ld, %ld)",
          OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, cst.get_table_id()),
          cst.get_constraint_id(),
          column_id,
          new_schema_version,
          is_deleted))) {
    LOG_WARN("assign insert into __all_constraint_column_history fail",
        K(ret),
        K(tenant_id),
        K(cst.get_table_id()),
        K(cst.get_constraint_id()),
        K(column_id),
        K(new_schema_version));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql fail", K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no row has inserted", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE "
                               "tenant_id = %lu AND table_id = %lu AND constraint_id = %lu AND column_id = %lu",
            OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, cst.get_table_id()),
            cst.get_constraint_id(),
            column_id))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows is expected to one", K(ret), K(affected_rows));
    }
  }

  return ret;
}

int ObTableSqlService::supplement_for_core_table(
    ObISQLClient& sql_client, const bool is_all_table, const ObColumnSchemaV2& column)
{
  int ret = OB_SUCCESS;

  int64_t orig_default_value_len = 0;
  const int64_t value_buf_len = 2 * OB_MAX_DEFAULT_VALUE_LENGTH + 3;
  char* orig_default_value_buf = NULL;
  orig_default_value_buf = static_cast<char*>(malloc(value_buf_len));
  if (OB_ISNULL(orig_default_value_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMSET(orig_default_value_buf, 0, value_buf_len);
    ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
    if (!ob_is_string_type(column.get_data_type()) && !ob_is_json(column.get_data_type())) {
      if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(column.get_tenant_id(), compat_mode))) {
        LOG_WARN("fail to get tenant mode", K(ret));
      } else {
        share::CompatModeGuard compat_mode_guard(compat_mode);
        ObTimeZoneInfo tz_info;
        if (OB_FAIL(column.get_orig_default_value().print_plain_str_literal(
                orig_default_value_buf, value_buf_len, orig_default_value_len, &tz_info))) {
          LOG_WARN("failed to print orig default value", K(ret), K(value_buf_len), K(orig_default_value_len));
        }
      }
    }
  }
  ObString orig_default_value;
  if (OB_SUCC(ret)) {
    if (ob_is_string_type(column.get_data_type()) || ob_is_json(column.get_data_type())) {
      ObString orig_default_value_str = column.get_orig_default_value().get_string();
      orig_default_value.assign_ptr(orig_default_value_str.ptr(), orig_default_value_str.length());
    } else {
      orig_default_value.assign_ptr(orig_default_value_buf, static_cast<int32_t>(orig_default_value_len));
    }
    if (column.get_orig_default_value().is_null()) {
      orig_default_value.reset();
    }
  }
  const char* supplement_tbl_name = NULL;
  if (OB_FAIL(ret)) {
  } else if (is_all_table) {
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(OB_SYS_TENANT_ID, supplement_tbl_name))) {
      LOG_WARN("fail to get all table name", K(ret));
    }
  } else {
    supplement_tbl_name = OB_ALL_COLUMN_TNAME;
  }
  if (OB_SUCC(ret)) {
    ObCoreTableProxy kv(supplement_tbl_name, sql_client);
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
        if (OB_FAIL(sql_append_hex_escape_str(ObHexEscapeSqlStr(orig_default_value).str(), sql_string))) {
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
          LOG_WARN("supplement_cell failed");
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
int ObTableSqlService::add_single_constraint(ObISQLClient& sql_client, const ObConstraint& constraint,
    const bool only_history, const bool need_to_deal_with_cst_cols)
{
  int ret = OB_SUCCESS;
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
      if (OB_FAIL(exec_insert(sql_client, table_id, OB_ALL_CONSTRAINT_TNAME, dml, affected_rows))) {
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
  if (OB_SUCC(ret) && (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100)) {
    if (CONSTRAINT_TYPE_CHECK == constraint.get_constraint_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "do ddl with check constraint before cluster upgrade to 310");
    }
  }
  if (OB_SUCC(ret) && (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100) && need_to_deal_with_cst_cols) {
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
          if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME, dml, affected_rows))) {
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
          } else if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
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

int ObTableSqlService::add_single_column(
    ObISQLClient& sql_client, const ObColumnSchemaV2& column, const bool only_history)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = column.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (is_inner_table(column.get_table_id()) && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(column));
  } else if (OB_FAIL(gen_column_dml(exec_tenant_id, column, dml))) {
    LOG_WARN("gen column dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      const int64_t table_id = column.get_table_id();
      if (OB_FAIL(exec_insert(sql_client, table_id, OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      } else {
        /*
         * We try to modify __all_table's content in __all_core_table when __all_table/__all_table_v2 adds new columns.
         * For compatibility, we will add new columns to __all_table/__all_table_v2 at the same time, which means we
         * modify
         * __all_table's content in __all_core_table twice in such situaction.
         *
         * When we add string-like columns to __all_table/__all_table_v2, it may cause -4016 error because the second
         * modification will do nothing and affected_rows won't change. To fix that, we skip the modification caused by
         * adding columns to __all_table_v2.
         *
         */
        bool is_all_table = combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_TID) == table_id;
        //|| combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_V2_TID) == table_id;
        bool is_all_column = combine_id(OB_SYS_TENANT_ID, OB_ALL_COLUMN_TID) == table_id;
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

int ObTableSqlService::add_table(ObISQLClient& sql_client, const ObTableSchema& table, const bool only_history)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = table.get_tenant_id();
  // For compatibility, system table's schema should be recorded in system tenant.
  const uint64_t exec_tenant_id =
      is_inner_table(table.get_table_id()) ? OB_SYS_TENANT_ID : ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(gen_table_dml(exec_tenant_id, table, dml))) {
    LOG_WARN("gen table dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      const int64_t table_id = table.get_table_id();
      const char* table_name = NULL;
      if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
        LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
      } else if (OB_FAIL(exec_insert(sql_client, table_id, table_name, dml, affected_rows))) {
        LOG_WARN("exec insert failed", K(ret));
      } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t is_deleted = 0;
      const char* table_name = NULL;
      if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(exec_tenant_id, table_name))) {
        LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
      } else if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(table_name, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
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
int ObTableSqlService::update_table_options(ObISQLClient& sql_client, const ObTableSchema& table_schema,
    ObTableSchema& new_table_schema, share::schema::ObSchemaOperationType operation_type,
    const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  uint64_t table_id = table_schema.get_table_id();
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table_schema.get_tenant_id());
  if (is_inner_table(table_id) && OB_SYS_TENANT_ID != exec_tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(table_schema));
  } else if (OB_FAIL(gen_table_options_dml(exec_tenant_id, new_table_schema, dml))) {
    LOG_WARN("gen table options dml failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(ret), K(affected_rows));
    }
  }

  // add to __all_table_history table
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, new_table_schema, only_history))) {
      LOG_WARN("add_table failed", K(new_table_schema), K(only_history), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if ((OB_DDL_DROP_TABLE_TO_RECYCLEBIN == operation_type) ||
        (OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN == operation_type)) {
      // 1. For oracle compatibility, drop foreign key while drop table.
      // 2. Foreign key will be rebuilded while truncate table.
      if (OB_FAIL(delete_foreign_key(sql_client, new_table_schema, new_table_schema.get_schema_version()))) {
        LOG_WARN("failed to delete foreign key", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(table_schema.get_tenant_id(), is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode", K(ret));
    }
  }

  // rename constraint name while drop/truncate table and recyclebin is on.
  if (OB_SUCC(ret) && is_oracle_mode) {
    if (OB_DDL_DROP_TABLE_TO_RECYCLEBIN == operation_type) {
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
  // delete from __all_table_stat, __all_column_stat and __all_histogram_stat
  if (OB_SUCC(ret)) {
    if (operation_type != OB_DDL_DROP_TABLE_TO_RECYCLEBIN) {
      // do nothing
    } else if (OB_FAIL(delete_from_all_table_stat(sql_client, table_id))) {
      LOG_WARN("delete from all table stat failed", K(ret));
    } else if (OB_FAIL(delete_from_all_column_stat(sql_client, table_id, table_schema.get_column_count(), false))) {
      LOG_WARN(
          "failed to delete all column stat", K(table_id), "column count", table_schema.get_column_count(), K(ret));
    } else if (OB_FAIL(delete_from_all_histogram_stat(sql_client, table_id))) {
      LOG_WARN("failed to delete all histogram_stat", K(table_id), K(ret));
    }
  }

  // log operation
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    if (NULL != ddl_stmt_str) {
      opt.ddl_stmt_str_ = *ddl_stmt_str;
    }
    opt.tenant_id_ = new_table_schema.get_tenant_id();
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
      const ObIArray<ObForeignKeyInfo>& foreign_key_infos = table_schema.get_foreign_key_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
        const ObForeignKeyInfo& foreign_key_info = foreign_key_infos.at(i);
        const uint64_t update_table_id = table_schema.get_table_id() == foreign_key_info.parent_table_id_
                                             ? foreign_key_info.child_table_id_
                                             : foreign_key_info.parent_table_id_;
        if (OB_FAIL(update_data_table_schema_version(sql_client, update_table_id))) {
          LOG_WARN("failed to update parent table schema version", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("alter table", "table type", table_schema.get_table_type(), "index type", table_schema.get_index_type());
    if (new_table_schema.is_index_table()) {
      if (OB_FAIL(update_data_table_schema_version(sql_client, new_table_schema.get_data_table_id()))) {
        LOG_WARN("update data table schema version failed", K(ret));
      }
    }
  }

  return ret;
}

// alter table drop constraint
int ObTableSqlService::delete_single_constraint(const int64_t new_schema_version, common::ObISQLClient& sql_client,
    const ObTableSchema& new_table_schema, const ObConstraint& orig_constraint)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t table_id = new_table_schema.get_table_id();
  const uint64_t constraint_id = orig_constraint.get_constraint_id();

  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
      OB_FAIL(dml.add_pk_column("constraint_id", constraint_id))) {
    LOG_WARN("add constraint failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_CONSTRAINT_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(affected_rows), K(ret));
    } else if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    }
  }

  ObSqlString sql;
  int64_t affected_rows = 0;
  // mark delete in __all_constraint_history
  if (OB_SUCC(ret)) {
    const int64_t is_deleted = 1;
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (TENANT_ID, TABLE_ID, CONSTRAINT_ID, SCHEMA_VERSION, IS_DELETED) values "
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
  if (OB_SUCC(ret) && (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100)) {
    if (CONSTRAINT_TYPE_CHECK == orig_constraint.get_constraint_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "do ddl with check constraint before cluster upgrade to 310");
    }
  }
  if (OB_SUCC(ret) && (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100)) {
    const int64_t is_deleted = 1;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    const ObConstraint* constraint =
        const_cast<ObTableSchema&>(new_table_schema).get_constraint(orig_constraint.get_constraint_id());
    for (ObConstraint::const_cst_col_iterator cst_col_iter = constraint->cst_col_begin();
         OB_SUCC(ret) && (cst_col_iter != constraint->cst_col_end());
         ++cst_col_iter) {
      dml.reset();
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
          OB_FAIL(dml.add_pk_column(
              "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, orig_constraint.get_table_id()))) ||
          OB_FAIL(dml.add_pk_column("constraint_id", orig_constraint.get_constraint_id())) ||
          OB_FAIL(dml.add_column("column_id", *cst_col_iter)) ||
          OB_FAIL(dml.add_column("schema_version", new_schema_version)) ||
          OB_FAIL(dml.add_column("is_deleted", is_deleted)) || OB_FAIL(dml.add_gmt_create()) ||
          OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("dml add constraint column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
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

int ObTableSqlService::delete_single_column(const int64_t new_schema_version, common::ObISQLClient& sql_client,
    const ObTableSchema& new_table_schema, const ObColumnSchemaV2& orig_column_schema)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t table_id = new_table_schema.get_table_id();
  const uint64_t column_id = orig_column_schema.get_column_id();

  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
      OB_FAIL(dml.add_pk_column("column_id", column_id))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(affected_rows), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id)) || OB_FAIL(dml.add_pk_column("table_id", table_id)) ||
        OB_FAIL(dml.add_pk_column("column_id", column_id))) {
      LOG_WARN("add column failed", K(ret));
    } else {
      int64_t affected_rows = 0;
      if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_COLUMN_STAT_TNAME, dml, affected_rows))) {
        LOG_WARN("exec delete column stat failed", K(ret));
      } else if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_HISTOGRAM_STAT_TNAME, dml, affected_rows))) {
        LOG_WARN("exec delete histogram stat failed", K(ret));
      }
    }
  }

  ObSqlString sql;
  int64_t affected_rows = 0;
  // : TODO
  // mark delete in __all_column_history
  if (OB_SUCC(ret)) {
    const int64_t is_deleted = 1;
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (TENANT_ID, TABLE_ID, COLUMN_ID, SCHEMA_VERSION, IS_DELETED) values "
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
  if (OB_SUCC(ret)) {
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

int ObTableSqlService::create_table(ObTableSchema& table, ObISQLClient& sql_client,
    const ObString* ddl_stmt_str /*=NULL*/, const bool need_sync_schema_version, const bool is_truncate_table /*false*/)
{
  int ret = OB_SUCCESS;
  int64_t start_usec = ObTimeUtility::current_time();
  int64_t end_usec = 0;
  int64_t cost_usec = 0;

  if (!table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid create table argument, ", K(table));
  }
  if (OB_SUCCESS == ret && 0 != table.get_autoinc_column_id()) {
    if (OB_FAIL(add_sequence(
            table.get_tenant_id(), table.get_table_id(), table.get_autoinc_column_id(), table.get_auto_increment()))) {
      LOG_WARN("insert sequence record failed", K(ret), K(table));
    }
    end_usec = ObTimeUtility::current_time();
    cost_usec = end_usec - start_usec;
    start_usec = end_usec;
    LOG_INFO("add_sequence for autoinc cost: ", K(cost_usec));
  }
  // hidden primary key start with 1; incre
  if (OB_SUCCESS == ret && NULL != table.get_column_schema(OB_HIDDEN_PK_INCREMENT_COLUMN_ID)) {
    if (OB_FAIL(add_sequence(table.get_tenant_id(), table.get_table_id(), OB_HIDDEN_PK_INCREMENT_COLUMN_ID, 1))) {
      LOG_WARN("insert sequence record failed", K(ret), K(table));
    }
    end_usec = ObTimeUtility::current_time();
    cost_usec = end_usec - start_usec;
    start_usec = end_usec;
    LOG_INFO("add_sequence for hidden_pk cost: ", K(cost_usec));
  }

  bool only_history = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_table(sql_client, table, only_history))) {
    LOG_WARN("insert table schema failed, ", K(ret), "table", to_cstring(table));
  } else if (!table.is_view_table()) {
    end_usec = ObTimeUtility::current_time();
    cost_usec = end_usec - start_usec;
    start_usec = end_usec;
    LOG_INFO("add_table cost: ", K(cost_usec));

    // view don't preserve column schema
    if (OB_FAIL(add_columns(sql_client, table))) {
      LOG_WARN("insert column schema failed, ", K(ret), "table", to_cstring(table));
    } else if (OB_FAIL(add_constraints(sql_client, table))) {
      LOG_WARN("insert constraint schema failed, ", K(ret), "table", to_cstring(table));
    }
    end_usec = ObTimeUtility::current_time();
    cost_usec = end_usec - start_usec;
    start_usec = end_usec;
    LOG_INFO("add_column cost: ", K(cost_usec));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_table_part_info(sql_client, table))) {
        LOG_WARN("fail to add_table_part_info", K(ret));
      }
      end_usec = ObTimeUtility::current_time();
      cost_usec = end_usec - start_usec;
      start_usec = end_usec;
      LOG_INFO("add part info cost: ", K(cost_usec));
    }
    // insert into all_foreign_key.
    if (OB_SUCC(ret) && !is_inner_table(table.get_table_id())) {
      if (OB_FAIL(add_foreign_key(sql_client, table, false /* only_history */))) {
        LOG_WARN("failed to add foreign key", K(ret));
      }
    }
  }

  ObSchemaOperation opt;
  opt.tenant_id_ = table.get_tenant_id();
  opt.database_id_ = table.get_database_id();
  opt.tablegroup_id_ = table.get_tablegroup_id();
  opt.table_id_ = table.get_table_id();
  if (is_truncate_table) {
    opt.op_type_ = OB_DDL_TRUNCATE_TABLE_CREATE;
  } else {
    if (table.is_index_table()) {
      opt.op_type_ = table.is_global_index_table() ? OB_DDL_CREATE_GLOBAL_INDEX : OB_DDL_CREATE_INDEX;
    } else if (table.is_view_table()) {
      opt.op_type_ = OB_DDL_CREATE_VIEW;
    } else {
      opt.op_type_ = OB_DDL_CREATE_TABLE;
    }
  }
  opt.schema_version_ = table.get_schema_version();
  opt.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
    if (OB_SUCC(ret)) {
      end_usec = ObTimeUtility::current_time();
      cost_usec = end_usec - start_usec;
      start_usec = end_usec;
      LOG_INFO("log_operation cost: ", K(cost_usec));
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("add table", "table type", table.get_table_type(), "index type", table.get_index_type());
      if ((table.is_index_table() || table.is_materialized_view()) && need_sync_schema_version) {
        if (OB_FAIL(update_data_table_schema_version(sql_client, table.get_data_table_id()))) {
          LOG_WARN("fail to update schema_version", K(ret));
        }
        end_usec = ObTimeUtility::current_time();
        cost_usec = end_usec - start_usec;
        start_usec = end_usec;
        LOG_INFO("update_data_table_schema_version cost: ", K(cost_usec));
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_index_status(const uint64_t data_table_id, const uint64_t index_table_id,
    const ObIndexStatus status, const int64_t create_mem_version, const int64_t new_schema_version,
    common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTableSchema index_schema;
  const uint64_t tenant_id = extract_tenant_id(data_table_id);
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id || status <= INDEX_STATUS_NOT_FOUND ||
      status >= INDEX_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_table_id), K(index_table_id), K(status));
  } else if (OB_FAIL(update_data_table_schema_version(sql_client, data_table_id))) {
    LOG_WARN("update data table schema version failed", K(ret));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, index_table_id))) ||
        OB_FAIL(dml.add_column("create_mem_version", create_mem_version)) ||
        OB_FAIL(dml.add_column("schema_version", new_schema_version)) ||
        OB_FAIL(dml.add_column("index_status", status)) || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    } else {
      int64_t affected_rows = 0;
      const char* table_name = NULL;
      if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
        LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
      } else if (OB_FAIL(exec_update(sql_client, index_table_id, table_name, dml, affected_rows))) {
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
    if (OB_FAIL(schema_service_.get_table_schema_from_inner_table(
            schema_status, index_table_id, sql_client, index_schema))) {
      LOG_WARN("get_table_schema failed", K(index_table_id), K(ret));
    } else {
      const bool only_history = true;
      index_schema.set_index_status(status);
      index_schema.set_create_mem_version(create_mem_version);
      index_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(add_table(sql_client, index_schema, only_history))) {
        LOG_WARN("add_table failed", K(index_schema), K(only_history), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = extract_tenant_id(index_table_id);
    opt.database_id_ = 0;
    opt.tablegroup_id_ = 0;
    opt.table_id_ = index_table_id;
    opt.op_type_ =
        index_schema.is_global_index_table() ? OB_DDL_MODIFY_GLOBAL_INDEX_STATUS : OB_DDL_MODIFY_INDEX_STATUS;
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::gen_table_dml(const uint64_t exec_tenant_id, const ObTableSchema& table, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  char* zone_list_buf = NULL;
  common::ObArray<common::ObZone> zone_list;
  if (NULL == (zone_list_buf = static_cast<char*>(ob_malloc(MAX_ZONE_LIST_LENGTH, ObModIds::OB_SCHEMA)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (table.get_locality_str().empty()) {
    MEMSET(zone_list_buf, 0, MAX_ZONE_LIST_LENGTH);
  } else if (OB_FAIL(table.get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(table.zone_array2str(zone_list, zone_list_buf, MAX_ZONE_LIST_LENGTH))) {
    LOG_WARN("zone array2str failed", K(ret), K(zone_list));
  } else {
  }
  if (OB_SUCC(ret)) {
    const ObPartitionOption& part_option = table.get_part_option();
    const ObPartitionOption& sub_part_option = table.get_sub_part_option();
    const char* expire_info = table.get_expire_info().length() <= 0 ? "" : table.get_expire_info().ptr();
    const char* part_func_expr =
        part_option.get_part_func_expr_str().length() <= 0 ? "" : part_option.get_part_func_expr_str().ptr();
    const char* sub_part_func_expr =
        sub_part_option.get_part_func_expr_str().length() <= 0 ? "" : sub_part_option.get_part_func_expr_str().ptr();
    const char* primary_zone = table.get_primary_zone().length() <= 0 ? "" : table.get_primary_zone().ptr();
    const char* locality = table.get_locality_str().length() <= 0 ? "" : table.get_locality_str().ptr();
    const char* previous_locality =
        table.get_previous_locality_str().empty() ? "" : table.get_previous_locality_str().ptr();
    const char* encryption = table.get_encryption_str().empty() ? "" : table.get_encryption_str().ptr();
    const int64_t INVALID_REPLICA_NUM = -1;
    if (OB_FAIL(check_table_options(table))) {
      LOG_WARN("fail to check table option", K(ret), K(table));
    } else if (OB_FAIL(dml.add_pk_column(
                   "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table.get_tenant_id()))) ||
               OB_FAIL(dml.add_pk_column(
                   "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_table_id()))) ||
               OB_FAIL(dml.add_column("table_name", ObHexEscapeSqlStr(table.get_table_name()))) ||
               OB_FAIL(dml.add_column(
                   "database_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_database_id()))) ||
               OB_FAIL(dml.add_column("table_type", table.get_table_type())) ||
               OB_FAIL(dml.add_column("load_type", table.get_load_type())) ||
               OB_FAIL(dml.add_column("def_type", table.get_def_type())) ||
               OB_FAIL(dml.add_column("rowkey_column_num", table.get_rowkey_column_num())) ||
               OB_FAIL(dml.add_column("index_column_num", table.get_index_column_num())) ||
               OB_FAIL(dml.add_column("max_used_column_id", table.get_max_used_column_id())) ||
               OB_FAIL(dml.add_column("max_used_constraint_id", table.get_max_used_constraint_id())) ||
               OB_FAIL(dml.add_column("session_id", table.get_session_id())) ||
               OB_FAIL(dml.add_column("sess_active_time", table.get_sess_active_time()))
               //|| OB_FAIL(dml.add_column("create_host", table.get_create_host()))
               || OB_FAIL(dml.add_column("replica_num", INVALID_REPLICA_NUM)) ||
               OB_FAIL(dml.add_column("tablet_size", table.get_tablet_size())) ||
               OB_FAIL(dml.add_column("pctfree", table.get_pctfree())) ||
               OB_FAIL(dml.add_column("autoinc_column_id", table.get_autoinc_column_id())) ||
               OB_FAIL(dml.add_column("auto_increment", share::ObRealUInt64(table.get_auto_increment()))) ||
               OB_FAIL(dml.add_column("read_only", table.is_read_only())) ||
               OB_FAIL(dml.add_column("rowkey_split_pos", table.get_rowkey_split_pos())) ||
               OB_FAIL(dml.add_column("compress_func_name", ObHexEscapeSqlStr(table.get_compress_func_name()))) ||
               OB_FAIL(dml.add_column("expire_condition", ObHexEscapeSqlStr(expire_info))) ||
               OB_FAIL(dml.add_column("is_use_bloomfilter", table.is_use_bloomfilter())) ||
               OB_FAIL(dml.add_column("index_attributes_set", table.get_index_attributes_set())) ||
               OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(table.get_comment()))) ||
               OB_FAIL(dml.add_column("block_size", table.get_block_size())) ||
               OB_FAIL(dml.add_column("collation_type", table.get_collation_type())) ||
               OB_FAIL(dml.add_column(
                   "data_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_data_table_id()))) ||
               OB_FAIL(dml.add_column("index_status", table.get_index_status())) ||
               OB_FAIL(dml.add_column(
                   "tablegroup_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_tablegroup_id()))) ||
               OB_FAIL(dml.add_column("progressive_merge_num", table.get_progressive_merge_num())) ||
               OB_FAIL(dml.add_column("index_type", table.get_index_type())) ||
               OB_FAIL(dml.add_column("index_using_type", table.get_index_using_type())) ||
               OB_FAIL(dml.add_column("part_level", table.get_part_level())) ||
               OB_FAIL(dml.add_column("part_func_type", part_option.get_part_func_type())) ||
               OB_FAIL(dml.add_column("part_func_expr", ObHexEscapeSqlStr(part_func_expr))) ||
               OB_FAIL(dml.add_column("part_num", part_option.get_part_num())) ||
               OB_FAIL(dml.add_column("max_used_part_id", part_option.get_max_used_part_id())) ||
               OB_FAIL(dml.add_column(
                   "partition_cnt_within_partition_table", part_option.get_partition_cnt_within_partition_table())) ||
               OB_FAIL(dml.add_column("sub_part_func_type", sub_part_option.get_part_func_type())) ||
               OB_FAIL(dml.add_column("sub_part_func_expr", ObHexEscapeSqlStr(sub_part_func_expr))) ||
               OB_FAIL(dml.add_column("sub_part_num", sub_part_option.get_part_num())) ||
               OB_FAIL(dml.add_column("create_mem_version", table.get_create_mem_version())) ||
               OB_FAIL(dml.add_column("schema_version", table.get_schema_version())) ||
               OB_FAIL(dml.add_column(
                   "view_definition", ObHexEscapeSqlStr(table.get_view_schema().get_view_definition()))) ||
               OB_FAIL(dml.add_column("view_check_option", table.get_view_schema().get_view_check_option())) ||
               OB_FAIL(dml.add_column("view_is_updatable", table.get_view_schema().get_view_is_updatable())) ||
               OB_FAIL(dml.add_column("zone_list", ObHexEscapeSqlStr(zone_list_buf))) ||
               OB_FAIL(dml.add_column("parser_name", ObHexEscapeSqlStr(table.get_parser_name_str()))) ||
               OB_FAIL(dml.add_gmt_create()) || OB_FAIL(dml.add_gmt_modified()) ||
               OB_FAIL(dml.add_column("primary_zone", ObHexEscapeSqlStr(primary_zone))) ||
               OB_FAIL(dml.add_column("locality", ObHexEscapeSqlStr(locality))) ||
               OB_FAIL(dml.add_column("partition_status", table.get_partition_status())) ||
               OB_FAIL(dml.add_column("partition_schema_version", table.get_partition_schema_version())) ||
               OB_FAIL(dml.add_column("previous_locality", ObHexEscapeSqlStr(previous_locality))) ||
               OB_FAIL(dml.add_column("pk_comment", ObHexEscapeSqlStr(table.get_pk_comment()))) ||
               OB_FAIL(dml.add_column("row_store_type",
                   ObHexEscapeSqlStr(ObStoreFormat::get_row_store_name(table.get_row_store_type())))) ||
               OB_FAIL(dml.add_column("store_format",
                   ObHexEscapeSqlStr(ObStoreFormat::get_store_format_name(table.get_store_format())))) ||
               OB_FAIL(dml.add_column("duplicate_scope", table.get_duplicate_scope())) ||
               OB_FAIL(dml.add_column("binding", table.get_binding())) ||
               OB_FAIL(dml.add_column("progressive_merge_round", table.get_progressive_merge_round())) ||
               OB_FAIL(dml.add_column("storage_format_version", table.get_storage_format_version())) ||
               OB_FAIL(dml.add_column("table_mode", table.get_table_mode())) ||
               OB_FAIL(dml.add_column("encryption", ObHexEscapeSqlStr(encryption))) ||
               OB_FAIL(dml.add_column(
                   "tablespace_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_tablespace_id()))) ||
               OB_FAIL(dml.add_column("drop_schema_version", table.get_drop_schema_version()))
               // To avoid compatibility problems (such as error while upgrade virtual schema) in upgrade post stage,
               // cluster version judgemenet is needed if columns are added in upgrade post stage.
               || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2270 &&
                      OB_FAIL(dml.add_column("is_sub_part_template", table.is_sub_part_template()))) ||
               (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2270 && OB_FAIL(dml.add_column("dop", table.get_dop()))) ||
               (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2271 &&
                   OB_FAIL(
                       dml.add_column("character_set_client", table.get_view_schema().get_character_set_client()))) ||
               (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2271 &&
                   OB_FAIL(
                       dml.add_column("collation_connection", table.get_view_schema().get_collation_connection()))) ||
               (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100 &&
                   OB_FAIL(dml.add_column("auto_part", table.get_part_option().is_auto_range_part()))) ||
               (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100 &&
                   OB_FAIL(dml.add_column("auto_part_size", table.get_part_option().get_auto_part_size())))) {
      LOG_WARN("add column failed", K(ret));
    }
  }
  if (NULL != zone_list_buf) {
    ob_free(zone_list_buf);
    zone_list_buf = NULL;
  }
  return ret;
}

int ObTableSqlService::gen_table_options_dml(
    const uint64_t exec_tenant_id, const ObTableSchema& table, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;

  char* zone_list_buf = NULL;
  common::ObArray<common::ObZone> zone_list;
  if (NULL == (zone_list_buf = static_cast<char*>(ob_malloc(MAX_ZONE_LIST_LENGTH, ObModIds::OB_SCHEMA)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (table.get_locality_str().empty()) {
    MEMSET(zone_list_buf, 0, MAX_ZONE_LIST_LENGTH);
  } else if (OB_FAIL(table.get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(table.zone_array2str(zone_list, zone_list_buf, MAX_ZONE_LIST_LENGTH))) {
    LOG_WARN("zone array2str failed", K(ret), K(zone_list));
  } else {
  }
  if (OB_SUCC(ret)) {
    const ObPartitionOption& part_option = table.get_part_option();
    const ObPartitionOption& sub_part_option = table.get_sub_part_option();
    const char* table_name = table.get_table_name_str().length() <= 0 ? "" : table.get_table_name_str().ptr();
    const char* comment = table.get_comment_str().length() <= 0 ? "" : table.get_comment_str().ptr();
    const char* expire_info = table.get_expire_info().length() <= 0 ? "" : table.get_expire_info().ptr();
    const char* part_func_expr =
        part_option.get_part_func_expr_str().length() <= 0 ? "" : part_option.get_part_func_expr_str().ptr();
    const char* sub_part_func_expr =
        sub_part_option.get_part_func_expr_str().length() <= 0 ? "" : sub_part_option.get_part_func_expr_str().ptr();
    const char* primary_zone = table.get_primary_zone().length() <= 0 ? "" : table.get_primary_zone().ptr();
    const char* locality = table.get_locality_str().length() <= 0 ? "" : table.get_locality_str().ptr();
    const char* previous_locality =
        table.get_previous_locality_str().empty() ? "" : table.get_previous_locality_str().ptr();
    const char* encryption = table.get_encryption_str().empty() ? "" : table.get_encryption_str().ptr();
    const int64_t INVALID_REPLICA_NUM = -1;

    if (OB_FAIL(check_table_options(table))) {
      LOG_WARN("fail to check table option", K(ret), K(table));
    } else if (OB_FAIL(dml.add_pk_column(
                   "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table.get_tenant_id()))) ||
               OB_FAIL(dml.add_pk_column(
                   "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_table_id()))) ||
               OB_FAIL(dml.add_column("block_size", table.get_block_size())) ||
               OB_FAIL(dml.add_column("progressive_merge_num", table.get_progressive_merge_num())) ||
               OB_FAIL(dml.add_column(
                   "database_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_database_id()))) ||
               OB_FAIL(dml.add_column("autoinc_column_id", table.get_autoinc_column_id())) ||
               OB_FAIL(dml.add_column("auto_increment", share::ObRealUInt64(table.get_auto_increment()))) ||
               OB_FAIL(dml.add_column("read_only", table.is_read_only())) ||
               OB_FAIL(dml.add_column("replica_num", INVALID_REPLICA_NUM)) ||
               OB_FAIL(dml.add_column("tablet_size", table.get_tablet_size())) ||
               OB_FAIL(dml.add_column("pctfree", table.get_pctfree())) ||
               OB_FAIL(dml.add_column("collation_type", table.get_collation_type())) ||
               OB_FAIL(dml.add_column("is_use_bloomfilter", table.is_use_bloomfilter())) ||
               OB_FAIL(dml.add_column("index_attributes_set", table.get_index_attributes_set())) ||
               OB_FAIL(dml.add_column("expire_condition", ObHexEscapeSqlStr(expire_info))) ||
               OB_FAIL(dml.add_column("max_used_column_id", table.get_max_used_column_id())) ||
               OB_FAIL(dml.add_column("max_used_constraint_id", table.get_max_used_constraint_id())) ||
               OB_FAIL(dml.add_column("compress_func_name", ObHexEscapeSqlStr(table.get_compress_func_name()))) ||
               OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(comment))) ||
               OB_FAIL(dml.add_column(
                   "tablegroup_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_tablegroup_id()))) ||
               OB_FAIL(dml.add_column("primary_zone", ObHexEscapeSqlStr(primary_zone))) ||
               OB_FAIL(dml.add_column("table_name", ObHexEscapeSqlStr(table_name))) ||
               OB_FAIL(dml.add_column("zone_list", ObHexEscapeSqlStr(zone_list_buf))) ||
               OB_FAIL(dml.add_column("part_func_expr", ObHexEscapeSqlStr(part_func_expr))) ||
               OB_FAIL(dml.add_column("sub_part_func_expr", ObHexEscapeSqlStr(sub_part_func_expr))) ||
               OB_FAIL(dml.add_column("schema_version", table.get_schema_version())) ||
               OB_FAIL(dml.add_gmt_modified()) || OB_FAIL(dml.add_column("locality", ObHexEscapeSqlStr(locality))) ||
               OB_FAIL(dml.add_column("previous_locality", ObHexEscapeSqlStr(previous_locality))) ||
               OB_FAIL(dml.add_column("pk_comment", ObHexEscapeSqlStr(table.get_pk_comment()))) ||
               OB_FAIL(dml.add_column("row_store_type",
                   ObHexEscapeSqlStr(ObStoreFormat::get_row_store_name(table.get_row_store_type())))) ||
               OB_FAIL(dml.add_column("store_format",
                   ObHexEscapeSqlStr(ObStoreFormat::get_store_format_name(table.get_store_format())))) ||
               OB_FAIL(dml.add_column("duplicate_scope", table.get_duplicate_scope())) ||
               OB_FAIL(dml.add_column("binding", table.get_binding())) ||
               OB_FAIL(dml.add_column("progressive_merge_round", table.get_progressive_merge_round())) ||
               OB_FAIL(dml.add_column("storage_format_version", table.get_storage_format_version())) ||
               OB_FAIL(dml.add_column("table_mode", table.get_table_mode())) ||
               OB_FAIL(dml.add_column("encryption", ObHexEscapeSqlStr(encryption))) ||
               OB_FAIL(dml.add_column(
                   "tablespace_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table.get_tablespace_id()))) ||
               OB_FAIL(dml.add_column("drop_schema_version", table.get_drop_schema_version()))
               // To avoid compatibility problems (such as error while upgrade virtual schema) in upgrade post stage,
               // cluster version judgemenet is needed if columns are added in upgrade post stage.
               || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2270 &&
                      OB_FAIL(dml.add_column("is_sub_part_template", table.is_sub_part_template()))) ||
               (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100 &&
                   OB_FAIL(dml.add_column("auto_part", table.get_part_option().is_auto_range_part()))) ||
               (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100 &&
                   OB_FAIL(dml.add_column("auto_part_size", table.get_part_option().get_auto_part_size()))) ||
               (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2270 && OB_FAIL(dml.add_column("dop", table.get_dop())))) {
      LOG_WARN("add column failed", K(ret));
    }
  }

  if (NULL != zone_list_buf) {
    ob_free(zone_list_buf);
    zone_list_buf = NULL;
  }

  return ret;
}

int ObTableSqlService::update_table_attribute(ObISQLClient& sql_client, ObTableSchema& new_table_schema,
    const ObSchemaOperationType operation_type, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t table_id = new_table_schema.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (is_inner_table(table_id) && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(new_table_schema));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
             OB_FAIL(dml.add_column("schema_version", new_table_schema.get_schema_version())) ||
             OB_FAIL(dml.add_column("max_used_column_id", new_table_schema.get_max_used_column_id())) ||
             OB_FAIL(dml.add_column("max_used_constraint_id", new_table_schema.get_max_used_constraint_id())) ||
             OB_FAIL(dml.add_column("session_id", new_table_schema.get_session_id())) ||
             OB_FAIL(dml.add_column("sess_active_time", new_table_schema.get_sess_active_time()))
             //|| OB_FAIL(dml.add_column("create_host", new_table_schema.get_create_host()))
             || OB_FAIL(dml.add_column("autoinc_column_id", new_table_schema.get_autoinc_column_id())) ||
             OB_FAIL(dml.add_column("auto_increment", share::ObRealUInt64(new_table_schema.get_auto_increment())))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }

  // add updated table to __all_table_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, new_table_schema, only_history))) {
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

int ObTableSqlService::gen_partition_option_dml(const ObTableSchema& table, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table.get_tenant_id());
  const ObPartitionOption& part_option = table.get_part_option();
  const ObPartitionOption& sub_part_option = table.get_sub_part_option();
  const char* part_func_expr =
      part_option.get_part_func_expr_str().length() <= 0 ? "" : part_option.get_part_func_expr_str().ptr();
  const char* sub_part_func_expr =
      sub_part_option.get_part_func_expr_str().length() <= 0 ? "" : sub_part_option.get_part_func_expr_str().ptr();
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
      OB_FAIL(dml.add_column("part_level", table.get_part_level())) ||
      OB_FAIL(dml.add_column("part_func_type", part_option.get_part_func_type())) ||
      OB_FAIL(dml.add_column("part_func_expr", ObHexEscapeSqlStr(part_func_expr))) ||
      OB_FAIL(dml.add_column("part_num", part_option.get_part_num())) ||
      OB_FAIL(dml.add_column("sub_part_func_type", sub_part_option.get_part_func_type())) ||
      OB_FAIL(dml.add_column("sub_part_func_expr", ObHexEscapeSqlStr(sub_part_func_expr))) ||
      OB_FAIL(dml.add_column("sub_part_num", sub_part_option.get_part_num())) ||
      OB_FAIL(dml.add_column("max_used_part_id", table.get_part_option().get_max_used_part_id())) ||
      OB_FAIL(dml.add_column("schema_version", table.get_schema_version())) ||
      OB_FAIL(dml.add_column("partition_status", table.get_partition_status())) ||
      OB_FAIL(dml.add_column("partition_schema_version", table.get_partition_schema_version())) ||
      OB_FAIL(dml.add_column("auto_part", table.get_part_option().is_auto_range_part())) ||
      OB_FAIL(dml.add_column("auto_part_size", table.get_part_option().get_auto_part_size())) ||
      OB_FAIL(dml.add_gmt_create()) || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObTableSqlService::update_partition_option(
    ObISQLClient& sql_client, ObTableSchema& table, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = table.get_table_id();
  ObDMLSqlSplicer dml;
  if (is_inner_table(table_id) && OB_SYS_TENANT_ID != table.get_tenant_id()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(table));
  } else if (OB_FAIL(gen_partition_option_dml(table, dml))) {
    LOG_WARN("fail to gen dml", K(ret));
  } else {
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table.get_tenant_id());
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }

  // add updated table to __all_table_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, table, only_history))) {
      LOG_WARN("add_table failed", K(table), K(only_history));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = table.get_tenant_id();
      opt.database_id_ = table.get_database_id();
      opt.tablegroup_id_ = table.get_tablegroup_id();
      opt.table_id_ = table.get_table_id();
      opt.op_type_ = OB_DDL_ALTER_TABLE;
      opt.schema_version_ = table.get_schema_version();
      opt.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_partition_option(
    ObISQLClient& sql_client, ObTableSchema& table, const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  const uint64_t table_id = table.get_table_id();
  table.set_schema_version(new_schema_version);
  ObDMLSqlSplicer dml;
  if (is_inner_table(table_id) && OB_SYS_TENANT_ID != table.get_tenant_id()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(table));
  } else if (OB_FAIL(gen_partition_option_dml(table, dml))) {
    LOG_WARN("fail to gen dml", K(ret));
  } else {
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table.get_tenant_id());
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }

  // add updated table to __all_table_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, table, only_history))) {
      LOG_WARN("add_table failed", K(table), K(only_history));
    } else {
    }
  }
  return ret;
}

int ObTableSqlService::update_partition_cnt_within_partition_table(ObISQLClient& sql_client, ObTableSchema& table)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const ObPartitionOption& part_option = table.get_part_option();
  ObDMLSqlSplicer dml;
  if (is_inner_table(table_id) && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(table));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
             OB_FAIL(dml.add_column(
                 "partition_cnt_within_partition_table", part_option.get_partition_cnt_within_partition_table())) ||
             OB_FAIL(dml.add_column("schema_version", table.get_schema_version())) || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }

  // add updated table to __all_table_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, table, only_history))) {
      LOG_WARN("add_table failed", K(table), K(only_history));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = table.get_tenant_id();
      opt.database_id_ = table.get_database_id();
      opt.tablegroup_id_ = table.get_tablegroup_id();
      opt.table_id_ = table.get_table_id();
      opt.op_type_ = OB_DDL_ALTER_TABLE;
      opt.schema_version_ = table.get_schema_version();
      opt.ddl_stmt_str_ = ObString();
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_max_used_part_id(ObISQLClient& sql_client, ObTableSchema& table)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const ObPartitionOption& part_option = table.get_part_option();
  ObDMLSqlSplicer dml;
  if (is_inner_table(table_id) && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(table));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
             OB_FAIL(dml.add_column("max_used_part_id", part_option.get_max_used_part_id())) ||
             OB_FAIL(dml.add_column("schema_version", table.get_schema_version())) || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }

  // add updated table to __all_table_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, table, only_history))) {
      LOG_WARN("add_table failed", K(table), K(only_history));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = table.get_tenant_id();
      opt.database_id_ = table.get_database_id();
      opt.tablegroup_id_ = table.get_tablegroup_id();
      opt.table_id_ = table.get_table_id();
      opt.op_type_ = OB_DDL_ALTER_TABLE;
      opt.schema_version_ = table.get_schema_version();
      opt.ddl_stmt_str_ = ObString();
      if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
        LOG_WARN("log operation failed", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_all_part_for_subpart(
    ObISQLClient& sql_client, const ObTableSchema& table, const ObIArray<ObPartition*>& update_part_array)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  ObDMLSqlSplicer dml;
  if (table.is_sub_part_template()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("template cannot modify subpart", K(ret), K(table));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < update_part_array.count(); i++) {
      ObPartition* inc_part = update_part_array.at(i);
      if (OB_ISNULL(inc_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc_part_array[i] is NULL", K(ret), K(i));
      } else {
        if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
            OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
            OB_FAIL(dml.add_pk_column("part_id", inc_part->get_part_id())) ||
            OB_FAIL(dml.add_column("max_used_sub_part_id", inc_part->get_max_used_sub_part_id())) ||
            OB_FAIL(dml.add_column("sub_part_num", inc_part->get_sub_part_num())) ||
            OB_FAIL(dml.add_column("schema_version", table.get_schema_version())) || OB_FAIL(dml.add_gmt_modified())) {
          LOG_WARN("add column failed", K(ret));
        } else {
          int64_t affected_rows = 0;
          if (OB_FAIL(exec_update(sql_client, table_id, OB_ALL_PART_TNAME, dml, affected_rows))) {
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
      ObPartition* inc_part = update_part_array.at(i);
      if (OB_ISNULL(inc_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc_part_array[i] is NULL", K(ret), K(i));
      } else {
        ObAddIncPartDMLGenerator part_dml_gen(&table, *inc_part, -1, -1, table.get_schema_version());
        if (OB_FAIL(part_dml_gen.gen_dml(history_dml))) {
          LOG_WARN("gen dml failed", K(ret));
        } else if (OB_FAIL(history_dml.add_column("is_deleted", false))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(history_dml.finish_row())) {
          LOG_WARN("failed to finish row", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && update_part_array.count() != 0) {
      int64_t affected_rows = 0;
      ObSqlString part_history_sql;
      if (OB_FAIL(history_dml.splice_batch_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
      } else if (affected_rows != update_part_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("history affected_rows is unexpected", K(ret), K(update_part_array.count()), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_subpartition_option(
    ObISQLClient& sql_client, const ObTableSchema& table, const ObIArray<ObPartition*>& update_part_array)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  if (table.is_sub_part_template()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("template cannot update subpart option", K(ret), K(table));
  } else if (!table.has_self_partition()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table has not self partition", KR(ret));
  } else if (OB_FAIL(update_all_part_for_subpart(sql_client, table, update_part_array))) {
    LOG_WARN("fail to update all part", K(ret));
  }

  return ret;
}

int ObTableSqlService::delete_from_all_table(ObISQLClient& sql_client, uint64_t table_id)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = extract_tenant_id(table_id);
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_delete(sql_client, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_temp_table(
    ObISQLClient& sql_client, const uint64_t tenant_id, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_TEMP_TABLE_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::delete_from_all_table_stat(ObISQLClient& sql_client, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id)) || OB_FAIL(dml.add_pk_column("table_id", table_id))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_TABLE_STAT_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    }
  }

  return ret;
}
int ObTableSqlService::delete_from_all_histogram_stat(ObISQLClient& sql_client, uint64_t table_id)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
      OB_FAIL(dml.add_pk_column("table_id", table_id))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_HISTOGRAM_STAT_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_column(
    ObISQLClient& sql_client, uint64_t table_id, int64_t column_count, bool check_affect_rows)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (check_affect_rows && affected_rows < column_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not all row deleted, ", K(column_count), K(affected_rows), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_column_stat(
    ObISQLClient& sql_client, uint64_t table_id, int64_t column_count, bool check_affect_rows)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
      OB_FAIL(dml.add_pk_column("table_id", table_id))) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_delete(sql_client, table_id, OB_ALL_COLUMN_STAT_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (check_affect_rows && affected_rows < column_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not all row deleted, ", K(column_count), K(affected_rows), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::delete_from_all_table_history(
    ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  // insert into __all_table_history
  const char* table_name = NULL;
  if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(exec_tenant_id, table_name))) {
    LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id,table_id,schema_version,data_table_id,is_deleted)"
                                    " VALUES(%lu,%lu,%ld,%lu,%ld)",
                 table_name,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, table_schema.get_tenant_id()),
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schema.get_table_id()),
                 new_schema_version,
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schema.get_data_table_id()),
                 is_deleted))) {
    LOG_WARN("assign insert into all talbe history fail", K(table_schema), K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql fail", K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no row has inserted", K(ret));
  }
  return ret;
}

int ObTableSqlService::delete_from_all_column_history(
    ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(sql.append_fmt("INSERT /*+use_plan_cache(none)*/ INTO %s "
                             "(TENANT_ID, TABLE_ID, COLUMN_ID, SCHEMA_VERSION, IS_DELETED) VALUES ",
          OB_ALL_COLUMN_HISTORY_TNAME))) {
    LOG_WARN("append_fmt failed", K(ret));
  }
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
       OB_SUCCESS == ret && iter != table_schema.column_end();
       ++iter) {
    if (OB_FAIL(sql.append_fmt("%s(%lu, %lu, %lu, %ld, %ld)",
            (iter == table_schema.column_begin()) ? "" : ",",
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, (*iter)->get_tenant_id()),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (*iter)->get_table_id()),
            (*iter)->get_column_id(),
            new_schema_version,
            is_deleted))) {
      LOG_WARN("append_fmt failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (table_schema.get_column_count() != affected_rows) {
      LOG_WARN("affected_rows not same with column_count",
          K(affected_rows),
          "column_count",
          table_schema.get_column_count(),
          K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_data_table_schema_version(
    ObISQLClient& sql_client, const uint64_t data_table_id, int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(data_table_id);
  const uint64_t fetch_tenant_id = is_inner_table(data_table_id) ? OB_SYS_TENANT_ID : tenant_id;
  const uint64_t exec_tenant_id =
      is_inner_table(data_table_id) ? OB_SYS_TENANT_ID : ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = fetch_tenant_id;
  ObTableSchema table_schema;
  if (OB_INVALID_VERSION == new_schema_version &&
      OB_FAIL(schema_service_.gen_new_schema_version(fetch_tenant_id, OB_INVALID_VERSION, new_schema_version))) {
    // for generating different schema version for the same table in one trans
    LOG_WARN("fail to gen new schema version", K(ret), K(fetch_tenant_id));
  } else if (OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data table id", K(data_table_id));
  } else if (OB_FAIL(schema_service_.get_table_schema_from_inner_table(
                 schema_status, data_table_id, sql_client, table_schema))) {
    LOG_WARN("get_table_schema failed", K(data_table_id), K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(
                 dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, data_table_id))) ||
             OB_FAIL(dml.add_column("schema_version", new_schema_version)) || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, data_table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (!table_schema.is_dropped_schema() && !is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret), K(tenant_id), K(data_table_id));
    } else if (table_schema.is_dropped_schema() && !is_zero_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret), K(exec_tenant_id), K(data_table_id));
    }
  }
  // add new table_schema to __all_table_history
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    table_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(add_table(sql_client, table_schema, only_history))) {
      LOG_WARN("add_table failed", K(table_schema), K(only_history), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = extract_tenant_id(data_table_id);
    opt.database_id_ = 0;
    opt.tablegroup_id_ = 0;
    opt.table_id_ = data_table_id;
    opt.op_type_ = OB_DDL_MODIFY_TABLE_SCHEMA_VERSION;
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::add_sequence(
    const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, const uint64_t auto_increment)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  // FIXME:__all_time_zone contains auto increment column. Cyclic dependence may occur.
  const uint64_t exec_tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : tenant_id;
  if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans, ", K(ret));
  } else {
    ObSqlString sql;
    ObSqlString values;
    if (OB_FAIL(sql.append_fmt("INSERT INTO %s (", OB_ALL_SEQUENCE_V2_TNAME))) {
      LOG_WARN("append table name failed, ", K(ret));
    } else {
      SQL_COL_APPEND_VALUE(
          sql, values, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), "tenant_id", "%lu");
      SQL_COL_APPEND_VALUE(
          sql, values, ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id), "sequence_key", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, column_id, "column_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, 0 == auto_increment ? 1 : auto_increment, "sequence_value", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, 0 == auto_increment ? 0 : auto_increment - 1, "sync_value", "%lu");
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(
              ", gmt_modified) VALUES (%.*s, now(6))", static_cast<int32_t>(values.length()), values.ptr()))) {
        LOG_WARN("append sql failed, ", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute. ", "sql", sql.ptr(), K(ret));
      } else {
        if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), "sql", sql.ptr(), K(ret));
        }
      }
    }
  }
  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(temp_ret));
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
    }
  }
  return ret;
}

int ObTableSqlService::sync_aux_schema_version_for_history(
    ObISQLClient& sql_client, const ObTableSchema& aux_schema1, const uint64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const bool only_history = true;
  ObTableSchema aux_schema;
  if (OB_FAIL(aux_schema.assign(aux_schema1))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else {
    aux_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(add_table(sql_client, aux_schema, only_history))) {
      LOG_WARN("add_table failed", K(aux_schema), K(only_history), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = aux_schema.get_tenant_id();
    opt.database_id_ = aux_schema.get_database_id();
    opt.tablegroup_id_ = aux_schema.get_tablegroup_id();
    opt.table_id_ = aux_schema.get_table_id();
    opt.op_type_ = OB_DDL_MODIFY_TABLE_SCHEMA_VERSION;  // TODO@: rename
    opt.schema_version_ = new_schema_version;
    if (OB_FAIL(log_operation_wrapper(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::sync_schema_version_for_history(
    ObISQLClient& sql_client, ObTableSchema& schema, uint64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const bool only_history = true;
  schema.set_schema_version(new_schema_version);
  if (OB_FAIL(add_table(sql_client, schema, only_history))) {
    LOG_WARN("fail to add table for history", K(schema), K(ret));
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
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::update_tablegroup(ObSchemaGetterGuard& schema_guard, ObTableSchema& new_table_schema,
    common::ObISQLClient& sql_client, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  char* zone_list_buf = NULL;
  common::ObArray<common::ObZone> zone_list;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (is_inner_table(new_table_schema.get_table_id()) && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(new_table_schema));
  } else if (NULL == (zone_list_buf = static_cast<char*>(ob_malloc(MAX_ZONE_LIST_LENGTH, ObModIds::OB_SCHEMA)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(new_table_schema.get_zone_list(schema_guard, zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(new_table_schema.zone_array2str(zone_list, zone_list_buf, MAX_ZONE_LIST_LENGTH))) {
    LOG_WARN("zone array2str failed", K(ret), K(zone_list));
  } else {
  }
  if (OB_SUCC(ret)) {
    const char* primary_zone =
        new_table_schema.get_primary_zone().length() <= 0 ? "" : new_table_schema.get_primary_zone().ptr();
    const char* locality =
        new_table_schema.get_locality_str().length() <= 0 ? "" : new_table_schema.get_locality_str().ptr();

    if (OB_FAIL(check_table_options(new_table_schema))) {
      LOG_WARN("fail to check table option", K(ret), K(new_table_schema));
    } else if (OB_FAIL(
                   dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
               OB_FAIL(dml.add_pk_column("table_id",
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, new_table_schema.get_table_id()))) ||
               OB_FAIL(dml.add_column("schema_version", new_table_schema.get_schema_version())) ||
               OB_FAIL(dml.add_column("tablegroup_id",
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, new_table_schema.get_tablegroup_id()))) ||
               OB_FAIL(dml.add_column("primary_zone", ObHexEscapeSqlStr(primary_zone))) ||
               OB_FAIL(dml.add_column("locality", ObHexEscapeSqlStr(locality))) ||
               OB_FAIL(dml.add_column("zone_list", ObHexEscapeSqlStr(zone_list_buf)))) {
      LOG_WARN("add column failed", K(ret));
    } else {
      int64_t affected_rows = 0;
      const char* table_name = NULL;
      if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
        LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
      } else if (OB_FAIL(exec_update(sql_client, new_table_schema.get_table_id(), table_name, dml, affected_rows))) {
        LOG_WARN("exec update failed", K(ret));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(affected_rows), K(ret));
      }
    }
    // insert __all_table_history
    const bool only_history = true;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_table(sql_client, new_table_schema, only_history))) {
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
  if (NULL != zone_list_buf) {
    ob_free(zone_list_buf);
    zone_list_buf = NULL;
  }
  return ret;
}

int ObTableSqlService::gen_column_dml(
    const uint64_t exec_tenant_id, const ObColumnSchemaV2& column, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;

  ObString orig_default_value;
  ObString cur_default_value;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  char* extended_type_info_buf = NULL;
  
  if (ob_is_json(column.get_data_type()) && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_313) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("column is json type not support until cluster version not less than 3.1.3.");
  } else if (column.is_generated_column() ||
      ob_is_string_type(column.get_data_type()) ||
      ob_is_json(column.get_data_type())) {
    // The default value of the generated column is the expression definition of the generated column
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
    char* orig_default_value_buf = NULL;
    char* cur_default_value_buf = NULL;
    orig_default_value_buf = static_cast<char*>(allocator.alloc(value_buf_len));
    cur_default_value_buf = static_cast<char*>(allocator.alloc(value_buf_len));
    extended_type_info_buf = static_cast<char*>(allocator.alloc(OB_MAX_VARBINARY_LENGTH));
    ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
    if (OB_ISNULL(orig_default_value_buf) || OB_ISNULL(cur_default_value_buf) || OB_ISNULL(extended_type_info_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for default value buffer failed");
    } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(column.get_tenant_id(), compat_mode))) {
      LOG_WARN("fail to get tenant mode", K(ret));
    } else {
      MEMSET(orig_default_value_buf, 0, value_buf_len);
      MEMSET(cur_default_value_buf, 0, value_buf_len);
      MEMSET(extended_type_info_buf, 0, OB_MAX_VARBINARY_LENGTH);

      int64_t orig_default_value_len = 0;
      int64_t cur_default_value_len = 0;
      share::CompatModeGuard compat_mode_guard(compat_mode);
      ObTimeZoneInfo tz_info;
      if (OB_FAIL(OTTZ_MGR.get_tenant_tz(exec_tenant_id, tz_info.get_tz_map_wrap()))) {
        LOG_WARN("get tenant timezone failed", K(ret));
      } else if (OB_FAIL(column.get_orig_default_value().print_plain_str_literal(
                     orig_default_value_buf, value_buf_len, orig_default_value_len, &tz_info))) {
        LOG_WARN("failed to print orig default value", K(ret), K(value_buf_len), K(orig_default_value_len));
      } else if (OB_FAIL(column.get_cur_default_value().print_plain_str_literal(
                     cur_default_value_buf, value_buf_len, cur_default_value_len, &tz_info))) {
        LOG_WARN("failed to print cur default value", K(ret), K(value_buf_len), K(cur_default_value_len));
      } else {
        orig_default_value.assign_ptr(orig_default_value_buf, static_cast<int32_t>(orig_default_value_len));
        cur_default_value.assign_ptr(cur_default_value_buf, static_cast<int32_t>(cur_default_value_len));
      }
      LOG_DEBUG("begin gen_column_dml",
          K(ret),
          K(compat_mode),
          K(orig_default_value),
          K(cur_default_value),
          K(orig_default_value_len),
          K(cur_default_value_len));
    }
  }
  LOG_DEBUG("begin gen_column_dml", K(ret), K(orig_default_value), K(cur_default_value), K(column));
  if (OB_SUCC(ret)) {
    ObString cur_default_value_v1;
    if (column.get_orig_default_value().is_null()) {
      orig_default_value.reset();
    }
    if (column.get_cur_default_value().is_null()) {
      cur_default_value.reset();
    }
    ObString bin_extended_type_info;
    if (OB_SUCC(ret) && column.is_enum_or_set()) {
      int64_t pos = 0;
      if (OB_FAIL(column.serialize_extended_type_info(extended_type_info_buf, OB_MAX_VARBINARY_LENGTH, pos))) {
        LOG_WARN("fail to serialize_extended_type_info", K(ret));
      } else {
        bin_extended_type_info.assign_ptr(extended_type_info_buf, static_cast<int32_t>(pos));
      }
    }
    if (OB_SUCC(ret) &&
        (OB_FAIL(dml.add_pk_column(
             "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, column.get_tenant_id()))) ||
            OB_FAIL(dml.add_pk_column(
                "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column.get_table_id()))) ||
            OB_FAIL(dml.add_pk_column("column_id", column.get_column_id())) ||
            OB_FAIL(dml.add_column("column_name", ObHexEscapeSqlStr(column.get_column_name()))) ||
            OB_FAIL(dml.add_column("rowkey_position", column.get_rowkey_position())) ||
            OB_FAIL(dml.add_column("index_position", column.get_index_position())) ||
            OB_FAIL(dml.add_column("partition_key_position", column.get_tbl_part_key_pos())) ||
            OB_FAIL(dml.add_column("data_type", column.get_data_type())) ||
            OB_FAIL(dml.add_column("data_length", column.get_data_length())) ||
            OB_FAIL(dml.add_column("data_precision", column.get_data_precision())) ||
            OB_FAIL(dml.add_column("data_scale", column.get_data_scale())) ||
            OB_FAIL(dml.add_column("zero_fill", column.is_zero_fill())) ||
            OB_FAIL(dml.add_column("nullable", column.is_nullable())) ||
            OB_FAIL(dml.add_column("autoincrement", column.is_autoincrement())) ||
            OB_FAIL(dml.add_column("is_hidden", column.is_hidden())) ||
            OB_FAIL(dml.add_column("on_update_current_timestamp", column.is_on_update_current_timestamp())) ||
            OB_FAIL(dml.add_column("orig_default_value_v2", ObHexEscapeSqlStr(orig_default_value))) ||
            OB_FAIL(dml.add_column("cur_default_value_v2", ObHexEscapeSqlStr(cur_default_value))) ||
            OB_FAIL(dml.add_column("cur_default_value", ObHexEscapeSqlStr(cur_default_value_v1))) ||
            OB_FAIL(dml.add_column("order_in_rowkey", column.get_order_in_rowkey())) ||
            OB_FAIL(dml.add_column("collation_type", column.get_collation_type())) ||
            OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(column.get_comment()))) ||
            OB_FAIL(dml.add_column("schema_version", column.get_schema_version())) ||
            OB_FAIL(dml.add_column("column_flags", column.get_stored_column_flags())) ||
            OB_FAIL(dml.add_column("extended_type_info", ObHexEscapeSqlStr(bin_extended_type_info))) ||
            OB_FAIL(dml.add_column("prev_column_id", column.get_prev_column_id())) || OB_FAIL(dml.add_gmt_create()) ||
            OB_FAIL(dml.add_gmt_modified()))) {
      LOG_WARN("dml add column failed", K(ret));
    }
  }

  return ret;
}

int ObTableSqlService::gen_constraint_dml(
    const uint64_t exec_tenant_id, const ObConstraint& constraint, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dml.add_pk_column(
          "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, constraint.get_tenant_id()))) ||
      OB_FAIL(dml.add_pk_column(
          "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, constraint.get_table_id()))) ||
      OB_FAIL(dml.add_pk_column("constraint_id", constraint.get_constraint_id())) ||
      OB_FAIL(dml.add_column("schema_version", constraint.get_schema_version())) ||
      OB_FAIL(dml.add_column("constraint_type", constraint.get_constraint_type())) ||
      OB_FAIL(dml.add_column("constraint_name", ObHexEscapeSqlStr(constraint.get_constraint_name()))) ||
      OB_FAIL(dml.add_column("check_expr", ObHexEscapeSqlStr(constraint.get_check_expr()))) ||
      OB_FAIL(dml.add_column("rely_flag", constraint.get_rely_flag())) ||
      OB_FAIL(dml.add_column("enable_flag", constraint.get_enable_flag())) ||
      OB_FAIL(dml.add_column("validate_flag", constraint.get_validate_flag())) || OB_FAIL(dml.add_gmt_create()) ||
      OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("dml add constraint failed", K(ret));
  }

  return ret;
}

int ObTableSqlService::gen_constraint_column_dml(
    const uint64_t exec_tenant_id, const ObConstraint& constraint, uint64_t column_id, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dml.add_pk_column(
          "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, constraint.get_tenant_id()))) ||
      OB_FAIL(dml.add_pk_column(
          "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, constraint.get_table_id()))) ||
      OB_FAIL(dml.add_pk_column("constraint_id", constraint.get_constraint_id())) ||
      OB_FAIL(dml.add_pk_column("column_id", column_id)) ||
      OB_FAIL(dml.add_column("schema_version", constraint.get_schema_version())) || OB_FAIL(dml.add_gmt_create()) ||
      OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("dml add constraint column failed", K(ret));
  }

  return ret;
}

// Generate new constraint name while drop table to recyclebin.
int ObTableSqlService::gen_constraint_update_name_dml(const uint64_t exec_tenant_id, const ObString& cst_name,
    const int64_t new_schema_version, const ObConstraint& constraint, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dml.add_pk_column(
          "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, constraint.get_tenant_id()))) ||
      OB_FAIL(dml.add_pk_column(
          "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, constraint.get_table_id()))) ||
      OB_FAIL(dml.add_pk_column("constraint_id", constraint.get_constraint_id())) ||
      OB_FAIL(dml.add_column("schema_version", new_schema_version)) ||
      OB_FAIL(dml.add_column("constraint_name", ObHexEscapeSqlStr(cst_name))) || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("dml add constraint failed", K(ret));
  }

  return ret;
}

// Generate new constraint name while drop table to recyclebin.
int ObTableSqlService::gen_constraint_insert_new_name_row_dml(const uint64_t exec_tenant_id, const ObString& cst_name,
    const int64_t new_schema_version, const ObConstraint& constraint, share::ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  const int64_t is_deleted = 0;

  if (OB_FAIL(dml.add_pk_column(
          "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, constraint.get_tenant_id()))) ||
      OB_FAIL(dml.add_pk_column(
          "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, constraint.get_table_id()))) ||
      OB_FAIL(dml.add_pk_column("constraint_id", constraint.get_constraint_id())) ||
      OB_FAIL(dml.add_column("schema_version", new_schema_version)) ||
      OB_FAIL(dml.add_column("constraint_type", constraint.get_constraint_type())) ||
      OB_FAIL(dml.add_column("constraint_name", ObHexEscapeSqlStr(cst_name))) ||
      OB_FAIL(dml.add_column("check_expr", ObHexEscapeSqlStr(constraint.get_check_expr()))) ||
      OB_FAIL(dml.add_gmt_create()) || OB_FAIL(dml.add_gmt_modified()) ||
      OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
    LOG_WARN("dml add constraint failed", K(ret));
  }

  return ret;
}

int ObTableSqlService::log_core_operation(common::ObISQLClient& sql_client, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatProxy proxy(sql_client);
  if (OB_FAIL(proxy.set_core_schema_version(schema_version))) {
    LOG_WARN("set_core_schema_version failed", "core_schema_version", schema_version, K(ret));
  }
  return ret;
}

int ObTableSqlService::add_table_part_info(ObISQLClient& sql_client, const ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  if (is_core_table(table.get_table_id())) {
    // do nothing
  } else if (is_inner_table(table.get_table_id())) {
    // do nothing
  } else {
    const bool is_tablegroup_def = false;
    const ObPartitionSchema* table_schema = &table;
    ObAddPartInfoHelper part_helper(table_schema, sql_client, is_tablegroup_def);
    if (OB_FAIL(part_helper.add_partition_info())) {
      LOG_WARN("add partition info failed", K(ret));
    } else if (OB_FAIL(part_helper.add_partition_info_for_gc())) {
      LOG_WARN("failed to add partition info for gc", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::add_split_partition_for_gc(common::ObISQLClient& sql_client, const ObPartitionSchema& table)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const int64_t inc_part_num = table.get_partition_num();
  ObPartition** part_array = table.get_part_array();
  ObPartition* part = NULL;
  int64_t count = 0;
  const uint64_t tenant_id = table.get_tenant_id();
  if (!table.has_self_partition()) {
    // skip
  } else if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part array is null", K(ret), K(part_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
      part = part_array[i];
      // TODO: For now, we only support split partition in first partition table.
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", K(ret), K(i), K(table));
      } else if (1 != part->get_source_part_ids().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("source part id is invalid", K(ret), K(i), "partition", *part);
      } else if (OB_FAIL(dml.add_pk_column(
                     "tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, table.get_tenant_id()))) ||
                 OB_FAIL(dml.add_pk_column(
                     "table_id", ObSchemaUtils::get_extract_schema_id(tenant_id, table.get_table_id()))) ||
                 OB_FAIL(dml.add_pk_column("partition_id", part->get_part_id()))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish row", K(ret), "part", *part);
      } else {
        count++;
      }
    }
    if (OB_SUCC(ret)) {
      ObSqlString part_gc_sql;
      int64_t affected_rows = 0;
      if (1 >= count) {
        // non partition table to partition table(which partition num is also 1)
      } else if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_TENANT_GC_PARTITION_INFO_TNAME, part_gc_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_gc_sql));
      } else if (OB_FAIL(sql_client.write(tenant_id, part_gc_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_gc_sql));
      } else if (affected_rows != count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is unexpected", K(ret), K(count), K(affected_rows));
      }
    }
  }
  return ret;
}
// has_physical_partition: PG or standalone partition need be recorded in __all_tenant_gc_partition_info.
int ObTableSqlService::split_part_info(ObISQLClient& sql_client, const ObPartitionSchema& new_schema,
    const int64_t schema_version, const share::ObSplitInfo& split_info, const bool has_physical_partition)
{
  int ret = OB_SUCCESS;
  if (is_core_table(new_schema.get_table_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("split core table not supported", K(ret), "table_id", new_schema.get_table_id());
  } else if (is_inner_table(new_schema.get_table_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("split inner table not supported", K(ret), "table_id", new_schema.get_table_id());
  } else {
    // split table
    ObSplitPartHelperV2 part_helper(new_schema, schema_version, split_info, sql_client);
    if (OB_FAIL(part_helper.split_partition())) {
      LOG_WARN("add partition info failed", K(ret));
    } else if (has_physical_partition && OB_FAIL(add_split_partition_for_gc(sql_client, new_schema))) {
      LOG_WARN("add gc part info failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::add_inc_part_info(ObISQLClient& sql_client, const ObTableSchema& ori_table,
    const ObTableSchema& inc_table, const int64_t schema_version, bool is_truncate_table)
{
  int ret = OB_SUCCESS;
  if (is_core_table(ori_table.get_table_id())) {
    // do nothing
  } else if (is_inner_table(ori_table.get_table_id())) {
    // do nothing
  } else {
    const bool is_tablegroup_def = false;
    const ObPartitionSchema* ori_table_schema = &ori_table;
    const ObPartitionSchema* inc_table_schema = &inc_table;
    ObAddIncPartHelper part_helper(ori_table_schema, inc_table_schema, schema_version, sql_client, is_tablegroup_def);
    if (OB_FAIL(part_helper.add_partition_info(false))) {
      LOG_WARN("add partition info failed", K(ret));
    } else if (OB_FAIL(part_helper.add_partition_info_for_gc())) {
      LOG_WARN("failed to add partition info for gc", K(ret));
    } else if (!is_truncate_table && OB_INVALID_ID == ori_table.get_tablegroup_id()) {
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

/*
 * add subpartition in non-template secondary partition table
 * [@input]:
 * 1. inc_table_schema: only following members are useful.
 *    - partition_num_ : first partition num
 *      (Truncate subpartitions in different first partitions in one ddl stmt is supported.)
 *    - partition_array_ : related first partition schema array
 * 2. For each partition in partition_array_: only following members are useful.
 *    - part_id_ : physical first partition_id
 *    - max_used_sub_part_id_: max used sub part id in first partition
 *    - subpartition_num_ : (to be added) secondary partition num in first partition
 *    - subpartition_array_ : related secondary partition schema array
 * 3. For each subpartition in subpartition_array_: only following members are useful.
 *    - subpart_id_ : should be -1, actual physical secondary subpart_id will be caculated by max_used_sub_part_id_.
 */
int ObTableSqlService::add_inc_subpart_info(ObISQLClient& sql_client, const ObTableSchema& ori_table,
    const ObTableSchema& inc_table, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (ori_table.is_sub_part_template()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("template cannot update subpart option", KR(ret), K(ori_table));
  } else if (!ori_table.has_self_partition()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table has not self partition", KR(ret), K(ori_table));
  } else {
    const bool is_tablegroup_def = false;
    const ObPartitionSchema* ori_table_schema = &ori_table;
    const ObPartitionSchema* inc_table_schema = &inc_table;
    ObAddIncSubPartHelper subpart_helper(
        ori_table_schema, inc_table_schema, schema_version, sql_client, is_tablegroup_def);
    if (OB_FAIL(subpart_helper.add_subpartition_info(false))) {
      LOG_WARN("add partition info failed", KR(ret));
    } else if (OB_FAIL(subpart_helper.add_subpartition_info_for_gc())) {
      LOG_WARN("failed to add partition info for gc", KR(ret));
    }
  }
  return ret;
}

int ObTableSqlService::log_operation_wrapper(ObSchemaOperation& opt, ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;

  const ObSchemaOperationType type = opt.op_type_;
  const uint64_t table_id = opt.table_id_;
  const int64_t schema_version = opt.schema_version_;
  // For compatibility, system table's schema should be recorded in system tenant.
  const uint64_t exec_tenant_id = is_inner_table(opt.table_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(opt.table_id_);
  if (type <= OB_DDL_TABLE_OPERATION_BEGIN || type >= OB_DDL_TABLE_OPERATION_END) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected operation type", K(ret), K(type));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(table_id));
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema_version", K(ret), K(schema_version));
  } else if (OB_FAIL(log_operation(opt, sql_client, exec_tenant_id))) {
    LOG_WARN("failed to log operation", K(opt), K(ret));
  } else if (is_core_table(table_id)) {
    if (OB_FAIL(log_core_operation(sql_client, schema_version))) {
      LOG_WARN("log_core_version failed", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::remove_source_partition(ObISQLClient& client, const ObSplitInfo& split_info,
    const int64_t schema_version, const ObTablegroupSchema* tablegroup_schema, const ObTableSchema* table_schema,
    bool is_delay_delete, const ObArray<ObPartition>& split_part_array)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t table_id;
  const ObPartitionSchema* origin_schema = NULL;

  if (table_schema != NULL) {
    table_id = table_schema->get_table_id();
    origin_schema = table_schema;
  } else if (tablegroup_schema != NULL) {
    table_id = tablegroup_schema->get_table_id();
    origin_schema = tablegroup_schema;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Both tablegroup_schema and table_schema is null", K(ret));
  }
  int64_t tenant_id = extract_tenant_id(table_id);
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_ID == table_id || schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(schema_version));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE TENANT_ID = %ld "
                                    "AND TABLE_ID = %ld AND PART_ID IN (",
                 OB_ALL_PART_TNAME,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else {
    for (int64_t i = 0; i < split_info.source_part_ids_.count() && OB_SUCC(ret); i++) {
      if (i == 0) {
        if (OB_FAIL(sql.append_fmt("%lu", split_info.source_part_ids_.at(i)))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      } else {
        if (OB_FAIL(sql.append_fmt(", %lu", split_info.source_part_ids_.at(i)))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("append_fmt failed", K(ret));
    }
  }
  int64_t affected_rows = 0;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
      //} else if (affected_rows != pairs.count()) {
      //  ret = OB_ERR_UNEXPECTED;
      //  LOG_WARN("get invalid affected_rows", K(ret), K(affected_rows),
      //           "expect_rows", pairs.count());
    }
  }

  // insert into __all_table_history
  if (OB_FAIL(ret)) {
  } else if (is_delay_delete) {
    ObDMLSqlSplicer history_dml;
    ObString delay_deleted_name;
    for (int64_t i = 0; OB_SUCC(ret) && i < split_part_array.count(); ++i) {
      ObAddIncPartDMLGenerator part_dml_gen(origin_schema, split_part_array.at(i), 0, 0, schema_version);
      if (OB_FAIL(part_dml_gen.gen_dml_for_delay_delete(history_dml))) {
        LOG_WARN("gen dml history failed", K(ret));
      } else if (OB_FAIL(history_dml.add_column("is_deleted", false))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(history_dml.finish_row())) {
        LOG_WARN("failed to finish row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObSqlString part_history_sql;
      if (OB_FAIL(history_dml.splice_batch_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(part_history_sql));
      } else if (OB_FAIL(client.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(part_history_sql));
        //} else if (affected_rows != split_info.source_part_ids_.count()) {
        //  ret = OB_ERR_UNEXPECTED;
        //  LOG_WARN("history affected_rows is unexpected", K(ret),
        //           K(split_info.source_part_ids_.count()), K(affected_rows));
      }
    }
  } else {
    ObSqlString history_sql;
    if (OB_FAIL(history_sql.assign_fmt("INSERT INTO %s (TENANT_ID, TABLE_ID, PART_ID, "
                                       "SCHEMA_VERSION, IS_DELETED) VALUES ",
            OB_ALL_PART_HISTORY_TNAME))) {
      LOG_WARN("fail to assign fmt", K(ret));
    } else {
      for (int64_t i = 0; i < split_info.source_part_ids_.count() && OB_SUCC(ret); i++) {
        if (i == 0) {
          if (OB_FAIL(history_sql.append_fmt("(%ld, %ld, %ld, %ld, %d)",
                  ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                  ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                  split_info.source_part_ids_.at(i),
                  schema_version,
                  true))) {
            LOG_WARN("fail to append fmt", K(ret));
          }
        } else {
          if (OB_FAIL(history_sql.append_fmt(", (%ld, %ld, %ld, %ld, %d)",
                  tenant_id,
                  table_id,
                  split_info.source_part_ids_.at(i),
                  schema_version,
                  true))) {
            LOG_WARN("fail to append fmt", K(ret));
          }
        }
      }
    }
    affected_rows = 0;
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (OB_FAIL(client.write(exec_tenant_id, history_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", K(ret), K(history_sql));
      //} else if (affected_rows != pairs.count()) {
      //  ret = OB_ERR_UNEXPECTED;
      //  LOG_WARN("get invalid affected_rows", K(ret), K(affected_rows),
      //           "expect_rows", pairs.count());
    }
  }

  return ret;
}

int ObTableSqlService::modify_dest_partition(common::ObISQLClient& client, const ObSimpleTableSchemaV2& table_schema,
    const obrpc::ObSplitPartitionArg& arg, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t table_id = table_schema.get_table_id();
  int64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!table_schema.is_valid() || schema_version <= 0 || arg.split_info_.get_spp_array().count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(schema_version), K(arg));
  } else if (OB_FAIL(
                 sql.assign_fmt("UPDATE %s SET SOURCE_PARTITION_ID = -1, SCHEMA_VERSION = %ld  WHERE TENANT_ID = %ld "
                                "AND TABLE_ID = %ld AND PART_ID IN (",
                     OB_ALL_PART_TNAME,
                     schema_version,
                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else {
    for (int64_t i = 0; i < arg.split_info_.get_spp_array().count() && OB_SUCC(ret); i++) {
      const ObSplitPartitionPair& pair = arg.split_info_.get_spp_array().at(i);
      for (int64_t j = 0; j < pair.get_dest_array().count() && OB_SUCC(ret); j++) {
        int64_t partition_id = pair.get_dest_array().at(j).get_partition_id();
        if (i == 0 && j == 0) {
          if (OB_FAIL(sql.append_fmt("%ld", partition_id))) {
            LOG_WARN("fail to append fmt", K(ret), K(tenant_id), K(table_id), K(partition_id));
          }
        } else {
          if (OB_FAIL(sql.append_fmt(", %ld", partition_id))) {
            LOG_WARN("fail to append fmt", K(ret), K(tenant_id), K(table_id), K(partition_id));
          }
        }
      }  // end for
    }    // end for
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("append_fmt failed", K(ret));
    }
  }
  int64_t affected_rows = 0;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
    }
  }

  // insert to __all_table_history
  if (OB_FAIL(ret)) {
    // nothing todo
  } else {
    ObUpdatePartHisInfoHelper part_helper(arg, table_schema, client, schema_version);
    if (OB_FAIL(part_helper.update_partition_info())) {
      LOG_WARN("fail to upadte partition info", K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::add_partition_key(common::ObISQLClient& client, const ObTableSchema& origin_table_schema,
    const ObTableSchema& table_schema, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!table_schema.is_valid() || schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(schema_version));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(client, exec_tenant_id);
    int64_t affected_rows = 0;
    for (int64_t i = 0; i < table_schema.get_column_count() && OB_SUCC(ret); i++) {
      const ObColumnSchemaV2* column = table_schema.get_column_schema_by_idx(i);
      if (OB_ISNULL(column)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("get invalid column schema", K(ret), K(i), K(table_schema));
      } else if (!column->is_tbl_part_key_column()) {
        // nothing todo
      } else {
        const ObColumnSchemaV2* origin_column = origin_table_schema.get_column_schema(column->get_column_id());
        if (OB_ISNULL(origin_column)) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("get invalid column schema", K(ret), K(origin_table_schema));
        } else {
          ObColumnSchemaV2 new_column = *origin_column;
          dml.reset();
          new_column.set_tbl_part_key_pos(column->get_tbl_part_key_pos());
          new_column.set_schema_version(schema_version);
          new_column.set_tenant_id(table_schema.get_tenant_id());
          new_column.set_table_id(table_schema.get_table_id());
          if (OB_FAIL(gen_column_dml(exec_tenant_id, new_column, dml))) {
            LOG_WARN("fail to gen column dml", K(ret), K(i), K(new_column));
          } else if (OB_FAIL(exec.exec_update(OB_ALL_COLUMN_TNAME, dml, affected_rows))) {
            LOG_WARN("fail to exec update", K(ret));
          } else if (affected_rows > 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value", K(ret), K(affected_rows));
          } else if (OB_FAIL(dml.add_column("is_deleted", 0))) {
            LOG_WARN("fail to add column", K(ret));
          } else if (OB_FAIL(exec.exec_insert(OB_ALL_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
            LOG_WARN("fail to exec insert", K(ret));
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

int ObTableSqlService::batch_update_partition_option(common::ObISQLClient& client, const uint64_t tenant_id,
    const ObIArray<const ObTableSchema*>& table_schemas, const int64_t schema_version, const ObPartitionStatus status)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (table_schemas.count() <= 0) {
    // nothing todo
  } else {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                   dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
               OB_FAIL(dml.add_column("schema_version", schema_version)) ||
               OB_FAIL(dml.add_column("partition_status", status))) {
      LOG_WARN("fail to add column", K(ret));
    } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("TABLE_ID in ("))) {
      LOG_WARN("fail to append fmt", K(ret));
    } else {
      for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
        const ObTableSchema* table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema error", K(ret), K(i));
        } else {
          const uint64_t table_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schema->get_table_id());
          if (0 == i) {
            if (OB_FAIL(dml.get_extra_condition().append_fmt("%ld", table_id))) {
              LOG_WARN("fail to append fmt", K(ret), K(i));
            }
          } else if (OB_FAIL(dml.get_extra_condition().append_fmt(", %ld", table_id))) {
            LOG_WARN("fail to append fmt", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.get_extra_condition().append_fmt(")"))) {
        LOG_WARN("append_fmt failed", K(ret));
      }
    }
    ObDMLExecHelper exec(client, exec_tenant_id);
    int64_t affected_rows = 0;
    const char* table_name = NULL;
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec.exec_update(table_name, dml, affected_rows))) {
      LOG_WARN("fail to exec update", K(ret));
    }
  }

  // UPDATE HISTRORY;
  if (OB_FAIL(ret)) {
  } else {
    ObDMLSqlSplicer dml;
    ObSqlString history_sql;
    ObSqlString value_str;
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      dml.reset();
      const ObTableSchema* table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema error", K(ret), K(i));
      } else {
        ObTableSchema copy_schema;
        if (OB_FAIL(copy_schema.assign(*table_schemas.at(i)))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else {
          // TODO: assign tables by tablegroup
          copy_schema.set_schema_version(schema_version);
          copy_schema.set_partition_status(status);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(gen_table_dml(exec_tenant_id, copy_schema, dml))) {
          LOG_WARN("fail to gen table dml", K(ret));
        } else if (OB_FAIL(dml.add_column("is_deleted", false))) {
          LOG_WARN("fail to add column", K(ret));
        } else if (0 == i) {
          const char* table_name = NULL;
          if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(exec_tenant_id, table_name))) {
            LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
          } else if (OB_FAIL(dml.splice_insert_sql(table_name, history_sql))) {
            LOG_WARN("fail to splice insert sql", K(ret));
          }
        } else {
          value_str.reset();
          if (OB_FAIL(dml.splice_values(value_str))) {
            LOG_WARN("fail to splice value", K(ret));
          } else if (OB_FAIL(history_sql.append_fmt(", (%s)", value_str.ptr()))) {
            LOG_WARN("fail to append fmt", K(ret), K(value_str));
          }
        }
      }
    }  // end for
    int64_t affected_rows = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(client.write(exec_tenant_id, history_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write to history table", K(ret), K(history_sql));
    }
  }
  return ret;
}

int ObTableSqlService::batch_modify_dest_partition(ObISQLClient& client, const int64_t tenant_id,
    const ObIArray<const ObTableSchema*>& table_schemas, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObSqlString sql;
  if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_version));
  } else if (table_schemas.count() <= 0) {
    // nothing todo
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE %s SET SOURCE_PARTITION_ID = -1, SCHEMA_VERSION = %ld WHERE TENANT_ID = %ld AND TABLE_ID IN (",
                 OB_ALL_PART_TNAME,
                 schema_version,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else {
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      if (OB_ISNULL(table_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema error", K(ret), K(i));
        break;
      }
      if (i == 0) {
        if (OB_FAIL(sql.append_fmt(
                "%ld", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schemas.at(i)->get_table_id())))) {
          LOG_WARN("fail to append fmt", K(ret), K(i));
        }
      } else if (OB_FAIL(sql.append_fmt(", %ld",
                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schemas.at(i)->get_table_id())))) {
        LOG_WARN("fail to append fmt", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(")"))) {
        LOG_WARN("append_fmt failed", K(ret));
      }
    }
    int64_t affected_rows = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
      }
    }
    // insert into __all_table_history
    if (OB_FAIL(ret)) {
    } else {
      ObDMLSqlSplicer dml;
      ObSqlString part_history_sql;
      ObSqlString value_str;
      bool sql_init = false;
      for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
        if (OB_ISNULL(table_schemas.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid schema", K(ret), K(i));
        } else {
          ObPartition** partition_array = table_schemas.at(i)->get_part_array();
          if (OB_ISNULL(partition_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid partition array", K(ret), K(i));
          }
          for (int64_t j = 0; j < table_schemas.at(i)->get_partition_num() && OB_SUCC(ret); j++) {
            if (partition_array[j]->get_source_part_ids().count() > 0 &&
                partition_array[j]->get_source_part_ids().at(0) >= 0) {
              dml.reset();
              ObUpdatePartHisDMLGenerator part_dml_gen(*partition_array[j], schema_version, *table_schemas.at(i));
              if (OB_FAIL(part_dml_gen.gen_dml(dml))) {
                LOG_WARN("fail to gen dml", K(ret));
              } else if (!sql_init) {
                if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
                  LOG_WARN("fail to splice insert sql", K(ret));
                }
                sql_init = true;
              } else {
                value_str.reset();
                if (OB_FAIL(dml.splice_values(value_str))) {
                  LOG_WARN("fail to splice values", K(ret));
                } else if (OB_FAIL(part_history_sql.append_fmt(", (%s)", value_str.ptr()))) {
                  LOG_WARN("append_fmt failed", K(value_str), K(ret));
                }
              }
            }
          }  // end for j
        }
      }  // end for i
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(client.write(exec_tenant_id, part_history_sql.ptr(), affected_rows))) {
          LOG_WARN("fail to write", K(ret), K(part_history_sql));
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::batch_update_max_used_part_id(ObISQLClient& trans, const int64_t tenant_id,
    const int64_t schema_version, const ObIArray<const ObTableSchema*>& table_schemas,
    const ObTablegroupSchema& alter_tablegroup_schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t inc_part_num = alter_tablegroup_schema.get_part_option().get_part_num();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const char* table_name = NULL;
  if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schemas));
  } else if (table_schemas.count() <= 0) {
    // nothing todo
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
    LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE %s SET SCHEMA_VERSION = %ld, MAX_USED_PART_ID = (CASE WHEN (MAX_USED_PART_ID = -1)"
                 " THEN %ld ELSE (MAX_USED_PART_ID + %ld) END)"
                 " WHERE TENANT_ID = %ld and TABLE_ID IN (",
                 table_name,
                 schema_version,
                 inc_part_num - 1,
                 inc_part_num,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
    LOG_WARN("fail to update max part id", K(ret));
  } else {
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      if (OB_ISNULL(table_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid schema", K(ret), K(i));
      } else {
        const uint64_t table_id =
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_schemas.at(i)->get_table_id());
        if (i == 0) {
          if (OB_FAIL(sql.append_fmt("%ld", table_id))) {
            LOG_WARN("fail to append fmt", K(ret));
          }
        } else if (OB_FAIL(sql.append_fmt(", %ld", table_id))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.append(")"))) {
        LOG_WARN("fail to append sql", K(ret));
      } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write", K(ret));
      }
    }
    // insert into __all_table_history
    if (OB_SUCC(ret)) {
      ObDMLSqlSplicer dml;
      ObSqlString history_sql;
      ObSqlString value_str;
      for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
        dml.reset();
        if (OB_ISNULL(table_schemas.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid schema", K(ret), K(i));
        } else {
          ObTableSchema copy_schema;
          if (OB_FAIL(copy_schema.assign(*table_schemas.at(i)))) {
            LOG_WARN("fail to assign schema", K(ret));
          } else {
            copy_schema.set_schema_version(schema_version);
            int64_t max_used_id = inc_part_num + copy_schema.get_part_option().get_max_used_part_id();
            copy_schema.get_part_option().set_max_used_part_id(max_used_id);
            if (OB_FAIL(gen_table_dml(exec_tenant_id, copy_schema, dml))) {
              LOG_WARN("fail to gen table dml", K(ret));
            } else if (OB_FAIL(dml.add_column("is_deleted", false))) {
              LOG_WARN("fail to add column", K(ret));
            } else if (i == 0) {
              const char* table_name = NULL;
              if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(exec_tenant_id, table_name))) {
                LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
              } else if (OB_FAIL(dml.splice_insert_sql(table_name, history_sql))) {
                LOG_WARN("fail to splice insert sql", K(ret));
              }
            } else {
              value_str.reset();
              if (OB_FAIL(dml.splice_values(value_str))) {
                LOG_WARN("fail to splice values", K(ret));
              } else if (OB_FAIL(history_sql.append_fmt(", (%s)", value_str.ptr()))) {
                LOG_WARN("fail to append fmt", K(ret));
              }
            }
          }
        }
      }
      int64_t affected_rows = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(trans.write(exec_tenant_id, history_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write to history table", K(ret), K(history_sql));
      }
    }  // end  history_table
  }
  return ret;
}

int ObTableSqlService::insert_ori_schema_version(
    ObISQLClient& sql_client, const uint64_t table_id, const int64_t& ori_schema_version)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = extract_tenant_id(table_id);
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (is_inner_table(table_id)) {
    // To avoid cyclic dependence, inner table won't record ori schema version
  } else {
    int64_t affected_rows = 0;
    ObSqlString insert_sql_string;
    if (OB_SUCCESS != (ret = insert_sql_string.append_fmt(
                           "INSERT INTO %s (TENANT_ID, TABLE_ID, ORI_SCHEMA_VERSION, BUILDING_SNAPSHOT,"
                           " gmt_create, gmt_modified) "
                           "values(%lu, %lu, %ld, 0, now(6), now(6))",
                           OB_ALL_ORI_SCHEMA_VERSION_TNAME,
                           ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                           ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                           ori_schema_version))) {
      LOG_WARN("sql string append format string failed, ", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, insert_sql_string.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed,  ", "sql", insert_sql_string.ptr(), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expect to 1, ", K(affected_rows), K(ret));
    } else {
      // do-nothing
    }
  }
  return ret;
}

int ObTableSqlService::insert_temp_table_info(ObISQLClient& sql_client, const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t table_id = table_schema.get_table_id();
  int64_t affected_rows = 0;
  ObSqlString insert_sql_string;
  if (table_schema.is_ctas_tmp_table() || table_schema.is_tmp_table()) {
    if (OB_SUCCESS != (ret = insert_sql_string.append_fmt(
                           "INSERT INTO %s (TENANT_ID, TABLE_ID, CREATE_HOST) values(%lu, %lu, \"%.*s\")",
                           OB_ALL_TEMP_TABLE_TNAME,
                           ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                           ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                           table_schema.get_create_host_str().length(),
                           table_schema.get_create_host_str().ptr()))) {
      LOG_WARN("sql string append format string failed, ", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, insert_sql_string.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed,  ", "sql", insert_sql_string.ptr(), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expect to 1, ", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObTableSqlService::gen_foreign_key_dml(
    const uint64_t exec_tenant_id, uint64_t tenant_id, const ObForeignKeyInfo& foreign_key_info, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  dml.reset();
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(dml.add_pk_column(
          "foreign_key_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_info.foreign_key_id_))) ||
      OB_FAIL(dml.add_column("foreign_key_name", ObHexEscapeSqlStr(foreign_key_info.foreign_key_name_))) ||
      OB_FAIL(dml.add_column(
          "child_table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_info.child_table_id_))) ||
      OB_FAIL(dml.add_column("parent_table_id",
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_info.parent_table_id_))) ||
      OB_FAIL(dml.add_column("update_action", foreign_key_info.update_action_)) ||
      OB_FAIL(dml.add_column("delete_action", foreign_key_info.delete_action_)) ||
      OB_FAIL(dml.add_column("enable_flag", foreign_key_info.enable_flag_)) ||
      OB_FAIL(dml.add_column("validate_flag", foreign_key_info.validate_flag_)) ||
      OB_FAIL(dml.add_column("rely_flag", foreign_key_info.rely_flag_)) ||
      OB_FAIL(dml.add_column("ref_cst_type", foreign_key_info.ref_cst_type_)) ||
      OB_FAIL(dml.add_column("ref_cst_id", foreign_key_info.ref_cst_id_)) || OB_FAIL(dml.add_gmt_create()) ||
      OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObTableSqlService::gen_foreign_key_column_dml(const uint64_t exec_tenant_id, uint64_t tenant_id,
    uint64_t foreign_key_id, uint64_t child_column_id, uint64_t parent_column_id, int64_t position,
    ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  dml.reset();
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
      OB_FAIL(
          dml.add_pk_column("foreign_key_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_id))) ||
      OB_FAIL(dml.add_pk_column("child_column_id", child_column_id)) ||
      OB_FAIL(dml.add_pk_column("parent_column_id", parent_column_id)) ||
      OB_FAIL(dml.add_column("position", position)) || OB_FAIL(dml.add_gmt_create()) ||
      OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObTableSqlService::delete_from_all_foreign_key(ObISQLClient& sql_client, const uint64_t tenant_id,
    const int64_t new_schema_version, const ObForeignKeyInfo& foreign_key_info)
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
          new_schema_version,
          is_deleted,
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
      // Checking affected rows here is unnecessary because schema may be delay deleted
      // or record maybe be deleted before in the same trans.
    }
  }
  return ret;
}

int ObTableSqlService::delete_from_all_foreign_key_column(ObISQLClient& sql_client, const uint64_t tenant_id,
    const uint64_t foreign_key_id, const uint64_t child_column_id, const uint64_t parent_column_id,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t is_deleted = 1;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  // insert into __all_foreign_key_history
  if (OB_FAIL(sql.assign_fmt(
          "INSERT INTO %s(tenant_id,foreign_key_id,child_column_id,parent_column_id,schema_version,is_deleted)"
          " VALUES(%lu,%lu,%lu,%lu,%ld,%ld)",
          OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TNAME,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_id),
          child_column_id,
          parent_column_id,
          new_schema_version,
          is_deleted))) {
    LOG_WARN("assign insert into __all_foreign_key_column_history fail", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql fail", K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no row has inserted", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND foreign_key_id = %lu AND child_column_id = "
                               "%lu AND parent_column_id = %lu",
            OB_ALL_FOREIGN_KEY_COLUMN_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_id),
            child_column_id,
            parent_column_id))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      // Checking affected rows here is unnecessary because schema may be delay deleted
      // or record maybe be deleted before in the same trans.
    }
  }
  return ret;
}

int ObTableSqlService::delete_foreign_key(
    common::ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObForeignKeyInfo>& foreign_key_infos = table_schema.get_foreign_key_infos();
  uint64_t tenant_id = table_schema.get_tenant_id();
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
    const ObForeignKeyInfo& foreign_key_info = foreign_key_infos.at(i);
    uint64_t foreign_key_id = foreign_key_info.foreign_key_id_;
    if (OB_FAIL(delete_from_all_foreign_key(sql_client, tenant_id, new_schema_version, foreign_key_info))) {
      LOG_WARN("failed to delete __all_foreign_key_history", K(table_schema), K(ret));
    } else if (OB_UNLIKELY(foreign_key_info.child_column_ids_.count() != foreign_key_info.parent_column_ids_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("child column num and parent column num should be equal",
          K(ret),
          K(foreign_key_info.child_column_ids_.count()),
          K(foreign_key_info.parent_column_ids_.count()));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.child_column_ids_.count(); j++) {
        // delete from __all_foreign_key_column_history
        uint64_t child_column_id = foreign_key_info.child_column_ids_.at(j);
        uint64_t parent_column_id = foreign_key_info.parent_column_ids_.at(j);
        if (OB_FAIL(delete_from_all_foreign_key_column(
                sql_client, tenant_id, foreign_key_id, child_column_id, parent_column_id, new_schema_version))) {
          LOG_WARN("failed to delete __all_foreign_key_column_history", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableSqlService::update_check_constraint_state(
    common::ObISQLClient& sql_client, const ObTableSchema& table, const ObConstraint& cst)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;

  if (is_inner_table(table.get_table_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys table doesn't have check constraints", K(ret), K(table));
  } else {
    dml.reset();
    if (!cst.get_is_modify_rely_flag()         // not modify rely attribute
        && !cst.get_is_modify_enable_flag()    // nor enable attribute
        && !cst.get_is_modify_validate_flag()  // nor validate attribute
        && !cst.get_is_modify_check_expr()) {  // nor check constraint expr
      // do nothing
    } else if (OB_FAIL(
                   dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
               OB_FAIL(dml.add_pk_column(
                   "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, cst.get_table_id()))) ||
               OB_FAIL(dml.add_pk_column("constraint_id", cst.get_constraint_id())) ||
               OB_FAIL(dml.add_column("schema_version", cst.get_schema_version())) ||
               OB_FAIL(dml.add_column("check_expr", ObHexEscapeSqlStr(cst.get_check_expr()))) ||
               OB_FAIL(dml.add_gmt_modified())) {
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
      if (OB_FAIL(exec_update(sql_client, table_id, OB_ALL_CONSTRAINT_TNAME, dml, affected_rows))) {
        LOG_WARN("exec update failed", K(ret));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(affected_rows), K(ret));
      } else if (OB_FAIL(add_single_constraint(sql_client, cst, true, false))) {
        SHARE_SCHEMA_LOG(WARN, "add_single_constraint failed", K(ret), K(cst));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = table.get_tenant_id();
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

int ObTableSqlService::update_foreign_key(common::ObISQLClient& sql_client, const ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  const ObIArray<ObForeignKeyInfo>& foreign_key_infos = table.get_foreign_key_infos();
  if (is_inner_table(table.get_table_id()) && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate sys table", K(ret), K(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); ++i) {
    const ObForeignKeyInfo& foreign_key_info = foreign_key_infos.at(i);
    if (!foreign_key_info.is_modify_fk_state_) {
      // skip
    } else {
      dml.reset();
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
          OB_FAIL(dml.add_pk_column("foreign_key_id",
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, foreign_key_info.foreign_key_id_))) ||
          OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("failed to add columns", K(ret));
      } else if (foreign_key_info.is_modify_enable_flag_ &&
                 OB_FAIL(dml.add_column("enable_flag", foreign_key_info.enable_flag_))) {
        LOG_WARN("failed to add enable_flag column", K(ret));
      } else if (foreign_key_info.is_modify_validate_flag_ &&
                 OB_FAIL(dml.add_column("validate_flag", foreign_key_info.validate_flag_))) {
        LOG_WARN("failed to add validate_flag column", K(ret));
      } else if (foreign_key_info.is_modify_rely_flag_ &&
                 OB_FAIL(dml.add_column("rely_flag", foreign_key_info.rely_flag_))) {
        LOG_WARN("failed to add rely_flag column", K(ret));
      } else {
        int64_t affected_rows = 0;
        uint64_t table_id = foreign_key_info.table_id_;
        if (OB_FAIL(exec_update(sql_client, table_id, OB_ALL_FOREIGN_KEY_TNAME, dml, affected_rows))) {
          LOG_WARN("exec update failed", K(ret));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(affected_rows), K(ret));
        }
        if (OB_SUCC(ret)) {
          dml.reset();
          if (OB_FAIL(gen_foreign_key_dml(exec_tenant_id, tenant_id, foreign_key_info, dml))) {
            LOG_WARN("failed to gen foreign key dml", K(ret));
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
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(update_data_table_schema_version(sql_client, foreign_key_info.parent_table_id_))) {
            LOG_WARN("failed to update parent table schema version", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableSqlService::add_foreign_key(ObISQLClient& sql_client, const ObTableSchema& table, const bool only_history)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  const ObIArray<ObForeignKeyInfo>& foreign_key_infos = table.get_foreign_key_infos();
  if (is_inner_table(table.get_table_id())) {
    // To avoid cyclic dependence
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("should not be here", K(ret), K(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
    const ObForeignKeyInfo& foreign_key_info = foreign_key_infos.at(i);
    const int64_t is_deleted = only_history ? 1 : 0;
    if (foreign_key_info.is_modify_fk_state_) {
      // skip
    } else if (OB_FAIL(update_data_table_schema_version(sql_client, foreign_key_info.parent_table_id_))) {
      LOG_WARN("failed to update parent table schema version", K(ret));
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
        if (OB_UNLIKELY(foreign_key_info.child_column_ids_.count() != foreign_key_info.parent_column_ids_.count())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("child column num and parent column num should be equal",
              K(ret),
              K(foreign_key_info.child_column_ids_.count()),
              K(foreign_key_info.parent_column_ids_.count()));
        } else {
          uint64_t foreign_key_id = foreign_key_info.foreign_key_id_;
          for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.child_column_ids_.count(); j++) {
            uint64_t child_column_id = foreign_key_info.child_column_ids_.at(j);
            uint64_t parent_column_id = foreign_key_info.parent_column_ids_.at(j);
            if (OB_FAIL(gen_foreign_key_column_dml(
                    exec_tenant_id, tenant_id, foreign_key_id, child_column_id, parent_column_id, j + 1, dml))) {
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
                if (OB_FAIL(dml.add_column("schema_version", table.get_schema_version()))) {
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
int ObTableSqlService::drop_foreign_key(const int64_t new_schema_version, ObISQLClient& sql_client,
    const ObTableSchema& table_schema, const ObString& foreign_key_name)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObForeignKeyInfo>& foreign_key_infos = table_schema.get_foreign_key_infos();
  const ObForeignKeyInfo* foreign_key_info = NULL;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
    if (0 == foreign_key_name.case_compare(foreign_key_infos.at(i).foreign_key_name_) &&
        table_schema.get_table_id() == foreign_key_infos.at(i).child_table_id_) {
      foreign_key_info = &foreign_key_infos.at(i);
      break;
    }
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
    } else if (OB_UNLIKELY(
                   foreign_key_info->child_column_ids_.count() != foreign_key_info->parent_column_ids_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("child column num and parent column num should be equal",
          K(ret),
          K(foreign_key_info->child_column_ids_.count()),
          K(foreign_key_info->parent_column_ids_.count()));
    } else {
      uint64_t foreign_key_id = foreign_key_info->foreign_key_id_;
      for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info->child_column_ids_.count(); j++) {
        uint64_t child_column_id = foreign_key_info->child_column_ids_.at(j);
        uint64_t parent_column_id = foreign_key_info->parent_column_ids_.at(j);
        if (OB_FAIL(gen_foreign_key_column_dml(
                exec_tenant_id, tenant_id, foreign_key_id, child_column_id, parent_column_id, j + 1, dml))) {
          LOG_WARN("failed to gen foreign key column dml", K(ret));
        } else if (OB_FAIL(exec.exec_delete(OB_ALL_FOREIGN_KEY_COLUMN_TNAME, dml, affected_rows))) {
          LOG_WARN("failed to delete foreign key column", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
        } else if (OB_FAIL(dml.add_column("schema_version", new_schema_version))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(dml.add_column("is_deleted", 1))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(exec.exec_insert(OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_data_table_schema_version(sql_client, foreign_key_info->child_table_id_))) {
        LOG_WARN("failed to update child table schema version", K(ret));
      } else if (OB_FAIL(update_data_table_schema_version(sql_client, foreign_key_info->parent_table_id_))) {
        LOG_WARN("failed to update parent table schema version", K(ret));
      }
    }
  } else if (OB_SUCC(ret) && OB_ISNULL(foreign_key_info)) {
    bool is_oracle_mode = false;
    if (OB_FAIL(
            ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(table_schema.get_tenant_id(), is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode", K(ret));
    } else if (is_oracle_mode) {
      ret = OB_ERR_NONEXISTENT_CONSTRAINT;
      LOG_WARN("Cannot drop foreign key constraint  - nonexistent constraint",
          K(ret),
          K(foreign_key_name),
          K(table_schema.get_table_name_str()));
    } else {
      ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
      LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, foreign_key_name.length(), foreign_key_name.ptr());
      LOG_WARN("Cannot drop foreign key constraint  - nonexistent constraint",
          K(ret),
          K(foreign_key_name),
          K(table_schema.get_table_name_str()));
    }
  }
  return ret;
}

int ObTableSqlService::check_table_options(const ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2270 && !table.is_sub_part_template()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("nontemplate not supported in less CLUSTER_VERSION_2270", K(ret), "table_id", table.get_table_id());
  }

  if (OB_SUCC(ret)) {
    if (table.is_auto_partitioned_table() && OB_INVALID_ID != table.get_tablegroup_id()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("auto part table in tablegroup not allowed", K(ret), K(table));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set atuo part table in tablegroup");
    }
  }
  if (OB_SUCC(ret)) {
    if (table.get_dop() > 1 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2270) {
      // defendence, new feature can take effect after upgradation is done.
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("DDL operation where dop is greater than 1 during cluster upgrade to 310 is not supported",
          K(ret),
          K(GET_MIN_CLUSTER_VERSION()));
      LOG_USER_ERROR(
          OB_NOT_SUPPORTED, "DDL operation where dop is greater than 1 during cluster upgrade to 310 is not supported");
    }
  }
  return ret;
}

int ObTableSqlService::update_table_schema_version(ObISQLClient& sql_client, const ObTableSchema& table_schema,
    share::schema::ObSchemaOperationType operation_type, const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = table_schema.get_table_id();
  uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table_schema.get_tenant_id());
  ObDMLSqlSplicer dml;
  if (!is_inner_table(table_id) || OB_SYS_TENANT_ID != exec_tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("normal tenant can not operate table schema", KR(ret), K(table_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id))) ||
             OB_FAIL(dml.add_column("schema_version", table_schema.get_schema_version())) ||
             OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    const char* table_name = NULL;
    if (OB_FAIL(ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(exec_update(sql_client, table_id, table_name, dml, affected_rows))) {
      LOG_WARN("exec update failed", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(ret), K(affected_rows));
    }
  }
  // add to __all_table_history table
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_table(sql_client, table_schema, only_history))) {
      LOG_WARN("add_table failed", K(table_schema), K(only_history), K(ret));
    }
  }
  // log operation
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    if (NULL != ddl_stmt_str) {
      opt.ddl_stmt_str_ = *ddl_stmt_str;
    }
    opt.tenant_id_ = table_schema.get_tenant_id();
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

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
