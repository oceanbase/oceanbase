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
#include "ob_tablegroup_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_partition_sql_helper.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase {
using namespace common;
namespace rootserver {
class ObRootService;
}
namespace share {
namespace schema {

int ObTablegroupSqlService::insert_tablegroup(const ObTablegroupSchema& tablegroup_schema,
    common::ObISQLClient& sql_client, const ObString* ddl_stmt_str /*=NULL*/)
{
  int ret = OB_SUCCESS;
  if (!tablegroup_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "tablegroup_schema is invalid, ", K(ret));
  } else {
    // add tablegroup/tablegroup_history
    const bool only_history = false;
    if (OB_FAIL(add_tablegroup(sql_client, tablegroup_schema, only_history))) {
      LOG_WARN("fail to add tablegroup", K(ret));
    }
    // tablegroup_id is encoded to distinguish tablegroups created before and after ver 2.0.
    // For tablegroup created before ver 2.0, it doesn't contain any schema of partitions.
    if (OB_FAIL(ret)) {
      // skip
    } else if (is_new_tablegroup_id(tablegroup_schema.get_tablegroup_id())) {
      // add partition info
      const bool is_tablegroup_def = true;
      const ObPartitionSchema* tg_schema = &tablegroup_schema;
      ObAddPartInfoHelper part_helper(tg_schema, sql_client, is_tablegroup_def);
      if (OB_FAIL(part_helper.add_partition_info())) {
        LOG_WARN("add partition info failed", K(ret));
      } else if (OB_FAIL(part_helper.add_partition_info_for_gc())) {
        LOG_WARN("failed to add partition", K(ret), K(tablegroup_schema));
      }
    }

    // log operations
    if (OB_SUCC(ret)) {
      ObSchemaOperation create_tg_op;
      create_tg_op.tenant_id_ = tablegroup_schema.get_tenant_id();
      create_tg_op.database_id_ = 0;
      create_tg_op.tablegroup_id_ = tablegroup_schema.get_tablegroup_id();
      create_tg_op.table_id_ = 0;
      create_tg_op.op_type_ = OB_DDL_ADD_TABLEGROUP;
      create_tg_op.schema_version_ = tablegroup_schema.get_schema_version();
      create_tg_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(create_tg_op, sql_client))) {
        LOG_WARN("log create tablegroup ddl operation failed, ", K(ret));
      }
    }
  }

  return ret;
}

int ObTablegroupSqlService::update_tablegroup(
    ObTablegroupSchema& new_schema, common::ObISQLClient& sql_client, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(check_tablegroup_options(new_schema))) {
    LOG_WARN("fail to check tablegroup's options", K(ret), K(new_schema));
  } else if (OB_FAIL(dml.add_pk_column(
                 "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, new_schema.get_tenant_id()))) ||
             OB_FAIL(dml.add_pk_column("tablegroup_id",
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, new_schema.get_tablegroup_id()))) ||
             OB_FAIL(dml.add_column("comment", new_schema.get_comment())) ||
             OB_FAIL(dml.add_column("tablegroup_name", ObHexEscapeSqlStr(new_schema.get_tablegroup_name_str()))) ||
             OB_FAIL(dml.add_column("primary_zone", new_schema.get_primary_zone())) ||
             OB_FAIL(dml.add_column("locality", new_schema.get_locality())) ||
             OB_FAIL(dml.add_column("previous_locality", new_schema.get_previous_locality())) ||
             OB_FAIL(dml.add_column("binding", new_schema.get_binding()))) {
    LOG_WARN("fail to add pk column", K(ret), K(new_schema));
  }
  int64_t affected_rows = 0;
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(exec.exec_update(OB_ALL_TABLEGROUP_TNAME, dml, affected_rows))) {
    LOG_WARN("fail to exec update", K(ret));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(affected_rows), K(ret));
  }
  // add tablegroup_history
  const bool only_history = true;
  if (OB_FAIL(add_tablegroup(sql_client, new_schema, only_history))) {
    LOG_WARN("fail to add tablegroup history", K(ret));
  }

  // insert log
  if (OB_FAIL(ret)) {
  } else {
    ObSchemaOperation create_tg_op;
    create_tg_op.tenant_id_ = new_schema.get_tenant_id();
    create_tg_op.database_id_ = 0;
    create_tg_op.tablegroup_id_ = new_schema.get_tablegroup_id();
    create_tg_op.table_id_ = 0;
    create_tg_op.op_type_ = OB_DDL_ALTER_TABLEGROUP;
    create_tg_op.schema_version_ = new_schema.get_schema_version();
    create_tg_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();  // TODO
    if (OB_FAIL(log_operation(create_tg_op, sql_client))) {
      LOG_WARN("log create tablegroup ddl operation failed, ", K(ret));
    }
  }
  return ret;
}

int ObTablegroupSqlService::drop_tablegroup_for_inspection(
    const ObTablegroupSchema& tablegroup_schema, const int64_t new_schema_version, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  ObTablegroupSchema new_tablegroup_schema;
  // mark delete in __all_tablegroup_history
  const int64_t is_deleted = 1;
  if (OB_FAIL(new_tablegroup_schema.assign(tablegroup_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else {
    new_tablegroup_schema.set_schema_version(new_schema_version);
  }
  if (OB_SUCC(ret)) {
    if (FAILEDx(sql.assign_fmt("INSERT INTO %s(tenant_id,tablegroup_id,schema_version, is_deleted) "
                               "VALUES(%lu,%lu,%ld, %ld)",
            OB_ALL_TABLEGROUP_HISTORY_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_id),
            new_schema_version,
            is_deleted))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows is expected to one", K(affected_rows), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // insert delete record in __all_part_info_history, __all_part_history,
    //__all_subpart_history, __all_def_subpart_history
    const bool is_tablegroup_def = true;
    const ObPartitionSchema* tg_schema = &new_tablegroup_schema;
    ObDropPartInfoHelper part_helper(tg_schema, sql_client, is_tablegroup_def);
    if (OB_FAIL(part_helper.delete_partition_info())) {
      LOG_WARN("delete partition info failed", K(ret));
    } else if (OB_FAIL(part_helper.delete_dropped_partition_info())) {
      LOG_WARN("delete delay_deleted partition info failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation delete_tg_op;
    delete_tg_op.tenant_id_ = extract_tenant_id(tablegroup_id);
    delete_tg_op.database_id_ = 0;
    delete_tg_op.tablegroup_id_ = tablegroup_id;
    delete_tg_op.table_id_ = 0;
    delete_tg_op.schema_version_ = new_schema_version;
    delete_tg_op.op_type_ = OB_DDL_DEL_TABLEGROUP;
    if (OB_FAIL(log_operation(delete_tg_op, sql_client))) {
      LOG_WARN("log delete tablegroup operation failed", K(ret), K(delete_tg_op));
    }
  }

  return ret;
}

int ObTablegroupSqlService::delete_tablegroup(const ObTablegroupSchema& tablegroup_schema,
    const int64_t new_schema_version, common::ObISQLClient& sql_client, const ObString* ddl_stmt_str /*=NULL*/,
    bool is_delay_delete /*=false*/, const common::ObString* delay_deleted_name /*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  ObTablegroupSchema new_tablegroup_schema;

  // delete from __all_tablegroup
  if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(new_tablegroup_schema.assign(tablegroup_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else if (FALSE_IT(new_tablegroup_schema.set_schema_version(new_schema_version))) {
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND tablegroup_id = %lu",
                 OB_ALL_TABLEGROUP_TNAME,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_id)))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows is expected to one", K(affected_rows), K(ret));
  }
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);

  if (OB_SUCC(ret) && !is_delay_delete) {
    // mark delete in __all_tablegroup_history
    const int64_t is_deleted = 1;
    if (OB_SUCC(ret)) {
      if (FAILEDx(sql.assign_fmt("INSERT INTO %s(tenant_id,tablegroup_id,schema_version, is_deleted) "
                                 "VALUES(%lu,%lu,%ld, %ld)",
              OB_ALL_TABLEGROUP_HISTORY_TNAME,
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_id),
              new_tablegroup_schema.get_schema_version(),
              is_deleted))) {
        LOG_WARN("assign_fmt failed", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is expected to one", K(affected_rows), K(ret));
      }
    }
  } else if (OB_SUCC(ret) && is_delay_delete) {
    // delay delete
    ObString delay_deleted_table_name;
    if (OB_ISNULL(delay_deleted_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delay_deleted_name is null", K(ret));
    } else if (OB_FAIL(new_tablegroup_schema.set_tablegroup_name(*delay_deleted_name))) {
      LOG_WARN("set tablegroup name failed", K(ret), K(*delay_deleted_name));
    } else {
      new_tablegroup_schema.set_drop_schema_version(new_schema_version);
      if (OB_FAIL(check_tablegroup_options(new_tablegroup_schema))) {
        LOG_WARN("fail to check tablegroup's options", K(ret), K(new_tablegroup_schema));
      } else if (OB_FAIL(add_tablegroup(sql_client, new_tablegroup_schema, true))) {
        LOG_WARN("fail to add tablegroup history", K(ret));
      }
    }
  }

  // tablegroup_id is encoded to distinguish tablegroups created before and after ver 2.0.
  // For tablegroup created before ver 2.0, it doesn't contain any schema of partitions.
  if (OB_FAIL(ret)) {
  } else if (is_new_tablegroup_id(new_tablegroup_schema.get_tablegroup_id())) {
    // drop data in __all_part_info, __all_part, __all_subpart, __all_def_subpart,
    int64_t affected_rows = 0;
    bool is_two_level = PARTITION_LEVEL_TWO == new_tablegroup_schema.get_part_level() ? true : false;
    const char* tname[] = {OB_ALL_PART_INFO_TNAME, OB_ALL_PART_TNAME, OB_ALL_SUB_PART_TNAME, OB_ALL_DEF_SUB_PART_TNAME};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
      sql.reset();
      if (!is_two_level &&
          (0 == STRCMP(tname[i], OB_ALL_SUB_PART_TNAME) || 0 == STRCMP(tname[i], OB_ALL_DEF_SUB_PART_TNAME))) {
        continue;
      } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu",
                     tname[i],
                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                     ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_id)))) {
        LOG_WARN("append_fmt failed", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
      } else {
      }
    }
    // insert delete record in __all_part_info_history, __all_part_history,
    //__all_subpart_history, __all_def_subpart_history
    const bool is_tablegroup_def = true;
    const ObPartitionSchema* tg_schema = &new_tablegroup_schema;
    ObDropPartInfoHelper part_helper(tg_schema, sql_client, is_tablegroup_def);
    if (OB_FAIL(ret)) {
    } else if (is_delay_delete) {
      // skip
    } else if (OB_FAIL(part_helper.delete_partition_info())) {
      LOG_WARN("delete partition info failed", K(ret));
    } else if (OB_FAIL(part_helper.delete_dropped_partition_info())) {
      LOG_WARN("delete delay_deleted partition info failed", K(ret));
    }
  }

  // log operations
  if (OB_SUCC(ret)) {
    ObSchemaOperation delete_tg_op;
    delete_tg_op.tenant_id_ = extract_tenant_id(tablegroup_id);
    delete_tg_op.database_id_ = 0;
    delete_tg_op.tablegroup_id_ = tablegroup_id;
    delete_tg_op.table_id_ = 0;
    delete_tg_op.schema_version_ = new_schema_version;
    delete_tg_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (is_delay_delete) {
      delete_tg_op.op_type_ = OB_DDL_DELAY_DELETE_TABLEGROUP;
    } else {
      delete_tg_op.op_type_ = OB_DDL_DEL_TABLEGROUP;
    }
    if (OB_FAIL(log_operation(delete_tg_op, sql_client))) {
      LOG_WARN("log delete tablegroup ddl operation failed", K(delete_tg_op), K(ret));
    }
  }

  return ret;
}

int ObTablegroupSqlService::add_inc_part_info(ObISQLClient& sql_client, const ObTablegroupSchema& ori_tablegroup,
    const ObTablegroupSchema& inc_tablegroup, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = ori_tablegroup.get_tablegroup_id();
  if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(schema_version));
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablegroup before ver2.0 add partition not supported", K(ret), K(tablegroup_id));
  } else if (!ori_tablegroup.is_range_part() && !ori_tablegroup.is_list_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support range/list", K(ret), K(tablegroup_id));
  } else {
    const bool is_tablegroup_def = true;
    const ObPartitionSchema* ori_tablegroup_schema = &ori_tablegroup;
    const ObPartitionSchema* inc_tablegroup_schema = &inc_tablegroup;
    ObAddIncPartHelper part_helper(
        ori_tablegroup_schema, inc_tablegroup_schema, schema_version, sql_client, is_tablegroup_def);
    if (OB_FAIL(part_helper.add_partition_info(false))) {
      LOG_WARN("add partition info failed", K(ret), K(ori_tablegroup_schema), K(inc_tablegroup_schema));
    } else if (OB_FAIL(part_helper.add_partition_info_for_gc())) {
      LOG_WARN("failed to add partition info for gc", K(ret), K(inc_tablegroup));
    }
  }
  return ret;
}

// used by force_drop_schema()
int ObTablegroupSqlService::drop_part_info_for_inspection(ObISQLClient& sql_client,
    const ObTablegroupSchema& tablegroup_schema, const ObTablegroupSchema& inc_tablegroup, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const bool is_tablegroup_def = true;
  const ObPartitionSchema* tablegroup_schema_ptr = &tablegroup_schema;
  const ObPartitionSchema* inc_tablegroup_ptr = &inc_tablegroup;

  ObDropIncPartHelper drop_part_helper(
      tablegroup_schema_ptr, inc_tablegroup_ptr, schema_version, sql_client, is_tablegroup_def);
  if (OB_FAIL(drop_part_helper.drop_partition_info())) {
    LOG_WARN(
        "drop increment partition info failed", K(tablegroup_schema), K(inc_tablegroup), K(schema_version), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tablegroup_schema.get_tenant_id();
    opt.database_id_ = 0;
    opt.tablegroup_id_ = tablegroup_schema.get_tablegroup_id();
    opt.table_id_ = 0;
    opt.op_type_ = OB_DDL_ALTER_TABLEGROUP_PARTITION;
    opt.schema_version_ = schema_version;
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTablegroupSqlService::drop_inc_part_info(ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup_schema,
    const ObTablegroupSchema& inc_tablegroup, const int64_t schema_version, bool is_delay_delete)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_FAIL(ret)) {
  } else if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(schema_version));
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablegroup before ver2.0 add partition not supported", K(ret), K(tablegroup_id));
  } else if (!tablegroup_schema.is_range_part() && !tablegroup_schema.is_list_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support range/list", K(ret), K(tablegroup_id));
  } else {
    int64_t tenant_id = tablegroup_schema.get_tenant_id();
    ObSqlString sql;
    const int64_t inc_part_num = inc_tablegroup.get_part_option().get_part_num();
    ObPartition** part_array = inc_tablegroup.get_part_array();

    // delete from __all_part
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND table_id=%lu AND (0 = 1",
            OB_ALL_PART_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_schema.get_table_id())))) {
      LOG_WARN("append_fmt failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; i++) {
      if (OB_FAIL(sql.append_fmt(" OR part_id = %lu", part_array[i]->get_part_id()))) {
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

    // insert into __all_part_history
    if (OB_SUCC(ret)) {
      const bool is_tablegroup_def = true;
      const ObPartitionSchema* tablegroup_schema_ptr = &tablegroup_schema;
      const ObPartitionSchema* inc_tablegroup_ptr = &inc_tablegroup;
      if (is_delay_delete) {
        ObAddIncPartHelper add_part_helper(
            tablegroup_schema_ptr, inc_tablegroup_ptr, schema_version, sql_client, is_tablegroup_def);
        if (OB_FAIL(ret)) {
        } else if (add_part_helper.add_partition_info(true)) {
          LOG_WARN("drop increment partition info failed",
              K(ret),
              K(tablegroup_schema),
              KPC(inc_tablegroup_ptr),
              K(schema_version));
        }
      } else {
        ObDropIncPartHelper drop_part_helper(
            tablegroup_schema_ptr, inc_tablegroup_ptr, schema_version, sql_client, is_tablegroup_def);
        if (OB_FAIL(drop_part_helper.drop_partition_info())) {
          LOG_WARN("drop increment partition info failed",
              K(tablegroup_schema),
              K(inc_tablegroup),
              K(schema_version),
              K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTablegroupSqlService::update_partition_option(ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup,
    const ObSchemaOperationType opt_type, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = tablegroup.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t tablegroup_id = tablegroup.get_tablegroup_id();

  const ObPartitionOption& part_option = tablegroup.get_part_option();
  const ObPartitionOption& sub_part_option = tablegroup.get_sub_part_option();
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  if (OB_INVALID_ID == tablegroup_id || !is_tablegroup_id(tablegroup_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablegroup before ver2.0 add partition not supported", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column(
                 "tablegroup_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_id))) ||
             OB_FAIL(dml.add_column("part_level", tablegroup.get_part_level())) ||
             OB_FAIL(dml.add_column("part_func_type", part_option.get_part_func_type())) ||
             OB_FAIL(dml.add_column("part_func_expr_num", tablegroup.get_part_func_expr_num())) ||
             OB_FAIL(dml.add_column("part_num", part_option.get_part_num())) ||
             OB_FAIL(dml.add_column("sub_part_func_type", sub_part_option.get_part_func_type())) ||
             OB_FAIL(dml.add_column("sub_part_func_expr_num", tablegroup.get_sub_part_func_expr_num())) ||
             OB_FAIL(dml.add_column("sub_part_num", sub_part_option.get_part_num())) ||
             OB_FAIL(dml.add_column("max_used_part_id", part_option.get_max_used_part_id())) ||
             OB_FAIL(dml.add_column("schema_version", tablegroup.get_schema_version())) ||
             OB_FAIL(dml.add_column("partition_status", tablegroup.get_partition_status())) ||
             OB_FAIL(dml.add_column("partition_schema_version", tablegroup.get_partition_schema_version())) ||
             OB_FAIL(dml.add_column("binding", tablegroup.get_binding())) || OB_FAIL(dml.add_gmt_create()) ||
             OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_update(OB_ALL_TABLEGROUP_TNAME, dml, affected_rows))) {
    LOG_WARN("exec update failed", K(ret));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(affected_rows), K(ret));
  }

  // add tablegroup_history
  const bool only_history = true;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(add_tablegroup(sql_client, tablegroup, only_history))) {
    LOG_WARN("fail to add tablegroup history", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tablegroup.get_tenant_id();
    opt.database_id_ = 0;
    opt.tablegroup_id_ = tablegroup.get_tablegroup_id();
    opt.table_id_ = 0;
    opt.op_type_ = opt_type;
    opt.schema_version_ = tablegroup.get_schema_version();
    opt.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("log operation failed", K(opt), K(ret));
    }
  }
  return ret;
}

int ObTablegroupSqlService::add_tablegroup(
    ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup, const bool only_history)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tablegroup.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  ObDMLSqlSplicer dml;
  if (OB_FAIL(gen_tablegroup_dml(exec_tenant_id, tablegroup, dml))) {
    LOG_WARN("gen tablegroup dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      if (OB_FAIL(exec.exec_insert(OB_ALL_TABLEGROUP_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_TABLEGROUP_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObTablegroupSqlService::gen_tablegroup_dml(
    const uint64_t exec_tenant_id, const ObTablegroupSchema& tablegroup_schema, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (!tablegroup_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_schema is invalid, ", K(ret), K(tablegroup_schema));
  } else if (OB_FAIL(check_tablegroup_options(tablegroup_schema))) {
    LOG_WARN("fail to check tablegroup's options", K(ret), K(tablegroup_schema));
  } else {
    const ObPartitionOption& part_option = tablegroup_schema.get_part_option();
    const ObPartitionOption& sub_part_option = tablegroup_schema.get_sub_part_option();

    if (OB_FAIL(dml.add_pk_column(
            "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tablegroup_schema.get_tenant_id()))) ||
        OB_FAIL(dml.add_pk_column("tablegroup_id",
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_schema.get_tablegroup_id()))) ||
        OB_FAIL(dml.add_column("tablegroup_name", ObHexEscapeSqlStr(tablegroup_schema.get_tablegroup_name_str()))) ||
        OB_FAIL(dml.add_column("comment", tablegroup_schema.get_comment())) ||
        OB_FAIL(dml.add_column("primary_zone", tablegroup_schema.get_primary_zone())) ||
        OB_FAIL(dml.add_column("locality", tablegroup_schema.get_locality())) ||
        OB_FAIL(dml.add_column("part_level", tablegroup_schema.get_part_level())) ||
        OB_FAIL(dml.add_column("part_func_type", part_option.get_part_func_type())) ||
        OB_FAIL(dml.add_column("part_func_expr_num", tablegroup_schema.get_part_func_expr_num())) ||
        OB_FAIL(dml.add_column("part_num", part_option.get_part_num())) ||
        OB_FAIL(dml.add_column("sub_part_func_type", sub_part_option.get_part_func_type())) ||
        OB_FAIL(dml.add_column("sub_part_func_expr_num", tablegroup_schema.get_sub_part_func_expr_num())) ||
        OB_FAIL(dml.add_column("sub_part_num", sub_part_option.get_part_num())) ||
        OB_FAIL(dml.add_column("max_used_part_id", part_option.get_max_used_part_id())) ||
        OB_FAIL(dml.add_column("previous_locality", tablegroup_schema.get_previous_locality())) ||
        OB_FAIL(dml.add_column("partition_status", tablegroup_schema.get_partition_status())) ||
        OB_FAIL(dml.add_column("partition_schema_version", tablegroup_schema.get_partition_schema_version())) ||
        OB_FAIL(dml.add_column("schema_version", tablegroup_schema.get_schema_version())) ||
        OB_FAIL(dml.add_column("binding", tablegroup_schema.get_binding())) ||
        OB_FAIL(dml.add_column("drop_schema_version", tablegroup_schema.get_drop_schema_version())) ||
        OB_FAIL(dml.add_column("is_sub_part_template", tablegroup_schema.is_sub_part_template())) ||
        OB_FAIL(dml.add_gmt_create()) || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }
  }
  return ret;
}

int ObTablegroupSqlService::update_tablegroup_schema_version(
    ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = tablegroup.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t tablegroup_id = tablegroup.get_tablegroup_id();
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  if (OB_INVALID_ID == tablegroup_id || !is_tablegroup_id(tablegroup_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablegroup before ver2.0 add partition not supported", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column(
                 "tablegroup_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_id))) ||
             OB_FAIL(dml.add_column("schema_version", tablegroup.get_schema_version())) ||
             OB_FAIL(dml.add_gmt_create()) || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_update(OB_ALL_TABLEGROUP_TNAME, dml, affected_rows))) {
    LOG_WARN("exec update failed", K(ret));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(affected_rows), K(ret));
  }

  // add tablegroup_history
  const bool only_history = true;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(add_tablegroup(sql_client, tablegroup, only_history))) {
    LOG_WARN("fail to add tablegroup history", K(ret));
  }
  return ret;
}

int ObTablegroupSqlService::update_partition_option_without_log(
    ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = tablegroup.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t tablegroup_id = tablegroup.get_tablegroup_id();

  const ObPartitionOption& part_option = tablegroup.get_part_option();
  const ObPartitionOption& sub_part_option = tablegroup.get_sub_part_option();
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  int64_t affected_rows = 0;
  if (OB_INVALID_ID == tablegroup_id || !is_tablegroup_id(tablegroup_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablegroup before ver2.0 add partition not supported", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column(
                 "tablegroup_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_id))) ||
             OB_FAIL(dml.add_column("part_level", tablegroup.get_part_level())) ||
             OB_FAIL(dml.add_column("part_func_type", part_option.get_part_func_type())) ||
             OB_FAIL(dml.add_column("part_func_expr_num", tablegroup.get_part_func_expr_num())) ||
             OB_FAIL(dml.add_column("part_num", part_option.get_part_num())) ||
             OB_FAIL(dml.add_column("sub_part_func_type", sub_part_option.get_part_func_type())) ||
             OB_FAIL(dml.add_column("sub_part_func_expr_num", tablegroup.get_sub_part_func_expr_num())) ||
             OB_FAIL(dml.add_column("sub_part_num", sub_part_option.get_part_num())) ||
             OB_FAIL(dml.add_column("max_used_part_id", part_option.get_max_used_part_id())) ||
             OB_FAIL(dml.add_column("schema_version", tablegroup.get_schema_version())) ||
             OB_FAIL(dml.add_column("partition_status", tablegroup.get_partition_status())) ||
             OB_FAIL(dml.add_column("partition_schema_version", tablegroup.get_partition_schema_version())) ||
             OB_FAIL(dml.add_gmt_create()) || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_update(OB_ALL_TABLEGROUP_TNAME, dml, affected_rows))) {
    LOG_WARN("exec update failed", K(ret));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(affected_rows), K(ret));
  }

  // add tablegroup_history
  const bool only_history = true;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(add_tablegroup(sql_client, tablegroup, only_history))) {
    LOG_WARN("fail to add tablegroup history", K(ret));
  }
  return ret;
}

int ObTablegroupSqlService::update_max_used_part_id(common::ObISQLClient& client, const int64_t schema_version,
    const ObTablegroupSchema& orig_schema, const ObTablegroupSchema& alter_schema)
{
  int ret = OB_SUCCESS;
  int64_t tablegroup_id = orig_schema.get_tablegroup_id();
  int64_t tenant_id = orig_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t max_used_part_id =
      orig_schema.get_part_option().get_max_used_part_id() + alter_schema.get_part_option().get_part_num();
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt(
          "UPDATE %s SET SCHEMA_VERSION = %ld, MAX_USED_PART_ID = %ld WHERE TENANT_ID = %ld AND TABLEGROUP_ID = %ld",
          OB_ALL_TABLEGROUP_TNAME,
          schema_version,
          max_used_part_id,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablegroup_id)))) {
    LOG_WARN("fail to update all_tablegroup", K(ret), K(tenant_id), K(tablegroup_id), K(max_used_part_id));
  } else if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write", K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows is expected to one", K(affected_rows), K(ret));
  }
  ObTablegroupSchema new_tablegroup = orig_schema;
  new_tablegroup.get_part_option().set_max_used_part_id(max_used_part_id);
  new_tablegroup.set_schema_version(schema_version);
  const bool only_history = true;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_tablegroup(client, new_tablegroup, only_history))) {
    LOG_WARN("fail to add tablegroup history", K(ret));
  }
  return ret;
}
int ObTablegroupSqlService::modify_dest_partition(
    common::ObISQLClient& client, const ObTablegroupSchema& tablegroup_schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t table_id = tablegroup_schema.get_table_id();
  int64_t tenant_id = tablegroup_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  if (!tablegroup_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tablegroup_schema));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET SOURCE_PARTITION_ID = -1, SCHEMA_VERSION = %ld WHERE TENANT_ID = "
                                    "%ld AND TABLE_ID = %ld",
                 OB_ALL_PART_TNAME,
                 tablegroup_schema.get_schema_version(),
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
  } else {
    ObUpdatePartHisInfoHelperV2 part_help(tablegroup_schema, client);
    if (OB_FAIL(part_help.update_partition_info())) {
      LOG_WARN("fail to update partition info", K(ret));
    }
  }
  return ret;
}

int ObTablegroupSqlService::ddl_log(
    common::ObISQLClient& client, const ObSchemaOperationType op_type, const ObTablegroupSchema& tablegroup_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaOperation opt;
  opt.tenant_id_ = tablegroup_schema.get_tenant_id();
  opt.database_id_ = 0;
  opt.tablegroup_id_ = tablegroup_schema.get_tablegroup_id();
  opt.table_id_ = 0;
  opt.op_type_ = op_type;
  opt.schema_version_ = tablegroup_schema.get_schema_version();
  if (OB_FAIL(log_operation(opt, client))) {
    LOG_WARN("fail to log operation", K(ret), K(opt));
  }
  return ret;
}

int ObTablegroupSqlService::check_tablegroup_options(const ObTablegroupSchema& tablegroup)
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2270 && !tablegroup.is_sub_part_template()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("nontemplate not supported in less CLUSTER_VERSION_2270", K(ret), "table_id", tablegroup.get_table_id());
  }
  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
