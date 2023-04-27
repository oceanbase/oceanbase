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
#include "ob_trigger_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_trigger_info.h"
#include "ob_routine_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
int ObTriggerSqlService::create_trigger(const ObTriggerInfo &trigger_info,
                                        bool is_replace,
                                        ObISQLClient &sql_client,
                                        const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObDMLExecHelper exec(sql_client, trigger_info.get_exec_tenant_id());
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  OV (trigger_info.is_valid(), OB_INVALID_ARGUMENT, trigger_info);
  // insert or update all_trigger.
  OZ (fill_dml_sql(trigger_info, trigger_info.get_schema_version(), dml));
  if (is_replace) {
    OZ (exec.exec_update(OB_ALL_TENANT_TRIGGER_TNAME, dml, affected_rows));
  } else {
    OZ (exec.exec_insert(OB_ALL_TENANT_TRIGGER_TNAME, dml, affected_rows));
  }
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  // insert all_trigger_history.
  OZ (dml.add_column("is_deleted", 0));
  OZ (exec.exec_insert(OB_ALL_TENANT_TRIGGER_HISTORY_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  OZ (log_trigger_operation(trigger_info, trigger_info.get_schema_version(),
                            OB_DDL_CREATE_TRIGGER, ddl_stmt_str, sql_client));
  return ret;
}

int ObTriggerSqlService::drop_trigger(const ObTriggerInfo &trigger_info,
                                      bool drop_to_recyclebin,
                                      int64_t new_schema_version,
                                      ObISQLClient &sql_client,
                                      const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObDMLExecHelper exec(sql_client, trigger_info.get_exec_tenant_id());
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  ObSchemaOperationType drop_type = OB_INVALID_DDL_OP;
  OV (trigger_info.is_valid(), OB_INVALID_ARGUMENT, trigger_info);
  OZ (fill_dml_sql(trigger_info, new_schema_version, dml));
  // update or delete all_trigger.
  if (drop_to_recyclebin) {
    OZ (exec.exec_update(OB_ALL_TENANT_TRIGGER_TNAME, dml, affected_rows));
    OX (drop_type = OB_DDL_DROP_TRIGGER_TO_RECYCLEBIN);
  } else {
    OZ (exec.exec_delete(OB_ALL_TENANT_TRIGGER_TNAME, dml, affected_rows));
    OX (drop_type = OB_DDL_DROP_TRIGGER);
  }
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  // insert all_trigger_history.
  OZ (dml.add_column("is_deleted", drop_to_recyclebin ? 0 : 1));
  OZ (exec.exec_insert(OB_ALL_TENANT_TRIGGER_HISTORY_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  OZ (log_trigger_operation(trigger_info, new_schema_version,
                            drop_type, ddl_stmt_str, sql_client));
  return ret;
}

int ObTriggerSqlService::alter_trigger(const ObTriggerInfo &trigger_info,
                                       int64_t new_schema_version,
                                       ObISQLClient &sql_client,
                                       const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObDMLExecHelper exec(sql_client, trigger_info.get_exec_tenant_id());
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  OV (trigger_info.is_valid(), OB_INVALID_ARGUMENT, trigger_info);
  OZ (fill_dml_sql(trigger_info, new_schema_version, dml));
  // alter trigger disable/enable
  OZ (exec.exec_update(OB_ALL_TENANT_TRIGGER_TNAME, dml, affected_rows));
  OZ (dml.add_column("is_deleted", 0));
  OZ (exec.exec_insert(OB_ALL_TENANT_TRIGGER_HISTORY_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  OZ (log_trigger_operation(trigger_info, new_schema_version, OB_DDL_ALTER_TRIGGER, ddl_stmt_str,
                            sql_client));
  return ret;
}


int ObTriggerSqlService::flashback_trigger(const ObTriggerInfo &trigger_info,
                                           int64_t new_schema_version,
                                           ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObTriggerValues new_values;
  ObDMLExecHelper exec(sql_client, trigger_info.get_exec_tenant_id());
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  OV (trigger_info.is_valid(), OB_INVALID_ARGUMENT, trigger_info);
  OX (new_values.set_database_id(trigger_info.get_database_id()));
  OX (new_values.set_trigger_name(trigger_info.get_trigger_name()));
  // update all_trigger.
  OZ (fill_dml_sql(trigger_info, new_values, new_schema_version, dml));
  OZ (exec.exec_update(OB_ALL_TENANT_TRIGGER_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  // insert all_trigger_history.
  OZ (dml.add_column("is_deleted", 0));
  OZ (exec.exec_insert(OB_ALL_TENANT_TRIGGER_HISTORY_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  OZ (log_trigger_operation(trigger_info, new_schema_version,
                            OB_DDL_FLASHBACK_TRIGGER, NULL, sql_client));
  return ret;
}

int ObTriggerSqlService::rebuild_trigger_package(const ObTriggerInfo &trigger_info,
                                                 const ObString &base_object_database,
                                                 const ObString &base_object_name,
                                                 int64_t new_schema_version,
                                                 ObISQLClient &sql_client,
                                                 ObSchemaOperationType op_type)
{
  int ret = OB_SUCCESS;
  ObString spec_source;
  ObString body_source;
  ObArenaAllocator inner_alloc;
  ObDMLExecHelper exec(sql_client, trigger_info.get_exec_tenant_id());
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  bool is_oracle_mode = false;
  ObTriggerInfo new_trigger_info(&inner_alloc);
  OV (trigger_info.is_valid(), OB_INVALID_ARGUMENT, trigger_info);
  OZ (new_trigger_info.deep_copy(trigger_info));
  OZ (ObCompatModeGetter::check_is_oracle_mode_with_table_id(new_trigger_info.get_tenant_id(),
                                                             new_trigger_info.get_base_object_id(),
                                                             is_oracle_mode));
  OZ (ObTriggerInfo::replace_table_name_in_body(new_trigger_info, inner_alloc, base_object_database,
                                                base_object_name, is_oracle_mode));

  // update all_trigger.
  OZ (fill_dml_sql(new_trigger_info, new_schema_version, dml));
  OZ (exec.exec_update(OB_ALL_TENANT_TRIGGER_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  // insert all_trigger_history.
  OZ (dml.add_column("is_deleted", 0));
  OZ (exec.exec_insert(OB_ALL_TENANT_TRIGGER_HISTORY_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  OZ (log_trigger_operation(new_trigger_info, new_schema_version,
                            op_type, NULL, sql_client));
  return ret;
}

int ObTriggerSqlService::update_base_object_id(const ObTriggerInfo &trigger_info,
                                               uint64_t base_object_id,
                                               int64_t new_schema_version,
                                               ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObTriggerValues new_values;
  ObDMLExecHelper exec(sql_client, trigger_info.get_exec_tenant_id());
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  OV (trigger_info.is_valid(), OB_INVALID_ARGUMENT, trigger_info);
  OX (new_values.set_base_object_id(base_object_id));
  // update all_trigger.
  OZ (fill_dml_sql(trigger_info, new_values, new_schema_version, dml));
  OZ (exec.exec_update(OB_ALL_TENANT_TRIGGER_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  // insert all_trigger_history.
  OZ (dml.add_column("is_deleted", 0));
  OZ (exec.exec_insert(OB_ALL_TENANT_TRIGGER_HISTORY_TNAME, dml, affected_rows));
  OV (is_single_row(affected_rows), OB_ERR_UNEXPECTED, affected_rows);
  OZ (log_trigger_operation(trigger_info, new_schema_version,
                            OB_DDL_ALTER_TRIGGER, NULL, sql_client));
  return ret;
}

int ObTriggerSqlService::fill_dml_sql(const ObTriggerInfo &trigger_info,
                                      int64_t new_schema_version,
                                      ObDMLSqlSplicer &dml)
{
  ObTriggerValues tmp_values;
  return fill_dml_sql(trigger_info, tmp_values, new_schema_version, dml);
}

int ObTriggerSqlService::fill_dml_sql(const ObTriggerInfo &trigger_info,
                                      const ObTriggerValues &new_values,
                                      int64_t new_schema_version,
                                      ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = new_values.new_database_id() ?
                           new_values.get_database_id() :
                           trigger_info.get_database_id();
  uint64_t base_object_id = new_values.new_base_object_id() ?
                              new_values.get_base_object_id() :
                              trigger_info.get_base_object_id();
  const ObString &trigger_name = new_values.new_trigger_name() ?
                                   new_values.get_trigger_name() :
                                   trigger_info.get_trigger_name();
  const ObString &spec_source = new_values.new_spec_source() ?
                                  new_values.get_spec_source() :
                                  trigger_info.get_package_spec_source();
  const ObString &body_source = new_values.new_body_source() ?
                                  new_values.get_body_source() :
                                  trigger_info.get_package_body_source();
  uint64_t exec_tenant_id = trigger_info.get_exec_tenant_id();
  uint64_t pure_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id,
                                                                 trigger_info.get_tenant_id());
  uint64_t pure_trigger_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                  trigger_info.get_trigger_id());
  uint64_t pure_owner_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                trigger_info.get_owner_id());
  uint64_t pure_database_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                   database_id);
  uint64_t pure_base_object_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                      base_object_id);
  OZ (dml.add_pk_column("tenant_id", pure_tenant_id));
  OZ (dml.add_pk_column("trigger_id", pure_trigger_id));
  OZ (dml.add_column("owner_id", pure_owner_id));
  OZ (dml.add_column("database_id", pure_database_id));
  OZ (dml.add_column("schema_version", new_schema_version));
  OZ (dml.add_column("base_object_type", trigger_info.get_base_object_type()));
  OZ (dml.add_column("base_object_id", pure_base_object_id));
  OZ (dml.add_column("trigger_type", trigger_info.get_trigger_type()));
  OZ (dml.add_column("trigger_events", trigger_info.get_trigger_events()));
  OZ (dml.add_column("timing_points", trigger_info.get_timing_points()));
  OZ (dml.add_column("trigger_flags", trigger_info.get_trigger_flags()));
  OZ (dml.add_column("trigger_name", ObHexEscapeSqlStr(trigger_name)));
  OZ (dml.add_column("update_columns", ObHexEscapeSqlStr(trigger_info.get_update_columns())));
  OZ (dml.add_column("ref_old_name", ObHexEscapeSqlStr(trigger_info.get_ref_old_name())));
  OZ (dml.add_column("ref_new_name", ObHexEscapeSqlStr(trigger_info.get_ref_new_name())));
  OZ (dml.add_column("ref_parent_name", ObHexEscapeSqlStr(trigger_info.get_ref_parent_name())));
  OZ (dml.add_column("when_condition", ObHexEscapeSqlStr(trigger_info.get_when_condition())));
  OZ (dml.add_column("trigger_body", ObHexEscapeSqlStr(trigger_info.get_trigger_body())));
  OZ (dml.add_column("package_spec_source", ObHexEscapeSqlStr(spec_source)));
  OZ (dml.add_column("package_body_source", ObHexEscapeSqlStr(body_source)));
  OZ (dml.add_column("package_flag", trigger_info.get_package_flag()));
  OZ (dml.add_column("package_comp_flag", trigger_info.get_package_comp_flag()));
  OZ (dml.add_column("package_exec_env", ObHexEscapeSqlStr(trigger_info.get_package_exec_env())));
  OZ (dml.add_column("sql_mode", trigger_info.get_sql_mode()));
  OZ (dml.add_column("trigger_priv_user", ObHexEscapeSqlStr(trigger_info.get_trigger_priv_user())));
  OZ (dml.add_column("order_type", trigger_info.get_order_type_value()));
  OZ (dml.add_column("ref_trg_db_name", ObHexEscapeSqlStr(trigger_info.get_ref_trg_db_name())));
  OZ (dml.add_column("ref_trg_name", ObHexEscapeSqlStr(trigger_info.get_ref_trg_name())));
  OZ (dml.add_column("action_order", trigger_info.get_action_order()));
  if (OB_SUCC(ret)) {
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(trigger_info.get_tenant_id(), data_version))) {
      LOG_WARN("failed to get data version", K(ret));
    } else if (data_version < DATA_VERSION_4_2_0_0 && 0 != trigger_info.get_analyze_flag()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tenant data version is less than 4.2, analyze_flag column is not supported", K(ret), K(data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2, analyze_flag column");
    } else if (data_version >= DATA_VERSION_4_2_0_0) {
      OZ (dml.add_column("analyze_flag", trigger_info.get_analyze_flag()));
    }
  }
  return ret;
}

int ObTriggerSqlService::log_trigger_operation(const ObTriggerInfo &trigger_info,
                                               int64_t new_schema_version,
                                               ObSchemaOperationType op_type,
                                               const ObString *ddl_stmt_str,
                                               ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSchemaOperation opt;
  OX (opt.tenant_id_ = trigger_info.get_tenant_id());
  OX (opt.database_id_ = trigger_info.get_database_id());
  OX (opt.table_id_ = trigger_info.get_trigger_id());
  OX (opt.schema_version_ = new_schema_version);
  OX (opt.op_type_ = op_type);
  OX (opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString());
  OZ (log_operation(opt, sql_client));
  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
