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
#include "ob_sequence_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_cluster_version.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_unit_getter.h"
#include "share/ob_srv_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "share/ob_autoincrement_service.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObSequenceSqlService::insert_sequence(const ObSequenceSchema &sequence_schema,
                                          common::ObISQLClient *sql_client,
                                          const common::ObString *ddl_stmt_str,
                                          const uint64_t *old_sequence_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL, ", K(ret));
  } else if (!sequence_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "sequence_schema is invalid", K(sequence_schema.get_sequence_name_str()), K(ret));
  } else {
    if (OB_FAIL(add_sequence(*sql_client, sequence_schema, false, old_sequence_id))) {
      LOG_WARN("failed to add sequence", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = sequence_schema.get_tenant_id();
      opt.database_id_ = sequence_schema.get_database_id();
      opt.sequence_id_ = sequence_schema.get_sequence_id();
      opt.table_id_ = sequence_schema.get_sequence_id();
      opt.table_name_ = sequence_schema.get_sequence_name();
      opt.op_type_ = OB_DDL_CREATE_SEQUENCE;
      opt.schema_version_ = sequence_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceSqlService::alter_sequence_start_with(const ObSequenceSchema &sequence_schema,
                                                    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = sequence_schema.get_tenant_id();
  uint64_t sequence_id = sequence_schema.get_sequence_id();
  int64_t affected_rows = 0;
  ObSqlString sql;
  const char *tname = OB_ALL_SEQUENCE_VALUE_TNAME;
  if (OB_FAIL(sql.assign_fmt("UPDATE %s set next_value = %s where sequence_id = %lu",
                            tname, sequence_schema.get_start_with().format(), sequence_id))) {
    LOG_WARN("append table name failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id,
                                      sql.ptr(),
                                      affected_rows))) {
    LOG_WARN("fail to execute sql", K(sql), K(ret));
  } else if (!is_zero_row(affected_rows) && !is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(affected_rows), K(sql), K(ret));
  }
  return ret;
}

// to get sync value from inner table.
int ObSequenceSqlService::get_sequence_sync_value(const uint64_t tenant_id,
                                                  const uint64_t sequence_id,
                                                  const bool is_for_update,
                                                  common::ObISQLClient &sql_client,
                                                  ObIAllocator &allocator,
                                                  common::number::ObNumber &next_value)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::number::ObNumber tmp;
  const char *tname = OB_ALL_SEQUENCE_VALUE_TNAME;
  const char *is_for_update_str = "FOR UPDATE";
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
                "SELECT NEXT_VALUE FROM %s "
                "WHERE SEQUENCE_ID = %lu",
                tname, sequence_id))) {
      LOG_WARN("fail to format sql", K(ret));
    } else if (is_for_update) {
      if (OB_FAIL(sql.append_fmt(" %s", is_for_update_str))) {
        LOG_WARN("fail to assign sql", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("can't find sequence", K(ret), K(tname), K(tenant_id), K(sequence_id));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret), K(tname), K(tenant_id), K(sequence_id));
        } else {
          // OB_ITER_END means there is no record in table,
          // thus the sync value is its' start value, and init the table when operate it.
        }
      } else {
        EXTRACT_NUMBER_FIELD_MYSQL(*result, NEXT_VALUE, tmp);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to get NEXT_VALUE", K(ret));
        } else if (OB_FAIL(next_value.from(tmp, allocator))) {
          LOG_WARN("fail to deep copy next_val", K(tmp), K(ret));
        } else if (OB_ITER_END != (ret = result->next())) {
          LOG_WARN("expected OB_ITER_END", K(ret));
          ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObSequenceSqlService::clean_sequence_cache(uint64_t tenant_id, uint64_t sequence_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 8> server_list;
  ObSrvRpcProxy srv_rpc_proxy;
  ObUnitInfoGetter ui_getter;
  if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.net_frame_) || OB_ISNULL(GCTX.net_frame_->get_req_transport())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy or net_frame in GCTX is null", K(GCTX.sql_proxy_), K(GCTX.net_frame_));
  } else if (OB_FAIL(ui_getter.init(*GCTX.sql_proxy_, &GCONF))) {
    LOG_WARN("init unit info getter failed", K(ret));
  } else if (OB_FAIL(ui_getter.get_tenant_servers(tenant_id, server_list))) {
    LOG_WARN("get tenant servers failed", K(ret));
  } else if (OB_FAIL(srv_rpc_proxy.init(GCTX.net_frame_->get_req_transport(), GCTX.self_addr()))) {
    LOG_WARN("fail to init srv rpc proxy", KR(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      const uint64_t timeout = THIS_WORKER.get_timeout_remain();
      if (OB_FAIL(srv_rpc_proxy
                  .to(server_list.at(i))
                  .by(tenant_id)
                  .timeout(timeout)
                  .clean_sequence_cache(sequence_id))) {
        if (is_timeout_err(ret) || is_server_down_error(ret)) {
          LOG_WARN("rpc call time out, ignore the error", "server", server_list.at(i),
                    K(tenant_id), K(sequence_id), K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("clean sequnece cache failed", K(ret), K(sequence_id), K(server_list.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObSequenceSqlService::replace_sequence(const ObSequenceSchema &sequence_schema,
                                           const bool is_rename,
                                           common::ObISQLClient *sql_client,
                                           bool alter_start_with,
                                           bool need_clean_cache,
                                           const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t tenant_id = sequence_schema.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    uint64_t sequence_id = sequence_schema.get_sequence_id();
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    // modify __all_sequence table
    if (OB_SUCC(ret)) {
      ObDMLExecHelper exec(*sql_client, exec_tenant_id);
      ObDMLSqlSplicer dml;
      if (!is_rename) {
        if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                   exec_tenant_id, tenant_id)))
            || OB_FAIL(dml.add_pk_column("sequence_id", ObSchemaUtils::get_extract_schema_id(
                                                        exec_tenant_id, sequence_id)))
            || OB_FAIL(dml.add_column("min_value", sequence_schema.get_min_value()))
            || OB_FAIL(dml.add_column("max_value", sequence_schema.get_max_value()))
            || OB_FAIL(dml.add_column("increment_by", sequence_schema.get_increment_by()))
            || OB_FAIL(dml.add_column("start_with", sequence_schema.get_start_with()))
            || OB_FAIL(dml.add_column("cache_size", sequence_schema.get_cache_size()))
            || OB_FAIL(dml.add_column("order_flag", sequence_schema.get_order_flag()))
            || OB_FAIL(dml.add_column("cycle_flag", sequence_schema.get_cycle_flag()))
            || OB_FAIL(dml.add_column("schema_version", sequence_schema.get_schema_version()))
            || OB_FAIL(dml.add_gmt_modified())) {
          LOG_WARN("add column failed", K(ret));
        } else {
          uint64_t compat_version = 0;
          if (FAILEDx(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
            LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
          } else if (((compat_version < MOCK_DATA_VERSION_4_2_3_0)
                      || (compat_version >= DATA_VERSION_4_3_0_0
                          && compat_version < DATA_VERSION_4_3_2_0))
                     && sequence_schema.get_flag() != 0) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not suppported flag != 0 when tenant's data version is below 4.2.3.0",
                     KR(ret));
          } else if ((compat_version >= MOCK_DATA_VERSION_4_2_3_0
                      && compat_version < DATA_VERSION_4_3_0_0)
                     || (compat_version >= DATA_VERSION_4_3_2_0)) {
            if (OB_FAIL(dml.add_column("flag", sequence_schema.get_flag()))) {
              LOG_WARN("add flag column failed", K(ret));
            }
          }
        }
      } else { // rename sequence
        if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                   exec_tenant_id, tenant_id)))
            || OB_FAIL(dml.add_pk_column("sequence_id", ObSchemaUtils::get_extract_schema_id(
                                                        exec_tenant_id, sequence_id)))
            || OB_FAIL(dml.add_column("sequence_name", ObHexEscapeSqlStr(sequence_schema.get_sequence_name())))
            || OB_FAIL(dml.add_column("schema_version", sequence_schema.get_schema_version()))
            || OB_FAIL(dml.add_gmt_modified())) {
          LOG_WARN("add column failed", K(ret));
        }
      }
      // udpate __all_sequence_object table
      int64_t affected_rows = 0;
      if (FAILEDx(exec.exec_update(OB_ALL_SEQUENCE_OBJECT_TNAME, dml, affected_rows))) {
        LOG_WARN("execute update sql fail", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
      } else {/*do nothing*/}
    }

    // add to __all_sequence_object_history table
    if (OB_SUCC(ret)) {
      const bool only_history = true;
      if (OB_FAIL(add_sequence(*sql_client, sequence_schema, only_history, nullptr /* old_sequence_id */))) {
        LOG_WARN("add_sequence failed",
                 K(sequence_schema.get_sequence_name_str()),
                 K(only_history),
                 K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (alter_start_with && OB_FAIL(alter_sequence_start_with(sequence_schema, *sql_client))) {
        LOG_WARN("alter sequence for start with failed", K(ret));
      } else if (need_clean_cache && OB_FAIL(clean_sequence_cache(tenant_id, sequence_id))) {
        LOG_WARN("clean sequence cache failed", K(ret));
      }
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = sequence_schema.get_tenant_id();
      opt.database_id_ = sequence_schema.get_database_id();
      opt.sequence_id_ = sequence_schema.get_sequence_id();
      opt.table_id_ = sequence_schema.get_sequence_id();
      opt.table_name_ = sequence_schema.get_sequence_name();
      opt.op_type_ = OB_DDL_ALTER_SEQUENCE;
      opt.schema_version_ = sequence_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceSqlService::delete_sequence(const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          const uint64_t sequence_id,
                                          const int64_t new_schema_version,
                                          common::ObISQLClient *sql_client,
                                          const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const int64_t IS_DELETED = 1;

  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql client is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || OB_INVALID_ID == sequence_id
                         || OB_INVALID_ID == database_id
                         || new_schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sequence info in drop sequence", K(tenant_id), K(database_id),
             K(sequence_id), K(ret));
  } else {
    // insert into __all_sequence_object_history
    if (FAILEDx(sql.assign_fmt(
                "INSERT INTO %s(tenant_id, sequence_id,schema_version,is_deleted)"
                " VALUES(%lu,%lu,%ld,%ld)",
                OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME,
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, sequence_id),
                new_schema_version, IS_DELETED))) {
      LOG_WARN("assign insert into all sequence history fail",
               K(tenant_id), K(sequence_id), K(ret));
    } else if (OB_FAIL(sql_client->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    }

    // delete from __all_sequence_object
    if (FAILEDx(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND sequence_id=%lu",
                               OB_ALL_SEQUENCE_OBJECT_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, sequence_id)))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(sql), K(affected_rows), K(ret));
    } else {
      LOG_INFO("success delete sequence schema",
               K(tenant_id),
               K(database_id),
               K(sequence_id),
               K(OB_ALL_SEQUENCE_OBJECT_TNAME));
    }

    if (FAILEDx(sql.assign_fmt("DELETE FROM %s WHERE sequence_id=%lu",
                               OB_ALL_SEQUENCE_VALUE_TNAME, sequence_id))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client->write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
    } else if (1 < affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("more than one row deleted", K(sql), K(affected_rows), K(ret));
    } else {
      LOG_INFO("success delete sequence value",
               K(tenant_id),
               K(database_id),
               K(sequence_id),
               K(OB_ALL_SEQUENCE_VALUE_TNAME));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = tenant_id;
      opt.sequence_id_ = sequence_id;
      opt.table_id_ = sequence_id;
      opt.database_id_ = database_id;
      opt.op_type_ = OB_DDL_DROP_SEQUENCE;
      opt.schema_version_ = new_schema_version;
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}


int ObSequenceSqlService::drop_sequence(const ObSequenceSchema &sequence_schema,
                                        const int64_t new_schema_version,
                                        common::ObISQLClient *sql_client,
                                        const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = sequence_schema.get_tenant_id();
  const uint64_t database_id = sequence_schema.get_database_id();
  const uint64_t sequence_id = sequence_schema.get_sequence_id();
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql client is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || OB_INVALID_ID == database_id
                         || OB_INVALID_ID == sequence_id
                         || new_schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sequence info in drop sequence",
             K(sequence_schema.get_sequence_name_str()), K(ret));
  } else if (OB_FAIL(delete_sequence(tenant_id, database_id, sequence_id,
                                     new_schema_version, sql_client, ddl_stmt_str))) {
    LOG_WARN("failed to delete sequence", K(sequence_schema.get_sequence_name_str()), K(ret));
  } else {/*do nothing*/}
  return ret;
}

// if old_sequence_id not null, it may be offline ddl/truncate operation that needs to sync origin sequence value.
int ObSequenceSqlService::add_sequence(common::ObISQLClient &sql_client,
                                       const ObSequenceSchema &sequence_schema,
                                       const bool only_history,
                                       const uint64_t *old_sequence_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  const bool need_sync_seq_val = nullptr == old_sequence_id ? false : true;
  const char *tname[] = {OB_ALL_SEQUENCE_OBJECT_TNAME, OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME};
  ObString max_sequence_params_hex_str;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const uint64_t tenant_id = sequence_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_SEQUENCE_OBJECT_TNAME)) {
      continue;
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                                        exec_tenant_id, sequence_schema.get_tenant_id()), "tenant_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                                        exec_tenant_id, sequence_schema.get_sequence_id()), "sequence_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                                        exec_tenant_id, sequence_schema.get_database_id()), "database_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_schema_version(), "schema_version", "%ld");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, sequence_schema.get_sequence_name().ptr(),
                                      sequence_schema.get_sequence_name().length(), "sequence_name");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_min_value().format(), "min_value", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_max_value().format(), "max_value", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_increment_by().format(), "increment_by", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_start_with().format(), "start_with", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_cache_size().format(), "cache_size", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_order_flag(), "order_flag", "%d");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_cycle_flag(), "cycle_flag", "%d");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_is_system_generated(), "is_system_generated", "%d");
      uint64_t compat_version = 0;
      if (FAILEDx(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
        LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
      } else if (((compat_version < MOCK_DATA_VERSION_4_2_3_0)
                  || (compat_version >= DATA_VERSION_4_3_0_0
                      && compat_version < DATA_VERSION_4_3_2_0))
                 && sequence_schema.get_flag() != 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not suppported flag != 0 when tenant's data version is below 4.2.3.0", KR(ret));
      } else if ((compat_version >= MOCK_DATA_VERSION_4_2_3_0
                  && compat_version < DATA_VERSION_4_3_0_0)
                 || (compat_version >= DATA_VERSION_4_3_2_0)) {
        SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_flag(), "flag", "%ld");
      }
      if (0 == STRCMP(tname[i], OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME)) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
      }
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else {
          if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
          }
        }
      }
    }
    values.reset();
  }
  if (OB_FAIL(ret)) {
  } else if (need_sync_seq_val && OB_FAIL(add_sequence_to_value_table(tenant_id,
                                                                      exec_tenant_id,
                                                                      *old_sequence_id,
                                                                      sequence_schema.get_sequence_id(),
                                                                      sql_client,
                                                                      allocator))) {
    LOG_WARN("fail to sync value to all_sequence_value", K(ret), K(tenant_id), K(exec_tenant_id), K(*old_sequence_id));
  }
  return ret;
}

// Notice that, offline ddl and truncate operation, sequence object is inherited from origin one.
// And get next_val from all_sequence_value via old_sequence_id, do nothing if no record found due to next_val is start_val,
// else fill the next val to the table.
int ObSequenceSqlService::add_sequence_to_value_table(const uint64_t tenant_id,
                                                      const uint64_t exec_tenant_id,
                                                      const uint64_t old_sequence_id,
                                                      const uint64_t new_sequence_id,
                                                      common::ObISQLClient &sql_client,
                                                      ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  common::number::ObNumber next_value;
  if (OB_FAIL(get_sequence_sync_value(tenant_id,
                                      old_sequence_id,
                                      false,/*is select for update*/
                                      sql_client,
                                      allocator,
                                      next_value))) {
    ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get sequence sync value", K(ret), K(tenant_id), K(old_sequence_id));
    }
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", OB_ALL_SEQUENCE_VALUE_TNAME))) {
    LOG_WARN("append table name failed, ", K(ret));
  } else {
    SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                                      exec_tenant_id, new_sequence_id), "sequence_id", "%lu");
    SQL_COL_APPEND_VALUE(sql, values, next_value.format(), "next_value", "%s");
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)",
                                 static_cast<int32_t>(values.length()),
                                 values.ptr()))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id,
                                          sql.ptr(),
                                          affected_rows))) {
      } else {
        if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
  }
  return ret;
}

} //end of schema
} //end of share
} //end of oceanbase
