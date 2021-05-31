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

#define USING_LOG_PREFIX RS

#include "ob_restore_meta.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/restore/ob_restore_table_operator.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_system_admin_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObRestoreMeta::ObRestoreMeta(observer::ObRestoreCtx& restore_ctx, RestoreJob& job_info, const volatile bool& is_stop)
    : inited_(false),
      is_stop_(is_stop),
      restore_ctx_(restore_ctx),
      dropped_index_ids_(),
      job_info_(job_info),
      restore_args_(),
      oss_reader_(restore_args_),
      conn_(),
      sql_modifier_(restore_args_, dropped_index_ids_),
      table_id_pairs_(),
      index_id_pairs_()
{}

ObRestoreMeta::~ObRestoreMeta()
{
  inited_ = false;
}

int ObRestoreMeta::init()
{
  int ret = OB_SUCCESS;
  if (!restore_ctx_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid restore ctx", K(ret));
  } else if (!job_info_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job info", K_(job_info), K(ret));
  } else {
    // init connection for restore sqls
    inited_ = true;
  }
  return ret;
}

/*
 * restore meta follow the steps below:
 * 1. Get SQL(Resource Cmd, DDL) from backup data.
 * 2. Resolve Resource Cmd and replace parameters.
 * 3. Execute Resource Cmd and create tenant.
 * 4. Resolve DDL and replace parameters.
 * 5. Execute DDL to construct schema for backup data.
 * 6. Generate logical restore tasks according to schema.
 */
int ObRestoreMeta::execute()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init())) {
    LOG_WARN("fail init meta restore enviroment", K_(job_info), K(ret));
    ;
  } else if (OB_FAIL(oss_reader_.init(job_info_.backup_uri_))) {  // will help init restore args
    LOG_WARN("fail init oss reader", K_(job_info), K(ret));
  } else if (OB_FAIL(init_sql_modifier())) {
    LOG_WARN("fail init sql modifier", K_(job_info), K(ret));
  } else if (OB_FAIL(init_connection())) {
    LOG_WARN("fail generate tenant id", K_(job_info), K(ret));
  } else if (OB_FAIL(restore_resource())) {
    LOG_WARN("fail restore resource", K_(job_info), K(ret));
  } else if (OB_FAIL(restore_schema())) {
    LOG_WARN("fail restore schema", K_(job_info), K(ret));
  } else if (OB_FAIL(generate_task())) {
    LOG_WARN("fail generate task", K_(job_info), K(ret));
  }
  return ret;
}

int ObRestoreMeta::init_sql_modifier()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    sql_modifier_.set_tenant_name(job_info_.tenant_name_);
  }
  return ret;
}

int ObRestoreMeta::init_connection()
{
  int ret = OB_SUCCESS;
  oracle_conn_.set_oracle_compat_mode();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(conn_.init(NULL,
                 restore_ctx_.schema_service_,
                 restore_ctx_.ob_sql_,
                 restore_ctx_.vt_iter_creator_,
                 restore_ctx_.partition_table_operator_,
                 restore_ctx_.server_config_,
                 NULL,
                 NULL,
                 &sql_modifier_))) {
    LOG_WARN("fail init connection", K(ret));
  } else if (OB_FAIL(oracle_conn_.init(NULL,
                 restore_ctx_.schema_service_,
                 restore_ctx_.ob_sql_,
                 restore_ctx_.vt_iter_creator_,
                 restore_ctx_.partition_table_operator_,
                 restore_ctx_.server_config_,
                 NULL,
                 NULL,
                 &sql_modifier_))) {
    LOG_WARN("fail init connection", K(ret));
  }
  return ret;
}

int ObRestoreMeta::restore_resource()
{
  LOG_INFO("begin restore resource");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObArray<ObString> unit_stmts;
    ObArray<ObString> pool_stmts;
    ObString tenant_stmt;
    if (OB_FAIL(oss_reader_.get_create_tenant_stmt(tenant_stmt))) {
      LOG_WARN("fail get restore tenant resource", K_(job_info), K(ret));
    } else if (OB_UNLIKELY(tenant_stmt.empty())) {
      // note: unit_stmts could be null, e.g. share sys_unit_config
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid stmt", K(tenant_stmt), K(ret));
    } else {
      // restore_sql_proxy_ replace params internally
      int64_t affected_rows = 0;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(conn_.execute_write(OB_SYS_TENANT_ID, tenant_stmt.ptr(), affected_rows))) {
          LOG_WARN("fail execute sql", K(tenant_stmt), K(affected_rows));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
      LOG_INFO("restore write sql", K(ret));
    }
  }
  LOG_INFO("end restore resource", K(ret));
  return ret;
}

int ObRestoreMeta::restore_schema()
{
  LOG_INFO("begin restore schema");
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  ObWorker::CompatMode compat_mode;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(restore_ctx_.schema_service_) || OB_ISNULL(restore_ctx_.sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service pointer", K(ret));
  } else if (OB_FAIL(restore_ctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_id(job_info_.tenant_name_, tenant_id))) {
    LOG_WARN("fail get tenant id", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("failed to get compat mode", K(ret));
  } else {
    ObArray<ObString> tg_stmts;
    ObArray<ObString> fk_stmts;
    ObArray<ObString> db_stmts;
    ObArray<ObString> tb_stmts;
    ObArray<ObString> idx_stmts;
    ObArray<ObString> sys_var_stmts;
    ObArray<ObString> user_stmt;
    ObArray<ObString> user_priv_stmts;
    ObArray<ObString> tenant_parameter_stmts;
    ObArray<ObRecycleObject> recycle_objects;
    ObArray<ObString> create_synonym_stmts;
    ObArray<ObString> timezone_info_stmts;

    if (OB_FAIL(oss_reader_.get_create_tablegroup_stmts(tg_stmts))) {
      LOG_WARN("fail get restore table group", K_(job_info), K(ret));
    } else if (OB_FAIL(oss_reader_.get_create_foreign_key_stmts(fk_stmts))) {
      LOG_WARN("fail get restore foreign key", K_(job_info), K(ret));
    } else if (OB_FAIL(oss_reader_.get_create_database_stmts(db_stmts))) {
      LOG_WARN("fail get restore database", K_(job_info), K(ret));
    } else if (OB_FAIL(oss_reader_.get_create_data_table_stmts(tb_stmts))) {
      LOG_WARN("fail get restore data table", K_(job_info), K(ret));
    } else if (OB_FAIL(oss_reader_.get_create_index_table_stmts(idx_stmts, dropped_index_ids_))) {
      LOG_WARN("fail get restore index table", K_(job_info), K(ret));
    } else if (OB_FAIL(oss_reader_.get_direct_executable_stmts(OB_SYSTEM_VARIABLE_DEFINITION, sys_var_stmts))) {
      LOG_WARN("fail to get system variable definition", K(ret));
    } else if (OB_FAIL(oss_reader_.get_direct_executable_stmts(OB_CREATE_USER_DEINITION, user_stmt))) {
      LOG_WARN("fail to get create user definition", K(ret));
    } else if (OB_FAIL(oss_reader_.get_direct_executable_stmts(OB_USER_PRIVILEGE_DEFINITION, user_priv_stmts))) {
      LOG_WARN("fail to get user privilege definition", K(ret));
    } else if (OB_FAIL(
                   oss_reader_.get_direct_executable_stmts(OB_TENANT_PARAMETER_DEFINITION, tenant_parameter_stmts))) {
      LOG_WARN("fail to get tenant parameter definition", K(ret));
    } else if (OB_FAIL(oss_reader_.get_create_synonym_stmts(create_synonym_stmts))) {
      LOG_WARN("fail to get create_synonym definition", K(ret));
      //} else if (OB_FAIL(oss_reader_.get_recycle_objects(recycle_objects))) {
      //  LOG_WARN("fail to get recycle objects", K(ret));
    } else if (OB_FAIL(oss_reader_.get_create_all_timezone_stmts(timezone_info_stmts))) {
      LOG_WARN("fail to get all timezone info stmts", K(ret));
    } else {

      // restore_sql_proxy_ replace params internally
      observer::ObInnerSQLConnection* tenant_conn =
          (ObWorker::CompatMode::ORACLE == compat_mode) ? &oracle_conn_ : &conn_;
      int64_t affected_rows = 0;

      uint64_t created_tablegroup_id = OB_INVALID_ID;
      tablegroup_id_pairs_.reset();
      FOREACH_X(u, tg_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup create tablegroup stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          if (OB_TABLEGROUP_EXIST == ret) {
            LOG_INFO("table group already exist!", K(*u), K(tenant_id));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail execute sql", K(*u), K(affected_rows));
          }
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one row affected, unexpected", K(affected_rows), K(ret));
        } else if (OB_FAIL(get_created_tablegroup_id(
                       tenant_id, restore_args_.sql_info_.backup_tablegroup_name_, created_tablegroup_id))) {
          LOG_WARN("get_created_tablegroup_id fail", K(ret), KPC(u));
        } else {
          TablegroupIdPair tablegroup_id_pair =
              TablegroupIdPair(created_tablegroup_id, restore_args_.sql_info_.backup_tablegroup_id_);
          if (OB_FAIL(tablegroup_id_pairs_.push_back(tablegroup_id_pair))) {
            LOG_WARN("fail push back tablegroup_id_pair", K(tablegroup_id_pair), K(ret));
          } else {
            LOG_INFO("restore tablegroup", K(tablegroup_id_pair));
          }
        }
      }

      FOREACH_X(u, create_synonym_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          LOG_WARN("fail execute sql", K(*u), K(affected_rows));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one row affected, unexpected", K(*u), K(affected_rows), K(ret));
        }
      }

      FOREACH_X(u, db_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup create db stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          // Ignore failure of database creation, since we may lack of privilege to create databases such as
          // INFORMATION_SCHEMA, RECYCLEBIN, OCEANBASE.
          // FIXME: Ignore failure of database according to specific database_name.
          if (OB_ERR_NO_DB_PRIVILEGE == ret) {
            LOG_INFO("IGNORE privilege error", K(tenant_id), "sql", u->ptr(), K(ret));
            ret = OB_SUCCESS;
          } else if (OB_DATABASE_EXIST == ret) {
            LOG_INFO("database or user already exist", K(tenant_id), "sql", u->ptr(), K(ret));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail execute sql", K(*u), K(tenant_id), K(affected_rows));
          }
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
        }
      }

      // Create restore user for logical incremental restore.
      char restore_user_sql[OB_SHORT_SQL_LENGTH];
      char restore_priv_sql[OB_SHORT_SQL_LENGTH];
      char restore_grant_priv_sql[OB_SHORT_SQL_LENGTH];
      char restore_grant_ora_priv_sql[OB_SHORT_SQL_LENGTH];
      if (OB_SUCC(ret)) {
        if (OB_FAIL(databuff_printf(restore_user_sql,
                OB_SHORT_SQL_LENGTH,
                ObWorker::CompatMode::ORACLE == compat_mode ? "CREATE USER '%s'@'%%' IDENTIFIED BY \"%s\""
                                                            : "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'",
                restore_args_.restore_user_,
                restore_args_.restore_pass_))) {
          LOG_WARN("fail format sql", K_(restore_args), K(ret));
        } else if (OB_FAIL(databuff_printf(restore_priv_sql,
                       OB_SHORT_SQL_LENGTH,
                       "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%'",
                       restore_args_.restore_user_))) {
          LOG_WARN("fail format sql", K_(restore_args), K(ret));
        } else if (OB_FAIL(databuff_printf(restore_grant_priv_sql,
                       OB_SHORT_SQL_LENGTH,
                       "GRANT grant option ON *.* TO '%s'@'%%'",
                       restore_args_.restore_user_))) {
          LOG_WARN("fail format sql", K_(restore_args), K(ret));
        } else if (ObWorker::CompatMode::ORACLE == compat_mode && OB_FAIL(databuff_printf(restore_grant_ora_priv_sql,
                                                                      OB_SHORT_SQL_LENGTH,
                                                                      "GRANT dba TO \"%s\"",
                                                                      restore_args_.restore_user_))) {
          LOG_WARN("fail format sql", K_(restore_args), K(ret));
        } else if (OB_FAIL(user_stmt.push_back(ObString(restore_user_sql)))) {
          LOG_WARN("fail push sql to stmts", K(restore_user_sql), K(ret));
        } else if (OB_FAIL(user_stmt.push_back(ObString(restore_priv_sql)))) {
          LOG_WARN("fail push sql to stmts", K(restore_priv_sql), K(ret));
        } else if (OB_FAIL(user_stmt.push_back(ObString(restore_grant_priv_sql)))) {
          LOG_WARN("fail push sql to stmts", K(restore_grant_priv_sql), K(ret));
        }
      }

      FOREACH_X(u, user_stmt, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup create db stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          if (OB_ERR_USER_EXIST == ret) {
            LOG_INFO("database or user already exist", K(tenant_id), "sql", u->ptr(), K(ret));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail execute sql", K(*u), K(tenant_id), K(affected_rows));
          }
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
        }
      }

      table_id_pairs_.reset();
      uint64_t created_table_id = OB_INVALID_ID;
      FOREACH_X(u, tb_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup create table stmt, skip!");
          continue;
        }
        // Create table with lots of partitions or other concurent DDL may cause timeout error.
        // Here, we retry 3 times at most when failure occured.
        const int max_retry_times = 3;
        int retry_times = 0;
        bool need_retry = false;
        do {
          need_retry = false;
          if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
            if (retry_times > 0 && OB_ERR_TABLE_EXIST == ret) {
              LOG_INFO("Table created before retry", K(tenant_id), K(ret));
              ret = OB_SUCCESS;
            } else if (retry_times++ < max_retry_times && OB_TIMEOUT == ret) {
              LOG_INFO("Table create timeout and retry", K(tenant_id), K(retry_times), K(max_retry_times), K(ret));
              need_retry = true;
              usleep(500 * 1000);  // 500ms
            } else {
              LOG_WARN("fail execute sql", K(*u), K(tenant_id), K(affected_rows), K(ret));
            }
          }
        } while (need_retry);

        if (OB_FAIL(ret)) {
          // fail or ignore
          if (OB_ERR_NO_DB_PRIVILEGE == ret) {
            LOG_INFO("IGNORE database privilege error", K(tenant_id), "sql", u->ptr(), K(ret));
            ret = OB_SUCCESS;
          }
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(get_created_table_id(tenant_id,
                       restore_args_.sql_info_.backup_database_name_,
                       restore_args_.sql_info_.backup_table_name_,
                       created_table_id))) {
          LOG_WARN("get_created_table_id fail", K(ret), KPC(u));
        } else {
          TableIdPair table_id_pair = TableIdPair(created_table_id, restore_args_.sql_info_.backup_table_id_);
          if (OB_FAIL(table_id_pairs_.push_back(table_id_pair))) {
            LOG_WARN("fail push back table_id_pair", K(table_id_pair), K(ret));
          } else {
            LOG_INFO("restore table", K(table_id_pair));
          }
        }
      }

      uint64_t created_index_id = OB_INVALID_ID;
      index_id_pairs_.reset();
      const ObSimpleTableSchemaV2* index_schema = NULL;
      FOREACH_X(u, idx_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup create index stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          if (OB_ERR_NO_DB_PRIVILEGE == ret) {
            LOG_INFO("IGNORE database privilege error", K(tenant_id), "sql", u->ptr(), K(ret));
            ret = OB_SUCCESS;
          } else if (OB_ERR_KEY_NAME_DUPLICATE == ret) {
            LOG_INFO("IGNORE Index already created error", K(ret));
            ret = OB_SUCCESS;
          } else if (OB_ERR_COLUMN_LIST_ALREADY_INDEXED == ret) {
            LOG_INFO("IGNORE column list already indexed error", K(ret));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail execute sql", K(*u), K(tenant_id), K(affected_rows), K(ret));
          }
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(get_created_table_id(tenant_id,
                       restore_args_.sql_info_.backup_database_name_,
                       restore_args_.sql_info_.backup_table_name_,
                       created_table_id))) {
          LOG_WARN("get_created_table_id for data table fail", "info", restore_args_.sql_info_, K(ret), KPC(u));
        } else if (OB_FAIL(restore_args_.sql_info_.add_backup_index_name_prefix(created_table_id))) {
          LOG_WARN("fail add inner prefix to backup index name", "info", restore_args_.sql_info_, K(ret));
        } else if (OB_FAIL(get_created_index_schema(tenant_id,
                       restore_args_.sql_info_.backup_database_name_,
                       restore_args_.sql_info_.backup_index_name_,
                       index_schema))) {
          LOG_WARN("get_created_index_schema fail", "info", restore_args_.sql_info_, KPC(u), K(ret));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index schema is null", K(ret), K(tenant_id), KPC(u));
        } else {
          created_index_id = index_schema->get_table_id();
          IndexIdPair index_id_pair =
              IndexIdPair(created_table_id, created_index_id, restore_args_.sql_info_.backup_index_id_);
          int hash_ret = dropped_index_ids_.exist_refactored(restore_args_.sql_info_.backup_index_id_);

          if (!index_id_pair.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid id pair", K(index_id_pair), K(ret));
          } else if (OB_HASH_EXIST == hash_ret) {
            LOG_INFO("restore index, but not add to index id pairs", K(index_id_pair));
          } else if (OB_HASH_NOT_EXIST != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR(
                "invalid hash set ret", K(ret), K(hash_ret), "index_id", restore_args_.sql_info_.backup_index_id_);
          } else {
            if (OB_FAIL(index_id_pairs_.push_back(index_id_pair))) {
              LOG_WARN("fail push back index_id_pair", K(index_id_pair), K(ret));
            } else {
              LOG_INFO("restore index", K(index_id_pair));
            }

            if (OB_SUCC(ret) && index_schema->has_partition()) {
              // global index with partition
              TableIdPair table_id_pair = TableIdPair(created_index_id, restore_args_.sql_info_.backup_index_id_);
              if (OB_FAIL(table_id_pairs_.push_back(table_id_pair))) {
                LOG_WARN("fail push back table_id_pair", K(table_id_pair), K(ret));
              } else {
                LOG_INFO("restore index with partition", K(table_id_pair));
              }
            }
          }
        }
      }

      DEBUG_SYNC(BEFORE_RESTORE_PARTITIONS);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(restore_partitions(tenant_id))) {
          LOG_WARN("fail to restore partitions", K(ret), K(tenant_id));
        }
      }

      // restore __all_recyclebin after create table schema
      // if (OB_SUCC(ret)) {
      //  if (OB_FAIL(restore_all_recyclebin(tenant_id, recycle_objects))) {
      //    LOG_WARN("fail to restore __all_recyclebin", K(ret), K(tenant_id));
      //  }
      //}

      FOREACH_X(u, sys_var_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          LOG_WARN("fail execute sql", K(*u), K(affected_rows));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one row affected, unexpected", K(*u), K(affected_rows), K(ret));
        }
      }

      FOREACH_X(u, user_priv_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          LOG_WARN("fail execute sql", K(*u), K(affected_rows));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one row affected, unexpected", K(*u), K(affected_rows), K(ret));
        }
      }

      FOREACH_X(u, tenant_parameter_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          LOG_WARN("fail execute sql", K(*u), K(affected_rows));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one row affected, unexpected", K(*u), K(affected_rows), K(ret));
        }
      }

      FOREACH_X(u, fk_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          LOG_WARN("fail execute sql", K(*u), K(affected_rows));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one row affected, unexpected", K(*u), K(affected_rows), K(ret));
        }
      }

      FOREACH_X(u, create_synonym_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          LOG_WARN("fail execute sql", K(*u), K(affected_rows));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one row affected, unexpected", K(*u), K(affected_rows), K(ret));
        }
      }

      FOREACH_X(u, timezone_info_stmts, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (u->empty()) {
          LOG_WARN("found a empty backup stmt, skip!");
          continue;
        }
        if (OB_FAIL(tenant_conn->execute_write(tenant_id, u->ptr(), affected_rows))) {
          LOG_WARN("fail execute sql", K(*u), K(affected_rows));
        }
      }
    }
  }
  LOG_INFO("end restore schema for tenant", K(tenant_id), K(ret));
  return ret;
}

int ObRestoreMeta::restore_all_recyclebin(const uint64_t tenant_id, const ObIArray<ObRecycleObject>& recycle_objects)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArenaAllocator allocator;
  ObArray<const ObRecycleObject*> new_objects;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(clear_all_recyclebin(tenant_id))) {
    LOG_WARN("fail to clear __all_recyclebin", K(ret), K(tenant_id));
  } else if (OB_FAIL(fill_recycle_objects(allocator, tenant_id, recycle_objects, new_objects))) {
    LOG_WARN("fail to fill recycle schema", K(ret), K(tenant_id));
  } else {
    int64_t start = 0;
    int64_t end = 0;
    int64_t num = new_objects.count();
    const int64_t MAX_INSERT_BATCH_NUM = 1000;
    while (OB_SUCC(ret) && start < num) {
      end = min(start + MAX_INSERT_BATCH_NUM - 1, num - 1);
      share::ObDMLSqlSplicer dml;
      ObSqlString sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.append_fmt("INSERT INTO %s(tenant_id, object_name, type, "
                                 "database_id, table_id, tablegroup_id, original_name) "
                                 "VALUES ",
              OB_ALL_RECYCLEBIN_TNAME))) {
        LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
      }
      for (int64_t i = start; OB_SUCC(ret) && i <= end; i++) {
        const ObRecycleObject* object = new_objects.at(i);
        if (OB_ISNULL(object)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema is null", K(ret));
        } else if (OB_FAIL(sql.append_fmt("%s (%lu, '%.*s', %d, %ld, %ld, %ld, '%.*s')",
                       start == i ? "" : ",",
                       ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                       object->get_object_name().length(),
                       object->get_object_name().ptr(),
                       object->get_type(),
                       ObSchemaUtils::get_extract_schema_id(tenant_id, object->get_database_id()),
                       ObSchemaUtils::get_extract_schema_id(tenant_id, object->get_table_id()),
                       ObSchemaUtils::get_extract_schema_id(tenant_id, object->get_tablegroup_id()),
                       object->get_original_name().length(),
                       object->get_original_name().ptr()))) {
          LOG_WARN("fail to append fmt", K(ret), K(tenant_id));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(conn_.execute_write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (affected_rows != end - start + 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows not match", K(ret), K(affected_rows), K(start), K(end));
      }
      start = end + 1;
    }
  }
  return ret;
}

int ObRestoreMeta::fill_recycle_objects(ObIAllocator& allocator, const uint64_t tenant_id,
    const ObIArray<ObRecycleObject>& orig_objects, ObIArray<const ObRecycleObject*>& new_objects)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService* schema_service = restore_ctx_.schema_service_;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null ", K(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else {
    new_objects.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_objects.count(); i++) {
      uint64_t table_id = OB_INVALID_ID;
      uint64_t tablegroup_id = OB_INVALID_ID;
      uint64_t database_id = OB_INVALID_ID;
      ObString index_orig_name;
      const ObRecycleObject& orig_object = orig_objects.at(i);
      ObRecycleObject tmp_object;
      ObRecycleObject::RecycleObjType type = orig_object.get_type();

      // table_id
      if (ObRecycleObject::DATABASE == type) {
        table_id = OB_INVALID_ID;
      } else if (ObRecycleObject::VIEW == type || ObRecycleObject::TABLE == type) {
        if (OB_FAIL(schema_guard.get_table_id(tenant_id,
                combine_id(tenant_id, OB_RECYCLEBIN_SCHEMA_ID),
                orig_object.get_object_name(),
                false, /*is_index*/
                ObSchemaGetterGuard::ALL_TYPES,
                table_id))) {
          LOG_WARN("fail to get table_id", K(ret), K(tenant_id), K(orig_object));
        } else if (OB_INVALID_ID == table_id) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table not exist", K(ret), K(tenant_id), K(orig_object));
        }
      } else if (ObRecycleObject::INDEX == type) {
        const ObTableSchema* index_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                combine_id(tenant_id, OB_RECYCLEBIN_SCHEMA_ID),
                orig_object.get_object_name(),
                true, /*is_index*/
                index_schema))) {
          LOG_WARN("fail to get table_id", K(ret), K(tenant_id), K(orig_object));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table not exist", K(ret), K(tenant_id), K(orig_object));
        } else {
          table_id = index_schema->get_table_id();
          // Index table should rewrite index table since data_table_id
          // which is used to generate index name may has been changed.
          ObString orig_name;
          if (ObSimpleTableSchemaV2::get_index_name(orig_object.get_original_name(), orig_name)) {
            LOG_WARN("fail to get original index name", K(ret), K(orig_object));
          } else if (ObTableSchema::build_index_table_name(
                         allocator, index_schema->get_data_table_id(), orig_name, index_orig_name)) {
            LOG_WARN("fail to reconstruct index original name", K(ret), K(orig_object));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid recycle object", K(ret), K(orig_object));
      }

      // tablegroup_id
      if (OB_FAIL(ret)) {
      } else if (orig_object.get_tablegroup_name().empty()) {
        tablegroup_id = OB_INVALID_ID;
      } else if (OB_FAIL(schema_guard.get_tablegroup_id(tenant_id, orig_object.get_tablegroup_name(), tablegroup_id))) {
        LOG_WARN("fail to get tablegroup_id", K(ret), K(tenant_id), K(orig_object));
      } else if (OB_INVALID_ID == tablegroup_id) {
        ret = OB_TABLEGROUP_NOT_EXIST;
        LOG_WARN("tablegroup not exist", K(ret), K(tenant_id), K(orig_object));
      }

      // database_id
      if (OB_SUCC(ret)) {
        const ObString& db_name =
            ObRecycleObject::DATABASE == type ? orig_object.get_object_name() : orig_object.get_database_name();
        if (db_name.empty()) {
          if (ObRecycleObject::DATABASE == type) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database not exist", K(ret), K(tenant_id), K(db_name));
          } else {
            database_id = OB_INVALID_ID;
          }
        } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, db_name, database_id))) {
          LOG_WARN("fail to get database id", K(ret), K(tenant_id), K(db_name));
        } else if (OB_INVALID_ID == database_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database not exist", K(ret), K(tenant_id), K(db_name));
        }
      }

      if (OB_SUCC(ret)) {
        ObRecycleObject* new_object = NULL;
        tmp_object.reset();
        tmp_object.set_tenant_id(tenant_id);
        tmp_object.set_database_id(database_id);
        tmp_object.set_tablegroup_id(tablegroup_id);
        tmp_object.set_table_id(table_id);
        tmp_object.set_type(type);
        const ObString& original_name =
            ObRecycleObject::INDEX == type ? index_orig_name : orig_object.get_original_name();
        if (OB_FAIL(tmp_object.set_object_name(orig_object.get_object_name()))) {
          LOG_WARN("fail to set object name", K(ret), K(orig_object));
        } else if (OB_FAIL(tmp_object.set_original_name(original_name))) {
          LOG_WARN("fail to set original name", K(ret), K(orig_object), K(original_name));
        } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, tmp_object, new_object))) {
          LOG_WARN("fail to alloc schema", K(ret), K(tmp_object));
        } else if (OB_FAIL(new_objects.push_back(new_object))) {
          LOG_WARN("fail to push back new object", K(ret), K(tmp_object));
        }
      }
    }
  }
  return ret;
}

int ObRestoreMeta::clear_all_recyclebin(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu",
                 OB_ALL_RECYCLEBIN_TNAME,
                 ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
    LOG_WARN("fail to assign_fmt", K(ret), K(tenant_id));
  } else if (OB_FAIL(conn_.execute_write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(tenant_id), K(sql));
  } else {
    // no need to check affected_rows
  }
  return ret;
}

int ObRestoreMeta::restore_partitions(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService* schema_service = restore_ctx_.schema_service_;
  obrpc::ObCommonRpcProxy* rs_rpc_proxy = restore_ctx_.rs_rpc_proxy_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service) || OB_ISNULL(rs_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service or rs_rpc_proxy is null ", K(ret), KP(schema_service), KP(rs_rpc_proxy));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else {
    // process pg
    if (OB_SUCC(ret)) {
      ObArray<const ObSimpleTablegroupSchema*> tablegroups;
      if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
        LOG_WARN("fail to get tablegroups", K(ret), K(tenant_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tablegroups.count(); i++) {
          const ObSimpleTablegroupSchema* tablegroup = tablegroups.at(i);
          if (OB_ISNULL(tablegroup)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablegroup is null", K(ret));
          } else if (!is_new_tablegroup_id(tablegroup->get_tablegroup_id())) {
            // skip
          } else if (tablegroup->has_self_partition()) {
            obrpc::ObRestorePartitionsArg arg;
            arg.schema_id_ = tablegroup->get_tablegroup_id();
            if (OB_FAIL(rs_rpc_proxy->restore_partitions(arg))) {
              LOG_WARN("fail to restore tablegroup partitions", K(ret), K(arg));
            } else {
              LOG_INFO(
                  "create tablegroup partitions success", K(ret), "tablegroup_id", tablegroup->get_tablegroup_id());
            }
          }
        }
      }
    }
    // process partition
    if (OB_SUCC(ret)) {
      ObArray<const ObSimpleTableSchemaV2*> tables;
      if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, tables))) {
        LOG_WARN("fail to get tables", K(ret), K(tenant_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
          const ObSimpleTableSchemaV2* table = tables.at(i);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table is null", K(ret));
          } else if (is_inner_table(table->get_table_id())) {
            // skip
          } else if (table->has_partition()) {
            obrpc::ObRestorePartitionsArg arg;
            arg.schema_id_ = table->get_table_id();
            if (OB_FAIL(rs_rpc_proxy->restore_partitions(arg))) {
              LOG_WARN("fail to restore table partitions", K(ret), K(arg));
            } else {
              LOG_INFO("create table partitions success", K(ret), "table_id", table->get_table_id());
            }
          } else {
            LOG_INFO("skip create table partitions", K(ret), "table_id", table->get_table_id());
          }
        }
      }
    }
  }
  return ret;
}

int ObRestoreMeta::generate_task()
{
  LOG_INFO("begin generate task");
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  PartitionRestoreTask task;
  ObArray<PartitionRestoreTask> tasks;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(restore_ctx_.schema_service_) || OB_ISNULL(restore_ctx_.sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service pointer", K(ret));
  } else if (OB_FAIL(restore_ctx_.schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_id(job_info_.tenant_name_, tenant_id))) {
    LOG_WARN("fail get tenant id", K(ret));
  } else {
    // process pg
    if (OB_SUCC(ret)) {
      ObArray<const ObTablegroupSchema*> tablegroup_schemas;
      if (OB_FAIL((schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroup_schemas)))) {
        LOG_WARN("fail to get tablegroup info", K(ret), K(tenant_id));
      }
      FOREACH_X(schema, tablegroup_schemas, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (OB_ISNULL(*schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected", K(ret));
        } else if (!is_new_tablegroup_id((*schema)->get_tablegroup_id())) {
          // skip
        } else if ((*schema)->has_self_partition()) {
          LOG_INFO("restore tablegroup schema",
              "name",
              (*schema)->get_tablegroup_name_str(),
              "part_cnt",
              (*schema)->get_all_part_num());
          bool check_dropped_schema = false;
          ObTablegroupPartitionKeyIter iter(**schema, check_dropped_schema);
          int64_t phy_part_id = OB_INVALID_ID;
          while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_id_v2(phy_part_id))) {
            task.tenant_id_ = tenant_id;
            task.tablegroup_id_ = (*schema)->get_tablegroup_id();
            task.partition_id_ = phy_part_id;
            task.job_id_ = job_info_.job_id_;
            task.status_ = RESTORE_INIT;
            task.start_time_ = 0;
            task.schema_id_pairs_.reuse();
            if (OB_FAIL(get_backup_tablegroup_id(task.tablegroup_id_, task.backup_tablegroup_id_))) {
              LOG_WARN("fail get backup tablegroup id", K(task), K(ret));
            } else {
              ObArray<uint64_t> table_ids;
              if (OB_FAIL(schema_guard.get_table_ids_in_tablegroup(tenant_id, task.tablegroup_id_, table_ids))) {
                LOG_WARN("fail to get table_ids", K(tenant_id), "tablegroup_id", task.tablegroup_id_);
              } else {
                for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i++) {
                  ObSchemaIdPair table_id_pair;
                  table_id_pair.schema_id_ = table_ids.at(i);
                  if (OB_FAIL(get_backup_table_id(table_id_pair.schema_id_, table_id_pair.backup_schema_id_))) {
                    LOG_WARN("fail get backup table id", K(table_id_pair), K(ret));
                  } else if (OB_FAIL(task.schema_id_pairs_.push_back(table_id_pair))) {
                    LOG_WARN("fail to push back table_id pair", K(table_id_pair), K(ret));
                  } else if (OB_FAIL(get_backup_index_id(table_id_pair.schema_id_, task.schema_id_pairs_))) {
                    LOG_WARN("fail get backup index id", K(task), K(ret));
                  }
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(tasks.push_back(task))) {
                LOG_WARN("fail push back task to partition", K(task), K(ret));
              }
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }

    // process standalone partition
    if (OB_SUCC(ret)) {
      ObArray<const ObSimpleTableSchemaV2*> table_schemas;
      if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
        LOG_WARN("fail get tables info", K(tenant_id), K(ret));
      }
      FOREACH_X(schema, table_schemas, OB_SUCC(ret) && OB_SUCC(check_stop()))
      {
        if (OB_ISNULL(*schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected", K(ret));
        } else if (is_inner_table((*schema)->get_table_id())) {
          // skip
        } else if ((*schema)->has_self_partition()) {
          LOG_INFO("restore table schema",
              "name",
              (*schema)->get_table_name_str(),
              "part_cnt",
              (*schema)->get_all_part_num());
          bool check_dropped_schema = false;
          ObTablePartitionKeyIter iter(**schema, check_dropped_schema);
          int64_t phy_part_id = OB_INVALID_ID;
          while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_id_v2(phy_part_id))) {
            task.tenant_id_ = tenant_id;
            task.table_id_ = (*schema)->get_table_id();
            task.partition_id_ = phy_part_id;
            task.job_id_ = job_info_.job_id_;
            task.status_ = RESTORE_INIT;
            task.start_time_ = 0;
            task.schema_id_pairs_.reuse();
            if ((*schema)->is_index_table() &&
                is_error_index_status((*schema)->get_index_status(), (*schema)->is_dropped_schema())) {
              LOG_INFO("skip restore error index", K(ret), K(tenant_id), "table_id", task.table_id_);
            } else if (OB_FAIL(get_backup_table_id(task.table_id_, task.backup_table_id_))) {
              LOG_WARN("fail get backup table id", K(task), K(ret));
            } else if (OB_FAIL(get_backup_index_id(task.table_id_, task.schema_id_pairs_))) {
              LOG_WARN("fail get backup table id", K(task), K(ret));
            } else if (OB_FAIL(tasks.push_back(task))) {
              LOG_WARN("fail push back task to partition", K(task), K(ret));
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObRestoreTableOperator restore_op;
      if (OB_FAIL(restore_op.init(restore_ctx_.sql_client_))) {
        LOG_WARN("fail init restore op", K(ret));
      } else if (tasks.count() > 0 && OB_FAIL(restore_op.insert_task(tasks))) {
        LOG_WARN("fail insert job and partitions", K(ret));
      } else if (OB_FAIL(restore_op.update_job_status(job_info_.job_id_, RESTORE_DOING))) {
        LOG_WARN("fail update job status as doing", K_(job_info), K(ret));
      }
    }
  }

  LOG_INFO("end generate task", K(ret));
  return ret;
}

int ObRestoreMeta::get_created_tablegroup_id(
    uint64_t tenant_id, const ObString& tg_name, uint64_t& created_tablegroup_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  // unused
  // bool is_index = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(restore_ctx_.schema_service_) || OB_ISNULL(restore_ctx_.sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service pointer", K(ret));
  } else if (OB_FAIL(restore_ctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_id(tenant_id, tg_name, created_tablegroup_id))) {
    LOG_WARN("fail get tablegroups info", K(tenant_id), K(tg_name), K(ret));
  } else if (OB_INVALID_ID == created_tablegroup_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(tenant_id), K(tg_name), K(created_tablegroup_id), K(ret));
  }
  return ret;
}

int ObRestoreMeta::get_created_table_id(
    uint64_t tenant_id, const ObString& db_name, const ObString& tb_name, uint64_t& created_table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_index = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(restore_ctx_.schema_service_) || OB_ISNULL(restore_ctx_.sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service pointer", K(ret));
  } else if (OB_FAIL(restore_ctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_id(
                 tenant_id, db_name, tb_name, is_index, ObSchemaGetterGuard::ALL_TYPES, created_table_id))) {
    LOG_WARN("fail get tables info", K(tenant_id), K(db_name), K(tb_name), K(ret));
  } else if (OB_INVALID_ID == created_table_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(tenant_id), K(db_name), K(is_index), K(tb_name), K(created_table_id), K(ret));
  }
  return ret;
}

int ObRestoreMeta::get_created_index_schema(
    uint64_t tenant_id, const ObString& db_name, const ObString& index_name, const ObSimpleTableSchemaV2*& index)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_index = true;
  uint64_t index_id = OB_INVALID_ID;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(restore_ctx_.schema_service_) || OB_ISNULL(restore_ctx_.sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service pointer", K(ret));
  } else if (OB_FAIL(restore_ctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_id(
                 tenant_id, db_name, index_name, is_index, ObSchemaGetterGuard::ALL_TYPES, index_id))) {
    LOG_WARN("fail get index id", K(tenant_id), K(db_name), K(index_name), K(ret));
  } else if (OB_INVALID_ID == index_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(tenant_id), K(db_name), K(index_name), K(index_id), K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_id, index))) {
    LOG_WARN("fail get index schema", K(tenant_id), K(db_name), K(index_name), K(ret));
  }
  return ret;
}

int ObRestoreMeta::get_backup_tablegroup_id(const uint64_t tablegroup_id, uint64_t& backup_tablegroup_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  FOREACH_X(p, tablegroup_id_pairs_, OB_SUCC(ret) && !found)
  {
    if (OB_ISNULL(p)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("p is null", K(ret));
    } else if (p->tablegroup_id_ == tablegroup_id) {
      backup_tablegroup_id = p->backup_tablegroup_id_;
      found = true;
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObRestoreMeta::get_backup_table_id(const uint64_t table_id, uint64_t& backup_table_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  FOREACH_X(p, table_id_pairs_, OB_SUCC(ret) && !found)
  {
    if (OB_ISNULL(p)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("p is null", K(ret));
    } else if (p->table_id_ == table_id) {
      backup_table_id = p->backup_table_id_;
      found = true;
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObRestoreMeta::get_backup_index_id(const uint64_t table_id, ObIArray<ObSchemaIdPair>& backup_index_ids)
{
  int ret = OB_SUCCESS;
  FOREACH_X(p, index_id_pairs_, OB_SUCC(ret))
  {
    if (OB_ISNULL(p)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("p is null", K(ret));
    } else if (p->table_id_ == table_id) {
      ObSchemaIdPair pair(p->index_id_, p->backup_index_id_);
      if (!pair.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index id", K(*p), K(pair), K(ret));
      } else if (OB_FAIL(backup_index_ids.push_back(pair))) {
        LOG_WARN("fail pushback data", K(*p), K(pair), K(ret));
      }
    }
  }
  return ret;
}
