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

#include "ob_sequence_migrator.h"

#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/ob_upgrade_utils.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
namespace rootserver {

ObSequenceMigrator::ObSequenceMigrator()
    : inited_(false), stopped_(false), migrate_(false), sql_proxy_(NULL), schema_service_(NULL)
{}

ObSequenceMigrator::~ObSequenceMigrator()
{}

int ObSequenceMigrator::init(ObMySQLProxy& sql_proxy, ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    stopped_ = false;
    migrate_ = false;
    inited_ = true;
  }
  return ret;
}

void ObSequenceMigrator::start()
{
  SpinWLockGuard guard(rwlock_);
  stopped_ = false;
}

// run job 'sequence_migration' use DDL thread to prevent single server concurrent
// Called when RS switch leader to ensure that the running JOB is stopped
int ObSequenceMigrator::stop()
{
  int ret = OB_SUCCESS;
  const uint64_t WAIT_US = 100 * 1000L;            // 100ms
  const uint64_t MAX_WAIT_US = 10 * 1000 * 1000L;  // 10s
  const int64_t start = ObTimeUtility::current_time();
  SpinWLockGuard guard(rwlock_);
  stopped_ = true;
  while (OB_SUCC(ret)) {
    if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
      ret = OB_TIMEOUT;
      LOG_WARN("use too much time", K(ret), "cost_us", ObTimeUtility::current_time() - start);
    } else if (!migrate_) {
      break;
    } else {
      usleep(WAIT_US);
    }
  }
  return ret;
}

int ObSequenceMigrator::check_stop()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("migrator should stopped", K(ret));
  }
  return ret;
}

int ObSequenceMigrator::set_migrate_mark()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_ || migrate_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't run job at the same time", K(ret));
  } else {
    migrate_ = true;
  }
  return ret;
}

// ensure single server not concurrent
int ObSequenceMigrator::run()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(set_migrate_mark())) {
    LOG_WARN("run multi job in the same time not allowed", K(ret));
  } else {
    int64_t job_id = OB_INVALID_ID;
    bool can_run_job = false;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_MIGRATE_SEQUENCE_TABLE;
    if (OB_FAIL(ObUpgradeUtils::can_run_upgrade_job(job_type, can_run_job))) {
      LOG_WARN("fail to check if can run upgrade job now", K(ret), K(job_type));
    } else if (!can_run_job) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("no job exist or success job exist", K(ret));
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy_ is null", K(ret));
    } else if (OB_FAIL(check_stop())) {
      LOG_WARN("migrator should stopped", K(ret));
    } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
      LOG_WARN("fail to create rs job", K(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_id is invalid", K(ret), K(job_id));
    } else if (OB_FAIL(migrate())) {
      LOG_WARN("fail to do migrate", K(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (job_id > 0) {
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
        ret = OB_FAIL(ret) ? ret : tmp_ret;
      }
    }
    // no need lock, because single-machine concurrency is prevented in the process
    migrate_ = false;
  }
  LOG_INFO("migrate sequence finish", K(ret));
  return ret;
}

int ObSequenceMigrator::create_tables()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  int64_t job_id = OB_INVALID_ID;
  ObRsJobType job_type = ObRsJobType::JOB_TYPE_MIGRATE_SEQUENCE_TABLE;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!enable_ddl || !enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't create sys table now", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", K(ret));
  } else if (OB_FAIL(ObUpgradeUtils::create_tenant_tables(*schema_service_, OB_ALL_SEQUENCE_V2_TID))) {
    LOG_WARN("fail to create tenant tables", K(ret));
  } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
    LOG_WARN("fail to create rs job", K(ret));
  } else if (job_id <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job_id is invalid", K(ret), K(job_id));
  } else if (OB_FAIL(RS_JOB_COMPLETE(job_id, OB_CANCELED, *sql_proxy_))) {
    LOG_WARN("fail to complete job", K(ret), K(job_id));
  }
  return ret;
}

int ObSequenceMigrator::migrate()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("migrator should stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else {
    for (int64_t i = tenant_ids.size() - 1; i >= 0 && OB_SUCC(ret); i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(do_migrate(tenant_id))) {
        LOG_WARN("do migrate failed", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObSequenceMigrator::do_migrate(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  do {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ObMySQLTransaction sys_trans;
    if (OB_FAIL(check_stop())) {
      LOG_WARN("migrator should stopped", K(ret));
    } else if (OB_FAIL(sys_trans.start(sql_proxy_))) {
      LOG_WARN("failed to start trans", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, sequence_key, column_id,"
                                      " sequence_value, sync_value"
                                      " FROM %s WHERE tenant_id = %lu and migrated != 1 LIMIT 1 FOR UPDATE",
                   OB_ALL_SEQUENCE_TNAME,
                   tenant_id))) {
      LOG_WARN("sql assign_fmt failed", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        ObMySQLResult* result = NULL;
        if (OB_FAIL(sys_trans.read(res, sql.ptr()))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("result next failed", K(ret));
          } else {
            LOG_INFO("all sequence of tenant have been migrated, will break", K(tenant_id), K(ret));
          }
        } else {
          int64_t tenant_id = 0;
          int64_t sequence_key = 0;
          int64_t column_id = 0;
          uint64_t sequence_value = 0;
          uint64_t sync_value = 0;
          if (OB_FAIL(result->get_int(0l, tenant_id))) {
            LOG_WARN("fail to get int_value.", K(ret));
          } else if (OB_FAIL(result->get_int(1l, sequence_key))) {
            LOG_WARN("fail to get int_value.", K(ret));
          } else if (OB_FAIL(result->get_int(2l, column_id))) {
            LOG_WARN("fail to get int_value.", K(ret));
          } else if (OB_FAIL(result->get_uint(3l, sequence_value))) {
            LOG_WARN("fail to get int_value.", K(ret));
          } else if (OB_FAIL(result->get_uint(4l, sync_value))) {
            LOG_WARN("fail to get int_value.", K(ret));
          }
          LOG_INFO(
              "dump sequence", K(ret), K(tenant_id), K(sequence_key), K(column_id), K(sequence_value), K(sync_value));

          if (OB_SUCC(ret)) {
            ObMySQLTransaction tenant_trans;
            if (OB_FAIL(check_stop())) {
              LOG_WARN("migrator should stopped", K(ret));
            } else if (OB_FAIL(tenant_trans.start(sql_proxy_))) {
              LOG_WARN("failed to start trans", K(ret));
            } else {
              sql.reset();
              ObSqlString values;
              if (OB_FAIL(sql.append_fmt(
                      "REPLACE /*+log_level('sql.*:debug, common.*:info')*/ INTO %s (", OB_ALL_SEQUENCE_V2_TNAME))) {
                LOG_WARN("append table name failed, ", K(ret));
              } else {
                SQL_COL_APPEND_VALUE(sql, values, tenant_id, "tenant_id", "%lu");
                SQL_COL_APPEND_VALUE(sql, values, sequence_key, "sequence_key", "%lu");
                SQL_COL_APPEND_VALUE(sql, values, column_id, "column_id", "%lu");
                SQL_COL_APPEND_VALUE(sql, values, sequence_value, "sequence_value", "%lu");
                SQL_COL_APPEND_VALUE(sql, values, sync_value, "sync_value", "%lu");
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)", static_cast<int32_t>(values.length()), values.ptr()))) {
                  LOG_WARN("append sql failed, ", K(ret));
                } else if (OB_FAIL(tenant_trans.write(tenant_id, sql.ptr(), affected_rows))) {
                  LOG_WARN("fail to execute. ", "sql", sql.ptr(), K(ret));
                } else if (!is_single_row(affected_rows)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(sql));
                }
              }
            }
            if (tenant_trans.is_started()) {
              int temp_ret = OB_SUCCESS;
              if (OB_SUCCESS != (temp_ret = tenant_trans.end(OB_SUCC(ret)))) {
                LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(temp_ret));
                ret = (OB_SUCC(ret)) ? temp_ret : ret;
              }
            }
            // make sure %res destructed before execute other sql in the same transaction
            res.~ReadResult();
            if (OB_SUCC(ret)) {
              sql.reset();
              int64_t affected_rows = 0;
              if (OB_FAIL(check_stop())) {
                LOG_WARN("migrator should stopped", K(ret));
              } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET migrated = 1 WHERE"
                                                " tenant_id = %lu AND sequence_key = %lu AND column_id = %lu",
                             OB_ALL_SEQUENCE_TNAME,
                             tenant_id,
                             sequence_key,
                             column_id))) {
                LOG_WARN("assign sql failed", K(ret));
              } else if (OB_FAIL(sys_trans.write(sql.ptr(), affected_rows))) {
                LOG_WARN("execute sql failed", K(ret), K(sql));
              } else if (!is_single_row(affected_rows)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(sql));
              }
            }
          }
        }
      }
    }
    if (sys_trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = sys_trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
  } while (OB_SUCC(ret));

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
