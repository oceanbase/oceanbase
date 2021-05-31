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

#include "lib/string/ob_sql_string.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_upgrade_utils.h"
#include "share/ob_global_stat_proxy.h"
#include "share/schema/ob_part_mgr_util.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_schema_split_executor.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace rootserver {

int64_t ObSchemaSplitTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObSchemaSplitTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), K(ret));
  } else {
    task = new (buf) ObSchemaSplitTask(*schema_split_executor_, type_);
  }
  return task;
}

int ObSchemaSplitTask::process()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to do execute schema split task", K(start));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_split_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_split_executor_ is null", K(ret));
  } else if (OB_FAIL(schema_split_executor_->execute(type_))) {
    LOG_WARN("fail to execute schema split", K(ret));
  }
  LOG_INFO("[UPGRADE] finish execute schema split", K(ret), K_(type), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

ObSchemaSplitExecutor::ObSchemaSplitExecutor()
    : inited_(false),
      stopped_(false),
      execute_(false),
      rwlock_(),
      sql_proxy_(NULL),
      rpc_proxy_(NULL),
      server_mgr_(NULL),
      schema_service_(NULL)
{}

int ObSchemaSplitExecutor::init(share::schema::ObMultiVersionSchemaService& schema_service,
    common::ObMySQLProxy* sql_proxy, obrpc::ObSrvRpcProxy& rpc_proxy, ObServerManager& server_mgr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can't init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = sql_proxy;
    server_mgr_ = &server_mgr;
    rpc_proxy_ = &rpc_proxy;
    stopped_ = false;
    execute_ = false;
    inited_ = true;
  }
  return ret;
}

void ObSchemaSplitExecutor::start()
{
  SpinWLockGuard guard(rwlock_);
  stopped_ = false;
}

int ObSchemaSplitExecutor::stop()
{
  int ret = OB_SUCCESS;
  const uint64_t WAIT_US = 100 * 1000L;            // 100ms
  const uint64_t MAX_WAIT_US = 10 * 1000 * 1000L;  // 10s
  const int64_t start = ObTimeUtility::current_time();
  {
    SpinWLockGuard guard(rwlock_);
    stopped_ = true;
  }
  while (OB_SUCC(ret)) {
    if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
      ret = OB_TIMEOUT;
      LOG_WARN("use too much time", K(ret), "cost_us", ObTimeUtility::current_time() - start);
    } else if (!execute_) {
      break;
    } else {
      usleep(WAIT_US);
    }
  }
  return ret;
}

int ObSchemaSplitExecutor::check_stop()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("executor should stopped", K(ret));
  }
  return ret;
}

int ObSchemaSplitExecutor::set_execute_mark()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_ || execute_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't run job at the same time", K(ret));
  } else {
    execute_ = true;
  }
  return ret;
}

int ObSchemaSplitExecutor::execute(ObRsJobType job_type)
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(set_execute_mark())) {
    LOG_WARN("fail to execute schema split", K(ret));
  } else {
    int64_t job_id = OB_INVALID_ID;
    bool can_run_job = false;
    if (ObRsJobType::JOB_TYPE_SCHEMA_SPLIT != job_type && ObRsJobType::JOB_TYPE_SCHEMA_SPLIT_V2 != job_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job type", K(job_type));
    } else if (OB_FAIL(ObUpgradeUtils::can_run_upgrade_job(job_type, can_run_job))) {
      LOG_WARN("fail to check if can run upgrade job now", K(ret), K(job_type));
    } else if (!can_run_job) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("no job exist or success job exist", K(ret));
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy_ is null", K(ret));
    } else if (OB_FAIL(check_stop())) {
      LOG_WARN("executor should stopped", K(ret));
    } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
      LOG_WARN("fail to create rs job", K(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_id is invalid", K(ret), K(job_id));
    } else if (ObRsJobType::JOB_TYPE_SCHEMA_SPLIT == job_type) {
      if (OB_FAIL(do_execute())) {
        LOG_WARN("fail to execute", K(ret));
      }
    } else {
      if (OB_FAIL(do_execute_v2())) {
        LOG_WARN("fail to execute", K(ret));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (job_id > 0) {
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
        ret = OB_FAIL(ret) ? ret : tmp_ret;
      }
    }
    // already use lock by set_execute_mark(), it's no need to use lock here.
    execute_ = false;
  }
  return ret;
}

// ver 2.2.60:
// migrate data from __all_table/__all_table_history to __all_table_v2/__all_table_v2_history
int ObSchemaSplitExecutor::do_execute_v2()
{
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (enable_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("split schema while enable_ddl is on not allowed", K(ret), K(enable_ddl));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
      LOG_WARN("get tenant ids failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObTableSchema all_table_v2_schema;
    ObTableSchema all_table_v2_history_schema;
    const schema_create_func* creator_ptr_arrays[] = {core_table_schema_creators, sys_table_schema_creators};
    int64_t find_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
      for (const schema_create_func* creator_ptr = creator_ptr_arrays[i]; OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        ObTableSchema table_schema;
        uint64_t table_id = OB_INVALID_ID;
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor should stopped", K(ret));
        } else if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("construct_schema failed", K(table_schema), K(ret));
        } else if (FALSE_IT(table_id = table_schema.get_table_id())) {
        } else if (OB_ALL_TABLE_V2_TID == extract_pure_id(table_id)) {
          if (OB_FAIL(all_table_v2_schema.assign(table_schema))) {
            LOG_WARN("fail to assign table_schema", K(ret), K(table_schema));
          } else {
            find_cnt++;
          }
        } else if (OB_ALL_TABLE_V2_HISTORY_TID == extract_pure_id(table_id)) {
          if (OB_FAIL(all_table_v2_history_schema.assign(table_schema))) {
            LOG_WARN("fail to assign table_schema", K(ret), K(table_schema));
          } else {
            find_cnt++;
          }
        }
      }
    }
    if (OB_SUCCESS && 2 != find_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema cnt should be 2", K(ret), K(all_table_v2_schema), K(all_table_v2_history_schema));
    }
    bool is_standby_cluster = GCTX.is_standby_cluster();
    for (int64_t i = tenant_ids.size() - 1; i >= 0 && OB_SUCC(ret); i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      bool need_retry = false;
      do {
        enable_ddl = GCONF.enable_ddl;
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor should stopped", K(ret));
        } else if (is_standby_cluster && OB_SYS_TENANT_ID != tenant_id) {
          LOG_INFO("skip split normal tenant schema in standby cluster", K(ret), K(tenant_id));
        } else if (enable_ddl) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("split schema while enable_ddl is on not allowed", K(ret), K(enable_ddl));
        } else if (OB_FAIL(check_schema_sync())) {
          LOG_WARN("fail to check schema sync", K(ret));
        } else {
          // step 0: try migrate __all_core_table
          if (OB_SUCC(ret) && OB_SYS_TENANT_ID == tenant_id) {
            LOG_INFO("[UPGRADE] try migrate core table schemas", K(ret), K(tenant_id));
            if (OB_FAIL(migrate_core_table_schema())) {
              LOG_WARN("migrate core table schema failed", K(ret));
            }
            LOG_INFO("[UPGRADE] migrate core table schemas finish", K(ret), K(tenant_id));
          }
          // step 1: try migrate __all_table_v2
          LOG_INFO("[UPGRADE] start to migrate and check by table",
              K(ret),
              K(tenant_id),
              "table_name",
              OB_ALL_TABLE_V2_TNAME);
          ObTableSchemaSpliter* iter = NULL;
          ObAllTableV2SchemaSpliter all_table_v2_spliter(*this, *schema_service_, sql_proxy_, all_table_v2_schema);
          iter = &all_table_v2_spliter;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(iter->init())) {
            LOG_WARN("fail to init schema spliter", K(ret), K(tenant_id), "table_name", OB_ALL_TABLE_V2_TNAME);
          } else if (OB_FAIL(iter->process(tenant_id))) {
            LOG_WARN("fail to execute schema split task", K(ret), K(tenant_id), "table_name", OB_ALL_TABLE_V2_TNAME);
          }
          LOG_INFO(
              "[UPGRADE] migrate and check by table finish", K(ret), K(tenant_id), "table_name", OB_ALL_TABLE_V2_TNAME);
          // step 2: try migrate __all_table_v2_history
          LOG_INFO("[UPGRADE] start to migrate and check by table",
              K(ret),
              K(tenant_id),
              "table_name",
              OB_ALL_TABLE_V2_HISTORY_TNAME);
          ObAllTableV2HistorySchemaSpliter all_table_v2_history_spliter(
              *this, *schema_service_, sql_proxy_, all_table_v2_history_schema);
          iter = &all_table_v2_history_spliter;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(iter->init())) {
            LOG_WARN("fail to init schema spliter", K(ret), K(tenant_id), "table_name", OB_ALL_TABLE_V2_HISTORY_TNAME);
          } else if (OB_FAIL(iter->process(tenant_id))) {
            LOG_WARN(
                "fail to execute schema split task", K(ret), K(tenant_id), "table_name", OB_ALL_TABLE_V2_HISTORY_TNAME);
          }
          LOG_INFO("[UPGRADE] migrate and check by table finish",
              K(ret),
              K(tenant_id),
              "table_name",
              OB_ALL_TABLE_V2_HISTORY_TNAME);
          // step 3: mark tenant split finish
          if (OB_SUCC(ret) && OB_FAIL(finish_schema_split_v2(tenant_id))) {
            if (OB_EAGAIN == ret && OB_SYS_TENANT_ID == tenant_id && is_standby_cluster) {
              // Because schema version of system tenant is not sync,
              // it may cause error when execute ddl on system tenant is standby cluster.
              // To avoid error, we retry here.
              ret = OB_SUCCESS;
              need_retry = true;
              LOG_WARN("fail to finish schema split, need retry", K(ret), K(tenant_id));
              usleep(1 * 1000 * 1000L);  // 1s
            } else {
              LOG_WARN("fail to finish schema split", K(ret), K(tenant_id));
            }
          } else {
            need_retry = false;
            LOG_WARN("[UPGRRADE] finish schema split", K(ret), K(tenant_id));
          }
        }
      } while (OB_SUCC(ret) && need_retry);
    }
  }
  return ret;
}

int ObSchemaSplitExecutor::finish_schema_split_v2(const uint64_t tenant_id)
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start finish schema split");
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  ObAddr rs_addr;
  obrpc::ObFinishSchemaSplitArg arg;
  arg.exec_tenant_id_ = tenant_id;
  arg.tenant_id_ = tenant_id;
  arg.type_ = ObRsJobType::JOB_TYPE_SCHEMA_SPLIT_V2;
  bool pass = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (enable_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", K(ret), K(enable_ddl));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service_->check_tenant_can_use_new_table(tenant_id, pass))) {
    LOG_WARN("fail to check tenant can use new table", K(ret), K(tenant_id));
  } else if (pass) {
    // do nothing
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get rootservice address failed", K(ret));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).finish_schema_split(arg))) {
    LOG_WARN("fail to finish schema split", K(ret), K(arg));
  }
  LOG_INFO("[UPGRADE] finish schema split", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

// create tenant while execute schema split job is not supported
int ObSchemaSplitExecutor::check_schema_sync(obrpc::ObTenantSchemaVersions& primary_schema_versions,
    obrpc::ObTenantSchemaVersions& standby_schema_versions, bool& schema_sync)
{
  int ret = OB_SUCCESS;
  int64_t primary_cnt = primary_schema_versions.tenant_schema_versions_.count();
  int64_t standby_cnt = standby_schema_versions.tenant_schema_versions_.count();
  if (primary_cnt <= 0 || standby_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cnt", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else {
    schema_sync = true;
    for (int64_t i = 0; schema_sync && OB_SUCC(ret) && i < primary_cnt; i++) {
      bool find = false;
      TenantIdAndSchemaVersion& primary = primary_schema_versions.tenant_schema_versions_.at(i);
      // check normal tenant only
      if (OB_SYS_TENANT_ID == primary.tenant_id_) {
        continue;
      } else {
        for (int64_t j = 0; !find && OB_SUCC(ret) && j < standby_cnt; j++) {
          TenantIdAndSchemaVersion& standby = standby_schema_versions.tenant_schema_versions_.at(j);
          if (OB_FAIL(check_stop())) {
            LOG_WARN("executor should stopped", K(ret));
          } else if (primary.tenant_id_ == standby.tenant_id_) {
            find = true;
            schema_sync = (primary.schema_version_ <= standby.schema_version_);
            LOG_INFO("check if tenant schema is sync", K(ret), K(primary), K(standby), K(schema_sync));
          }
        }
        if (OB_SUCC(ret) && !find) {
          schema_sync = false;
        }
      }
    }
  }
  return ret;
}

// ver 2.2.0/2.2.1: execute schema split
int ObSchemaSplitExecutor::do_execute()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  }
  bool has_done = false;
  // step 0. pre_check
  if (OB_SUCC(ret) && OB_FAIL(pre_check(has_done))) {
    LOG_WARN("fail to execute pre_check", K(ret));
  }
  if (OB_SUCC(ret) && !has_done) {
    // step 1. enable ddl (enable_sys_table_ddl,enable_ddl)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_enable_sys_table_ddl(true))) {
        LOG_WARN("fail to set sys parameter", K(ret));
      } else if (OB_FAIL(set_enable_ddl(true))) {
        LOG_WARN("fail to set sys parameter", K(ret));
      }
    }
    // step 2. create tenant space tables
    if (OB_SUCC(ret) && OB_FAIL(create_tables())) {
      LOG_WARN("fail to create tables", K(ret));
    }
    //    // step 3: while upgrade from ver 1.4.x to ver 2.2.0
    //    // trigger __all_tenant_partition_meta_table report tasks
    //    // before schema split to reduce the total upgrade job cost.
    //    bool migrate = false;
    //    if (OB_SUCC(ret)) {
    //      if (OB_ISNULL(migrator_)) {
    //        LOG_WARN("migrator is null", K(ret));
    //      } else if (OB_FAIL(migrator_->set_execute_mark())) {
    //        LOG_WARN("fail to set execute mark", K(ret));
    //      } else {
    //        migrate = true;
    //        if (OB_FAIL(migrator_->report())) {
    //          LOG_WARN("fail to trigger report replica", K(ret));
    //        }
    //      }
    //    }
    // step 4. disable ddl (enable_sys_table_ddl, enable_ddl)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_enable_sys_table_ddl(false))) {
        LOG_WARN("fail to set sys parameter", K(ret));
      } else if (OB_FAIL(set_enable_ddl(false))) {
        LOG_WARN("fail to set sys parameter", K(ret));
      }
    }
    // step 4. check schema sync
    if (OB_SUCC(ret) && OB_FAIL(check_schema_sync())) {
      LOG_WARN("fail to check schema sync", K(ret));
    }
    // step 5. migrate & check
    if (OB_SUCC(ret) && OB_FAIL(migrate_and_check())) {
      LOG_WARN("fail to migrate and check", K(ret));
    }
    //    // step 7. check report finish
    //    if (OB_SUCC(ret)) {
    //      if (OB_ISNULL(migrator_)) {
    //        LOG_WARN("migrator is null", K(ret));
    //      } else if (OB_FAIL(migrator_->check_report_finish())) {
    //        LOG_WARN("fail to check report finish", K(ret));
    //      }
    //    }
    //    // reset execute mark
    //    if (OB_NOT_NULL(migrator_) && migrate) {
    //      migrator_->unset_execute_mark();
    //    }
  }
  // step 6. finish schema split
  if (OB_SUCC(ret) && OB_FAIL(finish_schema_split())) {
    LOG_WARN("fail to finish schema split", K(ret));
  }
  // step 7. check after schema split
  if (OB_SUCC(ret) && OB_FAIL(check_after_schema_split())) {
    LOG_WARN("fail to check after schema split", K(ret));
  }
  return ret;
}

int ObSchemaSplitExecutor::migrate_core_table_schema()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.append_fmt("REPLACE INTO %s (table_name, row_id, column_name, column_value) "
                               "SELECT '%s' as table_name, row_id, column_name, column_value "
                               "FROM %s where table_name = '%s'",
            OB_ALL_CORE_TABLE_TNAME,
            OB_ALL_TABLE_V2_TNAME,
            OB_ALL_CORE_TABLE_TNAME,
            OB_ALL_TABLE_TNAME))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (affected_rows <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row cnt", K(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObSchemaSplitExecutor::set_enable_ddl(bool value)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (sql.append_fmt("ALTER SYSTEM SET enable_ddl = %s", value ? "True" : "False")) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t start = ObTimeUtility::current_time();
    const uint64_t MAX_WAIT_US = 20 * 1000 * 1000L;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
        ret = OB_TIMEOUT;
        LOG_WARN(
            "[UPGRADE] sync enable_ddl use too much time", K(ret), "cost_us", ObTimeUtility::current_time() - start);
      } else if (value == GCONF.enable_ddl) {
        LOG_INFO("[UPGRADE] sync enable_ddl success", K(ret), "cost_us", ObTimeUtility::current_time() - start);
        break;
      }
    }
  }
  return ret;
}

int ObSchemaSplitExecutor::set_enable_sys_table_ddl(bool value)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (sql.append_fmt("ALTER SYSTEM SET enable_sys_table_ddl = %s", value ? "True" : "False")) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t start = ObTimeUtility::current_time();
    const uint64_t MAX_WAIT_US = 20 * 1000 * 1000L;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stopped", K(ret));
      } else if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
        ret = OB_TIMEOUT;
        LOG_WARN("[UPGRADE] sync enable_sys_table_ddl use too much time",
            K(ret),
            "cost_us",
            ObTimeUtility::current_time() - start);
      } else if (value == GCONF.enable_sys_table_ddl) {
        LOG_INFO(
            "[UPGRADE] sync enable_sys_table_ddl success", K(ret), "cost_us", ObTimeUtility::current_time() - start);
        break;
      }
    }
  }
  return ret;
}

// if cluster has already finished schema split, it's no need to run schema split job.
int ObSchemaSplitExecutor::pre_check(bool& has_done)
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] STEP 0. start to pre_check", K(start));
  int ret = OB_SUCCESS;
  has_done = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (GCTX.is_schema_splited()) {
    has_done = true;
    LOG_INFO("schema split task has done, do nothing", K(ret));
  } else {
    int64_t version_in_core_table = OB_INVALID_VERSION;
    int64_t version_in_ddl_table = OB_INVALID_VERSION;
    if (OB_ISNULL(GCTX.root_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rs is null", K(ret));
    } else if (OB_FAIL(GCTX.root_service_->get_schema_split_version(version_in_core_table, version_in_ddl_table))) {
      LOG_WARN("fail to check schema split", K(ret));
    } else if (version_in_ddl_table < 0 && version_in_core_table < 0) {
      has_done = false;
      LOG_INFO(
          "schema split task not finish, need continue", K(ret), K(version_in_ddl_table), K(version_in_core_table));
    } else if (version_in_ddl_table >= 0 && version_in_core_table >= 0) {
      if (version_in_ddl_table != version_in_core_table) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("version not equal", K(ret), K(version_in_ddl_table), K(version_in_core_table));
      } else {
        has_done = true;
        LOG_INFO("set split_schema_version in memory, and schema split task has done",
            K(ret),
            K(version_in_ddl_table),
            K(version_in_core_table));
      }
    } else if (version_in_ddl_table >= 0 && version_in_core_table < 0) {
      has_done = true;
      LOG_INFO("set split_schema_verison in table, and schema split task has done",
          K(ret),
          K(version_in_ddl_table),
          K(version_in_core_table));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("version_in_ddl_table invalid", K(ret), K(version_in_ddl_table), K(version_in_core_table));
    }
  }
  LOG_INFO("[UPGRADE] STEP 0. pre_check finish",
      K(ret),
      "has_done",
      static_cast<int64_t>(has_done),
      "cost_us",
      ObTimeUtility::current_time() - start);
  return ret;
}

int ObSchemaSplitExecutor::create_tables()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] STEP 1. start to create tenant space tables", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  ObArray<uint64_t> tenant_ids;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!enable_ddl || !enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't create sys table now", K(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else {
    ObTableSchema table_schema;
    const schema_create_func* creator_ptr_arrays[] = {core_table_schema_creators, sys_table_schema_creators};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
      for (const schema_create_func* creator_ptr = creator_ptr_arrays[i]; OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        table_schema.reset();
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor should stopped", K(ret));
        } else if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("construct_schema failed", K(table_schema), K(ret));
        } else if (!ObSysTableChecker::is_tenant_table_in_version_2200(table_schema.get_table_id())) {
          // skip
        } else if (tenant_ids.size() > 0 && OB_SYS_TENANT_ID != tenant_ids.at(0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fisrt tenant should be sys_tenant", K(ret), K(tenant_ids.at(0)));
        } else {
          bool allow_sys_create_table = true;
          bool in_sync = false;
          for (int64_t i = tenant_ids.size() - 1; i >= 0 && OB_SUCC(ret); i--) {
            if (OB_FAIL(check_stop())) {
              LOG_WARN("executor should stopped", K(ret));
            } else if (OB_SYS_TENANT_ID == tenant_ids.at(i)) {
              // skip
            } else if (OB_FAIL(ObUpgradeUtils::create_tenant_table(
                           tenant_ids.at(i), table_schema, in_sync, allow_sys_create_table))) {
              LOG_WARN("create new tenant table failed", K(ret), "tenant_id", tenant_ids.at(i));
            }
          }
        }
      }
    }
  }
  LOG_INFO(
      "[UPGRADE] STEP 1. create tenant space tables finish", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObSchemaSplitExecutor::migrate_and_check()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] STEP 2. start to migrate and check table", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", K(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else {
    ObTableSchema table_schema;
    ObTableSchema all_ddl_operation_schema;
    const schema_create_func* creator_ptr_arrays[] = {core_table_schema_creators, sys_table_schema_creators};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
      for (const schema_create_func* creator_ptr = creator_ptr_arrays[i]; OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        table_schema.reset();
        uint64_t table_id = OB_INVALID_ID;
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor should stopped", K(ret));
        } else if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("construct_schema failed", K(table_schema), K(ret));
        } else if (FALSE_IT(table_id = table_schema.get_table_id())) {
        } else if (ObSysTableChecker::is_tenant_table_in_version_2200(table_id) ||
                   OB_ALL_SEQUENCE_V2_TID == extract_pure_id(table_id) ||
                   OB_ALL_TENANT_GC_PARTITION_INFO_TID == extract_pure_id(table_id)) {
          LOG_INFO(
              "[UPGRADE] start to migrate and check by table", K(ret), "table_name", table_schema.get_table_name());
          // extra upgrade action for __all_ddl_operation
          if (OB_ALL_DDL_OPERATION_TID == extract_pure_id(table_id)) {
            if (OB_FAIL(all_ddl_operation_schema.assign(table_schema))) {
              LOG_WARN("fail to assign schema", K(ret));
            }
          }
          int64_t end = ObTimeUtility::current_time();
          if (OB_SUCC(ret)) {
            switch (extract_pure_id(table_id)) {
#define MIGRATE_TABLE_BEFORE_2200_EXECUTE
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef MIGRATE_TABLE_BEFORE_2200_EXECUTE
              case OB_ALL_SEQUENCE_V2_TID: {
                ObAllSequenceV2SchemaSpliter schema_spliter(*this, *schema_service_, sql_proxy_, table_schema);
                ObTableSchemaSpliter* iter = &schema_spliter;
                if (OB_FAIL(iter->init())) {
                  LOG_WARN("fail to init schema spliter", K(ret), "table_id", table_schema.get_table_id());
                } else if (OB_FAIL(iter->process())) {
                  LOG_WARN("fail to execute schema split task", K(ret), "table_id", table_schema.get_table_id());
                } else {
                  LOG_INFO("execute schema split task success", K(ret), "table_id", table_schema.get_table_id());
                }
                break;
              }
              case OB_ALL_TENANT_GC_PARTITION_INFO_TID: {
                ObAllTenantGcPartitionInfoSchemaSpliter schema_spliter(
                    *this, *schema_service_, sql_proxy_, table_schema);
                ObTableSchemaSpliter* iter = &schema_spliter;
                if (OB_FAIL(iter->init())) {
                  LOG_WARN("fail to init schema spliter", K(ret), "table_id", table_schema.get_table_id());
                } else if (OB_FAIL(iter->process())) {
                  LOG_WARN("fail to execute schema split task", K(ret), "table_id", table_schema.get_table_id());
                } else {
                  LOG_INFO("execute schema split task success", K(ret), "table_id", table_schema.get_table_id());
                }
                break;
              }
              default: {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid table_id", K(ret), K(table_id));
              }
            }
            LOG_INFO("[UPGRADE] migrate and check by table finish",
                K(ret),
                "table_name",
                table_schema.get_table_name(),
                "cost_us",
                ObTimeUtility::current_time() - end);
          }
        }
      }
    }
    // extra upgrade action for __all_ddl_operation
    if (OB_SUCC(ret)) {
      LOG_INFO("[UPGRADE] start to migrate and check by table",
          K(ret),
          "table_name",
          all_ddl_operation_schema.get_table_name());
      int64_t end = ObTimeUtility::current_time();
      ObSysVarDDLOperationSchemaSpliter schema_spliter(*this, *schema_service_, sql_proxy_, all_ddl_operation_schema);
      ObTableSchemaSpliter* iter = &schema_spliter;
      if (OB_FAIL(iter->init())) {
        LOG_WARN("fail to init schema spliter", K(ret), "table_id", all_ddl_operation_schema.get_table_id());
      } else if (OB_FAIL(iter->process())) {
        LOG_WARN(
            "fail to execute special schema split task", K(ret), "table_id", all_ddl_operation_schema.get_table_id());
      } else {
        LOG_INFO(
            "execute special schema split task success", K(ret), "table_id", all_ddl_operation_schema.get_table_id());
      }
      LOG_INFO("[UPGRADE] migrate and check by table finish",
          K(ret),
          "table_name",
          all_ddl_operation_schema.get_table_name(),
          "cost_us",
          ObTimeUtility::current_time() - end);
    }
  }
  LOG_INFO(
      "[UPGRADE] STEP 2. migrate and check table finish", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

/*
 * this function is reentrant.
 * step 1. write ddl operation in sys tenant's __all_ddl_operation
 * step 2. write split_schema_version in __all_core_table
 * step 3. set GCTX.split_schema_version_
 * step 4. publish_schema
 */
int ObSchemaSplitExecutor::finish_schema_split()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] STEP 3. start to finish schema split", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  ObAddr rs_addr;
  obrpc::ObFinishSchemaSplitArg arg;
  arg.type_ = ObRsJobType::JOB_TYPE_SCHEMA_SPLIT;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now",
        K(ret),
        "enable_ddl",
        static_cast<int64_t>(enable_ddl),
        "enable_sys_table_ddl",
        static_cast<int64_t>(enable_sys_table_ddl));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", K(ret));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get rootservice address failed", K(ret));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).finish_schema_split(arg))) {
    LOG_WARN("fail to finish schema split", K(ret), K(arg));
  }
  LOG_INFO("[UPGRADE] STEP 3. finish schema split success",
      K(ret),
      "split_schema_version",
      GCTX.split_schema_version_,
      "cost_us",
      ObTimeUtility::current_time() - start);
  return ret;
}

/*
 * step 1. check all server has refresh schema in new mode
 * step 2. enable_ddl = on
 */
int ObSchemaSplitExecutor::check_after_schema_split()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] STEP 4. start to check after schema split", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  int64_t split_schema_version = GCTX.split_schema_version_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", K(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", K(ret));
  } else if (split_schema_version < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split schema version should be valid", K(ret), K(split_schema_version));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (OB_FAIL(check_all_server())) {
    LOG_WARN("fail to check all server", K(ret));
  } else if (OB_FAIL(set_enable_ddl(true))) {
    LOG_WARN("fail to enable ddl", K(ret));
  }
  LOG_INFO(
      "[UPGRADE] STEP 4. check after schema split finish", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObSchemaSplitExecutor::check_all_server()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to check all server", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  int64_t split_schema_version = GCTX.split_schema_version_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", K(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else if (split_schema_version < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split schema version should be valid", K(ret), K(split_schema_version));
  } else {
    ObArray<ObServerStatus> all_servers;
    if (OB_ISNULL(server_mgr_) || OB_ISNULL(rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server_mgr or rpc_proxy is null", K(ret), KP_(server_mgr), KP_(rpc_proxy));
    } else if (OB_FAIL(server_mgr_->get_server_statuses("", all_servers))) {
      LOG_WARN("fail to get all server", K(ret));
    } else {
      const int64_t WAIT_US = 1000 * 1000L;  // 1 second
      obrpc::ObGetTenantSchemaVersionArg arg;
      arg.tenant_id_ = OB_SYS_TENANT_ID;
      for (int i = 0; OB_SUCC(ret) && i < all_servers.count(); i++) {
        ObServerStatus& status = all_servers.at(i);
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor is stop", K(ret));
        } else if (OB_FAIL(!status.is_alive())) {
          ret = OB_SERVER_NOT_ALIVE;
          LOG_WARN("server is not alive", K(ret), K(status));
        } else {
          obrpc::ObGetTenantSchemaVersionResult result;
          const int64_t end = ObTimeUtility::current_time();
          while (OB_SUCC(ret)) {
            if (OB_FAIL(check_stop())) {
              LOG_WARN("executor is stop", K(ret));
            } else if (OB_FAIL(rpc_proxy_->to(status.server_)
                                   .timeout(GCONF.rpc_timeout)
                                   .get_tenant_refreshed_schema_version(arg, result))) {
              LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(status));
            } else if (result.schema_version_ >= split_schema_version) {
              LOG_INFO("[UPGRADE] server refresh schema in new mode",
                  K(ret),
                  K(status),
                  K(result),
                  K(split_schema_version),
                  "cost us",
                  ObTimeUtility::current_time() - end);
              break;
            } else {
              LOG_INFO("check server", K(ret), K(status), K(result), K(split_schema_version));
              usleep(static_cast<useconds_t>((WAIT_US)));
            }
          }
        }
      }
    }
  }
  LOG_INFO("[UPGRADE] check all server finish", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

// Obs' schema_version maybe not new enough after we create new sys table,
// which may cause error in the following data migration. To avoid such situation,
// we should wait all obs' schema new enough before we migrate data.
int ObSchemaSplitExecutor::check_schema_sync()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to check schema sync", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", K(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else {
    const int64_t WAIT_US = 1000 * 1000L;  // 1 second
    bool is_sync = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stop", K(ret));
      } else if (OB_FAIL(ObUpgradeUtils::check_schema_sync(is_sync))) {
        LOG_WARN("fail to check schema sync", K(ret));
      } else if (is_sync) {
        break;
      } else {
        LOG_INFO("schema not sync, should wait", K(ret));
        usleep(static_cast<useconds_t>((WAIT_US)));
      }
    }
  }
  LOG_INFO("[UPGRADE] check schema sync finish", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObSchemaSplitExecutor::can_execute()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_ || execute_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN(
        "status not matched", K(ret), "stopped", stopped_ ? "true" : "false", "build", execute_ ? "true" : "false");
  }
  return ret;
}
}  // namespace rootserver
}  // namespace oceanbase
