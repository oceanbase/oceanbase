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
#include "ob_upgrade_storage_format_version_executor.h"
#include "share/ob_upgrade_utils.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/ob_time_zone_info_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;

int64_t ObUpgradeStorageFormatVersionTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask *ObUpgradeStorageFormatVersionTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObAsyncTask *task = nullptr;
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || buf_size < deep_copy_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(deep_copy_size), K(buf_size));
  } else {
    task = new (buf) ObUpgradeStorageFormatVersionTask(*executor_);
  }
  return task;
}

int ObUpgradeStorageFormatVersionTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to execute upgrade storage format version task", K(start));
  if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, executor must not be NULL", K(ret));
  } else if (OB_FAIL(executor_->execute())) {
    LOG_WARN("fail to execute upgrade storage format version task", K(ret));
  }
  LOG_INFO("[UPGRADE] finish execute upgrade storage format version task", K(ret), "cost_time", ObTimeUtility::current_time() - start);
  return ret;
}

ObUpgradeStorageFormatVersionExecutor::ObUpgradeStorageFormatVersionExecutor()
  : is_inited_(false), is_stopped_(false), execute_(false), rwlock_(), root_service_(nullptr), ddl_service_(nullptr)
{
}

int ObUpgradeStorageFormatVersionExecutor::init(ObRootService &root_service, ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObUpgradeStorageFormatVersionExecutor has been inited twice", K(ret));
  } else {
    root_service_ = &root_service;
    ddl_service_ = &ddl_service;
    is_inited_ = true;
    is_stopped_ = false;
  }
  return ret;
}

int ObUpgradeStorageFormatVersionExecutor::stop()
{
  int ret = OB_SUCCESS;
  const uint64_t WAIT_US = 100 * 1000L; //100ms
  const uint64_t MAX_WAIT_US = 10 * 1000 * 1000L; //10s
  const int64_t start = ObTimeUtility::current_time();
  {
    SpinWLockGuard guard(rwlock_);
    is_stopped_ = true;
  }
  while (OB_SUCC(ret)) {
    if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
      ret = OB_TIMEOUT;
      LOG_WARN("use too much time", K(ret), "cost_us", ObTimeUtility::current_time() - start);
    } else if (!execute_) {
      break;
    } else {
      ob_usleep(WAIT_US);
    }
  }
  return ret;
}

int ObUpgradeStorageFormatVersionExecutor::check_stop()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("executor should stopped", K(ret));
  }
  return ret;
}

void ObUpgradeStorageFormatVersionExecutor::start()
{
  SpinWLockGuard guard(rwlock_);
  is_stopped_ = false;
}

int ObUpgradeStorageFormatVersionExecutor::set_execute_mark()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_ || execute_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't run job at the same time", K(ret), K(is_stopped_), K(execute_));
  } else {
    execute_ = true;
  }
  return ret;
}

int ObUpgradeStorageFormatVersionExecutor::execute()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUpgradeStorageFormatVersionExecutor has not been inited", K(ret));
  } else if (OB_FAIL(set_execute_mark())) {
    LOG_WARN("fail to execute upgrade storage format", K(ret));
  } else {
    int64_t job_id = OB_INVALID_ID;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_UPGRADE_STORAGE_FORMAT_VERSION;
    if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, root_service_->get_sql_proxy(), "tenant_id", 0))) {
      LOG_WARN("fail to create rs job", K(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job is is invalid", K(ret), K(job_id));
    } else {
      int64_t cur_version = 0;
      if (OB_FAIL(root_service_->get_zone_mgr().get_storage_format_version(cur_version))) {
         LOG_WARN("fail to get current version", K(ret), K(cur_version));
       } else if (cur_version >= OB_STORAGE_FORMAT_VERSION_MAX - 1) {
         LOG_INFO("UpgradeStorageFormatVersion task skipped", K(cur_version), K(OB_STORAGE_FORMAT_VERSION_MAX));
       } else {
         if (OB_FAIL(upgrade_storage_format_version())) {
           LOG_WARN("fail to optimize all tenant", K(ret));
         } else if (OB_FAIL(root_service_->get_zone_mgr().set_storage_format_version(OB_STORAGE_FORMAT_VERSION_MAX - 1))) {
           LOG_WARN("fail to set storage format version", K(ret), K(OB_STORAGE_FORMAT_VERSION_MAX));
         }
       }
    }
    if (job_id > 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, root_service_->get_sql_proxy()))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
                ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
    // no need lock, because single-machine concurrency is prevented in the process
    execute_ = false;
  }
  return ret;
}

int ObUpgradeStorageFormatVersionExecutor::upgrade_storage_format_version()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to execute upgrade_storage_format_version", K(start));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else {
    ObArray<const ObTableSchema *> table_schemas;
    const int64_t all_core_table_id = OB_ALL_CORE_TABLE_TID;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", K(ret));
      } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
        LOG_WARN("fail to get table schemas in tenant", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < table_schemas.count(); ++j) {
          ObSqlString sql;
          const ObTableSchema *table_schema = table_schemas.at(j);
          const ObDatabaseSchema *database_schema = nullptr;
          SMART_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
            alter_table_arg.tz_info_wrap_.set_tz_info_offset(0);
            alter_table_arg.is_alter_options_ = true;
            alter_table_arg.skip_sys_table_check_ = true;
            alter_table_arg.is_inner_ = true;
            alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_DATE] = ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT;
            alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
            alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
            bool is_oracle_mode = false;
            ObTZMapWrap tz_map_wrap;
            if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap))) {
              LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id));
            } else if (FALSE_IT(alter_table_arg.set_tz_info_map(tz_map_wrap.get_tz_map()))) {
            } else if (OB_FAIL(check_stop())) {
              LOG_WARN("executor should stopped", K(ret));
            } else if (OB_ISNULL(table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("error unexpected, table schema must not be NULL", K(ret));
            } else if (all_core_table_id == table_schema->get_table_id()) {
              // do nothing
            } else if (OB_SYS_TENANT_ID != tenant_id && table_schema->is_sys_table()) {
              // do nothing
            } else if (table_schema->is_index_table() || table_schema->is_vir_table() || table_schema->is_view_table()) {
              // do nothing
            } else if (table_schema->get_storage_format_version() >= OB_STORAGE_FORMAT_VERSION_MAX) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected storage format table on table", K(ret), K(*table_schema), K(OB_STORAGE_FORMAT_VERSION_MAX));
            } else if (OB_STORAGE_FORMAT_VERSION_MAX - 1 == table_schema->get_storage_format_version()) {
              // already upgraded
            } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), database_schema))) {
              LOG_WARN("fail to get database schema", K(ret), K(tenant_id));
            } else if (OB_FAIL(sql.append_fmt("ALTER TABLE %.*s SET STORAGE_FORMAT_VERSION = '%d'",
                table_schema->get_table_name_str().length(), table_schema->get_table_name_str().ptr(), OB_STORAGE_FORMAT_VERSION_MAX - 1))) {
              LOG_WARN("fail to assign sql", K(ret));
            } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
              LOG_WARN("fail to check if oracle compat mode", K(ret));
            } else {
              const int64_t end = ObTimeUtility::current_time();
              LOG_INFO("[UPGRADE] start to optimize table", K(ret), K(tenant_id), "table_name", table_schema->get_table_name());
              alter_table_arg.exec_tenant_id_ = table_schema->get_tenant_id();
              alter_table_arg.ddl_stmt_str_ = sql.string();
              alter_table_arg.alter_table_schema_.set_origin_database_name(database_schema->get_database_name());
              alter_table_arg.alter_table_schema_.set_origin_table_name(table_schema->get_table_name());
              alter_table_arg.alter_table_schema_.set_tenant_id(tenant_id);
              alter_table_arg.alter_table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_MAX - 1);
              alter_table_arg.compat_mode_ = is_oracle_mode ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
              obrpc::ObAlterTableRes res;
              if (OB_FAIL(alter_table_arg.alter_table_schema_.alter_option_bitset_.add_member(ObAlterTableArg::STORAGE_FORMAT_VERSION))) {
                LOG_WARN("fail to add member", K(ret));
              } else if (OB_FAIL(ddl_service_->get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).alter_table(alter_table_arg, res))) {
                LOG_WARN("fail to alter table", K(ret), K(alter_table_arg));
              }
              LOG_INFO("[UPGRADE] finish optimize table", K(ret),
                       K(tenant_id), "table_name", table_schema->get_table_name(),
                       "cost_us", ObTimeUtility::current_time() - end);
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_SYS_TENANT_ID == tenant_id) {
        if (OB_FAIL(check_schema_sync())) {
          LOG_WARN("fail to check schema sync", K(ret));
        }
      }
    }
  }
  LOG_INFO("[UPGRADE] execute job upgrade_storage_format_version finish",
           K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

// If RS and tenant leader are not in a same server. 
// When normal tenants execute DDL, the observer which the tenant leader is located refresh schema slowly,
// May cause failure to write normal tenant system tables.
// In order to avoid this situation, you need to wait for newest schema refreshed.
int ObUpgradeStorageFormatVersionExecutor::check_schema_sync()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to check schema sync", K(start));
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    const int64_t WAIT_US = 1000 * 1000L; // 1 second
    bool is_sync = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stop", K(ret));
      } else if (OB_FAIL(ObUpgradeUtils::check_schema_sync(OB_INVALID_TENANT_ID, is_sync))) {
        LOG_WARN("fail to check schema sync", K(ret));
      } else if (is_sync) {
        break;
      } else {
        LOG_INFO("schema not sync, should wait", K(ret));
        ob_usleep(static_cast<useconds_t>((WAIT_US)));
      }
    }
  }
  LOG_INFO("[UPGRADE] check schema sync finish", K(ret),
           "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObUpgradeStorageFormatVersionExecutor::can_execute()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("status not matched", K(ret),
             "stopped", is_stopped_ ? "true" : "false");
  }
  return ret;
}
