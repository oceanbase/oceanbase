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

#include "ob_root_inspection.h"

#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_zone_info.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/system_variable/ob_system_variable_init.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_root_service.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/ob_primary_zone_util.h"
#include "share/ob_upgrade_utils.h"
#include "share/rc/ob_context.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/ob_schema_status_proxy.h"//ObSchemaStatusProxy
#include "share/ob_global_stat_proxy.h"//ObGlobalStatProxy
#include "share/ob_tenant_info_proxy.h" // ObAllTenantInfoProxy
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace sql;
namespace rootserver
{
////////////////////////////////////////////////////////////////
int ObTenantChecker::inspect(bool &passed, const char* &warning_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  passed = true;
  UNUSED(warning_info);
  if (OB_SUCCESS != (tmp_ret = alter_tenant_primary_zone_())) {
    ret = OB_SUCC(ret) ? tmp_ret : ret;
    LOG_WARN("fail to alter tenant primary_zone", KR(ret), KR(tmp_ret));
  }

  if (OB_SUCCESS != (tmp_ret = check_create_tenant_end_())) {
    ret = OB_SUCC(ret) ? tmp_ret : ret;
    LOG_WARN("fail to check create tenant end", KR(ret), KR(tmp_ret));
  }

  if (OB_SUCCESS != (tmp_ret = check_garbage_tenant_(passed))) {
    ret = OB_SUCC(ret) ? tmp_ret : ret;
    LOG_WARN("fail to check garbage tenant", KR(ret), KR(tmp_ret));
  }
  return ret;
}

// If the primary_zone of tenant is null, need to set to 'RANDOM'
int ObTenantChecker::alter_tenant_primary_zone_()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else {
    const ObTenantSchema *tenant_schema = NULL;
    int64_t affected_rows = 0;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCCESS == ret) {
      if (OB_ISNULL(tenant_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_id is null", K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_info(*tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", K(ret), K(*tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema is null", K(ret), K(*tenant_id));
      } else if (tenant_schema->get_primary_zone().empty()) {
        ObSqlString sql;
        if (OB_FAIL(sql.append_fmt("ALTER TENANT %s set primary_zone = RANDOM",
                                   tenant_schema->get_tenant_name()))) {
          LOG_WARN("fail to generate sql", K(ret), K(*tenant_id));
        } else if (OB_ISNULL(sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql_proxy is null", K(ret));
        } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("inspector", "alter_tenant_primary_zone",
                                "tenant_id", tenant_schema->get_tenant_id(),
                                "tenant", tenant_schema->get_tenant_name());
        }
      }
    }
  }
  return ret;
}

int ObTenantChecker::check_create_tenant_end_()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service not init", K(ret));
  } else if (!schema_service_->is_sys_full_schema()) {
    // skip
  } else if (GCTX.is_standby_cluster()) {
    // skip
  } else if (OB_FAIL(schema_service_->get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", KR(ret));
  } else {
    const ObSimpleTenantSchema *tenant_schema = NULL;
    int64_t schema_version = OB_INVALID_VERSION;
    int64_t baseline_schema_version = OB_INVALID_VERSION;
    FOREACH_CNT(tenant_id, tenant_ids) {
      // overwrite ret
      if (!GCTX.root_service_->is_full_service()) {
        ret = OB_CANCELED;
        LOG_WARN("rs is not in full service", KR(ret));
        break;
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(*tenant_id, schema_guard))) {
        LOG_WARN("get_schema_guard failed", KR(ret), K(*tenant_id));
      } else if (OB_FAIL(schema_guard.get_schema_version(*tenant_id, schema_version))) {
        LOG_WARN("fail to get tenant schema version", KR(ret), K(*tenant_id));
      } else if (!share::schema::ObSchemaService::is_formal_version(schema_version)) {
        // tenant is still in creating
      } else if (OB_FAIL(schema_guard.get_tenant_info(*tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", KR(ret), K(*tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant not exist", KR(ret), K(*tenant_id));
      } else if (OB_FAIL(schema_service_->get_baseline_schema_version(*tenant_id, false/*auto update*/,
                                                                      baseline_schema_version))) {
        LOG_WARN("fail to get baseline schema_version", KR(ret), K(*tenant_id));
      } else if (OB_INVALID_VERSION == baseline_schema_version) {
        //baseline_schema_version is not valid, just skip to create this kind of tenant
      } else if (tenant_schema->is_creating()) {
        obrpc::ObCreateTenantEndArg arg;
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        arg.tenant_id_ = *tenant_id;
        if (OB_FAIL(rpc_proxy_.create_tenant_end(arg))) {
          LOG_WARN("fail to execute create tenant end", KR(ret), K(*tenant_id));
        } else {
          LOG_INFO("execute create_tenant_end", KR(ret), K(*tenant_id), K(schema_version));
          ROOTSERVICE_EVENT_ADD("inspector", "tenant_checker", "info", "execute create_tenant_end", "tenant_id", *tenant_id);
        }
      }
    }
  }
  return ret;
}

int ObTenantChecker::check_garbage_tenant_(bool &passed)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service not init", KR(ret));
  } else if (!schema_service_->is_sys_full_schema()) {
    // skip
  } else {
    obrpc::ObGetSchemaArg arg;
    obrpc::ObTenantSchemaVersions result;
    ObSchemaGetterGuard schema_guard;
    arg.ignore_fail_ = true;
    arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
    const int64_t rpc_timeout = GCONF.rpc_timeout * 10;
    // There may have multi RootService, so we won't force drop tenant here.
    if (OB_FAIL(rpc_proxy_.timeout(rpc_timeout).get_tenant_schema_versions(arg, result))) {
      LOG_WARN("fail to get tenant schema versions", KR(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get_schema_guard failed", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < result.tenant_schema_versions_.count(); i++) {
      TenantIdAndSchemaVersion &tenant = result.tenant_schema_versions_.at(i);
      int tmp_ret = OB_SUCCESS;
      if (!GCTX.root_service_->is_full_service()) {
        ret = OB_CANCELED;
        LOG_WARN("rs is not in full service", KR(ret));
      } else if (!ObSchemaService::is_formal_version(tenant.schema_version_)) {
        const ObSimpleTenantSchema *tenant_schema = NULL;
        uint64_t tenant_id = tenant.tenant_id_;
        if (OB_SUCCESS != (tmp_ret = schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
          LOG_WARN("fail to get tenant info", KR(tmp_ret), K(tenant_id));
        } else if (OB_ISNULL(tenant_schema)) {
          tmp_ret = OB_TENANT_NOT_EXIST;
        } else if (tenant_schema->is_restore()) {
          LOG_INFO("tenant is in restore", KPC(tenant_schema));
        } else if (tenant_schema->is_creating()) {
          LOG_ERROR("the tenant may be in the process of creating, if the error reports continuously, please check", K(tenant_id));
          LOG_DBA_WARN(OB_ERR_ROOT_INSPECTION, "msg", "the tenant may be in the process of creating, if the error reports continuously, please check",
                       K(tenant_id), "tenant_name", tenant_schema->get_tenant_name());
          ROOTSERVICE_EVENT_ADD("inspector", "tenant_checker",
                                "info", "the tenant may be in the process of creating, if the error reports continuously, please check",
                                "tenant_id", tenant_id,
                                "tenant_name", tenant_schema->get_tenant_name_str());
        }
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    } // end for
    passed = OB_SUCC(ret);
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObTableGroupChecker::ObTableGroupChecker(share::schema::ObMultiVersionSchemaService &schema_service)
    : schema_service_(schema_service),
      check_part_option_map_(),
      part_option_not_match_set_(),
      allocator_(ObModIds::OB_SCHEMA),
      is_inited_(false)
{
}

int ObTableGroupChecker::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(check_part_option_map_.create(TABLEGROUP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_TABLEGROUP_MAP))) {
    LOG_WARN("init check_part_option_map failed", K(ret));
  } else if (OB_FAIL(part_option_not_match_set_.create(TABLEGROUP_BUCKET_NUM))) {
    LOG_WARN("init part_option_not_match_set failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObTableGroupChecker::~ObTableGroupChecker()
{
}

int ObTableGroupChecker::inspect(bool &passed, const char* &warning_info)
{
  int ret = OB_SUCCESS;
  passed = true;
  ObArray<uint64_t> tenant_ids;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", K(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    FOREACH(tenant_id, tenant_ids) { // ignore error for each tenant
      if (OB_SUCCESS != (tmp_ret = inspect_(*tenant_id, passed))) {
        LOG_WARN("inspect tablegroup options by tenant failed",
                 KR(tmp_ret), "tenant_id", *tenant_id);
      }
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (!passed) {
      warning_info = "tablegroup has tables that have different primary_zone/locality/part_option";
    }
  }
  return ret;
}

int ObTableGroupChecker::inspect_(
    const uint64_t tenant_id,
    bool &passed)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  allocator_.reset();
  check_part_option_map_.reuse();
  part_option_not_match_set_.reuse();
  ObArray<uint64_t> table_ids;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", KR(ret));
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, table_ids))) {
    LOG_WARN("fail to get table_ids", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", KR(ret));
  } else if (!GCTX.root_service_->is_full_service()) {
    ret = OB_CANCELED;
    LOG_WARN("rs is not in full service", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i++) {
      const uint64_t table_id = table_ids.at(i);
      const ObTableSchema *table = NULL;
      // schema guard cannot be used repeatedly in iterative logic,
      // otherwise it will cause a memory hike in schema cache
      if (!GCTX.root_service_->is_full_service()) {
        ret = OB_CANCELED;
        LOG_WARN("rs is not in full service", KR(ret));
      } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table))) {
        LOG_WARN("get table schema failed", K(ret), KT(table_id));
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table not exist", KR(ret), K(table_id));
      } else if (is_sys_table(table->get_table_id()) || !table->has_partition()) {
        // skip, check the partitioned user table
      } else if (OB_FAIL(check_part_option(*table, schema_guard))) {
        LOG_WARN("check part option fail", KR(ret), KPC(table));
      } else {}
    }
  }
  if (OB_SUCC(ret)) {
    if (part_option_not_match_set_.size() > 0) {
      passed = false;
      LOG_WARN("tables part option in one tablegroup are not the same", K(tenant_id), K_(part_option_not_match_set));
      ROOTSERVICE_EVENT_ADD("inspector", "check_part_option", K(tenant_id), K_(part_option_not_match_set));
    }
  }
  return ret;
}

// Check the partition_option of tables in the same tablegroup:
// 1. For tablegroups created before 2.0, the part_type and part_num of tables in tablegroup should be same.
// 2. For tablegroups created after 2.0:
//    1) tablegroup is nonpartition, Allow "non partitioned table" or "partitioned table with 1 number of partitions" in tablegroup.
//       in addition, the partition_num, partition_type, partition_value and number of expression vectors of tables must be same.
//    2) tablegroup is partitioned, the partition_num, partition_type, partition_value and number of expression vectors
//       of tables in tablegroup should be same.
int ObTableGroupChecker::check_part_option(const ObSimpleTableSchemaV2 &table, ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSqlString user_error;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t tablegroup_id = table.get_tablegroup_id();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", KR(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    // skip check while tablegroup_id is default value
  } else if (!table.is_user_table()) {
    // only check user table
  } else if (OB_HASH_NOT_EXIST != (tmp_ret = part_option_not_match_set_.exist_refactored(tablegroup_id))) {
   //skip check while already in part_option_not_match_set_
   if (OB_HASH_EXIST != tmp_ret) {
     ret = tmp_ret;
     LOG_WARN("fail to check if tablegroup_id exist", KR(ret), K(tablegroup_id));
   }
  } else {
    const ObSimpleTableSchemaV2 *table_in_map = NULL;
    bool is_matched = true;
    const ObTablegroupSchema *tablegroup = NULL;
    const ObSimpleTableSchemaV2 *primary_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup))) {
      LOG_WARN("fail to get tablegroup schema", KR(ret), K(tenant_id), KT(tablegroup_id));
    } else if (OB_ISNULL(tablegroup)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablegroup schema is null", KR(ret), KT(tablegroup_id));
    } else if (tablegroup->get_sharding() == OB_PARTITION_SHARDING_NONE) {
      //no need to check,just ignore
    } else if (tablegroup->get_sharding() == OB_PARTITION_SHARDING_PARTITION
              || tablegroup->get_sharding() == OB_PARTITION_SHARDING_ADAPTIVE) {
      bool check_sub_part = tablegroup->get_sharding() == OB_PARTITION_SHARDING_PARTITION ? false : true;
      if (OB_FAIL(check_part_option_map_.get_refactored(tablegroup_id, table_in_map))) {
        //set to the map while not in check_part_option_map_
        if (OB_HASH_NOT_EXIST == ret) {
          ObSimpleTableSchemaV2 *new_table_schema = NULL;
          if (OB_FAIL(schema_guard.get_primary_table_schema_in_tablegroup(tenant_id, tablegroup_id, primary_table_schema))) {
            LOG_WARN("fail to get primary table schema in tablegroup", KR(ret), K(tablegroup_id));
          } else if (OB_ISNULL(primary_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("primary table schema is NULL", KR(ret), K(tenant_id), K(tablegroup_id));
          } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, *primary_table_schema, new_table_schema))) {
            LOG_WARN("alloc schema failed", KR(ret), KPC(primary_table_schema));
          } else if (OB_FAIL(check_part_option_map_.set_refactored(tablegroup_id, new_table_schema))) {
            LOG_WARN("set table_schema in hashmap fail", KR(ret), K(tablegroup_id), KPC(primary_table_schema));
          }
        } else {
          LOG_WARN("check tablegroup_id in hashmap fail", KR(ret), K(tablegroup_id));
        }
      } else if (OB_ISNULL(table_in_map)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_is_map is NULL", KR(ret), K(tablegroup_id));
      } else if (OB_FAIL(ObSimpleTableSchemaV2::compare_partition_option(table, *table_in_map, check_sub_part, is_matched, &user_error))) {
        LOG_WARN("fail to check partition option", KR(ret), K(table), KPC(table_in_map));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sharding type not suit", KR(ret), KPC(tablegroup));
    }
    if (OB_FAIL(ret) || is_matched) {
      //skip
    } else if (OB_FAIL(part_option_not_match_set_.set_refactored(tablegroup_id))) {
      LOG_WARN("set tablegroup_id in hashset fail", KR(ret));
    } else if (OB_ISNULL(table_in_map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_is_map is NULL", KR(ret), K(tablegroup_id));
    } else {
      LOG_INFO("tables in one tablegroup have different part/subpart option",
               K(tablegroup_id), "table_id", table.get_table_id(), K(user_error), K(table_in_map->get_table_id()));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObInspector::ObInspector(ObRootService &rs)
    :ObAsyncTimerTask(rs.get_inspect_task_queue()),
    rs_(rs)
{}

int ObInspector::process()
{
  // @todo ObTaskController::get().switch_task(share::ObTaskType::ROOT_SERVICE);
  int ret = OB_SUCCESS;
  ObTableGroupChecker tablegroup_checker(rs_.get_schema_service());
  ObRootInspection system_schema_checker;
  ObTenantChecker tenant_checker(rs_.get_schema_service(), rs_.get_sql_proxy(), rs_.get_common_rpc_proxy());

  ret = OB_E(EventTable::EN_STOP_ROOT_INSPECTION) OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablegroup_checker.init())) {
    LOG_WARN("init tablegroup_checker failed", K(ret));
  } else if (OB_FAIL(system_schema_checker.init(rs_.get_schema_service(), rs_.get_zone_mgr(),
                                         rs_.get_sql_proxy()))) {
    LOG_WARN("init root inspection failed", K(ret));
  } else {
    ObInspectionTask *inspection_tasks[] = {
      &tablegroup_checker,
      &system_schema_checker,
      &tenant_checker
    };
    bool passed = true;
    const char* warning_info = NULL;
    int N = ARRAYSIZEOF(inspection_tasks);
    for (int i = 0; i < N; ++i) {
      passed = true;
      warning_info = NULL;
      int tmp_ret = inspection_tasks[i]->inspect(passed, warning_info);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("inpection task failed", K(tmp_ret), K(i), "task", inspection_tasks[i]->get_task_name());
      } else if (passed) {
        LOG_INFO("inspection task succ", K(i), "task", inspection_tasks[i]->get_task_name());
      } else {
        LOG_ERROR(warning_info);
        ROOTSERVICE_EVENT_ADD("inspector", inspection_tasks[i]->get_task_name(),
                              "info", (warning_info == NULL ? "": warning_info));
      }
    }
  }
  return ret;
}

ObAsyncTask *ObInspector::deep_copy(char *buf, const int64_t buf_size) const
{
  ObInspector *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size));
  } else {
    task = new(buf) ObInspector(rs_);
  }
  return task;
}

////////////////////////////////////////////////////////////////
ObPurgeRecyclebinTask::ObPurgeRecyclebinTask(ObRootService &rs)
    :ObAsyncTimerTask(rs.get_inspect_task_queue()),
    root_service_(rs)
{}

int ObPurgeRecyclebinTask::process()
{
  LOG_INFO("purge recyclebin task begin");
  int ret = OB_SUCCESS;
  const int64_t PURGE_EACH_TIME = 1000;
  int64_t delay = 1 * 60 * 1000 * 1000;
  int64_t expire_time = GCONF.recyclebin_object_expire_time;
  int64_t purge_interval = GCONF._recyclebin_object_purge_frequency;
  if (expire_time > 0 && purge_interval > 0) {
   if (OB_FAIL(root_service_.purge_recyclebin_objects(PURGE_EACH_TIME))) {
      LOG_WARN("fail to purge recyclebin objects", KR(ret));
    }
    delay = purge_interval;
  }
  // the error code is only for outputtion log, the function will return success.
  // the task no need retry, because it will be triggered periodically.
  if (OB_FAIL(root_service_.schedule_recyclebin_task(delay))) {
    LOG_WARN("schedule purge recyclebin task failed", KR(ret), K(delay));
  } else {
    LOG_INFO("submit purge recyclebin task success", K(delay));
  }
  LOG_INFO("purge recyclebin task end", K(delay));
  return OB_SUCCESS;
}

ObAsyncTask *ObPurgeRecyclebinTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObPurgeRecyclebinTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size));
  } else {
    task = new(buf) ObPurgeRecyclebinTask(root_service_);
  }
  return task;
}

ObRootInspection::ObRootInspection()
  : inited_(false), stopped_(false), zone_passed_(false),
    sys_param_passed_(false), sys_stat_passed_(false),
    sys_table_schema_passed_(false), data_version_passed_(false),
    all_checked_(false), all_passed_(false), can_retry_(false),
    sql_proxy_(NULL), rpc_proxy_(NULL), schema_service_(NULL),
    zone_mgr_(NULL)
{
}

ObRootInspection::~ObRootInspection()
{
}

int ObRootInspection::init(ObMultiVersionSchemaService &schema_service,
                           ObZoneManager &zone_mgr,
                           ObMySQLProxy &sql_proxy,
                           obrpc::ObCommonRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    zone_mgr_ = &zone_mgr;
    sql_proxy_ = &sql_proxy;
    stopped_ = false;
    zone_passed_ = false;
    sys_param_passed_ = false;
    sys_stat_passed_ = false;
    sys_table_schema_passed_ = false;
    data_version_passed_ = false;
    all_checked_ = false;
    all_passed_ = false;
    can_retry_ = false;
    rpc_proxy_ = rpc_proxy;
    inited_ = true;
  }
  return ret;
}

int ObRootInspection::check_all()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (!schema_service_->is_tenant_full_schema(OB_SYS_TENANT_ID)) {
    ret = OB_EAGAIN;
    LOG_WARN("schema is not ready, try again", K(ret));
  } else {
    can_retry_ = false;
    int tmp = OB_SUCCESS;

    // check __all_zone
    if (OB_SUCCESS != (tmp = check_zone())) {
      LOG_WARN("check_zone failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    zone_passed_ = (OB_SUCCESS == tmp);

    // check sys stat
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_stat_())) {
      LOG_WARN("check_sys_stat failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_stat_passed_ = (OB_SUCCESS == tmp);

    // check sys param
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_param_())) {
      LOG_WARN("check_sys_param failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_param_passed_ = (OB_SUCCESS == tmp);

    // check sys schema
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_table_schemas_())) {
      LOG_WARN("check_sys_table_schemas failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_table_schema_passed_ = (OB_SUCCESS == tmp);

    // check tenant's data version
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_data_version_())) {
      LOG_WARN("check_data_version failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    data_version_passed_ = (OB_SUCCESS == tmp);

    // upgrade job may still running, in order to avoid the upgrade process error stuck,
    // ignore the 4674 error
    for (int64_t i = 0; i < UPGRADE_JOB_TYPE_COUNT; i++) {
      tmp = OB_SUCCESS;
      ObRsJobType job_type = upgrade_job_type_array[i];
      if (job_type > JOB_TYPE_INVALID && job_type < JOB_TYPE_MAX) {
        if (OB_SUCCESS != (tmp = check_cancel())) {
          LOG_WARN("check_cancel failed", KR(ret), K(tmp));
          ret = (OB_SUCCESS == ret) ? tmp : ret;
          break;
        } else if (OB_SUCCESS != (tmp = ObUpgradeUtils::check_upgrade_job_passed(job_type))) {
          LOG_WARN("fail to check upgrade job passed", K(tmp), K(job_type));
          if (OB_RUN_JOB_NOT_SUCCESS != tmp) {
            ret = (OB_SUCCESS == ret) ? tmp : ret;
          } else {
            LOG_WARN("upgrade job may still running, check with __all_virtual_uprade_inspection",
                     K(ret), K(tmp), "job_type", ObRsJobTableOperator::get_job_type_str(job_type));
          }
        }
      }
    }

    all_checked_ = true;
    all_passed_ = OB_SUCC(ret);
  }
  return ret;
}

int ObRootInspection::check_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(check_sys_stat_(tenant_id))) {
      LOG_WARN("fail to check sys stat", KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_TMP_FAIL(check_sys_param_(tenant_id))) {
      LOG_WARN("fail to check param", KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_TMP_FAIL(check_sys_table_schemas_(tenant_id))) {
      LOG_WARN("fail to check sys table", KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObRootInspection::inspect(bool &passed, const char* &warning_info)
{
  int ret = OB_SUCCESS;
  if (!GCONF.in_upgrade_mode()) {
    ret = check_all();
    if (OB_SUCC(ret)) {
      passed = all_passed_;
      warning_info = "system metadata error";
    }
  } else {
    passed = true;
  }
  return ret;
}


// standby tenant may stay at lower data version,
// root_inspection won't check standby tenant's schema.
int ObRootInspection::construct_tenant_ids_(
    common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObArray<uint64_t> standby_tenants;
  ObArray<uint64_t> tmp_tenants;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tmp_tenants))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    bool is_standby = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenants.count(); i++) {
      const uint64_t tenant_id = tmp_tenants.at(i);
      if (OB_FAIL(ObAllTenantInfoProxy::is_standby_tenant(sql_proxy_, tenant_id, is_standby))) {
        LOG_WARN("fail to check is standby tenant", KR(ret), K(tenant_id));
      } else if (is_standby) {
        // skip
      } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("fail to push back tenant_id", KR(ret), K(tenant_id));
      }
    } // end for
  }
  return ret;
}

int ObRootInspection::check_zone()
{
  int ret = OB_SUCCESS;
  ObSqlString extra_cond;
  HEAP_VAR(ObGlobalInfo, global_zone_info) {
    ObArray<ObZoneInfo> zone_infos;
    ObArray<const char *> global_zone_item_names;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(check_cancel())) {
      LOG_WARN("check_cancel failed", K(ret));
    } else if (OB_FAIL(extra_cond.assign_fmt("zone = '%s'", global_zone_info.zone_.ptr()))) {
      LOG_WARN("extra_cond assign_fmt failed", K(ret));
    } else if (OB_FAIL(get_names(global_zone_info.list_, global_zone_item_names))) {
      LOG_WARN("get global zone item names failed", K(ret));
    } else if (OB_FAIL(check_names(OB_SYS_TENANT_ID, OB_ALL_ZONE_TNAME, global_zone_item_names, extra_cond))) {
      LOG_WARN("check global zone item names failed", "table_name", OB_ALL_ZONE_TNAME,
          K(global_zone_item_names), K(extra_cond), K(ret));
    } else if (OB_FAIL(zone_mgr_->get_zone(zone_infos))) {
      LOG_WARN("zone manager get_zone failed", K(ret));
    } else {
      ObArray<const char *> zone_item_names;
      FOREACH_CNT_X(zone_info, zone_infos, OB_SUCCESS == ret) {
        zone_item_names.reuse();
        extra_cond.reset();
        if (OB_FAIL(check_cancel())) {
          LOG_WARN("check_cancel failed", K(ret));
        } else if (OB_FAIL(extra_cond.assign_fmt("zone = '%s'", zone_info->zone_.ptr()))) {
          LOG_WARN("extra_cond assign_fmt failed", K(ret));
        } else if (OB_FAIL(get_names(zone_info->list_, zone_item_names))) {
          LOG_WARN("get zone item names failed", K(ret));
        } else if (OB_FAIL(check_names(OB_SYS_TENANT_ID, OB_ALL_ZONE_TNAME, zone_item_names, extra_cond))) {
          LOG_WARN("check zone item names failed", "table_name", OB_ALL_ZONE_TNAME,
              K(zone_item_names), "zone_info", *zone_info, K(extra_cond), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootInspection::check_sys_stat_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(construct_tenant_ids_(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCC(ret)) {
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", KR(ret));
      } else if (OB_TMP_FAIL(check_sys_stat_(*tenant_id))) {
        LOG_WARN("fail to check sys stat", KR(tmp_ret), K(*tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
    } // end foreach
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_sys_stat_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<const char *> sys_stat_names;
  ObSqlString extra_cond;
  ObSysStat sys_stat;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(check_tenant_status_(tenant_id))) {
    LOG_WARN("fail to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_stat.set_initial_values(tenant_id))) {
    LOG_WARN("set initial values failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(extra_cond.assign_fmt("tenant_id = %lu",
             ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
    LOG_WARN("extra_cond assign_fmt failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_names(sys_stat.item_list_, sys_stat_names))) {
    LOG_WARN("get sys stat names failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_names(tenant_id, OB_ALL_SYS_STAT_TNAME, sys_stat_names, extra_cond))) {
    LOG_WARN("check all sys stat names failed", K(ret), K(tenant_id),
             "table_name", OB_ALL_SYS_STAT_TNAME, K(sys_stat_names));
  }
  return ret;
}

int ObRootInspection::check_sys_param_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(construct_tenant_ids_(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCC(ret)) {
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", K(ret));
      } else if (OB_TMP_FAIL(check_sys_param_(*tenant_id))) {
        LOG_WARN("fail to check sys param", KR(tmp_ret), K(*tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
    } // end foreach
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_sys_param_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObArray<const char *> sys_param_names;
  ObSqlString extra_cond;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(check_tenant_status_(tenant_id))) {
    LOG_WARN("fail to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(extra_cond.assign_fmt("tenant_id = %lu",
             ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
    LOG_WARN("extra_cond assign_fmt failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sys_param_names(sys_param_names))) {
    LOG_WARN("get sys param names failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_names(tenant_id, OB_ALL_SYS_VARIABLE_TNAME,
             sys_param_names, extra_cond))) {
    LOG_WARN("check all sys params names failed", KR(ret), K(tenant_id),
             "table_name", OB_ALL_SYS_VARIABLE_TNAME, K(sys_param_names), K(extra_cond));
  }
  if (OB_SCHEMA_ERROR != ret) {
  } else if (GCONF.in_upgrade_mode()) {
    LOG_WARN("check sys_variable failed", KR(ret));
  } else {
    LOG_DBA_ERROR(OB_ERR_ROOT_INSPECTION, "msg", "system variables are unmatched", KR(ret));
  }
  return ret;
}

template<typename Item>
int ObRootInspection::get_names(const ObDList<Item> &list, ObIArray<const char*> &names)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (list.get_size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("list is empty", K(ret));
  } else {
    const Item *it = list.get_first();
    while (OB_SUCCESS == ret && it != list.get_header()) {
      if (OB_FAIL(names.push_back(it->name_))) {
        LOG_WARN("push_back failed", K(ret));
      } else {
        it = it->get_next();
      }
    }
  }
  return ret;
}

int ObRootInspection::get_sys_param_names(ObIArray<const char *> &names)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t param_count = ObSysVariables::get_amount();
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      if (OB_FAIL(names.push_back(ObSysVariables::get_name(i).ptr()))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRootInspection::check_names(const uint64_t tenant_id,
                                  const char *table_name,
                                  const ObIArray<const char *> &names,
                                  const ObSqlString &extra_cond)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (NULL == table_name || names.count() <= 0) {
    // extra_cond can be empty, so wo don't check it here
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is null or names is empty", KP(table_name), K(names), K(ret));
  } else {
    ObArray<Name> fetch_names; // Get the data of the internal table
    ObArray<Name> extra_names; // data inner table more than hard code
    ObArray<Name> miss_names; // data inner table less than hard code
    if (OB_FAIL(calc_diff_names(tenant_id, table_name, names, extra_cond,
        fetch_names, extra_names, miss_names))) {
      LOG_WARN("fail to calc diff names", K(ret), KP(table_name), K(names), K(extra_cond));
    } else {
      if (fetch_names.count() <= 0) {
        // don't need to set ret
        LOG_WARN("maybe tenant or zone has been deleted, ignore it",
                 K(tenant_id), K(table_name), K(extra_cond));
      } else {
        if (extra_names.count() > 0) {
          // don't need to set ret
          LOG_WARN("some item exist in table, but not hard coded",
                   K(tenant_id), K(table_name), K(extra_names));
        }
        if (miss_names.count() > 0) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("some item exist in hard code, but not exist in inner table",
                   K(ret), K(tenant_id), K(table_name), K(miss_names));
        }
      }
    }
  }
  return ret;
}

int ObRootInspection::calc_diff_names(const uint64_t tenant_id,
                                      const char *table_name,
                                      const ObIArray<const char *> &names,
                                      const ObSqlString &extra_cond,
                                      ObIArray<Name> &fetch_names, /* data reading from inner table*/
                                      ObIArray<Name> &extra_names, /* data inner table more than hard code*/
                                      ObIArray<Name> &miss_names /* data inner table less than hard code*/)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id;
  fetch_names.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret), K(tenant_id));
  } else if (NULL == table_name || names.count() <= 0) {
    // extra_cond can be empty, don't need to check it
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is null or names is empty",
             KR(ret), K(tenant_id), KP(table_name), K(names));
  } else if (GCTX.is_standby_cluster() && is_user_tenant(tenant_id)) {
    if (OB_ISNULL(GCTX.schema_status_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema status proxy is null", K(ret));
    } else if (OB_FAIL(GCTX.schema_status_proxy_->get_refresh_schema_status(tenant_id, schema_status))) {
      LOG_WARN("fail to get schema status", KR(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t exec_tenant_id = schema_status.tenant_id_;
    int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
    ObSqlString sql;
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_,
                                               snapshot_timestamp);
    if (OB_FAIL(sql.append_fmt("SELECT name FROM %s%s%s", table_name,
        (extra_cond.empty()) ? "" : " WHERE ", extra_cond.ptr()))) {
      LOG_WARN("append_fmt failed", KR(ret), K(tenant_id), K(table_name), K(extra_cond));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
          can_retry_ = true;
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is not expected to be NULL",
                   KR(ret), K(tenant_id), "result", OB_P(result));
        } else {
          //only for filling the out parameter,
          //Ensure that there is no '\ 0' character in the middle of the corresponding string
          int64_t tmp_real_str_len = 0;
          Name name;
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("get next result failed", KR(ret), K(tenant_id));
              }
            } else {
              EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name.ptr(),
                  static_cast<int64_t>(NAME_BUF_LEN), tmp_real_str_len);
              (void) tmp_real_str_len; // make compiler happy
              if (OB_FAIL(fetch_names.push_back(name))) {
                LOG_WARN("push_back failed", KR(ret), K(tenant_id));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (fetch_names.count() <= 0) {
        LOG_WARN("maybe tenant or zone has been deleted, ignore it",
                 KR(ret), K(schema_status), K(table_name), K(extra_cond));
      } else {
        extra_names.reset();
        miss_names.reset();
        FOREACH_CNT_X(fetch_name, fetch_names, OB_SUCCESS == ret) {
          bool found = false;
          FOREACH_CNT_X(name, names, OB_SUCC(ret)) {
            if (Name(*name) == *fetch_name) {
              found = true;
              break;
            }
          }
          if (!found) {
            if (OB_FAIL(extra_names.push_back(*fetch_name))) {
              LOG_WARN("fail to push name into fetch_names",
                       KR(ret), K(tenant_id), K(*fetch_name), K(fetch_names));
            }
          }
        }
        FOREACH_CNT_X(name, names, OB_SUCCESS == ret) {
          bool found = false;
          FOREACH_CNT_X(fetch_name, fetch_names, OB_SUCCESS == ret) {
            if (Name(*name) == *fetch_name) {
              found = true;
              break;
            }
          }
          if (!found) {
            if (OB_FAIL(miss_names.push_back(Name(*name)))) {
              LOG_WARN("fail to push name into miss_names",
                       KR(ret), K(tenant_id), K(*name), K(miss_names));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRootInspection::check_sys_table_schemas_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(construct_tenant_ids_(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    FOREACH_X(tenant_id, tenant_ids, OB_SUCC(ret)) {
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", KR(ret));
      } else if (OB_TMP_FAIL(check_sys_table_schemas_(*tenant_id))) {
        LOG_WARN("fail to check sys table schemas by tenant", KR(tmp_ret), K(*tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
    } // end foreach
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_sys_table_schemas_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_UNLIKELY(
             is_virtual_tenant_id(tenant_id)
             || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    const schema_create_func *creator_ptr_array[] = {
      share::all_core_table_schema_creator,
      share::core_table_schema_creators,
      share::sys_table_schema_creators,
      share::virtual_table_schema_creators,
      share::sys_view_schema_creators,
      share::core_index_table_schema_creators,
      share::sys_index_table_schema_creators,
      NULL };

    int back_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObTableSchema table_schema;
    bool exist = false;
    for (const schema_create_func **creator_ptr_ptr = creator_ptr_array;
         OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr_ptr); ++creator_ptr_ptr) {
      for (const schema_create_func *creator_ptr = *creator_ptr_ptr;
           OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr); ++creator_ptr) {
        table_schema.reset();
        if (OB_FAIL(check_cancel())) {
          LOG_WARN("check_cancel failed", KR(ret));
        } else if (OB_FAIL(check_tenant_status_(tenant_id))) {
          LOG_WARN("fail to check tenant status", KR(ret), K(tenant_id));
        } else if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("create table schema failed", KR(ret));
        } else if (!is_sys_tenant(tenant_id)
                   && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
                              tenant_id, table_schema))) {
          LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
        } else if (OB_FAIL(ObSysTableChecker::is_inner_table_exist(
                   tenant_id, table_schema, exist))) {
          LOG_WARN("fail to check inner table exist",
                   KR(ret), K(tenant_id), K(table_schema));
        } else if (!exist) {
          // skip
        } else {
          if (OB_TMP_FAIL(check_table_schema(tenant_id, table_schema))) {
            // don't print table_schema, otherwise log will be too much
            LOG_WARN("check table schema failed", KR(tmp_ret), K(tenant_id),
                     "table_id", table_schema.get_table_id(), "table_name", table_schema.get_table_name());
            back_ret = OB_SUCCESS == back_ret ? tmp_ret : back_ret;
          }

          if (OB_TMP_FAIL(check_sys_view_(tenant_id, table_schema))) {
            LOG_WARN("check sys view failed", KR(tmp_ret), K(tenant_id),
                     "table_id", table_schema.get_table_id(), "table_name", table_schema.get_table_name());
            back_ret = OB_SUCCESS == back_ret ? tmp_ret : back_ret;
            // sql may has occur other error except OB_SCHEMA_ERROR, we should not continue is such situation.
            if (OB_SCHEMA_ERROR != tmp_ret) {
              ret = OB_SUCC(ret) ? back_ret : tmp_ret;
            }
          }
        }
      } // end for
    } // end for
    ret = OB_SUCC(ret) ? back_ret : ret;
  }
  if (OB_SCHEMA_ERROR != ret) {
  } else if (GCONF.in_upgrade_mode()) {
    LOG_WARN("check sys table schema failed", KR(ret), K(tenant_id));
  } else {
    LOG_ERROR("check sys table schema failed", KR(ret), K(tenant_id));
    LOG_DBA_ERROR(OB_ERR_ROOT_INSPECTION, "msg", "inner tables are unmatched", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObRootInspection::check_table_schema(
    const uint64_t tenant_id,
    const ObTableSchema &hard_code_table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table = NULL;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(
             is_virtual_tenant_id(tenant_id)
             || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!hard_code_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", KR(ret), K(tenant_id), K(hard_code_table));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
             tenant_id, hard_code_table.get_table_id(), table))) {
    LOG_WARN("get_table_schema failed", KR(ret), K(tenant_id),
             "table_id", hard_code_table.get_table_id(),
             "table_name", hard_code_table.get_table_name());
    // fail may cause by load table schema sql, set retry flag.
    can_retry_ = true;
  } else if (OB_ISNULL(table)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table should not be null", KR(ret), K(tenant_id),
             "table_id", hard_code_table.get_table_id(),
             "table_name", hard_code_table.get_table_name());
    can_retry_ = true;
  } else if (OB_FAIL(check_table_schema(hard_code_table, *table))) {
    LOG_WARN("fail to check table schema", KR(ret), K(tenant_id), K(hard_code_table), KPC(table));
  }
  return ret;
}

int ObRootInspection::check_table_schema(const ObTableSchema &hard_code_table,
                                         const ObTableSchema &inner_table)
{
  int ret = OB_SUCCESS;
  if (!hard_code_table.is_valid()
      || !inner_table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", K(hard_code_table), K(inner_table), K(ret));
  } else if (OB_FAIL(check_table_options_(inner_table, hard_code_table))) {
    LOG_WARN("check_table_options failed", "table_id", hard_code_table.get_table_id(), K(ret));
  } else if (!inner_table.is_view_table()) { //view table do not check column info
    if (hard_code_table.get_column_count() != inner_table.get_column_count()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("column count mismatch", "table_id", inner_table.get_table_id(),
          "table_name",inner_table.get_table_name(), "table_column_cnt",inner_table.get_column_count(),
          "hard_code_table_column_cnt", hard_code_table.get_column_count(), K(ret));
    } else {
      int back_ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < hard_code_table.get_column_count(); ++i) {
        const ObColumnSchemaV2 *hard_code_column = hard_code_table.get_column_schema_by_idx(i);
        const ObColumnSchemaV2 *column = NULL;
        if (NULL == hard_code_column) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("hard_code_column is null", "hard_code_column", OB_P(hard_code_column), K(ret));
        } else if (NULL == (column = inner_table.get_column_schema(
            hard_code_column->get_column_name()))) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("hard code column not found", "table_id", hard_code_table.get_table_id(),
              "table_name", hard_code_table.get_table_name(), "column",
              hard_code_column->get_column_name(), K(ret));
        } else {
          const bool ignore_column_id = is_virtual_table(hard_code_table.get_table_id());
          if (OB_FAIL(check_column_schema_(hard_code_table.get_table_name(),
              *column, *hard_code_column, ignore_column_id))) {
            LOG_WARN("column schema mismatch with hard code column schema",
                "table_name",inner_table.get_table_name(), "column", *column,
                "hard_code_column", *hard_code_column, K(ret));
          }
        }
        back_ret = OB_SUCCESS == back_ret ? ret : back_ret;
        ret = OB_SUCCESS;
      }
      ret = back_ret;
    }
  }
  return ret;
}

int ObRootInspection::check_and_get_system_table_column_diff(
    const share::schema::ObTableSchema &table_schema,
    const share::schema::ObTableSchema &hard_code_schema,
    common::ObIArray<uint64_t> &add_column_ids,
    common::ObIArray<uint64_t> &alter_column_ids)
{
  int ret = OB_SUCCESS;
  add_column_ids.reset();
  alter_column_ids.reset();
  if (!table_schema.is_valid() || !hard_code_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", KR(ret), K(table_schema), K(hard_code_schema));
  } else if (table_schema.get_tenant_id() != hard_code_schema.get_tenant_id()
             || table_schema.get_table_id() != hard_code_schema.get_table_id()
             || 0 != table_schema.get_table_name_str().compare(hard_code_schema.get_table_name_str())
             || !is_system_table(table_schema.get_table_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", KR(ret),
             "tenant_id", table_schema.get_tenant_id(),
             "table_id", table_schema.get_table_id(),
             "table_name", table_schema.get_table_name(),
             "hard_code_tenant_id", hard_code_schema.get_tenant_id(),
             "hard_code_table_id", hard_code_schema.get_table_id(),
             "hard_code_table_name", hard_code_schema.get_table_name());
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = table_schema.get_table_id();
    const ObColumnSchemaV2 *column = NULL;
    const ObColumnSchemaV2 *hard_code_column = NULL;
    ObColumnSchemaV2 tmp_column; // check_column_can_be_altered_online() may change dst_column, is ugly.
    bool ignore_column_id = false;

    // case 1. check if columns should be dropped.
    // case 2. check if column can be altered online.
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); i++) {
      column = table_schema.get_column_schema_by_idx(i);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", KR(ret), K(tenant_id), K(table_id), K(i));
      } else if (OB_ISNULL(hard_code_column = hard_code_schema.get_column_schema(column->get_column_id()))) {
        ret = OB_NOT_SUPPORTED; // case 1
        LOG_WARN("can't drop system table's column", KR(ret),
                 K(tenant_id), K(table_id),
                 "table_name", table_schema.get_table_name(),
                 "column_id", column->get_column_id(),
                 "column_name", column->get_column_name());
      } else {
        // case 2
        int tmp_ret = check_column_schema_(table_schema.get_table_name_str(),
                                           *column,
                                           *hard_code_column,
                                           ignore_column_id);
        if (OB_SUCCESS == tmp_ret) {
          // not changed
        } else if (OB_SCHEMA_ERROR != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to check column schema", KR(ret),
                   K(tenant_id), K(table_id), KPC(column), KPC(hard_code_column));
        } else if (OB_FAIL(tmp_column.assign(*hard_code_column))) {
          LOG_WARN("fail to assign hard code column schema", KR(ret),
                   K(tenant_id), K(table_id),  "column_id", hard_code_column->get_column_id());
        } else if (OB_FAIL(table_schema.check_column_can_be_altered_online(column, &tmp_column))) {
          LOG_WARN("fail to check alter column online", KR(ret),
                   K(tenant_id), K(table_id),
                   "table_name", table_schema.get_table_name(),
                   "column_id", column->get_column_id(),
                   "column_name", column->get_column_name());
        } else if (OB_FAIL(alter_column_ids.push_back(column->get_column_id()))) {
          LOG_WARN("fail to push back column_id", KR(ret), K(tenant_id), K(table_id),
                   "column_id", column->get_column_id());
        }
      }
    } // end for

    // case 3: check if columns should be added.
    for (int64_t i = 0; OB_SUCC(ret) && i < hard_code_schema.get_column_count(); i++) {
      hard_code_column = hard_code_schema.get_column_schema_by_idx(i);
      if (OB_ISNULL(hard_code_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", KR(ret), K(tenant_id), K(table_id), K(i));
      } else if (OB_NOT_NULL(column = table_schema.get_column_schema(hard_code_column->get_column_id()))) {
        // column exist, just skip
      } else {
        const uint64_t hard_code_column_id = hard_code_column->get_column_id();
        const ObColumnSchemaV2 *last_column = NULL;
        if (table_schema.get_column_count() <= 0
            || OB_ISNULL(last_column = table_schema.get_column_schema_by_idx(
                         table_schema.get_column_count() - 1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column count or column", KR(ret), K(table_schema));
        } else if (table_schema.get_max_used_column_id() >= hard_code_column_id
                  || last_column->get_column_id() >= hard_code_column_id) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("column should be added at last", KR(ret), KPC(hard_code_column), K(table_schema));
        } else if (OB_FAIL(add_column_ids.push_back(hard_code_column_id))) {
          LOG_WARN("fail to push back column_id", KR(ret), K(tenant_id), K(table_id),
                   "column_id", hard_code_column_id);
        }
      }
    } // end for
  }
  return ret;
}

bool ObRootInspection::check_str_with_lower_case_(const ObString &str)
{
  bool bret = false;
  if (str.length() > 0) {
    for (int64_t i = 0; !bret && i < str.length(); i++) {
      if (str.ptr()[i] >= 'a' && str.ptr()[i] <= 'z') {
        bret = true;
      }
    }
  }
  return bret;
}

int ObRootInspection::check_sys_view_(
    const uint64_t tenant_id,
    const ObTableSchema &hard_code_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (hard_code_table.is_view_table()) {
    // check view definition
    const ObString &table_name = hard_code_table.get_table_name();
    const uint64_t table_id = hard_code_table.get_table_id();
    const uint64_t database_id = hard_code_table.get_database_id();
    bool is_oracle = is_oracle_sys_database_id(database_id);
    bool check_lower_case = !is_mysql_database_id(database_id);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      // case 0: check expansion of sys view definition
      if (is_oracle) {
        if (OB_FAIL(sql.assign_fmt("SELECT FIELD FROM \"%s\".\"%s\" WHERE TABLE_ID = %lu",
                                   OB_ORA_SYS_SCHEMA_NAME,
                                   OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TNAME,
                                   table_id))) {
          LOG_WARN("failed to assign sql", KR(ret), K(sql));
        } else if (OB_FAIL(GCTX.root_service_->get_oracle_sql_proxy().read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(table_name), K(sql));
        }
      } else {
        if (OB_FAIL(sql.assign_fmt("SELECT FIELD FROM `%s`.`%s` WHERE TABLE_ID = %lu",
                                   OB_SYS_DATABASE_NAME,
                                   OB_TENANT_VIRTUAL_TABLE_COLUMN_TNAME,
                                   table_id))) {
          LOG_WARN("failed to assign sql", KR(ret), K(sql));
        } else if (!is_oracle && OB_FAIL(GCTX.root_service_->get_sql_proxy().read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(table_name), K(sql));
        }
      }
      if (OB_FAIL(ret)) {
        if (OB_ERR_VIEW_INVALID == ret) {
          ret = OB_SCHEMA_ERROR;
          LOG_ERROR("check sys view: expand failed", KR(ret), K(tenant_id), K(table_name));
        } else {
          LOG_WARN("check sys view: expand failed", KR(ret), K(tenant_id), K(table_name));
        }
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret), K(tenant_id));
      } else if (check_lower_case) {
        // case 1: check column name with lower case
        ObString col_name;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          if (OB_FAIL(result->get_varchar(0L, col_name))) {
            LOG_WARN("fail to get filed", KR(ret), K(tenant_id), K(table_name));
          } else if (check_str_with_lower_case_(col_name)) {
            ret = OB_SCHEMA_ERROR;
            LOG_ERROR("check sys view: column name should be uppercase",
                      KR(ret), K(tenant_id), K(table_name), K(col_name));
          }
        } // end while
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("iterate failed", KR(ret));
        }
        // case 2: check view name with lower case
        if (OB_SUCC(ret) && check_str_with_lower_case_(hard_code_table.get_table_name())) {
          ret = OB_SCHEMA_ERROR;
          LOG_ERROR("check sys view: table name should be uppercase", KR(ret), K(tenant_id), K(table_name));
        }
      }
    }
  }
  return ret;
}

int ObRootInspection::check_table_options_(const ObTableSchema &table,
                                           const ObTableSchema &hard_code_table)
{
  int ret = OB_SUCCESS;
  if (!table.is_valid() || !hard_code_table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table or invalid hard_code_table", K(table), K(hard_code_table), K(ret));
  } else if (table.get_table_id() != hard_code_table.get_table_id()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table id not match", "table_id", table.get_table_id(),
        "hard_code table_id", hard_code_table.get_table_id(), K(ret));
  } else if (table.get_table_name_str() != hard_code_table.get_table_name_str()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table name mismatch with hard code table",
        "table_id", table.get_table_id(), "table_name", table.get_table_name(),
        "hard_code_table name", hard_code_table.get_table_name(), K(ret));
  } else {
    const ObString &table_name = table.get_table_name_str();

    if (table.get_tenant_id() != hard_code_table.get_tenant_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tenant_id mismatch", K(table_name), "in_memory", table.get_tenant_id(),
          "hard_code", hard_code_table.get_tenant_id(), K(ret));
    } else if (table.get_database_id() != hard_code_table.get_database_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("database_id mismatch", K(table_name), "in_memory", table.get_database_id(),
          "hard_code", hard_code_table.get_database_id(), K(ret));
    } else if (table.get_tablegroup_id() != hard_code_table.get_tablegroup_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tablegroup_id mismatch", K(table_name), "in_memory", table.get_tablegroup_id(),
          "hard_code", hard_code_table.get_tablegroup_id(), K(ret));
    } else if (table.get_auto_increment() != hard_code_table.get_auto_increment()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("auto_increment mismatch", K(table_name), "in_memory", table.get_auto_increment(),
          "hard_code", hard_code_table.get_auto_increment(), K(ret));
    } else if (table.is_read_only() != hard_code_table.is_read_only()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("read_only mismatch", K(table_name), "in_memory", table.is_read_only(),
          "hard code", hard_code_table.is_read_only(), K(ret));
    } else if (table.get_load_type() != hard_code_table.get_load_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("load_type mismatch", K(table_name), "in_memory", table.get_load_type(),
          "hard_code", hard_code_table.get_load_type(), K(ret));
    } else if (table.get_table_type() != hard_code_table.get_table_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("table_type mismatch", K(table_name), "in_memory", table.get_table_type(),
          "hard_code", hard_code_table.get_table_type(), K(ret));
    } else if (table.get_index_type() != hard_code_table.get_index_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_type mismatch", K(table_name), "in_memory", table.get_index_type(),
          "hard_code", hard_code_table.get_index_type(), K(ret));
    } else if (table.get_index_using_type() != hard_code_table.get_index_using_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_using_type mismatch", K(table_name), "in_memory", table.get_index_using_type(),
          "hard_code", hard_code_table.get_index_using_type(), K(ret));
    } else if (table.get_def_type() != hard_code_table.get_def_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("def_type mismatch", K(table_name), "in_memory", table.get_def_type(),
          "hard_code", hard_code_table.get_def_type(), K(ret));
    } else if (table.get_data_table_id() != hard_code_table.get_data_table_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("data_table_id mismatch", K(table_name), "in_memory", table.get_data_table_id(),
          "hard_code", hard_code_table.get_data_table_id(), K(ret));
    } else if (table.get_tablegroup_name() != hard_code_table.get_tablegroup_name()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tablegroup_name mismatch", K(table_name), "in_memory", table.get_tablegroup_name(),
          "hard_code", hard_code_table.get_tablegroup_name(), K(ret));
    } else if (table.get_view_schema() != hard_code_table.get_view_schema()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("view_schema mismatch", K(table_name), "in_memory", table.get_view_schema(),
          "hard_code", hard_code_table.get_view_schema(), K(ret));
    } else if (table.get_part_level() != hard_code_table.get_part_level()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("part_level mismatch", K(table_name), "in_memory", table.get_part_level(),
          "hard_code", hard_code_table.get_part_level(), K(ret));
    } else if ((table.get_part_option().get_part_func_expr_str()
        != hard_code_table.get_part_option().get_part_func_expr_str())
        || (table.get_part_option().get_part_func_type()
        != hard_code_table.get_part_option().get_part_func_type())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("part_expr mismatch", K(table_name), "in_memory",
          table.get_part_option(), "hard_code", hard_code_table.get_part_option(), K(ret));
    } else if ((table.get_sub_part_option().get_part_func_expr_str()
        != hard_code_table.get_sub_part_option().get_part_func_expr_str())
        || (table.get_sub_part_option().get_part_func_type()
        != hard_code_table.get_sub_part_option().get_part_func_type())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("sub_part_expr mismatch", K(table_name), "in_memory",
          table.get_sub_part_option(), "hard_code", hard_code_table.get_sub_part_option(), K(ret));
    } else if (table.is_view_table()) {
      // view table do not check column info
    } else if (table.get_max_used_column_id() < hard_code_table.get_max_used_column_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("max_used_column_id mismatch", K(table_name), "in_memory",
          table.get_max_used_column_id(), "hard_code",
          hard_code_table.get_max_used_column_id(), K(ret));
    } else if (table.get_rowkey_column_num() != hard_code_table.get_rowkey_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("rowkey_column_num mismatch", K(table_name), "in_memory",
          table.get_rowkey_column_num(), "hard_code",
          hard_code_table.get_rowkey_column_num(), K(ret));
    } else if (table.get_index_column_num() != hard_code_table.get_index_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_column_num mismatch", K(table_name), "in_memory",
          table.get_index_column_num(), "hard_code",
          hard_code_table.get_index_column_num(), K(ret));
    } else if (table.get_rowkey_split_pos() != hard_code_table.get_rowkey_split_pos()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("rowkey_split_pos mismatch", K(table_name), "in_memory",
          table.get_rowkey_split_pos(), "hard_code",
          hard_code_table.get_rowkey_split_pos(), K(ret));
    } else if (table.get_partition_key_column_num()
        != hard_code_table.get_partition_key_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partition_key_column_num mismatch", K(table_name), "in_memory",
          table.get_partition_key_column_num(), "hard_code",
          hard_code_table.get_partition_key_column_num(), K(ret));
    } else if (table.get_subpartition_key_column_num()
        != hard_code_table.get_subpartition_key_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partition_key_column_num mismatch", K(table_name), "in_memory",
          table.get_subpartition_key_column_num(), "hard_code",
          hard_code_table.get_subpartition_key_column_num(), K(ret));
    } else if (table.get_autoinc_column_id() != hard_code_table.get_autoinc_column_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("autoinc_column_id mismatch", K(table_name), "in_memory",
          table.get_autoinc_column_id(), "hard_code",
          hard_code_table.get_autoinc_column_id(), K(ret));
    }

    // options may be different between different ob instance, don't check
    // block_size
    // is_user_bloomfilter
    // progressive_merge_num
    // replica_num
    // index_status
    // name_case_mode
    // charset_type
    // collation_type
    // schema_version
    // comment
    // compress_func_name
    // expire_info
    // zone_list
    // primary_zone
    // part_expr.part_num_
    // sub_part_expr.part_num_
    // store_format
    // row_store_type
    // progressive_merge_round
    // storage_format_version
  }
  return ret;
}

int ObRootInspection::check_column_schema_(const ObString &table_name,
                                           const ObColumnSchemaV2 &column,
                                           const ObColumnSchemaV2 &hard_code_column,
                                           const bool ignore_column_id)
{
  int ret = OB_SUCCESS;
  if (table_name.empty() || !column.is_valid() || !hard_code_column.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is empty or invalid column or invalid hard_code_column",
             KR(ret), K(table_name), K(column), K(hard_code_column));
  } else {
#define CMP_COLUMN_ATTR(attr) \
  if (OB_SUCC(ret)) { \
    if (column.get_##attr() != hard_code_column.get_##attr()) { \
      ret = OB_SCHEMA_ERROR; \
      LOG_WARN(#attr " mismatch", KR(ret), K(table_name), "column_name", column.get_column_name(), \
               "in_memory", column.get_##attr(), "hard_code", hard_code_column.get_##attr()); \
    } \
  }

#define CMP_COLUMN_IS_ATTR(attr) \
  if (OB_SUCC(ret)) { \
    if (column.is_##attr() != hard_code_column.is_##attr()) { \
      ret = OB_SCHEMA_ERROR; \
      LOG_WARN(#attr " mismatch", KR(ret), K(table_name), "column_name", column.get_column_name(), \
               "in_memory", column.is_##attr(), "hard_code", hard_code_column.is_##attr()); \
    } \
  }
    if (OB_SUCC(ret)) {
      if (column.get_column_name_str() != hard_code_column.get_column_name_str()) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("column_name mismatch", KR(ret), K(table_name),
                 "in_memory", column.get_column_name(),
                 "hard_code", hard_code_column.get_column_name());
      }
    }

    if (!ignore_column_id) {
      CMP_COLUMN_ATTR(column_id);
    }
    CMP_COLUMN_ATTR(tenant_id);
    CMP_COLUMN_ATTR(table_id);
    // don't need to check schema version
    CMP_COLUMN_ATTR(rowkey_position);
    CMP_COLUMN_ATTR(index_position);
    CMP_COLUMN_ATTR(order_in_rowkey);
    CMP_COLUMN_ATTR(tbl_part_key_pos);
    CMP_COLUMN_ATTR(meta_type);
    CMP_COLUMN_ATTR(accuracy);
    CMP_COLUMN_ATTR(data_length);
    CMP_COLUMN_IS_ATTR(nullable);
    CMP_COLUMN_IS_ATTR(zero_fill);
    CMP_COLUMN_IS_ATTR(autoincrement);
    CMP_COLUMN_IS_ATTR(hidden);
    CMP_COLUMN_IS_ATTR(on_update_current_timestamp);
    CMP_COLUMN_ATTR(charset_type);
    // don't need to check orig default value
    if (ObString("row_store_type") == column.get_column_name()
        && (ObString("__all_table") == table_name || ObString("__all_table_history") == table_name)) {
      // row_store_type may have two possible default values
    } else {
      CMP_COLUMN_ATTR(cur_default_value);
    }
    CMP_COLUMN_ATTR(comment);

  }

#undef CMP_COLUMN_IS_ATTR
#undef CMP_COLUMN_INT_ATTR
  return ret;
}

int ObRootInspection::check_data_version_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(construct_tenant_ids_(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    FOREACH_X(tenant_id, tenant_ids, OB_SUCC(ret)) {
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", KR(ret));
      } else if (OB_FAIL(check_tenant_status_(*tenant_id))) {
        LOG_WARN("fail to check tenant status", KR(ret), K(*tenant_id));
      } else if (OB_TMP_FAIL(check_data_version_(*tenant_id))) {
        LOG_WARN("fail to check data version by tenant", KR(tmp_ret), K(*tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
    } // end foreach
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_data_version_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else {
    share::ObGlobalStatProxy proxy(*sql_proxy_, tenant_id);
    uint64_t target_data_version = 0;
    uint64_t current_data_version = 0;
    uint64_t compatible_version = 0;
    bool for_update = false;
    if (OB_FAIL(proxy.get_target_data_version(for_update, target_data_version))) {
      LOG_WARN("fail to get target data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(proxy.get_current_data_version(current_data_version))) {
      LOG_WARN("fail to get current data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compatible_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    } else if (target_data_version != current_data_version
               || target_data_version != compatible_version
               || target_data_version != DATA_CURRENT_VERSION) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("data_version not match, upgrade process should be run",
               KR(ret), K(tenant_id), K(target_data_version),
               K(current_data_version), K(compatible_version));
    }
  }
  return ret;
}

int ObRootInspection::check_cancel()
{
  int ret = OB_SUCCESS;
  if (stopped_) {
    ret = OB_CANCELED;
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", KR(ret));
  } else if (!GCTX.root_service_->is_full_service()) {
    ret = OB_CANCELED;
  }
  return ret;
}

int ObRootInspection::check_tenant_status_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObSimpleTenantSchema *tenant = NULL;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant)) {
    // tenant may has been dropped;
    ret = OB_EAGAIN;
    LOG_WARN("tenant may be dropped, don't continue", KR(ret), K(tenant_id));
  } else if (!tenant->is_normal()) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant status is not noraml, should check next round", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get tenant schema version", KR(ret), K(tenant_id));
  } else if (!ObSchemaService::is_formal_version(schema_version)) {
    ret = OB_EAGAIN;
    LOG_WARN("schema version is not formal, observer may be restarting or inner table schema changed, "
             "should check next round", KR(ret), K(tenant_id), K(schema_version));
  }
  return ret;
}

ObUpgradeInspection::ObUpgradeInspection()
  : inited_(false), schema_service_(NULL), root_inspection_(NULL)
{
}

ObUpgradeInspection::~ObUpgradeInspection()
{
}

int ObUpgradeInspection::init(ObMultiVersionSchemaService &schema_service,
                              ObRootInspection &root_inspection)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    root_inspection_ = &root_inspection;
    inited_ = true;
  }
  return ret;
}

int ObUpgradeInspection::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard error", K(ret));
  } else if (!start_to_read_) {
    const ObTableSchema *table_schema = NULL;
    const uint64_t table_id = OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID;
    if (OB_FAIL(schema_guard.get_table_schema(OB_SYS_TENANT_ID, table_id, table_schema))) {
      LOG_WARN("get_table_schema failed", K(table_id), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", KP(table_schema), K(ret));
    } else {
      ObArray<Column> columns;

#define ADD_ROW(name, info) \
  do { \
    columns.reuse(); \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(get_full_row(table_schema, name, info, columns))) { \
      LOG_WARN("get_full_row failed", "table_schema", *table_schema, \
          K(name), K(info), K(ret)); \
    } else if (OB_FAIL(project_row(columns, cur_row_))) { \
      LOG_WARN("project_row failed", K(columns), K(ret)); \
    } else if (OB_FAIL(scanner_.add_row(cur_row_))) { \
      LOG_WARN("add_row failed", K(cur_row_), K(ret)); \
    } \
  } while (false)

#define CHECK_RESULT(checked, value) (checked ? (value ? "succeed" : "failed") : "checking")

      ADD_ROW("zone_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_zone_passed()));
      ADD_ROW("sys_stat_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_sys_stat_passed()));
      ADD_ROW("sys_param_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_sys_param_passed()));
      ADD_ROW("sys_table_schema_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_sys_table_schema_passed()));
      ADD_ROW("data_version_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_data_version_passed()));

      bool upgrade_job_passed = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < UPGRADE_JOB_TYPE_COUNT; i++) {
        int tmp = OB_SUCCESS;
        ObRsJobType job_type = upgrade_job_type_array[i];
        if (job_type > JOB_TYPE_INVALID && job_type < JOB_TYPE_MAX) {
          if (OB_SUCCESS != (tmp = ObUpgradeUtils::check_upgrade_job_passed(job_type))) {
            LOG_WARN("fail to check upgrade job passed", K(tmp), K(job_type));
            upgrade_job_passed = false;
          }
          ADD_ROW(ObRsJobTableOperator::get_job_type_str(job_type),
                  CHECK_RESULT(root_inspection_->is_all_checked(), (OB_SUCCESS == tmp)));
        }
      }

      ADD_ROW("all_check", CHECK_RESULT(root_inspection_->is_all_checked(),
              (root_inspection_->is_all_passed() && upgrade_job_passed)));

#undef CHECK_RESULT
#undef ADD_ROW
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get_next_row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;

}

int ObUpgradeInspection::get_full_row(const share::schema::ObTableSchema *table,
                                      const char *name, const char *info,
                                      ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table || NULL == name || NULL == info) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(table), KP(name), KP(info), K(ret));
  } else {
    ADD_COLUMN(set_varchar, table, "name", name, columns);
    ADD_COLUMN(set_varchar, table, "info", info, columns);
  }

  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
