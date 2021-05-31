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
#include "share/schema/ob_part_mgr_util.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_gc_partition_builder.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace rootserver {

int64_t ObBuildGCPartitionTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObBuildGCPartitionTask::deep_copy(char* buf, const int64_t buf_size) const
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
    task = new (buf) ObBuildGCPartitionTask(*gc_partition_builder_);
  }
  return task;
}

int ObBuildGCPartitionTask::process()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to do build __all_tenant_gc_partition task", K(start));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gc_partition_builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc_partition_builder is null", K(ret));
  } else if (OB_FAIL(gc_partition_builder_->build())) {
    LOG_WARN("fail to build __all_tenant_gc_partition", K(ret));
  }
  LOG_INFO(
      "[UPGRADE] finish build __all_tenant_gc_partition", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

ObGCPartitionBuilder::ObGCPartitionBuilder()
    : inited_(false), stopped_(false), build_(false), rwlock_(), sql_proxy_(NULL), schema_service_(NULL)
{}

int ObGCPartitionBuilder::init(
    share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can't init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    stopped_ = false;
    build_ = false;
    inited_ = true;
  }
  return ret;
}

void ObGCPartitionBuilder::start()
{
  SpinWLockGuard guard(rwlock_);
  stopped_ = false;
}

int ObGCPartitionBuilder::stop()
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
    } else if (!build_) {
      break;
    } else {
      usleep(WAIT_US);
    }
  }
  return ret;
}

int ObGCPartitionBuilder::check_stop()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("builder should stopped", K(ret));
  }
  return ret;
}

int ObGCPartitionBuilder::set_build_mark()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_ || build_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't run job at the same time", K(ret));
  } else {
    build_ = true;
  }
  return ret;
}

// There's no concurrency in one observer.
// Disable DDL is not required for this logic.
int ObGCPartitionBuilder::build()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(set_build_mark())) {
    LOG_WARN("fail to build __all_tenant_gc_partition", K(ret));
  } else {
    int64_t job_id = OB_INVALID_ID;
    bool can_run_job = false;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_BUILD_GC_PARTITION;
    if (OB_FAIL(ObUpgradeUtils::can_run_upgrade_job(job_type, can_run_job))) {
      LOG_WARN("fail to check if can run upgrade job now", K(ret), K(job_type));
    } else if (!can_run_job) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("no job exist or success job exist", K(ret));
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy_ is null", K(ret));
    } else if (OB_FAIL(check_stop())) {
      LOG_WARN("builder should stopped", K(ret));
    } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
      LOG_WARN("fail to create rs job", K(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_id is invalid", K(ret), K(job_id));
    } else if (OB_FAIL(build_gc_partition_info())) {
      LOG_WARN("fail to build gc partition info", K(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (job_id > 0) {
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
        ret = OB_FAIL(ret) ? ret : tmp_ret;
      }
    }
    build_ = false;
  }
  return ret;
}

/* 1. This function should be called by upgrade cmd before build().
 * 2. This function should be called synchronously before cluster_version rises to ver 2.0/2.1.x.
 * 3. Enable_ddl/enable_sys_table_ddl should be true by upgrade script before calling this function.
 */
int ObGCPartitionBuilder::create_tables()
{
  ObCurTraceId::init(GCONF.self_addr_);
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to create __all_tenant_gc_partition_info", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  int64_t job_id = OB_INVALID_ID;
  ObRsJobType job_type = ObRsJobType::JOB_TYPE_BUILD_GC_PARTITION;
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
  } else if (OB_FAIL(ObUpgradeUtils::create_tenant_tables(*schema_service_, OB_ALL_TENANT_GC_PARTITION_INFO_TID))) {
    LOG_WARN("fail to create tenant tables", K(ret));
  } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
    LOG_WARN("fail to create rs job", K(ret));
  } else if (job_id <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job_id is invalid", K(ret), K(job_id));
  } else if (OB_FAIL(RS_JOB_COMPLETE(job_id, OB_CANCELED, *sql_proxy_))) {
    LOG_WARN("fail to complete job", K(ret), K(job_id));
  }
  LOG_INFO("[UPGRADE] create __all_tenant_gc_partition_info finish",
      K(ret),
      "cost_us",
      ObTimeUtility::current_time() - start);
  return ret;
}

/* This upgrade job is not required for enable_ddl is true.
 * For compatibility, following actions are required:
 * 1. Create all tenant's __all_tenant_gc_partition_info before cluster_version rises to ver 2.0/2.1.x.
 * 2. Failed upgrade record should be inserted into __all_rootservice_job to distinguish different upgrade path.
 * 3. Try to execute this upgrade job after cluster_version rises to ver 2.0/2.1.x.
 * 4. Partition related DDL should insert records into __all_tenant_gc_partition_info after cluster_version rises to
 * ver 2.0/2.1.x.
 * 5. Enable new GC logic after upgrade job is done.
 */
int ObGCPartitionBuilder::build_gc_partition_info()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to build gc partition info", K(start));
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObSchemaGetterGuard schema_guard;
  ObString enable_ddl("enable_ddl");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("builder should stopped", K(ret));
  } else {
    for (int64_t i = tenant_ids.size() - 1; i >= 0 && OB_SUCC(ret); i--) {
      if (OB_FAIL(build_tenant_gc_partition_info(tenant_ids.at(i)))) {
        LOG_WARN("build __all_tenant_gc_partition_info failed", K(ret), "tenant_id", tenant_ids.at(i));
      }
    }
  }
  LOG_INFO("[UPGRADE] build gc partition info finish", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObGCPartitionBuilder::build_tenant_gc_partition_info(uint64_t tenant_id)
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to build tenant gc partition info", K(tenant_id), K(start));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2*> table_schemas;
  ObSEArray<common::ObPartitionKey, BATCH_INSERT_COUNT> partitions;
  int64_t partition_cnt = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("builder should stopped", K(ret));
  } else {
    const ObSimpleTableSchemaV2* table_schema = NULL;
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      table_schema = table_schemas.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("builder should stopped", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_schema is null", K(ret));
      } else if (!table_schema->has_self_partition()) {
        // only deal with tables with partitions
      } else {
        bool check_dropped_schema = false;
        ObTablePartitionKeyIter partition_iter(*table_schema, check_dropped_schema);
        uint64_t table_id = table_schema->get_table_id();
        int64_t partition_id = OB_INVALID_INDEX;
        int64_t partition_cnt = table_schema->get_partition_cnt();
        ObPartitionKey part_key;
        while (OB_SUCC(ret) && OB_SUCC(partition_iter.next_partition_id_v2(partition_id))) {
          if (partitions.count() >= BATCH_INSERT_COUNT) {
            if (OB_FAIL(batch_insert_partitions(partitions))) {
              LOG_WARN("batch insert partitions failed", K(ret));
            }
            partitions.reset();
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(part_key.init(table_id, partition_id, partition_cnt))) {
            LOG_WARN("fail to init part_key", K(ret), K(table_id), K(partition_id), K(partition_cnt));
          } else if (OB_FAIL(partitions.push_back(part_key))) {
            LOG_WARN("fail to push back part_key", K(ret), K(part_key));
          } else {
            partition_cnt++;
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(batch_insert_partitions(partitions))) {
        LOG_WARN("batch insert partitions failed", K(ret));
      }
      partitions.reset();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_tenant_gc_partition_info(tenant_id, partition_cnt))) {
        LOG_WARN("fail to check __all_tenant_gc_partition_info count matched", K(ret), K(tenant_id), K(partition_cnt));
      }
    }
  }
  LOG_INFO("[UPGRADE] build tenant gc partition info finish",
      K(ret),
      K(tenant_id),
      "cost_us",
      ObTimeUtility::current_time() - start);
  return ret;
}

int ObGCPartitionBuilder::batch_insert_partitions(common::ObIArray<common::ObPartitionKey>& partitions)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("builder should stopped", K(ret));
  } else if (partitions.count() > 0) {
    ObSqlString sql;
    uint64_t tenant_id = partitions.at(0).get_tenant_id();
    for (int64_t i = 0; i < partitions.count() && OB_SUCC(ret); i++) {
      uint64_t table_id = partitions.at(i).get_table_id();
      int64_t partition_id = partitions.at(i).get_partition_id();
      if (OB_FAIL(check_stop())) {
        LOG_WARN("builder should stopped", K(ret));
      } else if (0 == i) {
        if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (tenant_id, table_id, partition_id) VALUES (%lu, %lu, %lu)",
                OB_ALL_TENANT_GC_PARTITION_INFO_TNAME,
                tenant_id,
                table_id,
                partition_id))) {
          LOG_WARN("fail to assign sql", K(ret));
        }
      } else {
        if (OB_FAIL(sql.append_fmt(", (%lu, %lu, %lu)", tenant_id, table_id, partition_id))) {
          LOG_WARN("fail to append sql", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(" ON DUPLICATE KEY UPDATE tenant_id = tenant_id"))) {
        LOG_WARN("fail to append fmt", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(ret));
      } else {
        // This upgrade job runs after cluster_version rised,
        // so affected_rows may be less than partition_cnt because enable_ddl is true
        // and partition related ddl may already insert new records into __all_tenant_gc_partition_info.
      }
    }
  }
  return ret;
}

int ObGCPartitionBuilder::check_tenant_gc_partition_info(uint64_t tenant_id, int64_t partition_cnt)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else if (partition_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_cnt is invalid", K(ret), K(partition_cnt));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("builder should stopped", K(ret));
  } else if (partition_cnt > 0) {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult* result = NULL;
      if (OB_ISNULL(sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_proxy is null", K(ret));
      } else if (OB_FAIL(sql.assign_fmt(
                     "SELECT floor(count(*)) as count FROM %s", OB_ALL_TENANT_GC_PARTITION_INFO_TNAME))) {
        LOG_WARN("fail to append sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get result", K(ret));
      } else {
        int32_t count = OB_INVALID_INDEX;
        EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);
        if (OB_FAIL(ret)) {
        } else if (count < partition_cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition_cnt not matched", K(ret), K(tenant_id), K(count), K(partition_cnt));
        }
      }
    }
  }
  return ret;
}

int ObGCPartitionBuilder::can_build()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_ || build_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("status not matched", K(ret), "stopped", stopped_ ? "true" : "false", "build", build_ ? "true" : "false");
  }
  return ret;
}
}  // namespace rootserver
}  // namespace oceanbase
