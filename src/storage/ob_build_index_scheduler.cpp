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

#define USING_LOG_PREFIX STORAGE

#include "ob_build_index_scheduler.h"
#include "share/ob_index_trans_status_reporter.h"
#include "share/ob_index_status_table_operator.h"
#include "share/ob_index_task_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_utils.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_sstable_checksum_operator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_thread_mgr.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_build_index_task.h"
#include "storage/ob_all_server_tracer.h"
#include "storage/ob_partition_storage.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/ob_tenant_meta_memory_mgr.h"
#include "storage/ob_pg_storage.h"
#include "observer/ob_service.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_index_builder.h"

using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObBuildIndexBaseTask::ObBuildIndexBaseTask(const ObIDDLTaskType task_type) : ObIDDLTask(task_type)
{}

ObBuildIndexBaseTask::~ObBuildIndexBaseTask()
{}

int ObBuildIndexBaseTask::check_partition_exist_in_current_server(const ObTableSchema& index_schema,
    const ObTableSchema& table_schema, const ObPartitionKey& pkey, ObIPartitionGroupGuard& guard, bool& exist)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  if (table_schema.get_binding()) {
    common::ObPGKey pg_key;
    if (OB_FAIL(table_schema.get_pg_key(pkey, pg_key))) {
      STORAGE_LOG(WARN, "fail to get pg key", K(ret), K(pkey));
    } else if (OB_FAIL(ObPartitionService::get_instance().check_pg_partition_exist(pg_key, pkey))) {
      if (OB_PG_PARTITION_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
        STORAGE_LOG(INFO, "partition not bind to pg now, retry later", K(ret), K(pg_key), K(pkey));
      } else if (OB_PARTITION_NOT_EXIST == ret) {
        STORAGE_LOG(INFO, "pg not exist, do not need to build index", K(ret), K(pg_key), K(pkey));
        if (OB_FAIL(check_restore_need_retry(pkey.get_tenant_id(), need_retry))) {
          STORAGE_LOG(WARN, "fail to check restore need retry", K(ret));
        } else if (need_retry) {
          ret = OB_EAGAIN;
        }
      } else {
        STORAGE_LOG(WARN, "fail to check pg partition exist", K(ret), K(pg_key), K(pkey));
      }
      exist = false;
    } else {
      exist = true;
    }
  }

  if (OB_SUCC(ret) && exist) {
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(
            INFO, "partition not exist, do not need to build index", K(pkey), "index_id", index_schema.get_table_id());
        exist = false;
        ret = OB_SUCCESS;
        if (OB_FAIL(check_restore_need_retry(pkey.get_tenant_id(), need_retry))) {
          STORAGE_LOG(WARN, "fail to check restore need retry", K(ret));
        } else if (need_retry) {
          ret = OB_EAGAIN;
        }
      } else {
        STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey), "index_id", index_schema.get_table_id());
      }
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, partition should not be null", K(ret));
    } else {
      exist = ObReplicaTypeCheck::is_replica_with_ssstore(partition->get_replica_type());
    }
  }
  return ret;
}

int ObBuildIndexBaseTask::check_restore_need_retry(const uint64_t tenant_id, bool& need_retry)
{
  int ret = OB_SUCCESS;
  bool is_restore = false;
  need_retry = false;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObMultiVersionSchemaService::get_instance().check_tenant_is_restore(nullptr, tenant_id, is_restore))) {
    if (OB_TENANT_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to check tenant is restore", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (is_restore) {
    PhysicalRestoreStatus restore_status;
    if (OB_FAIL(ObBackupInfoMgr::get_instance().get_restore_status(tenant_id, restore_status))) {
      STORAGE_LOG(WARN, "fail to get restore info", K(ret));
    } else {
      need_retry = restore_status <= PHYSICAL_RESTORE_USER_REPLICA;
    }
  }
  return ret;
}

int ObBuildIndexBaseTask::report_index_status(const uint64_t index_table_id, const int64_t partition_id,
    const ObIndexStatus index_status, const int build_index_ret, const ObRole role)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == index_table_id || partition_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(index_table_id), K(partition_id));
  } else {
    ObIndexStatusTableOperator::ObBuildIndexStatus status;
    status.index_status_ = index_status;
    status.role_ = role;
    status.ret_code_ = build_index_ret;
    if (OB_FAIL(ObIndexStatusTableOperator::report_build_index_status(
            index_table_id, partition_id, GCTX.self_addr_, status, *GCTX.sql_proxy_))) {
      STORAGE_LOG(WARN, "fail to report build index status", K(ret), K(index_table_id), K(partition_id), K(status));
    }
  }
  return ret;
}

int ObBuildIndexBaseTask::check_partition_need_build_index(const ObPartitionKey& pkey,
    const ObTableSchema& index_schema, const ObTableSchema& data_table_schema, ObIPartitionGroupGuard& guard,
    bool& need_build)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* new_index_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  bool is_partition_exist = false;
  bool check_dropped_partition = true;
  bool is_split_finished = true;
  need_build = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantDDLCheckSchemaTask has not been inited", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
                 extract_tenant_id(pkey.get_table_id()), schema_guard))) {
    STORAGE_LOG(WARN, "fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_schema.get_table_id(), new_index_schema))) {
    STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(pkey), "index_id", index_schema.get_table_id());
  } else if (OB_ISNULL(new_index_schema)) {
    need_build = false;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "The table does not exist, no need to create index, ", K(index_schema.get_table_id()));
  } else if (OB_FAIL(schema_guard.check_partition_exist(
                 pkey.get_table_id(), pkey.get_partition_id(), check_dropped_partition, is_partition_exist))) {
    STORAGE_LOG(WARN, "fail to check partition exist", K(ret), K(pkey), K(index_schema.get_table_id()));
  } else if (!is_partition_exist) {
    STORAGE_LOG(WARN, "partition has been droped, do not need build index", K(pkey), K(index_schema.get_table_id()));
  } else if (OB_FAIL(check_partition_exist_in_current_server(
                 index_schema, data_table_schema, pkey, guard, is_partition_exist))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "fail to check partition exist in current server", K(ret));
    }
  } else if (OB_FAIL(check_partition_split_finish(pkey, is_split_finished))) {
    STORAGE_LOG(WARN, "fail to check partition split finish", K(ret), K(pkey));
  } else if (!is_split_finished) {
    ret = OB_NOT_SUPPORTED;
  } else {
    need_build = is_partition_exist && new_index_schema->is_storage_local_index_table() &&
                 INDEX_STATUS_UNAVAILABLE == new_index_schema->get_index_status();
  }
  return ret;
}

int ObBuildIndexBaseTask::check_partition_split_finish(const ObPartitionKey& pkey, bool& is_split_finished)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  is_split_finished = true;

  ObIPartitionGroup* partition = NULL;
  if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
    }
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, partition should not be null", K(ret));
  } else if (OB_FAIL(partition->get_pg_storage().check_physical_split(is_split_finished))) {
    STORAGE_LOG(WARN, "fail to check physical split", K(ret), K(pkey));
  }

  return ret;
}

ObTenantDDLCheckSchemaTask::ObTenantDDLCheckSchemaTask()
    : ObBuildIndexBaseTask(DDL_TASK_CHECK_SCHEMA), base_version_(-1), refreshed_version_(-1), tenant_id_(OB_INVALID_ID)
{}

ObTenantDDLCheckSchemaTask::~ObTenantDDLCheckSchemaTask()
{}

int ObTenantDDLCheckSchemaTask::init(
    const uint64_t tenant_id, const int64_t base_version, const int64_t refreshed_version)
{
  int ret = OB_SUCCESS;
  if (base_version < 0 || refreshed_version < 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(base_version), K(refreshed_version), K(tenant_id));
  } else {
    base_version_ = base_version;
    refreshed_version_ = refreshed_version;
    task_id_.init(GCTX.self_addr_);
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

bool ObTenantDDLCheckSchemaTask::operator==(const ObIDDLTask& other) const
{
  bool is_equal = false;
  if (get_type() == other.get_type()) {
    const ObTenantDDLCheckSchemaTask& task = static_cast<const ObTenantDDLCheckSchemaTask&>(other);
    is_equal = base_version_ == task.base_version_ && refreshed_version_ == task.refreshed_version_;
  }
  return is_equal;
}

int64_t ObTenantDDLCheckSchemaTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&base_version_, sizeof(base_version_), hash_val);
  hash_val = murmurhash(&refreshed_version_, sizeof(refreshed_version_), hash_val);
  return hash_val;
}

int ObTenantDDLCheckSchemaTask::find_build_index_partitions(
    const ObTableSchema* index_schema, ObSchemaGetterGuard& guard, common::ObIArray<ObPartitionKey>& partition_keys)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  partition_keys.reuse();
  bool need_retry = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantDDLCheckSchemaTask has not been inited", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(index_schema));
  } else if (!index_schema->is_storage_local_index_table()) {
    // do nothing
  } else if (OB_FAIL(guard.get_table_schema(index_schema->get_data_table_id(), table_schema))) {
    STORAGE_LOG(WARN, "fail to get table schema", K(ret), "table_id", index_schema->get_data_table_id());
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "error unexpected, table schema must not be NULL",
        K(ret),
        "index_id",
        index_schema->get_table_id(),
        "data_table_id",
        index_schema->get_data_table_id());
  } else if (table_schema->is_vir_table()) {
    // fast path: virtual table do not need to create index
  } else {
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter part_iter(*table_schema, check_dropped_schema);
    const int64_t part_num = part_iter.get_partition_num();
    ObPartitionKey pkey;
    bool need_build = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
      ObIPartitionGroupGuard part_guard;
      if (OB_FAIL(part_iter.next_partition_key_v2(pkey))) {
        STORAGE_LOG(WARN, "fail to get next partition key", K(ret));
      } else if (OB_FAIL(
                     check_partition_need_build_index(pkey, *index_schema, *table_schema, part_guard, need_build))) {
        if (OB_EAGAIN != ret) {
          STORAGE_LOG(WARN, "fail to check partition need build index", K(ret), K(pkey));
        } else {
          ret = OB_SUCCESS;
          need_retry = true;
        }
      } else if (need_build) {
        if (OB_FAIL(partition_keys.push_back(pkey))) {
          STORAGE_LOG(WARN, "fail to push back partition key", K(ret));
        }
      }
      if (OB_NOT_SUPPORTED == ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS !=
            (tmp_ret = report_index_status(
                 index_schema->get_table_id(), pkey.get_partition_id(), INDEX_STATUS_UNAVAILABLE, ret, LEADER))) {
          STORAGE_LOG(WARN, "fail to report index status", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  if (OB_SUCC(ret) && partition_keys.count() > 0) {
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "find build index partitions", K(partition_keys), "index_id", index_schema->get_table_id());
  }
  if (OB_SUCC(ret) && need_retry) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTenantDDLCheckSchemaTask::create_index_partition_table_store(
    const common::ObPartitionKey& pkey, const uint64_t index_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard part_guard;
  ObPGPartitionGuard pg_partition_guard;
  UNUSED(schema_version);
  if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey), K(index_id));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, part_guard))) {
    if (OB_PARTITION_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret || OB_PARTITION_IS_REMOVED == ret) {
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "partition not exist, do not need to build index", K(pkey), K(index_id));
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey), K(index_id));
    }
  } else if (OB_ISNULL(part_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  } else if (OB_FAIL(part_guard.get_partition_group()->get_pg_storage().create_index_table_store(
                 pkey, index_id, schema_version))) {
    if (OB_ENTRY_EXIST != ret && OB_PARTITION_IS_REMOVED != ret && OB_PARTITION_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to create index table store", K(ret), K(index_id));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTenantDDLCheckSchemaTask::generate_schedule_index_task(const common::ObPartitionKey& pkey,
    const uint64_t index_id, const int64_t schema_version, const bool is_unique_index)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || OB_INVALID_ID == index_id || schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(index_id), K(schema_version));
  } else {
    ObBuildIndexScheduleTask task;
    if (OB_FAIL(create_index_partition_table_store(pkey, index_id, schema_version))) {
      if (OB_PARTITION_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to create index partition table store", K(pkey), K(index_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(task.init(pkey, index_id, schema_version, is_unique_index))) {
      STORAGE_LOG(WARN, "fail to init ObBuildIndexScheduleTask", K(ret));
    } else if (OB_FAIL(ObBuildIndexScheduler::get_instance().push_task(task))) {
      if (OB_ENTRY_EXIST == ret) {
        STORAGE_LOG(INFO, "task has been scheduled before", K(pkey), K(index_id));
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "fail to push back schedule build index task", K(ret), K(pkey), K(index_id));
      }
    }
  }
  return ret;
}

int ObTenantDDLCheckSchemaTask::get_candidate_tables(ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantDDLCheckSchemaTask has not been inited", K(ret));
  } else {
    if (0 == base_version_) {
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
              tenant_id_, schema_guard, refreshed_version_))) {
        if (OB_TENANT_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          STORAGE_LOG(INFO, "tenant is not exist, skip this task", K(tenant_id_), K(refreshed_version_));
        } else {
          STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(refreshed_version_));
        }
      } else if (OB_FAIL(schema_guard.check_formal_guard())) {
        LOG_WARN("schema_guard is not formal", K(ret), K_(tenant_id));
      } else if (OB_FAIL(schema_guard.get_tenant_unavailable_index(tenant_id_, table_ids))) {
        STORAGE_LOG(WARN, "fail to get unavailable index", K(ret));
      }
    } else {
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_all_increment_schema_ids(
              tenant_id_, base_version_, refreshed_version_, OB_DDL_TABLE_OPERATION, table_ids))) {
        STORAGE_LOG(WARN, "fail to get all increment table ids", K(ret), K(base_version_), K(refreshed_version_));
      }
    }
  }
  return ret;
}

int ObTenantDDLCheckSchemaTask::process_schedule_build_index_task()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  ObSchemaGetterGuard schema_guard;
  bool need_retry = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantDDLCheckSchemaTask has not been inited", K(ret));
  } else if (GCTX.is_standby_cluster()) {
    // nothing todo
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "tenant is not exist, skip this task", K(tenant_id_), K(refreshed_version_));
    } else {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(refreshed_version_));
    }
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K_(tenant_id));
  } else if (OB_FAIL(get_candidate_tables(table_ids))) {
    STORAGE_LOG(WARN, "fail to get candidate table ids");
  } else {
    ObArray<ObPartitionKey> partition_keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      // use simple table schema for performance optimization
      const ObTableSchema* simple_index_schema = nullptr;
      const ObTableSchema* index_schema = nullptr;
      const ObTableSchema* data_table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(table_ids.at(i), simple_index_schema))) {
        STORAGE_LOG(WARN, "fail to get table schema", K(ret));
      } else if (OB_ISNULL(simple_index_schema)) {
        ret = OB_SUCCESS;
        STORAGE_LOG(INFO, "table has been deleted, do not need to create index", K(ret), "table_id", table_ids.at(i));
      } else if (!simple_index_schema->is_storage_index_table()) {
        // do nothing
      } else if (OB_FAIL(schema_guard.get_table_schema(table_ids.at(i), index_schema))) {
        STORAGE_LOG(WARN, "fail to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SUCCESS;
        STORAGE_LOG(INFO, "table has been deleted, do not need to create index", K(ret), "table_id", table_ids.at(i));
      } else if (OB_FAIL(find_build_index_partitions(index_schema, schema_guard, partition_keys))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
          need_retry = true;
        } else {
          STORAGE_LOG(WARN, "fail to check need build index", K(ret));
        }
      } else if (partition_keys.count() > 0) {
        if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), data_table_schema))) {
          STORAGE_LOG(WARN, "fail to get data table schema", K(ret));
        } else if (OB_ISNULL(data_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          STORAGE_LOG(WARN, "schema error, data table not exist while index table exist", K(ret));
        } else {
          const int64_t schema_version =
              std::max(index_schema->get_schema_version(), data_table_schema->get_schema_version());
          for (int64_t i = 0; OB_SUCC(ret) && i < partition_keys.count(); ++i) {
            if (OB_FAIL(generate_schedule_index_task(partition_keys.at(i),
                    index_schema->get_table_id(),
                    schema_version,
                    index_schema->is_unique_index()))) {
              STORAGE_LOG(WARN, "fail to generate schedule build index task", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && need_retry) {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObTenantDDLCheckSchemaTask::process_tenant_memory_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRefreshMetaReservedMemoryTask has not been inited", K(ret));
  } else if (OB_FAIL(ObTenantMetaMemoryMgr::get_instance().try_update_tenant_info(tenant_id_, refreshed_version_))) {
    LOG_WARN("fail to try update tenant info", K(ret));
  }
  return ret;
}

int ObTenantDDLCheckSchemaTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> table_ids;

  DEBUG_SYNC(BEFORE_CREATE_INDEX_TASK);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantDDLCheckSchemaTask has not been inited", K(ret));
  } else {
    ObCurTraceId::set(task_id_);
    int64_t parallel_server_target = 0;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ObSchemaUtils::get_tenant_int_variable(
                                       OB_SYS_TENANT_ID, SYS_VAR_PARALLEL_SERVERS_TARGET, parallel_server_target)))) {
      STORAGE_LOG(WARN, "failed to get sys tenant parallel server target", K(tmp_ret));
    } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ObDagScheduler::get_instance().set_create_index_concurrency(
                                              static_cast<int32_t>(parallel_server_target * 2))))) {
      STORAGE_LOG(WARN, "failed to set create index concurrency", K(tmp_ret), K(parallel_server_target));
    }
    if (OB_FAIL(process_schedule_build_index_task())) {
      STORAGE_LOG(WARN, "fail to process build index schema check task", K(ret));
    } else if (OB_FAIL(process_tenant_memory_task())) {
      STORAGE_LOG(WARN, "fail to process tenant memory task", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    need_retry_ = false;
  } else {
    need_retry_ = true;
    STORAGE_LOG(WARN, "ObTenantDDLCheckSchemaTask need retry", K(ret), K(base_version_), K(refreshed_version_));
  }
  return ret;
}

ObIDDLTask* ObTenantDDLCheckSchemaTask::deep_copy(char* buf, const int64_t size) const
{
  int ret = OB_SUCCESS;
  ObTenantDDLCheckSchemaTask* task = NULL;
  UNUSED(size);
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf));
  } else {
    task = new (buf) ObTenantDDLCheckSchemaTask();
    *task = *this;
  }
  return task;
}

ObBuildIndexScheduleTask::ObBuildIndexScheduleTask()
    : ObBuildIndexBaseTask(DDL_TASK_SCHEDULE_BUILD_INDEX),
      pkey_(),
      index_id_(OB_INVALID_ID),
      schema_version_(0),
      state_(WAIT_TRANS_END),
      is_dag_scheduled_(false),
      last_role_(common::FOLLOWER),
      last_active_timestamp_(0),
      is_unique_index_(false),
      is_copy_request_sent_(false),
      retry_cnt_(0),
      build_snapshot_version_(0),
      candidate_replica_()
{}

ObBuildIndexScheduleTask::~ObBuildIndexScheduleTask()
{}

int ObBuildIndexScheduleTask::init(
    const ObPartitionKey& pkey, const uint64_t index_id, const int64_t schema_version, const bool is_unique_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (!pkey.is_valid() || OB_INVALID_ID == index_id || schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(index_id), K(schema_version));
  } else {
    pkey_ = pkey;
    index_id_ = index_id;
    schema_version_ = schema_version;
    is_unique_index_ = is_unique_index;
    task_id_.init(GCTX.self_addr_);
    is_inited_ = true;
  }
  return ret;
}

bool ObBuildIndexScheduleTask::operator==(const ObIDDLTask& other) const
{
  bool is_equal = false;
  if (get_type() == other.get_type()) {
    const ObBuildIndexScheduleTask& task = static_cast<const ObBuildIndexScheduleTask&>(other);
    is_equal = pkey_ == task.pkey_ && index_id_ == task.index_id_;
  }
  return is_equal;
}

int64_t ObBuildIndexScheduleTask::hash() const
{
  uint64_t hash_val = pkey_.hash();
  hash_val = murmurhash(&index_id_, sizeof(index_id_), hash_val);
  return hash_val;
}

int ObBuildIndexScheduleTask::report_trans_status(const int trans_status, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret), K(pkey_), K(index_id_));
  } else if (OB_FAIL(get_role(role))) {
    STORAGE_LOG(WARN, "fail to get role", K(ret));
  } else if (is_strong_leader(role)) {
    const int64_t frozen_version = 0;
    ObIndexTransStatus report_status;
    report_status.server_ = GCTX.self_addr_;
    report_status.trans_status_ = trans_status;
    report_status.snapshot_version_ = snapshot_version;
    report_status.frozen_version_ = frozen_version;
    report_status.schema_version_ = schema_version_;
    if (OB_FAIL(ObIndexTransStatusReporter::report_wait_trans_status(index_id_,
            ObIndexTransStatusReporter::OB_SERVER,
            pkey_.get_partition_id(),
            report_status,
            *GCTX.sql_proxy_))) {
      STORAGE_LOG(WARN, "fail to report trans wait status", K(ret), K(pkey_), K(index_id_));
    }
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObBuildIndexScheduleTask::get_snapshot_version(int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  const int64_t partition_id = -1;  // special partition idx
  ObIndexTransStatus status;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(ObIndexTransStatusReporter::get_wait_trans_status(
                 index_id_, ObIndexTransStatusReporter::ROOT_SERVICE, partition_id, *GCTX.sql_proxy_, status))) {
    STORAGE_LOG(WARN, "fail to check report record exist", K(ret), K(pkey_), K(index_id_));
  } else {
    snapshot_version = status.snapshot_version_;
    if (-1 == snapshot_version) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::check_trans_end(bool& is_trans_end, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  ObIndexTransStatus status;
  is_trans_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(ObIndexTransStatusReporter::get_wait_trans_status(index_id_,
                 ObIndexTransStatusReporter::OB_SERVER,
                 pkey_.get_partition_id(),
                 *GCTX.sql_proxy_,
                 status))) {
    STORAGE_LOG(WARN, "fail to check report record exist", K(ret), K(pkey_), K(index_id_));
  } else if (-1 != status.snapshot_version_) {
    is_trans_end = true;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "wait trans already end", K(index_id_));
  } else if (OB_FAIL(ObPartitionService::get_instance().check_schema_version_elapsed(
                 pkey_, schema_version_, index_id_, status.snapshot_version_))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "fail to check schema version eclapsed", K(ret), K(pkey_), K(index_id_));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    is_trans_end = true;
  }
  if (OB_SUCC(ret)) {
    snapshot_version = status.snapshot_version_;
  }
  return ret;
}

int ObBuildIndexScheduleTask::wait_trans_end(const bool is_leader)
{
  int ret = OB_SUCCESS;
  bool is_trans_end = false;
  int64_t snapshot_version = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (!is_leader) {
    state_ = WAIT_SNAPSHOT_READY;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "enter wait snapshot ready", K(pkey_), K(index_id_));
  } else if (OB_FAIL(check_trans_end(is_trans_end, snapshot_version))) {
    STORAGE_LOG(WARN, "fail to check trans end", K(ret), K(pkey_), K(index_id_));
  } else if (is_trans_end) {
    DEBUG_SYNC(BEFORE_LOCAL_INDEX_WAIT_TRANS_END_MID);
    if (OB_FAIL(report_trans_status(ret, snapshot_version))) {
      if (OB_LIKELY(OB_EAGAIN == ret)) {
        STORAGE_LOG(
            WARN, "partition changes to be follower", K(ret), K(pkey_), "server addr", GCTX.self_addr_, K(index_id_));
      } else {
        STORAGE_LOG(WARN, "fail to report trans status", K(ret), K(pkey_), K(index_id_));
      }
    } else {
      state_ = WAIT_SNAPSHOT_READY;
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "enter wait snapshot ready", K(pkey_), K(index_id_));
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::check_rs_snapshot_elapsed(const int64_t snapshot_version, bool& is_elapsed)
{
  int ret = OB_SUCCESS;
  is_elapsed = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(snapshot_version));
  } else if (OB_FAIL(OB_TS_MGR.wait_gts_elapse(extract_tenant_id(index_id_), snapshot_version))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "fail to wait gts elapse", K(ret), K(pkey_), K(index_id_));
    }
  } else if (OB_FAIL(ObPartitionService::get_instance().check_ctx_create_timestamp_elapsed(pkey_, snapshot_version))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to check ctx create timestmap elapsed", K(ret), K(pkey_), K(index_id_));
    }
  } else {
    is_elapsed = true;
  }
  return ret;
}

int ObBuildIndexScheduleTask::wait_snapshot_ready(const bool is_leader)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(get_snapshot_version(snapshot_version))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get snapshot version", K(ret), K(pkey_), K(index_id_));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (0 != snapshot_version) {
    DEBUG_SYNC(BEFORE_LOCAL_INDEX_WAIT_SNAPSHOT_READY_MID);
    bool is_elapsed = false;
    if (OB_FAIL(check_rs_snapshot_elapsed(snapshot_version, is_elapsed))) {
      if (OB_EAGAIN != ret) {
        STORAGE_LOG(WARN, "fail to check rs snapshot elapsed", K(ret), K(pkey_), K(index_id_));
      } else {
        // wait next round check
        ret = OB_SUCCESS;
      }
    } else if (is_elapsed) {
      build_snapshot_version_ = snapshot_version;
      state_ = is_leader ? CHOOSE_BUILD_INDEX_REPLICA : WAIT_CHOOSE_OR_BUILD_INDEX_END;
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "enter state", K(pkey_), K(index_id_), K(state_));
    }
  } else if (is_leader) {
    ObIndexTransStatus status;
    if (OB_FAIL(ObIndexTransStatusReporter::get_wait_trans_status(
            index_id_, ObIndexTransStatusReporter::OB_SERVER, pkey_.get_partition_id(), *GCTX.sql_proxy_, status))) {
      STORAGE_LOG(WARN, "fail to check report record exist", K(ret), K(pkey_), K(index_id_));
    } else if (-1 == status.snapshot_version_) {
      if (OB_FAIL(rollback_state(WAIT_TRANS_END))) {
        STORAGE_LOG(WARN, "fail to rollback state", K(ret), K(pkey_), K(index_id_));
      }
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::copy_build_index_data(const bool is_leader)
{
  int ret = OB_SUCCESS;
  bool need_copy = false;
  UNUSED(is_leader);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().check_single_replica_major_sstable_exist(pkey_, index_id_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      need_copy = true;
    } else {
      STORAGE_LOG(WARN, "fail to check replica has major sstable", K(ret), K(pkey_), K(index_id_));
    }
  }

  DEBUG_SYNC(BEFORE_LOCAL_INDEX_COPY_BUILD_INDEX_DATA_MID);
  if (OB_SUCC(ret)) {
    if (need_copy) {
      if (is_copy_request_sent_) {
        const int64_t now = ObTimeUtility::current_time();
        const int64_t timeout = last_active_timestamp_ + COPY_BUILD_INDEX_DATA_TIMEOUT;
        if (timeout < now) {
          is_copy_request_sent_ = false;
          last_active_timestamp_ = 0;
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(INFO, "reset send copy replica rpc", K(ret), K(pkey_), K(index_id_));
        } else {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            ObTaskController::get().allow_next_syslog();
            STORAGE_LOG(INFO, "wait copy build index end", K(pkey_), K(index_id_));
          }
        }
      } else if (OB_FAIL(send_copy_replica_rpc())) {
        if (OB_ENTRY_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to send copy replica rpc", K(ret), K(pkey_), K(index_id_));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        is_copy_request_sent_ = true;
        last_active_timestamp_ = ObTimeUtility::current_time();
        ObTaskController::get().allow_next_syslog();
        ++retry_cnt_;
        STORAGE_LOG(INFO, "send copy replica rpc", K(pkey_), K(index_id_));
      }
    } else {
      state_ = UNIQUE_INDEX_CHECKING;
      is_dag_scheduled_ = false;
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "enter unique checking", K(ret), K(pkey_), K(index_id_));
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::unique_index_checking(const bool is_leader)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  UNUSED(is_leader);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else {
    if (is_dag_scheduled_) {
      ObIndexStatusTableOperator::ObBuildIndexStatus status;
      if (OB_FAIL(ObIndexStatusTableOperator::get_build_index_status(
              index_id_, pkey_.get_partition_id(), GCTX.self_addr_, *GCTX.sql_proxy_, status))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to get build index status", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (status.is_valid()) {
        state_ = WAIT_REPORT_STATUS;
        ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(INFO, "enter wait report status", K(pkey_), K(index_id_), K(status));
      }
    } else {
      if (!is_unique_index_) {
        const share::schema::ObIndexStatus index_status = INDEX_STATUS_UNAVAILABLE;
        if (OB_FAIL(report_index_status(index_id_, pkey_.get_partition_id(), index_status, ret, common::FOLLOWER))) {
          STORAGE_LOG(WARN, "fail to report index status", K(ret), K(pkey_), K(index_id_));
        } else {
          state_ = WAIT_REPORT_STATUS;
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(INFO, "enter wait report status", K(pkey_), K(index_id_));
        }
      } else {
        ObIPartitionGroupGuard guard;
        ObIPartitionGroup* partition = NULL;
        if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey_, guard))) {
          if (OB_PARTITION_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey_), K(index_id_));
          }
        } else if (OB_ISNULL(partition = guard.get_partition_group())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected error, partition should not be null", K(ret));
        } else if (partition->is_replica_using_remote_memstore()) {
          // skip unique index checking D replica
          STORAGE_LOG(INFO, "skip unique index checking with D replica", K(index_id_), K(pkey_));
          const share::schema::ObIndexStatus index_status = INDEX_STATUS_UNAVAILABLE;
          if (OB_FAIL(report_index_status(index_id_, pkey_.get_partition_id(), index_status, ret, common::FOLLOWER))) {
            STORAGE_LOG(WARN, "fail to report index status", K(ret), K(pkey_), K(index_id_));
          } else {
            state_ = WAIT_REPORT_STATUS;
            ObTaskController::get().allow_next_syslog();
            STORAGE_LOG(INFO, "enter wait report status", K(pkey_), K(index_id_));
          }
        } else {
          // schedule unique checking task
          ObLocalUniqueIndexCallback* callback = NULL;
          ObUniqueCheckingDag* dag = NULL;
          DEBUG_SYNC(BEFORE_LOCAL_INDEX_UNIQUE_INDEX_CHECKING_MID);
          if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
            STORAGE_LOG(WARN, "fail to alloc dag", K(ret));
          } else if (OB_FAIL(dag->init(pkey_,
                         &ObPartitionService::get_instance(),
                         &ObMultiVersionSchemaService::get_instance(),
                         index_id_,
                         schema_version_))) {
            STORAGE_LOG(WARN, "fail to init ObUniqueCheckingDag", K(ret), K(pkey_), K(index_id_));
          } else if (OB_FAIL(dag->alloc_local_index_task_callback(callback))) {
            STORAGE_LOG(WARN, "fail to alloc local index task callback", K(ret), K(pkey_), K(index_id_));
          } else if (OB_FAIL(dag->alloc_unique_checking_prepare_task(callback))) {
            STORAGE_LOG(WARN, "fail to alloc unique checking prepare task", K(ret), K(pkey_), K(index_id_));
          } else if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
            if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
              STORAGE_LOG(WARN, "fail to add dag to queue", K(ret), K(pkey_), K(index_id_));
            } else {
              ret = OB_EAGAIN;
            }
          } else {
            is_dag_scheduled_ = true;
            STORAGE_LOG(INFO, "add unique checking dag success", K(pkey_), K(index_id_));
          }
          if (OB_FAIL(ret) && NULL != dag) {
            ObDagScheduler::get_instance().free_dag(*dag);
            dag = NULL;
          }
        }
      }
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::send_copy_replica_rpc()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "innner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
  } else if (OB_FAIL(get_candidate_source_replica(retry_cnt_ % REFRESH_CANDIDATE_REPLICA_COUNT == 0))) {
    STORAGE_LOG(WARN, "fail to get candidate source replicas", K(ret), K(pkey_), K(index_id_));
  } else if (!candidate_replica_.is_valid()) {
    if (OB_FAIL(rollback_state(WAIT_TRANS_END))) {
      STORAGE_LOG(WARN, "fail to rollback state", K(ret), K(pkey_), K(index_id_));
    }
    ret = OB_EAGAIN;
  } else {
    obrpc::ObServerCopyLocalIndexSSTableArg arg;
    ObAddr rs_addr;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* index_schema = nullptr;
    const ObTableSchema* data_table_schema = nullptr;
    arg.data_src_ = candidate_replica_;
    arg.dst_ = GCTX.self_addr_;
    arg.pkey_ = pkey_;
    arg.index_table_id_ = index_id_;
    arg.cluster_id_ = GCONF.cluster_id;
    if (arg.data_src_ == arg.dst_) {
      // if the source and destination are the same, it means that this replica builds the index sstable itself,
      // just retry the scheduling process will get the right way to next state
      ret = OB_EAGAIN;
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
                   extract_tenant_id(index_id_), schema_guard))) {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(schema_version_));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id_, index_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(pkey_), K(index_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), data_table_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, data table schema is not NULL", K(ret));
    } else if (data_table_schema->is_binding_table()) {
      if (OB_FAIL(data_table_schema->get_pg_key(pkey_, arg.pkey_))) {
        STORAGE_LOG(WARN, "fail to get pg key", K(ret));
      }
    }

    if (OB_SUCC(ret) && nullptr != index_schema) {
      if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
        STORAGE_LOG(WARN, "fail to get rootservice address", K(ret));
      } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).observer_copy_local_index_sstable(arg))) {
        if (OB_ENTRY_EXIST != ret && OB_STATE_NOT_MATCH != ret && OB_NOT_MASTER != ret) {
          STORAGE_LOG(WARN, "fail to send copy index sstable rpc", K(ret), K(rs_addr), K(pkey_), K(index_id_));
        } else {
          ret = OB_EAGAIN;
        }
      }
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::get_candidate_source_replica(const bool need_refresh)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> tmp_replicas;
  ObAddr origin_replica;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (need_refresh) {
    candidate_replica_.reset();
    origin_replica = candidate_replica_;
  }
  if (OB_SUCC(ret)) {
    // first check self
    if (OB_SUCC(ret) && !candidate_replica_.is_valid()) {
      if (OB_FAIL(ObPartitionService::get_instance().check_single_replica_major_sstable_exist(pkey_, index_id_))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to check single replica major sstable exist", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        candidate_replica_ = GCTX.self_addr_;
      }
    }

    if (OB_SUCC(ret) && !candidate_replica_.is_valid()) {
      if (OB_FAIL(ObSSTableDataChecksumOperator::get_replicas(pkey_.get_table_id(),
              index_id_,
              pkey_.get_partition_id(),
              ObITable::MAJOR_SSTABLE,
              tmp_replicas,
              *GCTX.sql_proxy_))) {
        STORAGE_LOG(WARN, "fail to get replicas", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_replicas.count() && !candidate_replica_.is_valid(); ++i) {
        if (tmp_replicas.at(i) != origin_replica) {
          candidate_replica_ = tmp_replicas.at(i);
        }
      }
    }
    ObTaskController::get().allow_next_syslog();
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      STORAGE_LOG(INFO, "get candidate source replicas", K(candidate_replica_), K(pkey_), K(index_id_));
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::check_need_choose_replica(bool& need)
{
  int ret = OB_SUCCESS;
  need = false;
  ObAddr build_index_server;
  ObMemberList member_list;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(ObIndexTaskTableOperator::get_build_index_server(
                 index_id_, pkey_.get_partition_id(), *GCTX.sql_proxy_, build_index_server))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      need = true;
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to get current build index server", K(ret), K(pkey_), K(index_id_));
    }
  } else if (OB_FAIL(ObPartitionService::get_instance().get_leader_curr_member_list(pkey_, member_list))) {
    STORAGE_LOG(WARN, "fail to get leader current member list", K(ret), K(pkey_), K(index_id_));
  } else if (!member_list.contains(build_index_server)) {
    need = true;
  }
  return ret;
}

int ObBuildIndexScheduleTask::choose_build_index_replica(const bool is_leader)
{
  int ret = OB_SUCCESS;
  bool need_choose_replica = false;
  ObIndexTransStatus status;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (!is_leader) {
    state_ = WAIT_CHOOSE_OR_BUILD_INDEX_END;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "enter wait choose or build index end", K(pkey_), K(index_id_));
  } else if (OB_FAIL(get_candidate_source_replica())) {
    STORAGE_LOG(WARN, "fail to get candidate source replicas", K(ret));
  } else if (candidate_replica_.is_valid()) {
    state_ = COPY_BUILD_INDEX_DATA;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "enter copy build index data", K(pkey_), K(index_id_));
  } else if (OB_FAIL(check_need_choose_replica(need_choose_replica))) {
    STORAGE_LOG(WARN, "fail to check need choose replica", K(ret), K(pkey_), K(index_id_));
  } else if (need_choose_replica) {
    DEBUG_SYNC(BEFORE_LOCAL_INDEX_CHOOSE_BUILD_INDEX_REPLICA_MID);
    const int64_t index_task_frozen_version = -1;
    if (OB_FAIL(ObIndexTaskTableOperator::generate_new_build_index_record(index_id_,
            pkey_.get_partition_id(),
            GCTX.self_addr_,
            build_snapshot_version_,
            index_task_frozen_version,
            *GCTX.sql_proxy_))) {
      STORAGE_LOG(WARN, "fail to generate build index server", K(ret), K(pkey_), K(index_id_));
    } else {
      state_ = WAIT_CHOOSE_OR_BUILD_INDEX_END;
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "enter wait choose or build index end", K(pkey_), K(index_id_));
    }
  } else {
    DEBUG_SYNC(BEFORE_LOCAL_INDEX_CHOOSE_BUILD_INDEX_REPLICA_MID);
    state_ = WAIT_CHOOSE_OR_BUILD_INDEX_END;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "enter wait choose or build index end", K(pkey_), K(index_id_));
  }
  return ret;
}

int ObBuildIndexScheduleTask::check_need_schedule_dag(bool& need_schedule_dag)
{
  int ret = OB_SUCCESS;
  ObAddr build_index_server;
  need_schedule_dag = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(ObIndexTaskTableOperator::get_build_index_server(
                 index_id_, pkey_.get_partition_id(), *GCTX.sql_proxy_, build_index_server))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      need_schedule_dag = false;
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to get build index server", K(ret), K(pkey_), K(index_id_));
    }
  } else {
    need_schedule_dag = build_index_server == GCTX.self_addr_;
  }
  return ret;
}

int ObBuildIndexScheduleTask::check_all_replica_report_build_index_end(const bool is_leader, bool& is_end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (!is_leader) {
    is_end = false;
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* index_schema = nullptr;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
            extract_tenant_id(index_id_), schema_guard))) {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(pkey_), K(index_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id_, index_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(pkey_), K(index_id_));
    } else {
      is_end = nullptr == index_schema ||
               is_final_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema());
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::wait_report_status(const bool is_leader)
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(check_all_replica_report_build_index_end(is_leader, is_end))) {
    STORAGE_LOG(WARN, "fail to check all replica report build index end", K(ret), K(pkey_), K(index_id_));
  } else if (is_end) {
    state_ = END;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "enter build index end", K(pkey_), K(index_id_));
  }
  return ret;
}

int ObBuildIndexScheduleTask::check_build_index_end(bool& build_index_end, bool& need_copy)
{
  int ret = OB_SUCCESS;
  ObAddr build_index_server;
  ObIndexStatusTableOperator::ObBuildIndexStatus status;
  ObMemberList member_list;
  build_index_end = false;
  need_copy = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else {
    if (OB_FAIL(get_candidate_source_replica())) {
      STORAGE_LOG(WARN, "fail to get candidate source replicas", K(ret));
    } else {
      build_index_end = candidate_replica_.is_valid();
      if (build_index_end) {
        need_copy = true;
        if (need_copy) {
          if (OB_FAIL(ObPartitionService::get_instance().check_single_replica_major_sstable_exist(pkey_, index_id_))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              STORAGE_LOG(WARN, "fail to check replica has major sstable", K(ret), K(pkey_), K(index_id_));
            }
          } else {
            need_copy = false;
          }
        }
      } else {
        if (OB_FAIL(ObIndexTaskTableOperator::get_build_index_server(
                index_id_, pkey_.get_partition_id(), *GCTX.sql_proxy_, build_index_server))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(WARN, "fail to get build index server", K(ret), K(pkey_), K(index_id_));
          }
        } else if (OB_FAIL(ObIndexStatusTableOperator::get_build_index_status(
                       index_id_, pkey_.get_partition_id(), build_index_server, *GCTX.sql_proxy_, status))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(WARN, "fail to get build index status", K(ret), K(pkey_), K(index_id_));
          }
        }
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          if (OB_FAIL(ObPartitionService::get_instance().get_curr_member_list(pkey_, member_list))) {
            STORAGE_LOG(WARN, "fail to get current member list", K(ret), K(pkey_), K(index_id_));
          } else if (!member_list.contains(build_index_server)) {
            if (OB_FAIL(rollback_state(WAIT_TRANS_END))) {
              STORAGE_LOG(WARN, "fail to rollback state", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_SUCCESS != status.ret_code_) {
            const ObIndexStatus index_status = INDEX_STATUS_UNAVAILABLE;
            if (OB_FAIL(report_index_status(
                    index_id_, pkey_.get_partition_id(), index_status, status.ret_code_, common::LEADER))) {
              STORAGE_LOG(WARN, "fail to report index status", K(ret), K(status), K(pkey_), K(index_id_));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::wait_choose_or_build_index_end(const bool is_leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(get_candidate_source_replica())) {
    STORAGE_LOG(WARN, "fail to get candidate source replicas", K(ret), K(pkey_), K(index_id_));
  } else if (candidate_replica_.is_valid()) {
    state_ = COPY_BUILD_INDEX_DATA;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "enter copy build index data state", K(pkey_), K(index_id_));
  } else {
    bool is_end = false;
    bool is_first_time = false;
    if (is_leader && !is_end && REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      // check build index server permenant offline
      bool need_choose_replica = false;
      if (OB_FAIL(check_need_choose_replica(need_choose_replica))) {
        STORAGE_LOG(WARN, "fail to check need choose replica", K(ret), K(pkey_), K(index_id_));
      } else if (need_choose_replica) {
        state_ = CHOOSE_BUILD_INDEX_REPLICA;
        ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(INFO, "enter choose build index replica", K(pkey_), K(index_id_));
        is_end = true;
      }
    }
    if (OB_SUCC(ret) && !is_dag_scheduled_ && !is_end) {
      bool need_schedule_dag = false;
      if (OB_FAIL(check_need_schedule_dag(need_schedule_dag))) {
        STORAGE_LOG(WARN, "fail to check need schedule dag", K(ret), K(is_leader), K(pkey_), K(index_id_));
      } else if (need_schedule_dag) {
        if (OB_FAIL(schedule_dag())) {
          STORAGE_LOG(WARN, "fail to schedule dag", K(ret), K(index_id_), K(pkey_));
        } else {
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(INFO, "schedule build index dag", K(pkey_), K(index_id_));
          is_dag_scheduled_ = true;
          is_first_time = true;
        }
      }
    }
    DEBUG_SYNC(BEFORE_LOCAL_INDEX_WAIT_CHOOSE_OR_BUILD_INDEX_END_MID);
  }
  return ret;
}

int ObBuildIndexScheduleTask::alloc_index_prepare_task(ObBuildIndexDag* dag)
{
  int ret = OB_SUCCESS;
  ObIndexPrepareTask* prepare_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag));
  } else if (OB_FAIL(dag->alloc_task(prepare_task))) {
    STORAGE_LOG(WARN, "fail to alloc index prepare task", K(ret), K(pkey_), K(index_id_));
  } else if (OB_FAIL(prepare_task->init(dag->get_param(), &dag->get_context()))) {
    STORAGE_LOG(WARN, "fail to init index prepare task", K(ret), K(pkey_), K(index_id_));
  } else if (OB_FAIL(dag->add_task(*prepare_task))) {
    STORAGE_LOG(WARN, "fail to add index prepare task to dag", K(ret), K(pkey_), K(index_id_));
  }

  return ret;
}

int ObBuildIndexScheduleTask::schedule_dag()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard part_guard;
  ObPGPartitionGuard pg_partition_guard;
  ObIPartitionStorage* storage = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey_, part_guard)) ||
             OB_ISNULL(part_guard.get_partition_group())) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K_(pkey));
  } else if (OB_FAIL(part_guard.get_partition_group()->get_pg_partition(pkey_, pg_partition_guard)) ||
             OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    STORAGE_LOG(WARN, "fail to get pg partition", K(ret), K_(pkey));
  } else if (OB_ISNULL(storage = pg_partition_guard.get_pg_partition()->get_storage())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition storage must not be NULL", K(ret));
  } else {
    ObBuildIndexDag* dag = NULL;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
      STORAGE_LOG(WARN, "fail to alloc dag", K(ret));
    } else if (OB_FAIL(
                   storage->get_build_index_param(index_id_, schema_version_, GCTX.ob_service_, dag->get_param()))) {
      STORAGE_LOG(WARN, "fail to get build index param", K(ret), K(pkey_), K(index_id_));
    } else if (OB_FAIL(storage->get_build_index_context(dag->get_param(), dag->get_context()))) {
      STORAGE_LOG(WARN, "fail to get build index context", K(ret), K(pkey_), K(index_id_));
    } else if (OB_FAIL(dag->init(pkey_, &ObPartitionService::get_instance()))) {
      STORAGE_LOG(WARN, "fail to init build index dag", K(ret), K(pkey_), K(index_id_));
    } else if (OB_FAIL(alloc_index_prepare_task(dag))) {
      STORAGE_LOG(WARN, "fail to alloc index prepare task", K(ret), K(pkey_), K(index_id_));
    } else if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        STORAGE_LOG(WARN, "fail to add dag to queue", K(ret), K(pkey_), K(index_id_));
      } else {
        ret = OB_EAGAIN;
      }
    }
    if (OB_FAIL(ret) && NULL != dag) {
      ObDagScheduler::get_instance().free_dag(*dag);
      dag = NULL;
    }
  }
  return ret;
}

int ObBuildIndexScheduleTask::process()
{
  int ret = OB_SUCCESS;
  bool need_build = false;
  bool is_end = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(
        WARN, "create index online is not supported in old verion", K(ret), "version", GET_MIN_CLUSTER_VERSION());
  } else {
    ObIPartitionGroupGuard part_guard;
    ObCurTraceId::set(task_id_);
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* index_schema = nullptr;
    const ObTableSchema* table_schema = nullptr;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
            extract_tenant_id(index_id_), schema_guard))) {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(pkey_), K(index_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id_, index_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(pkey_), K(index_id_));
    } else if (OB_ISNULL(index_schema)) {
      if (UNIQUE_INDEX_CHECKING != state_) {
        STORAGE_LOG(INFO, "index schema has been deleted, skip build it", K(pkey_), K(index_id_));
        is_end = true;
      }
    } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), table_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret));
    } else if (OB_FAIL(check_partition_need_build_index(pkey_, *index_schema, *table_schema, part_guard, need_build))) {
      STORAGE_LOG(WARN, "fail to check index need build", K(ret), K(pkey_), K(index_id_));
    } else if (need_build) {
      ObRole role;
      if (OB_FAIL(get_role(role))) {
        STORAGE_LOG(WARN, "fail to get role", K(ret), K(pkey_), K(index_id_));
      } else {
        const bool is_leader = is_strong_leader(role);
        int last_state = END;
        if (role != last_role_) {
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(INFO, "partition role changes", K(role), K(last_role_));
          if (OB_FAIL(rollback_state(WAIT_TRANS_END))) {
            STORAGE_LOG(WARN, "fail to rollback state", K(ret), K(pkey_), K(index_id_));
          } else {
            last_role_ = role;
          }
        }
        while (OB_SUCC(ret) && state_ != END && last_state != state_) {
          last_state = state_;
          switch (state_) {
            case WAIT_TRANS_END:
              DEBUG_SYNC(BEFORE_LOCAL_INDEX_WAIT_TRANS_END);
              if (OB_FAIL(wait_trans_end(is_leader))) {
                STORAGE_LOG(WARN, "fail to wait trans end", K(ret), K(pkey_), K(index_id_));
              }
              break;
            case WAIT_SNAPSHOT_READY:
              DEBUG_SYNC(BEFORE_LOCAL_INDEX_WAIT_SNAPSHOT_READY);
              if (OB_FAIL(wait_snapshot_ready(is_leader))) {
                STORAGE_LOG(WARN, "fail to wait snapshot ready", K(ret), K(pkey_), K(index_id_));
              }
              break;
            case CHOOSE_BUILD_INDEX_REPLICA:
              DEBUG_SYNC(BEFORE_LOCAL_INDEX_CHOOSE_BUILD_INDEX_REPLICA);
              if (OB_FAIL(choose_build_index_replica(is_leader))) {
                STORAGE_LOG(WARN, "fail to choose build index replica", K(ret), K(pkey_), K(index_id_));
              }
              break;
            case WAIT_CHOOSE_OR_BUILD_INDEX_END:
              DEBUG_SYNC(BEFORE_LOCAL_INDEX_WAIT_CHOOSE_OR_BUILD_INDEX_END);
              if (OB_FAIL(wait_choose_or_build_index_end(is_leader))) {
                STORAGE_LOG(WARN, "fail to wait or choose build index end", K(ret), K(pkey_), K(index_id_));
              }
              break;
            case COPY_BUILD_INDEX_DATA:
              DEBUG_SYNC(BEFORE_LOCAL_INDEX_COPY_BUILD_INDEX_DATA);
              if (OB_FAIL(copy_build_index_data(is_leader))) {
                STORAGE_LOG(WARN, "fail to copy build index data", K(ret), K(pkey_), K(index_id_));
              }
              break;
            case UNIQUE_INDEX_CHECKING:
              DEBUG_SYNC(BEFORE_LOCAL_INDEX_UNIQUE_INDEX_CHECKING);
              if (OB_FAIL(unique_index_checking(is_leader))) {
                STORAGE_LOG(WARN, "fail to do unique index checking", K(ret), K(pkey_), K(index_id_));
              }
              break;
            case WAIT_REPORT_STATUS:
              DEBUG_SYNC(BEFORE_LOCAL_INDEX_WAIT_REPORT_STATUS);
              if (OB_FAIL(wait_report_status(is_leader))) {
                if (OB_EAGAIN == ret) {
                  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
                    ObTaskController::get().allow_next_syslog();
                    STORAGE_LOG(WARN, "fail to wait report status", K(ret), K(pkey_), K(index_id_));
                  }
                } else {
                  STORAGE_LOG(WARN, "fail to wait report status", K(ret), K(pkey_), K(index_id_));
                }
              }
              DEBUG_SYNC(BEFORE_LOCAL_INDEX_END);
              break;
          }
        }
        if (END == state_) {
          is_end = true;
        }
      }
    } else {
      is_end = UNIQUE_INDEX_CHECKING != state_;
    }
  }
  if (OB_SUCC(ret)) {
    need_retry_ = !is_end;
  } else {
    need_retry_ = error_need_retry(ret);
    if (need_retry_) {
      ret = OB_EAGAIN;
    }
    if (!need_retry_) {
      const ObIndexStatus index_status = INDEX_STATUS_UNAVAILABLE;
      if (OB_FAIL(report_index_status(index_id_, pkey_.get_partition_id(), index_status, ret, common::LEADER))) {
        STORAGE_LOG(WARN, "fail to report index status", K(ret), K(pkey_), K(index_id_));
      }
    }
  }
  return ret;
}

ObIDDLTask* ObBuildIndexScheduleTask::deep_copy(char* buf, const int64_t size) const
{
  int ret = OB_SUCCESS;
  ObBuildIndexScheduleTask* task = NULL;
  if (OB_ISNULL(buf) || size < sizeof(*this)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf));
  } else {
    task = new (buf) ObBuildIndexScheduleTask();
    *task = *this;
  }
  return task;
}

int ObBuildIndexScheduleTask::get_role(common::ObRole& role)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_role(pkey_, role))) {
    STORAGE_LOG(WARN, "fail to get role", K(ret), K(pkey_), K(index_id_));
  }
  return ret;
}

int ObBuildIndexScheduleTask::rollback_state(const int state)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (state < WAIT_TRANS_END || state > END) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(state));
  } else {
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "rollback to state", K(pkey_), K(index_id_), "original_state", state_, "dest_state", state);
    state_ = state;
    is_copy_request_sent_ = false;
  }
  return ret;
}

ObCheckTenantSchemaTask::ObCheckTenantSchemaTask() : is_inited_(false), lock_(), tenant_refresh_map_()
{}

ObCheckTenantSchemaTask::~ObCheckTenantSchemaTask()
{
  destroy();
}

void ObCheckTenantSchemaTask::destroy()
{
  tenant_refresh_map_.destroy();
  is_inited_ = false;
}

int ObCheckTenantSchemaTask::init()
{
  int ret = OB_SUCCESS;
  lib::ObLockGuard<lib::ObMutex> guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObCheckTenantSchemaTask has been inited twice", K(ret));
  } else if (OB_FAIL(tenant_refresh_map_.create(
                 DEFAULT_TENANT_BUCKET_NUM, ObModIds::OB_BUILD_INDEX_SCHEDULER, ObModIds::OB_BUILD_INDEX_SCHEDULER))) {
    STORAGE_LOG(WARN, "fail to create tenant refresh map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObCheckTenantSchemaTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_schema_guard(schema_guard))) {
    STORAGE_LOG(WARN, "fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    STORAGE_LOG(WARN, "fail to get tenant ids", K(ret));
  } else {
    lib::ObLockGuard<lib::ObMutex> guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t schema_version = 0;
      int64_t orig_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
        LOG_WARN("fail to get tenant schema version", K(ret));
      } else if (OB_CORE_SCHEMA_VERSION == schema_version ||
                 !share::schema::ObSchemaService::is_formal_version(schema_version)) {
        LOG_INFO("invalid or unformal new tenant schema version", K(tenant_id), K(schema_version));
        continue;
      } else if (OB_FAIL(tenant_refresh_map_.get_refactored(tenant_id, orig_schema_version))) {
        if (common::OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret)) {
        if (orig_schema_version < schema_version) {
          ObTenantDDLCheckSchemaTask task;
          if (OB_FAIL(task.init(tenant_id, orig_schema_version, schema_version))) {
            STORAGE_LOG(WARN, "fail to init ObTenantDDLCheckSchemaTask", K(ret));
          } else if (OB_FAIL(ObBuildIndexScheduler::get_instance().add_tenant_ddl_task(task))) {
            if (OB_ENTRY_EXIST != ret) {
              STORAGE_LOG(WARN, "fail to add tenant ddl task", K(ret), K(task));
            } else {
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(tenant_refresh_map_.set_refactored(tenant_id, schema_version, true /*overwrite*/))) {
            STORAGE_LOG(WARN, "fail to set tenant schema version into map", K(ret));
          } else {
            STORAGE_LOG(INFO, "add tenant ddl task", K(task));
          }
        }
      }
    }
  }
}

ObBuildIndexScheduler::ObBuildIndexScheduler()
    : is_inited_(false), task_executor_(), lock_(), check_tenant_schema_task_(), is_stop_(false)
{}

ObBuildIndexScheduler::~ObBuildIndexScheduler()
{
  TG_DESTROY(lib::TGDefIDs::IndexSche);
}

int ObBuildIndexScheduler::init()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBuildIndexScheduler has been inited twice", K(ret));
  } else if (OB_FAIL(task_executor_.init(DEFAULT_DDL_BUCKET_NUM, lib::TGDefIDs::DDLTaskExecutor2))) {
    STORAGE_LOG(WARN, "fail to init schedule index executor", K(ret));
  } else if (OB_FAIL(ObTenantMetaMemoryMgr::get_instance().init())) {
    STORAGE_LOG(WARN, "fail to init tenant meta memory", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::IndexSche))) {
    STORAGE_LOG(WARN, "fail to init timer", K(ret));
  } else if (OB_FAIL(check_tenant_schema_task_.init())) {
    STORAGE_LOG(WARN, "fail to check tenant schema task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::IndexSche,
                 check_tenant_schema_task_,
                 CHECK_TENANT_SCHEMA_INTERVAL_US,
                 true /*repeat*/))) {
    STORAGE_LOG(WARN, "fail to scheduler timer", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBuildIndexScheduler::add_tenant_ddl_task(const ObTenantDDLCheckSchemaTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduler has not been inited", K(ret));
  } else if (OB_FAIL(task_executor_.push_task(task))) {
    STORAGE_LOG(WARN, "fail to push back task", K(ret));
  }
  return ret;
}

int ObBuildIndexScheduler::push_task(ObBuildIndexScheduleTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexScheduleTask has not been inited", K(ret));
  } else if (is_stop_) {
    // do nothing
  } else if (OB_FAIL(task_executor_.push_task(task))) {
    if (OB_LIKELY(OB_ENTRY_EXIST == ret)) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to push back ObBuildIndexScheduleTask", K(ret));
    }
  }
  return ret;
}

ObBuildIndexScheduler& ObBuildIndexScheduler::get_instance()
{
  static ObBuildIndexScheduler instance;
  return instance;
}

void ObBuildIndexScheduler::stop()
{
  is_stop_ = true;
  task_executor_.stop();
}

void ObBuildIndexScheduler::wait()
{
  task_executor_.wait();
}

void ObBuildIndexScheduler::destroy()
{
  task_executor_.destroy();
  is_inited_ = false;
}
