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

#define USING_LOG_PREFIX SQL

#include "sql/ob_sql_partition_location_cache.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "common/ob_partition_key.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase {
namespace sql {

ObSqlPartitionLocationCache::ObSqlPartitionLocationCache()
    : loc_cache_(NULL), self_addr_(), task_exec_ctx_(NULL), schema_guard_(nullptr)
{}

// get partition location of a partition
// return OB_LOCATION_NOT_EXIST if not record in partition table
int ObSqlPartitionLocationCache::get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
    const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update /*=true*/)
{
  int ret = OB_SUCCESS;
  is_cache_hit = false;
  bool is_mapping_real_vt = is_oracle_mapping_real_virtual_table(table_id);
  if (OB_UNLIKELY(NULL == loc_cache_ || !self_addr_.is_valid() || nullptr == schema_guard_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL or self addr is invalid", K(ret), K(loc_cache_), K(self_addr_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret), K(table_id));
  } else if (is_virtual_table(table_id) && !is_mapping_real_vt) {
    if (OB_FAIL(virtual_get(table_id, partition_id, location, expire_renew_time, is_cache_hit))) {
      LOG_WARN(
          "fail to virtual get partition location cache", K(ret), K(table_id), K(partition_id), K(expire_renew_time));
    }
  } else {
    uint64_t phy_tid = OB_INVALID_ID;
    int64_t phy_pid = -1;
    common::ObPGKey pg_key;
    uint64_t real_table_id = table_id;
    if (is_mapping_real_vt) {
      real_table_id = share::schema::ObSchemaUtils::get_real_table_mappings_tid(table_id);
      if (OB_INVALID_ID == real_table_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get real table id", K(ret), K(table_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_phy_key(real_table_id, partition_id, phy_tid, phy_pid, pg_key))) {
      LOG_WARN("fail to get phy key", K(ret), K(table_id), K(partition_id), K(pg_key));
    } else if (OB_FAIL(loc_cache_->get(phy_tid, phy_pid, location, expire_renew_time, is_cache_hit, auto_update))) {
      LOG_WARN("fail to get partition location cache",
          K(phy_tid),
          K(phy_pid),
          K(ret),
          K(table_id),
          K(partition_id),
          K(expire_renew_time));
    } else {
      location.set_table_id(real_table_id);
      location.set_partition_id(partition_id);
      location.set_pg_key(pg_key);
    }
  }
  return ret;
}

// get partition location of a partition
// return OB_LOCATION_NOT_EXIST if not record in partition table
int ObSqlPartitionLocationCache::get(
    const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t expire_renew_time, bool& is_cache_hit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == loc_cache_ || !self_addr_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL or self addr is invalid", K(ret), K(loc_cache_), K(self_addr_));
  } else if (OB_FAIL(
                 get(partition.table_id_, partition.get_partition_id(), location, expire_renew_time, is_cache_hit))) {
    LOG_WARN("fail to get", K(ret), K(partition), K(expire_renew_time));
  }
  return ret;
}

// get all partition locations of a table, virtabe table should set partition_num to 1
// return OB_LOCATION_NOT_EXIST if some partition doesn't have record in partition table
int ObSqlPartitionLocationCache::get(const uint64_t table_id, ObIArray<ObPartitionLocation>& locations,
    const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update /*=true*/)

{
  int ret = OB_SUCCESS;
  const share::schema::ObSimpleTableSchemaV2* table_schema = nullptr;
  if (OB_UNLIKELY(nullptr == loc_cache_ || nullptr == schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null member value", K(ret), KP(loc_cache_), KP(schema_guard_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema ptr is null", K(ret), K(table_id));
  } else if (OB_INVALID_ID == table_schema->get_tablegroup_id() || !table_schema->is_binding_table()) {
    if (OB_FAIL(loc_cache_->get(table_id, locations, expire_renew_time, is_cache_hit, auto_update))) {
      LOG_WARN("failed to get location cache", K(table_id), K(ret));
    }
  } else {
    bool check_dropped_schema = false;
    share::schema::ObTablePgKeyIter iter(*table_schema, table_schema->get_tablegroup_id(), check_dropped_schema);
    if (OB_FAIL(iter.init())) {
      LOG_WARN("fail to init table pg key iter", K(ret));
    } else {
      common::ObPartitionKey pkey;
      common::ObPGKey pg_key;
      share::ObPartitionLocation location;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(pkey, pg_key))) {
        if (loc_cache_->get(pg_key, location, expire_renew_time, is_cache_hit)) {
          LOG_WARN("fail to get", K(ret));
        } else if (FALSE_IT(location.set_table_id(pkey.get_table_id()))) {
          // never be here
        } else if (FALSE_IT(location.set_partition_id(pkey.get_partition_id()))) {
          // never be here
        } else if (FALSE_IT(location.set_pg_key(pg_key))) {
          // never be here
        } else if (OB_FAIL(locations.push_back(location))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          pkey.reset();
          pg_key.reset();
          location.reset();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

// get leader addr of a partition, return OB_LOCATION_NOT_EXIST if not replica exist in partition
// table, return OB_LOCATION_LEADER_NOT_EXIST if leader not exist
int ObSqlPartitionLocationCache::get_strong_leader(
    const ObPartitionKey& partition, ObAddr& leader, const bool force_renew)
{
  int ret = OB_SUCCESS;
  common::ObPGKey pg_key;
  if (OB_UNLIKELY(NULL == loc_cache_ || !self_addr_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL or self addr is invalid", K(ret), K(loc_cache_), K(self_addr_));
  } else if (OB_FAIL(get_phy_key(partition, pg_key))) {
    LOG_WARN("fail to get phy key", K(ret), K(partition));
  } else if (OB_FAIL(loc_cache_->get_strong_leader(pg_key, leader, force_renew))) {
    LOG_WARN("fail to get leader", K(ret), K(partition), K(pg_key), K(force_renew));
  }
  return ret;
}

// return OB_LOCATION_NOT_EXIST if partition's location not in cache
int ObSqlPartitionLocationCache::nonblock_get(
    const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  const int64_t expire_renew_time = 0;
  bool is_cache_hit = false;
  bool is_mapping_real_vt = is_oracle_mapping_real_virtual_table(table_id);
  UNUSED(cluster_id);

  if (OB_UNLIKELY(NULL == loc_cache_ || !self_addr_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL or self addr is invalid", K(ret), K(loc_cache_), K(self_addr_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret), K(table_id));
  } else if (is_virtual_table(table_id) && !is_mapping_real_vt) {
    if (OB_FAIL(virtual_get(table_id, partition_id, location, expire_renew_time, is_cache_hit))) {
      LOG_WARN("fail to virtual get partition location cache", K(ret), K(table_id), K(partition_id));
    }
  } else {
    uint64_t phy_tid = OB_INVALID_ID;
    int64_t phy_pid = -1;
    common::ObPGKey pg_key;
    uint64_t real_table_id = table_id;
    if (is_mapping_real_vt) {
      real_table_id = share::schema::ObSchemaUtils::get_real_table_mappings_tid(table_id);
      if (OB_INVALID_ID == real_table_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get real table id", K(ret), K(table_id));
      }
    }
    if (OB_FAIL(get_phy_key(real_table_id, partition_id, phy_tid, phy_pid, pg_key))) {
      LOG_WARN("fail to get phy key", K(ret), K(table_id), K(partition_id));
    } else if (OB_FAIL(loc_cache_->nonblock_get(phy_tid, phy_pid, location))) {
      LOG_WARN("fail to nonblock get", K(ret), K(table_id), K(partition_id), K(phy_tid), K(phy_pid));
    } else {
      location.set_table_id(real_table_id);
      location.set_partition_id(partition_id);
      location.set_pg_key(pg_key);
    }
  }
  return ret;
}

// return OB_LOCATION_NOT_EXIST if partition's location not in cache
int ObSqlPartitionLocationCache::nonblock_get(
    const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  UNUSED(cluster_id);
  if (OB_UNLIKELY(NULL == loc_cache_ || !self_addr_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL or self addr is invalid", K(ret), K(loc_cache_), K(self_addr_));
  } else if (OB_FAIL(nonblock_get(partition.table_id_, partition.get_partition_id(), location))) {
    LOG_WARN("fail to nonblock get", K(ret), K(partition));
  }
  return ret;
}

// return OB_LOCATION_NOT_EXIST if partition's location not in cache,
// return OB_LOCATION_LEADER_NOT_EXIST if leader not exist
int ObSqlPartitionLocationCache::nonblock_get_strong_leader(const ObPartitionKey& partition, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  common::ObPGKey pg_key;
  if (OB_UNLIKELY(NULL == loc_cache_ || !self_addr_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL or self addr is invalid", K(ret), K(loc_cache_), K(self_addr_));
  } else if (OB_FAIL(get_phy_key(partition, pg_key))) {
    LOG_WARN("fail to get phy key", K(ret), K(partition));
  } else if (OB_FAIL(loc_cache_->nonblock_get_strong_leader(pg_key, leader))) {
    LOG_WARN("fail to get leader", K(ret), K(partition), K(pg_key));
  }
  return ret;
}

int ObSqlPartitionLocationCache::nonblock_renew(
    const common::ObPartitionKey& partition, const int64_t expire_renew_time, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  common::ObPGKey pg_key;
  UNUSED(cluster_id);
  if (NULL == loc_cache_) {
    ret = OB_NOT_INIT;
    LOG_WARN("loc_cache is NULL", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition", K(partition), K(ret));
  } else if (OB_FAIL(get_phy_key(partition, pg_key))) {
    LOG_WARN("fail to get phy key", K(ret), K(partition));
  } else if (OB_FAIL(loc_cache_->nonblock_renew(pg_key, expire_renew_time))) {
    LOG_WARN("nonblock_renew failed", K(partition), K(pg_key), K(ret));
  }
  return ret;
}

int ObSqlPartitionLocationCache::nonblock_renew_with_limiter(
    const ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited)
{
  int ret = OB_SUCCESS;
  common::ObPGKey pg_key;
  if (NULL == loc_cache_) {
    ret = OB_NOT_INIT;
    LOG_WARN("loc_cache is NULL", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition", K(partition), K(ret));
  } else if (OB_FAIL(get_phy_key(partition, pg_key))) {
    LOG_WARN("fail to get phy key", K(ret), K(partition));
  } else if (OB_FAIL(loc_cache_->nonblock_renew_with_limiter(pg_key, expire_renew_time, is_limited))) {
    LOG_WARN("nonblock_renew_with_limiter failed", K(partition), K(pg_key), K(ret));
  }
  return ret;
}

ObSqlPartitionLocationCache::LocationDistributedMode ObSqlPartitionLocationCache::get_location_distributed_mode(
    const uint64_t table_id) const
{
  LocationDistributedMode loc_dist_mode = LOC_DIST_MODE_INVALID;
  const uint64_t org_pure_tid = extract_pure_id(table_id);
  const uint64_t pure_tid =
      is_oracle_mapping_virtual_table(org_pure_tid) ? get_origin_tid_by_oracle_mapping_tid(org_pure_tid) : org_pure_tid;
  switch (pure_tid) {
    case OB_TENANT_VIRTUAL_ALL_TABLE_TID:
    case OB_TENANT_VIRTUAL_SHOW_TABLES_TID:
    case OB_TENANT_VIRTUAL_TABLE_COLUMN_TID:
    case OB_TENANT_VIRTUAL_TABLE_INDEX_TID:
    case OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID:
    case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID:
    case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID:
    case OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID:
    case OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID:
    case OB_VIRTUAL_SHOW_RESTORE_PREVIEW_TID:
    case OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID:
    case OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID:
    case OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID:
    case OB_TENANT_VIRTUAL_WARNING_TID:
    case OB_TENANT_VIRTUAL_CURRENT_TENANT_TID:
    case OB_TENANT_VIRTUAL_PARTITION_STAT_TID:
    case OB_TENANT_VIRTUAL_STATNAME_TID:
    case OB_TENANT_VIRTUAL_EVENT_NAME_TID:
    case OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TID:
    case OB_ALL_VIRTUAL_DATA_TYPE_TID:
    case OB_ALL_VIRTUAL_PROXY_SCHEMA_TID:
    case OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID:
    case OB_ALL_VIRTUAL_PROXY_PARTITION_TID:
    case OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID:
    case OB_ALL_VIRTUAL_PROXY_ROUTE_TID:
    case OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TID:
    case OB_SESSION_VARIABLES_TID:
    case OB_TABLE_PRIVILEGES_TID:
    case OB_USER_PRIVILEGES_TID:
    case OB_SCHEMA_PRIVILEGES_TID:
    case OB_TABLE_CONSTRAINTS_TID:
    case OB_REFERENTIAL_CONSTRAINTS_TID:
    case OB_GLOBAL_STATUS_TID:
    case OB_PARTITIONS_TID:
    case OB_SESSION_STATUS_TID:
    case OB_ALL_VIRTUAL_TRACE_LOG_TID:
    case OB_ALL_VIRTUAL_ENGINE_TID:
    case OB_ALL_VIRTUAL_FILES_TID:
    case OB_USER_TID:
    case OB_DB_TID:
    case OB_PROC_TID:
    case OB_PARAMETERS_TID:
    case OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TID:
    case OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TID:
    case OB_TENANT_VIRTUAL_OUTLINE_TID:
    case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID:
    case OB_ALL_VIRTUAL_PARTITION_LOCATION_TID:
    case OB_ALL_VIRTUAL_PARTITION_ITEM_TID:
    case OB_TENANT_VIRTUAL_CHARSET_TID:
    case OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_TID:
    case OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TID:
#define AGENT_VIRTUAL_TABLE_LOCATION_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef AGENT_VIRTUAL_TABLE_LOCATION_SWITCH

#define ITERATE_VIRTUAL_TABLE_LOCATION_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef ITERATE_VIRTUAL_TABLE_LOCATION_SWITCH
    case OB_TENANT_VIRTUAL_COLLATION_TID: {
      loc_dist_mode = ObSqlPartitionLocationCache::LOC_DIST_MODE_ONLY_LOCAL;
      break;
    }
    case OB_ALL_VIRTUAL_PROCESSLIST_TID:
    case OB_TENANT_VIRTUAL_DATABASE_STATUS_TID:
    case OB_TENANT_VIRTUAL_TENANT_STATUS_TID:
    case OB_TENANT_VIRTUAL_INTERM_RESULT_TID:
    case OB_ALL_VIRTUAL_KVCACHE_INFO_TID:
    case OB_ALL_VIRTUAL_SESSION_EVENT_TID:
    case OB_ALL_VIRTUAL_SESSION_WAIT_TID:
    case OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID:
    case OB_ALL_VIRTUAL_SYSTEM_EVENT_TID:
    case OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TID:
    case OB_ALL_VIRTUAL_LATCH_TID:
    case OB_ALL_VIRTUAL_SESSTAT_TID:
    case OB_ALL_VIRTUAL_SYSSTAT_TID:
    case OB_ALL_VIRTUAL_STORAGE_STAT_TID:
    case OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_TID:
    case OB_ALL_VIRTUAL_DISK_STAT_TID:
    case OB_ALL_VIRTUAL_MEMSTORE_INFO_TID:
    case OB_ALL_VIRTUAL_PARTITION_INFO_TID:
    case OB_ALL_VIRTUAL_PARTITION_REPLAY_STATUS_TID:
    case OB_ALL_VIRTUAL_CLOG_STAT_TID:
    case OB_ALL_VIRTUAL_SERVER_CLOG_STAT_TID:
    case OB_ALL_VIRTUAL_SERVER_BLACKLIST_TID:
    case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TID:
    case OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID:
    case OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_TID:
    case OB_ALL_VIRTUAL_TRANS_STAT_TID:
    case OB_ALL_VIRTUAL_PG_PARTITION_INFO_TID:
    case OB_ALL_VIRTUAL_TRANS_MGR_STAT_TID:
    case OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID:
    case OB_ALL_VIRTUAL_TRANS_RESULT_INFO_STAT_TID:
    case OB_ALL_VIRTUAL_DUPLICATE_PARTITION_MGR_STAT_TID:
    case OB_ALL_VIRTUAL_TRANS_AUDIT_TID:
    case OB_ALL_VIRTUAL_TRANS_SQL_AUDIT_TID:
    case OB_ALL_VIRTUAL_WEAK_READ_STAT_TID:
    case OB_ALL_VIRTUAL_ELECTION_INFO_TID:
    case OB_ALL_VIRTUAL_ELECTION_GROUP_INFO_TID:
    case OB_ALL_VIRTUAL_ELECTION_MEM_STAT_TID:
    case OB_ALL_VIRTUAL_ELECTION_PRIORITY_TID:
    case OB_ALL_VIRTUAL_ELECTION_EVENT_HISTORY_TID:
    case OB_ALL_VIRTUAL_SQL_AUDIT_TID:
    case OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TID:
    case OB_ALL_VIRTUAL_PLAN_STAT_TID:
    case OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID:
    case OB_ALL_VIRTUAL_PS_STAT_TID:
    case OB_ALL_VIRTUAL_PS_ITEM_INFO_TID:
    case OB_ALL_VIRTUAL_MEM_LEAK_CHECKER_INFO_TID:
    case OB_ALL_VIRTUAL_TRANS_MEM_STAT_TID:
    case OB_ALL_VIRTUAL_PARTITION_SSTABLE_IMAGE_INFO_TID:
    case OB_ALL_VIRTUAL_CONCURRENCY_OBJECT_POOL_TID:
    case OB_ALL_VIRTUAL_MEMORY_INFO_TID:
    case OB_ALL_VIRTUAL_TENANT_DISK_STAT_TID:
    case OB_ALL_VIRTUAL_OBRPC_STAT_TID:
    case OB_ALL_VIRTUAL_PARTITION_SSTABLE_MERGE_INFO_TID:
    case OB_ALL_VIRTUAL_PARTITION_SPLIT_INFO_TID:
    case OB_ALL_VIRTUAL_PARTITION_SSTABLE_MACRO_INFO_TID:
    case OB_ALL_VIRTUAL_SQL_MONITOR_TID:
    case OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_TID:
    case OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID:
    case OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_TID:
    case OB_ALL_VIRTUAL_PARTITION_AMPLIFICATION_STAT_TID:
    case OB_ALL_VIRTUAL_PARTITION_STORE_INFO_TID:
    case OB_ALL_VIRTUAL_PARTITION_MIGRATION_STATUS_TID:
    case OB_ALL_VIRTUAL_SYS_TASK_STATUS_TID:
    case OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TID:
    case OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID:
    case OB_ALL_VIRTUAL_LONG_OPS_STATUS_TID:
    case OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_TID:
    case OB_ALL_VIRTUAL_TABLE_MGR_TID:
    case OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_TID:
    case OB_ALL_VIRTUAL_IO_STAT_TID:
    case OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_TID:
    case OB_ALL_VIRTUAL_PX_WORKER_STAT_TID:
    case OB_ALL_VIRTUAL_DTL_CHANNEL_TID:
    case OB_ALL_VIRTUAL_DTL_MEMORY_TID:
    case OB_ALL_VIRTUAL_DTL_FIRST_CACHED_BUFFER_TID:
    case OB_ALL_VIRTUAL_PARTITION_AUDIT_TID:
    case OB_ALL_VIRTUAL_PARTITION_TABLE_STORE_STAT_TID:
    case OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_TID:
    case OB_ALL_VIRTUAL_DEADLOCK_STAT_TID:
    case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TID:
    case OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TID:
    case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TID:
    case OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TID:
    case OB_ALL_VIRTUAL_DUMP_TENANT_INFO_TID:
    case OB_ALL_VIRTUAL_RAID_STAT_TID:
    case OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID:
    case OB_ALL_VIRTUAL_PG_BACKUP_LOG_ARCHIVE_STATUS_TID:
    case OB_ALL_VIRTUAL_SERVER_BACKUP_LOG_ARCHIVE_STATUS_TID:
    case OB_ALL_VIRTUAL_TABLE_MODIFICATIONS_TID:
    case OB_ALL_VIRTUAL_TRANS_TABLE_STATUS_TID:
    case OB_ALL_VIRTUAL_PG_LOG_ARCHIVE_STAT_TID:
    case OB_ALL_VIRTUAL_OPEN_CURSOR_TID:
    case OB_ALL_VIRTUAL_RESERVED_TABLE_MGR_TID:
    case OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_TID: {
      loc_dist_mode = ObSqlPartitionLocationCache::LOC_DIST_MODE_DISTRIBUTED;
      break;
    }
    case OB_ALL_VIRTUAL_CORE_META_TABLE_TID:
    case OB_ALL_VIRTUAL_CORE_ROOT_TABLE_TID:
    case OB_ALL_VIRTUAL_PARTITION_TABLE_TID:
    case OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID:
    case OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID:
    case OB_ALL_VIRTUAL_SERVER_STAT_TID:
    case OB_ALL_VIRTUAL_REBALANCE_TASK_STAT_TID:
    case OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID:
    case OB_ALL_VIRTUAL_TENANT_STAT_TID:
    case OB_ALL_VIRTUAL_REBALANCE_TENANT_STAT_TID:
    case OB_ALL_VIRTUAL_REBALANCE_MAP_STAT_TID:
    case OB_ALL_VIRTUAL_REBALANCE_MAP_ITEM_STAT_TID:
    case OB_ALL_VIRTUAL_REBALANCE_UNIT_STAT_TID:
    case OB_ALL_VIRTUAL_REBALANCE_REPLICA_STAT_TID:
    case OB_ALL_VIRTUAL_LEADER_STAT_TID:
    case OB_ALL_VIRTUAL_ROOTSERVICE_STAT_TID:
    case OB_ALL_VIRTUAL_REPLICA_TASK_TID:
    case OB_ALL_VIRTUAL_REBALANCE_UNIT_MIGRATE_STAT_TID:
    case OB_ALL_VIRTUAL_REBALANCE_UNIT_DISTRIBUTION_STAT_TID:
    case OB_ALL_VIRTUAL_FREEZE_INFO_TID:
    case OB_ALL_VIRTUAL_CLUSTER_TID:
    case OB_ALL_VIRTUAL_BACKUPSET_HISTORY_MGR_TID:
    case OB_ALL_VIRTUAL_BACKUP_CLEAN_INFO_TID: {
      loc_dist_mode = ObSqlPartitionLocationCache::LOC_DIST_MODE_ONLY_RS;
      break;
    }
    case OB_ALL_VIRTUAL_ZONE_STAT_TID: {
      if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_141) {
        loc_dist_mode = ObSqlPartitionLocationCache::LOC_DIST_MODE_ONLY_RS;
      } else {
        loc_dist_mode = ObSqlPartitionLocationCache::LOC_DIST_MODE_ONLY_LOCAL;
      }
      break;
    }
    default: {
      LOG_ERROR("pure table id is invalid", K(pure_tid));
      break;
    }
  }
  return loc_dist_mode;
}

int ObSqlPartitionLocationCache::virtual_get(const uint64_t table_id, const int64_t partition_id,
    share::ObPartitionLocation& location, const int64_t expire_renew_time, bool& is_cache_hit)
{
  UNUSED(partition_id);
  int ret = OB_SUCCESS;
  is_cache_hit = true;

  if (OB_UNLIKELY(NULL == loc_cache_ || !self_addr_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL or self addr is invalid", K(ret), K(loc_cache_), K(self_addr_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret), K(table_id));
  } else if (!is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table id must be virtual table id", K(ret), K(table_id));
  } else {
    location.reset();
    const LocationDistributedMode loc_dist_mode = get_location_distributed_mode(table_id);
    switch (loc_dist_mode) {
      case LOC_DIST_MODE_ONLY_LOCAL: {
        if (OB_FAIL(build_local_location(table_id, location))) {
          LOG_WARN("fail to build local location", K(ret), K(table_id));
        }
        break;
      }
      case LOC_DIST_MODE_DISTRIBUTED: {
        if (OB_FAIL(build_distribute_location(table_id, partition_id, location))) {
          LOG_WARN("fail to build distribute location", K(ret), K(table_id));
        }
        break;
      }
      case LOC_DIST_MODE_ONLY_RS: {
        ObSEArray<ObPartitionLocation, 1> locations;
        if (OB_FAIL(get(table_id, locations, expire_renew_time, is_cache_hit))) {
          LOG_WARN("fail to get locations", K(ret), K(table_id), K(locations), K(expire_renew_time));
        } else if (1 != locations.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("table only on rootserver, locations.count() must be 1", K(ret), K(table_id), K(locations));
        } else if (OB_FAIL(location.assign(locations.at(0)))) {
          LOG_WARN("copy failed", K(ret), K(location), K(locations.at(0)));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("location distributed mode is invalid", K(ret), K(loc_dist_mode), K(table_id));
      }
    }
  }
  return ret;
}

int ObSqlPartitionLocationCache::build_local_location(uint64_t table_id, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  location.set_table_id(table_id);
  location.set_partition_id(0);
  location.set_partition_cnt(1);
  ObReplicaLocation replica_location;
  replica_location.role_ = LEADER;
  replica_location.server_ = self_addr_;
  if (OB_FAIL(location.add(replica_location))) {
    LOG_WARN("location add failed", K(ret));
  }
  return ret;
}

int ObSqlPartitionLocationCache::build_distribute_location(
    uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  location.set_table_id(table_id);
  location.set_partition_id(partition_id);
  location.set_partition_cnt(partition_id + 1);  // partition_cnt should bigger than partition_id
  ObReplicaLocation replica_location;
  replica_location.role_ = LEADER;
  ObAddr addr;
  if (OB_UNLIKELY(NULL == task_exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task exec ctx is NULL", K(ret), K(table_id), K(partition_id));
  } else if (OB_FAIL(task_exec_ctx_->get_addr_by_virtual_partition_id(partition_id, addr))) {
    LOG_WARN("get addr by virtual partition id failed", K(ret), K(table_id), K(partition_id));
  } else if (FALSE_IT(replica_location.server_ = addr)) {
  } else if (OB_FAIL(location.add(replica_location))) {
    LOG_WARN("location add failed", K(ret), K(table_id), K(partition_id));
  }
  return ret;
}

int ObSqlPartitionLocationCache::get_phy_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard ptr is null", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const uint64_t table_id = pkey.get_table_id();
    const share::schema::ObSimpleTableSchemaV2* table_schema = nullptr;
    if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema ptr is null", K(ret));
    } else if (OB_INVALID_ID == table_schema->get_tablegroup_id() || !table_schema->is_binding_table()) {
      pg_key = pkey;
    } else if (OB_FAIL(table_schema->get_pg_key(pkey, pg_key))) {
      LOG_WARN("fail to get pgkey", K(ret));
    }
  }
  return ret;
}

int ObSqlPartitionLocationCache::get_phy_key(
    const uint64_t table_id, const int64_t partition_id, uint64_t& tg_id, int64_t& pg_id, common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_guard ptr is null", K(ret));
  } else if (OB_FAIL(get_phy_key(*schema_guard_, table_id, partition_id, tg_id, pg_id, pg_key))) {
    LOG_WARN("fail to get pg key", K(ret), K(table_id), K(partition_id));
  }

  return ret;
}

int ObSqlPartitionLocationCache::get_phy_key(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t table_id,
    const int64_t partition_id, uint64_t& tg_id, int64_t& pg_id, common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else {
    const share::schema::ObSimpleTableSchemaV2* table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema ptr is null", K(ret));
    } else if (OB_INVALID_ID == table_schema->get_tablegroup_id() || !table_schema->is_binding_table()) {
      tg_id = table_id;
      pg_id = partition_id;
      if (OB_FAIL(pg_key.init(tg_id, pg_id, table_schema->get_partition_cnt()))) {
        LOG_WARN("partition group key init error",
            K(table_id),
            K(partition_id),
            "partition_cnt",
            table_schema->get_partition_cnt());
      }
    } else if (OB_FAIL(table_schema->get_pg_key(table_id, partition_id, pg_key))) {
      LOG_WARN("fail to get pg key", K(ret));
    } else {
      tg_id = pg_key.get_tablegroup_id();
      pg_id = pg_key.get_partition_id();
    }
  }
  return ret;
}

// link table.

int ObSqlPartitionLocationCache::get_link_table_location(const uint64_t table_id, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(loc_cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL", K(ret));
  } else if (OB_FAIL(loc_cache_->get_link_table_location(table_id, location))) {
    LOG_WARN("get link table location failed", K(ret), K(table_id));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
