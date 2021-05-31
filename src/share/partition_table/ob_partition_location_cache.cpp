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

#define USING_LOG_PREFIX SHARE_PT

#include "ob_partition_location_cache.h"

#include "lib/allocator/ob_mod_define.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "share/ob_worker.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/time/ob_time_utility.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/ob_running_mode.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_timeout_ctx.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_rs_mgr.h"
#include "share/partition_table/ob_location_update_task.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_remote_partition_table_operator.h"
#include "share/ob_alive_server_tracer.h"
#include "ob_replica_filter.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_multi_cluster_util.h"
#include "share/ob_remote_server_provider.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_blacklist.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace obrpc;
using namespace share;
using namespace oceanbase::share::schema;

namespace share {

ObLocationCacheKey::ObLocationCacheKey()
    : table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_ID), cluster_id_(common::OB_INVALID_ID)
{}

ObLocationCacheKey::ObLocationCacheKey(const uint64_t table_id, const int64_t partition_id)
    : table_id_(table_id), partition_id_(partition_id), cluster_id_(common::OB_INVALID_ID)
{}

ObLocationCacheKey::ObLocationCacheKey(const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id)
    : table_id_(table_id), partition_id_(partition_id), cluster_id_(cluster_id)
{}

bool ObLocationCacheKey::operator==(const ObIKVCacheKey& other) const
{
  const ObLocationCacheKey& other_key = reinterpret_cast<const ObLocationCacheKey&>(other);
  return table_id_ == other_key.table_id_ && partition_id_ == other_key.partition_id_ &&
         cluster_id_ == other_key.cluster_id_;
}

bool ObLocationCacheKey::operator!=(const ObIKVCacheKey& other) const
{
  const ObLocationCacheKey& other_key = reinterpret_cast<const ObLocationCacheKey&>(other);
  return table_id_ != other_key.table_id_ || partition_id_ != other_key.partition_id_ ||
         cluster_id_ != other_key.cluster_id_;
}

uint64_t ObLocationCacheKey::get_tenant_id() const
{
  // NOTE: location cache isolation is disabled to minimize obproxy memory usage.
  return common::OB_SYS_TENANT_ID;
}

uint64_t ObLocationCacheKey::hash() const
{
  return common::murmurhash(this, sizeof(*this), 0);
}

uint64_t ObLocationCacheKey::hash_v2() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&partition_id_, sizeof(partition_id_), hash_val);
  hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), hash_val);
  return hash_val;
}

int64_t ObLocationCacheKey::size() const
{
  return sizeof(*this);
}

int ObLocationCacheKey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;

  if (NULL == buf || buf_len < size()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), "size", size());
  } else {
    ObLocationCacheKey* pkey = new (buf) ObLocationCacheKey();
    *pkey = *this;
    key = pkey;
  }

  return ret;
}

ObLocationCacheValue::ObLocationCacheValue() : size_(0), buffer_(NULL)
{}

int64_t ObLocationCacheValue::size() const
{
  return static_cast<int64_t>(sizeof(*this) + size_);
}

int ObLocationCacheValue::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;

  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), "size", size());
  } else {
    ObLocationCacheValue* pvalue = new (buf) ObLocationCacheValue();
    pvalue->size_ = size_;
    pvalue->buffer_ = buf + sizeof(*this);
    MEMCPY(pvalue->buffer_, buffer_, size_);
    value = pvalue;
  }

  return ret;
}
///////////////////////////////////////////
int ObTenantSqlRenewStat::assign(const ObTenantSqlRenewStat& other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    start_sql_renew_ts_ = other.start_sql_renew_ts_;
    sql_renew_count_ = other.sql_renew_count_;
  }
  return ret;
}
///////////////////////////////////////////
int ObTenantStatGuard::set_tenant_stat(const uint64_t tenant_id, TenantSqlRenewInfoMap& map,
    common::ObCachedAllocator<ObTenantSqlRenewStat>& object_pool, ObSpinLock& lock)
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_SUCC(map.get_refactored(tenant_id, tenant_stat_))) {
    tenant_stat_->lock_.lock();
    tenant_id_ = tenant_id;
  } else if (OB_HASH_NOT_EXIST != ret) {
    tenant_stat_ = NULL;
    STORAGE_LOG(WARN, "partition map get error", KR(ret), K(tenant_id), K(lbt()));
  } else {
    ObSpinLockGuard guard(lock);
    if (OB_SUCC(map.get_refactored(tenant_id, tenant_stat_))) {
      tenant_stat_->lock_.lock();
      tenant_id_ = tenant_id;
    } else if (OB_HASH_NOT_EXIST != ret) {
      tenant_stat_ = NULL;
      STORAGE_LOG(WARN, "partition map get error", KR(ret), K(tenant_id), K(lbt()));
    } else {
      tenant_stat_ = object_pool.alloc();
      if (OB_ISNULL(tenant_stat_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc tenant stat failed", K(ret), K(tenant_id));
      } else {
        tenant_stat_->tenant_id_ = tenant_id;
        tenant_stat_->start_sql_renew_ts_ = ObTimeUtility::current_time();
        tenant_stat_->sql_renew_count_ = 0;
        if (OB_FAIL(map.set_refactored(tenant_id, tenant_stat_))) {
          LOG_WARN("fail to insert and get", KR(ret), K(tenant_id));
          object_pool.free(tenant_stat_);
          tenant_stat_ = NULL;
        } else {
          tenant_stat_->lock_.lock();
          tenant_id_ = tenant_id;
        }
      }
    }
  }
  return ret;
}

///////////////////////////////////////////
void ObTenantSqlRenewStat::init(const uint64_t tenant_id)
{
  tenant_id_ = tenant_id;
  start_sql_renew_ts_ = ObTimeUtility::current_time();
  sql_renew_count_ = 0;
}

void ObTenantSqlRenewStat::reset()
{
  tenant_id_ = OB_INVALID_ID;
  start_sql_renew_ts_ = 0;
  sql_renew_count_ = 0;
}

ObLocationRpcRenewInfo::ObLocationRpcRenewInfo() : addr_(), arg_(NULL), idx_array_(NULL)
{}

ObLocationRpcRenewInfo::~ObLocationRpcRenewInfo()
{}

void ObLocationRpcRenewInfo::reset()
{
  addr_.reset();
  arg_ = NULL;
  idx_array_ = NULL;
}

bool ObLocationRpcRenewInfo::is_valid() const
{
  return addr_.is_valid() && OB_NOT_NULL(arg_) && OB_NOT_NULL(idx_array_) && arg_->keys_.count() == idx_array_->count();
}

ObReplicaRenewKey::ObReplicaRenewKey() : table_id_(OB_INVALID_ID), partition_id_(OB_INVALID_ID), addr_()
{}

ObReplicaRenewKey::ObReplicaRenewKey(const uint64_t table_id, const int64_t partition_id, const ObAddr& addr)
    : table_id_(table_id), partition_id_(partition_id), addr_(addr)
{}

ObReplicaRenewKey::~ObReplicaRenewKey()
{}

uint64_t ObReplicaRenewKey::hash() const
{
  uint64_t hash_val = addr_.hash();
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&partition_id_, sizeof(partition_id_), hash_val);
  return hash_val;
}

bool ObReplicaRenewKey::operator==(const ObReplicaRenewKey& other) const
{
  return table_id_ == other.table_id_ && partition_id_ == other.partition_id_ && addr_ == other.addr_;
}

///////////////////////////////////////////
int ObLocationLeaderCache::get_strong_leader_info(const ObLocationCacheKey& key, LocationInfo& leader_info)
{
  int ret = OB_SUCCESS;
  int64_t idx = key.hash_v2() % CACHE_NUM;
  ObLocationLeaderInfo& info = buffer_[idx];
  SpinRLockGuard guard(info.get_lock());
  if (OB_ISNULL(info.get_value())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (key != info.get_value()->get_key()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    leader_info = info.get_value()->get_strong_leader_info();
    if (!leader_info.is_valid()) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  if (OB_FAIL(ret) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("fail to get leader", K(ret));
  }
  return ret;
}

int ObLocationLeaderCache::set_strong_leader_info(
    const ObLocationCacheKey& key, const LocationInfo& location_info, bool force_update)
{
  int ret = OB_SUCCESS;
  int64_t idx = key.hash_v2() % CACHE_NUM;
  ObLocationLeaderInfo& info = buffer_[idx];
  SpinWLockGuard guard(info.get_lock());
  ObLocationLeader*& value_ptr = info.get_value();
  if (force_update) {
    if (OB_ISNULL(value_ptr)) {
      void* buf = NULL;
      if (NULL == (buf = allocator_.alloc(sizeof(ObLocationLeader)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem", K(ret), K(key), K(location_info));
      } else if (FALSE_IT(value_ptr = new (buf) ObLocationLeader())) {
        // will not reach here
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(value_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be here", K(ret), K(key), K(location_info));
    } else {
      value_ptr->set_key(key);
      value_ptr->set_strong_leader_info(location_info);
      LOG_DEBUG("force set leader", K(key), K(location_info));
    }
  } else if (OB_NOT_NULL(value_ptr) && value_ptr->get_key() == key) {
    value_ptr->set_key(key);
    value_ptr->set_strong_leader_info(location_info);
    LOG_DEBUG("set leader", K(key), K(location_info));
  }
  return ret;
}

///////////////////////////////////////////
bool ObILocationFetcher::treat_sql_as_timeout(const int error_code)
{
  bool bool_ret = false;

  switch (error_code) {
    case OB_CONNECT_ERROR:
    case OB_TIMEOUT:
    case OB_WAITQUEUE_TIMEOUT:
    case OB_SESSION_NOT_FOUND:
    case OB_TRANS_TIMEOUT:
    case OB_TRANS_STMT_TIMEOUT:
    case OB_TRANS_UNKNOWN:
    case OB_GET_LOCATION_TIME_OUT:
      bool_ret = true;
      break;
    default:
      bool_ret = false;
  }
  return bool_ret;
}

uint64_t ObILocationFetcher::get_rpc_tenant_id(const uint64_t table_id)
{
  uint64_t tenant_id = 0;

  if (extract_pure_id(table_id) == OB_ALL_CORE_TABLE_TID) {
    tenant_id = OB_LOC_CORE_TENANT_ID;
  } else if (extract_pure_id(table_id) == OB_ALL_ROOT_TABLE_TID) {
    tenant_id = OB_LOC_ROOT_TENANT_ID;
  } else if (is_sys_table(table_id)) {
    tenant_id = OB_LOC_SYS_TENANT_ID;
  } else {
    tenant_id = OB_LOC_USER_TENANT_ID;
  }

  return tenant_id;
}

int ObILocationFetcher::partition_table_fetch_location(ObPartitionTableOperator* pt, const int64_t cluster_id,
    const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator partition_info_allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition_info;
  partition_info.set_allocator(&partition_info_allocator);
  bool rpc_tenant_set = false;
  uint64_t old_rpc_tenant = THIS_WORKER.get_rpc_tenant();
  bool need_fetch_faillist = false;

  if (NULL == pt || OB_INVALID_ID == table_id || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pt), K(cluster_id), KT(table_id), K(partition_id));
  } else {
    if (extract_pure_id(table_id) == OB_ALL_CORE_TABLE_TID) {
      THIS_WORKER.set_rpc_tenant(OB_LOC_CORE_TENANT_ID);
    } else if (extract_pure_id(table_id) == OB_ALL_ROOT_TABLE_TID) {
      THIS_WORKER.set_rpc_tenant(OB_LOC_ROOT_TENANT_ID);
    } else if (is_sys_table(table_id)) {
      THIS_WORKER.set_rpc_tenant(OB_LOC_SYS_TENANT_ID);
    } else {
      THIS_WORKER.set_rpc_tenant(OB_LOC_USER_TENANT_ID);
    }
    rpc_tenant_set = true;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pt->get(table_id, partition_id, partition_info, need_fetch_faillist, cluster_id))) {
    LOG_WARN("Partition Operator get failed", K(cluster_id), KT(table_id), K(partition_id), K(ret));
    if (treat_sql_as_timeout(ret)) {
      ret = OB_GET_LOCATION_TIME_OUT;
    }
  } else if (OB_FAIL(fill_location(partition_info, location))) {
    LOG_WARN("fail to fill location", K(ret), K(partition_info));
  }

  if (rpc_tenant_set) {
    THIS_WORKER.set_rpc_tenant(old_rpc_tenant);
  }
  return ret;
}

int ObILocationFetcher::partition_table_batch_fetch_location(ObPartitionTableOperator* pt, const int64_t cluster_id,
    const common::ObIArray<common::ObPartitionKey>& keys, common::ObIAllocator& allocator,
    common::ObIArray<ObPartitionLocation*>& new_locations)
{
  int ret = OB_SUCCESS;
  uint64_t old_rpc_tenant = THIS_WORKER.get_rpc_tenant();

  if (keys.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("keys cnt is invalid", K(ret));
  } else if (NULL == pt || !keys.at(0).is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pt), "key", keys.at(0));
  } else {
    uint64_t table_id = keys.at(0).get_table_id();
    THIS_WORKER.set_rpc_tenant(get_rpc_tenant_id(table_id));
  }

  ObArray<ObPartitionInfo*> partition_infos;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pt->batch_fetch_partition_infos(keys, allocator, partition_infos, cluster_id))) {
    LOG_WARN("Partition Operator get failed", K(ret), K(cluster_id), "key_cnt", keys.count());
    if (treat_sql_as_timeout(ret)) {
      ret = OB_GET_LOCATION_TIME_OUT;
    }
  } else if (keys.count() != partition_infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cnt not match", K(ret), "key_cnt", keys.count(), "partition_cnt", partition_infos.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); i++) {
      ObPartitionInfo*& partition = partition_infos.at(i);
      ObPartitionLocation* location = NULL;
      if (OB_FAIL(ObPartitionLocation::alloc_new_location(allocator, location))) {
        LOG_WARN("fail to alloc location", K(ret), KPC(partition));
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret));
      } else if (OB_FAIL(fill_location(*partition, *location))) {
        LOG_WARN("fail to fill location", K(ret), KPC(partition));
      } else if (OB_FAIL(new_locations.push_back(location))) {
        LOG_WARN("fail to push back location", K(ret), KPC(location));
      } else if (location->get_replica_locations().count() > 0) {
        ObTaskController::get().allow_next_syslog();
        LOG_INFO(
            "location cache not hit, batch fetch location by partition_table", K(ret), K(cluster_id), KPC(location));
      }
    }
  }
  // always release partition_infos
  for (int64_t i = 0; i < partition_infos.count(); i++) {
    ObPartitionInfo*& partition = partition_infos.at(i);
    if (OB_NOT_NULL(partition)) {
      partition->~ObPartitionInfo();
      partition = NULL;
    }
  }

  if (old_rpc_tenant != THIS_WORKER.get_rpc_tenant()) {
    THIS_WORKER.set_rpc_tenant(old_rpc_tenant);
  }
  return ret;
}

int ObILocationFetcher::fill_location(ObPartitionInfo& partition_info, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = partition_info.get_table_id();
  const int64_t partition_id = partition_info.get_partition_id();
  int64_t partition_cnt = OB_INVALID_SIZE;
  ObInvalidPaxosReplicaFilter filter;
  ObStatusReplicaFilter status_filter(REPLICA_STATUS_NORMAL);
  LOG_TRACE(
      "get partition info through partition table operator succeed", KT(table_id), K(partition_id), K(partition_info));
  if (OB_FAIL(partition_info.filter(filter))) {
    LOG_WARN("filter replica type failed", K(ret), K(partition_info));
  } else if (OB_FAIL(partition_info.filter(status_filter))) {
    LOG_WARN("fail to filter status", K(ret), K(partition_info));
  } else {
    location.set_table_id(table_id);
    location.set_partition_id(partition_id);
    const ObIArray<ObPartitionReplica>& replicas = partition_info.get_replicas_v2();
    ObReplicaLocation replica_location;
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
      if (!replicas.at(i).is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is not valid", "replica", replicas.at(i), K(ret));
      } else {
        replica_location.reset();
        replica_location.server_ = replicas.at(i).server_;
        replica_location.role_ = replicas.at(i).role_;
        replica_location.replica_type_ = replicas.at(i).replica_type_;
        replica_location.sql_port_ = replicas.at(i).sql_port_;
        replica_location.property_ = replicas.at(i).property_;
        replica_location.restore_status_ = replicas.at(i).is_restore_;
        // TODO(): just to make it work
        partition_cnt = replicas.at(i).partition_cnt_;
        if (OB_FAIL(location.add(replica_location))) {
          LOG_WARN("add replica location to partition location failed",
              KT(table_id),
              K(partition_id),
              K(replica_location),
              K(location),
              K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      location.set_partition_cnt(partition_cnt);
      location.set_renew_time(ObTimeUtility::current_time());
      location.set_sql_renew_time(ObTimeUtility::current_time());
      ObReplicaLocation leader;
      if (location.size() <= 0) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("location is empty", K(ret), K(location));
      } else if (OB_FAIL(location.get_leader_by_election(leader))) {
        if (OB_LOCATION_LEADER_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_DEBUG("location leader not exist", K(ret), K(location));
        } else {
          LOG_DEBUG("location get leader failed", K(ret), K(location));
        }
      }
    }
  }
  if (EXECUTE_COUNT_PER_SEC(16)) {
    LOG_INFO("fetch location with tenant",
        "tenant",
        THIS_WORKER.get_rpc_tenant(),
        K(table_id),
        K(partition_id),
        K(ret),
        K(location));
  }
  return ret;
}

ObLocationFetcher::ObLocationFetcher()
    : inited_(false),
      config_(NULL),
      pt_(NULL),
      remote_pt_(NULL),
      rs_mgr_(NULL),
      rpc_proxy_(NULL),
      srv_rpc_proxy_(NULL),
      locality_manager_(NULL),
      cluster_id_(OB_INVALID_ID)
{}

ObLocationFetcher::~ObLocationFetcher()
{}

int ObLocationFetcher::init(ObServerConfig& config, ObPartitionTableOperator& pt,
    ObRemotePartitionTableOperator& remote_pt, ObRsMgr& rs_mgr, ObCommonRpcProxy& rpc_proxy,
    obrpc::ObSrvRpcProxy& srv_rpc_proxy, ObILocalityManager* locality_manager, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(locality_manager) || OB_INVALID_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(locality_manager), K(cluster_id));
  } else {
    config_ = &config;
    pt_ = &pt;
    remote_pt_ = &remote_pt;
    rs_mgr_ = &rs_mgr;
    rpc_proxy_ = &rpc_proxy;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    locality_manager_ = locality_manager;
    cluster_id_ = cluster_id;
    inited_ = true;
  }
  return ret;
}

int ObLocationFetcher::fetch_location(
    const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id, ObPartitionLocation& location)
{
  NG_TRACE(renew_loc_by_sql_begin);
  int ret = OB_SUCCESS;
  location.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else if (is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is virtual table", KT(table_id), K(ret));
  } else if (cluster_id_ == cluster_id) {
    int64_t tmp_cluster_id = OB_INVALID_ID;
    if (OB_FAIL(partition_table_fetch_location(pt_, tmp_cluster_id, table_id, partition_id, location))) {
      LOG_WARN("fetch location through partition table operator failed",
          K(cluster_id),
          KT(table_id),
          K(partition_id),
          K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("renew_location_with_sql success", K(table_id), K(partition_id), K(location));
    }
  } else {
    if (OB_FAIL(partition_table_fetch_location(remote_pt_, cluster_id, table_id, partition_id, location))) {
      LOG_WARN("fetch location through remote partition table operator failed",
          K(cluster_id_),
          K(cluster_id),
          KT(table_id),
          K(partition_id),
          K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("renew_remote_location_with_sql success",
          K(table_id),
          K(partition_id),
          K(cluster_id_),
          K(cluster_id),
          K(location));
    }
  }
  NG_TRACE_EXT(renew_loc_by_sql_end, OB_ID(ret), ret, OB_ID(table_id), table_id, OB_ID(partition_id), partition_id);
  return ret;
}

int ObLocationFetcher::fetch_vtable_location(const uint64_t table_id, ObSArray<ObPartitionLocation>& locations)
{
  NG_TRACE(fetch_vtable_loc_begin);
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || !is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not virtual table", KT(table_id), K(ret));
  } else {
    LOG_INFO("fetch virtual table location with tenant", "tenant", THIS_WORKER.get_rpc_tenant(), KT(table_id));
    int64_t timeout_us = OB_FETCH_LOCATION_TIMEOUT;
    if (ObTimeoutCtx::get_ctx().is_timeout_set()) {
      timeout_us = std::min(timeout_us, ObTimeoutCtx::get_ctx().get_timeout());
    }
    if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("process timeout", K(ret), K(timeout_us));
    } else if (OB_ISNULL(rs_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rs_mgr is null", KR(ret));
    } else if (OB_FAIL(rpc_proxy_->to_rs(*rs_mgr_)
                           .by(OB_LOC_SYS_TENANT_ID)
                           .timeout(timeout_us)
                           .fetch_location(table_id, locations))) {
      LOG_WARN("fetch location through rpc failed", KT(table_id), K(ret));
    } else {
      const int64_t now = ObTimeUtility::current_time();
      for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); ++i) {
        locations.at(i).set_renew_time(now);
      }
      if (0 == locations.count()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("locations is empty", K(ret));
      }
    }
  }
  NG_TRACE_EXT(fetch_vtable_loc_end, OB_ID(ret), ret, OB_ID(table_id), table_id);
  return ret;
}

int ObLocationFetcher::batch_fetch_location(const common::ObIArray<common::ObPartitionKey>& keys,
    const int64_t cluster_id, common::ObIAllocator& allocator, common::ObIArray<ObPartitionLocation*>& new_locations)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (keys.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("keys count is invalid", K(ret));
  } else if (cluster_id_ == cluster_id) {
    int64_t tmp_cluster_id = OB_INVALID_ID;
    if (OB_FAIL(partition_table_batch_fetch_location(pt_, tmp_cluster_id, keys, allocator, new_locations))) {
      LOG_WARN("fetch location through partition table operator failed", K(ret), K(cluster_id), "cnt", keys.count());
    } else {
      LOG_INFO("batch_renew_location_with_sql success", K(ret), "cnt", keys.count());
    }
  } else {
    if (OB_FAIL(partition_table_batch_fetch_location(remote_pt_, cluster_id, keys, allocator, new_locations))) {
      LOG_WARN("fetch location through remote partition table operator failed",
          K(ret),
          K(cluster_id_),
          K(cluster_id),
          "cnt",
          keys.count());
    } else {
      LOG_INFO(
          "batch_renew_remote_location_with_sql success", K(ret), K(cluster_id_), K(cluster_id), "cnt", keys.count());
    }
  }
  return ret;
}

/*
 * In most cases, location cache of sys tenant are almost the same. So, we can try to use __all_core_table's
 * location_cache to renew other system tables' location cache in sys tenant, which may also reduce the possibility
 * of circular dependency.
 */
int ObLocationFetcher::batch_renew_sys_table_location_by_rpc(const ObPartitionLocation& core_table_location,
    const common::ObIArray<ObLocationCacheKey>& keys, common::ObIArray<ObLocationLeader>& results)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy is null", K(ret));
  } else {
    results.reset();
    obrpc::ObBatchGetRoleArg arg;
    const int64_t timeout_ts = OB_FETCH_MEMBER_LIST_AND_LEADER_TIMEOUT;
    rootserver::ObBatchGetRoleProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::batch_get_role);
    const ObIArray<ObReplicaLocation>& location_array = core_table_location.get_replica_locations();
    /*
     * partition_cnt is meaningful to construct ObPartitionKey before ver 1.4.40,
     * and its value is 0 if table is created not less than ver 1.4.40.
     *
     * For compatibility, we should get partition_cnt from schema service, storage, or meta table.
     * But this function only works when partition's location cache is empty, which means we can get partiton_cnt from
     * nowhere.
     *
     * Because we won't upgrade cluster from ver 1.4.x to ver 2.x/3.x any more (Use OMS to migrate data across clusters
     * instead), we pretend partition_cnt is 0 here.
     */
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); i++) {
      ObPartitionKey key(keys.at(i).table_id_, keys.at(i).partition_id_, 0);
      if (OB_FAIL(arg.keys_.push_back(key))) {
        LOG_WARN("fail to push back arg", K(ret), K(key));
      }
    }
    for (int64_t i = 0; i < location_array.count() && OB_SUCC(ret); i++) {
      const ObAddr& addr = location_array.at(i).server_;
      if (common::REPLICA_TYPE_FULL != location_array.at(i).replica_type_) {
        // skip
      } else if (OB_FAIL(proxy.call(addr, timeout_ts, arg))) {
        LOG_WARN("fail to get role", K(ret), K(addr), K(arg));
      }
    }
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;  // always wait all
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", K(tmp_ret), K(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
      int res_ret = return_code_array.at(i);
      if (OB_SUCCESS == res_ret) {
        const ObBatchGetRoleResult* result = proxy.get_results().at(i);
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret));
        } else {
          const ObBatchGetRoleResult* result = proxy.get_results().at(i);
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", K(ret));
          } else if (result->results_.count() != arg.keys_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("count not match", K(ret), "keys_cnt", arg.keys_.count(), "result_cnt", result->results_.count());
          } else {
            const int64_t now = common::ObTimeUtility::current_time();
            for (int64_t j = 0; OB_SUCC(ret) && j < result->results_.count(); j++) {
              if (OB_SUCCESS == result->results_.at(j)) {
                ObLocationLeader info;
                info.set_strong_leader_info(LocationInfo(proxy.get_dests().at(i), now));
                info.set_key(keys.at(j));
                if (OB_FAIL(results.push_back(info))) {
                  LOG_WARN("fail to push back result", K(ret), K(info));
                }
              }
            }
          }
        }
      }
      ret = OB_SUCCESS;  // overwrite ret;
    }
  }
  return ret;
}

/*
 * Consider tenant locality may across multi region, it's a high cost if we fetch location cache
 * by sending rpc to all servers in member_list. So this function would only send rpc
 * to server which is in member_list and local region. In some cases, non paxos members' location cache
 * may be imprecise if we only renew location cache by RPC.
 *
 * TODO: () renew location asynchronously if only non-paxos member has changed.
 */
int ObLocationFetcher::renew_location_with_rpc_v2(
    common::hash::ObHashMap<ObReplicaRenewKey, const ObMemberListAndLeaderArg*>* result_map,
    const ObPartitionLocation& cached_location, ObPartitionLocation& new_location, bool& is_new_location_valid)
{
  NG_TRACE(renew_loc_by_rpc_begin);
  int ret = OB_SUCCESS;
  is_new_location_valid = true;
  common::ObPartitionKey pkey;
  const uint64_t tenant_id = get_rpc_tenant_id(cached_location.get_table_id());
  const int64_t default_timeout = OB_FETCH_MEMBER_LIST_AND_LEADER_TIMEOUT;
  int64_t timeout_us = default_timeout;
  const ObIArray<ObReplicaLocation>& location_array = cached_location.get_replica_locations();
  ObMemberListAndLeaderArg member_info;
  const ObMemberListAndLeaderArg* member_info_ptr = &member_info;
  ObReplicaLocation old_leader;
  ObReplicaLocation new_leader;  // only use server_ and role_
  ObSEArray<ObReplicaMember, OB_DEFAULT_SE_ARRAY_COUNT> non_paxos_replicas;
  bool batch_renew = OB_NOT_NULL(result_map);
  // can get restore leader by RPC not less than ver 2.2.74
  bool new_mode = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2274;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", KR(ret));
  } else if (!cached_location.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(cached_location));
  } else if (OB_FAIL(cached_location.get_partition_key(pkey))) {
    LOG_WARN("failed to get partition key", KR(ret), K(cached_location));
  } else if (OB_FAIL(new_location.assign(cached_location))) {
    LOG_WARN("failed to assign location", KR(ret), K(cached_location));
  } else {
    ret = new_mode ? cached_location.get_leader_by_election(old_leader) : cached_location.get_strong_leader(old_leader);
    if (OB_LOCATION_LEADER_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("no leader in cached location", K(new_mode), K(cached_location));
    } else if (OB_FAIL(ret)) {
      LOG_DEBUG("failed to get leader", KR(ret), K(new_mode), K(cached_location));
    }
  }
  /*
   * Using infos by RPC from servers in member_list(from old location cache)
   * and in local region to construct new location cache.
   *
   * If there are only the following difference between two location cache,
   * we consider renew location by RPC successfully.
   * 1. only leader changed.
   * 2. member_list not changed.
   * 3. only paxos member's replica type changed(F->L).
   * 4. non paxos member not changed.
   */
  int64_t local_paxos_cnt = 0;
  ObSEArray<ObAddr, OB_DEFAULT_SE_ARRAY_COUNT> leader_infos;  // leader list gather from server in local region
  for (int64_t i = 0; OB_SUCC(ret) && is_new_location_valid && i < location_array.count(); i++) {
    const ObReplicaLocation& replica_location = location_array.at(i);
    const ObAddr& addr = replica_location.server_;
    bool is_local = false;
    if (ObTimeoutCtx::get_ctx().is_timeout_set()) {
      timeout_us = std::min(ObTimeoutCtx::get_ctx().get_timeout(), default_timeout);
    }
    if (timeout_us <= 0) {
      ret = OB_GET_LOCATION_TIME_OUT;
      LOG_WARN("get location operation is already timeout", KR(ret), K(timeout_us));
    } else if (OB_ISNULL(locality_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("locality manager ptr is null", KR(ret));
    } else if (OB_FAIL(locality_manager_->is_local_server(addr, is_local))) {
      LOG_WARN("fail to check is local server", KR(ret), K(addr));
    } else if (!is_local || !ObReplicaTypeCheck::is_paxos_replica_V2(replica_location.replica_type_)) {
      continue;
    } else if (FALSE_IT(local_paxos_cnt++)) {
    } else if (!batch_renew) {
      member_info.reset();
      /*
       * Old implement of get_member_list_and_leader () will cause error when we call on restore replica.
       * When old and new observers are mixed in upgrade, renew location by RPC in new observer will cause error
       * and we will renew location by SQL instead.
       */
      if (OB_ISNULL(srv_rpc_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("srv_rpc_proxy ptr is null", KR(ret));
      } else if (OB_FAIL(srv_rpc_proxy_->to(addr)
                             .by(tenant_id)
                             .timeout(timeout_us)
                             .get_member_list_and_leader(pkey, member_info))) {
        LOG_WARN("fail to get replica info", KR(ret), K(addr), K(pkey), K(timeout_us), K(old_leader), K(member_info));
      } else {
        member_info_ptr = &member_info;
      }
    } else {
      ObReplicaRenewKey key(pkey.get_table_id(), pkey.get_partition_id(), addr);
      if (OB_FAIL(result_map->get_refactored(key, member_info_ptr))) {
        LOG_WARN("fail to get member info from map", KR(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(member_info_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("member_info is null", KR(ret), K(pkey));
    } else {
      LOG_DEBUG("get leader_and_member_list", K(pkey), K(addr), KPC(member_info_ptr));
      if (OB_FAIL(deal_with_replica_type_changed(
              new_mode, pkey, replica_location, *member_info_ptr, new_location, is_new_location_valid))) {
        LOG_WARN("fail to deal with replica_type changed",
            KR(ret),
            K(new_mode),
            K(pkey),
            K(replica_location),
            KPC(member_info_ptr));
      } else if (!is_new_location_valid) {
        // skip
      } else if (OB_FAIL(add_non_paxos_replica(*member_info_ptr, non_paxos_replicas))) {
        LOG_WARN("fail to add readonly replica", KR(ret), K(new_mode), K(pkey), KPC(member_info_ptr));
      } else if (OB_FAIL(check_leader_and_member_list(
                     new_mode, pkey, addr, location_array, *member_info_ptr, new_leader, is_new_location_valid))) {
        LOG_WARN("fail to check leader and member_list",
            KR(ret),
            K(new_mode),
            K(pkey),
            K(addr),
            K(location_array),
            KPC(member_info_ptr));
      } else if (!new_leader.is_valid() && member_info_ptr->leader_.is_valid() &&
                 !has_exist_in_array(leader_infos, member_info_ptr->leader_) &&
                 OB_FAIL(leader_infos.push_back(member_info_ptr->leader_))) {
        // Use leader returned by follower if local region don't have leader.
        LOG_WARN("fail to push back leader", KR(ret), KPC(member_info_ptr));
      }
    }
  }  // end for

  if (local_paxos_cnt <= 0) {
    // double check, local region should have at least one paxos member if we come here.
    is_new_location_valid = false;
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("local region paxos member cnt less than 1, rpc renew should failed",
        "table_id",
        cached_location.get_table_id(),
        "partition_id",
        cached_location.get_partition_id(),
        K(cached_location),
        K(lbt()));
  }

  if (OB_SUCC(ret) && !new_leader.server_.is_valid() && is_new_location_valid) {
    // no leader in local region, try use leader from followers(avoid renew location cache by SQL)
    if (1 == leader_infos.count()) {
      if (!GCTX.is_standby_cluster() || ObMultiClusterUtil::is_cluster_private_table(cached_location.get_table_id())) {
        /*
         * If local region have no leader, we can use followers' leader instead.
         *
         * In Primary Cluster:
         * 1. While locality across multi regions:
         *    At last followers will get lastest leader by election.
         * 2. Physical restore (create partitions with member_list):
         *    Followers can't get leader's role(leader maybe is restore leader), we pretend leader from
         *    followers is strong leader because we won't(or can't) strong read or write from restore leader.
         *
         * In Standby Cluster:
         *   For cluster private table, leader must be strong leader.
         *
         * So, we can pretend leader is strong leader while we can only get leader from followers in local region.
         */
        new_leader.server_ = leader_infos.at(0);
        new_leader.role_ = LEADER;
      } else {
        /*
         * Normally, standby cluster won't across regions. For non cluster private table in standby cluster,
         * we renew location by SQL if there are no leader in local region.
         */
        is_new_location_valid = false;
      }
    }
  }

  // TODO:() When leader not exist, renew location asynchronously.

  // check if non paxos members are not changed.
  if (OB_SUCC(ret) && is_new_location_valid) {
    bool is_same = false;
    if (OB_FAIL(check_non_paxos_replica(location_array, non_paxos_replicas, is_same))) {
      LOG_WARN("fail to check readonly replica", KR(ret), K(new_mode), K(cached_location), K(non_paxos_replicas));
    } else if (!is_same) {
      is_new_location_valid = false;
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("check non paxos replica not same, need renew by sql",
          K(pkey),
          K(new_mode),
          K(location_array),
          K(non_paxos_replicas));
    } else if (OB_FAIL(new_location.change_leader(new_leader))) {
      LOG_WARN("failed to change leader", KR(ret), K(new_mode), K(new_leader), K(new_location), K(cached_location));
    } else {
      new_location.set_renew_time(ObTimeUtility::current_time());
      new_location.unmark_fail();
      if (old_leader.server_ != new_leader.server_) {
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("leader changed, renew_location_with_rpc success",
            "table_id",
            cached_location.get_table_id(),
            "partition_id",
            cached_location.get_partition_id(),
            K(new_mode),
            K(cached_location),
            K(new_location),
            K(lbt()));
      } else {
        LOG_DEBUG("renew_location_with_rpc success",
            "table_id",
            cached_location.get_table_id(),
            "partition_id",
            cached_location.get_partition_id(),
            K(new_mode),
            K(cached_location),
            K(new_location),
            K(lbt()));
      }
    }
  }
  if (OB_FAIL(ret) || !is_new_location_valid) {
    new_location.reset();
    is_new_location_valid = false;
  }
  EVENT_INC(LOCATION_CACHE_RPC_CHECK);
  NG_TRACE_EXT(renew_loc_by_rpc_end,
      OB_ID(ret),
      ret,
      OB_ID(table_id),
      pkey.get_table_id(),
      OB_ID(partition_id),
      pkey.get_partition_id());
  return ret;
}

/*
 * @description:
 *   Check if only replica type of paxos member is changed.
 *   If so, update location cache and renew location cache by RPC successfully.
 *   Otherwise, try update location cache by SQL later.
 * @param[in]:
 *   - new_mode : new_mode = true means role is meaningful
 *   - pkey : partition key
 *   - old_replica_location : old location cache
 *   - member_info : got from rpc
 * @param[out]:
 *   - new_location : new location cache
 * @param[in/out]:
 *   - is_new_location_valid: If it's true, it means renew location by RPC is successful for now.
 * @return err_code
 */
int ObLocationFetcher::deal_with_replica_type_changed(const bool new_mode, const ObPartitionKey& pkey,
    const ObReplicaLocation& old_replica_location, const ObMemberListAndLeaderArg& member_info,
    ObPartitionLocation& new_location, bool& is_new_location_valid)
{
  int ret = OB_SUCCESS;
  const ObAddr& addr = old_replica_location.server_;
  // check if paxos member's replica type is changed by replica type from rpc
  if (!is_new_location_valid) {
    // skip
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1460) {
    // skip
  } else if (common::REPLICA_TYPE_MAX == member_info.replica_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica type is invalid", KR(ret), K(pkey), K(member_info));
  } else if (new_mode && common::INVALID_ROLE == member_info.role_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("role should be valid", KR(ret), K(member_info));
  } else if (!member_info.check_leader_is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("member_info's self_/leader_/role_ not match", KR(ret), K(new_mode), K(pkey), K(member_info));
  } else if (common::REPLICA_TYPE_FULL != member_info.replica_type_ && addr == member_info.leader_) {
    // defence, leader's replica type must be F.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica_type should be F", KR(ret), K(pkey), K(member_info), K(addr));
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(member_info.replica_type_)) {
    // If paxos member's replica type change to R, it should renew location cache by SQL,
    // because only renew location cache by RPC can't get precise non paxos member's locations.
    is_new_location_valid = false;
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("member replica_type has changed, need renew by sql", K(pkey), K(member_info));
  } else if (old_replica_location.replica_type_ != member_info.replica_type_) {
    // replica type changed, but it's still paxos type.
    ObReplicaLocation replica_location = old_replica_location;
    replica_location.replica_type_ = member_info.replica_type_;
    if (addr == member_info.leader_) {  // new_mode = true means role is meaningful
      replica_location.role_ = new_mode ? member_info.role_ : LEADER;
    } else {
      replica_location.role_ = FOLLOWER;
    }
    replica_location.property_ = member_info.property_;
    if (OB_FAIL(new_location.update(addr, replica_location))) {
      LOG_WARN("update cached_location fail", KR(ret), K(pkey), K(addr), K(replica_location));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("member replica_type has changed, need modify", K(pkey), K(new_location), K(member_info));
    }
  }
  return ret;
}

/*
 * @description:
 *   check if leader and member_list are same with old location.
 *   1. If leader exist, check leader's member_list only.
 *   2. If leader doesn't exist, check all followers' member_list.
 * @param[in]:
 *   - new_mode : new_mode = true means role is meaningful
 *   - pkey : partition key
 *   - addr : rpc destination
 *   - location_array : old location cache
 *   - member_info : got from rpc
 * @param[in/out]:
 *   - new_leader : leader got by RPC
 *   - is_new_location_valid: If it's true, it means renew location by RPC is successful for now.
 * @return err_code
 */
int ObLocationFetcher::check_leader_and_member_list(const bool new_mode, const ObPartitionKey& pkey, const ObAddr& addr,
    const ObIArray<ObReplicaLocation>& location_array, const ObMemberListAndLeaderArg& member_info,
    ObReplicaLocation& new_leader, bool& is_new_location_valid)
{
  int ret = OB_SUCCESS;
  if (!is_new_location_valid) {
    // skip
  } else if (new_mode && common::INVALID_ROLE == member_info.role_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("role should be valid", KR(ret), K(member_info));
  } else if (!member_info.check_leader_is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("member_info's self_/leader_/role_ not match", KR(ret), K(new_mode), K(pkey), K(member_info));
  } else if (new_leader.server_.is_valid()) {
    if (member_info.leader_.is_valid() && member_info.leader_ != new_leader.server_) {
      // If we can different leader by RPC, it means we need renew location by SQL.
      is_new_location_valid = false;
      ObTaskController::get().allow_next_syslog();
      LOG_WARN("has multi leaders, rpc renew failed", K(pkey), K(new_leader), K(member_info));
    } else {
      /*
       * new_leader.server_.is_valid() means we got leader once
       * and leader's member_list is same with old location cache.
       * So, we do nothing in this branch.
       */
    }
  } else if (OB_FAIL(check_member_list(location_array, member_info.member_list_, is_new_location_valid))) {
    LOG_WARN("fail to check member list", KR(ret), K(pkey), K(location_array), K(member_info));
  } else if (!is_new_location_valid) {
    ObTaskController::get().allow_next_syslog();
    LOG_WARN("obs 'member_list is not same with cached location, rpc renew failed",
        K(pkey),
        K(member_info),
        K(location_array));
  } else if (addr == member_info.leader_) {
    new_leader.role_ = new_mode ? member_info.role_ : LEADER;
    new_leader.server_ = addr;
  }
  return ret;
}

// add non paxos member got by RPC to array
int ObLocationFetcher::add_non_paxos_replica(
    const ObMemberListAndLeaderArg& member_info, ObIArray<ObReplicaMember>& non_paxos_replicas)
{
  int ret = OB_SUCCESS;
  bool is_local = false;
  const ObIArray<ObReplicaMember>& lower_list = member_info.lower_list_;
  const common::ObIArray<common::ObAddr>& member_list = member_info.member_list_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", KR(ret));
  }
  for (int64_t i = 0; i < lower_list.count() && OB_SUCC(ret); i++) {
    ObReplicaMember replica = lower_list.at(i);
    bool finded = false;
    for (int64_t j = 0; j < member_list.count() && !finded; j++) {
      if (replica.get_server() == member_list.at(j)) {
        finded = true;
      }
    }
    if (finded) {
      // {non paxos members} = {lower_list} - {member_list}
    } else if (OB_ISNULL(locality_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("locality manager ptr is null", KR(ret));
    } else if (OB_FAIL(locality_manager_->is_local_server(replica.get_server(), is_local))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("check if server in local region fail", K(ret), K(replica));
      } else {
        LOG_INFO("fail to find serer", "server", replica.get_server());
        // nothing todo
      }
    } else if (!is_local) {
      // filter non paxos member in other region
      LOG_DEBUG("replica is not local, ignore", K(replica));
    } else if (has_exist_in_array(non_paxos_replicas, replica)) {
      LOG_WARN("invalid lower list, should not same with other", K(non_paxos_replicas), K(lower_list));
    } else if (OB_FAIL(non_paxos_replicas.push_back(replica))) {
      LOG_WARN("fail to push back", K(ret), K(non_paxos_replicas));
    }
  }
  return ret;
}

// check if non paxos members are not changed
int ObLocationFetcher::check_non_paxos_replica(const common::ObIArray<ObReplicaLocation>& cached_member_list,
    const common::ObIArray<ObReplicaMember>& non_paxos_replicas, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  int64_t non_paxos_relica_num = 0;
  bool is_local = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", KR(ret));
  }
  // only check non paxos member from old location cache in local region
  for (int64_t i = 0; OB_SUCC(ret) && i < cached_member_list.count() && is_same; i++) {
    const ObReplicaLocation& location = cached_member_list.at(i);
    if (ObReplicaTypeCheck::is_paxos_replica_V2(location.replica_type_)) {
      // nothing todo
    } else if (OB_ISNULL(locality_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("locality manager ptr is null", KR(ret));
    } else if (OB_FAIL(locality_manager_->is_local_server(location.server_, is_local))) {
      LOG_DEBUG("fail to check is local server", K(ret), K(location));
      ret = OB_SUCCESS;
      is_same = false;
    } else if (!is_local) {
      // nothing todo
    } else {
      non_paxos_relica_num++;
      bool found = false;
      for (int64_t j = 0; OB_SUCC(ret) && is_same && j < non_paxos_replicas.count(); j++) {
        if (non_paxos_replicas.at(j).get_server() == location.server_) {
          found = true;
        }
      }  // end for
      if (!found) {
        LOG_DEBUG("fail to find server, not same", K(location.server_), K(non_paxos_replicas));
        is_same = false;
      }
    }
  }  // end for
  if (OB_SUCC(ret) && is_same) {
    if (non_paxos_relica_num != non_paxos_replicas.count()) {
      LOG_DEBUG("non paxos replica num is not equal ; not same", K(non_paxos_relica_num), K(non_paxos_replicas));
      is_same = false;
    }
  }
  return ret;
}

int ObLocationFetcher::check_member_list(const common::ObIArray<ObReplicaLocation>& cached_member_list,
    const common::ObIArray<common::ObAddr>& member_list, bool& is_same)
{
  int ret = OB_SUCCESS;
  bool found = false;

  is_same = true;

  int64_t member_cnt = 0;
  for (int64_t i = 0; is_same && i < cached_member_list.count(); ++i) {
    found = false;
    if (!ObReplicaTypeCheck::is_paxos_replica_V2(cached_member_list.at(i).replica_type_)) {
      // nothing todo
    } else {
      member_cnt++;
      for (int64_t j = 0; !found && j < member_list.count(); ++j) {
        if (member_list.at(j) == cached_member_list.at(i).server_) {
          found = true;
        }
      }
      if (!found) {
        is_same = false;
      }
    }
  }
  if (OB_SUCC(ret) && is_same) {
    if (member_cnt != member_list.count()) {
      is_same = false;
    }
  }
  return ret;
}

int ObLocationFetcher::batch_renew_location_with_rpc(const common::ObIArray<const ObPartitionLocation*>& rpc_locations,
    common::ObIAllocator& allocator, common::ObIArray<ObPartitionLocation*>& new_locations)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator arg_allocator("RenewLocation");
  common::ObArray<ObLocationRpcRenewInfo> infos;
  common::ObArray<int> key_results;  // key_results corresponds to rpc_locations
  common::hash::ObHashMap<ObReplicaRenewKey, const ObMemberListAndLeaderArg*> result_map;
  int64_t now = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (rpc_locations.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(init_batch_rpc_renew_struct(rpc_locations, arg_allocator, infos, key_results, result_map))) {
    LOG_WARN("fail to init struct", K(ret));
  } else if (OB_ISNULL(srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy is null", K(ret));
  } else {
    // We should renew location by SQL in the following cases:
    // case 1. Call RPC on observer failed.
    // case 2. Partial partitions' in one RPC return failure.
    // case 3. Partial partitions' new locations not matched.(member_list or non paxos member maybe changed)
    rootserver::ObBatchRpcRenewLocProxy proxy_batch(
        *srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::batch_get_member_list_and_leader);
    // gather rpc results
    const int64_t timeout_ts = OB_FETCH_MEMBER_LIST_AND_LEADER_TIMEOUT;
    for (int64_t i = 0; i < infos.count(); i++) {  // ignore failure
      ObLocationRpcRenewInfo& info = infos.at(i);
      if (!info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid info", K(ret), K(info));
      } else {
        ObAddr& addr = info.addr_;
        ObLocationRpcRenewArg& arg = *(info.arg_);
        const uint64_t tenant_id = get_rpc_tenant_id(arg.keys_.at(0).get_table_id());
        if (OB_FAIL(proxy_batch.call(addr, timeout_ts, tenant_id, arg))) {
          LOG_WARN("fail to call async batch rpc", K(ret), K(addr), K(arg), K(tenant_id));
        }
      }
      ret = OB_SUCCESS;
    }
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;  // always wait all
    if (OB_SUCCESS != (tmp_ret = proxy_batch.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", K(tmp_ret), K(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_FAIL(ret)) {
    } else if (return_code_array.count() != infos.count() ||
               return_code_array.count() != proxy_batch.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cnt not match",
          K(ret),
          "return_cnt",
          return_code_array.count(),
          "result_cnt",
          proxy_batch.get_results().count(),
          "arg_cnt",
          infos.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
      int res_ret = return_code_array.at(i);
      ObAddr& addr = infos.at(i).addr_;
      ObLocationRpcRenewArg*& arg = infos.at(i).arg_;
      ObArray<int64_t>*& idx_array = infos.at(i).idx_array_;
      if (!infos.at(i).is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("renew info is invalid", K(ret), "info", infos.at(i));
      } else if (OB_SUCCESS != res_ret) {  // case 1
        for (int j = 0; OB_SUCC(ret) && j < idx_array->count(); j++) {
          ObPartitionKey& pkey = arg->keys_.at(j);
          int64_t idx = idx_array->at(j);
          int& result = key_results.at(idx);
          if (OB_SUCCESS == result) {
            result = res_ret;
            LOG_INFO("rcode is not success, rpc renew failed", K(result), K(addr), K(pkey));
          }
        }
      } else {
        const ObLocationRpcRenewResult* result = proxy_batch.get_results().at(i);
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret), KP(result));
        } else if (result->results_.count() != arg->keys_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cnt not match", K(ret), "result_cnt", result->results_.count(), "arg_cnt", arg->keys_.count());
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < result->results_.count(); j++) {
          const ObMemberListAndLeaderArg& res = result->results_.at(j);
          ObPartitionKey& pkey = arg->keys_.at(j);
          if (!res.is_valid()) {  // case 2
            int64_t idx = idx_array->at(j);
            int& result = key_results.at(idx);
            if (OB_SUCCESS == result) {
              result = OB_RENEW_LOCATION_BY_RPC_FAILED;
              LOG_INFO("result is not success, rpc renew failed", K(result), K(addr), K(pkey));
            }
          } else {
            ObReplicaRenewKey key(pkey.get_table_id(), pkey.get_partition_id(), addr);
            if (OB_FAIL(result_map.set_refactored(key, &res))) {
              LOG_WARN("fail to set value", K(ret), K(key));
            }
          }
        }
      }
    }
    // renew location
    for (int64_t i = 0; i < key_results.count(); i++) {  // ignore failure
      bool is_new_location_valid = false;
      if (OB_SUCCESS != key_results.at(i)) {
        ret = key_results.at(i);
      } else if (OB_ISNULL(rpc_locations.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("location is null", K(ret), K(i));
      } else {
        const ObPartitionLocation* const& location = rpc_locations.at(i);
        ObPartitionLocation* new_location = NULL;
        if (OB_FAIL(ObPartitionLocation::alloc_new_location(allocator, new_location))) {
          LOG_WARN("fail to alloc new location", K(ret), KPC(location));
        } else if (OB_FAIL(renew_location_with_rpc_v2(&result_map, *location, *new_location, is_new_location_valid))) {
          LOG_WARN(
              "failed to renew location with rpc", K(ret), KPC(location), "cost", ObTimeUtility::current_time() - now);
        } else if (!is_new_location_valid) {  // case 3
          // skip
        } else if (OB_FAIL(new_locations.push_back(new_location))) {
          LOG_WARN("fail to push back location", K(ret), KPC(new_location));
        } else if (EXECUTE_COUNT_PER_SEC(16)) {
          LOG_INFO("fetch location with rpc success",
              KPC(location),
              KPC(new_location),
              "cost",
              ObTimeUtility::current_time() - now);
        }
      }
      if (OB_FAIL(ret) || !is_new_location_valid) {
        EVENT_INC(LOCATION_CACHE_RPC_RENEW_FAIL);
        LOG_WARN("rpc renew failed, need sql renew",
            K(ret),
            "old_location",
            *(rpc_locations.at(i)),
            "cost",
            ObTimeUtility::current_time() - now);
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/*
 * rpc_locations : Partitions that we should try to renew their locations by RPC.
 * infos : organized by server, record pkey and its corresponding index of rpc_locations.
 * key_results : rpc renew results corresponding to rpc_locations.
 * result_map : HashMap. Key is tuple (table_id, partition_id, addr). Value is rpc result of partition on single server.
 */
int ObLocationFetcher::init_batch_rpc_renew_struct(const common::ObIArray<const ObPartitionLocation*>& rpc_locations,
    common::ObIAllocator& allocator, common::ObArray<ObLocationRpcRenewInfo>& infos, common::ObArray<int>& key_results,
    common::hash::ObHashMap<ObReplicaRenewKey, const ObMemberListAndLeaderArg*>& result_map)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", KR(ret));
  } else if (rpc_locations.count() <= 0) {
    // do nothing
  } else {
    ObLocationRpcRenewInfo info;
    common::hash::ObHashMap<ObAddr, int64_t> dest_map;  // record index of infos
    int64_t bucket_num = UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM * OB_MAX_MEMBER_NUMBER;
    if (OB_FAIL(dest_map.create(bucket_num, "LocRpcFetcher", "LocRpcFetcher"))) {
      LOG_WARN("fail to init dest_map", K(ret));
    } else if (OB_FAIL(result_map.create(bucket_num, "LocRpcResMap", "LocRpcResMap"))) {
      LOG_WARN("fail to init result_map ", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rpc_locations.count(); i++) {
      info.reset();
      const ObPartitionLocation* const& location = rpc_locations.at(i);
      bool renew_by_sql = true;
      if (OB_ISNULL(location)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("location is null", K(ret));
      } else {
        const uint64_t table_id = location->get_table_id();
        const int64_t partition_id = location->get_partition_id();
        ObPartitionKey key(location->get_table_id(), location->get_partition_id(), 0);
        const ObIArray<ObReplicaLocation>& location_array = location->get_replica_locations();
        for (int64_t j = 0; OB_SUCC(ret) && j < location_array.count(); j++) {
          const ObAddr& addr = location_array.at(j).server_;
          int64_t idx = OB_INVALID_INDEX;
          bool is_local = false;
          if (OB_ISNULL(locality_manager_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("locality manager ptr is null", KR(ret));
          } else if (OB_FAIL(locality_manager_->is_local_server(addr, is_local))) {
            LOG_WARN("fail to check is local server", K(ret), K(addr));
          } else if (!is_local || !ObReplicaTypeCheck::is_paxos_replica_V2(location_array.at(j).replica_type_)) {
            // nothing todo
          } else if (OB_FAIL(dest_map.get_refactored(addr, idx))) {
            if (OB_HASH_NOT_EXIST != ret) {
              LOG_WARN("fail to get value", K(ret), K(addr));
            } else {
              // init ObLocationRpcRenewInfo
              void* buf = NULL;
              if (OB_ISNULL(buf = allocator.alloc(sizeof(ObLocationRpcRenewArg)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to alloc mem", K(ret));
              } else if (FALSE_IT(info.arg_ = new (buf) ObLocationRpcRenewArg(allocator))) {
              } else if (OB_FAIL(info.arg_->keys_.push_back(key))) {
                LOG_WARN("fail to push back key", K(ret), K(key));
              } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObArray<int64_t>)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to alloc mem", K(ret));
              } else if (FALSE_IT(info.idx_array_ = new (buf)
                                      ObArray<int64_t>(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)))) {
              } else if (OB_FAIL(info.idx_array_->push_back(i))) {
                LOG_WARN("fail to push back idx", K(ret), K(key), K(i));
              } else {
                info.addr_ = addr;
                if (OB_FAIL(infos.push_back(info))) {
                  LOG_WARN("fail to push back idx", K(ret), K(key), K(i));
                } else if (OB_FAIL(dest_map.set_refactored(addr, infos.count() - 1))) {
                  LOG_WARN("fail to set value", K(ret), K(addr));
                } else {
                  renew_by_sql = false;
                }
              }
            }
          } else if (idx >= infos.count() || idx < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid idx", K(ret), K(idx), "cnt", infos.count());
          } else {
            ObLocationRpcRenewInfo& tmp_info = infos.at(idx);
            if (!tmp_info.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid tmp_info", K(ret), K(tmp_info));
            } else if (OB_FAIL(tmp_info.arg_->keys_.push_back(key))) {
              LOG_WARN("fail to push back key", K(ret), K(key));
            } else if (OB_FAIL(tmp_info.idx_array_->push_back(i))) {
              LOG_WARN("fail to push back idx", K(ret), K(key), K(i));
            } else {
              renew_by_sql = false;
            }
          }
        }
        if (OB_SUCC(ret)) {
          // need renew by sql, marked failed
          int tmp_ret = renew_by_sql ? OB_SKIP_RENEW_LOCATION_BY_RPC : OB_SUCCESS;
          if (OB_FAIL(key_results.push_back(tmp_ret))) {  // init
            LOG_WARN("fail to init key results", K(ret), K(i));
          } else if (renew_by_sql) {
            LOG_DEBUG("skip renew location by rpc", K(tmp_ret), K(table_id), K(partition_id));
          }
        }
      }
    }
    if (OB_SUCC(ret) && key_results.count() != rpc_locations.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cnt not match", K(ret), "key_cnt", key_results.count(), "location_cnt", rpc_locations.count());
    }
  }
  return ret;
}

ObPartitionLocationCache::LocationSem::LocationSem() : cur_count_(0), max_count_(0), cond_()
{
  cond_.init(ObWaitEventIds::LOCATION_CACHE_COND_WAIT);
}

ObPartitionLocationCache::LocationSem::~LocationSem()
{}

void ObPartitionLocationCache::LocationSem::set_max_count(const int64_t max_count)
{
  cond_.lock();
  max_count_ = max_count;
  cond_.unlock();
  LOG_INFO("location cache fetch location concurrent max count changed", K(max_count));
}

int ObPartitionLocationCache::LocationSem::acquire(const int64_t abs_timeout_us)
{
  // when we change max_count to small value, cur_count > max_count is possible
  int ret = OB_SUCCESS;
  const int64_t default_wait_time_ms = 1000;
  int64_t wait_time_ms = default_wait_time_ms;
  bool has_wait = false;
  cond_.lock();
  if (max_count_ <= 0 || cur_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid max_count", K(ret), K_(max_count), K_(cur_count));
  } else {
    while (OB_SUCC(ret) && cur_count_ >= max_count_) {
      if (abs_timeout_us > 0) {
        wait_time_ms = (abs_timeout_us - ObTimeUtility::current_time()) / 1000 + 1;  // 1ms at least
        if (wait_time_ms <= 0) {
          ret = OB_TIMEOUT;
        }
      } else {
        wait_time_ms = default_wait_time_ms;
      }

      if (OB_SUCC(ret)) {
        if (wait_time_ms > INT32_MAX) {
          wait_time_ms = INT32_MAX;
          const bool force_print = true;
          BACKTRACE(ERROR,
              force_print,
              "Invalid wait time, use INT32_MAX. wait_time_ms=%ld abs_timeout_us=%ld",
              wait_time_ms,
              abs_timeout_us);
        }
        has_wait = true;
        cond_.wait(static_cast<int32_t>(wait_time_ms));
      }
    }

    if (has_wait) {
      EVENT_INC(LOCATION_CACHE_WAIT);
    }

    if (OB_SUCC(ret)) {
      ++cur_count_;
    }
  }
  cond_.unlock();
  return ret;
}

int ObPartitionLocationCache::LocationSem::release()
{
  int ret = OB_SUCCESS;
  cond_.lock();
  if (cur_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cur_count", K(ret), K_(cur_count));
  } else {
    --cur_count_;
  }
  cond_.signal();
  cond_.unlock();
  return ret;
}

bool ObPartitionLocationCache::RenewLimiter::try_update_last_renew_timestamp(
    const ObPartitionKey& key, int64_t renew_timestamp)
{
  bool update_succ = false;
  int64_t idx = key.hash() % PARTITION_HASH_BUCKET_COUNT;
  int64_t old_last_renew_timestamp = last_renew_timestamps_[idx];
  if (old_last_renew_timestamp ==
      ATOMIC_VCAS(&(last_renew_timestamps_[idx]), old_last_renew_timestamp, renew_timestamp)) {
    update_succ = true;
  }
  return update_succ;
}

static const char* location_queue_type_str_array[ObLocationCacheQueueSet::LOC_QUEUE_MAX + 1] = {
    "SYS_CORE", "SYS_RESTART_RELATED", "SYS", "USER_HA", "USER_TENANT_SPACE", "USER", "MAX"};

const char* ObLocationCacheQueueSet::get_str_by_queue_type(Type type)
{
  const char* str = NULL;
  if (ObLocationCacheQueueSet::LOC_QUEUE_SYS_CORE <= type && type < ObLocationCacheQueueSet::LOC_QUEUE_MAX) {
    str = location_queue_type_str_array[type];
  } else {
    str = location_queue_type_str_array[ObLocationCacheQueueSet::LOC_QUEUE_MAX];
  }
  return str;
}

ObLocationCacheQueueSet::ObLocationCacheQueueSet()
    : is_inited_(false),
      all_root_update_queue_(),
      rs_restart_queue_(),
      sys_update_queue_(),
      user_ha_update_queue_(),
      tenant_space_update_queue_(),
      user_update_queue_tg_id_(-1)
{}

ObLocationCacheQueueSet::~ObLocationCacheQueueSet()
{
  all_root_update_queue_.destroy();
  rs_restart_queue_.destroy();
  sys_update_queue_.destroy();
  user_ha_update_queue_.destroy();
  tenant_space_update_queue_.destroy();
  TG_DESTROY(user_update_queue_tg_id_);
}

int ObLocationCacheQueueSet::init(ObServerConfig& config)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("queue set has already inited", K(ret));
  } else if (OB_FAIL(all_root_update_queue_.init(
                 ALL_ROOT_THREAD_CNT, "RootLocUpdTask", PLC_TASK_QUEUE_SIZE, PLC_TASK_MAP_SIZE))) {
    LOG_WARN("all_root_update_queue init failed", K(ret));
  } else if (OB_FAIL(rs_restart_queue_.init(SYS_THREAD_CNT, "RsLocUpdTask", PLC_TASK_QUEUE_SIZE, PLC_TASK_MAP_SIZE))) {
    LOG_WARN("rs_restart_queue init failed", K(ret));
  } else if (OB_FAIL(sys_update_queue_.init(SYS_THREAD_CNT, "SysLocUpdTask", PLC_TASK_QUEUE_SIZE, PLC_TASK_MAP_SIZE))) {
    LOG_WARN("sys_update_queue init failed", K(ret));
  } else if (OB_FAIL(user_ha_update_queue_.init(
                 SYS_THREAD_CNT, "UsrHaLocUpdTask", PLC_TASK_QUEUE_SIZE, PLC_TASK_MAP_SIZE))) {
    LOG_WARN("user_ha_update_queue init failed", K(ret));
  } else if (OB_FAIL(tenant_space_update_queue_.init(static_cast<int32_t>(config.location_refresh_thread_count),
                 "TntLocUpdTask",
                 PLC_TASK_QUEUE_SIZE,
                 PLC_TASK_MAP_SIZE))) {
    LOG_WARN("tenant_space_update_queue init failed", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::UsrLocUpdTask, user_update_queue_tg_id_))) {
    LOG_WARN("create tg failed", K(ret));
  } else if (OB_FAIL(TG_START(user_update_queue_tg_id_))) {
    LOG_WARN("start failed", K(ret));
  } else {
    all_root_update_queue_.set_label(ObModIds::OB_LOCATION_CACHE_QUEUE);
    rs_restart_queue_.set_label(ObModIds::OB_LOCATION_CACHE_QUEUE);
    sys_update_queue_.set_label(ObModIds::OB_LOCATION_CACHE_QUEUE);
    user_ha_update_queue_.set_label(ObModIds::OB_LOCATION_CACHE_QUEUE);
    tenant_space_update_queue_.set_label(ObModIds::OB_LOCATION_CACHE_QUEUE);
    is_inited_ = true;
  }
  return ret;
}

ObLocationCacheQueueSet::Type ObLocationCacheQueueSet::get_queue_type(
    const uint64_t table_id, const int64_t partition_id)
{
  Type type = LOC_QUEUE_MAX;
  if (table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) ||
      table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID) ||
      table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_GTS_TID) ||
      table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_GTS_TID)) {
    type = LOC_QUEUE_SYS_CORE;
  } else if (is_sys_table(table_id)) {
    // sys table
    if (extract_pure_id(table_id) == OB_ALL_TENANT_META_TABLE_TID || extract_pure_id(table_id) == OB_ALL_DUMMY_TID) {
      // high priority in tenant space
      type = LOC_QUEUE_USER_HA;
    } else if (OB_SYS_TENANT_ID == extract_tenant_id(table_id)) {
      // in sys tenant
      if (ObSysTableChecker::is_rs_restart_related_partition(table_id, partition_id)) {
        type = LOC_QUEUE_SYS_RESTART_RELATED;
      } else {
        type = LOC_QUEUE_SYS;
      }
    } else {
      type = LOC_QUEUE_USER_TENANT_SPACE;
    }
  } else {
    type = LOC_QUEUE_USER;
  }
  return type;
}

int ObLocationCacheQueueSet::add_task(const ObLocationUpdateTask& task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), K(task));
  } else {
    Type type = get_queue_type(task.get_table_id(), task.get_partition_id());
    switch (type) {
      case LOC_QUEUE_SYS_CORE: {
        if (OB_FAIL(all_root_update_queue_.add_task(task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("all_root_update_queue add_task failed", K(task), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        break;
      }
      case LOC_QUEUE_SYS_RESTART_RELATED: {
        if (OB_FAIL(rs_restart_queue_.add_task(task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("rs_restart_queue add_task failed", K(task), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        break;
      }
      case LOC_QUEUE_SYS: {
        if (OB_FAIL(sys_update_queue_.add_task(task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("sys_update_queue add_task failed", K(task), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        break;
      }
      case LOC_QUEUE_USER_HA: {
        if (OB_FAIL(user_ha_update_queue_.add_task(task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("user_ha_update_queue add_task failed", K(task), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        break;
      }
      case LOC_QUEUE_USER_TENANT_SPACE: {
        if (OB_FAIL(tenant_space_update_queue_.add_task(task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("tenant_space_update_queue add_task failed", K(task), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        break;
      }
      case LOC_QUEUE_USER: {
        if (OB_FAIL(TG_PUSH_TASK(user_update_queue_tg_id_, task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("user_update_queue_ add_task failed", K(task), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid queue type", KR(ret), K(task), K(type));
      }
    }
  }
  return ret;
}

ObLocationAsyncUpdateTask::ObLocationAsyncUpdateTask()
    : loc_cache_(NULL),
      table_id_(INT64_MAX),
      partition_id_(OB_INVALID_ID),
      add_timestamp_(OB_INVALID_VERSION),
      cluster_id_(OB_INVALID_ID),
      type_(MODE_AUTO)
{}

ObLocationAsyncUpdateTask::ObLocationAsyncUpdateTask(ObIPartitionLocationCache& loc_cache, const uint64_t table_id,
    const int64_t partition_id, const int64_t add_timestamp, const int64_t cluster_id, const Type type)
    : loc_cache_(&loc_cache),
      table_id_(table_id),
      partition_id_(partition_id),
      add_timestamp_(add_timestamp),
      cluster_id_(cluster_id),
      type_(type)
{}

bool ObLocationAsyncUpdateTask::need_process_alone() const
{
  bool bret = false;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2260 || OB_ALL_CORE_TABLE_TID == extract_pure_id(table_id_) ||
      OB_ALL_ROOT_TABLE_TID == extract_pure_id(table_id_) || is_virtual_table(table_id_)) {
    bret = true;
  }
  return bret;
}

bool ObLocationAsyncUpdateTask::is_valid() const
{
  return OB_NOT_NULL(loc_cache_) && OB_INVALID_ID != table_id_ && OB_INVALID_ID != partition_id_ &&
         OB_INVALID_VERSION != add_timestamp_ && OB_INVALID_ID != cluster_id_;
}

void ObLocationAsyncUpdateTask::check_task_status() const
{
  const int64_t now = ObTimeUtility::current_time();
  const int64_t wait_cost = now - add_timestamp_;
  if (wait_cost >= WAIT_PROCESS_WARN_TIME) {
    LOG_WARN("location update task waits to be processed too long", "task", *this, K(now), K(wait_cost));
  }
}

int64_t ObLocationAsyncUpdateTask::hash() const
{
  int64_t hash_val = 0;
  if (!is_valid()) {
    LOG_WARN("invalid argument", "self", *this);
  } else {
    hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
    hash_val = murmurhash(&partition_id_, sizeof(partition_id_), hash_val);
    hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), hash_val);
    hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  }
  return hash_val;
}

bool ObLocationAsyncUpdateTask::operator==(const ObLocationAsyncUpdateTask& other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN("invalid argument", "self", *this, K(other));
  } else if (&other == this) {
    equal = true;
  } else {
    equal = (table_id_ == other.table_id_ && partition_id_ == other.partition_id_ && cluster_id_ == other.cluster_id_ &&
             type_ == other.type_);
  }
  return equal;
}

bool ObLocationAsyncUpdateTask::operator<(const ObLocationAsyncUpdateTask& other) const
{
  return table_id_ < other.table_id_ || partition_id_ < other.partition_id_ || cluster_id_ < other.cluster_id_ ||
         type_ < other.type_;
  ;
}

bool ObLocationAsyncUpdateTask::compare_without_version(const ObLocationAsyncUpdateTask& other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_ERROR("invalid argument", "self", *this, K(other));
  } else if (&other == this) {
    equal = true;
  } else {
    equal = (*this == other);
  }
  return equal;
}

uint64_t ObLocationAsyncUpdateTask::get_group_id() const
{
  return combine_id(extract_tenant_id(table_id_), type_);
}

bool ObLocationAsyncUpdateTask::is_barrier() const
{
  return false;
}

// discard remote location cache refresh task while switchover/failover
bool ObLocationAsyncUpdateTask::need_discard() const
{
  bool bret = false;
  if (GCTX.is_in_flashback_state() || GCTX.is_in_cleanup_state()) {
    if (cluster_id_ != GCONF.cluster_id) {
      bret = true;
    } else {
      // nothing todo
    }
  }
  return bret;
}

ObLocationAsyncUpdateQueueSet::ObLocationAsyncUpdateQueueSet(ObIPartitionLocationCache* loc_cache)
    : is_inited_(false),
      loc_cache_(loc_cache),
      all_root_update_queue_(),
      rs_restart_queue_(),
      sys_update_queue_(),
      user_ha_update_queue_(),
      tenant_space_update_queue_(),
      user_update_queue_()
{}

ObLocationAsyncUpdateQueueSet::~ObLocationAsyncUpdateQueueSet()
{}

void ObLocationAsyncUpdateQueueSet::stop()
{
  all_root_update_queue_.stop();
  rs_restart_queue_.stop();
  sys_update_queue_.stop();
  user_ha_update_queue_.stop();
  tenant_space_update_queue_.stop();
  user_update_queue_.stop();
}

void ObLocationAsyncUpdateQueueSet::wait()
{
  all_root_update_queue_.wait();
  rs_restart_queue_.wait();
  sys_update_queue_.wait();
  user_ha_update_queue_.wait();
  tenant_space_update_queue_.wait();
  user_update_queue_.wait();
}

int ObLocationAsyncUpdateQueueSet::init(ObServerConfig& config)
{
  int ret = OB_SUCCESS;
  int64_t user_thread_cnt =
      !lib::is_mini_mode() ? static_cast<int32_t>(config.location_refresh_thread_count) : MINI_MODE_UPDATE_THREAD_CNT;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("queue set has already inited", K(ret));
  } else if (OB_FAIL(all_root_update_queue_.init(
                 loc_cache_, MINI_MODE_UPDATE_THREAD_CNT, PLC_TASK_QUEUE_SIZE, "RootLocAsyncUp"))) {
    LOG_WARN("all_root_update_queue init failed", K(ret));
  } else if (OB_FAIL(rs_restart_queue_.init(
                 loc_cache_, MINI_MODE_UPDATE_THREAD_CNT, PLC_TASK_QUEUE_SIZE, "RsLocAsyncUp"))) {
    LOG_WARN("rs_restart_queue init failed", K(ret));
  } else if (OB_FAIL(sys_update_queue_.init(
                 loc_cache_, MINI_MODE_UPDATE_THREAD_CNT, PLC_TASK_QUEUE_SIZE, "SysLocAsyncUp"))) {
    LOG_WARN("sys_update_queue init failed", K(ret));
  } else if (OB_FAIL(user_ha_update_queue_.init(
                 loc_cache_, MINI_MODE_UPDATE_THREAD_CNT, PLC_TASK_QUEUE_SIZE, "UsrHaLocAsyncUp"))) {
    LOG_WARN("user_ha_update_queue init failed", K(ret));
  } else if (OB_FAIL(
                 tenant_space_update_queue_.init(loc_cache_, user_thread_cnt, PLC_TASK_QUEUE_SIZE, "TntLocAsyncUp"))) {
    LOG_WARN("tenant_space_update_queue init failed", K(ret));
  } else if (OB_FAIL(user_update_queue_.init(loc_cache_,
                 user_thread_cnt,
                 !lib::is_mini_mode() ? USER_TASK_QUEUE_SIZE : MINI_MODE_USER_TASK_QUEUE_SIZE,
                 "UserLocAsyncUp"))) {
    LOG_WARN("user_update_queue init failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLocationAsyncUpdateQueueSet::add_task(const ObLocationAsyncUpdateTask& task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), K(task));
  } else {
    const uint64_t table_id = task.get_table_id();
    const int64_t partition_id = task.get_partition_id();
    if (table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) ||
        table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID) ||
        table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_GTS_TID) ||
        table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_GTS_TID)) {
      // high priority
      if (OB_FAIL(all_root_update_queue_.add(task))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("all_root_update_queue add_task failed", K(task), K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    } else if (is_sys_table(table_id)) {
      // sys table
      if (extract_pure_id(table_id) == OB_ALL_TENANT_META_TABLE_TID || extract_pure_id(table_id) == OB_ALL_DUMMY_TID) {
        // high priority in tenant space
        if (OB_FAIL(user_ha_update_queue_.add(task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("user_ha_update_queue add_task failed", K(task), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
      } else if (OB_SYS_TENANT_ID == task.get_tenant_id()) {
        // in sys tenant
        if (ObSysTableChecker::is_rs_restart_related_partition(table_id, partition_id)) {
          if (OB_FAIL(rs_restart_queue_.add(task))) {
            if (OB_EAGAIN != ret) {
              LOG_WARN("rs_restart_queue add_task failed", K(task), K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        } else {
          if (OB_FAIL(sys_update_queue_.add(task))) {
            if (OB_EAGAIN != ret) {
              LOG_WARN("sys_update_queue add_task failed", K(task), K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
      } else {
        if (OB_FAIL(tenant_space_update_queue_.add(task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("tenant_space_update_queue add_task failed", K(task), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
    } else {
      if (OB_FAIL(user_update_queue_.add(task))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("user_update_queue add_task failed", K(task), K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

ObPartitionLocationCache::ObPartitionLocationCache(ObILocationFetcher& location_fetcher)
    : is_inited_(false),
      is_stopped_(false),
      location_fetcher_(location_fetcher),
      schema_service_(NULL),
      config_(NULL),
      sys_cache_(),
      sys_leader_cache_(),
      user_cache_(),
      server_tracer_(NULL),
      sem_(),
      renew_limiter_(),
      locality_manager_(NULL),
      cluster_id_(-1),
      remote_server_provider_(NULL),
      lock_(),
      sql_renew_map_(),
      sql_renew_pool_(),
      leader_cache_(),
      local_async_queue_set_(this),
      remote_async_queue_set_(this)
{}

ObPartitionLocationCache::~ObPartitionLocationCache()
{}

int ObPartitionLocationCache::init(ObMultiVersionSchemaService& schema_service, ObServerConfig& config,
    ObIAliveServerTracer& server_tracer, const char* cache_name, const int64_t priority,
    ObILocalityManager* locality_manager, const int64_t cluster_id, ObRemoteServerProvider* remote_server_provider)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition location cache has already inited", K(ret));
  } else if (NULL == cache_name || priority <= 0 || OB_ISNULL(locality_manager) || OB_ISNULL(remote_server_provider)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cache_name), K(priority), K(remote_server_provider));
  } else {
    const int64_t TENANT_COUNT = 128;
    if (OB_FAIL(sys_cache_.create(OB_SYS_LOCATION_CACHE_BUCKET_NUM, ObModIds::OB_RS_SYS_LOCATION_CACHE))) {
      LOG_WARN("create system table location cache failed", K(ret), LITERAL_K(OB_SYS_LOCATION_CACHE_BUCKET_NUM));
    } else if (OB_FAIL(
                   sys_leader_cache_.create(OB_SYS_LOCATION_CACHE_BUCKET_NUM, ObModIds::OB_RS_SYS_LOCATION_CACHE))) {
      LOG_WARN("create system table leader cache failed", K(ret), LITERAL_K(OB_SYS_LOCATION_CACHE_BUCKET_NUM));
    } else if (OB_FAIL(user_cache_.init(cache_name, priority))) {
      LOG_WARN("user table location cache init failed", K(cache_name), K(priority), K(ret));
    } else if (OB_FAIL(sql_renew_map_.create(TENANT_COUNT, ObModIds::OB_RS_SYS_LOCATION_CACHE))) {
      LOG_WARN("fail to init sql renew info", KR(ret));
    } else if (OB_FAIL(local_async_queue_set_.init(config))) {
      LOG_WARN("fail to init local async queue set", K(ret));
    } else if (OB_FAIL(remote_async_queue_set_.init(config))) {
      LOG_WARN("fail to init remote async queue set", K(ret));
    } else {
      sem_.set_max_count(config.location_fetch_concurrency);
      schema_service_ = &schema_service;
      config_ = &config;
      server_tracer_ = &server_tracer;
      locality_manager_ = locality_manager;
      cluster_id_ = cluster_id;
      is_stopped_ = false;
      remote_server_provider_ = remote_server_provider;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObPartitionLocationCache::stop()
{
  local_async_queue_set_.stop();
  remote_async_queue_set_.stop();
}

void ObPartitionLocationCache::wait()
{
  local_async_queue_set_.wait();
  remote_async_queue_set_.wait();
}

int ObPartitionLocationCache::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sys_cache_.destroy())) {
    LOG_WARN("destroy sys cache failed", K(ret));
  } else if (OB_FAIL(sys_leader_cache_.destroy())) {
    LOG_WARN("destroy sys cache failed", K(ret));
  } else if (OB_FAIL(sql_renew_map_.destroy())) {
    LOG_WARN("fail to clear map", KR(ret));
  } else {
    is_stopped_ = true;
    is_inited_ = false;
  }
  return ret;
}

int ObPartitionLocationCache::reload_config()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    sem_.set_max_count(config_->location_fetch_concurrency);
  }
  return ret;
}

bool ObPartitionLocationCache::is_duty_time(const ObPartitionLocation& location)
{
  bool bret = false;
  if (!location.is_valid()) {
    // nothing todo
  } else {
    int64_t curr_time = ObTimeUtility::current_time();
    bret = (curr_time - location.get_sql_renew_time()) >= config_->force_refresh_location_cache_interval;
    LOG_TRACE("is duty time", K(bret), K(location), K(curr_time));
    if (bret) {
      LOG_TRACE("is duty time", K(bret), K(location), K(curr_time), K(curr_time - location.get_sql_renew_time()));
    }
  }
  return bret;
}

bool ObPartitionLocationCache::can_do_force_sql_renew(const uint64_t table_id)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  int64_t tenant_id = extract_tenant_id(table_id);
  ObTenantSqlRenewStat* tenant_info = NULL;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else {
    ObTenantStatGuard guard(tenant_id, sql_renew_map_, sql_renew_pool_, lock_);
    tenant_info = guard.get_tenant_stat();
    if (OB_ISNULL(tenant_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tenant_info", KR(ret));
    } else if (ObTimeUtility::current_time() - tenant_info->start_sql_renew_ts_ > FORCE_SQL_RENEW_WINDOW) {
      bret = true;
      tenant_info->init(tenant_id);
      tenant_info->inc();
    } else if (tenant_info->sql_renew_count_ < config_->force_refresh_location_cache_threshold) {
      bret = true;
      tenant_info->inc();
    }
    if (bret) {
      LOG_TRACE("can do force sql renew", K(*tenant_info), K(bret));
    }
  }
  return bret;
}

// interface for obproxy to renew location cache by SQL directly in some cases.
int ObPartitionLocationCache::force_sql_get(const uint64_t table_id, const int64_t partition_id,
    ObPartitionLocation& location, const int64_t expire_renew_time, bool& is_cache_hit)
{
  int ret = OB_SUCCESS;
  is_cache_hit = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is not inited", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id) || is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table id or partition id is invalid", KT(table_id), K(partition_id), K(ret));
  } else {
    int64_t cluster_id = cluster_id_;
    const bool filter_not_readable_replica = true;
    const bool force_sql_renew = true;
    const bool auto_update = true;
    if (!can_do_force_sql_renew(table_id)) {
      if (OB_FAIL(get(table_id, partition_id, location, expire_renew_time, is_cache_hit, auto_update))) {
        LOG_WARN("fail to get", KR(ret), K(table_id), K(partition_id));
      }
    } else if (OB_FAIL(renew_location(table_id,
                   partition_id,
                   cluster_id,
                   location,
                   expire_renew_time,
                   filter_not_readable_replica,
                   force_sql_renew,
                   auto_update))) {
      LOG_WARN("renew location failed",
          K(ret),
          KT(table_id),
          K(partition_id),
          K(expire_renew_time),
          K(filter_not_readable_replica),
          K(lbt()));
    } else {
#if !defined(NDEBUG)
      LOG_INFO("sync renew location by sql success",
          K(ret),
          KT(table_id),
          K(partition_id),
          K(location),
          K(expire_renew_time),
          K(filter_not_readable_replica),
          K(lbt()));
#endif
    }
  }
  // TODO: should monitor frequency of force_sql_refresh()
#if !defined(NDEBUG)
  if (OB_FAIL(ret)) {
    LOG_WARN("LOCATION: force get partition location", K(table_id), K(partition_id), K(ret));
  } else {
    LOG_INFO("LOCATION: force get partition location", K(table_id), K(partition_id), K(ret));
  }
#endif
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LOCATION_NOT_EXIST;
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_GET_LOCATION_TIME_OUT;
  }
  return ret;
}

int ObPartitionLocationCache::get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
    const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update /*= true*/)
{
  int ret = OB_SUCCESS;
  bool force_renew = true;
  const bool is_nonblock = false;
  is_cache_hit = false;
  const bool filter_not_readable_replica = true;
  int64_t expire_renew_time_new = expire_renew_time;
  int64_t cluster_id = cluster_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is not inited", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id) || is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table id or partition id is invalid", KT(table_id), K(partition_id), K(ret));
  } else {
    location.reset();
    ret = get_from_cache(
        table_id, partition_id, cluster_id, is_nonblock, location, filter_not_readable_replica, auto_update);
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get location from cache failed", KT(table_id), K(partition_id), K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret || location.is_mark_fail() || location.get_renew_time() <= expire_renew_time) {
      force_renew = true;
      is_cache_hit = false;
      ret = OB_SUCCESS;
    } else {
      force_renew = false;
      is_cache_hit = true;
      bool alive = false;
      int64_t trace_time = 0;
      ObReplicaLocation leader;
      if (OB_FAIL(location.get_strong_leader(leader))) {
        if (OB_LOCATION_LEADER_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get location leader failed", K(ret), K(location));
        }
      } else if (!is_reliable(location.get_renew_time()) && share::ObServerBlacklist::get_instance().is_in_blacklist(
                                                                share::ObCascadMember(leader.server_, cluster_id_))) {
        LOG_DEBUG("leader in server blacklist, force renew", K(location));
        force_renew = true;
        expire_renew_time_new = location.get_renew_time();
      } else if (OB_FAIL(server_tracer_->is_alive(leader.server_, alive, trace_time))) {
        LOG_WARN("check server alive failed", K(ret), KT(table_id), K(partition_id), K(leader));
      } else if (!alive) {
        LOG_DEBUG("leader at non-alive server, force renew", K(location), K(trace_time));
        force_renew = true;
        expire_renew_time_new = trace_time;
      }
    }

    // Determine whether to use a synchronous or asynchronous method to renew location by SQL
    // when we force renew location by SQL.
    bool force_sql_renew = false;
    if (OB_SUCC(ret)) {
      force_sql_renew = is_duty_time(location) && can_do_force_sql_renew(table_id);
      const int64_t now = ObTimeUtility::current_time();
      if (!force_sql_renew) {
        // nothing todo
      } else if (!force_renew) {
        // cache is avaliable, renew location by SQL asynchronously.
        ObLocationAsyncUpdateTask task(
            *this, table_id, partition_id, now, cluster_id, ObLocationAsyncUpdateTask::MODE_SQL_ONLY);
        // use temp_ret to avoid overwrite ret
        int temp_ret = add_update_task(task);
        if (OB_SUCCESS != temp_ret) {
          LOG_WARN("add location update task failed", K(location), K(task), K(temp_ret));
        } else {
          LOG_INFO("add_update_task success", K(task));
        }
      } else {
        // cache is avaliable, renew location by SQL synchronously.
      }
    }
    if (OB_SUCC(ret) && force_renew) {
      is_cache_hit = false;
      int64_t cluster_id = cluster_id_;
      if (OB_FAIL(renew_location(table_id,
              partition_id,
              cluster_id,
              location,
              expire_renew_time_new,
              filter_not_readable_replica,
              force_sql_renew,
              auto_update))) {
        LOG_WARN("renew location failed",
            K(ret),
            KT(table_id),
            K(partition_id),
            K(expire_renew_time_new),
            K(filter_not_readable_replica),
            K(lbt()));
      } else {
#if !defined(NDEBUG)
        LOG_INFO("sync renew location success",
            K(ret),
            KT(table_id),
            K(partition_id),
            K(location),
            K(expire_renew_time),
            K(filter_not_readable_replica),
            K(force_renew),
            K(lbt()));
#endif
      }
    }
  }
  if (OB_SUCC(ret) && is_cache_hit) {
    EVENT_INC(LOCATION_CACHE_HIT);
  } else {
    EVENT_INC(LOCATION_CACHE_MISS);
  }
#if !defined(NDEBUG)
  if (OB_FAIL(ret)) {
    LOG_WARN("LOCATION: get partition location",
        K(table_id),
        K(partition_id),
        K(expire_renew_time),
        K(force_renew),
        K(is_cache_hit),
        K(location),
        K(ret));
  } else {
    LOG_INFO("LOCATION: get partition location",
        K(table_id),
        K(partition_id),
        K(expire_renew_time),
        K(force_renew),
        K(is_cache_hit),
        K(location),
        K(ret));
  }
#endif
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LOCATION_NOT_EXIST;
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_GET_LOCATION_TIME_OUT;
  }
  return ret;
}

// FIXME: (): to be removed
int ObPartitionLocationCache::get_across_cluster(const ObPartitionKey& partition, const int64_t expire_renew_time,
    ObPartitionLocation& location, const int64_t specific_cluster_id)
{
  int ret = OB_SUCCESS;
  int64_t table_id = partition.get_table_id();
  int64_t partition_id = partition.get_partition_id();
  location.reset();
  int64_t cluster_id = OB_INVALID_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is not inited", K(ret));
  } else if (is_virtual_table(table_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("virtual table should no use this function", KR(ret), K(partition));
  } else {
    cluster_id = OB_INVALID_ID == specific_cluster_id ? cluster_id_ : specific_cluster_id;
  }
  const bool filter_not_readable_replica = true;
  int64_t expire_renew_time_new = expire_renew_time;
  const bool is_nonblock = false;
  bool force_renew = true;
  bool auto_update = true;
  if (OB_FAIL(ret)) {
  } else {
    ret = get_from_cache(
        table_id, partition_id, cluster_id, is_nonblock, location, filter_not_readable_replica, auto_update);
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get location from cache failed", K(partition), K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret || location.is_mark_fail() || location.get_renew_time() <= expire_renew_time) {
      force_renew = true;
      ret = OB_SUCCESS;
    } else {
      force_renew = false;
    }

    if (OB_SUCC(ret) && force_renew) {
      const bool force_sql_renew = false;
      if (OB_FAIL(renew_location(table_id,
              partition_id,
              cluster_id,
              location,
              expire_renew_time_new,
              filter_not_readable_replica,
              force_sql_renew,
              auto_update))) {
        LOG_WARN("renew location failed",
            K(ret),
            KT(table_id),
            K(partition_id),
            K(expire_renew_time_new),
            K(filter_not_readable_replica),
            K(lbt()));
      } else {
#if !defined(NDEBUG)
        LOG_INFO("sync renew location success",
            K(ret),
            KT(table_id),
            K(partition_id),
            K(location),
            K(expire_renew_time),
            K(filter_not_readable_replica),
            K(lbt()));
#endif
      }
    }
  }
#if !defined(NDEBUG)
  if (OB_FAIL(ret)) {
    LOG_WARN("LOCATION: get partition location across",
        K(table_id),
        K(partition_id),
        K(expire_renew_time),
        K(force_renew),
        K(ret));
  } else {
    LOG_INFO("LOCATION: get partition location across",
        K(table_id),
        K(partition_id),
        K(expire_renew_time),
        K(force_renew),
        K(ret));
  }
#endif

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LOCATION_NOT_EXIST;
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_GET_LOCATION_TIME_OUT;
  }
  return ret;
}

int ObPartitionLocationCache::get(
    const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t expire_renew_time, bool& is_cache_hit)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(get(
                 partition.get_table_id(), partition.get_partition_id(), location, expire_renew_time, is_cache_hit))) {
    LOG_WARN("get partition location failed", K(ret), K(partition), K(expire_renew_time));
  }
  return ret;
}

int ObPartitionLocationCache::get(const uint64_t table_id, ObIArray<ObPartitionLocation>& locations,
    const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update /*=true*/)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is not inited", K(ret));
  } else {
    if (!is_virtual_table(table_id)) {
      const ObTableSchema* table = NULL;
      ObSchemaGetterGuard schema_guard;
      const uint64_t tenant_id = extract_tenant_id(table_id);
      if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table))) {
        LOG_WARN("get table schema failed", K(table_id), K(ret));
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(table_id), K(ret));
      } else {
        locations.reset();
        ObPartitionLocation location;
        bool check_dropped_schema = false;  // For SQL only, we won't get delay-deleted partitions.
        schema::ObTablePartitionKeyIter pkey_iter(*table, check_dropped_schema);
        int64_t partition_id = 0;
        while (OB_SUCC(ret)) {
          location.reset();
          if (OB_FAIL(pkey_iter.next_partition_id_v2(partition_id))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("Failed to get next partition id", K(table_id), K(ret));
            }
          } else if (OB_FAIL(get(table_id, partition_id, location, expire_renew_time, is_cache_hit, auto_update))) {
            LOG_WARN("get location failed", KT(table_id), K(partition_id), K(ret));
          } else if (OB_FAIL(locations.push_back(location))) {
            LOG_WARN("push back location failed", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    } else {
      locations.reset();
      if (OB_FAIL(vtable_get(table_id, locations, expire_renew_time, is_cache_hit))) {
        LOG_WARN("vtable_get failed", KT(table_id), K(expire_renew_time), K(ret));
      }
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LOCATION_NOT_EXIST;
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_GET_LOCATION_TIME_OUT;
  }
  return ret;
}

// FIXME: () For storage only, to be removed
int ObPartitionLocationCache::get_leader_across_cluster(
    const common::ObPartitionKey& partition, common::ObAddr& leader, const int64_t cluster_id, const bool force_renew)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  ObReplicaLocation replica_location;
  int64_t expire_renew_time = force_renew ? INT64_MAX : 0;  // INT64_MAX will force renew location

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(get_across_cluster(partition, expire_renew_time, location, cluster_id))) {
    LOG_WARN("get failed", K(partition), K(force_renew), K(expire_renew_time), K(ret));
  } else if (OB_FAIL(location.get_leader_by_election(replica_location))) {
    LOG_WARN("location.get_leader failed", K(location), K(ret));
  } else {
    leader = replica_location.server_;
  }
  return ret;
}

int ObPartitionLocationCache::get_strong_leader(
    const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  ObReplicaLocation replica_location;
  int64_t expire_renew_time = force_renew ? INT64_MAX : 0;  // INT64_MAX will force renew location
  bool is_cache_hit = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(get(partition, location, expire_renew_time, is_cache_hit))) {
    LOG_WARN("get failed", K(partition), K(force_renew), K(expire_renew_time), K(ret));
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (OB_FAIL(location.get_strong_leader(replica_location))) {
    LOG_WARN("location.get_leader failed", K(location), K(ret));
  } else if (is_reliable(location.get_renew_time())) {
    leader = replica_location.server_;
  } else if (!share::ObServerBlacklist::get_instance().is_in_blacklist(
                 share::ObCascadMember(replica_location.server_, cluster_id_))) {
    leader = replica_location.server_;
  } else {
    ret = OB_LOCATION_LEADER_NOT_EXIST;
    LOG_DEBUG("old partition leader in server blacklist", KR(ret), K(partition), K(replica_location));
  }
  return ret;
}

int ObPartitionLocationCache::get_leader_by_election(
    const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  ObReplicaLocation replica_location;
  int64_t expire_renew_time = force_renew ? INT64_MAX : 0;  // INT64_MAX will force renew location
  bool is_cache_hit = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(get(partition, location, expire_renew_time, is_cache_hit))) {
    LOG_WARN("get failed", K(partition), K(force_renew), K(expire_renew_time), K(ret));
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (OB_FAIL(location.get_leader_by_election(replica_location))) {
    LOG_WARN("location get standby leader failed", K(location), K(ret));
  } else {
    leader = replica_location.server_;
  }
  return ret;
}

int ObPartitionLocationCache::nonblock_get(
    const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  const bool is_nonblock = true;
  const bool filter_not_readable_replica = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is not inited", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id) || is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table id or partition id is invalid", KT(table_id), K(partition_id), K(ret));
  } else {
    location.reset();
    int64_t tmp_cluster_id = (cluster_id == -1) ? cluster_id_ : cluster_id;
    bool auto_update = true;
    ret = get_from_cache(
        table_id, partition_id, tmp_cluster_id, is_nonblock, location, filter_not_readable_replica, auto_update);
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get location from cache failed", KT(table_id), K(partition_id), K(ret));
    }
    if (OB_SUCC(ret)) {
      ObReplicaLocation replica_location;
      if (OB_SUCCESS != location.get_leader_by_election(replica_location)) {
        // ignore error log and ret
      } else if (!is_reliable(location.get_renew_time()) &&
                 share::ObServerBlacklist::get_instance().is_in_blacklist(
                     share::ObCascadMember(replica_location.server_, cluster_id_))) {
        ret = OB_LOCATION_NOT_EXIST;
        LOG_DEBUG(
            "old partition leader in server blacklist", KR(ret), K(table_id), K(partition_id), K(replica_location));
      }
    }
  }

  if (OB_SUCC(ret)) {
    EVENT_INC(LOCATION_CACHE_NONBLOCK_HIT);
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_LOCATION_NOT_EXIST;
    EVENT_INC(LOCATION_CACHE_NONBLOCK_MISS);
  }

  return ret;
}

int ObPartitionLocationCache::nonblock_get(
    const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(nonblock_get(partition.get_table_id(), partition.get_partition_id(), location, cluster_id))) {
    if (OB_LOCATION_NOT_EXIST != ret && OB_LOCATION_LEADER_NOT_EXIST != ret) {
      LOG_WARN("non-block get failed", K(ret), K(partition));
    }
  }
  return ret;
}

int ObPartitionLocationCache::nonblock_get_strong_leader(const ObPartitionKey& partition, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  ObReplicaLocation replica_location;
  int64_t cluster_id = cluster_id_;
  LocationInfo leader_location_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (use_sys_leader_cache(partition.get_table_id(), cluster_id_)) {
    ObLocationCacheKey key(partition.get_table_id(), partition.get_partition_id(), cluster_id_);
    int hash_ret = sys_leader_cache_.get_refactored(key, leader_location_info);
    if (OB_SUCCESS == hash_ret) {
      ret = leader_location_info.is_valid() ? hash_ret : OB_LOCATION_LEADER_NOT_EXIST;
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_LOCATION_LEADER_NOT_EXIST;
    } else {
      ret = hash_ret;
      LOG_WARN("nonblock get leader failed", K(ret), K(key));
    }
  } else {
    bool use_cache = !is_sys_table(partition.get_table_id());
    // try get leader from cache
    if (use_cache) {
      ObLocationCacheKey key(partition.get_table_id(), partition.get_partition_id(), cluster_id_);
      ret = leader_cache_.get_strong_leader_info(key, leader_location_info);
    }
    if (OB_FAIL(ret) || !use_cache) {
      // overwrite ret
      if (OB_FAIL(nonblock_get(partition, location, cluster_id))) {
        LOG_WARN("nonblock get", K(ret), K(partition), K(location), K(cluster_id));
        if (OB_LOCATION_LEADER_NOT_EXIST != ret && OB_LOCATION_NOT_EXIST != ret) {
          LOG_WARN("get failed", K(partition), K(ret));
        }
      } else if (OB_FAIL(location.get_strong_leader(replica_location))) {
        if (OB_LOCATION_LEADER_NOT_EXIST != ret) {
          LOG_WARN("location.get_leader failed", K(location), K(ret));
        }
      } else {
        leader_location_info.server_ = replica_location.server_;
        leader_location_info.renew_ts_ = location.get_renew_time();
        if (use_cache) {
          // try update cache
          int tmp_ret = OB_SUCCESS;
          bool force_update = true;
          ObLocationCacheKey key(partition.get_table_id(), partition.get_partition_id(), cluster_id_);
          if (OB_SUCCESS != (tmp_ret = leader_cache_.set_strong_leader_info(key, leader_location_info, force_update))) {
            LOG_WARN("fail to set leader info", K(tmp_ret), K(key), K(leader_location_info));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (is_reliable(leader_location_info.renew_ts_)) {
      // renew location cache just now
      leader = leader_location_info.server_;
    } else if (!share::ObServerBlacklist::get_instance().is_in_blacklist(
                   share::ObCascadMember(leader_location_info.server_, cluster_id_))) {
      // not in server blacklist
      leader = leader_location_info.server_;
    } else {
      // not a new location info and in server blacklist
      ret = OB_LOCATION_LEADER_NOT_EXIST;
      LOG_DEBUG(
          "old partition leader in server blacklist", KR(ret), K(partition), "replica_location", leader_location_info);
    }
  }
  return ret;
}

int ObPartitionLocationCache::nonblock_get_restore_leader(const ObPartitionKey& partition, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  ObReplicaLocation replica_location;
  int64_t cluster_id = cluster_id_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(nonblock_get(partition, location, cluster_id))) {
    LOG_WARN("nonblock get", K(ret), K(partition), K(location), K(cluster_id));
    if (OB_LOCATION_LEADER_NOT_EXIST != ret && OB_LOCATION_NOT_EXIST != ret) {
      LOG_WARN("get failed", K(partition), K(ret));
    }
  } else if (OB_FAIL(location.get_restore_leader(replica_location))) {
    if (OB_LOCATION_LEADER_NOT_EXIST != ret) {
      LOG_WARN("location.get_leader failed", K(location), K(ret));
    }
  } else {
    leader = replica_location.server_;
  }
  return ret;
}

int ObPartitionLocationCache::nonblock_get_leader_by_election(const ObPartitionKey& partition, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  ObReplicaLocation replica_location;
  int64_t cluster_id = cluster_id_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(nonblock_get(partition, location, cluster_id))) {
    if (OB_LOCATION_LEADER_NOT_EXIST != ret && OB_LOCATION_NOT_EXIST != ret) {
      LOG_WARN("get failed", K(partition), K(ret));
    }
  } else if (OB_FAIL(location.get_leader_by_election(replica_location))) {
    if (OB_LOCATION_LEADER_NOT_EXIST != ret) {
      LOG_WARN("location.get_leader failed", K(location), K(ret));
    }
  } else {
    leader = replica_location.server_;
  }
  return ret;
}

int ObPartitionLocationCache::nonblock_get_strong_leader_without_renew(
    const common::ObPartitionKey& partition, common::ObAddr& leader, const int64_t specific_cluster_id)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  ObReplicaLocation replica_location;
  int64_t cluster_id = OB_INVALID_ID == specific_cluster_id ? cluster_id_ : specific_cluster_id;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(
                 inner_get_from_cache(partition.get_table_id(), partition.get_partition_id(), cluster_id, location))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get failed", K(partition), K(ret));
    }
  } else if (OB_FAIL(location.get_strong_leader(replica_location))) {
    if (OB_LOCATION_LEADER_NOT_EXIST != ret) {
      LOG_WARN("location.get_leader failed", K(location), K(ret));
    }
  } else {
    leader = replica_location.server_;
  }
  return ret;
}

int ObPartitionLocationCache::nonblock_get_leader_by_election_without_renew(
    const common::ObPartitionKey& partition, common::ObAddr& leader, const int64_t specific_cluster_id)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation location;
  ObReplicaLocation replica_location;
  int64_t cluster_id = OB_INVALID_ID == specific_cluster_id ? cluster_id_ : specific_cluster_id;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(
                 inner_get_from_cache(partition.get_table_id(), partition.get_partition_id(), cluster_id, location))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get failed", K(partition), K(ret));
    }
  } else if (OB_FAIL(location.get_leader_by_election(replica_location))) {
    if (OB_LOCATION_LEADER_NOT_EXIST != ret) {
      LOG_WARN("location.get_leader failed", K(location), K(ret));
    }
  } else {
    leader = replica_location.server_;
  }
  return ret;
}

int ObPartitionLocationCache::nonblock_renew(
    const ObPartitionKey& partition, const int64_t expire_renew_time, const int64_t specific_cluster_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition key", K(partition), K(ret));
  } else {
    int64_t cluster_id = OB_INVALID_ID == specific_cluster_id ? cluster_id_ : specific_cluster_id;
    const int64_t now = ObTimeUtility::current_time();
    ObLocationAsyncUpdateTask task(*this,
        partition.get_table_id(),
        partition.get_partition_id(),
        now,
        cluster_id,
        ObLocationAsyncUpdateTask::MODE_AUTO);
    if (expire_renew_time > 0) {
      if (!is_virtual_table(partition.get_table_id())) {
        /*
         * is_mark_failed_ is not used any more.
         * if (OB_FAIL(clear_location(partition.get_table_id(), partition.get_partition_id(),
         *     expire_renew_time, cluster_id))) {
         *   LOG_WARN("clear_location failed", K(ret), K(partition), K(expire_renew_time));
         * }
         */
      } else {
        if (OB_FAIL(clear_vtable_location(partition.get_table_id(), expire_renew_time))) {
          LOG_WARN("clear_vtable_location failed", K(ret), "table_id", partition.get_table_id(), K(expire_renew_time));
        }
      }

      if (OB_SUCC(ret)) {
        LOG_TRACE("clear_location succeed", K(partition), K(expire_renew_time));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_update_task(task))) {
        LOG_WARN("add location update task failed", K(task), K(ret));
      } else if (EXECUTE_COUNT_PER_SEC(16)) {
        LOG_INFO("add_update_task succeed", K(partition), K(task));
      }
    }
  }
  return ret;
}

// if is_limited = true, it means nonblock_renew is not triggered.
int ObPartitionLocationCache::nonblock_renew_with_limiter(
    const ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited)
{
  int ret = OB_SUCCESS;
  is_limited = true;
  int64_t cur_timestamp = ObTimeUtility::current_time();
  ;
  int64_t last_renew_timestamp = renew_limiter_.get_last_renew_timestamp(partition);
  if (cur_timestamp - last_renew_timestamp > RenewLimiter::RENEW_INTERVAL) {
    if (renew_limiter_.try_update_last_renew_timestamp(partition, cur_timestamp)) {
      is_limited = false;
      if (OB_FAIL(nonblock_renew(partition, expire_renew_time))) {
        LOG_WARN("fail to nonblock renew", K(ret), K(partition), K(expire_renew_time));
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::inner_get_from_cache(
    const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id, ObPartitionLocation& result)
{
  int ret = OB_SUCCESS;
  ObLocationCacheKey cache_key(table_id, partition_id, cluster_id);
  result.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else {
    if (use_sys_cache(table_id)) {
      int hash_ret = sys_cache_.get_refactored(cache_key, result);
      if (OB_SUCCESS == hash_ret) {
        LOG_TRACE("location hit in sys cache", K(cache_key), K(ret));
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("location is not in sys cache", K(cache_key), K(ret));
      } else {
        ret = hash_ret;
        LOG_WARN("get location from sys cache failed", K(cache_key), K(ret));
      }
    } else {
      ObKVCacheHandle handle;
      const ObLocationCacheValue* cache_value = NULL;
      ret = user_cache_.get(cache_key, cache_value, handle);
      if (OB_FAIL(ret)) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get location from user cache failed", K(cache_key), K(ret));
        }
      } else {
        if (OB_FAIL(cache_value2location(*cache_value, result))) {
          LOG_WARN("transform cache value to location failed", K(cache_value), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::get_from_cache(const uint64_t table_id, const int64_t partition_id,
    const int64_t cluster_id, const bool is_nonblock, ObPartitionLocation& result,
    const bool filter_not_readable_replica, const bool auto_update)
{
  int ret = OB_SUCCESS;
  // NG_TRACE(plc_get_from_cache_begin);
  ObLocationCacheKey cache_key(table_id, partition_id, cluster_id);
  ObPartitionLocation location;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else if (OB_FAIL(inner_get_from_cache(table_id, partition_id, cluster_id, location))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get from cache", KR(ret), K(table_id), K(partition_id), K(cluster_id));
    }
  }

  // only nonblock get and leader not alive need add async update task here
  bool leader_inactive_async_update = false;
  bool leader_server_exist = true;
  int64_t trace_time = 0;
  bool alive = true;
  ObReplicaLocation leader;
  bool new_mode = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2274;
  if (OB_FAIL(ret) || cluster_id != cluster_id_) {
  } else if ((!new_mode && OB_FAIL(location.get_strong_leader(leader))) ||
             (new_mode && OB_FAIL(location.get_leader_by_election(leader)))) {
    // ignore result code
    ret = OB_SUCCESS;
  } else if (OB_FAIL(server_tracer_->get_server_status(leader.server_, alive, leader_server_exist, trace_time))) {
    LOG_WARN("fail to get server status", KR(ret), K(leader));
  }

  if (!GCTX.is_standby_cluster() || new_mode || ObMultiClusterUtil::is_cluster_private_table(table_id)) {
    if (OB_SUCC(ret) && !location.is_mark_fail() && is_nonblock && cluster_id_ == cluster_id) {
      // check leader alive
      if ((!new_mode && OB_FAIL(location.get_strong_leader(leader))) ||
          (new_mode && OB_FAIL(location.get_leader_by_election(leader)))) {
        if (OB_LOCATION_LEADER_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          leader_inactive_async_update = true;
        } else {
          LOG_WARN("get location leader failed", K(ret), K(location));
        }
      } else if (!alive || !leader_server_exist ||
                 share::ObServerBlacklist::get_instance().is_in_blacklist(
                     share::ObCascadMember(leader.server_, cluster_id_))) {
        leader_inactive_async_update = true;
      }
    }
  }

  const int64_t now = ObTimeUtility::current_time();
  if (OB_SUCC(ret) && auto_update) {
    bool has_task = false;
    if (is_nonblock && (cluster_id_ == cluster_id) && is_duty_time(location) && can_do_force_sql_renew(table_id)) {
      // To resolve problem of imprecise rpc renew result for non paxos members,
      // try generate asynchronous sql renew task if it's been long time since last renew location by SQL.
      ObLocationAsyncUpdateTask task(
          *this, table_id, partition_id, now, cluster_id, ObLocationAsyncUpdateTask::MODE_SQL_ONLY);
      // use temp_ret to avoid overwrite ret
      int temp_ret = add_update_task(task);
      if (OB_SUCCESS != temp_ret) {
        LOG_WARN("add location update task failed", K(location), K(task), K(temp_ret));
      } else {
        has_task = true;
        LOG_INFO("add_update_task success", K(task), K(location));
      }
    }

    // if location cache expires, add a update task. ObDequeQueue will filter duplicate task
    // if location is mark fail and is_nonblock is false, renew location will be called by caller, no need to add upadte
    // task if location leader on non-alive server and is nonblock get, need add a update task
    if (OB_SUCCESS == ret && !has_task && (!location.is_mark_fail() || is_nonblock || leader_inactive_async_update)) {
      if (now - location.get_renew_time() >= config_->location_cache_expire_time || location.is_mark_fail() ||
          leader_inactive_async_update) {
        ObLocationAsyncUpdateTask task(
            *this, table_id, partition_id, now, cluster_id, ObLocationAsyncUpdateTask::MODE_AUTO);
        // use temp_ret to avoid overwrite ret
        int temp_ret = add_update_task(task);
        if (OB_SUCCESS != temp_ret) {
          LOG_WARN("add location update task failed", K(location), K(task), K(temp_ret));
        } else {
          LOG_INFO(
              "add_update_task success", K(task), K(leader_inactive_async_update), K(now), K(location), K(is_nonblock));
        }
      }
    }
  }
  if (OB_SUCC(ret) && is_nonblock && location.is_mark_fail()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("location is mark fail, treat it as not exist for nonblock get", K(ret), K(location));
  }
  // NG_TRACE(plc_get_from_cache_end);
  // filter not readable replica

  int64_t safe_interval = 3 * ObAliveServerRefreshTask::REFRESH_INTERVAL_US;  // 15S
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (leader_server_exist) {
  } else if (now - trace_time < safe_interval) {
    // AliveServerTracer is credible enough if AliveServerTracer has refreshed in the recent period.
    // When server is inactive, using an inactive leader to query may cause -4012 error.
    // To avoid this, we try to reset leader to follower in location cache,
    // which may force SQL/Trans layer to renew location before they use old location cache.
    leader.role_ = FOLLOWER;
    int64_t tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = location.update(leader.server_, leader))) {
      LOG_WARN("fail to update location", KR(ret), K(leader));
    } else {
      LOG_INFO("update location to be follower success", K(leader));
    }
  } else {
  }

  if (OB_SUCC(ret)) {
    if (!filter_not_readable_replica && OB_FAIL(result.assign(location))) {
      LOG_WARN("assign location fail", K(ret), K(location));
    } else if (filter_not_readable_replica && OB_FAIL(result.assign_with_only_readable_replica(location))) {
      LOG_WARN("assign location with only readable replica fail", K(ret), K(location));
    }
  }
  return ret;
}

int ObPartitionLocationCache::renew_location(const uint64_t table_id, const int64_t partition_id,
    const int64_t cluster_id, ObPartitionLocation& location, const int64_t expire_renew_time,
    const bool result_filter_not_readable_replica, const bool force_sql_renew, const bool auto_update)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation new_location;
  bool sem_acquired = false;
  ObTimeoutCtx ctx;
  int64_t abs_timeout_us = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else if (OB_FAIL(set_timeout_ctx(ctx))) {
    LOG_WARN("failed to set timeout ctx", K(ret));
  } else {
    abs_timeout_us = ObTimeoutCtx::get_ctx().get_abs_timeout();
  }

  if (OB_SUCC(ret)) {
    // only limit user table fetch location concurrent requests,
    // if use table request count is limited, sys table request triggered by user table
    // request is limited naturally
    // we assume virtual table request is not numerous
    if (!is_sys_table(table_id) && !is_virtual_table(table_id)) {
      const uint64_t wait_event_time_out = 0;
      const int64_t unused = 0;
      ObWaitEventGuard wait_guard(ObWaitEventIds::PT_LOCATION_CACHE_LOCK_WAIT,
          wait_event_time_out,
          static_cast<int64_t>(table_id),
          partition_id,
          unused);

      // ignore acquire fail
      int tmp_ret = sem_.acquire(abs_timeout_us);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("acquire failed", K(tmp_ret));
        if (OB_TIMEOUT == tmp_ret) {
          ret = OB_TIMEOUT;
          LOG_WARN("already timeout", K(ret), K(abs_timeout_us));
        }
      } else {
        sem_acquired = true;
      }
    }
  }

  bool refresh_by_rpc = false;
  bool refresh_by_sql = false;
  bool exist_in_cache = true;
  // try to get from cache, maybe other thread has already fetched location
  if (OB_SUCC(ret)) {
    const bool is_nonblock = false;
    const bool filter_not_readable_replica = false;
    refresh_by_rpc = false;
    if (OB_FAIL(get_from_cache(
            table_id, partition_id, cluster_id, is_nonblock, location, filter_not_readable_replica, auto_update))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_from_cache failed", K(table_id), K(partition_id), K(cluster_id), K(ret));
      } else {
        exist_in_cache = false;
        refresh_by_sql = true;
        ret = OB_SUCCESS;
      }
    } else if (!location.is_mark_fail() &&
               ((!force_sql_renew && location.get_renew_time() > expire_renew_time) ||
                   (force_sql_renew && location.get_sql_renew_time() > expire_renew_time))) {
      refresh_by_sql = false;
    } else {
      // we need to treat mark_fail location as deleted, must renew it
      refresh_by_rpc = cluster_id_ == cluster_id;  // standby cluster should renew location by sql
      refresh_by_sql = true;
    }
  }

  const int64_t now = ObTimeUtility::current_time();
  if (OB_SUCC(ret) && (refresh_by_rpc || refresh_by_sql)) {
    EVENT_INC(LOCATION_CACHE_RENEW);
    // refresh by rpc
    if (OB_SUCC(ret) && refresh_by_rpc) {
      bool skip_rpc_renew = false;
      if (OB_FAIL(check_skip_rpc_renew_v2(location, skip_rpc_renew))) {
        LOG_WARN("check skip rpc renew failed", K(ret), K(location));
        ret = OB_SUCCESS;
        skip_rpc_renew = true;
      }
      if (skip_rpc_renew) {
        EVENT_INC(LOCATION_CACHE_IGNORE_RPC_RENEW);
      }
      if (OB_SUCC(ret) && !skip_rpc_renew) {
        bool is_new_location_valid = false;
        const int64_t rpc_timeout = ctx.get_timeout() / 2;  // reserve 50% for fetch from meta table
        ctx.set_timeout(rpc_timeout);
        if (OB_FAIL(
                location_fetcher_.renew_location_with_rpc_v2(nullptr, location, new_location, is_new_location_valid))) {
          LOG_WARN("failed to renew location with rpc",
              K(ret),
              K(table_id),
              K(partition_id),
              K(location),
              K(expire_renew_time),
              "cost",
              ObTimeUtility::current_time() - now,
              K(lbt()));
          // we can still fetch location from roottable
          ret = OB_SUCCESS;
        } else if (is_new_location_valid) {
          if (EXECUTE_COUNT_PER_SEC(16)) {
            LOG_INFO("fetch location with rpc success",
                K(table_id),
                K(partition_id),
                "old_location",
                location,
                K(new_location),
                K(expire_renew_time),
                "cost",
                ObTimeUtility::current_time() - now,
                K(lbt()));
          }
          refresh_by_sql = false;
        } else {
          EVENT_INC(LOCATION_CACHE_RPC_RENEW_FAIL);
          LOG_WARN("rpc renew failed, need sql renew",
              K(table_id),
              K(partition_id),
              K(location),
              K(expire_renew_time),
              "cost",
              ObTimeUtility::current_time() - now,
              K(lbt()));
        }
        ctx.set_abs_timeout(abs_timeout_us);  // reset timeout
      }
    }  // end rpc renew

    // refresh by sql
    if (OB_SUCC(ret) && refresh_by_sql) {
      if (!auto_update && OB_ALL_CORE_TABLE_TID == extract_pure_id(table_id)) {
        // To avoid -4012 error when renew __all_core_table's location cache.
        ctx.set_abs_timeout(OB_INVALID_TIMESTAMP);
      }
      if (OB_FAIL(location_fetcher_.fetch_location(table_id, partition_id, cluster_id, new_location))) {
        LOG_WARN("fetch location failed",
            K(ret),
            KT(table_id),
            K(partition_id),
            K(expire_renew_time),
            "cost",
            ObTimeUtility::current_time() - now,
            K(lbt()));
        if (!auto_update  // auto_update is false means renew_location() is running on background worker thread.
            && !exist_in_cache && use_sys_leader_cache(table_id, cluster_id)) {
          // When renew location failed and location cache is empty,
          // try renew location cache of sys tenant's system tables by RPC.
          int tmp_ret = OB_SUCCESS;
          ObArray<ObLocationCacheKey> keys;
          ObLocationCacheKey key(table_id, partition_id, cluster_id);
          if (OB_SUCCESS != (tmp_ret = keys.push_back(key))) {
            LOG_WARN("fail to push back key", K(tmp_ret), K(key));
          } else if (OB_SUCCESS != (tmp_ret = batch_renew_sys_table_location_by_rpc(keys))) {
            LOG_WARN("fail to batch renew sys table location by rpc", K(tmp_ret), K(key));
          }
        }
      } else {
        EVENT_INC(LOCATION_CACHE_SQL_RENEW);
      }
      if (location.get_replica_locations().count() > 0) {
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("location cache not hit, fetch location by partition_table",
            K(ret),
            K(location),
            K(new_location),
            K(expire_renew_time),
            "cost",
            ObTimeUtility::current_time() - now,
            K(lbt()));
      }
      ctx.set_abs_timeout(abs_timeout_us);  // reset timeout
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_location(table_id, partition_id, cluster_id, new_location))) {
        LOG_WARN("update location in cache failed", K(ret), KT(table_id), K(partition_id), K(new_location));
      } else if (result_filter_not_readable_replica &&
                 OB_FAIL(location.assign_with_only_readable_replica(new_location))) {
        LOG_WARN("assign with only readable replica fail", K(ret), K(location), K(new_location));
      } else if (!result_filter_not_readable_replica && OB_FAIL(location.assign(new_location))) {
        LOG_WARN("assign location fail", K(ret), K(location), K(new_location));
      }
    }
  } else {
    EVENT_INC(LOCATION_CACHE_RENEW_IGNORED);
  }

  if (sem_acquired) {
    // ignore release fail
    int tmp_ret = sem_.release();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("release failed", K(tmp_ret));
    }
  }
  return ret;
}

/*
 * renew location cache by RPC may failed in the following cases:
 * 1. location cache is empty.
 * 2. there is no avaliable paxos member in local region.
 * 3. there is inactive paxos member in local region.
 * 4. non cluster private tables in primary cluster before ver 2.2.74.
 * 5. cluster is on ofs mode with single zone deployment.
 * 5. partition is in physical restore status. TODO: () should be removed not less than ver 3.2.0
 */
int ObPartitionLocationCache::check_skip_rpc_renew_v2(const ObPartitionLocation& location, bool& skip)
{
  int ret = OB_SUCCESS;
  skip = false;
  bool alive = false;
  int64_t trace_time = 0;
  int64_t paxos_replica_num = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (location.get_replica_locations().count() <= 0) {
    skip = true;
    LOG_DEBUG("location count less than 0, skip rpc_renew");
  } else if (GCTX.is_standby_cluster() && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2274 &&
             !ObMultiClusterUtil::is_cluster_private_table(location.get_table_id())) {
    skip = true;
    LOG_DEBUG("slave cluster has no member list, skip rpc_renew");
  } else {
    skip = false;
    bool is_local = false;
    FOREACH_CNT_X(l, location.get_replica_locations(), OB_SUCC(ret) && !skip)
    {
      is_local = false;
      if (OB_ISNULL(l)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica location is null", K(ret));
      } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(l->replica_type_)) {
        // nothing todo
      } else if (share::ObServerBlacklist::get_instance().is_in_blacklist(
                     share::ObCascadMember(l->server_, cluster_id_))) {
        skip = true;
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("server is in blacklist, skip rpc renew", K(ret), "server", l->server_);
      } else if (OB_FAIL(locality_manager_->is_local_server(l->server_, is_local))) {
        LOG_WARN("fail to check is local server", K(ret), "server", l->server_);
        ret = OB_SUCCESS;
        skip = true;
      } else if (!is_local) {
        // noting todo
      } else if (OB_FAIL(server_tracer_->is_alive(l->server_, alive, trace_time))) {
        LOG_WARN("check server alive failed", K(ret), "server", l->server_);
      } else if (!alive) {
        skip = true;
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("server is not alive, skip rpc renew", K(ret), K(alive), "server", l->server_, K(is_local));
      } else if (l->is_phy_restore_replica()) {
        // restore replica refresh location cache by sql
        skip = true;
      }
      if (OB_SUCC(ret) && is_local && ObReplicaTypeCheck::is_paxos_replica_V2(l->replica_type_)) {
        paxos_replica_num++;
      }
    }  // end foreach
    if (OB_SUCC(ret) && !skip && paxos_replica_num <= 0) {
      LOG_DEBUG("paxos replica num less than 1, skip rpc renew", K(paxos_replica_num));
      skip = true;
    }
  }
  if (skip) {
    LOG_DEBUG("need skip rpc renew", K(location), K(skip));
  }
  return ret;
}

int ObPartitionLocationCache::update_location(
    const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id, const ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id) || !location.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(location));
  } else {
    ObLocationCacheKey cache_key(table_id, partition_id, cluster_id);
    if (use_sys_cache(table_id)) {
      int overwrite_flag = 1;
      // try update sys leader cache
      if (use_sys_leader_cache(table_id, cluster_id)) {
        LocationInfo leader_location_info;
        ObReplicaLocation leader_replica;
        int tmp_ret = location.get_strong_leader(leader_replica);
        if (OB_SUCCESS == tmp_ret) {
          leader_location_info.set(leader_replica.server_, location.get_renew_time());
        } else {
        }  // invalid leader location info, no need to set
        // set sys leader cache
        if (OB_SUCCESS !=
            (tmp_ret = sys_leader_cache_.set_refactored(cache_key, leader_location_info, overwrite_flag))) {
          LOG_WARN("fail to set sys leader info", KR(tmp_ret), K(cache_key), K(leader_location_info));
        } else {
          LOG_INFO("renew sys table leader info", KR(tmp_ret), K(cache_key), K(leader_location_info));
        }
      }
      if (OB_FAIL(sys_cache_.set_refactored(cache_key, location, overwrite_flag))) {
        LOG_WARN("set new location in sys_cache failed", K(cache_key), K(location), K(ret));
      } else {
        LOG_TRACE("renew location in sys_cache succeed", K(cache_key), K(location));
      }
    } else {
      // try update leader cache
      if (cluster_id_ == cluster_id) {
        LocationInfo leader_location_info;
        ObReplicaLocation leader_replica;
        const bool force_update = false;
        int tmp_ret = location.get_strong_leader(leader_replica);
        if (OB_SUCCESS == tmp_ret) {
          leader_location_info.set(leader_replica.server_, location.get_renew_time());
        } else {
        }  // invalid leader location info, no need to set
        // set leader cache
        if (OB_SUCCESS !=
            (tmp_ret = leader_cache_.set_strong_leader_info(cache_key, leader_location_info, force_update))) {
          LOG_WARN("fail to set leader", K(tmp_ret), K(cache_key), K(leader_location_info));
        }
      }
      ObLocationCacheValue cache_value;
      char* buffer = NULL;
      buffer = static_cast<char*>(ob_malloc(OB_MAX_LOCATION_SERIALIZATION_SIZE, ObModIds::OB_MS_LOCATION_CACHE));
      if (NULL == buffer) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(location2cache_value(location, buffer, OB_MAX_LOCATION_SERIALIZATION_SIZE, cache_value))) {
        LOG_WARN("transform location to cache_value failed", K(location), K(ret));
      } else if (OB_FAIL(user_cache_.put(cache_key, cache_value))) {
        LOG_WARN("put location to user location cache failed", K(cache_key), K(cache_value), K(ret));
      } else {
        LOG_TRACE("renew location in user_cache succeed", K(cache_key), K(location));
      }
      if (NULL != buffer) {
        ob_free(buffer);
        buffer = NULL;
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::clear_location(
    const uint64_t table_id, const int64_t partition_id, const int64_t expire_renew_time, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else {
    ObPartitionLocation location;
    ObLocationCacheKey cache_key(table_id, partition_id, cluster_id);
    if (use_sys_cache(table_id)) {
      int hash_ret = sys_cache_.get_refactored(cache_key, location);
      if (OB_SUCCESS != hash_ret) {
        if (OB_HASH_NOT_EXIST == hash_ret) {
          LOG_TRACE("location not exist, clear_location do nothing", K(table_id), K(partition_id));
        } else {
          ret = hash_ret;
          LOG_WARN("get location from sys_cache failed", K(ret), K(cache_key));
        }
      } else {
        if (location.get_renew_time() > expire_renew_time) {
          LOG_TRACE("location exist, but renew time is not expired, no need to clear it",
              K(table_id),
              K(partition_id),
              K(location.get_renew_time()),
              K(expire_renew_time));
          EVENT_INC(LOCATION_CACHE_CLEAR_LOCATION_IGNORED);
        } else {
          LOG_TRACE("location exist, clear_location mark it fail", K(table_id), K(partition_id));
          location.mark_fail();
          const int overwrite_flag = 1;
          if (OB_FAIL(sys_cache_.set_refactored(cache_key, location, overwrite_flag))) {
            LOG_WARN("failed to mark location deleted", K(ret), K(hash_ret), K(cache_key), K(location));
          } else {
            LOG_TRACE("mark_location_fail in sys_cache success", K(cache_key), K(location));
            EVENT_INC(LOCATION_CACHE_CLEAR_LOCATION);
          }
        }
      }
    } else {
      ObKVCacheHandle handle;
      const ObLocationCacheValue* cache_value = NULL;

      if (OB_FAIL(user_cache_.get(cache_key, cache_value, handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get location from user cache", K(ret), K(cache_key));
        } else {
          ret = OB_SUCCESS;
          LOG_TRACE("location not exist, clear_location do nothing", K(table_id), K(partition_id));
        }
      } else {
        ObLocationCacheValue new_cache_value;
        char* buffer = NULL;
        buffer = static_cast<char*>(ob_malloc(OB_MAX_LOCATION_SERIALIZATION_SIZE, ObModIds::OB_MS_LOCATION_CACHE));
        if (NULL == buffer) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_ISNULL(cache_value)) {
          ret = OB_ERR_SYS;
          LOG_WARN("cache value must not null", K(ret), K(table_id), K(partition_id));
        } else if (OB_FAIL(cache_value2location(*cache_value, location))) {
          LOG_WARN("failed to transform cache value to location", K(ret), K(cache_value));
        } else {
          if (location.get_renew_time() > expire_renew_time) {
            LOG_TRACE("location exist, but renew time is not expired, no need to clear it",
                K(table_id),
                K(partition_id),
                K(location.get_renew_time()),
                K(expire_renew_time));
            EVENT_INC(LOCATION_CACHE_CLEAR_LOCATION_IGNORED);
          } else {
            LOG_TRACE("location exist, clear_location mark it fail", K(table_id), K(partition_id));
            location.mark_fail();

            if (OB_FAIL(location2cache_value(location, buffer, OB_MAX_LOCATION_SERIALIZATION_SIZE, new_cache_value))) {
              LOG_WARN("failed to transform location 2 cache value", K(ret), K(location));
            } else if (OB_FAIL(user_cache_.put(cache_key, new_cache_value))) {
              LOG_WARN("failed to put new cache value", K(ret), K(cache_key), K(new_cache_value));
            } else {
              LOG_TRACE("mark_location_fail in user cache success", K(table_id), K(partition_id), K(location));
              EVENT_INC(LOCATION_CACHE_CLEAR_LOCATION);
            }
          }
        }
        if (NULL != buffer) {
          ob_free(buffer);
          buffer = NULL;
        }
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::vtable_get(const uint64_t table_id, ObIArray<ObPartitionLocation>& locations,
    const int64_t expire_renew_time, bool& is_cache_hit)
{
  int ret = OB_SUCCESS;
  ObSArray<ObPartitionLocation> vtable_locations;
  bool need_renew = false;
  is_cache_hit = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || !is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id));
  } else {
    ret = get_from_vtable_cache(table_id, vtable_locations);
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      need_renew = true;
      LOG_DEBUG("location not exist, need renew", K(table_id));
    } else if (OB_SUCCESS != ret) {
      LOG_WARN("get location from vtable cache failed", K(ret), KT(table_id));
    } else if (vtable_locations.size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vtable location size must larger than 0", K(ret), K(vtable_locations));
    } else if (vtable_locations.at(0).get_renew_time() <= expire_renew_time) {
      need_renew = true;
      LOG_DEBUG("location is too old, need renew",
          K(table_id),
          K(expire_renew_time),
          "location renew time",
          vtable_locations.at(0).get_renew_time());
    } else {
      // check server alive
      bool alive = false;
      int64_t trace_time = 0;
      FOREACH_CNT_X(location, vtable_locations, OB_SUCC(ret) && !need_renew)
      {
        FOREACH_CNT_X(l, location->get_replica_locations(), OB_SUCC(ret) && !need_renew)
        {
          if (OB_FAIL(server_tracer_->is_alive(l->server_, alive, trace_time))) {
            LOG_WARN("check server alive failed", K(ret), "server", l->server_);
          } else {
            if (!alive && trace_time > location->get_renew_time()) {
              need_renew = true;
              LOG_DEBUG(
                  "force renew virtual table location because location on non-alive server", "location", *location);
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (need_renew) {
        is_cache_hit = false;
      } else {
        is_cache_hit = true;
      }
    }

    if (OB_SUCC(ret) && need_renew) {
      if (OB_FAIL(renew_vtable_location(table_id, vtable_locations))) {
        LOG_WARN("renew vtable location failed", KT(table_id), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(locations.assign(vtable_locations))) {
      LOG_WARN("assign failed", K(ret));
    }
  }

  return ret;
}

int ObPartitionLocationCache::get_from_vtable_cache(const uint64_t table_id, ObSArray<ObPartitionLocation>& locations)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  ObLocationCacheKey cache_key(table_id, 0, cluster_id_);
  const ObLocationCacheValue* cache_value = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || !is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id));
  } else if (OB_FAIL(user_cache_.get(cache_key, cache_value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get location from user cache failed", K(cache_key), K(ret));
    } else {
      LOG_TRACE("location is not in user cache", K(cache_key), K(ret));
    }
  } else {
    LOG_TRACE("location hit in user cache", K(cache_key), K(ret));
    if (OB_FAIL(cache_value2location(*cache_value, locations))) {
      LOG_WARN("transform cache value to location failed", K(cache_value), K(ret));
    }
  }

  if (OB_SUCCESS == ret) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t first_index = 0;
    const int64_t partition_id = 0;
    if (locations.size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("locations is empty", K(ret));
    } else if (now - locations.at(first_index).get_renew_time() >= config_->virtual_table_location_cache_expire_time) {
      int64_t cluster_id = cluster_id_;
      ObLocationAsyncUpdateTask task(
          *this, table_id, partition_id, now, cluster_id, ObLocationAsyncUpdateTask::MODE_AUTO);
      // use temp_ret to avoid overwrite ret
      int temp_ret = add_update_task(task);
      if (OB_SUCCESS != temp_ret) {
        LOG_WARN("add location update task failed", K(locations), K(task), K(temp_ret));
      } else {
        LOG_DEBUG("add_update_task success", K(task));
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::renew_vtable_location(const uint64_t table_id, ObSArray<ObPartitionLocation>& locations)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || !is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id));
  } else if (OB_FAIL(set_timeout_ctx(ctx))) {
    LOG_WARN("failed to set timeout ctx", K(ret));
  } else if (OB_FAIL(location_fetcher_.fetch_vtable_location(table_id, locations))) {
    LOG_WARN("fetch_vtable_location failed", KT(table_id), K(ret));
  } else if (OB_FAIL(update_vtable_location(table_id, locations))) {
    LOG_WARN("update_vtable_location failed", KT(table_id), K(ret));
  } else {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("renew vtable location success", KT(table_id), K(locations), K(ret));
  }
  return ret;
}

int ObPartitionLocationCache::update_vtable_location(
    const uint64_t table_id, const ObSArray<ObPartitionLocation>& locations)
{
  const int64_t MAX_BUFFER_SIZE = 1L << 20;  // 1MB
  int ret = OB_SUCCESS;
  ObLocationCacheKey cache_key(table_id, 0, cluster_id_);
  ObLocationCacheValue cache_value;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || !is_virtual_table(table_id) || locations.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), "location count", locations.count());
  } else {
    const int64_t buffer_size = locations.get_serialize_size();
    if (buffer_size <= 0 || buffer_size > MAX_BUFFER_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", K(ret), K(buffer_size));
    } else {
      char* buffer = static_cast<char*>(ob_malloc(buffer_size, ObModIds::OB_MS_LOCATION_CACHE));
      if (NULL == buffer) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(location2cache_value(locations, buffer, buffer_size, cache_value))) {
        LOG_WARN("transform location to cache_value failed", K(locations), K(buffer_size), K(ret));
      } else if (OB_FAIL(user_cache_.put(cache_key, cache_value))) {
        LOG_WARN("put location to user location cache failed", K(cache_key), K(cache_value), K(ret));
      } else {
        LOG_TRACE("renew location in user_cache succeed", K(cache_key), K(locations));
      }
      if (NULL != buffer) {
        ob_free(buffer);
        buffer = NULL;
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::clear_vtable_location(const uint64_t table_id, const int64_t expire_renew_time)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  const ObLocationCacheKey cache_key(table_id, 0, cluster_id_);
  const ObLocationCacheValue* cache_value = NULL;
  ObSArray<ObPartitionLocation> locations;

  if (OB_INVALID_ID == table_id || !is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), KT(table_id), K(expire_renew_time));
  } else if (OB_FAIL(user_cache_.get(cache_key, cache_value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get location from user cache", K(ret), K(cache_key));
    } else {
      ret = OB_SUCCESS;
      LOG_TRACE("location not exist, clear_location do nothing", K(table_id));
    }
  } else {
    ObLocationCacheValue new_cache_value;
    char* buffer = static_cast<char*>(ob_malloc(OB_MAX_LOCATION_SERIALIZATION_SIZE, ObModIds::OB_MS_LOCATION_CACHE));
    if (NULL == buffer) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(cache_value)) {
      ret = OB_ERR_SYS;
      LOG_WARN("cache value must not null", K(ret), K(table_id));
    } else if (OB_FAIL(cache_value2location(*cache_value, locations))) {
      LOG_WARN("failed to transform cache value to location", K(ret), K(cache_value));
    } else if (locations.size() > 0) {
      const ObPartitionLocation& first_location = locations.at(0);
      if (first_location.get_renew_time() > expire_renew_time) {
        LOG_TRACE("location exist, but renew time is not expired, no need to clear it",
            K(table_id),
            K(first_location.get_renew_time()),
            K(expire_renew_time));
        EVENT_INC(LOCATION_CACHE_CLEAR_LOCATION_IGNORED);
      } else {
        LOG_TRACE("location exist, clear_location mark it fail", K(table_id));
        FOREACH_CNT(location, locations)
        {
          location->mark_fail();
        }

        if (OB_FAIL(location2cache_value(locations, buffer, OB_MAX_LOCATION_SERIALIZATION_SIZE, new_cache_value))) {
          LOG_WARN("failed to transform location 2 cache value", K(ret), K(locations));
        } else if (OB_FAIL(user_cache_.put(cache_key, new_cache_value))) {
          LOG_WARN("failed to put new cache value", K(ret), K(cache_key), K(new_cache_value));
        } else {
          LOG_TRACE("mark_location_fail in user cache success", K(table_id), K(locations));
          EVENT_INC(LOCATION_CACHE_CLEAR_LOCATION);
        }
      }
    }
    if (NULL != buffer) {
      ob_free(buffer);
      buffer = NULL;
    }
  }

  return ret;
}

int ObPartitionLocationCache::cache_value2location(
    const ObLocationCacheValue& cache_value, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  location.reset();
  if (NULL == cache_value.buffer_ || cache_value.size_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache_value", "buffer", OB_P(cache_value.buffer_), "size", cache_value.size_, K(ret));
  } else {
    int64_t pos = 0;
    if (OB_SUCC(ret)) {
      if (pos + static_cast<int64_t>(sizeof(location.table_id_)) > cache_value.size_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize table_id failed",
            "read pos",
            pos + sizeof(location.table_id_),
            "buf size",
            cache_value.size_,
            K(ret));
      } else {
        MEMCPY(&(location.table_id_), cache_value.buffer_ + pos, sizeof(location.table_id_));
        pos += sizeof(location.table_id_);
      }
    }

    if (OB_SUCC(ret)) {
      if (pos + static_cast<int64_t>(sizeof(location.partition_id_)) > cache_value.size_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize partition_id failed",
            "read pos",
            pos + sizeof(location.partition_id_),
            "buf size",
            cache_value.size_,
            K(ret));
      } else {
        MEMCPY(&(location.partition_id_), cache_value.buffer_ + pos, sizeof(location.partition_id_));
        pos += sizeof(location.partition_id_);
      }
    }

    if (OB_SUCC(ret)) {
      if (pos + static_cast<int64_t>(sizeof(location.partition_cnt_)) > cache_value.size_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize partition_cnt failed",
            "read pos",
            pos + sizeof(location.partition_cnt_),
            "buf size",
            cache_value.size_,
            K(ret));
      } else {
        MEMCPY(&(location.partition_cnt_), cache_value.buffer_ + pos, sizeof(location.partition_cnt_));
        pos += sizeof(location.partition_cnt_);
      }
    }

    int64_t replica_count = 0;
    if (OB_SUCC(ret)) {
      if (pos + static_cast<int64_t>(sizeof(replica_count)) > cache_value.size_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize replica_count failed",
            "read pos",
            pos + sizeof(replica_count),
            "buf size",
            cache_value.size_,
            K(ret));
      } else {
        MEMCPY(&replica_count, cache_value.buffer_ + pos, sizeof(replica_count));
        pos += sizeof(replica_count);
      }
    }

    if (OB_SUCCESS == ret && replica_count > 0) {
      if (pos + static_cast<int64_t>(sizeof(ObReplicaLocation)) * replica_count > cache_value.size_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize replica_locations failed",
            "read pos",
            pos + sizeof(ObReplicaLocation) * replica_count,
            "buf size",
            cache_value.size_,
            K(ret));
      } else {
        ObReplicaLocation replica_location;
        for (int64_t i = 0; OB_SUCC(ret) && i < replica_count; ++i) {
          if (OB_FAIL(location.replica_locations_.push_back(replica_location))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          MEMCPY(&(location.replica_locations_.at(0)),
              cache_value.buffer_ + pos,
              sizeof(ObReplicaLocation) * replica_count);
          pos += sizeof(ObReplicaLocation) * replica_count;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (pos + static_cast<int64_t>(sizeof(location.renew_time_)) > cache_value.size_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize renew_time failed",
            "read pos",
            pos + sizeof(location.renew_time_),
            "buf size",
            cache_value.size_,
            K(ret));
      } else {
        MEMCPY(&location.renew_time_, cache_value.buffer_ + pos, sizeof(location.renew_time_));
        pos += sizeof(location.renew_time_);
      }
    }

    if (OB_SUCC(ret)) {
      if (pos + static_cast<int64_t>(sizeof(location.is_mark_fail_)) > cache_value.size_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize is_mark_fail_ failed",
            "read pos",
            pos + sizeof(location.is_mark_fail_),
            "buf size",
            cache_value.size_,
            K(ret));
      } else {
        MEMCPY(&location.is_mark_fail_, cache_value.buffer_ + pos, sizeof(location.is_mark_fail_));
        pos += sizeof(location.is_mark_fail_);
      }
    }

    if (OB_SUCC(ret)) {
      if (pos + static_cast<int64_t>(sizeof(location.sql_renew_time_)) > cache_value.size_) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize renew_time failed",
            "read pos",
            pos + sizeof(location.sql_renew_time_),
            "buf size",
            cache_value.size_,
            K(ret));
      } else {
        MEMCPY(&location.sql_renew_time_, cache_value.buffer_ + pos, sizeof(location.sql_renew_time_));
        pos += sizeof(location.sql_renew_time_);
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::location2cache_value(
    const ObPartitionLocation& location, char* buf, const int64_t buf_size, ObLocationCacheValue& cache_value)
{
  int ret = OB_SUCCESS;
  if (!location.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid location", K(location), K(ret));
  } else if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null or invalid buf_size", "buf", OB_P(buf), K(buf_size), K(ret));
  } else {
    int64_t pos = 0;
    const int64_t need_size =
        sizeof(location.table_id_) + sizeof(location.partition_id_) + sizeof(location.partition_cnt_) +
        sizeof(int64_t) + sizeof(ObReplicaLocation) * location.replica_locations_.count() +
        sizeof(location.renew_time_) + sizeof(location.is_mark_fail_) + sizeof(location.sql_renew_time_);
    if (need_size > buf_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("serialize location failed", K(need_size), K(buf_size), K(ret));
    } else {
      MEMCPY(buf + pos, &(location.table_id_), sizeof(location.table_id_));
      pos += sizeof(location.table_id_);
      MEMCPY(buf + pos, &(location.partition_id_), sizeof(location.partition_id_));
      pos += sizeof(location.partition_id_);
      MEMCPY(buf + pos, &(location.partition_cnt_), sizeof(location.partition_cnt_));
      pos += sizeof(location.partition_cnt_);
      const int64_t replica_count = location.replica_locations_.count();
      MEMCPY(buf + pos, &replica_count, sizeof(replica_count));
      pos += sizeof(replica_count);
      if (location.replica_locations_.count() > 0) {
        MEMCPY(buf + pos, &(location.replica_locations_.at(0)), replica_count * sizeof(ObReplicaLocation));
        pos += replica_count * sizeof(ObReplicaLocation);
      }
      MEMCPY(buf + pos, &(location.renew_time_), sizeof(location.renew_time_));
      pos += sizeof(location.renew_time_);
      MEMCPY(buf + pos, &(location.is_mark_fail_), sizeof(location.is_mark_fail_));
      pos += sizeof(location.is_mark_fail_);
      MEMCPY(buf + pos, &(location.sql_renew_time_), sizeof(location.sql_renew_time_));
      pos += sizeof(location.sql_renew_time_);
      cache_value.buffer_ = buf;
      cache_value.size_ = pos;
    }
  }
  return ret;
}

template <typename LOCATION>
int ObPartitionLocationCache::cache_value2location(const ObLocationCacheValue& cache_value, LOCATION& location)
{
  int ret = OB_SUCCESS;
  NG_TRACE(plc_serialize_begin);
  int64_t pos = 0;
  if (NULL == cache_value.buffer_ || cache_value.size_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache_value", "buffer", OB_P(cache_value.buffer_), "size", cache_value.size_, K(ret));
  } else if (OB_FAIL(location.deserialize(cache_value.buffer_, cache_value.size_, pos))) {
    LOG_WARN("deserialize location failed", K(cache_value), K(ret));
  }
  NG_TRACE(plc_serialize_end);
  return ret;
}

template <typename LOCATION>
int ObPartitionLocationCache::location2cache_value(
    const LOCATION& location, char* buf, const int64_t buf_size, ObLocationCacheValue& cache_value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null or invalid buf_size", KP(buf), K(buf_size), K(ret));
  } else if (OB_FAIL(location.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize location failed", K(location), "buf", reinterpret_cast<int64_t>(buf), K(ret));
  } else {
    cache_value.size_ = pos;
    cache_value.buffer_ = buf;
  }
  return ret;
}

int ObPartitionLocationCache::add_update_task(const ObLocationAsyncUpdateTask& task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), K(task));
  } else if (task.get_cluster_id() == cluster_id_) {
    if (OB_FAIL(local_async_queue_set_.add_task(task))) {
      LOG_WARN("fail to add task", K(ret), K(task));
    }
  } else {
    if (OB_FAIL(remote_async_queue_set_.add_task(task))) {
      LOG_WARN("fail to add task", K(ret), K(task));
    }
  }
  return ret;
}

int ObPartitionLocationCache::set_timeout_ctx(common::ObTimeoutCtx& ctx)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout_us = ctx.get_abs_timeout();

  if (abs_timeout_us < 0) {
    abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_FETCH_LOCATION_TIMEOUT_US;
  }
  if (THIS_WORKER.get_timeout_ts() > 0 && THIS_WORKER.get_timeout_ts() < abs_timeout_us) {
    abs_timeout_us = THIS_WORKER.get_timeout_ts();
  }

  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret), K(abs_timeout_us));
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
        K(ret),
        "abs_timeout",
        ctx.get_abs_timeout(),
        "this worker timeout ts",
        THIS_WORKER.get_timeout_ts());
  }

  return ret;
}

// link table.

int ObPartitionLocationCache::get_link_table_location(const uint64_t table_id, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  uint64_t dblink_id = extract_dblink_id(table_id);
  ObSchemaGetterGuard schema_guard;
  const ObDbLinkSchema* dblink_schema = NULL;
  if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_dblink_schema(dblink_id, dblink_schema))) {
    LOG_WARN("get dblink schema failed", K(dblink_id), K(ret));
  } else if (OB_ISNULL(dblink_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(dblink_id), K(ret));
  } else {
    location.set_table_id(table_id);
    location.set_partition_id(0);
    ObReplicaLocation replica_location;
    replica_location.server_ = dblink_schema->get_host_addr();
    replica_location.role_ = LEADER;
    replica_location.replica_type_ = REPLICA_TYPE_FULL;
    replica_location.sql_port_ = dblink_schema->get_host_port();
    if (OB_FAIL(location.add(replica_location))) {
      LOG_WARN(
          "add replica location to partition location failed", KT(table_id), K(replica_location), K(location), K(ret));
    }
  }
  return ret;
}

/*-----batch async renew location-----*/
int ObPartitionLocationCache::batch_process_tasks(
    const common::ObIArray<ObLocationAsyncUpdateTask>& tasks, bool& stopped)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  UNUSED(stopped);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", K(ret));
  } else if (tasks.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task cnt", K(ret));
  } else if (OB_FAIL(pre_check(tasks))) {
    LOG_WARN("pre_check failed", K(ret), K(tasks));
  } else if (tasks.at(0).need_process_alone() && 1 == tasks.count()) {
    /*
     * In the following cases, we won't renew location by batch.
     * 1. renew virtual table's location cache.
     * 2. renew __all_core_table's location cache.
     * 3. renew core table's location cache.
     * P.S: We already deal with compatibility in need_process_alone().
     */
    for (int64_t i = 0; i < tasks.count(); i++) {  // overwrite ret
      const ObLocationAsyncUpdateTask& task = tasks.at(i);
      const int64_t now = ObTimeUtility::current_time();
      bool force_sql_renew = ObLocationAsyncUpdateTask::MODE_SQL_ONLY == task.get_type();
      ObLocationUpdateTask tmp_task(*this,
          is_stopped_,
          task.get_table_id(),
          task.get_partition_id(),
          now,
          task.get_cluster_id(),
          force_sql_renew);
      if (OB_FAIL(tmp_task.process())) {
        LOG_WARN("fail to process task", K(ret), K(task));
      }
    }
  } else if (OB_FAIL(batch_renew_location(tasks))) {
    LOG_WARN("fail to batch renew location", K(ret), K(tasks));
  }
  return ret;
}

/*
 * defence, to make sure tasks in one batch meet the following conditions:
 * 1. renew tasks of __all_core_table or core table are not included.
 * 2. renew tasks in one bacth has same type and cluster_id.
 * 3. renew tasks of system table and virtual table and user table can't be processed in one batch.
 */
int ObPartitionLocationCache::pre_check(const common::ObIArray<ObLocationAsyncUpdateTask>& tasks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tasks.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task cnt", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); i++) {
      const ObLocationAsyncUpdateTask& cur_task = tasks.at(i);
      if (!cur_task.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid task", K(ret), K(cur_task));
      } else if (cur_task.need_discard()) {
        ret = OB_EAGAIN;
        LOG_WARN("cluster type changed, need discard", K(ret), K(cur_task));
      } else if (0 == i) {
        // do nothing
      } else {
        // check cluster_id & table_id & type_
        const ObLocationAsyncUpdateTask& pre_task = tasks.at(i - 1);
        uint64_t cur_table_id = cur_task.get_table_id();
        uint64_t pre_table_id = pre_task.get_table_id();
        if (cur_task.get_cluster_id() != pre_task.get_cluster_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected cluster_id", K(ret), K(cur_task), K(pre_task));
        } else if (cur_task.get_type() != cur_task.get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", K(ret), K(cur_task), K(pre_task));
        } else if (cur_task.need_process_alone()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task need process alone, unexpected table_id", K(ret), K(cur_task), K(pre_task));
        } else if (is_sys_table(cur_table_id) != is_sys_table(pre_table_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table_id", K(ret), K(cur_task), K(pre_task));
        } else if (!is_inner_table(cur_table_id) &&
                   extract_tenant_id(cur_table_id) != extract_tenant_id(pre_table_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table_id", K(ret), K(cur_task), K(pre_task));
        } else {
          // For now, we ensure renew task of __all_core_table(or core table) will process alone.
        }
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::batch_renew_location(const common::ObIArray<ObLocationAsyncUpdateTask>& tasks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tasks.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task cnt", K(ret));
  } else {
    const int64_t start = ObTimeUtility::current_time();
    const int64_t expire_renew_time = start - GCONF.location_cache_refresh_min_interval;
    int64_t succ_cnt = 0;
    int64_t wait_cost = 0;
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < tasks.count(); i++) {  // overwrite ret
        wait_cost += (start - tasks.at(i).get_add_timestamp());
      }
    }
    // Step 1 : try acquire lock
    bool sem_acquired = false;
    int64_t abs_timeout_us = -1;
    if (OB_SUCC(ret)) {
      // only limit user table fetch location concurrent requests,
      // if use table request count is limited, sys table request triggered by user table
      // request is limited naturally
      // we assume virtual table request is not numerous
      uint64_t table_id = tasks.at(0).get_table_id();
      if (!is_sys_table(table_id) && !is_virtual_table(table_id)) {
        const uint64_t wait_event_time_out = 0;
        const int64_t unused = 0;
        ObWaitEventGuard wait_guard(
            ObWaitEventIds::PT_LOCATION_CACHE_LOCK_WAIT, wait_event_time_out, unused, unused, unused);

        // ignore acquire fail
        abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_FETCH_LOCATION_TIMEOUT_US;
        int tmp_ret = sem_.acquire(abs_timeout_us);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("acquire failed", K(tmp_ret));
          if (OB_TIMEOUT == tmp_ret) {
            ret = OB_TIMEOUT;
            LOG_WARN("already timeout", K(ret), K(abs_timeout_us));
          }
        } else {
          sem_acquired = true;
        }
      }
    }

    // Step 2 : filter tasks
    ObLocationAsyncUpdateTask::Type renew_type = tasks.at(0).get_type();
    int64_t cluster_id = tasks.at(0).get_cluster_id();
    common::ObArenaAllocator allocator("RenewLocation");
    ObSEArray<ObPartitionLocation*, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> locations;
    if (cluster_id != cluster_id_) {  // renew location by SQL when fetch locations across clusters.
      renew_type = ObLocationAsyncUpdateTask::MODE_SQL_ONLY;
    }
    if (OB_SUCC(ret)) {
      bool force_sql_renew = ObLocationAsyncUpdateTask::MODE_SQL_ONLY == renew_type;
      const bool is_nonblock = false;
      const bool filter_not_readable_replica = false;
      ObPartitionLocation* location = NULL;
      for (int64_t i = 0; i < tasks.count(); i++) {  // overwrite ret
        const ObLocationAsyncUpdateTask& task = tasks.at(i);
        location = NULL;
        uint64_t table_id = task.get_table_id();
        int64_t partition_id = task.get_partition_id();
        bool need_renew = false;
        bool auto_update = false;
        if (OB_FAIL(ObPartitionLocation::alloc_new_location(allocator, location))) {
          LOG_WARN("fail to alloc location", K(ret), K(task));
        } else if (OB_FAIL(get_from_cache(table_id,
                       partition_id,
                       cluster_id,
                       is_nonblock,
                       *location,
                       filter_not_readable_replica,
                       auto_update))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("get_from_cache failed", K(table_id), K(partition_id), K(cluster_id), K(ret));
          } else {
            location->set_table_id(table_id);
            location->set_partition_id(partition_id);
            need_renew = true;
            ret = OB_SUCCESS;
          }
        } else if (!location->is_mark_fail() &&
                   ((!force_sql_renew && location->get_renew_time() > expire_renew_time) ||
                       (force_sql_renew && location->get_sql_renew_time() > expire_renew_time))) {
          need_renew = false;
          succ_cnt++;
          EVENT_INC(LOCATION_CACHE_RENEW_IGNORED);
        } else {
          need_renew = true;
        }
        if (OB_SUCC(ret) && need_renew) {
          if (OB_FAIL(locations.push_back(location))) {
            LOG_WARN("fail to push back task", K(ret), K(task));
          }
        }
      }
      ret = OB_SUCCESS;
    }

    // Step 3 : batch renew
    if (OB_SUCC(ret) && locations.count() > 0) {
      ObSEArray<ObPartitionLocation*, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> new_locations;
      common::ObTimeoutCtx ctx;
      if (OB_FAIL(set_batch_timeout_ctx(locations.count(), renew_type, ctx))) {
        LOG_WARN("fail to set time ctx", K(ret));
      } else if (ObLocationAsyncUpdateTask::MODE_SQL_ONLY == renew_type) {
        // renew location by sql
        ObSEArray<common::ObPartitionKey, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> keys;
        for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); i++) {
          ObPartitionLocation*& location = locations.at(i);
          if (OB_ISNULL(location)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("location is null", K(ret));
          } else {
            ObPartitionKey key(location->get_table_id(), location->get_partition_id(), 0);
            if (OB_FAIL(keys.push_back(key))) {
              LOG_WARN("fail to push back key", K(ret), K(key));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(location_fetcher_.batch_fetch_location(keys, cluster_id, allocator, new_locations))) {
          LOG_WARN("batch fetch location failed",
              K(ret),
              K(expire_renew_time),
              "task_cnt",
              locations.count(),
              "cost",
              ObTimeUtility::current_time() - start);
          // When renew location failed and location cache is empty,
          // try renew location cache of sys tenant's system tables by RPC.
          int tmp_ret = OB_SUCCESS;
          ObArray<ObLocationCacheKey> renew_keys;
          for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < locations.count(); i++) {
            ObPartitionLocation*& location = locations.at(i);
            if (OB_ISNULL(location)) {
              tmp_ret = OB_ERR_UNEXPECTED;
              LOG_WARN("location is null", K(tmp_ret));
            } else if (0 == location->get_replica_locations().count() &&
                       use_sys_leader_cache(location->get_table_id(), cluster_id)) {
              ObLocationCacheKey key(location->get_table_id(), location->get_partition_id(), cluster_id);
              if (OB_FAIL(renew_keys.push_back(key))) {
                LOG_WARN("fail to push back key", K(ret), K(key));
              }
            }
          }
          if (OB_SUCCESS != tmp_ret) {
          } else if (renew_keys.count() <= 0) {
            // skip
          } else if (OB_SUCCESS != (tmp_ret = batch_renew_sys_table_location_by_rpc(renew_keys))) {
            LOG_WARN("fail to batch renew sys table location by rpc", K(tmp_ret), "key_cnt", renew_keys.count());
          }
        } else {
          EVENT_INC(LOCATION_CACHE_SQL_RENEW);
        }
      } else {
        // renew location by rpc
        if (OB_FAIL(batch_renew_location_by_rpc(locations, allocator, new_locations))) {
          LOG_WARN("fail to batch renew location by rpc", K(ret));
        } else {
          LOG_INFO("location cache not hit, batch fetch location by rpc",
              K(ret),
              "location_cnt",
              locations.count(),
              "new_location_cnt",
              new_locations.count());
        }
      }

      // update cache
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < new_locations.count(); i++) {  // overwrite ret
          ObPartitionLocation*& new_location = new_locations.at(i);
          if (OB_ISNULL(new_location)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("location is null", K(ret), K(i));
          } else if (OB_FAIL(update_location(
                         new_location->get_table_id(), new_location->get_partition_id(), cluster_id, *new_location))) {
            LOG_WARN("fail to update location", K(ret), KPC(new_location));
          }
          if (OB_SUCC(ret)) {
            succ_cnt++;
            LOG_DEBUG("renew location succ", K(ret), K(renew_type), KPC(new_location));
          }
        }
        ret = OB_SUCCESS;
      }

      // always release locations and new_locations
      for (int64_t i = 0; i < locations.count(); i++) {
        ObPartitionLocation*& location = locations.at(i);
        if (OB_NOT_NULL(location)) {
          location->~ObPartitionLocation();
          location = NULL;
        }
      }
      for (int64_t i = 0; i < new_locations.count(); i++) {
        ObPartitionLocation*& location = new_locations.at(i);
        if (OB_NOT_NULL(location)) {
          location->~ObPartitionLocation();
          location = NULL;
        }
      }
    }

    // Step 4 : try release lock
    if (sem_acquired) {
      // ignore release fail
      int tmp_ret = sem_.release();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("release failed", K(tmp_ret));
      }
    }

    // Step 5 : calc statistic
    const int64_t end = ObTimeUtility::current_time();
    auto* statistics = GET_TSI(TSILocationCacheStatistics);
    if (OB_ISNULL(statistics)) {
      LOG_WARN("fail to get statistic", "ret", OB_ERR_UNEXPECTED);
    } else {
      bool sql_renew = (ObLocationAsyncUpdateTask::MODE_SQL_ONLY == renew_type);
      ObLocationCacheQueueSet::Type type =
          ObLocationCacheQueueSet::get_queue_type(tasks.at(0).get_table_id(), tasks.at(0).get_partition_id());
      (void)statistics->calc(type, sql_renew, succ_cnt, tasks.count() - succ_cnt, wait_cost, end - start);
      const int64_t interval = 1 * 1000 * 1000;  // 1s
      if (TC_REACH_TIME_INTERVAL(interval)) {
        (void)statistics->dump();
        (void)statistics->reset();
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::batch_renew_location_by_rpc(const common::ObIArray<ObPartitionLocation*>& locations,
    common::ObIAllocator& allocator, common::ObIArray<ObPartitionLocation*>& new_locations)
{
  int ret = OB_SUCCESS;
  if (locations.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid location cnt", K(ret));
  } else {
    ObSEArray<const ObPartitionLocation*, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> rpc_locations;
    // filter location should renew by sql
    for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); i++) {
      bool skip_rpc_renew = false;
      const ObPartitionLocation* location = locations.at(i);
      if (OB_ISNULL(location)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("location is null", K(ret));
      } else if (OB_FAIL(check_skip_rpc_renew_v2(*location, skip_rpc_renew))) {
        LOG_WARN("check skip rpc renew failed", K(ret), K(location));
        ret = OB_SUCCESS;
        skip_rpc_renew = true;
      }
      if (skip_rpc_renew) {
        EVENT_INC(LOCATION_CACHE_IGNORE_RPC_RENEW);
      }
      if (OB_SUCC(ret) && !skip_rpc_renew) {
        if (OB_FAIL(rpc_locations.push_back(location))) {
          LOG_WARN("push back location failed", K(ret), KPC(location));
        }
      }
    }
    // renew location by rpc
    if (OB_SUCC(ret)) {
      if (OB_FAIL(location_fetcher_.batch_renew_location_with_rpc(rpc_locations, allocator, new_locations))) {
        LOG_WARN("fail to batch renew location by rpc", K(ret), "partition_cnt", rpc_locations.count());
      }
    }
    // partition should renew location by sql
    if (OB_SUCC(ret) && locations.count() != new_locations.count()) {
      ObHashSet<ObPartitionKey> key_set;
      if (OB_FAIL(key_set.create(UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM))) {
        LOG_WARN("fail to create set", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < new_locations.count(); i++) {
        ObPartitionLocation*& new_location = new_locations.at(i);
        if (OB_ISNULL(new_location)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("location is null", K(ret));
        } else if (OB_FAIL(key_set.set_refactored(
                       ObPartitionKey(new_location->get_table_id(), new_location->get_partition_id(), 0)))) {
          LOG_WARN("fail to set hash set", K(ret), KPC(new_location));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); i++) {
        const ObPartitionLocation* location = locations.at(i);
        if (OB_ISNULL(location)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("location is null", K(ret));
        } else {
          ObPartitionKey key(location->get_table_id(), location->get_partition_id(), 0);
          int hash_ret = key_set.exist_refactored(key);
          if (OB_HASH_EXIST == hash_ret) {
            // do nothing
          } else if (OB_HASH_NOT_EXIST == hash_ret) {
            ObLocationAsyncUpdateTask task(*this,
                key.get_table_id(),
                key.get_partition_id(),
                ObTimeUtility::current_time(),
                cluster_id_,
                ObLocationAsyncUpdateTask::MODE_SQL_ONLY);
            int temp_ret = add_update_task(task);
            if (OB_SUCCESS != temp_ret) {
              LOG_WARN("add location update task failed", K(temp_ret), KPC(location), K(task));
            } else {
              /* Should renew location by SQL in the following cases:
               * 1.check_skip_rpc_renew_v2() shows we should skip renew location by rpc.
               * 2.rpc renew failed
               *   - 2.1. Call RPC on observer failed.
               *   - 2.2. Partial partitions' in one RPC return failure.
               *   - 2.3. Partial partitions' new locations not matched.(member_list or non paxos member maybe changed)
               */
              LOG_DEBUG("should renew location by sql, add_update_task success", K(temp_ret), K(task));
            }
          } else {
            ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : hash_ret;
            LOG_WARN("check key exist failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionLocationCache::process_barrier(const ObLocationAsyncUpdateTask& task, bool& stopped)
{
  UNUSEDx(task, stopped);
  return OB_NOT_SUPPORTED;
}

int ObPartitionLocationCache::set_batch_timeout_ctx(
    const int64_t task_cnt, ObLocationAsyncUpdateTask::Type type, common::ObTimeoutCtx& ctx)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout_us = ctx.get_abs_timeout();
  UNUSED(task_cnt);
  if (abs_timeout_us < 0) {
    if (ObLocationAsyncUpdateTask::MODE_SQL_ONLY == type) {
      abs_timeout_us = ObTimeUtility::current_time() + 2 * DEFAULT_FETCH_LOCATION_TIMEOUT_US;
    } else {
      abs_timeout_us = ObTimeUtility::current_time() + 2 * ObLocationFetcher::OB_FETCH_MEMBER_LIST_AND_LEADER_TIMEOUT;
    }
  }
  if (THIS_WORKER.get_timeout_ts() > 0 && THIS_WORKER.get_timeout_ts() < abs_timeout_us) {
    abs_timeout_us = THIS_WORKER.get_timeout_ts();
  }

  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret), K(abs_timeout_us));
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
        K(ret),
        "abs_timeout",
        ctx.get_abs_timeout(),
        "this worker timeout ts",
        THIS_WORKER.get_timeout_ts());
  }

  return ret;
}
/*-----batch async renew location end -----*/

bool ObPartitionLocationCache::use_sys_cache(const uint64_t table_id) const
{
  return OB_SYS_TENANT_ID == extract_tenant_id(table_id) && is_sys_table(table_id);
}

bool ObPartitionLocationCache::use_sys_leader_cache(const uint64_t table_id, const int64_t cluster_id) const
{
  return OB_SYS_TENANT_ID == extract_tenant_id(table_id) && OB_ALL_CORE_TABLE_TID != extract_pure_id(table_id) &&
         is_sys_table(table_id) && cluster_id == cluster_id_;
}

int ObPartitionLocationCache::batch_renew_sys_table_location_by_rpc(common::ObIArray<ObLocationCacheKey>& keys)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ctx.set_timeout(ObLocationFetcher::OB_FETCH_MEMBER_LIST_AND_LEADER_TIMEOUT * 2);
  ObPartitionLocation core_table_location;
  bool is_nonblock = false;
  bool filter_not_readable_replica = true;
  bool auto_update = false;
  ObArray<ObLocationLeader> results;
  if (OB_FAIL(get_from_cache(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
          ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
          cluster_id_,
          is_nonblock,
          core_table_location,
          filter_not_readable_replica,
          auto_update))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get from cache", K(ret));
    } else {
      ret = OB_SUCCESS;  // overwrite ret
    }
  } else if (OB_FAIL(location_fetcher_.batch_renew_sys_table_location_by_rpc(core_table_location, keys, results))) {
    LOG_WARN("fail to batch renew sys table location by rpc", K(ret), "keys_cnt", keys.count());
  } else {
    int overwrite_flag = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); i++) {
      const ObLocationCacheKey& key = results.at(i).get_key();
      const LocationInfo& leader_info = results.at(i).get_strong_leader_info();
      if (OB_FAIL(sys_leader_cache_.set_refactored(key, leader_info, overwrite_flag))) {
        LOG_WARN("fail to set sys table leader cache", K(ret), "keys_cnt", keys.count());
      } else {
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("renew sys table leader by rpc", K(ret), K(key), K(leader_info));
      }
    }
  }
  return ret;
}

int64_t ObPartitionLocationCache::get_primary_cluster_id() const
{
  int64_t cluster_id = common::OB_INVALID_ID;
  if (OB_NOT_NULL(remote_server_provider_)) {
    cluster_id = remote_server_provider_->get_primary_cluster_id();
  }
  return cluster_id;
}

}  // end namespace share
}  // end namespace oceanbase
