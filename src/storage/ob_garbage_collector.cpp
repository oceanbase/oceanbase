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

#include "ob_garbage_collector.h"
#include "common/ob_clock_generator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/thread/ob_thread_name.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_cluster_version.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_upgrade_utils.h"
#include "share/ob_multi_cluster_util.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_gc_partition_adapter.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_partition_service.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "storage/ob_file_system_util.h"
#include "rootserver/ob_rs_job_table_operator.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace storage {
class ObGarbageCollector::InsertPGFunctor {
public:
  explicit InsertPGFunctor(const common::ObPGKey& pg_key) : pg_key_(pg_key), ret_value_(common::OB_SUCCESS)
  {}
  ~InsertPGFunctor()
  {}

public:
  bool operator()(const common::ObAddr& leader, common::ObPartitionArray& pg_array)
  {
    if (OB_SUCCESS != (ret_value_ = pg_array.push_back(pg_key_))) {
      STORAGE_LOG(WARN, "pg_array push_back failed", K(ret_value_), K(pg_key_), K(leader));
    }
    return common::OB_SUCCESS == ret_value_;
  }
  int get_ret_value() const
  {
    return ret_value_;
  }

private:
  common::ObPGKey pg_key_;
  int ret_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(InsertPGFunctor);
};

class ObGarbageCollector::QueryPGIsValidMemberFunctor {
public:
  QueryPGIsValidMemberFunctor(obrpc::ObSrvRpcProxy* rpc_proxy, ObPartitionService* partition_service,
      const common::ObAddr& self_addr, const int64_t gc_seq, ObGCCandidateArray& gc_candidates)
      : rpc_proxy_(rpc_proxy),
        partition_service_(partition_service),
        self_addr_(self_addr),
        gc_seq_(gc_seq),
        gc_candidates_(gc_candidates),
        ret_value_(common::OB_SUCCESS)
  {}
  ~QueryPGIsValidMemberFunctor()
  {}

public:
  bool operator()(const common::ObAddr& leader, common::ObPartitionArray& pg_array)
  {
    if (OB_SUCCESS != (ret_value_ = handle_pg_array_(leader, pg_array))) {
      STORAGE_LOG(WARN, "handle_pg_array_ failed", K(ret_value_), K(pg_array), K(leader));
    }
    return common::OB_SUCCESS == ret_value_;
  }
  int get_ret_value() const
  {
    return ret_value_;
  }

private:
  int handle_pg_array_(const common::ObAddr& leader, const common::ObPartitionArray& pg_array);
  int handle_rpc_response_(const common::ObAddr& leader, const obrpc::ObQueryIsValidMemberResponse& response);
  bool is_normal_readonly_replica_(ObIPartitionGroup* pg) const;
  int try_renew_location_(const common::ObPartitionArray& pg_array);

private:
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObPartitionService* partition_service_;
  common::ObAddr self_addr_;
  int64_t gc_seq_;
  ObGCCandidateArray& gc_candidates_;
  int ret_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(QueryPGIsValidMemberFunctor);
};

class ObGarbageCollector::QueryPGFlushedIlogIDFunctor {
public:
  QueryPGFlushedIlogIDFunctor(
      obrpc::ObSrvRpcProxy* rpc_proxy, PGOfflineIlogFlushedInfoMap& pg_offline_ilog_flushed_info_map)
      : rpc_proxy_(rpc_proxy),
        pg_offline_ilog_flushed_info_map_(pg_offline_ilog_flushed_info_map),
        ret_value_(common::OB_SUCCESS)
  {}
  ~QueryPGFlushedIlogIDFunctor()
  {}

public:
  bool operator()(const common::ObAddr& leader, common::ObPartitionArray& pg_array)
  {
    if (OB_SUCCESS != (ret_value_ = handle_pg_array_(leader, pg_array))) {
      STORAGE_LOG(WARN, "handle_pg_array_ failed", K(ret_value_), K(pg_array));
    }
    return common::OB_SUCCESS == ret_value_;
  }
  int get_ret_value() const
  {
    return ret_value_;
  }

private:
  int handle_pg_array_(const common::ObAddr& leader, const common::ObPartitionArray& pg_array);
  int handle_rpc_response_(const common::ObAddr& server, const obrpc::ObQueryMaxFlushedILogIdResponse& response);

private:
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  PGOfflineIlogFlushedInfoMap& pg_offline_ilog_flushed_info_map_;
  int ret_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(QueryPGFlushedIlogIDFunctor);
};

class ObGarbageCollector::ExecuteSchemaDropFunctor {
public:
  ExecuteSchemaDropFunctor(common::ObMySQLProxy* sql_proxy, ObPartitionService* partition_service)
      : sql_proxy_(sql_proxy), partition_service_(partition_service), ret_value_(common::OB_SUCCESS)
  {}
  ~ExecuteSchemaDropFunctor()
  {}

public:
  bool operator()(const common::ObPGKey& pg_key, const PGOfflineIlogFlushedInfo& info)
  {
    if (OB_SUCCESS != (ret_value_ = handle_each_pg_(pg_key, info))) {
      STORAGE_LOG(WARN, "handle_each_pg_ failed", K(ret_value_), K(info));
    }
    return common::OB_SUCCESS == ret_value_;
  }
  int get_ret_value() const
  {
    return ret_value_;
  }

private:
  int handle_each_pg_(const common::ObPGKey& pg_key, const PGOfflineIlogFlushedInfo& info);
  int delete_tenant_gc_partition_info_(const common::ObPGKey& pg_key);

private:
  common::ObMySQLProxy* sql_proxy_;
  ObPartitionService* partition_service_;
  int ret_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(ExecuteSchemaDropFunctor);
};

ObGarbageCollector::ObGarbageCollector()
    : is_inited_(false),
      partition_service_(NULL),
      trans_service_(NULL),
      schema_service_(NULL),
      rpc_proxy_(NULL),
      sql_proxy_(NULL),
      self_addr_(),
      seq_(1)
{}

ObGarbageCollector::~ObGarbageCollector()
{
  destroy();
}

int ObGarbageCollector::init(ObPartitionService* partition_service, transaction::ObTransService* trans_service,
    share::schema::ObMultiVersionSchemaService* schema_service, obrpc::ObSrvRpcProxy* rpc_proxy,
    common::ObMySQLProxy* sql_proxy, const common::ObAddr& self_addr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObGarbageCollector is inited twice");
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(trans_service) || OB_ISNULL(schema_service) ||
             OB_ISNULL(rpc_proxy) || OB_ISNULL(sql_proxy) || !self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid arguments",
        K(ret),
        KP(partition_service),
        KP(trans_service),
        KP(schema_service),
        KP(rpc_proxy),
        KP(sql_proxy),
        K(self_addr));
  } else {
    partition_service_ = partition_service;
    trans_service_ = trans_service;
    schema_service_ = schema_service;
    rpc_proxy_ = rpc_proxy;
    sql_proxy_ = sql_proxy;
    self_addr_ = self_addr;
    seq_ = 1;
    is_inited_ = true;
  }
  STORAGE_LOG(INFO, "ObGarbageCollector is inited", K(ret), K(self_addr_));
  return ret;
}

int ObGarbageCollector::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObGarbageCollector is not inited", K(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    STORAGE_LOG(ERROR, "ObGarbageCollector thread failed to start", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

void ObGarbageCollector::stop()
{
  ObThreadPool::stop();
  STORAGE_LOG(INFO, "ObGarbageCollector stop");
}

void ObGarbageCollector::wait()
{
  ObThreadPool::wait();
  STORAGE_LOG(INFO, "ObGarbageCollector wait");
}

void ObGarbageCollector::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  partition_service_ = NULL;
  trans_service_ = NULL;
  schema_service_ = NULL;
  rpc_proxy_ = NULL;
  sql_proxy_ = NULL;
  self_addr_.reset();
}

void ObGarbageCollector::run1()
{
  STORAGE_LOG(INFO, "Garbage Collector start to run");
  lib::set_thread_name("GCCollector");

  while (!has_set_stop()) {
    DEBUG_SYNC(BLOCK_GARBAGE_COLLECTOR);

    int ret = OB_SUCCESS;
    TenantSet gc_tenant_set;
    ObGCCandidateArray gc_candidates;
    const int64_t gc_interval = GC_INTERVAL;
#ifdef ERRSIM
    gc_interval = std::min(gc_interval, (int64_t)ObServerConfig::get_instance().schema_drop_gc_delay_time);
#endif
    STORAGE_LOG(INFO, "Garbage Collector is running", K(seq_), K(gc_interval));
    if (OB_FAIL(gc_tenant_set.create(241))) {  // 241 means HASH_BUCKET_NUM
      STORAGE_LOG(ERROR, "gc_tenant_set create failed", K(ret));
    } else if (check_gc_condition_()) {
      gc_candidates.reset();
      (void)gc_check_member_list_(gc_candidates);
      (void)execute_gc_except_leader_schema_drop_(gc_candidates);

      gc_candidates.reset();
      (void)gc_check_schema_(gc_candidates, gc_tenant_set);
      (void)execute_gc_except_leader_schema_drop_(gc_candidates);
      (void)execute_gc_for_leader_schema_drop_(gc_candidates);

      (void)execute_gc_tenant_tmp_file_(gc_tenant_set);
    }
    usleep(gc_interval);
    seq_++;
  }

  return;
}

bool ObGarbageCollector::check_gc_condition_() const
{
  bool bool_ret = true;
  if (!schema_service_->is_sys_full_schema()) {
    bool_ret = false;
    STORAGE_LOG(INFO, "no full-updated schema, skip gc");
  } else if (!partition_service_->is_scan_disk_finished()) {
    bool_ret = false;
    STORAGE_LOG(INFO, "in rebooting, skip gc");
  } else {
    // do nothing
  }
  return bool_ret;
}

bool ObGarbageCollector::is_gc_reason_leader_schema_drop_(const NeedGCReason& gc_reason)
{
  return (LEADER_PENDDING_SCHEMA_DROP == gc_reason) || (LEADER_PHYSICAL_SCHEMA_DROP == gc_reason);
}

int ObGarbageCollector::execute_gc_except_leader_schema_drop_(const ObGCCandidateArray& gc_candidates)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t begin_time = ObTimeUtility::current_time();

  for (int64_t index = 0; OB_SUCC(ret) && index < gc_candidates.count(); index++) {
    const common::ObPGKey& pg_key = gc_candidates[index].pg_key_;
    const NeedGCReason& gc_reason = gc_candidates[index].gc_reason_;
    if (is_gc_reason_leader_schema_drop_(gc_reason)) {
      // skip it
    } else if (OB_SUCCESS != (tmp_ret = partition_service_->remove_partition(pg_key))) {
      STORAGE_LOG(WARN, "remove_partition failed", K(tmp_ret), K(pg_key));
    } else {
      STORAGE_LOG(INFO, "GC remove_partition success", K(tmp_ret), K(pg_key));
      SERVER_EVENT_ADD("gc", "clearup_replica", "partition_group", pg_key);
    }
  }

  STORAGE_LOG(INFO,
      "execute_gc_except_leader_schema_drop_ cost time",
      K(ret),
      K(seq_),
      "time",
      ObTimeUtility::current_time() - begin_time);

  return ret;
}

int ObGarbageCollector::execute_gc_for_leader_schema_drop_(const ObGCCandidateArray& gc_candidates)
{
  int ret = OB_SUCCESS;

  const int64_t begin_time = ObTimeUtility::current_time();

  ObGCCandidateArray leader_schema_drop_gc_candidates;
  if (OB_FAIL(extract_leader_schema_drop_gc_candidates_(gc_candidates, leader_schema_drop_gc_candidates))) {
    STORAGE_LOG(WARN, "extract_leader_schema_drop_gc_candidates_ failed", K(ret));
  } else if (OB_FAIL(handle_leader_schema_drop_gc_candidates_(leader_schema_drop_gc_candidates))) {
    STORAGE_LOG(WARN, "handle_leader_schema_drop_gc_candidates_ failed", K(ret));
  }

  STORAGE_LOG(INFO,
      "execute_gc_for_leader_schema_drop_ cost time",
      K(ret),
      K(seq_),
      "time",
      ObTimeUtility::current_time() - begin_time);

  return ret;
}

int ObGarbageCollector::extract_leader_schema_drop_gc_candidates_(
    const ObGCCandidateArray& gc_candidates, ObGCCandidateArray& leader_schema_drop_gc_candidates)
{
  int ret = OB_SUCCESS;

  for (int64_t index = 0; OB_SUCC(ret) && index < gc_candidates.count(); index++) {
    const GCCandidate& candidate = gc_candidates[index];
    if (is_gc_reason_leader_schema_drop_(candidate.gc_reason_) &&
        OB_FAIL(leader_schema_drop_gc_candidates.push_back(candidate))) {
      STORAGE_LOG(WARN, "leader_schema_drop_gc_candidates push_back failed", K(ret), K(candidate));
    }
  }

  return ret;
}

int ObGarbageCollector::handle_leader_schema_drop_gc_candidates_(const ObGCCandidateArray& gc_candidates)
{
  int ret = OB_SUCCESS;

  ServerPGMap server_pg_map;
  PGOfflineIlogFlushedInfoMap pg_offline_ilog_flushed_info_map;
  if (OB_FAIL(server_pg_map.init(ObModIds::OB_HASH_BUCKET_SERVER_PARTITION_MAP))) {
    STORAGE_LOG(WARN, "server_pg_map init failed", K(ret));
  } else if (OB_FAIL(pg_offline_ilog_flushed_info_map.init(
                 ObModIds::OB_HASH_BUCKET_SERVER_PARTITION_MAP, gc_candidates.count()))) {
    STORAGE_LOG(WARN, "pg_offline_ilog_flushed_info_map failed", K(ret));
  } else if (OB_FAIL(construct_server_pg_map_for_leader_schema_drop_(
                 gc_candidates, server_pg_map, pg_offline_ilog_flushed_info_map))) {
    STORAGE_LOG(WARN, "construct_server_pg_map_for_leader_schema_drop_ failed", K(ret));
  } else if (OB_FAIL(handle_each_pg_for_leader_schema_drop_(server_pg_map, pg_offline_ilog_flushed_info_map))) {
    STORAGE_LOG(WARN, "handle_each_pg_for_leader_schema_drop_ failed", K(ret));
  }

  return ret;
}

int ObGarbageCollector::construct_server_pg_map_for_leader_schema_drop_(const ObGCCandidateArray& gc_candidates,
    ServerPGMap& server_pg_map, PGOfflineIlogFlushedInfoMap& pg_offline_ilog_flushed_info_map)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  for (int64_t index = 0; OB_SUCC(ret) && index < gc_candidates.count(); index++) {
    storage::ObIPartitionGroupGuard guard;
    clog::ObIPartitionLogService* log_service = NULL;
    common::ObMemberList leader_member_list;
    int64_t replica_num = 0;
    common::ObAddr server;

    const common::ObPGKey& pg_key = gc_candidates[index].pg_key_;
    const NeedGCReason& gc_reason = gc_candidates[index].gc_reason_;

    PGOfflineIlogFlushedInfo ilog_flushed_info;
    ilog_flushed_info.offline_ilog_flushed_replica_num_ = 0;
    ilog_flushed_info.gc_reason_ = gc_reason;

    if (OB_SUCCESS != (tmp_ret = partition_service_->get_partition(pg_key, guard)) ||
        NULL == guard.get_partition_group() || NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      tmp_ret = OB_PARTITION_NOT_EXIST;
      STORAGE_LOG(WARN, "partition not exist", K(tmp_ret), K(pg_key));
    } else if (!guard.get_partition_group()->is_valid()) {
      tmp_ret = OB_INVALID_PARTITION;
      STORAGE_LOG(WARN, "invalid partition", K(tmp_ret), K(pg_key));
    } else if (OB_SUCCESS != (tmp_ret = log_service->get_leader_curr_member_list(leader_member_list))) {
      STORAGE_LOG(WARN, "get_leader_curr_member_list failed", K(tmp_ret), K(pg_key));
    } else if (OB_SUCCESS != (tmp_ret = log_service->get_replica_num(replica_num))) {
      STORAGE_LOG(WARN, "get_replica_num failed", K(tmp_ret), K(pg_key));
    } else if (OB_FALSE_IT(ilog_flushed_info.replica_num_ = replica_num)) {
      // skip it
    } else if (OB_FALSE_IT(ilog_flushed_info.offline_log_id_ = guard.get_partition_group()->get_offline_log_id())) {
      // skip it
    } else if (OB_FAIL(pg_offline_ilog_flushed_info_map.insert(pg_key, ilog_flushed_info))) {
      STORAGE_LOG(WARN, "pg_offline_ilog_flushed_info_map insert failed", K(ret), K(pg_key));
    } else {
      for (int64_t index = 0; OB_SUCC(ret) && index < leader_member_list.get_member_number(); index++) {
        if (OB_FAIL(leader_member_list.get_server_by_index(index, server))) {
          STORAGE_LOG(WARN, "leader_member_list get_server_by_index failed", K(ret), K(pg_key), K(leader_member_list));
        } else if (OB_FAIL(construct_server_pg_map_(server_pg_map, server, pg_key))) {
          STORAGE_LOG(WARN, "construct_server_pg_map_ failed", K(ret), K(pg_key), K(server));
        }
      }
    }
  }

  return ret;
}

int ObGarbageCollector::handle_each_pg_for_leader_schema_drop_(
    ServerPGMap& server_pg_map, PGOfflineIlogFlushedInfoMap& pg_offline_ilog_flushed_info_map)
{
  int ret = OB_SUCCESS;

  QueryPGFlushedIlogIDFunctor functor(rpc_proxy_, pg_offline_ilog_flushed_info_map);
  if (OB_SUCCESS != server_pg_map.for_each(functor)) {
    ret = functor.get_ret_value();
    STORAGE_LOG(WARN, "for_each server_pg_map failed", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(handle_pg_offline_ilog_flushed_info_map_(pg_offline_ilog_flushed_info_map))) {
    STORAGE_LOG(WARN, "handle_pg_offline_ilog_flushed_info_map_ failed", K(ret));
  }

  return ret;
}

int ObGarbageCollector::handle_pg_offline_ilog_flushed_info_map_(
    PGOfflineIlogFlushedInfoMap& pg_offline_ilog_flushed_info_map)
{
  int ret = OB_SUCCESS;

  ExecuteSchemaDropFunctor functor(sql_proxy_, partition_service_);
  if (OB_SUCCESS != pg_offline_ilog_flushed_info_map.for_each(functor)) {
    ret = functor.get_ret_value();
    STORAGE_LOG(WARN, "for_each pg_offline_ilog_flushed_info_map failed", K(ret));
  }

  return ret;
}

int ObGarbageCollector::execute_gc_tenant_tmp_file_(TenantSet& gc_tenant_set)
{
  int ret = OB_SUCCESS;

  const int64_t begin_time = ObTimeUtility::current_time();

  for (TenantSet::iterator it = gc_tenant_set.begin(); OB_SUCC(ret) && it != gc_tenant_set.end(); ++it) {
    const uint64_t tenant_id = it->first;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove_tenant_file(tenant_id))) {
      STORAGE_LOG(WARN, "remove tenant tmp file failed", K(ret), K(tenant_id));
    } else {
      STORAGE_LOG(INFO, "remove tenant tmp file success", K(ret), K(tenant_id));
    }
  }

  STORAGE_LOG(INFO,
      "execute_gc_tenant_tmp_file_ cost time",
      K(ret),
      K(seq_),
      "time",
      ObTimeUtility::current_time() - begin_time);

  return ret;
}

int ObGarbageCollector::gc_check_member_list_(ObGCCandidateArray& gc_candidates)
{
  int ret = OB_SUCCESS;

  const int64_t begin_time = ObTimeUtility::current_time();

  ServerPGMap server_pg_map;
  if (OB_FAIL(server_pg_map.init(ObModIds::OB_HASH_BUCKET_SERVER_PARTITION_MAP))) {
    STORAGE_LOG(WARN, "server_pg_map init failed", K(ret));
  } else if (OB_FAIL(construct_server_pg_map_for_member_list_(server_pg_map))) {
    STORAGE_LOG(WARN, "construct_server_pg_map_for_member_list_ failed", K(ret));
  } else if (OB_FAIL(handle_each_pg_for_member_list_(server_pg_map, gc_candidates))) {
    STORAGE_LOG(WARN, "handle_each_pg_for_member_list_ failed", K(ret));
  }

  STORAGE_LOG(
      INFO, "gc_check_member_list_ cost time", K(ret), K(seq_), "time", ObTimeUtility::current_time() - begin_time);

  return ret;
}

int ObGarbageCollector::construct_server_pg_map_for_member_list_(ServerPGMap& server_pg_map) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  storage::ObIPartitionGroupIterator* partition_iter = NULL;
  if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "partition_service alloc_pg_iter failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      storage::ObIPartitionGroup* pg = NULL;
      clog::ObIPartitionLogService* log_service = NULL;
      common::ObPGKey pg_key;
      common::ObAddr leader;
      bool allow_gc = false;

      if (OB_FAIL(partition_iter->get_next(pg))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "partition_iter->get_next failed", K(ret));
        }
      } else if (NULL == pg || NULL == (log_service = pg->get_log_service()) ||
                 OB_FALSE_IT(pg_key = pg->get_partition_key())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected error, pg is NULL", K(ret));
      } else if (OB_SUCCESS != (tmp_ret = pg->get_leader(leader))) {
        STORAGE_LOG(WARN, "get_leader failed", K(tmp_ret), K(pg_key));
      } else if (leader == self_addr_) {
        STORAGE_LOG(TRACE, "self is leader, skip it", K(pg_key));
      } else if (!leader.is_valid()) {
        STORAGE_LOG(TRACE, "leader is invalid, need renew location cache", K(pg_key));
        if (OB_SUCCESS != (tmp_ret = log_service->try_update_leader_from_loc_cache())) {
          STORAGE_LOG(WARN, "try_update_leader_from_loc_cache failed", K(tmp_ret), K(pg_key));
        }
      } else if (OB_SUCCESS != (tmp_ret = pg->allow_gc(allow_gc))) {
        STORAGE_LOG(WARN, "allow gc failed", K(tmp_ret), K(pg_key));
      } else if (!allow_gc) {
        STORAGE_LOG(INFO, "this pg is not allowed to gc", K(pg_key));
      } else if (OB_SUCCESS != (tmp_ret = construct_server_pg_map_(server_pg_map, leader, pg_key))) {
        STORAGE_LOG(WARN, "construct_server_pg_map_ failed", K(tmp_ret), K(pg_key), K(leader));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (NULL != partition_iter) {
    partition_service_->revert_pg_iter(partition_iter);
    partition_iter = NULL;
  }
  return ret;
}

int ObGarbageCollector::construct_server_pg_map_(
    ServerPGMap& server_pg_map, const common::ObAddr& server, const common::ObPGKey& pg_key) const
{
  int ret = OB_SUCCESS;

  InsertPGFunctor functor(pg_key);
  if (!server.is_valid() || !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(pg_key), K(server));
  } else if (OB_SUCCESS == (ret = server_pg_map.operate(server, functor))) {
    // do nothing
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    common::ObPartitionArray tmp_pg_array;
    if (OB_FAIL(server_pg_map.insert(server, tmp_pg_array))) {
      STORAGE_LOG(WARN, "server_pg_map insert failed", K(ret), K(pg_key), K(server));
    } else if (OB_SUCCESS != server_pg_map.operate(server, functor)) {
      ret = functor.get_ret_value();
      STORAGE_LOG(WARN, "insert pg functor operate failed", K(ret), K(pg_key), K(server));
    }
  } else {
    ret = functor.get_ret_value();
    STORAGE_LOG(WARN, "insert pg functor operate failed", K(ret), K(pg_key), K(server));
  }

  return ret;
}

int ObGarbageCollector::handle_each_pg_for_member_list_(ServerPGMap& server_pg_map, ObGCCandidateArray& gc_candidates)
{
  int ret = OB_SUCCESS;

  QueryPGIsValidMemberFunctor functor(rpc_proxy_, partition_service_, self_addr_, seq_, gc_candidates);
  if (OB_SUCCESS != server_pg_map.for_each(functor)) {
    ret = functor.get_ret_value();
    STORAGE_LOG(WARN, "for_each server_pg_map failed", K(ret));
  }

  return ret;
}

int ObGarbageCollector::QueryPGIsValidMemberFunctor::handle_pg_array_(
    const common::ObAddr& leader, const common::ObPartitionArray& pg_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t TIMEOUT = 10 * 1000 * 1000;
  const int64_t MAX_PARTITION_CNT = 10000;
  obrpc::ObQueryIsValidMemberRequest request;
  obrpc::ObQueryIsValidMemberResponse response;

  for (int64_t index = 0; OB_SUCC(ret) && index < pg_array.count(); index++) {
    request.self_addr_ = self_addr_;
    if (OB_FAIL(request.partition_array_.push_back(pg_array[index]))) {
      STORAGE_LOG(WARN, "request pg_array push_back failed", K(ret), K(leader));
    } else if ((index + 1) % MAX_PARTITION_CNT == 0 || index == pg_array.count() - 1) {
      if (OB_SUCCESS != (tmp_ret = rpc_proxy_->to(leader).timeout(TIMEOUT).query_is_valid_member(request, response)) ||
          (OB_SUCCESS != (tmp_ret = response.ret_value_))) {
        STORAGE_LOG(WARN, "query_is_valid_member failed", K(tmp_ret), K(leader));
        if (OB_SUCCESS != (tmp_ret = try_renew_location_(pg_array))) {
          STORAGE_LOG(WARN, "try_renew_location_ failed", K(tmp_ret), K(pg_array));
        }
      } else if (OB_SUCCESS != (tmp_ret = handle_rpc_response_(leader, response))) {
        STORAGE_LOG(WARN, "handle_rpc_response failed", K(ret), K(leader));
      } else {
        request.reset();
        response.reset();
      }
    }
  }

  return ret;
}

bool ObGarbageCollector::QueryPGIsValidMemberFunctor::is_normal_readonly_replica_(ObIPartitionGroup* pg) const
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  bool is_offline = false;
  const common::ObPGKey& pg_key = pg->get_partition_key();

  if (ObReplicaTypeCheck::is_readonly_replica(pg->get_replica_type())) {
    if (OB_SUCCESS != (tmp_ret = pg->is_replica_need_gc(is_offline))) {
      STORAGE_LOG(WARN, "is_replica_need_gc failed", K(tmp_ret), K(pg_key));
    } else if (!is_offline) {
      bool_ret = true;
    } else {
      STORAGE_LOG(INFO, "is_replica_need_gc return true", K(tmp_ret), K(pg_key));
    }
  }

  return bool_ret;
}

int ObGarbageCollector::QueryPGIsValidMemberFunctor::try_renew_location_(const common::ObPartitionArray& pg_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < pg_array.count(); index++) {
    const common::ObPGKey& pg_key = pg_array[index];
    storage::ObIPartitionGroupGuard guard;
    storage::ObIPartitionGroup* pg = NULL;
    clog::ObIPartitionLogService* log_service = NULL;

    if (OB_SUCCESS != (tmp_ret = partition_service_->get_partition(pg_key, guard))) {
      STORAGE_LOG(WARN, "get_partition failed", K(tmp_ret), K(pg_key));
    } else if (NULL == (pg = guard.get_partition_group()) || !pg->is_valid() ||
               OB_ISNULL(log_service = pg->get_log_service())) {
      STORAGE_LOG(WARN, "invalid pg", K(pg_key));
    } else if (OB_SUCCESS != (tmp_ret = log_service->try_update_leader_from_loc_cache())) {
      STORAGE_LOG(WARN, "try_update_leader_from_loc_cache failed", K(tmp_ret), K(pg_key));
    }
  }
  return ret;
}

int ObGarbageCollector::QueryPGIsValidMemberFunctor::handle_rpc_response_(
    const common::ObAddr& leader, const obrpc::ObQueryIsValidMemberResponse& response)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const common::ObPartitionArray& pg_array = response.partition_array_;
  const common::ObSEArray<bool, 16>& candidates_status = response.candidates_status_;
  const common::ObSEArray<int, 16>& ret_array = response.ret_array_;

  if (pg_array.count() != candidates_status.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "response count not match, unexpected", K(ret), K(leader));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < pg_array.count(); index++) {
      storage::ObIPartitionGroupGuard guard;
      storage::ObIPartitionGroup* pg = NULL;
      clog::ObIPartitionLogService* log_service = NULL;
      bool allow_gc = false;

      const common::ObPGKey& pg_key = pg_array[index];
      const bool is_valid_member = candidates_status[index];

      if (OB_SUCCESS != (tmp_ret = partition_service_->get_partition(pg_key, guard))) {
        STORAGE_LOG(WARN, "get_partition failed", K(tmp_ret), K(pg_key));
      } else if (NULL == (pg = guard.get_partition_group()) || !pg->is_valid() ||
                 OB_ISNULL(log_service = pg->get_log_service())) {
        STORAGE_LOG(WARN, "invalid pg", K(pg_key));
      } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100 && ret_array.count() == pg_array.count() &&
                 OB_SUCCESS != ret_array[index]) {
        STORAGE_LOG(INFO,
            "remote_ret_code is not success, need renew location",
            K(pg_key),
            K(leader),
            "remote_ret_code",
            ret_array[index]);
        if (OB_SUCCESS != (tmp_ret = log_service->try_update_leader_from_loc_cache())) {
          STORAGE_LOG(WARN, "try_update_leader_from_loc_cache failed", K(tmp_ret), K(pg_key), K(leader));
        }
      } else if (is_normal_readonly_replica_(pg)) {
        // do nothing
      } else if (OB_SUCCESS != (tmp_ret = pg->gc_check_valid_member(is_valid_member, gc_seq_, allow_gc))) {
        STORAGE_LOG(WARN,
            "gc_check_valid_member failed",
            K(tmp_ret),
            K(pg_key),
            K(leader),
            K(is_valid_member),
            K(gc_seq_),
            K(allow_gc));
      } else if (allow_gc) {
        GCCandidate candidate;
        candidate.pg_key_ = pg_key;
        candidate.gc_reason_ = NOT_IN_LEADER_MEMBER_LIST;

        if (OB_FAIL(gc_candidates_.push_back(candidate))) {
          STORAGE_LOG(WARN, "gc_candidates push_back failed", K(ret), K(pg_key), K(leader));
        } else {
          STORAGE_LOG(
              INFO, "gc_candidates push_back pg success", K(ret), K(pg_key), "gc_reason", NOT_IN_LEADER_MEMBER_LIST);
          SERVER_EVENT_ADD("gc", "gc_candidates", "partition_group", pg_key, "reason", "not in leader member list");
        }
      }
    }
  }

  return ret;
}

int ObGarbageCollector::QueryPGFlushedIlogIDFunctor::handle_pg_array_(
    const common::ObAddr& server, const common::ObPartitionArray& pg_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t TIMEOUT = 10 * 1000 * 1000;
  const int64_t MAX_PARTITION_CNT = 10000;
  obrpc::ObQueryMaxFlushedILogIdRequest request;
  obrpc::ObQueryMaxFlushedILogIdResponse response;

  for (int64_t index = 0; OB_SUCC(ret) && index < pg_array.count(); index++) {
    if (OB_FAIL(request.partition_array_.push_back(pg_array[index]))) {
      STORAGE_LOG(WARN, "request pg_array push_back failed", K(ret), K(server));
    } else if ((index + 1) % MAX_PARTITION_CNT == 0 || index == pg_array.count() - 1) {
      if (OB_SUCCESS !=
              (tmp_ret = rpc_proxy_->to(server).timeout(TIMEOUT).query_max_flushed_ilog_id(request, response)) ||
          OB_SUCCESS != (tmp_ret = response.err_code_)) {
        STORAGE_LOG(WARN, "query_max_flushed_ilog_id failed", K(tmp_ret), K(server));
      } else if (OB_SUCCESS != (tmp_ret = handle_rpc_response_(server, response))) {
        STORAGE_LOG(WARN, "handle_rpc_response_ failed", K(tmp_ret), K(server));
      } else {
        request.reset();
        response.reset();
      }
    }
  }

  return ret;
}

int ObGarbageCollector::QueryPGFlushedIlogIDFunctor::handle_rpc_response_(
    const common::ObAddr& server, const obrpc::ObQueryMaxFlushedILogIdResponse& response)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const common::ObPartitionArray& pg_array = response.partition_array_;
  const common::ObSEArray<uint64_t, 16>& max_flushed_ilog_ids = response.max_flushed_ilog_ids_;

  if (pg_array.count() != max_flushed_ilog_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "response count not match, unexpected", K(ret), K(server));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < pg_array.count(); index++) {
      const common::ObPGKey& pg_key = pg_array[index];
      const uint64_t max_flushed_ilog_id = max_flushed_ilog_ids[index];

      PGOfflineIlogFlushedInfo pg_offline_ilog_flushed_info;
      if (OB_SUCCESS != (tmp_ret = pg_offline_ilog_flushed_info_map_.get(pg_key, pg_offline_ilog_flushed_info))) {
        STORAGE_LOG(WARN, "pg_offline_ilog_flushed_info_map_ get failed", K(ret), K(pg_key), K(server));
      } else if (OB_INVALID_ID != max_flushed_ilog_id &&
                 max_flushed_ilog_id >= pg_offline_ilog_flushed_info.offline_log_id_) {
        pg_offline_ilog_flushed_info.offline_ilog_flushed_replica_num_++;

        if (OB_SUCCESS != (tmp_ret = pg_offline_ilog_flushed_info_map_.update(pg_key, pg_offline_ilog_flushed_info))) {
          STORAGE_LOG(WARN, "pg_offline_ilog_flushed_info_map_ update failed", K(ret), K(pg_key), K(server));
        }
      }
    }
  }

  return ret;
}

int ObGarbageCollector::ExecuteSchemaDropFunctor::handle_each_pg_(
    const common::ObPGKey& pg_key, const PGOfflineIlogFlushedInfo& info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (info.offline_ilog_flushed_replica_num_ < info.replica_num_ / 2 + 1) {
    STORAGE_LOG(INFO, "offline_ilog_flushed_replica_num is not majority, skip this pg", K(pg_key), K(info));
  } else if (OB_SUCCESS != (tmp_ret = delete_tenant_gc_partition_info_(pg_key))) {
    STORAGE_LOG(WARN, "delete_tenant_gc_partition_info_ failed", K(ret), K(pg_key), K(info));
  } else if (LEADER_PHYSICAL_SCHEMA_DROP == info.gc_reason_) {
    if (OB_SUCCESS != (tmp_ret = partition_service_->remove_partition(pg_key))) {
      STORAGE_LOG(WARN, "remove_partition failed", K(tmp_ret), K(pg_key), K(info));
    } else {
      STORAGE_LOG(INFO, "GC remove_partition success", K(tmp_ret), K(pg_key), K(info));
      SERVER_EVENT_ADD("gc", "clearup_replica", "partition_group", pg_key);
    }
  } else {
    STORAGE_LOG(INFO, "this pg is in pennding schema drop, wait to gc", K(ret), K(pg_key), K(info));
  }

  return ret;
}

int ObGarbageCollector::ExecuteSchemaDropFunctor::delete_tenant_gc_partition_info_(const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = pg_key.get_tenant_id();
  const uint64_t table_id = pg_key.get_table_id();
  const int64_t partition_id = pg_key.get_partition_id();

  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sql proxy is NULL", K(ret));
  } else if (OB_FAIL(sql.append_fmt("DELETE FROM %s WHERE (tenant_id = %lu and table_id = %lu and partition_id = %ld)",
                 OB_ALL_TENANT_GC_PARTITION_INFO_TNAME,
                 ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                 ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
                 partition_id))) {
    STORAGE_LOG(WARN, "append sql failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" or (tenant_id = %lu and table_id = %lu and partition_id = %ld)",
                 OB_INVALID_TENANT_ID,
                 extract_pure_id(table_id),
                 partition_id))) {
    STORAGE_LOG(WARN, "append sql failed", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
    STORAGE_LOG(WARN, "execute sql failed", K(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "affected_rows should be not greator than 1", K(ret), K(affected_rows));
  } else {
    // do nothing
  }

  STORAGE_LOG(INFO, "delete_tenant_gc_partition_info", K(ret), K(pg_key), K(sql));

  return ret;
}

int ObGarbageCollector::gc_check_schema_(ObGCCandidateArray& gc_candidates, TenantSet& gc_tenant_set)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();

  storage::ObIPartitionGroupIterator* partition_iter = NULL;
  ObSchemaGetterGuard schema_guard;

  if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "partition_service alloc_pg_iter failed", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    STORAGE_LOG(WARN, "fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    STORAGE_LOG(WARN, "schema_guard is not formal", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      storage::ObIPartitionGroup* pg = NULL;
      ObPGKey pg_key;
      NeedGCReason gc_reason = NO_NEED_TO_GC;
      bool allow_gc = false;
      if (OB_FAIL(partition_iter->get_next(pg))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "partition_iter->get_next failed", K(ret));
        }
      } else if (NULL == pg || OB_FALSE_IT(pg_key = pg->get_partition_key())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected error, pg is NULL", K(ret));
      } else if (OB_SUCCESS != (tmp_ret = gc_check_schema_(pg, schema_guard, gc_tenant_set, gc_reason))) {
        STORAGE_LOG(WARN, "gc_check_schema_ failed", K(tmp_ret), K(pg_key));
      } else if (NO_NEED_TO_GC == gc_reason) {
        // skip it
      } else if (OB_SUCCESS != (tmp_ret = pg->allow_gc(allow_gc))) {
        STORAGE_LOG(WARN, "allow gc failed", K(tmp_ret), K(pg_key));
      } else if (!allow_gc) {
        (void)pg->set_need_gc();
        STORAGE_LOG(INFO, "this pg is needed to gc, but is not allowed to gc, set need_gc flag", K(pg_key));
      } else {
        GCCandidate candidate;
        candidate.pg_key_ = pg_key;
        candidate.gc_reason_ = gc_reason;

        if (OB_FAIL(gc_candidates.push_back(candidate))) {
          STORAGE_LOG(WARN, "gc_candidates push_back failed", K(ret), K(pg_key));
        } else {
          STORAGE_LOG(INFO, "gc_candidates push_back pg success", K(ret), K(pg_key), K(gc_reason));
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (NULL != partition_iter) {
    partition_service_->revert_pg_iter(partition_iter);
    partition_iter = NULL;
  }

  STORAGE_LOG(INFO, "gc_check_schema_ cost time", K(ret), K(seq_), "time", ObTimeUtility::current_time() - begin_time);

  return ret;
}

int ObGarbageCollector::gc_check_schema_(storage::ObIPartitionGroup* pg,
    share::schema::ObSchemaGetterGuard& schema_guard, TenantSet& gc_tenant_set, NeedGCReason& gc_reason)
{
  int ret = OB_SUCCESS;

  const common::ObPGKey& pg_key = pg->get_partition_key();
  const uint64_t tenant_id = pg_key.get_tenant_id();
  const uint64_t tenant_id_for_get_schema =
      is_inner_table(pg_key.get_table_id()) ? OB_SYS_TENANT_ID : pg_key.get_tenant_id();
  int64_t local_schema_version = OB_INVALID_VERSION;
  int64_t pg_create_schema_version = OB_INVALID_VERSION;
  bool tenant_has_been_dropped = false;
  bool tenant_created_success = true;

  if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id, tenant_has_been_dropped))) {
    STORAGE_LOG(WARN, "fail to check if tenant has been dropped", K(ret), K(tenant_id));
  } else if (tenant_has_been_dropped) {
    if (OB_FAIL(gc_tenant_set.set_refactored(tenant_id, 1 /*overwrite*/))) {
      STORAGE_LOG(WARN, "gc_tenant_set set failed", K(ret), K(tenant_id));
    } else {
      gc_reason = TENANT_SCHEMA_DROP;
      SERVER_EVENT_ADD("gc", "gc_candidates", "partition_group", pg_key, "reason", "tenant schema drop");
    }
  } else if (OB_FAIL(pg->get_pg_storage().get_create_schema_version(pg_create_schema_version))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "fail to get create schema version for pg", K(ret), K(pg_key));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_for_get_schema, local_schema_version))) {
    STORAGE_LOG(WARN, "fail to get schema version", K(ret), K(pg_key), K(tenant_id_for_get_schema));
  } else if (pg_create_schema_version > local_schema_version ||
             !share::schema::ObSchemaService::is_formal_version(local_schema_version)) {
    STORAGE_LOG(INFO,
        "new partition group, schema is not flushed, skip it",
        K(pg_key),
        K(tenant_id_for_get_schema),
        K(local_schema_version),
        K(pg_create_schema_version));
  } else if (OB_FAIL(schema_guard.check_tenant_exist(tenant_id, tenant_created_success))) {
    STORAGE_LOG(WARN, "failed to check tenant exist", K(ret), K(pg_key), K(tenant_id));
  } else if (!tenant_created_success) {
    gc_reason = TENANT_FAIL_TO_CREATE;
    SERVER_EVENT_ADD("gc",
        "gc_candidates",
        "partition_group",
        pg_key,
        "reason",
        "tenant is failed to create",
        "local_schema_version",
        local_schema_version);
  } else if (OB_FAIL(gc_check_pg_schema_(pg, schema_guard, gc_reason))) {
    STORAGE_LOG(WARN, "gc_check_pg_schema_ failed", K(ret), K(pg_key));
  } else if (pg->is_pg()) {
    ObPartitionArray pkey_array;
    if (OB_FAIL(pg->get_all_pg_partition_keys(pkey_array))) {
      STORAGE_LOG(WARN, "get all pg partition keys failed", K(ret), K(pg_key));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pkey_array.count(); ++i) {
        const ObPartitionKey& partition_key = pkey_array.at(i);
        if (OB_FAIL(gc_check_partition_schema_(pg, schema_guard, partition_key))) {
          STORAGE_LOG(WARN, "gc_check_partition_schema_ failed", K(ret), K(pg_key), K(partition_key));
        }
      }
    }
  }

  return ret;
}

int ObGarbageCollector::gc_check_pg_schema_(
    storage::ObIPartitionGroup* pg, share::schema::ObSchemaGetterGuard& schema_guard, NeedGCReason& gc_reason)
{
  int ret = OB_SUCCESS;
  bool can_remove = false;
  bool is_physical_removed = false;
  const common::ObPGKey& pg_key = pg->get_partition_key();
  if (OB_FAIL(schema_guard.check_partition_can_remove(pg_key.get_table_id(),
          pg_key.get_partition_id(),
          false, /* delay dropped schema is not visble */
          can_remove))) {
    STORAGE_LOG(WARN, "check_partition_can_remove failed", K(ret), K(pg_key));
  } else if (!can_remove) {
    STORAGE_LOG(TRACE, "pg can not gc, skip it", K(ret), K(pg_key));
  } else if (OB_FAIL(schema_guard.check_partition_can_remove(pg_key.get_table_id(),
                 pg_key.get_partition_id(),
                 true, /* delay dropped schema is visble */
                 is_physical_removed))) {
    STORAGE_LOG(WARN, "check_partition_can_remove failed", K(ret), K(pg_key));
  } else if (OB_FAIL(handle_schema_drop_pg_(pg, is_physical_removed, gc_reason))) {
    STORAGE_LOG(WARN, "handle_schema_drop_pg_ failed", K(ret), K(pg_key));
  } else if (NO_NEED_TO_GC != gc_reason) {
    STORAGE_LOG(INFO, "this pg need to gc", K(pg_key), K(gc_reason));
  }

  return ret;
}

int ObGarbageCollector::handle_schema_drop_pg_(
    storage::ObIPartitionGroup* pg, const bool is_physical_removed, NeedGCReason& gc_reason)
{
  int ret = OB_SUCCESS;

  ObRole role;
  const common::ObPGKey& pg_key = pg->get_partition_key();
  if (OB_FAIL(pg->get_role(role))) {
    STORAGE_LOG(WARN, "fail to get_role", K(ret), K(pg_key));
  } else if (is_strong_leader(role)) {
    if (OB_FAIL(leader_handle_schema_drop_pg_(pg, is_physical_removed, gc_reason))) {
      STORAGE_LOG(WARN, "leader_handle_schema_drop_pg_ failed", K(ret), K(pg_key), K(is_physical_removed));
    }
  } else if (!is_physical_removed) {
    STORAGE_LOG(TRACE, "follower should gc after schema is physical removed", K(ret), K(pg_key));
  } else if (OB_FAIL(follower_handle_schema_drop_pg_(pg, gc_reason))) {
    STORAGE_LOG(WARN, "follower_handle_schema_drop_pg_ failed", K(ret), K(pg_key));
  }

  return ret;
}

int ObGarbageCollector::leader_handle_schema_drop_pg_(
    storage::ObIPartitionGroup* pg, const bool is_physical_removed, NeedGCReason& gc_reason)
{
  int ret = OB_SUCCESS;

  int64_t max_decided_trans_version = OB_INVALID_TIMESTAMP;
  bool is_all_trans_clear = false;
  const int64_t MAX_DECIDED_RESERVED_TIME = 20 * 1000 * 1000;
  int64_t gc_schema_drop_delay = GC_SCHEMA_DROP_DELAY;
  if (!GCONF.enable_one_phase_commit) {
    gc_schema_drop_delay = MAX_DECIDED_RESERVED_TIME;
  }
#ifdef ERRSIM
  gc_schema_drop_delay = ObServerConfig::get_instance().schema_drop_gc_delay_time;
#endif
  const common::ObPGKey& pg_key = pg->get_partition_key();

  if (OB_FAIL(partition_service_->get_global_max_decided_trans_version(max_decided_trans_version))) {
    STORAGE_LOG(ERROR, "get_global_max_decided_trans_version failed", K(ret));
  } else if (!pg->check_pg_partition_offline(pg_key)) {
    if (OB_FAIL(trans_service_->block_partition(pg_key, is_all_trans_clear))) {
      if (OB_PARTITION_NOT_EXIST != ret && OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "block_partition failed", K(ret), K(pg_key));
      }
    }

    if ((OB_SUCCESS == ret && is_all_trans_clear) || OB_PARTITION_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(pg->offline_itself(is_physical_removed))) {
        STORAGE_LOG(WARN, "offline itself failed", K(ret), K(pg_key));
      }
    } else if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      STORAGE_LOG(
          WARN, "trans_service_ block_partition failed, please attention", K(ret), K(pg_key), K(is_all_trans_clear));
    }
  } else if (ObTimeUtility::current_time() - pg->get_gc_schema_drop_ts() <= gc_schema_drop_delay &&
             pg->get_gc_schema_drop_ts() + MAX_DECIDED_RESERVED_TIME > max_decided_trans_version) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      STORAGE_LOG(INFO,
          "leader schema drop pg, wait to gc",
          K(pg_key),
          "gc_schema_drop_ts",
          pg->get_gc_schema_drop_ts(),
          K(max_decided_trans_version));
    }
  } else if (OB_FAIL(gc_check_log_archive_(pg, is_physical_removed, gc_reason))) {
    STORAGE_LOG(WARN, "gc_check_log_archive_ failed", K(ret), K(pg_key), K(is_physical_removed));
  } else {
    // do nothing
  }

  return ret;
}

int ObGarbageCollector::follower_handle_schema_drop_pg_(storage::ObIPartitionGroup* pg, NeedGCReason& gc_reason)
{
  int ret = OB_SUCCESS;

  bool exist = true;
  const common::ObPGKey& pg_key = pg->get_partition_key();
  if (OB_FAIL(GC_PARTITION_ADAPTER.check_partition_exist(pg_key, exist))) {
    STORAGE_LOG(WARN, "check_partition_exist failed", K(ret), K(pg_key));
  } else if (!exist) {
    gc_reason = FOLLOWER_SCHEMA_DROP;
    SERVER_EVENT_ADD("gc", "gc_candidates", "partition_group", pg_key, "reason", "follower pg schema drop");
  }

  STORAGE_LOG(INFO, "follower_handle_schema_drop_pg_", K(ret), K(pg_key), K(gc_reason));
  return ret;
}

int ObGarbageCollector::gc_check_log_archive_(
    storage::ObIPartitionGroup* pg, const bool is_physical_removed, NeedGCReason& gc_reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const common::ObPGKey& pg_key = pg->get_partition_key();
  const bool enable_log_archive = ObServerConfig::get_instance().enable_log_archive;
  const bool is_mandatory = ObServerConfig::get_instance().backup_log_archive_option.is_mandatory();
  const bool is_restore = pg->get_pg_storage().is_restore();
  const bool is_sys_tenant = (OB_SYS_TENANT_ID == pg_key.get_tenant_id());
  ObLogArchiveBackupInfo backup_info;
  bool has_archived = false;
  bool need_gc = false;

  int64_t delay_interval = LOG_ARCHIVE_DROP_DELAY;
#ifdef ERRSIM
  delay_interval = std::min(LOG_ARCHIVE_DROP_DELAY, (int64_t)ObServerConfig::get_instance().schema_drop_gc_delay_time);
#endif

  if (enable_log_archive && !is_restore && !is_sys_tenant) {
    if (OB_SUCCESS != (tmp_ret = ObBackupInfoMgr::get_instance().get_log_archive_backup_info(backup_info))) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        STORAGE_LOG(INFO,
            "leader schema drop pg, wait to gc because of get_log_archive_backup_info failed",
            K(tmp_ret),
            K(pg_key));
      }
    } else if (ObLogArchiveStatus::STATUS::BEGINNING != backup_info.status_.status_ &&
               ObLogArchiveStatus::STATUS::DOING != backup_info.status_.status_) {
      need_gc = true;
    } else if (OB_SUCCESS ==
                   (tmp_ret = pg->check_offline_log_archived(
                        pg_key, backup_info.status_.incarnation_, backup_info.status_.round_, has_archived)) &&
               has_archived) {
      need_gc = true;
    } else if (is_mandatory) {
      STORAGE_LOG(INFO,
          "leader schema drop pg, wait to gc because offline log has not been archived in mandatory mode",
          K(pg_key));
    } else if (ObTimeUtility::current_time() - pg->get_gc_schema_drop_ts() <= delay_interval) {
      STORAGE_LOG(INFO,
          "leader schema drop pg, wait to gc because offline log has not been archived in optional mode",
          K(pg_key),
          K(delay_interval),
          "gc_schema_drop_ts",
          pg->get_gc_schema_drop_ts());
    } else if (OB_SUCCESS != (tmp_ret = partition_service_->mark_log_archive_encount_fatal_error(
                                  pg_key, backup_info.status_.incarnation_, backup_info.status_.round_))) {
      STORAGE_LOG(INFO,
          "leader schema drop pg, wait to mark_log_archive_interrupted in optional mode",
          K(tmp_ret),
          K(pg_key),
          K(backup_info));
    } else {
      STORAGE_LOG(ERROR,
          "LOG_ARCHIVE: pg is forced to gc when offline log has not been archived in optional mode",
          K(pg_key),
          K(backup_info));
    }
  } else {
    need_gc = true;
  }

  if (need_gc && is_physical_removed) {
    gc_reason = LEADER_PHYSICAL_SCHEMA_DROP;
    SERVER_EVENT_ADD("gc", "gc_candidates", "partition_group", pg_key, "reason", "leader physical schema drop");
  } else if (need_gc) {
    gc_reason = LEADER_PENDDING_SCHEMA_DROP;
    SERVER_EVENT_ADD("gc", "gc_candidates", "partition_group", pg_key, "reason", "leader pendding schema drop");
  }

  return ret;
}

int ObGarbageCollector::gc_check_partition_schema_(storage::ObIPartitionGroup* pg,
    share::schema::ObSchemaGetterGuard& schema_guard, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id_for_get_schema =
      is_inner_table(partition_key.get_table_id()) ? OB_SYS_TENANT_ID : partition_key.get_tenant_id();
  int64_t local_schema_version = OB_INVALID_VERSION;
  int64_t partition_create_schema_version = OB_INVALID_VERSION;
  const common::ObPGKey& pg_key = pg->get_partition_key();

  bool can_remove = false;
  bool is_physical_removed = false;

  if (OB_FAIL(pg->get_pg_storage().get_create_schema_version(partition_key, partition_create_schema_version))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "fail to get create schema version for partition", K(ret), K(partition_key));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_for_get_schema, local_schema_version))) {
    STORAGE_LOG(WARN, "fail to get schema version", K(ret), K(pg_key), K(partition_key), K(tenant_id_for_get_schema));
  } else if (partition_create_schema_version > local_schema_version ||
             !share::schema::ObSchemaService::is_formal_version(local_schema_version)) {
    STORAGE_LOG(INFO,
        "new partition, schema is not flushed, skip it",
        K(pg_key),
        K(partition_key),
        K(tenant_id_for_get_schema),
        K(local_schema_version),
        K(partition_create_schema_version));
  } else if (OB_FAIL(schema_guard.check_partition_can_remove(partition_key.get_table_id(),
                 partition_key.get_partition_id(),
                 false, /* delay dropped schema is not visble */
                 can_remove))) {
    STORAGE_LOG(WARN, "check_partition_can_remove failed", K(ret), K(pg_key), K(partition_key));
  } else if (!can_remove) {
    STORAGE_LOG(TRACE, "partition can not gc, skip it", K(ret), K(pg_key), K(partition_key));
  } else if (OB_FAIL(schema_guard.check_partition_can_remove(partition_key.get_table_id(),
                 partition_key.get_partition_id(),
                 true, /* delay dropped schema is visble */
                 is_physical_removed))) {
    STORAGE_LOG(WARN, "check_partition_can_remove failed", K(ret), K(pg_key), K(partition_key));
  } else if (is_physical_removed && OB_FAIL(handle_schema_drop_partition_(pg, partition_key))) {
    STORAGE_LOG(WARN, "handle_schema_drop_partition_ failed", K(ret), K(pg_key), K(partition_key));
  } else {
    // do nothing
  }

  return ret;
}

int ObGarbageCollector::handle_schema_drop_partition_(
    storage::ObIPartitionGroup* pg, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;

  ObRole role;
  const common::ObPGKey& pg_key = pg->get_partition_key();
  if (OB_FAIL(pg->get_role(role))) {
    STORAGE_LOG(WARN, "fail to get role", K(ret), K(pg_key), K(partition_key));
  } else if (is_strong_leader(role)) {
    if (OB_FAIL(leader_handle_schema_drop_partition_(pg, partition_key))) {
      STORAGE_LOG(WARN, "leader_handle_schema_drop_partition_ failed", K(ret), K(pg_key), K(partition_key));
    }
  } else if (OB_FAIL(follower_handle_schema_drop_partition_(pg, partition_key))) {
    STORAGE_LOG(WARN, "follower_handle_schema_drop_partition_ failed", K(ret), K(pg_key), K(partition_key));
  }

  return ret;
}

int ObGarbageCollector::leader_handle_schema_drop_partition_(
    storage::ObIPartitionGroup* pg, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;

  int64_t gc_start_ts = 0;
  ObPGPartitionGuard pg_partition_guard;
  const common::ObPGKey& pg_key = pg->get_partition_key();
  bool is_all_trans_clear = false;
  const uint64_t UNUSED = 100;
  const bool for_replay = false;

  if (OB_FAIL(pg->get_pg_partition(partition_key, pg_partition_guard))) {
    STORAGE_LOG(WARN, "get pg partition failed", K(ret), K(pg_key), K(partition_key));
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg partition is null", K(ret), K(pg_key), K(partition_key));
  } else if (OB_FAIL(pg_partition_guard.get_pg_partition()->set_gc_starting())) {
    STORAGE_LOG(WARN, "pg partition set_gc_starting failed", K(ret), K(pg_key), K(partition_key));
  } else if (OB_FAIL(pg_partition_guard.get_pg_partition()->get_gc_start_ts(gc_start_ts))) {
    STORAGE_LOG(WARN, "get gc start ts failed", K(ret), K(pg_key), K(partition_key));
  } else if (gc_start_ts <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected gc start ts", K(ret), K(pg_key), K(partition_key), K(gc_start_ts));
  } else if (OB_FAIL(trans_service_->check_ctx_create_timestamp_elapsed(pg_key, gc_start_ts))) {
    if (OB_PARTITION_NOT_EXIST != ret && OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "check_ctx_create_timestamp_elapsed failed", K(ret), K(pg_key), K(partition_key));
    }
  } else {
    is_all_trans_clear = true;
  }

  if ((OB_SUCCESS == ret && is_all_trans_clear) || OB_PARTITION_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
    if (OB_FAIL(partition_service_->remove_partition_from_pg(for_replay, pg_key, partition_key, UNUSED))) {
      STORAGE_LOG(WARN, "follower remove_partition_from_pg failed", K(ret), K(pg_key), K(partition_key));
    } else {
      SERVER_EVENT_ADD("gc", "clearup_partition", "partition", partition_key, "reason", "partition leader schema drop");
    }
  }

  return ret;
}

int ObGarbageCollector::follower_handle_schema_drop_partition_(
    storage::ObIPartitionGroup* pg, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;

  const uint64_t UNUSED = 100;
  const bool for_replay = true;
  const common::ObPGKey& pg_key = pg->get_partition_key();
  if (OB_FAIL(partition_service_->remove_partition_from_pg(for_replay, pg_key, partition_key, UNUSED))) {
    STORAGE_LOG(WARN, "follower remove_partition_from_pg failed", K(ret), K(pg_key), K(partition_key));
  } else {
    SERVER_EVENT_ADD("gc", "clearup_partition", "partition", partition_key, "reason", "partition follower schema drop");
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
