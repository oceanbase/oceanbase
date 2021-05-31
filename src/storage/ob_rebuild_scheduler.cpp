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
#include "ob_rebuild_scheduler.h"

#include "lib/allocator/ob_mod_define.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_storage.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_pg_storage.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace storage {

ObRebuildReplicaInfo::ObRebuildReplicaInfo()
    : pg_key_(),
      replica_type_(ObReplicaType::REPLICA_TYPE_MAX),
      replica_property_(),
      parent_addr_(),
      parent_replica_type_(ObReplicaType::REPLICA_TYPE_MAX),
      local_minor_snapshot_version_(0),
      local_major_snapshot_version_(0),
      remote_minor_snapshot_version_(0),
      remote_major_snapshot_version_(0),
      rebuild_action_(ObRebuildReplicaInfo::PREPARE),
      local_last_replay_log_id_(0),
      remote_last_replay_log_id_(0)
{}

bool ObRebuildReplicaInfo::is_valid() const
{
  return pg_key_.is_valid() && ObReplicaTypeCheck::is_replica_type_valid(replica_type_) &&
         replica_property_.is_valid() && parent_addr_.is_valid() && local_minor_snapshot_version_ >= 0 &&
         local_major_snapshot_version_ >= 0 && remote_minor_snapshot_version_ >= 0 &&
         remote_major_snapshot_version_ >= 0 && local_last_replay_log_id_ >= 0 && remote_last_replay_log_id_ >= 0;
  // parent_replica_type
}

void ObRebuildReplicaInfo::reset()
{
  pg_key_.reset();
  replica_type_ = ObReplicaType::REPLICA_TYPE_MAX;
  replica_property_.reset();
  parent_addr_.reset();
  local_minor_snapshot_version_ = 0;
  local_major_snapshot_version_ = 0;
  remote_minor_snapshot_version_ = 0;
  remote_major_snapshot_version_ = 0;
  is_share_major_ = false;
  rebuild_action_ = ObRebuildReplicaInfo::PREPARE;
  local_last_replay_log_id_ = 0;
  remote_last_replay_log_id_ = 0;
}

ObRebuildReplicaResult::ObRebuildReplicaResult() : results_(), pg_keys_()
{}

bool ObRebuildReplicaResult::is_valid() const
{
  return results_.count() == pg_keys_.count();
}

void ObRebuildReplicaResult::reset()
{
  results_.reset();
  pg_keys_.reset();
}

int ObRebuildReplicaResult::set_result(const ObPGKey& pg_key, const int32_t result)
{
  int ret = OB_SUCCESS;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "set result get invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(pg_keys_.push_back(pg_key))) {
    STORAGE_LOG(WARN, "failed to push pg key into array", K(ret), K(pg_key));
  } else if (OB_FAIL(results_.push_back(result))) {
    STORAGE_LOG(WARN, "failed to push result into array", K(ret), K(result), K(pg_key));
  }
  return ret;
}

ObRebuildReplicaTaskProducer::ObRebuildReplicaConvergeInfo::ObRebuildReplicaConvergeInfo()
    : replica_info_array_(), parent_addr_()
{}

int ObRebuildReplicaTaskProducer::ObRebuildReplicaConvergeInfo::assign(const ObRebuildReplicaConvergeInfo& task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replica_info_array_.assign(task.replica_info_array_))) {
    STORAGE_LOG(WARN, "failed to assign d replica info", K(ret));
  } else {
    parent_addr_ = task.parent_addr_;
  }
  return ret;
}

int ObRebuildReplicaTaskProducer::ObRebuildReplicaConvergeInfo::add_replica_info(
    const ObRebuildReplicaInfo& replica_info)
{
  int ret = OB_SUCCESS;
  if (!replica_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "add replica info get invalid argument", K(ret), K(replica_info));
  } else if (OB_FAIL(replica_info_array_.push_back(replica_info))) {
    STORAGE_LOG(WARN, "failed to push replica info into array", K(ret), K(replica_info));
  }
  return ret;
}

bool ObRebuildReplicaTaskProducer::ObRebuildReplicaConvergeInfo::is_valid() const
{
  return parent_addr_.is_valid() && replica_info_array_.count() > 0;
}

ObRebuildReplicaTaskProducer::ObRebuildReplicaTaskProducer(ObRebuildReplicaTaskScheduler& scheduler)
    : is_inited_(false),
      stopped_(false),
      partition_service_(NULL),
      srv_rpc_proxy_(NULL),
      rebuild_replica_service_(NULL),
      converge_info_array_(),
      scheduler_(scheduler)
{}

ObRebuildReplicaTaskProducer::~ObRebuildReplicaTaskProducer()
{
  destroy();
}

void ObRebuildReplicaTaskProducer::destroy()
{
  if (is_inited_) {
    converge_info_array_.destroy();
    is_inited_ = false;
  }
}

int ObRebuildReplicaTaskProducer::init(ObPartitionService* partition_service,
    obrpc::ObPartitionServiceRpcProxy* srv_rpc_proxy, ObRebuildReplicaService* rebuild_replica_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "rebuild replica task producer init twice", K(ret));
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(srv_rpc_proxy) || OB_ISNULL(rebuild_replica_service)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "rebuild replica task producer init get invalid argument",
        K(ret),
        KP(partition_service),
        KP(rebuild_replica_service),
        KP(srv_rpc_proxy));
  } else if (OB_FAIL(converge_info_array_.reserve(common::OB_MAX_PARTITION_NUM_PER_SERVER))) {
    STORAGE_LOG(WARN, "failed to reserve d replica info array", K(ret));
  } else {
    partition_service_ = partition_service;
    srv_rpc_proxy_ = srv_rpc_proxy;
    rebuild_replica_service_ = rebuild_replica_service;
    is_inited_ = true;
  }
  return ret;
}

void ObRebuildReplicaTaskProducer::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task producer do not init", K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "rebuild task producer is stopped", K(ret));
  } else {
    if (OB_FAIL(generate_replica_info())) {
      STORAGE_LOG(WARN, "failed to generate replica info", K(ret));
    } else if (OB_FAIL(add_replica_info())) {
      STORAGE_LOG(WARN, "failed to add replica info", K(ret));
    }
  }
}

int ObRebuildReplicaTaskProducer::generate_replica_info()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task producer do not init", K(ret));
  } else if (OB_FAIL(build_local_replica_info())) {
    STORAGE_LOG(WARN, "failed to build replica info", K(ret));
  } else if (OB_FAIL(get_remote_replica_info())) {
    STORAGE_LOG(WARN, "failed to get remote publish info", K(ret));
  }
  return ret;
}

int ObRebuildReplicaTaskProducer::build_local_replica_info()
{
  int ret = OB_SUCCESS;
  ObIPartitionArrayGuard partitions;
  ObPartitionGroupMeta pg_meta;
  ObRebuildReplicaInfo replica_info;
  ObAddr parent_addr;
  int64_t parent_cluster_id;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task producer do not init", K(ret));
  } else if (OB_FAIL(partition_service_->get_all_partitions(partitions))) {
    STORAGE_LOG(WARN, "failed to get all partitions guard", K(ret));
  } else {
    converge_info_array_.reuse();
    // Ignore the wrong partittion, continue to execute
    for (int64_t i = 0; i < partitions.count(); ++i) {
      ObIPartitionGroup* partition = partitions.at(i);
      pg_meta.reset();
      replica_info.reset();
      parent_addr.reset();
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition is NULL", K(ret), K(i));
      } else if (!partition->is_replica_using_remote_memstore() && !partition->is_share_major_in_zone()) {
        // skip
      } else if (OB_FAIL(partition->get_pg_storage().get_pg_meta(pg_meta))) {
        STORAGE_LOG(WARN, "failed to get pg meta", K(ret), K(partition->get_partition_key()));
      } else if (OB_FAIL(partition->get_log_service()->get_clog_parent(parent_addr, parent_cluster_id))) {
        STORAGE_LOG(WARN, "get parent addr failed", K(ret));
      } else {
        // now only consider memstore pecent 0 is D replica, and read-only replica in ofs mode.
        replica_info.pg_key_ = pg_meta.pg_key_;
        replica_info.parent_addr_ = parent_addr;
        replica_info.replica_property_ = pg_meta.replica_property_;
        replica_info.replica_type_ = pg_meta.replica_type_;
        replica_info.local_minor_snapshot_version_ = pg_meta.storage_info_.get_data_info().get_publish_version();
        replica_info.local_major_snapshot_version_ = pg_meta.report_status_.snapshot_version_;
        replica_info.is_share_major_ = partition->is_share_major_in_zone();
        replica_info.local_last_replay_log_id_ = pg_meta.storage_info_.get_data_info().get_last_replay_log_id();
        if (OB_FAIL(set_replica_info(replica_info))) {
          STORAGE_LOG(WARN, "failed to set replica info", K(ret), K(replica_info));
        }
      }
    }
  }
  return ret;
}

int ObRebuildReplicaTaskProducer::set_replica_info(const ObRebuildReplicaInfo& replica_info)
{
  int ret = OB_SUCCESS;
  ObRebuildReplicaConvergeInfo* converge_info = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task procuder do not init", K(ret));
  } else if (!replica_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "set replica info get invalid argument", K(ret), K(replica_info));
  } else {
    bool found = false;
    for (int64_t i = 0; i < converge_info_array_.count() && !found; ++i) {
      found = (replica_info.parent_addr_ == converge_info_array_.at(i).parent_addr_);
      if (found) {
        converge_info = &converge_info_array_.at(i);
      }
    }

    if (found) {
      if (OB_ISNULL(converge_info)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "converge info should not be NULL", K(ret), KP(converge_info));
      } else if (OB_FAIL(converge_info->add_replica_info(replica_info))) {
        STORAGE_LOG(WARN, "failed to add replica info into array", K(ret), K(replica_info));
      }
    } else {
      ObRebuildReplicaConvergeInfo tmp_converge_info;
      tmp_converge_info.parent_addr_ = replica_info.parent_addr_;
      if (OB_FAIL(tmp_converge_info.add_replica_info(replica_info))) {
        STORAGE_LOG(WARN, "failed to add replica info", K(ret), K(replica_info));
      } else if (OB_FAIL(converge_info_array_.push_back(tmp_converge_info))) {
        STORAGE_LOG(WARN, "failed to push converge info into array", K(ret));
      }
    }
  }
  return ret;
}

int ObRebuildReplicaTaskProducer::get_remote_replica_info()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task producer do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < converge_info_array_.count(); ++i) {
      ObRebuildReplicaConvergeInfo& converge_info = converge_info_array_.at(i);
      if (OB_FAIL(inner_get_remote_replica_info(converge_info))) {
        STORAGE_LOG(WARN, "failed to get remote publish info", K(ret), K(converge_info));
      }
    }
  }
  return ret;
}

int ObRebuildReplicaTaskProducer::inner_get_remote_replica_info(ObRebuildReplicaConvergeInfo& converge_info)
{
  int ret = OB_SUCCESS;
  obrpc::ObBatchFetchReplicaInfoArg batch_arg;
  obrpc::ObBatchFetchReplicaInfoRes batch_res;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task producer do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < converge_info.replica_info_array_.count(); ++i) {
      obrpc::ObFetchReplicaInfoArg arg;
      ObRebuildReplicaInfo& replica_info = converge_info.replica_info_array_.at(i);
      arg.pg_key_ = replica_info.pg_key_;
      arg.local_publish_version_ = replica_info.local_minor_snapshot_version_;
      if (OB_FAIL(batch_arg.replica_info_arg_.push_back(arg))) {
        STORAGE_LOG(WARN, "failed to push arg into array", K(ret), K(arg));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(srv_rpc_proxy_->to(converge_info.parent_addr_).fetch_replica_info(batch_arg, batch_res))) {
      STORAGE_LOG(WARN, "failed to fetch publish version", K(ret), K(converge_info));
    } else if (converge_info.replica_info_array_.count() != batch_res.replica_info_res_.count()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR,
          "converge info not eqaul remote result",
          K(ret),
          "converge info count",
          converge_info.replica_info_array_.count(),
          "result count",
          batch_res.replica_info_res_.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_res.replica_info_res_.count(); ++i) {
        ObRebuildReplicaInfo& replica_info = converge_info.replica_info_array_.at(i);
        obrpc::ObFetchReplicaInfoRes& res = batch_res.replica_info_res_.at(i);
        if (replica_info.pg_key_ != res.pg_key_) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(WARN, "pg key not equal", K(ret), K(replica_info), K(res));
        } else {
          replica_info.remote_minor_snapshot_version_ = res.remote_minor_snapshot_version_;
          replica_info.remote_major_snapshot_version_ = res.remote_major_snapshot_version_;
          replica_info.parent_replica_type_ = res.remote_replica_type_;
          replica_info.remote_last_replay_log_id_ = res.remote_last_replay_log_id_;
        }
      }
      STORAGE_LOG(INFO, "get replica info", K(converge_info.replica_info_array_.count()), K(converge_info));
    }
  }
  return ret;
}

int ObRebuildReplicaTaskProducer::add_replica_info()
{
  int ret = OB_SUCCESS;
  ObArray<ObRebuildReplicaInfo> replica_info_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task producer do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < converge_info_array_.count(); ++i) {
      ObRebuildReplicaConvergeInfo& converge_info = converge_info_array_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < converge_info.replica_info_array_.count(); ++j) {
        ObRebuildReplicaInfo& replica_info = converge_info.replica_info_array_.at(j);
        if (replica_info.is_share_major_) {
          if (replica_info.local_major_snapshot_version_ >= replica_info.remote_major_snapshot_version_) {
            // skip
          } else if (OB_FAIL(replica_info_array.push_back(replica_info))) {
            STORAGE_LOG(WARN, "failed to push replica info into array", K(ret), K(replica_info));
          }
        } else if ((replica_info.local_minor_snapshot_version_ < replica_info.remote_minor_snapshot_version_ &&
                       replica_info.local_last_replay_log_id_ < replica_info.remote_last_replay_log_id_) ||
                   replica_info.local_major_snapshot_version_ < replica_info.remote_major_snapshot_version_) {
          if (OB_FAIL(replica_info_array.push_back(replica_info))) {
            STORAGE_LOG(WARN, "failed to push replica info into array", K(ret), K(replica_info));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      // add replica info array
      if (replica_info_array.empty()) {
        // do nothing
      } else if (OB_FAIL(rebuild_replica_service_->set_replica_info(replica_info_array))) {
        STORAGE_LOG(WARN, "failed to add replica info", K(ret));
      } else {
        scheduler_.notify();
      }
    }
  }
  return ret;
}

ObRebuildReplicaService::ObRebuildReplicaService()
    : is_inited_(false),
      lock_(),
      scheduler_(),
      task_producer_(scheduler_),
      replica_info_array_(),
      task_procuder_timer_()
{}

ObRebuildReplicaService::~ObRebuildReplicaService()
{}

void ObRebuildReplicaService::destroy()
{
  LOG_INFO("start destroy ObRebuildReplicaService");
  task_procuder_timer_.destroy();
  scheduler_.destroy();
  LOG_INFO("finish destroy ObRebuildReplicaService");
}

void ObRebuildReplicaService::stop()
{
  LOG_INFO("ObRebuildReplicaService::stop");
  task_procuder_timer_.stop();
  task_procuder_timer_.wait();
  scheduler_.stop();
  scheduler_.wait();
  LOG_INFO("ObRebuildReplicaService stopped");
}

int ObRebuildReplicaService::init(
    ObPartitionService& partition_service, obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  const bool repeat = true;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(task_procuder_timer_.init("RebuildTask"))) {
    STORAGE_LOG(WARN, "failed to init task producer timer", K(ret));
  } else if (OB_FAIL(task_producer_.init(&partition_service, &srv_rpc_proxy, this))) {
    STORAGE_LOG(WARN, "failed to init task producer", K(ret));
  } else if (OB_FAIL(scheduler_.init(&partition_service, this))) {
    STORAGE_LOG(WARN, "failed to init task scheduler", K(ret));
  } else if (OB_FAIL(task_procuder_timer_.schedule(task_producer_, DELAY_US, repeat))) {
    STORAGE_LOG(WARN, "failed to schedule task producer, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObRebuildReplicaService::set_replica_info(const ObIArray<ObRebuildReplicaInfo>& replica_info_array)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica service do not init", K(ret));
  } else if (OB_FAIL(replica_info_array_.assign(replica_info_array))) {
    STORAGE_LOG(WARN, "failed to assign replica info array", K(ret));
  }
  return ret;
}

int ObRebuildReplicaService::get_replica_info(common::ObIArray<ObRebuildReplicaInfo>& replica_info_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica service do not init", K(ret));
  } else {
    TCWLockGuard lock_guard(lock_);
    if (OB_FAIL(replica_info_array.assign(replica_info_array_))) {
      STORAGE_LOG(WARN, "failed to assign replica info array", K(ret));
    } else {
      replica_info_array_.reset();
    }
  }
  return ret;
}

ObRebuildReplicaTaskScheduler::ObRebuildReplicaTaskScheduler()
    : is_inited_(false),
      partition_service_(NULL),
      rebuild_replica_service_(NULL),
      cmp_ret_(OB_SUCCESS),
      compare_(cmp_ret_),
      replica_info_map_(),
      cond_(),
      need_idle_(true)
{}

ObRebuildReplicaTaskScheduler::~ObRebuildReplicaTaskScheduler()
{
  destroy();
}

void ObRebuildReplicaTaskScheduler::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    partition_service_ = NULL;
    rebuild_replica_service_ = NULL;
    replica_info_map_.destroy();
    cond_.destroy();
    is_inited_ = false;
  }
}

int ObRebuildReplicaTaskScheduler::init(
    ObPartitionService* partition_service, ObRebuildReplicaService* rebuild_replica_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "rebuild task scheduler init twice", K(ret));
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(rebuild_replica_service)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "task scheduler init get invalid argument", K(ret), KP(rebuild_replica_service), KP(partition_service));
  } else if (OB_FAIL(replica_info_map_.create(common::OB_MAX_PARTITION_NUM_PER_SERVER, ObModIds::OB_PARTITION))) {
    STORAGE_LOG(WARN, "failed to create replica info map", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::REBUILD_RETRY_LIST_LOCK_WAIT))) {
    STORAGE_LOG(WARN, "failed to init task scheduler sync", K(ret));
  } else {
    partition_service_ = partition_service;
    rebuild_replica_service_ = rebuild_replica_service;
    need_idle_ = true;
    is_inited_ = true;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(start())) {
      STORAGE_LOG(WARN, "failed to start scheduler");
    }
  }

  return ret;
}

// TODO(): need to optimize task generation logic
void ObRebuildReplicaTaskScheduler::run1()
{
  int ret = OB_SUCCESS;
  const int64_t BUILD_TASK_HEAP_INTERVAL = 20 * 1000 * 1000;  // 20s
  int64_t last_build_heap_ts = ObTimeUtility::current_time();
  ObArenaAllocator allocator;
  Heap heap(compare_, &allocator);

  // lib::set_thread_name("RebuildSche");
  (void)prctl(PR_SET_NAME, "ObRebuildReplicaTaskScheduler", 0, 0, 0);
  while (!has_set_stop()) {
    ret = OB_SUCCESS;
    const int64_t cur_ts = ObTimeUtility::current_time();
    if (cur_ts - last_build_heap_ts > BUILD_TASK_HEAP_INTERVAL) {
      last_build_heap_ts = cur_ts;
      if (OB_FAIL(build_task_heap(heap))) {
        STORAGE_LOG(WARN, "failed to build task heap", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_task(heap))) {
        if (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT != ret) {
          STORAGE_LOG(WARN, "failed to schedule", K(ret));
        }
      }
    }

    FLOG_INFO("after scheduler rebuild task", K(ret), "left_task_count", heap.count());
    if (heap.empty() || OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT == ret) {
      ObThreadCondGuard guard(cond_);
      if (need_idle_) {
        cond_.wait(SCHEDULER_WAIT_TIME_MS);
      }

      need_idle_ = true;
    }
  }
}

int ObRebuildReplicaTaskScheduler::build_task_heap(Heap& heap)
{
  int ret = OB_SUCCESS;
  ObArray<ObRebuildReplicaInfo> replica_info_array;
  const int overwrite_flag = 1;
  const int64_t start_ts = ObTimeUtility::current_time();

  heap.reset();
  replica_info_map_.clear();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild task scheduler do not init", K(ret));
  } else if (OB_FAIL(rebuild_replica_service_->get_replica_info(replica_info_array))) {
    STORAGE_LOG(WARN, "failed to get rebuild replica info", K(ret));
  } else if (replica_info_array.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_info_array.count(); ++i) {
      const ObRebuildReplicaInfo& replica_info = replica_info_array.at(i);
      if (!replica_info.is_valid()) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "replica info should be valid", K(ret), K(replica_info));
      } else if (OB_FAIL(replica_info_map_.set_refactored(replica_info.pg_key_, replica_info, overwrite_flag))) {
        STORAGE_LOG(WARN, "failed to set replica info into map", K(ret), K(replica_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rebuild_replica_info_map())) {
      STORAGE_LOG(WARN, "failed to rebuild replica info map", K(ret));
    } else if (OB_FAIL(add_replica_info_to_heap(heap))) {
      STORAGE_LOG(WARN, "failed to add replica info to heap", K(ret));
    }
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  FLOG_INFO("build rebuild scheduler task heap", K(cost_ts), "count", heap.count());

  return ret;
}

int ObRebuildReplicaTaskScheduler::add_task(Heap& heap)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild task scheduler do not init", K(ret));
  } else {
    SMART_VAR(ObReplicaOpArg, arg)
    {
      const ObRebuildReplicaInfo::RebuildAction rebuild_action = ObRebuildReplicaInfo::DOING;
      while (OB_SUCC(ret) && !heap.empty()) {
        const ObRebuildReplicaInfo* replica_info = heap.top();
        if (OB_ISNULL(replica_info)) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(WARN, "replica info should not be NULL", K(ret), KP(replica_info));
        } else if (ObRebuildReplicaInfo::DOING == replica_info->rebuild_action_) {
          // do nothing
        } else {
          ObReplicaMember src(
              replica_info->parent_addr_, ObTimeUtility::current_time(), replica_info->parent_replica_type_);
          ObReplicaMember dst(MYADDR, ObTimeUtility::current_time(), replica_info->replica_type_);
          share::ObTaskId task_id;
          task_id.init(MYADDR);
          arg.reset();
          arg.key_ = replica_info->pg_key_;
          arg.dst_ = dst;
          arg.src_ = src;
          arg.data_src_ = src;
          arg.type_ = replica_info->is_share_major_ ? LINK_SHARE_MAJOR_OP : REBUILD_REPLICA_OP;
          arg.priority_ = ObReplicaOpPriority::PRIO_HIGH;
          arg.change_member_option_ = SKIP_CHANGE_MEMBER_LIST;
          arg.switch_epoch_ = GCTX.get_switch_epoch2();
          if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
            if (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT == ret) {
              FLOG_INFO("schedule task reach top limit", K(ret), "heap_count", heap.count(), K(arg));
              break;
            } else if (OB_TASK_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              STORAGE_LOG(WARN, "fail to schedule migrate task", K(ret), K(task_id), K(arg));
            }
          } else if (OB_FAIL(change_rebuild_action(replica_info->pg_key_, rebuild_action))) {
            STORAGE_LOG(WARN, "failed to change rebuild action", K(ret), K(*replica_info));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(heap.pop())) {
            STORAGE_LOG(WARN, "failed to pop element", K(ret));
          } else {
            FLOG_INFO("pop heap task", "heap_count", heap.count(), K(*replica_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObRebuildReplicaTaskScheduler::rebuild_replica_info_map()
{
  int ret = OB_SUCCESS;
  ObArray<ObPGKey> not_exist_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task scheduler do not init", K(ret));
  } else {
    for (ObReplicaInfoMap::const_iterator iter = replica_info_map_.begin();
         OB_SUCC(ret) && iter != replica_info_map_.end();
         ++iter) {
      storage::ObIPartitionGroupGuard guard;
      ret = partition_service_->get_partition(iter->first, guard);
      if (OB_PARTITION_NOT_EXIST == ret) {
        if (OB_FAIL(not_exist_key.push_back(iter->first))) {
          STORAGE_LOG(WARN, "failed to push key into array", K(ret), K(iter->first));
        }
      } else {
        ObIPartitionGroup* partition = NULL;
        if (OB_ISNULL(partition = guard.get_partition_group())) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "partition should not be NULL", K(ret), KP(partition));
        } else if (!partition->is_replica_using_remote_memstore() && !partition->is_share_major_in_zone()) {
          if (OB_FAIL(not_exist_key.push_back(iter->first))) {
            STORAGE_LOG(WARN, "failed to push key into array", K(ret), K(iter->first));
          }
        } else {
          const ObRebuildReplicaInfo& rebuild_replica_info = iter->second;
          if (rebuild_replica_info.is_share_major_) {
            if (rebuild_replica_info.local_major_snapshot_version_ >=
                rebuild_replica_info.remote_major_snapshot_version_) {
              if (OB_FAIL(not_exist_key.push_back(iter->first))) {
                STORAGE_LOG(WARN, "failed to push key into array", K(ret), K(iter->first));
              }
            }
          } else if (rebuild_replica_info.local_minor_snapshot_version_ >=
                         rebuild_replica_info.remote_minor_snapshot_version_ &&
                     rebuild_replica_info.local_major_snapshot_version_ >=
                         rebuild_replica_info.remote_major_snapshot_version_) {
            if (OB_FAIL(not_exist_key.push_back(iter->first))) {
              STORAGE_LOG(WARN, "failed to push key into array", K(ret), K(iter->first));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < not_exist_key.count(); ++i) {
        if (OB_FAIL(replica_info_map_.erase_refactored(not_exist_key.at(i)))) {
          STORAGE_LOG(WARN, "failed to erase map element", K(ret), K(not_exist_key.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObRebuildReplicaTaskScheduler::add_replica_info_to_heap(Heap& heap)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuild replica task scheduler do not init", K(ret));
  } else {
    for (ObReplicaInfoMap::const_iterator iter = replica_info_map_.begin();
         OB_SUCC(ret) && iter != replica_info_map_.end();
         ++iter) {
      if (OB_FAIL(heap.push(&iter->second))) {
        STORAGE_LOG(WARN, "failed to push replica info into heap", K(ret), K(iter->second));
      } else if (OB_SUCCESS != cmp_ret_) {
        ret = cmp_ret_;
        STORAGE_LOG(WARN, "failed to add replica info into heap", K(ret));
      }
    }
  }
  return ret;
}

void ObRebuildReplicaTaskScheduler::notify()
{
  ObThreadCondGuard cond_guard(cond_);
  need_idle_ = false;
  cond_.signal();
}

int ObRebuildReplicaTaskScheduler::change_rebuild_action(
    const ObPGKey& pg_key, const ObRebuildReplicaInfo::RebuildAction rebuild_action)
{
  int ret = OB_SUCCESS;
  ObRebuildReplicaInfo rebuild_replica_info;
  const int overwrite_flag = 1;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "change replica action get invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(replica_info_map_.get_refactored(pg_key, rebuild_replica_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to erase pg key from map", K(ret), K(pg_key));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    rebuild_replica_info.rebuild_action_ = rebuild_action;
    if (OB_FAIL(replica_info_map_.set_refactored(pg_key, rebuild_replica_info, overwrite_flag))) {
      STORAGE_LOG(WARN, "failed to set rebuild replica info", K(ret), K(pg_key), K(rebuild_replica_info));
    }
  }
  return ret;
}

ObRebuildReplicaTaskScheduler::HeapComparator::HeapComparator(int& ret) : ret_(ret)
{}

bool ObRebuildReplicaTaskScheduler::HeapComparator::operator()(
    const ObRebuildReplicaInfo* info1, const ObRebuildReplicaInfo* info2)
{
  bool bret = false;
  int ret = OB_SUCCESS;

  if (OB_FAIL(ret_)) {
  } else if (OB_ISNULL(info1) || OB_ISNULL(info2) || !info1->is_valid() || !info2->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "rebuild replica info should be valid", K(ret), K(info1), K(info2));
  } else {
    // TODO () priority calc
    const int64_t priority1 = info1->remote_minor_snapshot_version_ - info1->local_minor_snapshot_version_;
    const int64_t priority2 = info2->remote_minor_snapshot_version_ - info2->local_minor_snapshot_version_;
    const int64_t priority3 = info1->remote_last_replay_log_id_ - info1->local_last_replay_log_id_;
    const int64_t priority4 = info2->remote_last_replay_log_id_ - info2->local_last_replay_log_id_;
    int64_t cmp = priority1 - priority2 + priority3 - priority4;
    if (cmp < 0) {
      bret = true;
    } else {
      bret = false;
    }
  }
  ret_ = ret;
  return bret;
}

}  // end namespace storage
}  // end namespace oceanbase
