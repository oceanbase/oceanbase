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
#include "ob_partition_table_checker.h"

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

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace observer {
ObCheckDanglingReplicaTask::ObCheckDanglingReplicaTask(
    ObPartitionTableChecker& pt_checker, volatile const bool& stopped, int64_t version)
    : IObDedupTask(T_DANGLING_REPLICA_CHECK), pt_checker_(pt_checker), stopped_(stopped), version_(version)
{}

ObCheckDanglingReplicaTask::~ObCheckDanglingReplicaTask()
{}

int64_t ObCheckDanglingReplicaTask::hash() const
{
  uint64_t hash_val = reinterpret_cast<uint64_t>(&pt_checker_);
  hash_val = murmurhash(&version_, sizeof(version_), hash_val);
  return hash_val;
}

bool ObCheckDanglingReplicaTask::operator==(const IObDedupTask& other) const
{
  bool is_equal = false;
  if (!this->is_valid()) {
    LOG_WARN("invalid task", "self", *this);
  } else if (this == &other) {
    is_equal = true;
  } else {
    if (get_type() == other.get_type()) {
      // it's safe to do this transformation, we have checked the task's type
      const ObCheckDanglingReplicaTask& other_task = static_cast<const ObCheckDanglingReplicaTask&>(other);
      if (!other_task.is_valid()) {
        LOG_WARN("invalid task", K(other_task));
      } else {
        is_equal = (&pt_checker_ == &other_task.pt_checker_ && version_ == other_task.version_);
      }
    }
  }
  return is_equal;
}

int64_t ObCheckDanglingReplicaTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

IObDedupTask* ObCheckDanglingReplicaTask::deep_copy(char* buf, const int64_t buf_size) const
{
  IObDedupTask* task = NULL;
  if (!this->is_valid()) {
    LOG_WARN("invalid task", "self", *this);
  } else if (NULL == buf || buf_size < get_deep_copy_size()) {
    LOG_WARN(
        "invalid argument", "buf", reinterpret_cast<int64_t>(buf), K(buf_size), "need buf size", get_deep_copy_size());
  } else {
    task = new (buf) ObCheckDanglingReplicaTask(pt_checker_, stopped_, version_);
  }
  return task;
}

int ObCheckDanglingReplicaTask::process()
{
  int ret = OB_SUCCESS;
  if (!this->is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat error", "self", *this, K(ret));
  } else if (!stopped_) {
    if (OB_FAIL(pt_checker_.check_dangling_replica_exist(version_))) {
      LOG_WARN("check dangling replica exist failed", K(ret));
    }
  }
  return ret;
}

ObCheckPartitionTableTask::ObCheckPartitionTableTask(ObPartitionTableChecker& pt_checker, volatile const bool& stopped)
    : IObDedupTask(T_PT_CHECK), pt_checker_(pt_checker), stopped_(stopped)
{}

ObCheckPartitionTableTask::~ObCheckPartitionTableTask()
{}

int64_t ObCheckPartitionTableTask::hash() const
{
  return reinterpret_cast<int64_t>(&pt_checker_);
}

bool ObCheckPartitionTableTask::operator==(const IObDedupTask& other) const
{
  bool is_equal = false;
  if (!this->is_valid()) {
    LOG_WARN("invalid task", "self", *this);
  } else if (this == &other) {
    is_equal = true;
  } else {
    if (get_type() == other.get_type()) {
      // it's safe to do this transformation, we have checked the task's type
      const ObCheckPartitionTableTask& other_task = static_cast<const ObCheckPartitionTableTask&>(other);
      if (!other_task.is_valid()) {
        LOG_WARN("invalid task", K(other_task));
      } else {
        is_equal = &pt_checker_ == &other_task.pt_checker_;
      }
    }
  }
  return is_equal;
}

int64_t ObCheckPartitionTableTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

IObDedupTask* ObCheckPartitionTableTask::deep_copy(char* buf, const int64_t buf_size) const
{
  IObDedupTask* task = NULL;
  if (!this->is_valid()) {
    LOG_WARN("invalid task", "self", *this);
  } else if (NULL == buf || buf_size < get_deep_copy_size()) {
    LOG_WARN(
        "invalid argument", "buf", reinterpret_cast<int64_t>(buf), K(buf_size), "need buf size", get_deep_copy_size());
  } else {
    task = new (buf) ObCheckPartitionTableTask(pt_checker_, stopped_);
  }
  return task;
}

int ObCheckPartitionTableTask::process()
{
  int ret = OB_SUCCESS;
  if (!this->is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat error", "self", *this, K(ret));
  } else if (!stopped_) {
    if (OB_FAIL(pt_checker_.check_partition_table())) {
      LOG_WARN("check_partition_table failed", K(ret));
    }
  }
  return ret;
}

ObPartitionTableChecker::ObPartitionTableChecker()
    : inited_(false),
      stopped_(false),
      pt_operator_(NULL),
      schema_service_(NULL),
      partition_service_(NULL),
      tg_id_(-1),
      queue_()
{}

ObPartitionTableChecker::~ObPartitionTableChecker()
{
  destroy();
}

int ObPartitionTableChecker::init(ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service,
    ObPartitionService& partition_service, int tg_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(queue_.init(PT_CHECK_THREAD_CNT, "PtChecker", PT_CHECK_QUEUE_SIZE, PTTASK_MAP_SIZE))) {
    LOG_WARN("init queue failed",
        LITERAL_K(PT_CHECK_THREAD_CNT),
        LITERAL_K(PT_CHECK_QUEUE_SIZE),
        LITERAL_K(PTTASK_MAP_SIZE),
        K(ret));
  } else if (OB_FAIL(queue_.set_thread_dead_threshold(PT_CHECK_THREAD_DEAD_THRESHOLD))) {
    LOG_WARN("set_thread_dead_threshold failed", LITERAL_K(PT_CHECK_THREAD_DEAD_THRESHOLD), K(ret));
  } else {
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    partition_service_ = &partition_service;
    tg_id_ = tg_id;
    stopped_ = false;
    inited_ = true;
  }
  return ret;
}

int ObPartitionTableChecker::destroy()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    queue_.destroy();
    inited_ = false;
  }
  return ret;
}

void ObPartitionTableChecker::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("check partition table scheduler is stopped", K(ret));
  } else {
    if (OB_FAIL(schedule_task())) {
      LOG_WARN("schedule check partition table async task failed", K(ret));
    }

    // should continue even schedule_task fail, ignore ret overwrite
    const bool repeat = false;
    const int64_t delay = GCONF.partition_table_check_interval;
    if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, delay, repeat))) {
      LOG_WARN("schedule check partition table timer task failed", K(delay), K(repeat), K(ret));
    }
  }
}

int ObPartitionTableChecker::schedule_task()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("check partition table scheduler is stopped", K(ret));
  } else {
    ObCheckPartitionTableTask task(*this, stopped_);
    if (OB_FAIL(queue_.add_task(task))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("queue add_task failed", K(task), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPartitionTableChecker::schedule_task(int64_t version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("check partition table scheduler is stopped", K(ret));
  } else if (version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("version is invalid", K(ret));
  } else {
    ObCheckDanglingReplicaTask task(*this, stopped_, version);
    if (OB_FAIL(queue_.add_task(task))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("queue add_task failed", K(task), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPartitionTableChecker::check_partition_table()
{
  int ret = OB_SUCCESS;
  int64_t dangling_count = 0;  // replica only in partition table
  int64_t report_count = 0;    // replica not in/match partition table
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("start checking partition table");
    ObReplicaMap server_replica_map;
    if (OB_FAIL(build_replica_map(server_replica_map))) {
      LOG_WARN("build replica map from partition table failed", K(ret));
    } else if (OB_FAIL(check_dangling_replicas(server_replica_map, dangling_count))) {
      LOG_WARN("check replicas exist in partition table but not in observer failed", K(ret));
    } else if (OB_FAIL(check_report_replicas(server_replica_map, report_count))) {
      LOG_WARN("check replicas not in/match partition table failed", K(ret));
    }
    LOG_INFO("finish checking partition table", K(ret), K(dangling_count), K(report_count));
  }
  SERVER_EVENT_ADD("storage", "check_partition_table", K(ret), K(dangling_count), K(report_count));
  return ret;
}

int ObPartitionTableChecker::build_replica_map(ObReplicaMap& server_replica_map)
{
  int ret = OB_SUCCESS;
  ObFullPartitionTableIterator pt_iter;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_replica_map.create(
                 hash::cal_next_prime(SERVER_REPLICA_MAP_BUCKET_NUM), ObModIds::OB_HASH_BUCKET_PT_CHECKER_MAP))) {
    LOG_WARN("server_replica_map create failed",
        LITERAL_K(SERVER_REPLICA_MAP_BUCKET_NUM),
        "label",
        ObModIds::OB_HASH_BUCKET_PT_CHECKER_MAP,
        K(ret));
  } else if (OB_FAIL(pt_iter.init(*pt_operator_, *schema_service_))) {
    LOG_WARN("partition table iterator init failed", K(ret));
  } else if (OB_FAIL(pt_iter.get_filters().set_server(GCONF.self_addr_))) {
    LOG_WARN("set filter failed", K(ret), "server", GCONF.self_addr_);
  } else {
    ObPartitionInfo partition_info;
    while (OB_SUCC(ret)) {
      partition_info.reuse();
      if (OB_FAIL(pt_iter.next(partition_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("partition table iterator next failed", K(ret));
        }
      } else {
        if (1 == partition_info.replica_count()) {
          const int64_t index = 0;
          ObPartitionReplica replica;
          if (OB_FAIL(replica.assign(partition_info.get_replicas_v2().at(index)))) {
            LOG_WARN("assign replica fail", K(ret));
          } else {
            const ObPartitionKey partition_key(replica.table_id_, replica.partition_id_, replica.partition_cnt_);
            if (OB_FAIL(server_replica_map.set_refactored(partition_key, replica))) {
              if (OB_HASH_EXIST == ret) {
                LOG_WARN("partition iterator should not iterator two same partition", K(partition_key), K(ret));
              } else {
                LOG_WARN("server_replica_set_ set failed", K(partition_key), K(ret));
              }
            }
          }
        } else if (0 == partition_info.replica_count()) {
          // do nothing
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition_info should not have two replica in same server", K(partition_info), K(ret));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPartitionTableChecker::check_dangling_replicas(ObReplicaMap& server_replica_map, int64_t& dangling_count)
{
  int ret = OB_SUCCESS;
  const ObAddr self_addr = GCONF.self_addr_;
  const int64_t now = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (ObReplicaMap::const_iterator iter = server_replica_map.begin(); iter != server_replica_map.end(); ++iter) {
      storage::ObIPartitionGroupGuard guard;
      ret = partition_service_->get_partition(iter->first, guard);
      if (OB_PARTITION_NOT_EXIST == ret) {
        LOG_INFO("partition not exist in observer", "partition", iter->first);
        // overwrite ret
        ret = OB_SUCCESS;
        if (now - iter->second.modify_time_us_ > GCONF.replica_safe_remove_time) {
          ++dangling_count;
          if (OB_ISNULL(GCTX.ob_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ob_service is null", K(ret));
          } else {
            (void)GCTX.ob_service_->submit_pt_update_task(iter->first);
            LOG_INFO("add async task to remove replica from partition table",
                "table_id",
                iter->first.table_id_,
                "partition_id",
                iter->first.get_partition_id(),
                K(self_addr),
                "replica",
                iter->second);
          }
        }
      } else if (OB_FAIL(ret)) {
        LOG_WARN("partition_service get_partition failed", K(ret));
      } else if (OB_ISNULL(guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObPartitionTableChecker::check_report_replicas(ObReplicaMap& server_replica_map, int64_t& report_count)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service is null", K(ret));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ob_service is null", K(ret));
  } else if (NULL == (iter = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc scan iter", K(ret));
  } else {
    ObPartitionReplica local_replica;
    ObPartitionReplica replica;
    ObIPartitionGroup* partition = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("scan next partition failed.", K(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get partition", K(ret));
      } else {
        const ObPartitionKey& pkey = partition->get_partition_key();
        bool is_equal = false;
        replica.reset();
        local_replica.reset();
        if (OB_FAIL(server_replica_map.get_refactored(pkey, replica))) {
          // local observer has this partition, however partition_table doesn't have
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            ObPartitionArray pkeys;
            if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
              LOG_WARN("get all pg partition keys error", K(ret), K(pkeys));
            } else if (OB_FAIL(GCTX.ob_service_->submit_pt_update_task(pkey, true /*need report checksum*/))) {
              LOG_WARN("submit partition table update task failed", K(ret), K(pkey));
            } else if (OB_FAIL(GCTX.ob_service_->submit_pt_update_role_task(pkey))) {
              LOG_WARN("submit pt update role task", KR(ret), K(pkey));
            } else {
              // try to submit updating partition meta table task, ignore ret code
              GCTX.ob_service_->submit_pg_pt_update_task(pkeys);
              report_count++;
              LOG_INFO("add missing replica to partition table success", K(ret), K(pkey));
            }
          } else {
            LOG_WARN("get replica from hashmap fail", K(ret), K(pkey));
          }
        } else if (OB_FAIL(GCTX.ob_service_->fill_partition_replica(partition, local_replica))) {
          LOG_WARN("fail to fill partition replica", K(ret), K(pkey));
        } else if (OB_FAIL(check_if_replica_equal(replica, local_replica, is_equal))) {
          LOG_WARN("check replica equal fail", K(ret), K(replica));
        } else if (!is_equal) {
          ObPartitionArray pkeys;
          // this partition exists in both local observer and partition table, but not the same,
          // need to update partition table based on the partition status from this local observer
          if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
            LOG_WARN("get all pg partition keys error", K(ret), K(pkeys));
          } else if (OB_FAIL(GCTX.ob_service_->submit_pt_update_task(pkey, true /*need report checksum*/))) {
            LOG_WARN("submit partition table update task failed", K(ret), K(pkey));
          } else if (OB_FAIL(GCTX.ob_service_->submit_pt_update_role_task(pkey))) {
            LOG_WARN("submit pt update role task", KR(ret), K(pkey));
          } else {
            // try to submit updating partition meta table task, ignore ret code
            GCTX.ob_service_->submit_pg_pt_update_task(pkeys);
            report_count++;
            LOG_INFO("modify replica success", K(ret), K(pkey), K(replica), K(local_replica));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (NULL != iter) {
      partition_service_->revert_pg_iter(iter);
    }
  }
  return ret;
}

int ObPartitionTableChecker::check_if_replica_equal(
    share::ObPartitionReplica& replica, share::ObPartitionReplica& local_replica, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!replica.partition_key().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replica part_key is invalid", K(ret), K(replica));
  } else if (!local_replica.partition_key().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("local_replica part_key is invalid", K(ret), K(local_replica));
  } else if (replica.table_id_ == local_replica.table_id_ && replica.partition_id_ == local_replica.partition_id_ &&
             replica.partition_cnt_ == local_replica.partition_cnt_ && replica.role_ == local_replica.role_ &&
             replica.to_leader_time_ == local_replica.to_leader_time_ && replica.zone_ == local_replica.zone_ &&
             replica.server_ == local_replica.server_ && replica.sql_port_ == local_replica.sql_port_ &&
             replica.is_original_leader_ == local_replica.is_original_leader_ &&
             replica.rebuild_ == local_replica.rebuild_ && replica.row_count_ == local_replica.row_count_ &&
             replica.data_size_ == local_replica.data_size_ && replica.data_version_ == local_replica.data_version_ &&
             replica.data_checksum_ == local_replica.data_checksum_ &&
             replica.row_checksum_.checksum_ == local_replica.row_checksum_.checksum_ &&
             replica.replica_type_ == local_replica.replica_type_ &&
             replica.required_size_ == local_replica.required_size_ &&
             replica.partition_checksum_ == local_replica.partition_checksum_ &&
             replica.status_ == local_replica.status_ && replica.is_restore_ == local_replica.is_restore_ &&
             member_list_is_equal(replica.member_list_, local_replica.member_list_) &&
             replica.quorum_ == local_replica.quorum_ && replica.property_ == local_replica.property_ &&
             replica.data_file_id_ == local_replica.data_file_id_) {
    is_equal = true;
  }
  return ret;
}

int ObPartitionTableChecker::member_list_is_equal(
    share::ObPartitionReplica::MemberList& a, share::ObPartitionReplica::MemberList& b)
{
  bool is_equal = true;
  if (a.count() != b.count()) {
    is_equal = false;
  } else {
    for (int i = 0; is_equal && i < a.count(); ++i) {
      is_equal = false;
      for (int j = 0; j < b.count(); ++j) {
        if (a[i] == b[j]) {
          is_equal = true;
          break;
        }
      }
    }
  }
  return is_equal;
}

int ObPartitionTableChecker::check_dangling_replica_exist(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObFullPartitionTableIterator pt_iter;
  int64_t dangling_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(pt_iter.init(*pt_operator_, *schema_service_))) {
    LOG_WARN("partition table iterator init failed", K(ret));
  } else if (OB_FAIL(pt_iter.get_filters().set_server(GCONF.self_addr_))) {
    LOG_WARN("set filter failed", K(ret), "server", GCONF.self_addr_);
  } else {
    ObPartitionInfo partition_info;
    while (OB_SUCC(ret)) {
      partition_info.reuse();
      if (OB_FAIL(pt_iter.next(partition_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("partition table iterator next failed", K(ret));
        }
      } else {
        if (1 == partition_info.replica_count()) {
          const int64_t index = 0;
          storage::ObIPartitionGroupGuard guard;
          ObPartitionReplica replica;
          if (OB_FAIL(replica.assign(partition_info.get_replicas_v2().at(index)))) {
            LOG_WARN("assign replica fail", K(ret));
          } else if (OB_FAIL(partition_service_->get_partition(replica.partition_key(), guard))) {
            if (OB_PARTITION_NOT_EXIST == ret) {
              LOG_INFO("partition not exist in observer", "partition", replica.partition_key(), K(version));
              // overwrite ret
              ret = OB_SUCCESS;
              ++dangling_count;
            } else {
              LOG_WARN("partition_service get_partition failed", K(ret));
            }
          } else if (OB_ISNULL(guard.get_partition_group())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", K(ret));
          } else {
            // do nothing
          }
        } else if (0 == partition_info.replica_count()) {
          // do nothing
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition_info should not have two replica in same server", K(partition_info), K(ret));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      ObAddr rs_addr;
      if (version <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(version));
      } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid global context", K(ret), K(GCTX));
      } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
        LOG_WARN("get rootservice address failed", K(ret));
      } else {
        LOG_INFO("get dangling replicas num", K(version), K(dangling_count));
        obrpc::ObCheckDanglingReplicaFinishArg arg;
        arg.server_ = GCTX.self_addr_;
        arg.version_ = version;
        arg.dangling_count_ = dangling_count;
        // invoke not_rs_mgr() to disable rpc proxy retry logic.
        if (OB_FAIL(
                GCTX.rs_rpc_proxy_->to_addr(rs_addr).timeout(GCONF.rpc_timeout).check_dangling_replica_finish(arg))) {
          LOG_WARN("call check dangling replicas finish rpc failed", K(ret), K(arg));
        }
      }
    }
  }
  return ret;
}
}  // end namespace observer
}  // end namespace oceanbase
