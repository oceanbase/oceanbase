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

#include "ob_election_mgr.h"
#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"

namespace oceanbase {

using namespace obrpc;
using namespace common;
using namespace common::hash;

namespace election {

int64_t ObElectionFactory::alloc_count_ = 0;
int64_t ObElectionFactory::release_count_ = 0;
int64_t ElectionAllocHandle::TOTAL_RELEASE_COUNT = 0;
int64_t ObElectionMgr::TOTAL_ADD_COUNT = 0;
int64_t ObElectionMgr::TOTAL_REMOVE_COUNT = 0;
const char* ELECTION_TIMER_NAME = "ElectTimeWheel";

static bool stop_all_election(const ObPartitionKey& partition, ObIElection* e)
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;

  if (!partition.is_valid() || OB_ISNULL(e)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), KP(e));
  } else if (OB_SUCCESS != (tmp_ret = e->stop())) {
    ELECT_ASYNC_LOG(WARN, "election stop error", "ret", tmp_ret);
  } else {
    bool_ret = true;
  }

  return bool_ret;
}

// static bool destroy_all_election(const ObPartitionKey &partition, ObIElection *e)
// {
//   bool bool_ret = false;

//   if (!partition.is_valid() || OB_ISNULL(e)) {
//     ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), KP(e));
//   } else {
//     e->destroy();
//     ObElectionFactory::release(e);
//     e = NULL;
//     bool_ret = true;
//   }

//   return bool_ret;
// }

int ObElectionMgr::init(const ObAddr& self, obrpc::ObBatchRpc* batch_rpc, ObIElectionGroupPriorityGetter* eg_cb)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr inited twice");
    ret = OB_INIT_TWICE;
  } else if (!self.is_valid() || OB_ISNULL(eg_cb)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(self), KP(eg_cb));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(election_map_.init(ObModIds::OB_ELECTION_HASH_BUCKET))) {
    ELECT_ASYNC_LOG(WARN, "init hash map error", K(ret));
  } else if (OB_FAIL(batch_rpc_def_.init(batch_rpc))) {
    ELECT_ASYNC_LOG(WARN, "init rpc error", K(ret));
  } else if (OB_FAIL(tw_.init(TIME_WHEEL_PRECISION_US, TIMER_THREAD_COUNT, ELECTION_TIMER_NAME))) {
    ELECT_ASYNC_LOG(WARN, "init timewheel error", K(ret));
  } else if (OB_FAIL(election_cache_.init())) {
    ELECT_ASYNC_LOG(WARN, "election cache init error", K(ret));
  } else if (OB_FAIL(eg_mgr_.init(self, &batch_rpc_def_, &tw_, this, eg_cb))) {
    ELECT_ASYNC_LOG(WARN, "eg_mgr_ init error", K(ret));
  } else if (OB_FAIL(event_hist_array_.init(self))) {
    ELECT_ASYNC_LOG(WARN, "event_hist_array_ init failed", K(ret));
  } else if (OB_FAIL(gc_thread_.init(&eg_mgr_))) {
    ELECT_ASYNC_LOG(WARN, "gc_thread_ init failed", K(ret));
  } else {
    rpc_ = &batch_rpc_def_;
    self_ = self;
    is_inited_ = true;
    ELECT_ASYNC_LOG(INFO, "ObElectionMgr inited success", K(self), KP(this));
  }

  return ret;
}

// for test
int ObElectionMgr::init(const ObAddr& self, ObIElectionRpc* rpc, ObIElectionGroupPriorityGetter* eg_cb)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr inited twice");
    ret = OB_INIT_TWICE;
  } else if (!self.is_valid() || OB_ISNULL(rpc) || NULL == eg_cb) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(self), KP(rpc), KP(eg_cb));
  } else if (OB_FAIL(election_map_.init(ObModIds::OB_ELECTION_HASH_BUCKET))) {
    ELECT_ASYNC_LOG(WARN, "init hash map error", K(ret));
  } else if (OB_FAIL(tw_.init(TIME_WHEEL_PRECISION_US, TIMER_THREAD_COUNT, ELECTION_TIMER_NAME))) {
    ELECT_ASYNC_LOG(WARN, "init timewheel error", K(ret));
  } else if (OB_FAIL(election_cache_.init())) {
    ELECT_ASYNC_LOG(WARN, "election cache init error", K(ret));
  } else if (OB_FAIL(eg_mgr_.init(self, rpc, &tw_, this, eg_cb))) {
    ELECT_ASYNC_LOG(WARN, "eg_mgr_ init error", K(ret));
  } else if (OB_FAIL(gc_thread_.init(&eg_mgr_))) {
    ELECT_ASYNC_LOG(WARN, "gc_thread_ init failed", K(ret));
  } else {
    rpc_ = rpc;
    self_ = self;
    is_inited_ = true;
    ELECT_ASYNC_LOG(INFO, "ObElectionMgr inited success", K(self), KP(rpc), KP(this));
  }

  return ret;
}

int ObElectionMgr::init(const ObAddr& self, ObIElectionRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr inited twice");
    ret = OB_INIT_TWICE;
  } else if (!self.is_valid() || OB_ISNULL(rpc)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(self), KP(rpc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(election_map_.init(ObModIds::OB_ELECTION_HASH_BUCKET))) {
    ELECT_ASYNC_LOG(WARN, "init hash map error", K(ret));
  } else if (OB_FAIL(tw_.init(TIME_WHEEL_PRECISION_US, TIMER_THREAD_COUNT, ELECTION_TIMER_NAME))) {
    ELECT_ASYNC_LOG(WARN, "init timewheel error", K(ret));
  } else if (OB_FAIL(election_cache_.init())) {
    ELECT_ASYNC_LOG(WARN, "election cache init error", K(ret));
  } else {
    rpc_ = rpc;
    self_ = self;
    if (OB_FAIL(eg_mgr_.init(self_, rpc_, &tw_, this, NULL))) {
      ELECT_ASYNC_LOG(WARN, "eg_mgr_ init error", K(ret));
    } else if (OB_FAIL(gc_thread_.init(&eg_mgr_))) {
      ELECT_ASYNC_LOG(WARN, "gc_thread_ init failed", K(ret));
    } else {
      is_inited_ = true;
      ELECT_ASYNC_LOG(INFO, "ObElectionMgr inited success", K(self), KP(rpc), KP(this));
    }
  }

  return ret;
}

void ObElectionMgr::destroy()
{
  int tmp_ret = OB_SUCCESS;

  WRLockGuard guard(rwlock_);
  if (!is_inited_) {
    // do nothing
  } else {
    is_inited_ = false;
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop_())) {
        ELECT_ASYNC_LOG(WARN, "ObElectionMgr stop error", "ret", tmp_ret);
      } else if (OB_SUCCESS != (tmp_ret = wait_())) {
        ELECT_ASYNC_LOG(WARN, "ObElectionMgr wait error", "ret", tmp_ret);
      } else {
        // do nothing
      }
    }
    // if (OB_SUCCESS != (tmp_ret = election_map_.for_each(destroy_all_election))) {
    //   ELECT_ASYNC_LOG(WARN, "clear all election error", "ret", tmp_ret);
    // } else {
    //   ELECT_ASYNC_LOG(INFO, "clear all election success");
    // }
    election_map_.destroy();
    tw_.destroy();
    gc_thread_.destroy();
    eg_mgr_.destroy();
    ELECT_ASYNC_LOG(INFO, "ObElectionMgr destroy");
    ASYNC_LOG_DESTROY();  // need recall init() in election unittest case
  }
}

int ObElectionMgr::add_partition(const ObPartitionKey& partition, const int64_t replica_num,
    ObIElectionCallback* election_cb, ObIElection*& election)
{
  int ret = OB_SUCCESS;
  ObIElection* tmp_election = NULL;
  const uint64_t tenant_id = partition.get_tenant_id();
  RDLockGuard guard(rwlock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running", K(ret));
  } else if (!partition.is_valid() || replica_num <= 0 || OB_ISNULL(election_cb)) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(partition), K(replica_num), KP(election_cb));
  } else if (NULL == (tmp_election = ObElectionFactory::alloc(tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ELECT_ASYNC_LOG(WARN, "alloc election error", K(ret), K(partition));
  } else if (OB_FAIL(tmp_election->init(
                 partition, self_, rpc_, &tw_, replica_num, election_cb, &eg_mgr_, &event_hist_array_))) {
    ELECT_ASYNC_LOG(WARN, "election init error", K(ret), K(partition));
    ObElectionFactory::release(tmp_election);
    tmp_election = NULL;
  } else if (OB_FAIL(election_map_.insert_and_get(partition, tmp_election))) {
    ELECT_ASYNC_LOG(WARN, "hash insert error", K(ret), K(partition));
    ObElectionFactory::release(tmp_election);
    tmp_election = NULL;
  } else {
    (void)election_cache_.set(partition.hash(), static_cast<ObElection*>(tmp_election));
    FORCE_ELECT_LOG(
        INFO, "add partition success", K(partition), KP(this), "now partition count", election_map_.count());
    election = tmp_election;
    (void)ATOMIC_FAA(&TOTAL_ADD_COUNT, 1);
  }

  return ret;
}

int ObElectionMgr::remove_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  WRLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    // entry not exist, return success
    ELECT_ASYNC_LOG(WARN, "election not exist", K(partition));
  } else {
    if (OB_FAIL(election->stop())) {
      ELECT_ASYNC_LOG(WARN, "stop election error", K(ret), K(partition));
    } else if (OB_FAIL(election_map_.del(partition))) {
      ELECT_ASYNC_LOG(WARN, "erase partition error", K(ret), K(partition));
    } else if (OB_FAIL(election_cache_.remove(partition.hash()))) {
      ELECT_ASYNC_LOG(WARN, "remove election cache error", K(ret), K(partition));
    } else {
      FORCE_ELECT_LOG(INFO, "remove partition success", K(partition), "now partition count", election_map_.count());
    }
    revert_election_(election);
    (void)ATOMIC_FAA(&TOTAL_REMOVE_COUNT, 1);
  }

  return ret;
}

int ObElectionMgr::force_leader_async(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->force_leader_async())) {
      ELECT_ASYNC_LOG(WARN, "force leader async error", K(ret), K(partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "force leader async success", K(partition));
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::change_leader_async(
    const ObPartitionKey& partition, const ObAddr& leader, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid() || !leader.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), K(leader));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->change_leader_async(leader, changing_leader_windows))) {
      ELECT_ASYNC_LOG(WARN, "change leader async error", K(ret), K(partition), K(leader));
    } else {
      ELECT_ASYNC_LOG(INFO, "change leader async success", K(partition), K(leader));
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::leader_revoke(const ObPartitionKey& partition, const uint32_t revoke_type)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    election->leader_revoke(revoke_type);
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::start_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->start())) {
      ELECT_ASYNC_LOG(WARN, "election start error", K(ret), K(partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "election start success", K(partition));
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::start_partition(
    const ObPartitionKey& partition, const ObAddr& leader, const int64_t lease_start, int64_t& leader_epoch)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid() || !leader.is_valid() || lease_start <= 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), K(leader), K(lease_start));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->leader_takeover(leader, lease_start, leader_epoch))) {
      ELECT_ASYNC_LOG(WARN, "leader takeover error", K(ret), K(partition), K(leader), K(lease_start));
    } else if (OB_FAIL(election->start())) {
      ELECT_ASYNC_LOG(WARN, "election start error", K(ret), K(partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "election start success", K(partition), K(leader), K(lease_start), K(leader_epoch));
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::stop_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->stop())) {
      ELECT_ASYNC_LOG(WARN, "election stop error", K(ret), K(partition));
    } else {
      FORCE_ELECT_LOG(INFO, "election stop success", K(partition));
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::start()
{
  int ret = OB_SUCCESS;

  WRLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr already running");
    ret = OB_ELECTION_MGR_IS_RUNNING;
  } else if (OB_FAIL(tw_.start())) {
    ELECT_ASYNC_LOG(WARN, "ObTimeWheel start error", K(ret));
  } else if (OB_FAIL(eg_mgr_.start())) {
    ELECT_ASYNC_LOG(WARN, "election group manager start error", K(ret));
  } else if (OB_FAIL(gc_thread_.start())) {
    ELECT_ASYNC_LOG(WARN, "gc_thread_ start failed", K(ret));
  } else {
    ATOMIC_STORE(&is_running_, true);
    ELECT_ASYNC_LOG(INFO, "election manager start success");
  }

  return ret;
}

int ObElectionMgr::stop()
{
  WRLockGuard guard(rwlock_);
  return stop_();
}

int ObElectionMgr::wait()
{
  WRLockGuard guard(rwlock_);
  return wait_();
}

int ObElectionMgr::handle_election_req(int msg_type, ObPartitionKey& partition, const char* buf, int64_t limit)
{
  RDLockGuard guard(rwlock_);
  ObElectionRpcResult result;
  int64_t pos = 0;
  return handle_election_msg_unlock_(msg_type, partition, buf, limit, pos, result);
}

int ObElectionMgr::handle_election_msg_unlock_(int msg_type, ObPartitionKey& partition, const char* buf,
    const int64_t limit, int64_t& pos, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    // ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (NULL == (election = get_election_(partition))) {
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    switch (msg_type) {
      case OB_ELECTION_DEVOTE_PREPARE: {
        ObElectionMsgDEPrepare msg;
        if (OB_FAIL(msg.deserialize(buf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize devote prepare msg error", K(ret), K(partition), K_(self));
        } else if (OB_FAIL(election->handle_devote_prepare(msg, result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle devote prepare error", K(ret), K(partition), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_DEVOTE_VOTE: {
        ObElectionMsgDEVote msg;
        if (OB_FAIL(msg.deserialize(buf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize devote vote msg error", K(ret), K(partition), K_(self));
        } else if (OB_FAIL(election->handle_devote_vote(msg, result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle devote vote error", K(ret), K(partition), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_DEVOTE_SUCCESS: {
        ObElectionMsgDESuccess msg;
        if (OB_FAIL(msg.deserialize(buf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize devote success msg error", K(ret), K(partition), K_(self));
        } else if (OB_FAIL(election->handle_devote_success(msg, result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle devote success error", K(ret), K(partition), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_VOTE_PREPARE: {
        ObElectionMsgPrepare msg;
        if (OB_FAIL(msg.deserialize(buf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize vote prepare msg error", K(ret), K(partition), K_(self));
        } else if (OB_FAIL(election->handle_vote_prepare(msg, result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle vote prepare error", K(ret), K(partition), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_VOTE_VOTE: {
        ObElectionMsgVote msg;
        if (OB_FAIL(msg.deserialize(buf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize vote vote msg error", K(ret), K(partition), K_(self));
        } else if (OB_FAIL(election->handle_vote_vote(msg, result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle_vote_vote error", K(ret), K(partition), K_(self), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_VOTE_SUCCESS: {
        ObElectionMsgSuccess msg;
        if (OB_FAIL(msg.deserialize(buf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize vote success msg error", K(ret), K(partition), K_(self));
        } else if (OB_FAIL(election->handle_vote_success(msg, result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle vote success error", K(ret), K(partition), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_QUERY_LEADER: {
        ObElectionQueryLeader msg;
        if (OB_FAIL(msg.deserialize(buf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize query leader msg error", K(ret), K(partition), K_(self));
        } else if (OB_FAIL(election->handle_query_leader(msg, result))) {
          if (REACH_TIME_INTERVAL(100 * 1000)) {
            ELECT_ASYNC_LOG(WARN, "handle query leader error", K(ret), K(partition), K(msg));
          }
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_QUERY_LEADER_RESPONSE: {
        ObElectionQueryLeaderResponse msg;
        if (OB_FAIL(msg.deserialize(buf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize query leader response error", K(ret), K(partition), K_(self));
        } else if (OB_FAIL(election->handle_query_leader_response(msg, result))) {
          ELECT_ASYNC_LOG(WARN, "handle query leader repsonse error", K(ret), K(partition), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_NOTIFY_MEMBER_LIST: {
        ELECT_ASYNC_LOG(INFO, "not support now", K(msg_type));
        break;
      }
      default:
        ELECT_ASYNC_LOG(WARN, "unknown message type", K(msg_type));
        ret = OB_UNKNOWN_PACKET;
        break;
    }
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > 50 * 1000) {
      ELECT_ASYNC_LOG(
          WARN, "handle election msg cost too much time", K(msg_type), K(partition), "time", end_time - start_time);
    }
    revert_election_(election);
  }
  return ret;
}

int ObElectionMgr::handle_election_group_req(int msg_type, const char* msgbuf, int64_t limit)
{
  RDLockGuard guard(rwlock_);
  ObElectionRpcResult result;
  int64_t pos = 0;
  return handle_election_group_msg_unlock_(msg_type, msgbuf, limit, pos, result);
}

int ObElectionMgr::handle_election_group_msg_unlock_(
    int msg_type, const char* msgbuf, int64_t limit, int64_t& pos, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;
  ObElectionGroupId eg_id;
  int64_t array_start_pos = 0;
  int64_t eg_version = -1;
  int64_t array_serialize_size = 0;
  ObPartitionArray partition_array;
  bool is_array_deserialized = false;
  ObPartitionArray move_in_failed_array;
  ObIElectionGroup* election_group = NULL;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (NULL == msgbuf) {
    ret = OB_INVALID_ARGUMENT;
  } else if (ObElectionMsgTypeChecker::is_election_group_msg(msg_type)) {
    if (OB_FAIL(eg_id.deserialize(msgbuf, limit, pos))) {
      ELECT_ASYNC_LOG(WARN, "deserialize eg_id error", K(ret));
    } else if (OB_FAIL(serialization::decode_i64(msgbuf, limit, pos, &eg_version))) {
      ELECT_ASYNC_LOG(WARN, "deserialize eg_version error", K(ret));
    } else if (OB_FAIL(serialization::decode_i64(msgbuf, limit, pos, &array_serialize_size))) {
      ELECT_ASYNC_LOG(WARN, "deserialize array_serialize_size error", K(ret));
    } else {
      array_start_pos = pos;
    }
    if (NULL == (election_group = eg_mgr_.get_election_group(eg_id))) {
      // election_group need construct
      if (OB_FAIL(decode_partition_array_buf(msgbuf, limit, pos, partition_array))) {
        ELECT_ASYNC_LOG(WARN, "decode_partition_array_buf failed", K(ret));
      } else {
        is_array_deserialized = true;
        // need lock protected under construct in concurrency
        if (OB_FAIL(eg_mgr_.construct_empty_election_group(eg_id, partition_array))) {
          if (OB_ENTRY_EXIST != ret) {
            ELECT_ASYNC_LOG(WARN, "construct_empty_election_group failed", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        election_group = eg_mgr_.get_election_group(eg_id);
      }
    }
    if (NULL == election_group) {
      ret = OB_ELECTION_GROUP_NOT_EXIST;
    } else if (!election_group->is_need_check_eg_version(eg_version)) {
      // no need check partition_array
      if (!is_array_deserialized) {
        // array not deserialized, update pos
        pos += array_serialize_size;
      }
    } else {
      // need check partition_array
      if (!is_array_deserialized) {
        if (OB_FAIL(decode_partition_array_buf(msgbuf, limit, pos, partition_array))) {
          ELECT_ASYNC_LOG(WARN, "decode_partition_array_buf failed", K(ret));
        } else {
          is_array_deserialized = true;
        }
      }
      if (OB_SUCC(ret)) {
        ObPartitionArray pending_move_out_part_array;
        ObPartitionArray pending_move_in_part_array;
        if (OB_FAIL(election_group->check_eg_version(
                eg_version, partition_array, pending_move_out_part_array, pending_move_in_part_array))) {
          ELECT_ASYNC_LOG(WARN, "check_eg_version failed", K(ret));
        } else {
          common::ObAddr eg_leader;
          // move out partition, not allow failed
          if (pending_move_out_part_array.count() > 0) {
            ret = batch_move_out_partition_(pending_move_out_part_array, eg_id);
          }
          // move in partition, may failed
          if (pending_move_in_part_array.count() > 0) {
            (void)batch_move_in_partition_(pending_move_in_part_array, eg_id, move_in_failed_array);
          }
          // need update eg_version after move in/out
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = election_group->try_update_eg_version(eg_version, partition_array))) {
            ELECT_ASYNC_LOG(WARN, "try_update_eg_version failed", K(tmp_ret));
          }
        }
      }
    }
  } else {
    ret = OB_UNKNOWN_PACKET;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ELECT_ASYNC_LOG(WARN, "unknown packet", K(ret));
    }
  }
  // handle msg
  if (OB_SUCC(ret)) {
    const int64_t start_time = ObTimeUtility::current_time();
    switch (msg_type) {
      case OB_ELECTION_EG_VOTE_PREPARE: {
        ObElectionMsgEGPrepare msg;
        if (OB_FAIL(msg.deserialize(msgbuf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize ObElectionMsgEGPrepare error", K(ret), K_(self));
        } else if (OB_FAIL(election_group->handle_vote_prepare(msg, eg_version, result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle_vote_prepare error", K(ret), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_EG_VOTE_VOTE: {
        ObElectionMsgEGVote msg;
        if (OB_FAIL(msg.deserialize(msgbuf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize ObElectionMsgEGVote error", K(ret), K_(self));
        } else if (OB_FAIL(election_group->handle_vote_vote(msg,
                       eg_version,
                       is_array_deserialized,
                       msgbuf,
                       limit,
                       array_start_pos,
                       partition_array,
                       result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle_vote_vote error", K(ret), K_(self), K(eg_id), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_ELECTION_EG_VOTE_SUCCESS: {
        ObElectionMsgEGSuccess msg;
        if (OB_FAIL(msg.deserialize(msgbuf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize ObElectionMsgEGSuccess error", K(ret), K_(self));
        } else if (OB_FAIL(election_group->handle_vote_success(msg,
                       eg_version,
                       is_array_deserialized,
                       msgbuf,
                       limit,
                       array_start_pos,
                       partition_array,
                       move_in_failed_array,
                       result))) {
          ELECT_ASYNC_LOG(DEBUG, "handle_vote_success error", K(ret), K(msg));
        } else if (move_in_failed_array.count() > 0) {
          int tmp_ret = OB_SUCCESS;
          const int64_t fail_cnt = move_in_failed_array.count();
          // update lease for those move in failed but successfully renew lease partitions
          ObElectionMsgSuccess mock_election_msg(msg.get_cur_leader(),
              msg.get_new_leader(),
              msg.get_T1_timestamp(),
              msg.get_send_timestamp(),
              msg.get_sender(),
              msg.get_lease_time(),
              0,
              0);
          for (int64_t i = 0; i < fail_cnt; ++i) {
            const ObPartitionKey tmp_pkey = move_in_failed_array.at(i);
            ObIElection* tmp_election = NULL;
            ObElectionRpcResult mock_result;
            if (NULL == (tmp_election = get_election_(tmp_pkey))) {
            } else {
              if (OB_SUCCESS != (tmp_ret = tmp_election->handle_vote_success(mock_election_msg, mock_result))) {
                ELECT_ASYNC_LOG(WARN, "handle_vote_success failed", K(tmp_ret), K(tmp_pkey));
              } else {
              }
              revert_election_(tmp_election);
            }
          }
        }
        break;
      }
      case OB_ELECTION_EG_DESTROY: {
        ObElectionMsg msg;
        if (OB_FAIL(msg.deserialize(msgbuf, limit, pos))) {
          ELECT_ASYNC_LOG(WARN, "deserialize ObElectionMsg error", K(ret), K_(self));
        } else if (OB_FAIL(election_group->handle_prepare_destroy_msg(msg, eg_version, result))) {
          ELECT_ASYNC_LOG(WARN, "handle_prepare_destroy_msg error", K(ret), K(msg), K(eg_id));
        } else {
          // do nothing
        }
        break;
      }
      default:
        ELECT_ASYNC_LOG(WARN, "unknown message type", K(msg_type));
        ret = OB_UNKNOWN_PACKET;
        break;
    }
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > 50 * 1000) {
      ELECT_ASYNC_LOG(
          WARN, "handle election_group msg cost too much time", K(msg_type), K(eg_id), "time", end_time - start_time);
    }
  }
  if (NULL != election_group) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = eg_mgr_.revert_election_group(election_group))) {
      ELECT_ASYNC_LOG(WARN, "revert_election_group failed", K(tmp_ret));
    }
  }

  return ret;
}

int decode_partition_array_buf(const char* buf, const int64_t buf_len, int64_t& pos, ObPartitionArray& part_array)
{
  int ret = OB_SUCCESS;
  int64_t pkey_cnt = 0;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &pkey_cnt))) {
    ELECT_ASYNC_LOG(WARN, "deserialize pkey_cnt error", K(ret));
  } else {
    common::ObPartitionKey pkey;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_cnt; ++i) {
      if (OB_FAIL(pkey.deserialize(buf, buf_len, pos))) {
        ELECT_ASYNC_LOG(WARN, "deserialize pkey error", K(ret));
      } else if (OB_FAIL(part_array.push_back(pkey))) {
        ELECT_ASYNC_LOG(WARN, "part_array.push_back failed", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObElectionMgr::handle_election_msg(const ObElectionMsgBuffer& msgbuf, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;
  int32_t msg_type = 0;
  int64_t pos = 0;
  ObPartitionKey partition;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    // ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(serialization::decode_i32(msgbuf.get_data(), msgbuf.get_position(), pos, &msg_type))) {
    ELECT_ASYNC_LOG(WARN, "deserialize msg_type error", K(ret));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    // abort process
  } else if (ObElectionMsgTypeChecker::is_partition_msg(msg_type)) {
    // partition msg
    if (OB_FAIL(partition.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos))) {
      ELECT_ASYNC_LOG(WARN, "deserialize partition error", K(ret));
    } else if (NULL == (election = get_election_(partition))) {
      ret = OB_PARTITION_NOT_EXIST;
    } else {
      ret = handle_election_msg_unlock_(msg_type, partition, msgbuf.get_data(), msgbuf.get_position(), pos, result);
      revert_election_(election);
    }
  } else if (ObElectionMsgTypeChecker::is_election_group_msg(msg_type)) {
    // election_group msg
    ret = handle_election_group_msg_unlock_(msg_type, msgbuf.get_data(), msgbuf.get_position(), pos, result);
  } else {
    ret = OB_UNKNOWN_PACKET;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ELECT_ASYNC_LOG(WARN, "unknown packet", K(ret));
    }
  }

  return ret;
}

int ObElectionMgr::set_candidate(const ObPartitionKey& partition, const int64_t replica_num,
    const ObMemberList& curr_mlist, const int64_t membership_version)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid() || replica_num <= 0 || replica_num > OB_MAX_MEMBER_NUMBER ||
             curr_mlist.get_member_number() <= 0 || !is_valid_membership_version(membership_version)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), K(replica_num), K(curr_mlist), K(membership_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->set_candidate(replica_num, curr_mlist, membership_version))) {
      ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K(partition), K(replica_num), K(curr_mlist));
    } else {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        ELECT_ASYNC_LOG(
            INFO, "set candidate success", K(partition), K(replica_num), K(curr_mlist), K(membership_version));
      }
      ELECT_ASYNC_LOG(
          TRACE, "set candidate success", K(partition), K(replica_num), K(curr_mlist), K(membership_version));
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::get_curr_candidate(const ObPartitionKey& partition, ObMemberList& mlist) const
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->get_curr_candidate(mlist))) {
      ELECT_ASYNC_LOG(WARN, "get curr candidate error", K(ret), K(partition));
    } else {
      // do nothing
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::get_valid_candidate(const ObPartitionKey& partition, ObMemberList& mlist) const
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->get_valid_candidate(mlist))) {
      if (OB_EAGAIN != ret) {
        ELECT_ASYNC_LOG(WARN, "get valid candidate error", K(ret), K(partition));
      }
    } else {
      // do nothing
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::get_all_partition_status(int64_t& inactive_num, int64_t& total_num)
{
  int ret = OB_SUCCESS;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else {
    GetElectionStatusFunctor fn;
    if (OB_FAIL(foreach_election_(fn))) {
      ELECT_ASYNC_LOG(WARN, "for each all election error", K(ret));
    }
    inactive_num = fn.get_inactive_num();
    total_num = fn.get_total_num();
  }
  return ret;
}

int ObElectionMgr::get_leader(const ObPartitionKey& partition, ObAddr& leader, int64_t& leader_poch,
    bool& is_elected_by_changing_leader, ObTsWindows& changing_leader_windows) const
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;
  bool hit = false;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != (election = election_cache_.get(partition.hash())) &&
             const_cast<ObPartitionKey&>(partition) == static_cast<ObElection*>(election)->get_partition()) {
    hit = true;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(election->get_leader(leader, leader_poch, is_elected_by_changing_leader, changing_leader_windows))) {
      ELECT_ASYNC_LOG(DEBUG, "get leader error", K(ret), K(partition));
    } else if (!hit) {
      (void)election_cache_.set(partition.hash(), static_cast<ObElection*>(election));
    } else {
      // do nothing
    }
    if (!hit) {
      revert_election_(election);  // revert only when not hit cache
    } else {                       /*do nothing*/
    }
  }

  return ret;
}

int ObElectionMgr::get_leader(const ObPartitionKey& partition, ObAddr& leader, ObAddr& previous_leader,
    int64_t& leader_poch, bool& is_elected_by_changing_leader) const
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;
  bool hit = false;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != (election = election_cache_.get(partition.hash())) &&
             const_cast<ObPartitionKey&>(partition) == static_cast<ObElection*>(election)->get_partition()) {
    hit = true;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(election->get_leader(leader, previous_leader, leader_poch, is_elected_by_changing_leader))) {
      ELECT_ASYNC_LOG(DEBUG, "get leader error", K(ret), K(partition));
    } else if (!hit) {
      (void)election_cache_.set(partition.hash(), static_cast<ObElection*>(election));
    } else {
      // do nothing
    }
    if (!hit) {
      revert_election_(election);  // revert only when not hit cache
    } else {                       /*do nothing*/
    }
  }

  return ret;
}

int ObElectionMgr::get_current_leader(const ObPartitionKey& partition, ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;
  bool hit = false;

  RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), K(ret));
  } else if (NULL != (election = election_cache_.get(partition.hash())) &&
             const_cast<ObPartitionKey&>(partition) == static_cast<ObElection*>(election)->get_partition()) {
    hit = true;
  } else if (NULL == (election = get_election_(partition))) {
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(election->get_current_leader(leader))) {
      ELECT_ASYNC_LOG(DEBUG, "get current leader error", K(ret), K(partition));
    } else if (!hit) {
      (void)election_cache_.set(partition.hash(), static_cast<ObElection*>(election));
    } else {
      // do nothing
    }
    if (!hit) {
      revert_election_(election);  // revert only when not hit cache
    } else {                       /*do nothing*/
    }
  }

  return ret;
}

int ObElectionMgr::iterate_election_group_info(ObElectionGroupInfoIterator& eg_info_iter)
{
  int ret = OB_SUCCESS;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "election manager is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(eg_mgr_.iterate_election_group_info(eg_info_iter))) {
    ELECT_ASYNC_LOG(WARN, "iterate_election_group_info failed", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObElectionMgr::iterate_election_info(ObElectionInfoIterator& election_info_iter)
{
  int ret = OB_SUCCESS;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "election manager is not running");
    ret = OB_NOT_RUNNING;
  } else {
    IterateElectionFunctor fn(election_info_iter);
    if (OB_FAIL(foreach_election_(fn))) {
      ELECT_ASYNC_LOG(WARN, "for each all election error", K(ret));
    }
  }

  return ret;
}

int ObElectionMgr::revoke_all(const uint32_t revoke_type)
{
  int ret = OB_SUCCESS;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "election manager is not running");
    ret = OB_NOT_RUNNING;
  } else {
    RevokeElectionFunctor fn(revoke_type);
    if (OB_FAIL(foreach_election_(fn))) {
      ELECT_ASYNC_LOG(WARN, "for each all election error", K(ret));
    }
  }

  return ret;
}

int ObElectionMgr::iterate_election_mem_stat(ObElectionMemStatIterator& election_mem_stat_iter)
{
  int ret = OB_SUCCESS;
  static const char* log_time_type_name = "async_log_item_factory";
  static const char* election_type_name = "election_factory";

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "election manager is not running");
    ret = OB_NOT_RUNNING;
  } else {
    ObElectionMemStat log_item_mem_stat;
    ObElectionMemStat election_mem_stat;

    if (OB_FAIL(log_item_mem_stat.init(
            self_, log_time_type_name, ObLogItemFactory::alloc_count_, ObLogItemFactory::release_count_))) {
      ELECT_ASYNC_LOG(WARN, "log item factory mem stat init error", K(ret));
    } else if (OB_FAIL(election_mem_stat_iter.push(log_item_mem_stat))) {
      ELECT_ASYNC_LOG(WARN, "push log item factory mem stat error", K(ret));
    } else if (OB_FAIL(election_mem_stat.init(
                   self_, election_type_name, ObElectionFactory::alloc_count_, ObElectionFactory::release_count_))) {
      ELECT_ASYNC_LOG(WARN, "election factory mem stat init error", K(ret));
    } else if (OB_FAIL(election_mem_stat_iter.push(election_mem_stat))) {
      ELECT_ASYNC_LOG(WARN, "push election factory mem stat error", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObElectionMgr::iterate_election_event_history(ObElectionEventHistoryIterator& election_event_history_iter)
{
  int ret = OB_SUCCESS;
  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "election manager is not running");
    ret = OB_NOT_RUNNING;
  } else {
    const int32_t end_idx = event_hist_array_.get_event_cursor();
    int32_t idx = end_idx;
    const int64_t now = ObClockGenerator::getClock();
    int tmp_ret = OB_SUCCESS;
    ObElectionEventHistory event_history;
    do {
      if (OB_SUCCESS != (tmp_ret = event_hist_array_.get_prev_event_history(now, idx, event_history))) {
        ELECT_ASYNC_LOG(WARN, "get election event history error.", "ret", tmp_ret, K(idx));
      } else if (OB_SUCCESS != (tmp_ret = election_event_history_iter.push(event_history))) {
        if (OB_ITER_END != tmp_ret) {
          ELECT_ASYNC_LOG(WARN, "ObElectionInfoIterator push election event history error.", "ret", tmp_ret, K(idx));
        }
      } else {
        // do nothing
      }
    } while (OB_SUCCESS == tmp_ret && idx != end_idx);
  }
  return ret;
}

int ObElectionMgr::inc_replica_num(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->inc_replica_num())) {
      ELECT_ASYNC_LOG(WARN, "increase replica number error", K(ret), K(partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "increase replica number success", K(partition));
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::dec_replica_num(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    if (OB_FAIL(election->dec_replica_num())) {
      ELECT_ASYNC_LOG(WARN, "decrease replica number error", K(ret), K(partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "decrease replica number success", K(partition));
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::batch_move_in_partition_(const ObPartitionArray& pending_move_in_array,
    const ObElectionGroupId& eg_id, ObPartitionArray& move_in_failed_array)
{
  // call ObElection's method, move in partition in batch
  int ret = OB_SUCCESS;

  int64_t fail_cnt = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "election_group not inited", K(ret));
  } else if (pending_move_in_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int tmp_ret = OB_SUCCESS;
    int64_t array_count = pending_move_in_array.count();
    for (int64_t i = 0; i < array_count; ++i) {
      const ObPartitionKey& tmp_pkey = pending_move_in_array.at(i);
      if (OB_SUCCESS != (tmp_ret = move_into_election_group_unlock_(tmp_pkey, eg_id))) {
        fail_cnt++;
        if (OB_SUCCESS != (tmp_ret = move_in_failed_array.push_back(tmp_pkey))) {
          ELECT_ASYNC_LOG(WARN, "array push_back failed", K(tmp_ret), K(tmp_pkey));
        }
      }
    }
  }
  if (OB_SUCC(ret) && fail_cnt > 0) {
    ret = OB_PARTIAL_FAILED;
  }
  return ret;
}

int ObElectionMgr::move_into_election_group_unlock_(const ObPartitionKey& partition, const ObElectionGroupId& eg_id)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  if (!partition.is_valid() || !eg_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(partition), K(eg_id));
  } else if (NULL == (election = get_election_(partition))) {
    ret = OB_PARTITION_NOT_EXIST;
    ELECT_ASYNC_LOG(WARN, "get election error", K(ret), K(partition));
  } else {
    if (OB_FAIL(election->move_into_election_group(eg_id))) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        ELECT_ASYNC_LOG(WARN, "move_into_election_group failed", K(ret), K(partition), K(eg_id));
      }
    } else {
      // do nothing
    }
    revert_election_(election);
  }

  return ret;
}

int ObElectionMgr::batch_move_out_partition_(
    const ObPartitionArray& pending_move_out_array, const ObElectionGroupId& eg_id)
{
  // call ObElection's method, move out partition in batch
  int ret = OB_SUCCESS;

  int64_t fail_cnt = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "election_mgr not inited", K(ret));
  } else if (pending_move_out_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "pending_move_out_array is empty", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    int64_t array_count = pending_move_out_array.count();
    for (int64_t i = 0; i < array_count; ++i) {
      const ObPartitionKey& tmp_pkey = pending_move_out_array.at(i);
      if (OB_SUCCESS != (tmp_ret = move_out_election_group_unlock_(tmp_pkey, eg_id))) {
        fail_cnt++;
        ELECT_ASYNC_LOG(WARN, "move_out_election_group failed", K(tmp_ret), K(fail_cnt), K(tmp_pkey), K(eg_id));
      }
    }
  }
  if (OB_SUCC(ret) && fail_cnt > 0) {
    ret = OB_PARTIAL_FAILED;
  }
  return ret;
}

int ObElectionMgr::move_out_election_group(const ObPartitionKey& partition, const ObElectionGroupId& eg_id)
{
  int ret = OB_SUCCESS;
  const int64_t RDLOCK_TIMEOUT_US = 10 * 1000;
  int tmp_ret = OB_SUCCESS;
  while (true) {
    RDLockGuardWithTimeout guard(rwlock_, RDLOCK_TIMEOUT_US + ObTimeUtility::current_time(), tmp_ret);
    if (OB_SUCCESS == tmp_ret) {
      ret = move_out_election_group_unlock_(partition, eg_id);
      break;
    } else if (false == ATOMIC_LOAD(&is_running_)) {
      // election_mgr already stop, give up move out, or deadlock
      ret = OB_ELECTION_MGR_NOT_RUNNING;
      break;
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObElectionMgr::move_out_election_group_unlock_(const ObPartitionKey& partition, const ObElectionGroupId& eg_id)
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  if (!partition.is_valid() || !eg_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(partition), K(eg_id));
  } else if (NULL == (election = get_election_(partition))) {
    ret = OB_PARTITION_NOT_EXIST;
    ELECT_ASYNC_LOG(WARN, "get election error", K(ret), K(partition));
  } else {
    if (OB_FAIL(election->move_out_election_group(eg_id))) {
      ELECT_ASYNC_LOG(WARN, "move_out_election_group failed", K(ret), K(partition), K(eg_id));
    } else {
      // do nothing
    }
    revert_election_(election);
  }

  return ret;
}

void ObElectionMgr::revert_election(const ObIElection* election) const
{
  if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
  } else if (nullptr == election) {
    ELECT_ASYNC_LOG(ERROR, "election is nullptr");
  } else {
    RDLockGuard guard(rwlock_);
    revert_election_(election);
  }
}

ObIElection* ObElectionMgr::get_election_(const ObPartitionKey& partition) const
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;

  if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
  } else if (OB_FAIL(const_cast<ElectionMap&>(election_map_).get(partition, election))) {
    // ELECT_ASYNC_LOG(WARN, "get election error", K(ret), K(partition));
  } else if (NULL == election) {
    ELECT_ASYNC_LOG(WARN, "election is null", KP(election), K(partition));
  } else {
    // do nothing
  }

  return election;
}

void ObElectionMgr::revert_election_(const ObIElection* election) const
{
  if (NULL == election) {
    ELECT_ASYNC_LOG(ERROR, "someone is trying to revert a null pointer");
  } else {
    const_cast<ElectionMap&>(election_map_).revert(const_cast<ObIElection*>(election));
  }
}

int ObElectionMgr::stop_()
{
  int ret = OB_SUCCESS;

  if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
  } else {
    ATOMIC_STORE(&is_running_, false);
    (void)gc_thread_.stop();
    if (OB_FAIL(election_map_.for_each(stop_all_election))) {
      ELECT_ASYNC_LOG(WARN, "election manager stop error", K(ret));
    } else if (OB_FAIL(eg_mgr_.stop())) {
      ELECT_ASYNC_LOG(WARN, "election group manager stop error", K(ret));
    } else if (OB_FAIL(tw_.stop())) {
      ELECT_ASYNC_LOG(WARN, "ObTimeWheel stop error", K(ret));
    } else if (OB_FAIL(tw_.wait())) {
      ELECT_ASYNC_LOG(WARN, "ObTimeWheel wait error", K(ret));
    } else {
      ELECT_ASYNC_LOG(INFO, "election manager stop success");
    }
  }

  return ret;
}

int ObElectionMgr::wait_()
{
  ELECT_ASYNC_LOG(INFO, "election manager wait success");
  return OB_SUCCESS;
}

ObIElection* ObElectionFactory::alloc(const uint64_t tenant_id)
{
  ObIElection* e = NULL;
  int tmp_ret = OB_SUCCESS;
  ObTenantMutilAllocator* allocator = NULL;

  if (tenant_id <= 0) {
    ELECT_ASYNC_LOG(WARN, "invalid arguments", K(tenant_id));
  } else if (OB_SUCCESS != (tmp_ret = TMA_MGR_INSTANCE.get_tenant_mutil_allocator(tenant_id, allocator))) {
    ELECT_ASYNC_LOG(WARN, "get_tenant_log_allocator failed", K(tmp_ret), K(tenant_id));
  } else if (NULL == (e = allocator->alloc_election())) {
    ELECT_ASYNC_LOG(WARN, "alloc election error");
  } else {
    (void)ATOMIC_FAA(&alloc_count_, 1);
  }

  return e;
}

void ObElectionFactory::release(ObIElection* e)
{
  if (NULL == e) {
    ELECT_ASYNC_LOG(WARN, "invalid arguments", "election", OB_P(e));
  } else {
    ObElection* tmp_election = NULL;
    if (NULL == (tmp_election = static_cast<ObElection*>(e))) {
      ELECT_ASYNC_LOG(WARN, "static_cast return NULL", "election", OB_P(e));
    } else {
      common::ob_slice_free_election(tmp_election);
    }
    (void)ATOMIC_FAA(&release_count_, 1);
  }
}

bool IterateElectionFunctor::operator()(const common::ObPartitionKey& partition, ObIElection* e)
{
  int tmp_ret = OB_SUCCESS;
  bool bool_ret = false;
  ObElectionInfo election_info;

  if (!partition.is_valid() || OB_ISNULL(e)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), KP(e));
    tmp_ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (tmp_ret = e->get_election_info(election_info))) {
    ELECT_ASYNC_LOG(WARN, "get election info error.", "ret", tmp_ret);
  } else if (OB_SUCCESS != (tmp_ret = election_info_iter_.push(election_info))) {
    ELECT_ASYNC_LOG(WARN, "ObElectionInfoIterator push election info error.", "ret", tmp_ret);
  } else {
    bool_ret = true;
  }

  return bool_ret;
}

bool RevokeElectionFunctor::operator()(const common::ObPartitionKey& partition, ObIElection* e)
{
  bool bool_ret = false;

  if (!partition.is_valid() || OB_ISNULL(e)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), KP(e));
  } else {
    ELECT_ASYNC_LOG(INFO, "take the initiative leader revoke", K(partition));
    e->leader_revoke(revoke_type_);
    bool_ret = true;
  }

  return bool_ret;
}

bool GetElectionStatusFunctor::operator()(const common::ObPartitionKey& partition, ObIElection* e)
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;

  if (!partition.is_valid() || OB_ISNULL(e)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), KP(e));
  } else {
    total_num_++;
    common::ObAddr leader;
    common::ObAddr prev_leader;
    int64_t leader_epoch = 0;
    bool is_elected_by_changing_leader = false;
    if (OB_SUCCESS != (tmp_ret = e->get_leader(leader, prev_leader, leader_epoch, is_elected_by_changing_leader))) {
      inactive_num_++;
    }
    bool_ret = true;
  }

  return bool_ret;
}

int ObElectionMgr::get_timestamp(const ObPartitionKey& partition, int64_t& gts, ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  ObIElection* election = NULL;
  bool hit = false;

  RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != (election = election_cache_.get(partition.hash())) &&
             const_cast<ObPartitionKey&>(partition) == static_cast<ObElection*>(election)->get_partition()) {
    hit = true;
  } else if (NULL == (election = get_election_(partition))) {
    ELECT_ASYNC_LOG(WARN, "get election error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(election->get_timestamp(gts, leader))) {
      ELECT_ASYNC_LOG(DEBUG, "get election timestamp failed", K(ret), K(partition));
    } else if (!hit) {
      (void)election_cache_.set(partition.hash(), static_cast<ObElection*>(election));
    } else {
      // do nothing
    }
    if (!hit) {
      revert_election_(election);  // revert only when not hit cache
    } else {                       /*do nothing*/
    }
  }

  return ret;
}

}  // namespace election
}  // namespace oceanbase
