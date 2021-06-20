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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_MGR_
#define OCEANBASE_ELECTION_OB_ELECTION_MGR_

#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "share/ob_define.h"
#include "common/ob_simple_iterator.h"
#include "common/ob_member_list.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/transaction/ob_time_wheel.h"
#include "storage/transaction/ob_gts_mgr.h"
#include "ob_election.h"
#include "ob_election_async_log.h"
#include "ob_election_base.h"
#include "ob_election_event_history.h"
#include "ob_election_group_mgr.h"
#include "ob_election_info.h"
#include "ob_election_mem_stat.h"
#include "ob_election_rpc.h"
#include "ob_election_gc_thread.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
class ObAddr;
}  // namespace common

namespace obrpc {
class ObBatchRpc;
class ObElectionRpcResult;
class ObElectionRpcProxy;
}  // namespace obrpc

namespace election {
class ObElectionMemStat;
class ObElectionMsgBuffer;
class ObIElectionCallback;
class ObIElectionGroupPriorityGetter;
}  // namespace election

namespace election {

int decode_partition_array_buf(
    const char* buf, const int64_t buf_len, int64_t& pos, common::ObPartitionArray& part_array);
typedef common::ObSimpleIterator<ObElectionInfo, common::ObModIds::OB_ELECTION_VIRTAUL_TABLE_ELECTION_INFO, 16>
    ObElectionInfoIterator;
typedef common::ObSimpleIterator<ObElectionMemStat, common::ObModIds::OB_ELECTION_VIRTAUL_TABLE_MEM_STAT, 16>
    ObElectionMemStatIterator;
typedef common::ObSimpleIterator<ObElectionEventHistory, common::ObModIds::OB_ELECTION_VIRTAUL_TABLE_EVENT_HISTORY,
    1024>
    ObElectionEventHistoryIterator;

class ObIElectionMgr : public transaction::ObITimestampService {
public:
  ObIElectionMgr()
  {}
  virtual ~ObIElectionMgr()
  {}
  virtual int init(const common::ObAddr& self, obrpc::ObBatchRpc* batch_rpc, ObIElectionGroupPriorityGetter* eg_cb) = 0;
  virtual void destroy() = 0;
  virtual int start_partition(const common::ObPartitionKey& partition) = 0;
  virtual int start_partition(const common::ObPartitionKey& partition, const common::ObAddr& leader,
      const int64_t lease_start, int64_t& leader_epoch) = 0;
  virtual int stop_partition(const common::ObPartitionKey& partition) = 0;
  virtual int start() = 0;
  virtual int wait() = 0;
  virtual int stop() = 0;

public:
  // add partition
  virtual int add_partition(const common::ObPartitionKey& partition, const int64_t replica_num,
      ObIElectionCallback* election_cb, ObIElection*& election) = 0;
  // remove partition
  virtual int remove_partition(const common::ObPartitionKey& partition) = 0;
  // change leader async
  virtual int change_leader_async(const common::ObPartitionKey& partition, const common::ObAddr& leader,
      common::ObTsWindows& changing_leader_windows) = 0;
  virtual int force_leader_async(const common::ObPartitionKey& partition) = 0;
  virtual int leader_revoke(const common::ObPartitionKey& partition, const uint32_t revoke_type) = 0;
  virtual int revoke_all(const uint32_t revoke_type) = 0;
  virtual int move_out_election_group(const ObPartitionKey& partition, const ObElectionGroupId& eg_id) = 0;

public:
  virtual int handle_election_msg(const ObElectionMsgBuffer& msgbuf, obrpc::ObElectionRpcResult& result) = 0;

public:
  virtual int set_candidate(const common::ObPartitionKey& partition, const int64_t replica_num,
      const common::ObMemberList& curr_mlist, const int64_t membership_version) = 0;
  // get current candidate
  virtual int get_curr_candidate(const common::ObPartitionKey& partition, common::ObMemberList& mlist) const = 0;
  virtual int get_valid_candidate(const common::ObPartitionKey& partition, common::ObMemberList& mlist) const = 0;
  // get current leader
  virtual int get_leader(const common::ObPartitionKey& partition, common::ObAddr& leader,
      common::ObAddr& previous_leader, int64_t& leader_epoch, bool& is_elected_by_changing_leader) const = 0;
  virtual int get_leader(const common::ObPartitionKey& partition, common::ObAddr& leader, int64_t& leader_epoch,
      bool& is_elected_by_changing_leader, common::ObTsWindows& changing_leader_windows) const = 0;
  virtual int get_current_leader(const common::ObPartitionKey& partition, common::ObAddr& leader) const = 0;
  virtual int inc_replica_num(const common::ObPartitionKey& partition) = 0;
  virtual int dec_replica_num(const common::ObPartitionKey& partition) = 0;
  virtual int get_all_partition_status(int64_t& inactive_num, int64_t& total_num) = 0;
  // virtual int get_election(const common::ObPartitionKey &partition, ObIElection *&election) const = 0;
  virtual void revert_election(const ObIElection* election) const = 0;
};

class ObElectionFactory {
public:
  static ObIElection* alloc(const uint64_t tenant_id);
  static void release(ObIElection* e);
  static int64_t alloc_count_;
  static int64_t release_count_;
};

class ElectionAllocHandle {
public:
  typedef common::LinkHashNode<common::ObPartitionKey> Node;
  static int64_t TOTAL_RELEASE_COUNT;  // for unittest

  static ObIElection* alloc_value()
  {
    // do not allow alloc val in hashmap
    return NULL;
  }
  static void free_value(ObIElection* e)
  {
    ObElectionFactory::release(e);
    e = NULL;
    (void)ATOMIC_FAA(&TOTAL_RELEASE_COUNT, 1);  // for unittest
  }
  static Node* alloc_node(ObIElection* e)
  {
    UNUSED(e);
    return op_alloc(Node);
  }
  static void free_node(Node* node)
  {
    op_free(node);
    node = NULL;
  }
};

template <typename T, int64_t CACHE_NUM>
class ObPointerCache {
public:
  ObPointerCache()
  {
    reset();
  }
  ~ObPointerCache()
  {
    destroy();
  }
  int init()
  {
    reset();
    return common::OB_SUCCESS;
  }
  void reset()
  {
    for (int64_t i = 0; i < CACHE_NUM; i++) {
      cache_[i] = NULL;
    }
  }
  void destroy()
  {
    reset();
  }
  T* get(const uint64_t hv)
  {
    return ATOMIC_LOAD(&(cache_[hv % CACHE_NUM]));
  }
  int set(const uint64_t hv, T* p)
  {
    ATOMIC_SET(&(cache_[hv % CACHE_NUM]), p);
    return common::OB_SUCCESS;
  }
  int remove(const uint64_t hv)
  {
    ATOMIC_SET(&(cache_[hv % CACHE_NUM]), NULL);
    return common::OB_SUCCESS;
  }

private:
  T* cache_[CACHE_NUM];
};

class ObElectionMgr : public ObIElectionMgr {
public:
  // typedef common::ObConcurrentHashMap<common::ObPartitionKey, ObIElection *> ElectionMap;
  static int64_t TOTAL_ADD_COUNT;
  static int64_t TOTAL_REMOVE_COUNT;
  typedef common::RefHandle ElectionRefHandle;
  typedef common::ObLinkHashMap<common::ObPartitionKey, ObIElection, ElectionAllocHandle, ElectionRefHandle>
      ElectionMap;
  ObElectionMgr() : rpc_(NULL), is_inited_(false), is_running_(false)
  {}
  virtual ~ObElectionMgr()
  {
    destroy();
  }
  int init(const common::ObAddr& self, obrpc::ObBatchRpc* batch_rpc, ObIElectionGroupPriorityGetter* eg_cb);
  // for test
  int init(const common::ObAddr& self, ObIElectionRpc* rpc);
  int init(const common::ObAddr& self, ObIElectionRpc* rpc, ObIElectionGroupPriorityGetter* eg_cb);
  void destroy();

  int start_partition(const common::ObPartitionKey& partition);
  int start_partition(const common::ObPartitionKey& partition, const common::ObAddr& leader, const int64_t lease_start,
      int64_t& leader_epoch);
  int stop_partition(const common::ObPartitionKey& partition);
  int start();
  int wait();
  int stop();

public:
  int add_partition(const common::ObPartitionKey& partition, const int64_t replica_num,
      ObIElectionCallback* election_cb, ObIElection*& election);
  int remove_partition(const common::ObPartitionKey& partition);
  int change_leader_async(const common::ObPartitionKey& partition, const common::ObAddr& leader,
      common::ObTsWindows& changing_leader_windows);
  int force_leader_async(const common::ObPartitionKey& partition);
  int leader_revoke(const common::ObPartitionKey& partition, const uint32_t revoke_type);
  int revoke_all(const uint32_t revoke_type);

public:
  int handle_election_group_req(int msg_type, const char* buf, int64_t limit);
  int handle_election_req(int msg_type, common::ObPartitionKey& pkey, const char* buf, int64_t limit);
  int handle_election_msg(const ObElectionMsgBuffer& msgbuf, obrpc::ObElectionRpcResult& result);

public:
  int set_candidate(const common::ObPartitionKey& partition, const int64_t replica_num,
      const common::ObMemberList& curr_mlist, const int64_t membership_version);
  int get_curr_candidate(const common::ObPartitionKey& partition, common::ObMemberList& mlist) const;
  int get_valid_candidate(const common::ObPartitionKey& partition, common::ObMemberList& mlist) const;
  int get_leader(const common::ObPartitionKey& partition, common::ObAddr& leader, common::ObAddr& previous_leader,
      int64_t& leader_epoch, bool& is_elected_by_changing_leader) const;
  int get_leader(const common::ObPartitionKey& partition, common::ObAddr& leader, int64_t& leader_epoch,
      bool& is_elected_by_changing_leader, common::ObTsWindows& changing_leader_windows) const;
  int get_current_leader(const common::ObPartitionKey& partition, common::ObAddr& leader) const;
  int iterate_election_group_info(ObElectionGroupInfoIterator& eg_info_iter);
  int iterate_election_info(ObElectionInfoIterator& election_info_iter);
  int iterate_election_mem_stat(ObElectionMemStatIterator& election_mem_stat_iter);
  int iterate_election_event_history(ObElectionEventHistoryIterator& election_event_history_iter);
  int inc_replica_num(const common::ObPartitionKey& partition);
  int dec_replica_num(const common::ObPartitionKey& partition);
  int get_all_partition_status(int64_t& inactive_num, int64_t& total_num);
  int get_timestamp(const common::ObPartitionKey& partition, int64_t& gts, common::ObAddr& leader) const;
  int move_out_election_group(const ObPartitionKey& partition, const ObElectionGroupId& eg_id);
  // int get_election(const common::ObPartitionKey &partition, ObIElection *&election) const;
  void revert_election(const ObIElection* election) const;

private:
  static const int64_t TIMER_THREAD_COUNT = 6;
  // precision of time wheel is 1000us
  static const int64_t TIME_WHEEL_PRECISION_US = 1000;
  static const int64_t CACHE_NUM = 17313;

private:
  ObIElection* get_election_(const common::ObPartitionKey& partition) const;
  void revert_election_(const ObIElection* election) const;
  template <typename Fn>
  int foreach_election_(Fn& fn)
  {
    return election_map_.for_each(fn);
  }
  typedef ObPointerCache<ObElection, CACHE_NUM> ObElectionPointerCache;
  int stop_();
  int wait_();
  int batch_move_in_partition_(const common::ObPartitionArray& pending_move_in_array, const ObElectionGroupId& eg_id,
      ObPartitionArray& move_in_failed_array);
  int move_into_election_group_unlock_(const ObPartitionKey& partition, const ObElectionGroupId& eg_id);
  int batch_move_out_partition_(const common::ObPartitionArray& pending_move_out_array, const ObElectionGroupId& eg_id);
  int move_out_election_group_unlock_(const ObPartitionKey& partition, const ObElectionGroupId& eg_id);
  int handle_election_group_msg_unlock_(
      int msg_type, const char* msgbuf, int64_t limit, int64_t& pos, obrpc::ObElectionRpcResult& result);
  int handle_election_msg_unlock_(int msg_type, ObPartitionKey& partition, const char* buf, const int64_t limit,
      int64_t& pos, obrpc::ObElectionRpcResult& result);

protected:
  ObIElectionRpc* rpc_;

private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RDLockGuard;
  typedef RWLock::WLockGuard WRLockGuard;
  typedef RWLock::RLockGuardWithTimeout RDLockGuardWithTimeout;

private:
  bool is_inited_;
  bool is_running_;
  ElectionMap election_map_;
  common::ObAddr self_;
  ObElectionBatchRpc batch_rpc_def_;
  common::ObTimeWheel tw_;
  mutable RWLock rwlock_;
  mutable ObElectionPointerCache election_cache_;
  ObElectionGroupMgr eg_mgr_;
  ObElectionEventHistoryArray event_hist_array_;
  ObElectionGCThread gc_thread_;
};

class IterateElectionFunctor {
public:
  explicit IterateElectionFunctor(ObElectionInfoIterator& election_info_iter) : election_info_iter_(election_info_iter)
  {}
  bool operator()(const common::ObPartitionKey& partition, ObIElection* e);

private:
  ObElectionInfoIterator& election_info_iter_;
};

class RevokeElectionFunctor {
public:
  explicit RevokeElectionFunctor(const uint32_t revoke_type) : revoke_type_(revoke_type)
  {}
  bool operator()(const common::ObPartitionKey& partition, ObIElection* e);

private:
  uint32_t revoke_type_;
};

class GetElectionStatusFunctor {
public:
  explicit GetElectionStatusFunctor() : total_num_(0), inactive_num_(0)
  {}
  bool operator()(const common::ObPartitionKey& partition, ObIElection* e);
  int64_t get_total_num()
  {
    return total_num_;
  }
  int64_t get_inactive_num()
  {
    return inactive_num_;
  }

private:
  int64_t total_num_;
  int64_t inactive_num_;
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_MGR_
