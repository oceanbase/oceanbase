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

#ifndef OCEANBASE_DUP_TABLE_H_
#define OCEANBASE_DUP_TABLE_H_

#include "storage/transaction/ob_trans_factory.h"
#include "storage/transaction/ob_dup_table_rpc.h"
#include "storage/transaction/ob_trans_timer.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/container/ob_mask_set2.h"

namespace oceanbase {

namespace storage {
class ObPartitionService;
}

namespace transaction {
class ObTransService;

typedef common::LinkHashNode<common::ObAddr> DupTableLeaseInfoHashNode;
typedef common::LinkHashValue<common::ObAddr> DupTableLeaseInfoHashValue;

class ObDupTableLeaseInfo : public DupTableLeaseInfoHashValue {
public:
  ObDupTableLeaseInfo()
  {
    reset();
  }
  ~ObDupTableLeaseInfo()
  {
    destroy();
  }
  int update_lease_expired_ts(const int64_t lease_interval_us);
  int update_cur_log_id(const uint64_t cur_log_id);
  int64_t get_lease_expired_ts() const
  {
    return ATOMIC_LOAD(&lease_expired_ts_);
  }
  uint64_t get_cur_log_id() const
  {
    return ATOMIC_LOAD(&cur_log_id_);
  }
  bool is_lease_expired() const;
  void reset();
  void destroy();
  TO_STRING_KV(K_(lease_expired_ts), K_(cur_log_id));

private:
  // expire time of lease, -1 means already expired
  int64_t lease_expired_ts_;
  uint64_t cur_log_id_;
};

// for virtual table display only
class ObDupTableLeaseInfoStat {
public:
  ObDupTableLeaseInfoStat(const common::ObAddr& addr, const int64_t lease_expired_ts, const uint64_t cur_log_id)
      : addr_(addr), lease_expired_ts_(lease_expired_ts), cur_log_id_(cur_log_id)
  {}
  ObDupTableLeaseInfoStat()
  {
    reset();
  }
  ~ObDupTableLeaseInfoStat()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  TO_STRING_KV(K_(addr), K_(lease_expired_ts), K_(cur_log_id));

private:
  common::ObAddr addr_;
  int64_t lease_expired_ts_;
  uint64_t cur_log_id_;
};

// statistics data about lease request
class ObDupTableLeaseRequestStatistics {
public:
  ObDupTableLeaseRequestStatistics()
  {
    reset();
  }
  ~ObDupTableLeaseRequestStatistics()
  {
    reset();
  }
  void reset()
  {
    request_count_ = 0;
    resp_succ_count_ = 0;
    resp_lease_expired_count_ = 0;
    resp_log_too_old_count_ = 0;
    total_rt_ = 0;
    last_print_ts_ = 0;
  }
  void inc_request_count()
  {
    request_count_++;
  }
  void inc_resp_succ_count()
  {
    resp_succ_count_++;
  }
  void inc_resp_lease_expired_count()
  {
    resp_lease_expired_count_++;
  }
  void inc_resp_log_too_old_count()
  {
    resp_log_too_old_count_++;
  }
  void add_total_rt(const int64_t rt)
  {
    total_rt_ += rt;
  }
  void statistics(const common::ObPartitionKey& pkey);

private:
  int64_t request_count_;
  int64_t resp_succ_count_;
  int64_t resp_lease_expired_count_;
  int64_t resp_log_too_old_count_;
  int64_t total_rt_;
  // record the last print time
  int64_t last_print_ts_;
};

typedef common::ObSEArray<ObDupTableLeaseInfoStat, 4> ObDupTableLeaseInfoArray;

class ObDupTablePartitionInfo {
public:
  ObDupTablePartitionInfo()
  {
    reset();
  }
  ~ObDupTablePartitionInfo()
  {
    destroy();
  }
  int init(ObPartitionTransCtxMgr* partition_mgr);
  void reset();
  void destroy();
  int handle_lease_response(const ObDupTableLeaseResponseMsg& msg);
  int handle_redo_log_sync_request(const ObRedoLogSyncRequestMsg& msg, storage::ObPartitionService* partition_service);
  bool is_lease_expired() const
  {
    return ATOMIC_LOAD(&lease_expired_ts_) <= ObTimeUtility::current_time();
  }
  bool need_renew_lease() const;
  bool is_serving();
  int update_replay_log_id(const uint64_t cur_log_id);
  uint64_t get_replay_log_id() const
  {
    return ATOMIC_LOAD(&replay_log_id_);
  }
  ObDupTableLeaseRequestStatistics& get_lease_request_statistics()
  {
    return lease_request_statistics_;
  }
  bool need_refresh_location();
  void statistics(const common::ObPartitionKey& pkey)
  {
    lease_request_statistics_.statistics(pkey);
  }

private:
  bool check_trans_log_id_replayed_(const ObTransID& trans_id, const uint64_t log_id);

private:
  ObPartitionTransCtxMgr* partition_mgr_;
  // expire time of lease
  int64_t lease_expired_ts_;
  // leader's log id when the copy replica registered successfully
  uint64_t leader_log_id_;
  // the continuous replayed log id of the partition
  uint64_t replay_log_id_;
  // dup table will only trigger location refresh when needed
  bool need_refresh_location_now_;
  ObDupTableLeaseRequestStatistics lease_request_statistics_;
};

class DupTableLeaseInfoAlloc {
public:
  ObDupTableLeaseInfo* alloc_value()
  {
    return NULL;
  }
  void free_value(ObDupTableLeaseInfo* info)
  {
    if (NULL != info) {
      ObDupTableLeaseInfoFactory::release(info);
      info = NULL;
    }
  }
  DupTableLeaseInfoHashNode* alloc_node(ObDupTableLeaseInfo* info)
  {
    UNUSED(info);
    return op_alloc(DupTableLeaseInfoHashNode);
  }
  void free_node(DupTableLeaseInfoHashNode* node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }
};

typedef common::ObLinkHashMap<common::ObAddr, ObDupTableLeaseInfo, DupTableLeaseInfoAlloc, common::RefHandle, 2>
    DupTableLeaseInfoHashMap;

class GenPlaFromDupTableLeaseHashMapFunctor {
public:
  GenPlaFromDupTableLeaseHashMapFunctor(ObAddrLogIdArray& addr_logid_array, const uint64_t log_id)
      : addr_logid_array_(addr_logid_array), log_id_(log_id), err_(OB_SUCCESS)
  {}

  bool operator()(const common::ObAddr& addr, ObDupTableLeaseInfo* info)
  {
    bool need_remove = false;
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!addr.is_valid()) || OB_ISNULL(info)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(addr), KP(info));
    } else if (info->is_lease_expired()) {
      // delete nodes with expired lease
      need_remove = true;
    } else if (info->get_cur_log_id() >= log_id_) {
      // when the max continuouly replayed log id brought by heartbeat
      // is greater than the log id that need to be synced,
      // there is no need to query this replica
    } else if (INT64_MAX == info->get_lease_expired_ts()) {
      // there is no need to query a replica in init state
    } else {
      ObAddrLogId addr_logid(addr, log_id_);
      if (OB_FAIL(addr_logid_array_.push_back(addr_logid))) {
        TRANS_LOG(WARN, "push addr and logid error", KR(ret), K(addr), K_(log_id));
      }
    }
    if (OB_FAIL(ret)) {
      err_ = ret;
    }

    return need_remove;
  }
  int return_err() const
  {
    return err_;
  }

private:
  ObAddrLogIdArray& addr_logid_array_;
  uint64_t log_id_;
  int err_;
};

class PrintDupTableLeaseHashMapFunctor {
public:
  PrintDupTableLeaseHashMapFunctor(common::ObPartitionKey& pkey) : pkey_(pkey)
  {}
  ~PrintDupTableLeaseHashMapFunctor()
  {
    destroy();
  }
  bool operator()(const common::ObAddr& addr, ObDupTableLeaseInfo* info)
  {
    int ret = OB_SUCCESS;

    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ObDupTableLeaseInfo is NULL", KR(ret), K_(pkey));
    } else {
      ObDupTableLeaseInfoStat stat(addr, info->get_lease_expired_ts(), info->get_cur_log_id());
      if (OB_FAIL(lease_list_.push_back(stat))) {
        TRANS_LOG(WARN, "push ObDupTableLeaseInfo error", KR(ret), K_(pkey));
      }
    }

    return lease_list_.count() < DUP_TABLE_LEASE_LIST_MAX_COUNT;
  }
  void reset();
  void destroy()
  {
    reset();
  }
  const ObDupTableLeaseInfoArray& get_dup_table_lease_list() const
  {
    return lease_list_;
  }
  TO_STRING_KV(K_(pkey), K_(lease_list));

private:
  common::ObPartitionKey& pkey_;
  ObDupTableLeaseInfoArray lease_list_;
};

class ObDupTableLeaseStatistics {
public:
  ObDupTableLeaseStatistics()
  {
    reset();
  }
  ~ObDupTableLeaseStatistics()
  {
    reset();
  }
  void reset()
  {
    not_master_count_ = 0;
    get_lease_info_err_count_ = 0;
    insert_lease_info_err_count_ = 0;
    rpc_err_count_ = 0;
    not_dup_table_count_ = 0;
    last_print_ts_ = 0;
  }
  void inc_not_master_count()
  {
    not_master_count_++;
  }
  void inc_get_lease_info_err_count()
  {
    get_lease_info_err_count_++;
  }
  void inc_insert_lease_info_err_count()
  {
    insert_lease_info_err_count_++;
  }
  void inc_rpc_err_count()
  {
    rpc_err_count_++;
  }
  void inc_not_dup_table_count()
  {
    not_dup_table_count_++;
  }
  void statistics(const common::ObPartitionKey& pkey);

private:
  // The following situations are unexpected and need to be counted
  uint32_t not_master_count_;
  uint32_t get_lease_info_err_count_;
  uint32_t insert_lease_info_err_count_;
  uint32_t rpc_err_count_;
  uint32_t not_dup_table_count_;
  // record the last print time
  int64_t last_print_ts_;
};

class ObDupTablePartitionMgr {
public:
  ObDupTablePartitionMgr() : dup_table_lease_infos_(1 << 7)
  {
    reset();
  }
  ~ObDupTablePartitionMgr()
  {
    destroy();
  }
  int init(storage::ObPartitionService* partition_service, const common::ObPartitionKey& partition,
      ObPartitionTransCtxMgr* partition_mgr, const bool is_master);
  // When each copy replica applies for a lease from the replica leader, it calls this interface
  int handle_lease_request(const ObDupTableLeaseRequestMsg& request);
  int handle_redo_log_sync_response(const ObRedoLogSyncResponseMsg& msg);
  void reset();
  void destroy();
  int generate_redo_log_sync_set(
      common::ObMaskSet2<ObAddrLogId>& msg_mask_set, ObAddrLogIdArray& dup_table_lease_addrs, const uint64_t log_id);
  int update_cur_log_id(const uint64_t log_id);
  int leader_revoke();
  int leader_active(const uint64_t cur_log_id, const bool election_by_changing_leader);
  void print_lease_info();
  bool is_serving() const;
  const DupTableLeaseInfoHashMap& get_dup_table_lease_info_hashmap() const
  {
    return dup_table_lease_infos_;
  }
  bool is_master() const
  {
    return is_master_;
  }
  uint64_t get_cur_log_id() const
  {
    return cur_log_id_;
  }
  void statistics()
  {
    lease_statistics_.statistics(partition_);
  }

private:
  int update_lease_expired_ts_and_return_(
      bool need_update, ObDupTableLeaseInfo* lease_info, const ObDupTableLeaseRequestMsg& request);
  void check_is_dup_table_();

public:
  // the maximum number of missing logs allowed when the replica requests a lease
  static const int MAX_ALLOWED_LOG_MISSING_COUNT = 500;

private:
  storage::ObPartitionService* partition_service_;
  ObPartitionTransCtxMgr* partition_mgr_;
  common::ObPartitionKey partition_;
  DupTableLeaseInfoHashMap dup_table_lease_infos_;
  // The latest maximum log id written on the leader,
  // including those that has been written but has not formed a majority
  uint64_t cur_log_id_;
  // When the server is switched to leader, need to wait for
  // lease time before it can serve externally
  int64_t start_serving_ts_;
  bool is_master_;
  bool is_dup_table_;
  ObDupTableLeaseStatistics lease_statistics_;
};

typedef common::LinkHashValue<common::ObPartitionKey> ObDupTableLeaseTaskHashValue;

class ObDupTableLeaseTask : public ObITimeoutTask, public ObDupTableLeaseTaskHashValue {
public:
  ObDupTableLeaseTask() : is_inited_(false), trans_service_(NULL)
  {}
  virtual ~ObDupTableLeaseTask()
  {}

  int init(const common::ObPartitionKey pkey, ObTransService* trans_service);
  void reset();

public:
  virtual void runTimerTask() override;
  virtual uint64_t hash() const override
  {
    return pkey_.hash();
  }

private:
  bool is_inited_;
  ObTransService* trans_service_;
  common::ObPartitionKey pkey_;
};

class ObDupTableRedoSyncTask : public ObTransTask {
public:
  ObDupTableRedoSyncTask() : ObTransTask(ObTransRetryTaskType::UNKNOWN)
  {
    reset();
  }
  ~ObDupTableRedoSyncTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int make(const int64_t task_type, const ObTransID& trans_id, const common::ObPartitionKey& partition,
      const uint64_t log_id, const int64_t log_type, const int64_t timestamp);
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  bool is_valid() const;
  int64_t get_log_type() const
  {
    return log_type_;
  }
  int64_t get_timestamp() const
  {
    return timestamp_;
  }
  int64_t get_last_generate_mask_set_ts() const
  {
    return last_generate_mask_set_ts_;
  }
  void set_last_generate_mask_set_ts(const int64_t last_generate_mask_set_ts)
  {
    last_generate_mask_set_ts_ = last_generate_mask_set_ts;
  }
  bool is_mask_set_ready() const
  {
    return is_mask_set_ready_;
  }
  void set_mask_set_ready(const bool is_ready)
  {
    is_mask_set_ready_ = is_ready;
  }
  int64_t get_used_time() const
  {
    return ObTimeUtility::current_time() - create_ts_;
  }
  TO_STRING_KV(K_(trans_id), K_(partition), K_(log_id), K_(task_type), K_(log_type), K_(timestamp),
      K_(last_generate_mask_set_ts), K_(is_mask_set_ready));

public:
  ObTransID trans_id_;
  common::ObPartitionKey partition_;
  uint64_t log_id_;
  int64_t log_type_;
  int64_t timestamp_;
  int64_t create_ts_;
  // Record the time of the last generation of the synchronized mask set
  int64_t last_generate_mask_set_ts_;
  // Record whether the mask_set is successfully generated
  bool is_mask_set_ready_;
};

template <typename T>
inline bool atomic_update(T* v, const T x)
{
  bool bool_ret = false;
  int64_t ov = ATOMIC_LOAD(v);
  while (ov < x) {
    if (ATOMIC_BCAS(v, ov, x)) {
      bool_ret = true;
      break;
    } else {
      ov = ATOMIC_LOAD(v);
    }
  }
  return bool_ret;
}

}  // namespace transaction
}  // namespace oceanbase

#endif
