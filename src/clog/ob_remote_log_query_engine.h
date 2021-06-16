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

#ifndef OCEANBASE_CLOG_OB_REMOTE_LOG_QUERY_ENGINE_H_
#define OCEANBASE_CLOG_OB_REMOTE_LOG_QUERY_ENGINE_H_

#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/net/ob_addr.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace clog {
class ObILogEngine;
class ObIRemoteLogQueryEngine {
public:
  ObIRemoteLogQueryEngine()
  {}
  virtual ~ObIRemoteLogQueryEngine()
  {}

public:
  virtual int get_log(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const common::ObMemberList& curr_member_list, transaction::ObTransID& trans_id, int64_t& submit_timestamp) = 0;
};

class ObPartitionLogInfo {
public:
  ObPartitionLogInfo(const common::ObPartitionKey& partition_key, const uint64_t log_id)
      : partition_key_(partition_key), log_id_(log_id)
  {}
  ObPartitionLogInfo() : partition_key_(), log_id_(common::OB_INVALID_ID)
  {}
  ~ObPartitionLogInfo()
  {}

public:
  uint64_t hash() const;
  bool operator==(const ObPartitionLogInfo& other) const;
  void set(const common::ObPartitionKey& partition_key, const uint64_t log_id)
  {
    partition_key_ = partition_key;
    log_id_ = log_id;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  bool is_valid() const
  {
    return partition_key_.is_valid() && is_valid_log_id(log_id_);
  }
  TO_STRING_KV(K(partition_key_), K(log_id_));

private:
  common::ObPartitionKey partition_key_;
  uint64_t log_id_;
};

class ObRemoteLogQETask {
public:
  ObRemoteLogQETask() : partition_key_(), log_id_(common::OB_INVALID_ID), curr_member_list_()
  {}
  ~ObRemoteLogQETask()
  {}

public:
  void set(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const common::ObMemberList& curr_member_list)
  {
    int ret = common::OB_SUCCESS;
    partition_key_ = partition_key;
    log_id_ = log_id;
    if (OB_FAIL(curr_member_list_.deep_copy(curr_member_list))) {
      CLOG_LOG(ERROR, "curr_member_list_ deep_copy failed", K(ret));
    }
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  const common::ObMemberList& get_member_list() const
  {
    return curr_member_list_;
  }
  TO_STRING_KV(K(partition_key_), K(log_id_), K(curr_member_list_));

private:
  common::ObPartitionKey partition_key_;
  uint64_t log_id_;
  common::ObMemberList curr_member_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogQETask);
};

class ObTransIDInfo {
public:
  ObTransIDInfo(const transaction::ObTransID& trans_id, const int64_t submit_timestamp, const int64_t ts)
      : trans_id_(trans_id), submit_timestamp_(submit_timestamp), ts_(ts)
  {}
  ObTransIDInfo() : trans_id_(), submit_timestamp_(common::OB_INVALID_TIMESTAMP), ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObTransIDInfo()
  {}

public:
  const transaction::ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  int64_t get_submit_timestamp() const
  {
    return submit_timestamp_;
  }
  uint64_t get_ts() const
  {
    return ts_;
  }
  TO_STRING_KV(K(trans_id_), K(submit_timestamp_), K(ts_));

private:
  transaction::ObTransID trans_id_;
  int64_t submit_timestamp_;
  // The time inserted into hash map, used to cache replacement
  int64_t ts_;
};

class ObRemoteLogQueryEngine : public ObIRemoteLogQueryEngine, public lib::TGTaskHandler {
public:
  const static int64_t REMOTE_LOG_QUERY_ENGINE_THREAD_NUM = 2;
  const static int64_t MINI_MODE_REMOTE_LOG_QUERY_ENGINE_THREAD_NUM = 1;
  const static int64_t REMOTE_LOG_QUERY_ENGINE_TASK_NUM = 1000;

public:
  ObRemoteLogQueryEngine();
  virtual ~ObRemoteLogQueryEngine();

public:
  int init(common::ObMySQLProxy* sql_proxy, ObILogEngine* log_engine, const common::ObAddr& self);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  virtual int get_log(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const common::ObMemberList& curr_member_list, transaction::ObTransID& trans_id, int64_t& submit_timestamp);
  virtual void handle(void* task);
  int run_clear_cache_task();

private:
  int get_addr_array_(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, common::ObAddrArray& addr_array);
  int execute_query_(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const common::ObAddrArray& addr_array, bool& succeed);
  static int transfer_addr_array_(const common::ObMemberList& member_list, common::ObAddrArray& addr_array);
  const static int64_t CLEAR_CACHE_INTERVAL = 300 * 1000 * 1000L;  // 5 minutes
  class ClearCacheTask : public common::ObTimerTask {
  public:
    ClearCacheTask();
    virtual ~ClearCacheTask()
    {}

  public:
    int init(ObRemoteLogQueryEngine* host);
    virtual void runTimerTask();
    void destroy();

  private:
    bool is_inited_;
    ObRemoteLogQueryEngine* host_;
  };
  class RemoveIfFunctor;

private:
  bool is_inited_;
  bool is_running_;
  // Query __all_clog_hisotry_info
  common::ObMySQLProxy* sql_proxy_;
  // Provide reading remote log
  ObILogEngine* log_engine_;
  common::ObLinearHashMap<ObPartitionLogInfo, ObTransIDInfo> cache_;
  // This thread used to clean cache
  ClearCacheTask clear_cache_task_;
  common::ObAddr self_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogQueryEngine);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_REMOTE_LOG_QUERY_ENGINE_H_
