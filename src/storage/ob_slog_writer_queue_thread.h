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

#ifndef OCEANBASE_STORAGE_SLOG_WRITER_QUEUE_THREAD_
#define OCEANBASE_STORAGE_SLOG_WRITER_QUEUE_THREAD_

#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/thread/thread_mgr_interface.h"
#include "share/ob_proposal_id.h"
#include "clog/ob_log_define.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;

class ObMsInfoTask {
public:
  ObMsInfoTask()
      : pkey_(),
        server_(),
        cluster_id_(common::OB_INVALID_CLUSTER_ID),
        log_type_(clog::OB_LOG_UNKNOWN),
        ms_log_id_(common::OB_INVALID_ID),
        mc_timestamp_(common::OB_INVALID_TIMESTAMP),
        replica_num_(0),
        prev_member_list_(),
        curr_member_list_(),
        ms_proposal_id_()
  {}
  ObMsInfoTask(const common::ObPartitionKey& pkey, const common::ObAddr& server, const int64_t cluster_id,
      const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObProposalID& ms_proposal_id)
      : pkey_(pkey),
        server_(server),
        cluster_id_(cluster_id),
        log_type_(log_type),
        ms_log_id_(ms_log_id),
        mc_timestamp_(mc_timestamp),
        replica_num_(replica_num),
        ms_proposal_id_(ms_proposal_id)
  {}
  ~ObMsInfoTask()
  {}
  bool is_valid() const
  {
    return pkey_.is_valid();
  }
  common::ObPartitionKey get_pkey() const
  {
    return pkey_;
  }
  common::ObAddr get_server() const
  {
    return server_;
  }
  int64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  clog::ObLogType get_log_type() const
  {
    return log_type_;
  }
  uint64_t get_ms_log_id() const
  {
    return ms_log_id_;
  }
  int64_t get_mc_timestamp() const
  {
    return mc_timestamp_;
  }
  int64_t get_replica_num() const
  {
    return replica_num_;
  }
  common::ObProposalID get_ms_proposal_id() const
  {
    return ms_proposal_id_;
  }
  common::ObMemberList get_prev_member_list() const
  {
    return prev_member_list_;
  }
  common::ObMemberList get_curr_member_list() const
  {
    return curr_member_list_;
  }
  void set_pkey(const common::ObPartitionKey& pkey);
  void set_server(const common::ObAddr& server);
  void set_cluster_id(const int64_t cluster_id);
  void set_log_type(const clog::ObLogType log_type);
  void set_ms_log_id(const uint64_t ms_log_id);
  void set_mc_timestamp(const int64_t mc_timestamp);
  void set_replica_num(const int64_t replica_num);
  void set_ms_proposal_id(const common::ObProposalID& ms_proposal_id);
  int update_prev_member_list(const common::ObMemberList& prev_member_list);
  int update_curr_member_list(const common::ObMemberList& curr_member_list);
  ObMsInfoTask& operator=(const ObMsInfoTask& rv);

  TO_STRING_KV(N_KEY, pkey_, "server", server_, "cluster_id", cluster_id_, "log_type", log_type_, "ms_log_id",
      ms_log_id_, "mc_timestamp", mc_timestamp_, "replica_num", replica_num_, "prev_member_list", prev_member_list_,
      "curr_member_list", curr_member_list_, "ms_proposal_id", ms_proposal_id_);

private:
  common::ObPartitionKey pkey_;
  common::ObAddr server_;
  int64_t cluster_id_;
  clog::ObLogType log_type_;
  uint64_t ms_log_id_;
  int64_t mc_timestamp_;
  int64_t replica_num_;
  common::ObMemberList prev_member_list_;
  common::ObMemberList curr_member_list_;
  common::ObProposalID ms_proposal_id_;
};

class ObSlogWriterQueueThread : public lib::TGTaskHandler {
public:
  static const int64_t QUEUE_THREAD_NUM = 4;
  static const int64_t MINI_MODE_QUEUE_THREAD_NUM = 2;
  ObSlogWriterQueueThread();
  virtual ~ObSlogWriterQueueThread();

public:
  virtual int init(ObPartitionService* partition_service, int tg_id);
  virtual int push(const ObMsInfoTask* task);
  virtual void handle(void* task);
  virtual void destroy();
  int get_tg_id() const
  {
    return tg_id_;
  }

private:
  int get_task(ObMsInfoTask*& task);
  void free_task(ObMsInfoTask* task);

private:
  bool inited_;
  ObPartitionService* partition_service_;
  common::ObFixedQueue<ObMsInfoTask> free_queue_;
  ObMsInfoTask* tasks_;
  int tg_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSlogWriterQueueThread);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_SLOG_WRITER_QUEUE_THREAD_
