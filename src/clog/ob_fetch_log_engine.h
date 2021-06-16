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

#ifndef OCEANBASE_CLOG_OB_FETCH_LOG_ENGINE_
#define OCEANBASE_CLOG_OB_FETCH_LOG_ENGINE_

#include <stdint.h>
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObFetchLogTask {
public:
  ObFetchLogTask() : is_inited_(false)
  {
    reset();
  }
  ~ObFetchLogTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }

public:
  int init(const common::ObPartitionKey& partition_key, const common::ObAddr& server, const int64_t cluster_id,
      const uint64_t start_log_id, const uint64_t end_log_id, const ObFetchLogType fetch_type,
      const common::ObProposalID& proposal_id, const int64_t network_limit);
  int64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  int64_t get_timestamp() const
  {
    return timestamp_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  const common::ObAddr& get_server() const
  {
    return server_;
  }
  uint64_t get_start_log_id() const
  {
    return start_log_id_;
  }
  uint64_t get_end_log_id() const
  {
    return end_log_id_;
  }
  ObFetchLogType get_fetch_log_type() const
  {
    return fetch_type_;
  }
  const common::ObProposalID& get_proposal_id() const
  {
    return proposal_id_;
  }
  int64_t get_network_limit() const
  {
    return network_limit_;
  }

  TO_STRING_KV(K_(timestamp), K_(partition_key), K_(server), K_(cluster_id), K_(start_log_id), K_(end_log_id),
      K_(fetch_type), K_(proposal_id), K_(network_limit));

private:
  DISALLOW_COPY_AND_ASSIGN(ObFetchLogTask);

private:
  bool is_inited_;
  int64_t timestamp_;
  common::ObPartitionKey partition_key_;
  common::ObAddr server_;
  int64_t cluster_id_;
  uint64_t start_log_id_;
  uint64_t end_log_id_;
  ObFetchLogType fetch_type_;
  common::ObProposalID proposal_id_;
  int64_t network_limit_;
};

class ObIFetchLogEngine {
public:
  ObIFetchLogEngine()
  {}
  virtual ~ObIFetchLogEngine()
  {}

public:
  virtual int submit_fetch_log_task(ObFetchLogTask* fetch_log_task) = 0;
};

class ObFetchLogEngine : public ObIFetchLogEngine, public lib::TGTaskHandler {
public:
  ObFetchLogEngine() : is_inited_(false), partition_service_(NULL)
  {}
  virtual ~ObFetchLogEngine()
  {}

public:
  int init(storage::ObPartitionService* partition_service);
  int destroy();
  virtual int submit_fetch_log_task(ObFetchLogTask* fetch_log_task);

public:
  virtual void handle(void* task);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFetchLogEngine);

private:
  bool is_inited_;
  bool is_task_queue_timeout_(ObFetchLogTask* fetch_log_task) const;
  storage::ObPartitionService* partition_service_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_FETCH_LOG_ENGINE_
