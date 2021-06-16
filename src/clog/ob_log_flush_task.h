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

#ifndef OCEANBASE_CLOG_OB_LOG_FLUSH_TASK_
#define OCEANBASE_CLOG_OB_LOG_FLUSH_TASK_

#include "lib/objectpool/ob_resource_pool.h"
#include "ob_i_disk_log_buffer.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObILogEngine;
class ObILogFlushCb {
public:
  ObILogFlushCb(){};
  virtual ~ObILogFlushCb()
  {}
  virtual int flush_cb(const ObLogFlushCbArg& arg) = 0;
};

class ObLogFlushTask : public ObDiskBufferTask {
public:
  ObLogFlushTask();
  virtual ~ObLogFlushTask();

public:
  int init(const ObLogType log_type, const uint64_t log_id, const common::ObProposalID proposal_id,
      const common::ObPartitionKey& partition_key, storage::ObPartitionService* partition_service,
      const common::ObAddr& leader, const int64_t cluster_id_, ObILogEngine* log_engine, const int64_t submit_timestamp,
      const int64_t pls_epoch);
  void reset();
  virtual int st_after_consume(const int handle_err);
  virtual bool is_aggre_task() const
  {
    return OB_LOG_AGGRE == log_type_;
  }
  virtual int after_consume(const int handle_err, const void* arg, const int64_t before_push_cb_ts);
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  TO_STRING_KV(N_LOG_TYPE, log_type_, N_LOG_ID, log_id_, "submit_timestamp", submit_timestamp_, N_PARTITION_KEY,
      partition_key_, "leader", leader_, "cluster_id", cluster_id_)
private:
  ObLogType log_type_;
  uint64_t log_id_;
  common::ObPartitionKey partition_key_;
  storage::ObPartitionService* partition_service_;
  common::ObAddr leader_;
  int64_t cluster_id_;
  ObILogEngine* log_engine_;
  int64_t submit_timestamp_;
  // Keep flush_cb only be called in same partition_log_service
  int64_t pls_epoch_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFlushTask);
};
}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_FLUSH_TASK_
