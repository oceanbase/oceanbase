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

#ifndef OCEANBASE_CLOG_OB_EXTERNAL_LEADER_HEARTBEAT_HANDLER_
#define OCEANBASE_CLOG_OB_EXTERNAL_LEADER_HEARTBEAT_HANDLER_

#include "ob_log_external_rpc.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObIPartitionLogService;
class ObILogEngine;
}  // namespace clog
namespace logservice {

/*
 * LeaderHeartbeat
 * case request from leader,   return <OB_SUCCESS,    next_served_log_id/ts>
 * case request from follower, return <OB_NOT_MASTER, next_served_log_id/ts>
 *
 * note: leader will process dounle check
 */

class ObExtLeaderHeartbeatHandler {
public:
  ObExtLeaderHeartbeatHandler()
  {
    partition_service_ = NULL;
  }
  ~ObExtLeaderHeartbeatHandler()
  {
    destroy();
  }
  int init(storage::ObPartitionService* partition_service);
  void destroy()
  {
    partition_service_ = NULL;
  }
  int leader_heartbeat(const obrpc::ObLogLeaderHeartbeatReq& req_msg, obrpc::ObLogLeaderHeartbeatResp& resp_msg);

private:
  inline int get_leader_info(const common::ObPartitionKey& pkey, clog::ObIPartitionLogService* pls,
      common::ObRole& role, int64_t& leader_epoch);
  inline int get_last_slide(const common::ObPartitionKey& pkey, clog::ObIPartitionLogService* pls,
      uint64_t& last_slide_log_id, int64_t& last_slide_ts);
  int get_heartbeat_on_leader(const common::ObPartitionKey& pkey, clog::ObIPartitionLogService* pls,
      const int64_t leader_epoch, uint64_t& next_served_log_id, int64_t& next_served_ts);
  int get_heartbeat_on_follower(const common::ObPartitionKey& pkey, clog::ObIPartitionLogService* pls,
      uint64_t& next_served_log_id, int64_t& next_served_ts);
  int get_heartbeat_on_partition(
      const common::ObPartitionKey& pkey, uint64_t& next_served_log_id, int64_t& next_served_ts);
  int do_req_heartbeat_info(const obrpc::ObLogLeaderHeartbeatReq& req_msg, obrpc::ObLogLeaderHeartbeatResp& resp_msg);

private:
  storage::ObPartitionService* partition_service_;
};

}  // namespace logservice
}  // namespace oceanbase

#endif
