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

#ifndef OCEANBASE_CLOG_OB_EXTERNAL_HEARTBEAT_HANDLER_
#define OCEANBASE_CLOG_OB_EXTERNAL_HEARTBEAT_HANDLER_

#include "lib/hash/ob_linear_hash_map.h"
#include "ob_log_external_rpc.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObILogEngine;
}
namespace logservice {
class ObExtHeartbeatHandler {
public:
  ObExtHeartbeatHandler() : partition_service_(NULL), log_engine_(NULL)
  {}
  ~ObExtHeartbeatHandler()
  {
    destroy();
  }
  int init(storage::ObPartitionService* partition_service, clog::ObILogEngine* log_engine);
  void destroy();
  int req_heartbeat_info(
      const obrpc::ObLogReqHeartbeatInfoRequest& req_msg, obrpc::ObLogReqHeartbeatInfoResponse& response);

private:
  uint64_t get_last_slide_log_id(const common::ObPartitionKey& pkey);
  int get_leader_info(const common::ObPartitionKey& pkey, common::ObRole& role, int64_t& leader_epoch);
  int get_predict_timestamp(const common::ObPartitionKey& pkey, const uint64_t last_log_id, int64_t& predict_ts);
  int get_predict_heartbeat(const common::ObPartitionKey& pkey, const uint64_t last_log_id, int64_t& res_ts);
  int get_heartbeat_on_partition(const common::ObPartitionKey& pkey, const uint64_t last_log_id, int64_t& res_ts);
  int do_req_heartbeat_info(
      const obrpc::ObLogReqHeartbeatInfoRequest& req_msg, obrpc::ObLogReqHeartbeatInfoResponse& response);

private:
  storage::ObPartitionService* partition_service_;
  clog::ObILogEngine* log_engine_;
};

}  // namespace logservice
}  // namespace oceanbase

#endif
