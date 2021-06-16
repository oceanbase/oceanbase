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

#ifndef OCEANBASE_CLOG_OB_LOG_EXECUTOR_
#define OCEANBASE_CLOG_OB_LOG_EXECUTOR_

#include "lib/hash/ob_linear_hash_map.h"
#include "ob_log_define.h"
#include "ob_i_partition_log_packet_handler.h"
#include "ob_log_req.h"
#include "ob_info_block_handler.h"

namespace oceanbase {
namespace common {
class ObPacket;
}

namespace obrpc {
class ObLogReqStartLogIdByTsRequest;
class ObLogReqStartLogIdByTsResponse;
class ObLogReqStartPosByLogIdRequest;
class ObLogReqStartPosByLogIdResponse;
class ObLogExternalFetchLogRequest;
class ObLogExternalFetchLogResponse;
class ObLogReqHeartbeatInfoRequest;
class ObLogReqHeartbeatInfoResponse;

// deprecated
class ObLogLocateStartWithTsRequest;
class ObLogLocateStartWithTsResponse;
class ObLogLocateStartWithIDRequest;
class ObLogLocateStartWithIDResponse;
class ObLogExternalFetchRequest;
class ObLogExternalFetchResult;
}  // namespace obrpc

namespace clog {
class ObILogEngine;
class ObIPartitionLogPacketHandler;
class ObIndexEntry;
class ObLogEntry;

class ObLogExecutor {
public:
  ObLogExecutor() : is_inited_(false), pkt_handler_(NULL), log_engine_(NULL)
  {}
  ~ObLogExecutor()
  {}
  int init(ObIPartitionLogPacketHandler* pkt_handler, ObILogEngine* log_engine);
  int handle_packet(int pcode, const char* data, int64_t len);
  int handle_clog_req(
      common::ObAddr& sender, const int64_t cluster_id, int type, ObPartitionKey& pkey, const char* data, int64_t len);

  // for liboblog. new interface
  int req_start_log_id_by_ts(
      const obrpc::ObLogReqStartLogIdByTsRequest& req_msg, obrpc::ObLogReqStartLogIdByTsResponse& result);
  int req_start_pos_by_log_id(
      const obrpc::ObLogReqStartPosByLogIdRequest& req_msg, obrpc::ObLogReqStartPosByLogIdResponse& result);
  int fetch_log(const obrpc::ObLogExternalFetchLogRequest& req_msg, obrpc::ObLogExternalFetchLogResponse& result);
  int req_heartbeat_info(
      const obrpc::ObLogReqHeartbeatInfoRequest& req_msg, obrpc::ObLogReqHeartbeatInfoResponse& result);

private:
  int handle_request(const common::ObAddr& server, const char* data, int64_t len, int64_t& pos);

private:
  bool is_inited_;
  ObIPartitionLogPacketHandler* pkt_handler_;
  ObILogEngine* log_engine_;

  DISALLOW_COPY_AND_ASSIGN(ObLogExecutor);
};
};      // end namespace clog
};      // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_EXECUTOR_
