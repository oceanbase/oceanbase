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

#include "ob_log_executor.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "storage/ob_i_partition_storage.h"
#include "ob_i_log_engine.h"
#include "ob_log_req.h"
#include "ob_batch_packet.h"
#include "ob_log_reader_interface.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace storage;

namespace clog {
int ObLogExecutor::init(ObIPartitionLogPacketHandler* pkt_handler, ObILogEngine* log_engine)
{
  int ret = OB_SUCCESS;
  if (is_inited_ || NULL != pkt_handler_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "log executor init error", K(ret));
  } else if (NULL == pkt_handler || NULL == log_engine) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "log executor init error", K(ret), K(pkt_handler), K(log_engine));
  } else {
    pkt_handler_ = pkt_handler;
    log_engine_ = log_engine;
    is_inited_ = true;
  }
  return ret;
}

int ObLogExecutor::handle_clog_req(
    common::ObAddr& sender, const int64_t src_cluster_id, int type, ObPartitionKey& pkey, const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  int handle_ret = OB_SUCCESS;
  ObLogReqContext ctx;
  ctx.type_ = (ObLogReqType)type;
  ctx.server_ = sender;
  ctx.pkey_ = pkey;
  ctx.req_id_ = 0;
  ctx.data_ = data;
  ctx.len_ = len;
  ctx.cluster_id_ = src_cluster_id;
  EVENT_INC(CLOG_RPC_COUNT);
  const int64_t start_handle_request = ObTimeUtility::current_time();
  if (OB_SUCCESS != (handle_ret = pkt_handler_->handle_request(ctx))) {
    CLOG_LOG(DEBUG, "handle request failed", K(handle_ret), K(ctx));
    if (OB_ENTRY_NOT_EXIST == handle_ret && NULL != log_engine_ &&
        (OB_LEADER_SW_INFO_RESP_V2 == type || OB_LEADER_SW_INFO_RESP == type || OB_KEEPALIVE_MSG == type ||
            OB_FETCH_LOG_V2 == type)) {
      // partition is already not exist, reject sender explicitly
      ObReplicaMsgType msg_type = OB_REPLICA_MSG_TYPE_NOT_EXIST;
      (void)log_engine_->reject_server(sender, src_cluster_id, pkey, msg_type);
    }
  } else {
    CLOG_LOG(DEBUG, "handle_request succ", K(sender), K(ctx), K(handle_ret));
    EVENT_ADD(CLOG_RPC_REQUEST_HANDLE_TIME, common::ObTimeUtility::current_time() - start_handle_request);
    EVENT_INC(CLOG_RPC_REQUEST_COUNT);
  }
  return ret;
}

int ObLogExecutor::handle_packet(int pcode, const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  int32_t count = 0;
  if (!is_inited_ || NULL == pkt_handler_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "pkt_handler not init", K(ret));
  } else if (NULL == data || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(data), K(len));
  } else {
    int64_t pos = 0;
    ObAddr server;
    int64_t send_cs = 0;
    int64_t timestamp = 0;
    int64_t head_size = ObBatchPacketCodec::get_batch_header_size();
    int64_t cal_cs = ob_crc64(data + head_size, len - head_size);
    if (OB_FAIL(ObBatchPacketCodec::decode_batch_header(data, len, pos, &timestamp, server, &count, &send_cs))) {
      CLOG_LOG(WARN, "decode batch packet failed", K(ret));
    } else if (send_cs != cal_cs) {
      ret = OB_CHECKSUM_ERROR;
      CLOG_LOG(ERROR,
          "clog rqst checksum error",
          K(ret),
          K(timestamp),
          K(send_cs),
          K(cal_cs),
          K(server),
          K(len),
          K(count),
          KPHEX(data, static_cast<int32_t>(len)));
    } else {
      CLOG_LOG(DEBUG, "-->CLOG_RPC.handle_packet", K(pcode), K(server), K(timestamp), K(len), K(count), K(send_cs));
      const int64_t cost_time = ObTimeUtility::current_time() - timestamp;
      EVENT_ADD(CLOG_RPC_DELAY_TIME, cost_time);
      EVENT_INC(CLOG_RPC_COUNT);
      for (int32_t i = 0; OB_SUCC(ret) && i < count; i++) {
        int64_t start_handle_request = common::ObTimeUtility::current_time();
        if (OB_FAIL(handle_request(server, data, len, pos))) {
          CLOG_LOG(WARN, "handle_request fail", K(ret), K(server), KP(data), K(len), K(pos));
        } else {
          EVENT_ADD(CLOG_RPC_REQUEST_HANDLE_TIME, common::ObTimeUtility::current_time() - start_handle_request);
          EVENT_INC(CLOG_RPC_REQUEST_COUNT);
        }
      }
    }
  }
  CLOG_LOG(DEBUG, "<--CLOG_RPC.handle_packet", K(ret));
  return ret;
}

int ObLogExecutor::handle_request(const ObAddr& send_server, const char* buf, const int64_t len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int handle_ret = OB_SUCCESS;
  ObPartitionKey partition_key;
  int type = 0;
  int32_t size = 0;
  int64_t req_id = 0;
  const char* content = 0;
  if (!is_inited_ || NULL == pkt_handler_) {
    ret = OB_NOT_INIT;
  } else if (!send_server.is_valid() || NULL == buf || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(send_server), KP(buf), K(pos));
  } else if (OB_FAIL(ObBatchPacketCodec::decode_packet(buf, len, pos, partition_key, type, req_id, size, content))) {
    CLOG_LOG(ERROR, "decode_packet failed", K(ret));
  } else {
    ObLogReqContext ctx;
    ctx.type_ = (ObLogReqType)type;
    ctx.server_ = send_server;
    ctx.pkey_ = partition_key;
    ctx.req_id_ = req_id;
    ctx.data_ = content;
    ctx.len_ = size;
    if (OB_SUCCESS != (handle_ret = pkt_handler_->handle_request(ctx))) {
      CLOG_LOG(DEBUG, "handle request failed", K(handle_ret), K(ctx));
    } else {
      CLOG_LOG(DEBUG, "handle_request succ", K(send_server), K(ctx), K(handle_ret));
    }
  }
  return ret;
}
};  // end namespace clog
};  // end namespace oceanbase
