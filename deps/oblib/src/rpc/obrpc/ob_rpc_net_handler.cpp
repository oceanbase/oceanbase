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

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_rpc_net_handler.h"

#include <byteswap.h>
#include "io/easy_io.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "lib/utility/serialization.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_virtual_rpc_protocol_processor.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "common/ob_clock_generator.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::rpc::frame;

namespace oceanbase
{
namespace obrpc
{

int64_t ObRpcNetHandler::CLUSTER_ID = common::INVALID_CLUSTER_ID;
uint64_t ObRpcNetHandler::CLUSTER_NAME_HASH = 0;

// Function return value definition:
//     0-non-keepalive packets;
//     1-keepalive message;
//   <0-There is an error.
// 
// The result is only meaningful for keepalive packets and is defined as follows:
//     1-Receive complete and correct keepalive message;
//     0-The keepalive message received is incomplete;
//   <0-The format of the received keepalive message is illegal.
//
int ObRpcNetHandler::try_decode_keepalive(easy_message_t *ms, int &result)
{
  int ret = 0; // means it is a normal message.
  int32_t recv_len = 0;
  char *in_data = NULL;

  if (OB_ISNULL(ms)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ms));
  } else if (OB_ISNULL(ms->input) || OB_ISNULL(ms->pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(ms->input), KP(ms->pool), K(ret));
  } else if (OB_ISNULL(in_data = ms->input->pos) || OB_ISNULL(ms->input->last)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(in_data), KP(ms->input->last), K(ret));
  } else if ((recv_len = static_cast<uint32_t>(ms->input->last - ms->input->pos)) < 1) {
    // data is not enough
    ret = 0;
  } else {
    int64_t pos = 0;
    char *net_header = NULL, *keepalive_payload = NULL;
    int8_t magic = 0, magic_ver = 0;
    uint8_t mflag[4];
    uint32_t dlen = 0;
    uint32_t chid = 0;
    uint8_t type = 0;
    uint8_t flag = 0;
    uint8_t is_bg_flow = 0;
    uint32_t full_demanded_len = 0;
    
    if (OB_FAIL(serialization::decode_i8(ms->input->pos, recv_len, pos, &magic))) {
      LOG_ERROR("failed to decode magic_num", K(ret));
    } else {
      if ((magic != (int8_t)0xff) && (magic & MAGIC_KEEPALIVE_INDICATOR_MASK)) {
        // Received a keepalive message.
        ret = 1;
        result = 1;
        if (recv_len  < common::OB_NET_HEADER_LENGTH) {
          //data is not enough
          result = 0;
        } else {
          net_header = ms->input->pos;
          mflag[0] = static_cast<uint8_t> (net_header[0]);
          mflag[1] = static_cast<uint8_t> (net_header[1]);
          mflag[2] = static_cast<uint8_t> (net_header[2]);
          mflag[3] = static_cast<uint8_t> (net_header[3]);
          dlen = bswap_32(*((uint32_t *)(net_header + 4)));
          chid = bswap_32(*((uint32_t *)(net_header + 8)));

          full_demanded_len = common::OB_NET_HEADER_LENGTH + dlen;
          if (dlen > get_max_rpc_packet_size()) {
            result = OB_RPC_PACKET_TOO_LONG;
            LOG_WARN("obrpc packet payload exceeds its limit", K(magic), K(result), K(dlen), "limit", get_max_rpc_packet_size());
          } else if (recv_len < full_demanded_len) {
            //data is not enough
            result = 0;
          } else {
            magic_ver = (magic & MAGIC_VERSION_MASK) >> MAGIC_VERSION_OFF;
            ms->c->peer_magic_ver = magic_ver;

            if (magic_ver >= ms->c->local_magic_ver) {
              ms->c->magic_ver = ms->c->local_magic_ver;
            }

            ms->input->pos += full_demanded_len;
            if (dlen > 0) {
              keepalive_payload = net_header + common::OB_NET_HEADER_LENGTH;
              type = static_cast<uint8_t> (keepalive_payload[0]);
              flag = static_cast<uint8_t> (keepalive_payload[1]);
              is_bg_flow = static_cast<uint8_t> (keepalive_payload[2]);

              if ((EASY_TYPE_SERVER == ms->c->type) && (KEEPALIVE_DATA_TYPE_RL == type)) {
                if (flag & KEEPALIVE_DATA_FLAG_EN_RL_MASK) {
                  if (!ms->c->ratelimit_enabled) {
                    ms->c->ratelimit_enabled = 1;
                    LOG_INFO("Ratelimit enabled on server side, ", KP(ms->c));
                  }
                }
              }
            }
          }
        }
      } else {
        // not a keepalive message.
      }
    }
  }

  return ret;
}

void *ObRpcNetHandler::decode(easy_message_t *ms)
{
  ObRpcPacket *pkt = NULL;
  easy_connection_t *easy_conn = NULL;
  bool is_current_normal_mode = true;
  const int64_t start_ts = common::ObClockGenerator::getClock();
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ms)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid easy_message_t is NULL", K(ret));
  } else if (OB_ISNULL(ms->input) || OB_ISNULL(easy_conn = ms->c)) {
    ms->status = EASY_ERROR;
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid input or connection", KP(ms->input), KP(easy_conn), K(ret));
  } else if (NULL == easy_conn->user_data) {
    is_current_normal_mode = true;
  } else {
    ObRpcCompressCtxSet *ctx_set = static_cast<ObRpcCompressCtxSet *>(easy_conn->user_data);
    is_current_normal_mode = ctx_set->decompress_ctx_.is_normal_mode();
  }

  if (OB_SUCC(ret)) {
    int result;
    ObVirtualRpcProtocolProcessor *processor = NULL;
    ret = try_decode_keepalive(ms, result);
    if (ret < 0) {
      ms->status = EASY_ERROR;
      pkt = NULL;
      LOG_ERROR("failed to decode keepalive", K(easy_conn), KP(ms), K(ret));
    } else if (ret > 0) {
      // Keepalive message.
      if (result < 0) {
        ms->status = EASY_ERROR;
        pkt = NULL;
        LOG_ERROR("received wrong keepalive message", K(easy_conn), KP(ms), K(result));
      } else if (result > 0) {
        ms->status = EASY_MESG_SKIP;
        pkt = NULL;
      } else {
        //receive data is not enough
      }
    } else {
      // Not keepalive message.
      if (is_current_normal_mode) {
        processor = &rpc_processor_;
      } else {
        processor = &rpc_compress_processor_;
      }
      if (OB_FAIL(processor->decode(ms, pkt))) {
        ms->status = EASY_ERROR;
        pkt = NULL;
        LOG_ERROR("failed to decode", K(easy_conn), KP(ms), K(is_current_normal_mode), K(ret));
      } else {
        if (NULL != pkt) {
          const int64_t receive_ts = common::ObClockGenerator::getClock();
          const int64_t fly_ts = receive_ts - pkt->get_timestamp();
          if (!pkt->is_resp() && fly_ts > common::OB_MAX_PACKET_FLY_TS && TC_REACH_TIME_INTERVAL(100 * 1000)) {
            LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "packet fly cost too much time", "pcode", pkt->get_pcode(),
                    "fly_ts", fly_ts, "send_timestamp", pkt->get_timestamp(), "connection", easy_connection_str(ms->c));
          }
          pkt->set_receive_ts(receive_ts);
          if (receive_ts - start_ts > common::OB_MAX_PACKET_DECODE_TS && TC_REACH_TIME_INTERVAL(100 * 1000)) {
            LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "packet decode cost too much time", "pcode", pkt->get_pcode(), "connection", easy_connection_str(ms->c));
          }
        } else {
          //receive data is not enough
        }
      }
    }
  }
  return pkt;
}

int ObRpcNetHandler::encode(easy_request_t *req, void *packet)
{
  int eret = EASY_OK;
  easy_connection_t *easy_conn = NULL;
  bool is_current_normal_mode = true;

  if (OB_ISNULL(req)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "req should not be NULL", K(eret));
  } else if (OB_ISNULL(packet)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "packet should not be NULL", K(eret));
  } else if (OB_ISNULL(req->ms) || OB_ISNULL(easy_conn = req->ms->c)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "Easy message session(ms) or ms pool is NULL", K(eret), "ms", "req->ms");
  } else if (NULL == easy_conn->user_data) {
    is_current_normal_mode = true;
  } else {
    ObRpcCompressCtxSet *ctx_set = static_cast<ObRpcCompressCtxSet *>(easy_conn->user_data);
    is_current_normal_mode = ctx_set->compress_ctx_.is_normal_mode();
  }

  if (EASY_OK == eret) {
    ObRpcPacket *pkt = static_cast<ObRpcPacket*>(packet);
    pkt->set_packet_id(req->packet_id);
    if (common::OB_INVALID_CLUSTER_ID == pkt->get_dst_cluster_id()) {
      pkt->set_dst_cluster_id(CLUSTER_ID);
      pkt->set_cluster_name_hash(ObRpcPacket::INVALID_CLUSTER_NAME_HASH);
    } else if (common::OB_INVALID_CLUSTER_ID != ObRpcNetHandler::CLUSTER_ID
               && common::OB_INVALID_CLUSTER_ID != pkt->get_dst_cluster_id()
               && ObRpcNetHandler::CLUSTER_ID == pkt->get_dst_cluster_id()) {
      pkt->set_cluster_name_hash(ObRpcPacket::INVALID_CLUSTER_NAME_HASH);
    } else {
      pkt->set_cluster_name_hash(ObRpcPacket::get_self_cluster_name_hash());
    }
    req->pcode = pkt->get_pcode();
    const uint64_t *trace_id = pkt->get_trace_id();
    if (trace_id == NULL) {
      LOG_WARN_RET(common::OB_ERR_UNEXPECTED, "trace_id should not be NULL");
    } else {
      req->trace_id[0] = trace_id[0];
      req->trace_id[1] = trace_id[1];
      req->trace_id[2] = trace_id[2];
      req->trace_id[3] = trace_id[3];
    }

    const int64_t receive_ts = common::ObClockGenerator::getClock();
    const int64_t wait_ts = receive_ts - pkt->get_timestamp();

    if (!pkt->is_resp() && wait_ts > common::OB_MAX_PACKET_FLY_TS && TC_REACH_TIME_INTERVAL(100 * 1000)) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "packet wait too much time before encode", "pcode", pkt->get_pcode(),
               "wait_ts", wait_ts, "send_timestamp", pkt->get_timestamp());
    }

    ObVirtualRpcProtocolProcessor *processor = NULL;
    if (is_current_normal_mode) {
      processor = &rpc_processor_;
    } else {
      processor = &rpc_compress_processor_;
    }

    int ret = OB_SUCCESS;
    if (OB_FAIL(processor->encode(req, pkt))) {
      eret = EASY_ERROR;
      LOG_ERROR("faile to encode with processor", K(easy_conn), K(*pkt), K(ret), K(eret));
    }
  }
  return eret;
}

uint64_t ObRpcNetHandler::get_packet_id(easy_connection_t *c, void *packet)
{
  UNUSED(c);
  uint64_t pkt_id = common::OB_INVALID_ID;
  if (OB_ISNULL(packet)) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "packet should not be NULL");
  } else {
    pkt_id = static_cast<ObRpcPacket *>(packet)->get_chid();
    pkt_id = (pkt_id<<16) | (c->fd & 0xffff);
  }
  return pkt_id;
}

int ObRpcNetHandler::on_connect(easy_connection_t *c)
{
  int eret = EASY_OK;
  if (OB_ISNULL(c)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(eret), K(c));
  } else {
    LOG_INFO("rpc connection accept", "dest", easy_connection_str(c));
  }
  return eret;
}

int ObRpcNetHandler::on_disconnect(easy_connection_t *c)
{
  int eret = EASY_OK;
  if (OB_ISNULL(c)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(eret), K(c));
  } else {
    if (NULL != c->user_data) {
      //free ctx memory of compress ctx when necessary
      ObRpcCompressCtxSet *ctx_set = static_cast<ObRpcCompressCtxSet *>(c->user_data);
      ctx_set->free_ctx_memory();
    }

    LOG_INFO("connection disconnect", KCSTRING(easy_connection_str(c)));
    if (c->reconn_fail > 10) {
      c->auto_reconn = 0;
    }
  }
  return eret;
}

void ObRpcNetHandler::set_trace_info(easy_request_t *req, void *packet)
{
  if (OB_ISNULL(req) || OB_ISNULL(packet)) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "packet should not be NULL");
  } else {
    ObRpcPacket *pkt = static_cast<ObRpcPacket *>(packet);
    req->request_arrival_time = (pkt->get_request_arrival_time() ? pkt->get_request_arrival_time() : common::ObTimeUtility::current_time());
    req->arrival_push_diff = pkt->get_arrival_push_diff();
    req->push_pop_diff = pkt->get_push_pop_diff();
    req->pop_process_start_diff = pkt->get_pop_process_start_diff();
    req->process_start_end_diff =  pkt->get_process_start_end_diff();
    req->process_end_response_diff = pkt->get_process_end_response_diff();
    req->tenant_id = pkt->get_tenant_id();
    req->packet_id = pkt->get_packet_id();
    req->pcode = pkt->get_pcode();
    req->session_id = pkt->get_session_id();
    const uint64_t *trace_id = pkt->get_trace_id();
    if (trace_id == NULL) {
      LOG_WARN_RET(common::OB_ERR_UNEXPECTED, "trace_id should not be NULL");
    } else {
      req->trace_id[0] = trace_id[0];
      req->trace_id[1] = trace_id[1];
      req->trace_id[2] = trace_id[2];
      req->trace_id[3] = trace_id[3];
    }
  }
}

char *ObRpcNetHandler::easy_alloc(easy_pool_t *pool, int64_t size) const
{
  char *buf = NULL;
  if (size > UINT32_MAX || size < 0) {
    LOG_WARN_RET(common::OB_ALLOCATE_MEMORY_FAILED, "easy alloc fail", K(size));
  } else if (OB_ISNULL(pool)) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "Easy pool is NULL");
  } else {
    buf = static_cast<char*>(easy_pool_alloc(pool, static_cast<uint32_t>(size)));
  }
  return buf;
}

int ObRpcNetHandler::on_idle(easy_connection_t *c)
{
  int eret = EASY_OK;
  if (c != nullptr)  {
    ev_tstamp now = ev_time();
    if (now - c->last_time > 600) {
      return EASY_ERROR;
    }
  }
  return eret;
}

} // end of namespace obrpc
} // end of namespace oceanbase
