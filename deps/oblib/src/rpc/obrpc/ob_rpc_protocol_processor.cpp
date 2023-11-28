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
#include "rpc/obrpc/ob_rpc_protocol_processor.h"

#include <byteswap.h>
#include "io/easy_io.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "lib/utility/serialization.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/frame/ob_req_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::rpc::frame;

namespace oceanbase
{
namespace obrpc
{
// When payload in packet isn't empty, we allocate a run of memory to
// store *two* buffer meta and a Net Header. Then assign first meta
// pointer to the header and the second to the payload. Finally we
// tell easy send these two buffer out.
//
// |      *buf1*     |      *buf2*     |  *NET Header*   |
// | pos | last | .. | pos | last | .. |                 |
//
// Otherwise, when payload in packet is empty, we create a *single*
// buffer meta and a NET Header, then fill the meta with the header
// address. That is, we only tell easy send one buffer instead of two.
//
// |      *buf1*     |  *NET Header*   |
// | pos | last | .. |                 |
//

int ObRpcProtocolProcessor::encode(easy_request_t *req, ObRpcPacket *pkt)
{
  //init  ctx, block_size
  int ret = OB_SUCCESS;
  easy_message_session_t *ms = NULL;
  easy_connection_t *easy_conn = NULL;
  //If it involves switching modes, or the compression parameters have changed, you need to send cmd packet
  common::ObTimeGuard timeguard("ObRpcProtocolProcessor::encode", common::OB_EASY_HANDLER_COST_TIME);
  if (OB_ISNULL(req) || OB_ISNULL(pkt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Easy request or pkt is NULL", KP(req), KP(pkt), K(ret));
  } else if (OB_ISNULL(ms = req->ms) || OB_ISNULL(easy_conn = ms->c)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ms pool is NULL or connection is NULL", KP(ms), KP(easy_conn), K(ret));
  } else {
    ObRpcCompressMode compress_mode = ObRpcCompressCtx::get_compress_mode_from_type(pkt->get_compressor_type());
    if (RPC_STREAM_COMPRESS_NONE == compress_mode) {
      int32_t full_size = 0;
      if (OB_FAIL(encode_raw_rpc_packet(timeguard, req, pkt, full_size))) {
        LOG_WARN("failed to encode raw_rpc_packet", "pkt", *pkt, K(ret));
      }
    } else if (RPC_STREAM_COMPRESS_NONE != compress_mode) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("stream compress mode is not supported any more.", K(compress_mode), K(ret));
    } else if (OB_FAIL(init_compress_ctx(ms->c, compress_mode, COMPRESS_BLOCK_SIZE, COMPRESS_RING_BUFFER_SIZE))) {
      LOG_ERROR("failed to init_compress_ctx", K(compress_mode), K(ret));
    } else {
      //send CmdPacketInNormalFrist Frrst
      ObCmdPacketInNormal cmd_packet(compress_mode, COMPRESS_BLOCK_SIZE, COMPRESS_RING_BUFFER_SIZE);
      char *cmd_buf = NULL;
      const int32_t cmd_packet_encode_size = static_cast<int32_t>(cmd_packet.get_encode_size());
      uint32_t alloc_size = cmd_packet_encode_size + static_cast<uint32_t>(sizeof(easy_buf_t));
      timeguard.click();
      char *buf = easy_alloc(ms->pool, alloc_size);
      timeguard.click();
      int32_t full_size = cmd_packet_encode_size;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory available", K(alloc_size), K(ret));
      } else {
        easy_buf_t *ebuf = reinterpret_cast<easy_buf_t*>(buf); // pointer to easy buffer
        cmd_buf = reinterpret_cast<char*>(ebuf + 1);        // pointer to net header
        easy_buf_set_data(ms->pool, ebuf, cmd_buf, cmd_packet_encode_size);
        easy_request_addbuf(req, ebuf);

        timeguard.click();
        if (OB_FAIL(encode_compressed_rpc_packet(timeguard, req, pkt, full_size))) {
          LOG_WARN("failed to encode compressed_rpc_packet", "pkt", *pkt, K(ret));
        } else {
          cmd_packet.full_size_ = full_size;
          int64_t pos = 0;
          timeguard.click();
          if (OB_FAIL(cmd_packet.encode(cmd_buf, cmd_packet_encode_size, pos))) {
            LOG_WARN("failed to encode raw_rpc_packet",  "pkt", *pkt, K(ret));
          } else {
            timeguard.click();
          }
        }
      }
    }
  }
  return ret;
}

int ObRpcProtocolProcessor::decode(easy_message_t *ms, ObRpcPacket *&pkt)
{
  int ret = OB_SUCCESS;
  bool is_demand_data_enough = false;
  int64_t preceding_data_len = 0;
  int64_t decode_data_len = 0;
  common::ObTimeGuard timeguard("ObRpcProtocolProcessor::decode", common::OB_EASY_HANDLER_COST_TIME);
  if (OB_FAIL(resolve_packet_type(timeguard, ms, is_demand_data_enough, preceding_data_len, decode_data_len))) {
    LOG_ERROR("failed to resolve packet type", K(ret));
  } else if (is_demand_data_enough) {
    if (preceding_data_len > 0) {
      //beginning with a cmd packet
      if (OB_FAIL(decode_compressed_packet_data(timeguard, ms, preceding_data_len, decode_data_len, pkt))) {
        LOG_ERROR("failed to decode_compressed_packet_data", K(preceding_data_len), K(decode_data_len), K(ret));
      }
    } else {
      if (OB_FAIL(decode_raw_net_rpc_packet(timeguard, ms, preceding_data_len, pkt))) {
        LOG_ERROR("failed to decode_raw_net_rpc_packet", K(preceding_data_len), K(decode_data_len), K(ret));
      }
    }
  } else {
    /*recv data is not enough*/
  }
  return ret;
}

int ObRpcProtocolProcessor::resolve_packet_type(ObTimeGuard &timeguard,
                                                easy_message_t *ms,
                                                bool &is_demand_data_enough,
                                                int64_t &preceding_data_len,
                                                int64_t &decode_data_len)
{
  int ret = OB_SUCCESS;
  uint32_t recv_len = 0;
  preceding_data_len = 0;
  is_demand_data_enough = false;
  decode_data_len = 0;
  char *in_data = NULL;
  timeguard.click();
  if (OB_ISNULL(ms)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ms));
  } else if (OB_ISNULL(ms->input) || OB_ISNULL(ms->pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(ms->input), KP(ms->pool), K(ret));
  } else if (OB_ISNULL(in_data = ms->input->pos) || OB_ISNULL(ms->input->last)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(in_data), KP(ms->input->last), K(ret));
  } else if ((recv_len = static_cast<uint32_t>(ms->input->last - ms->input->pos)) < OB_NET_HEADER_LENGTH) {
    is_demand_data_enough = false;
  } else {
    // decode net header (16 bytes)
    uint8_t mflag[4] = { static_cast<uint8_t> (in_data[0]),
      static_cast<uint8_t> (in_data[1]),
      static_cast<uint8_t> (in_data[2]),
      static_cast<uint8_t> (in_data[3]) };
    if (0 == MEMCMP(&mflag[0], ObRpcPacket::MAGIC_HEADER_FLAG, 4)) {
      timeguard.click();
      //is normal rpc packet
      uint32_t plen = bswap_32(*((uint32_t *)(in_data + 4)));
      // ignore 4 bytes for reserved

      // net header length plus packet content length
      const uint32_t full_len = OB_NET_HEADER_LENGTH + plen;

      if (plen > get_max_rpc_packet_size()) {
        ret = OB_RPC_PACKET_TOO_LONG;
        LOG_WARN("obrpc packet payload exceed its limit",
                 K(ret), K(plen), "limit", get_max_rpc_packet_size());
      } else if (recv_len < full_len) {
        if (NULL != ms->c && NULL != ms->c->sc) {
          //ssl will decrypt data into ssl buff, we should not set next_read_len here
        } else {
          ms->next_read_len = full_len - recv_len;
        }
        is_demand_data_enough = false;
      } else {
        decode_data_len = full_len;
        is_demand_data_enough = true;
        preceding_data_len = 0;
      }
    } else if (0 == MEMCMP(&mflag[0], ObRpcPacket::MAGIC_COMPRESS_HEADER_FLAG, sizeof(mflag))) {
      //magic_num  and full_size
      timeguard.click();
      if (recv_len < 4 + sizeof(int32_t)) {
        is_demand_data_enough = false;
      } else {
        uint32_t full_size = bswap_32(*((uint32_t *)(in_data + 4)));
        if (recv_len < full_size) {
          is_demand_data_enough = false;
          if (NULL != ms->c && NULL != ms->c->sc) {
            //ssl will decrypt data into ssl buff, we should not set next_read_len here
          } else {
            ms->next_read_len = full_size - recv_len;
          }
        } else {
          is_demand_data_enough = true;
        }

        timeguard.click();
        if (OB_SUCC(ret) && is_demand_data_enough) {
          ObCmdPacketInNormal cmd_packet;
          int64_t pos = 0;
          if (OB_FAIL(cmd_packet.decode(in_data, recv_len, pos))) {
            LOG_ERROR("failed to decode cmd packet", K(ret));
          } else {
            timeguard.click();
            ObRpcCompressMode new_mode = static_cast<ObRpcCompressMode>(cmd_packet.compress_mode_);
            if (RPC_STREAM_COMPRESS_NONE != new_mode) {
              int16_t block_size = cmd_packet.block_size_;
              int32_t ring_buffer_size = cmd_packet.ring_buffer_size_;
              if (OB_FAIL(init_decompress_ctx(ms->c, new_mode, block_size, ring_buffer_size))) {
                LOG_ERROR("failed to init decompress ctx", K(new_mode), K(block_size), K(ring_buffer_size), K(ret));
              } else {
                timeguard.click();
                preceding_data_len = cmd_packet.get_decode_size();
                decode_data_len = full_size - preceding_data_len;
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("invalid compresss mode", K(new_mode), K(ret));
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid mflag", K(mflag[0]), K(mflag[1]), K(mflag[2]), K(mflag[3]), K(ret));
    }
  }
  return ret;
}


int ObRpcProtocolProcessor::init_compress_ctx(easy_connection_t *easy_conn,
                                              ObRpcCompressMode mode,
                                              int16_t block_size,
                                              int32_t ring_buffer_size)
{
  bool is_compress = true;
  return init_ctx(easy_conn, mode, block_size, ring_buffer_size, is_compress);
}

int ObRpcProtocolProcessor::init_decompress_ctx(easy_connection_t *easy_conn,
                                                ObRpcCompressMode mode,
                                                int16_t block_size,
                                                int32_t ring_buffer_size)
{
  bool is_compress = false;
  return init_ctx(easy_conn, mode, block_size, ring_buffer_size, is_compress);
}

int ObRpcProtocolProcessor::init_ctx(easy_connection_t *easy_conn,
                                     ObRpcCompressMode mode, int16_t block_size,
                                     int32_t ring_buffer_size, bool is_compress)
{
  int ret = OB_SUCCESS;
  easy_pool_t *pool = NULL;
  ObRpcProxy::PCodeGuard pcode_guard(obrpc::OB_RPC_STREAM_TEST_INIT_CTX_PCODE);
  if (OB_ISNULL(easy_conn) || OB_ISNULL(pool = easy_conn->pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ms pool is NULL or connection is NULL", KP(easy_conn), KP(pool), K(ret));
  } else {
    ObRpcCompressCtxSet *ctx_set = NULL;
    if (NULL == easy_conn->user_data) {
      //this connection handle compressed packet for the first time
      uint32_t alloc_size = static_cast<uint32_t>(sizeof(ObRpcCompressCtxSet));
      char *buf = easy_alloc(pool, alloc_size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("easy failed to allocate memory", K(alloc_size), K(ret));
      } else {
        ctx_set = new (buf) ObRpcCompressCtxSet();
        easy_conn->user_data = ctx_set;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ctx_set = static_cast<ObRpcCompressCtxSet *>(easy_conn->user_data))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ctx_set should not be NULL", K(mode), K(block_size), K(ring_buffer_size), K(is_compress), K(ret));
      } else {
        ObRpcCompressCtx *ctx = is_compress ? static_cast<ObRpcCompressCtx *>(&ctx_set->compress_ctx_)
            : static_cast<ObRpcCompressCtx *>(&ctx_set->decompress_ctx_);
        if (!ctx->is_compress_mode_changed(mode)) {
          //do nothing
        } else if (ctx->is_inited_) {
          if (OB_FAIL(ctx->reset_mode(mode))) {
            LOG_WARN("failed to reset mode", K(mode), K(ret));
          } else {
            LOG_WARN("succ to reset mode", K(mode), "ctx", *ctx, K(ret));
          }
        } else {
          char *ring_buffer = NULL;
          if (OB_UNLIKELY(NULL == (ring_buffer = easy_alloc(pool, static_cast<uint32_t>(ring_buffer_size))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("failed to allocate memory for ring_buffer", K(ring_buffer_size), K(ret));
          } else if (OB_FAIL(ctx->init(mode, block_size, ring_buffer, ring_buffer_size))) {
            LOG_ERROR("failed to init compress ctx", K(mode), K(ret));
          } else {
            LOG_INFO("succ to init compress ctx", K(mode), "ctx", *ctx, K(ret));
          }
        }
      }
    }
  }
  return ret;
}

}//end of namespace obrpc
}//end of namespace oceanbase
