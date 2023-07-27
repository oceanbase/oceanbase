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
#include "rpc/obrpc/ob_rpc_compress_protocol_processor.h"

#include "io/easy_io.h"
#include "lib/compress/ob_compressor_pool.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_compress_struct.h"
#include "lib/utility/utility.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obrpc
{

int ObRpcCompressProtocolProcessor::encode(easy_request_t *req, ObRpcPacket *pkt)
{
  //TODO(yaoying.yyy): length check MAX_PACKET_LENGTH
  int ret = OB_SUCCESS;
  easy_message_session_t *ms = NULL;
  easy_connection_t *easy_conn = NULL;
  //If it involves switching modes, or the compression parameters have changed, you need to send cmd packet
  bool is_still_need_compress = true;
  ObRpcCompressMode compress_mode = ObRpcCompressCtx::get_compress_mode_from_type(pkt->get_compressor_type());

  ObCmdPacketInCompress::CmdType cmd_type = ObCmdPacketInCompress::CmdType::RPC_CMD_INVALID;
  common::ObTimeGuard timeguard("ObRpcCompressProtocolProcessor::encode", common::OB_EASY_HANDLER_COST_TIME);
  if (OB_ISNULL(req) || OB_ISNULL(pkt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Easy request or pkt is NULL", KP(req), KP(pkt), K(ret));
  } else if (OB_ISNULL(ms = req->ms) || OB_ISNULL(ms->pool) || OB_ISNULL(easy_conn = ms->c)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ms pool is NULL or connection is NULL", KP(ms), KP(easy_conn), K(ret));
  } else if (OB_FAIL(reset_compress_ctx_mode(easy_conn, compress_mode, pkt,
                                             cmd_type, is_still_need_compress))) {
    LOG_ERROR("failed to reset_compress_ctx", K(compress_mode), K(ret));
  } else {
    int32_t full_size = 0;
    if (ObCmdPacketInCompress::RPC_CMD_INVALID != cmd_type) {
      //send a CmdPacketInCompress first,
      ObCmdPacketInCompress cmd_packet(cmd_type);
      cmd_packet.set_compress_mode(compress_mode);

      char *cmd_buf = NULL;
      const int32_t cmd_packet_encode_size = static_cast<int32_t>(cmd_packet.get_encode_size());
      int32_t alloc_size = cmd_packet_encode_size + static_cast<int32_t>(sizeof(easy_buf_t));

      timeguard.click();
      char *buf = easy_alloc(ms->pool, alloc_size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory available", K(alloc_size), K(ret));
      } else {
        timeguard.click();
        easy_buf_t *ebuf = reinterpret_cast<easy_buf_t*>(buf); // pointer to easy buffer
        cmd_buf = reinterpret_cast<char*>(ebuf + 1);        // pointer to net header
        easy_buf_set_data(req->ms->pool, ebuf, cmd_buf, cmd_packet_encode_size);
        easy_request_addbuf(req, ebuf);
        timeguard.click();
      }

      if (OB_SUCC(ret)) {
        //encode packet data
        full_size += cmd_packet_encode_size;
        if (is_still_need_compress) {
          if (OB_FAIL(encode_compressed_rpc_packet(timeguard, req, pkt, full_size))) {
            LOG_WARN("failed to encode compressed_rpc_packet", "pkt", *pkt, K(ret));
          }
        } else {
          if (OB_FAIL(encode_raw_rpc_packet(timeguard, req, pkt, full_size))) {
            LOG_WARN("failed to encode raw_rpc_packet", "pkt", *pkt, K(ret));
          }
        }
        timeguard.click();
      }

      if (OB_SUCC(ret)) {
        //update full_size_ of ObCmdPacketIncompress and encode cmd packet
        cmd_packet.full_size_ = full_size;
        int64_t pos = 0;                                       // skip 4 bytes for magic flag
        if (OB_FAIL(cmd_packet.encode(cmd_buf, cmd_packet_encode_size, pos))) {
          LOG_WARN("failed to encode cmd packet", "pkt", *pkt, K(cmd_packet), K(cmd_packet_encode_size), K(ret));
        } else {
          LOG_INFO("succ to encode cmd_packet", K(cmd_packet), K(compress_mode), K(ret));
        }
      }
    } else {
      if (OB_FAIL(encode_compressed_rpc_packet(timeguard, req, pkt, full_size))) {
        LOG_WARN("failed to encode compressed_rpc_packet", "pkt", *pkt, K(ret));
      }
      timeguard.click();
    }
  }
  return ret;
}

int ObRpcCompressProtocolProcessor::decode(easy_message_t *m, ObRpcPacket *&pkt)
{
  int ret = OB_SUCCESS;
  int32_t recv_len = 0;
  char *in_data = NULL;
  common::ObTimeGuard timeguard("ObRpcCompressProtocolProcessor::decode", common::OB_EASY_HANDLER_COST_TIME);
  int32_t magic_with_size = static_cast<int32_t>(ObCompressPacketHeader::get_decode_size());
  if (OB_ISNULL(m)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(m));
  } else if (OB_ISNULL(m->input) || OB_ISNULL(m->pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(m->input), KP(m->pool), K(ret));
  } else if (OB_ISNULL(in_data = m->input->pos) || OB_ISNULL(m->input->last)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(in_data), KP(m->input->last), K(ret));
  } else if ((recv_len = static_cast<uint32_t>(m->input->last - m->input->pos)) < magic_with_size) {
    //data is not enough
  } else {
    int8_t magic_num = (int8_t)(0xff);
    int32_t full_size = 0;
    int64_t pos = 0;
    timeguard.click();
    if (OB_FAIL(serialization::decode_i8(m->input->pos, recv_len, pos, &magic_num))) {
      LOG_ERROR("failed to decode magic_num", K(ret));
    } else if (OB_FAIL(serialization::decode_i32(m->input->pos, recv_len, pos, &full_size))) {
      LOG_ERROR("failed to decode full_size", K(ret));
    } else if (recv_len < full_size) {
      //data is not enough
      if (NULL != m->c && NULL != m->c->sc) {
        //ssl will decrypt data into ssl buff, we should not set next_read_len here
      } else {
        m->next_read_len = full_size - recv_len;
      }
    } else {
      int64_t preceding_data_len = 0;
      if (static_cast<int8_t>(0xff) == magic_num) {
        //beginning with a cmd packet
        timeguard.click();
        ObCmdPacketInCompress cmd_packet;
        pos = 0;
        if (OB_FAIL(cmd_packet.decode(in_data, recv_len, pos))) {
          LOG_ERROR("failed to decode cmd_packet", K(ret));
        } else {
          ObRpcCompressMode compress_mode = static_cast<ObRpcCompressMode>(cmd_packet.compress_mode_);
          if (cmd_packet.is_modify_compress_mode()) {
            if (OB_FAIL(reset_decompress_ctx_mode(m->c, compress_mode))) {
              LOG_ERROR("failed to reset_compress_ctx", K(compress_mode), K(cmd_packet), K(ret));
            }
          } else if (cmd_packet.is_reset_compress_ctx()) {
            if (OB_FAIL(reset_decompress_ctx_ctx(m->c))) {
              LOG_WARN("failed to reset decompress ctx ctx", K(cmd_packet), K(ret));
            }

          } else {/*do nothing*/}

          if (OB_SUCC(ret)) {
            timeguard.click();
            preceding_data_len = cmd_packet.get_decode_size();
            if (RPC_STREAM_COMPRESS_NONE != compress_mode) {
              if (OB_FAIL(decode_compressed_packet_data(timeguard, m, preceding_data_len,
                                                        full_size - preceding_data_len, pkt))) {
                LOG_ERROR("failed to decode compressed packet data", K(cmd_packet), K(preceding_data_len),
                          K(full_size), K(ret));

              }
            } else {
              //From compressed mode to normal mode
              if (OB_FAIL(decode_raw_net_rpc_packet(timeguard, m, preceding_data_len, pkt))) {
                LOG_ERROR("failed to decode raw net packet", KP(m), K(cmd_packet), K(preceding_data_len), K(ret));
              }
            }
          }
        }
      } else {
        //Normal segmentPacket or HeadPacket
        timeguard.click();
        if (OB_FAIL(decode_compressed_packet_data(timeguard, m, preceding_data_len,
                                                  full_size - preceding_data_len, pkt))) {
          LOG_ERROR("failed to decode compressed packet data", KP(m), K(magic_num),
                    K(full_size), K(preceding_data_len), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRpcCompressProtocolProcessor::reset_compress_ctx_mode(easy_connection_t *easy_conn,
                                                            ObRpcCompressMode mode,
                                                            ObRpcPacket *&pkt,
                                                            ObCmdPacketInCompress::CmdType &cmd_type,
                                                            bool &is_still_need_compress)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(easy_conn) || OB_ISNULL(pkt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("connection or pkt is NULL", KP(easy_conn), KP(pkt), K(ret));
  } else if (NULL == easy_conn->user_data) {
    ret = OB_ERR_UNEXPECTED;;
    LOG_ERROR("user data of connection is NULL", K(ret));
  } else {
    cmd_type = ObCmdPacketInCompress::CmdType::RPC_CMD_INVALID;
    ObRpcCompressCtxSet *ctx_set = static_cast<ObRpcCompressCtxSet *>(easy_conn->user_data);
    ObRpcCompressCCtx &compress_ctx = ctx_set->compress_ctx_;
    bool is_compress_mode_changed = compress_ctx.is_compress_mode_changed(mode);
    if (is_compress_mode_changed) {
      cmd_type = ObCmdPacketInCompress::CmdType::RPC_CMD_MODIFY_COMPRESS_MODE;
      if (OB_FAIL(compress_ctx.reset_mode(mode))) {
        LOG_WARN("failed to reset compress mode", K(compress_ctx), K(mode), K(ret));
      } else {
        LOG_INFO("compress ctx reset mode", KP(easy_conn), K(mode), K(compress_ctx), K(ret));
      }
    } else {
      ObStreamCompressor *compressor = compress_ctx.get_stream_compressor();
      uint32_t rpc_pkt_size = static_cast<uint32_t>(pkt->get_encoded_size());
      int64_t net_pkt_size = rpc_pkt_size + OB_NET_HEADER_LENGTH;
      int64_t &total_data_size_before_compress = compress_ctx.total_data_size_before_compress_;
      if (OB_ISNULL(compressor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("compressor is NULL", K(compress_ctx), K(mode), K(*pkt), K(ret));
      } else if ((total_data_size_before_compress + net_pkt_size >= MAX_COMPRESS_DATA_SIZE)
                  && ObCompressorType::ZSTD_COMPRESSOR == compressor->get_compressor_type()) {
        if (OB_FAIL(compressor->reset_compress_ctx(compress_ctx.cctx_))) {
          LOG_WARN("failed to reset compress ctx", K(compress_ctx), K(ret));
        } else {
          cmd_type = ObCmdPacketInCompress::CmdType::RPC_CMD_RESET_COMPRESS_CTX;
          total_data_size_before_compress = 0;
        }
      } else {
        //do nothing
      }
    }

    if (OB_SUCC(ret)) {
      is_still_need_compress = !compress_ctx.is_normal_mode();
    }
  }
  return ret;
}

int ObRpcCompressProtocolProcessor::reset_decompress_ctx_mode(easy_connection_t *easy_conn,
                                                              ObRpcCompressMode mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(easy_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("connection is NULL", KP(easy_conn), K(ret));
  } else if (OB_ISNULL(easy_conn->user_data)) {
    ret = OB_ERR_UNEXPECTED;;
    LOG_ERROR("user data of connection is NULL", K(mode), K(ret));
  } else {
    ObRpcCompressCtxSet *ctx_set = static_cast<ObRpcCompressCtxSet *>(easy_conn->user_data);
    ObRpcCompressDCtx &decompress_ctx = ctx_set->decompress_ctx_;
    LOG_INFO("begin to reset decompress ctx", KP(easy_conn), K(mode), K(decompress_ctx), "ctx_set", *ctx_set, K(ret));
    bool is_compress_mode_changed = decompress_ctx.is_compress_mode_changed(mode);
    if (is_compress_mode_changed) {
      if (OB_FAIL(decompress_ctx.reset_mode(mode))) {
        LOG_WARN("failed to reset decompress mode", K(mode), K(ret));
      } else {
        LOG_INFO("decompress ctx reset mode", KP(easy_conn), KP(mode), K(decompress_ctx), K(ret));
      }
    }
  }
  return ret;
}

int ObRpcCompressProtocolProcessor::reset_decompress_ctx_ctx(easy_connection_t *easy_conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(easy_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("connection is NULL", KP(easy_conn), K(ret));
  } else if (OB_ISNULL(easy_conn->user_data)) {
    ret = OB_ERR_UNEXPECTED;;
    LOG_ERROR("user data of connection is NULL", KP(easy_conn), K(ret));
  } else {
    common::ObStreamCompressor *compressor = NULL;
    ObRpcCompressCtxSet *ctx_set = static_cast<ObRpcCompressCtxSet *>(easy_conn->user_data);
    ObRpcCompressDCtx &decompress_ctx = ctx_set->decompress_ctx_;
    if (OB_ISNULL(compressor = decompress_ctx.get_stream_compressor())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compressor from ctx is NULL", K(decompress_ctx), K(ret));
    } else if (OB_FAIL(compressor->reset_decompress_ctx(decompress_ctx.dctx_))) {
      LOG_WARN("failed to reset decompress ctx", K(decompress_ctx), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

}//end of namespace obrpc
}//end of namespace oceanbase
