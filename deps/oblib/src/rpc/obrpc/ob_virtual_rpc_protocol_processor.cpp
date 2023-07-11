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
#include "rpc/obrpc/ob_virtual_rpc_protocol_processor.h"

#include <byteswap.h>
#include "io/easy_io.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_tracepoint.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "rpc/frame/ob_req_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::rpc::frame;

namespace oceanbase
{
namespace obrpc
{

char *ObVirtualRpcProtocolProcessor::easy_alloc(easy_pool_t *pool, int64_t size) const
{
  char *buf = NULL;
  if (size > UINT32_MAX || size < 0) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid size", K(size));
  } else if (OB_ISNULL(pool)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Easy pool is NULL");
  } else {
    buf = static_cast<char*>(easy_pool_alloc(pool, static_cast<uint32_t>(size)));
  }
  return buf;
}

int ObVirtualRpcProtocolProcessor::set_next_read_len(easy_message_t *ms,
                                                     const int64_t fallback_len,
                                                     const int64_t next_read_len)
{
  INIT_SUCC(ret);
  if (OB_UNLIKELY(NULL == ms) || OB_UNLIKELY(NULL == ms->input)
      || OB_UNLIKELY(fallback_len < 0) || OB_UNLIKELY(next_read_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ms->input->pos -= fallback_len;
    if (NULL != ms->c && NULL != ms->c->sc) {
      //ssl will decrypt data into ssl buff, we should not set next_read_len here
    } else {
      ms->next_read_len = static_cast<int>(next_read_len);
    }
  }
  return ret;
}

/*  if (size of rpc_packet is less than block size) {
 *     send one ObCompressSegmentPacket;
 *   } else {
 *     send one ObCompressHeadPacket + N ObCompressSegmentPacket
 *   }
 */
int ObVirtualRpcProtocolProcessor::encode_compressed_rpc_packet(ObTimeGuard &timeguard,
                                                                easy_request_t *req,
                                                                ObRpcPacket *&pkt,
                                                                int32_t &full_size)
{
  int ret = OB_SUCCESS;
  easy_connection_t *easy_conn = NULL;
  ObRpcCompressCtxSet *ctx_set = NULL;
  common::ObStreamCompressor *compressor = NULL;
  void *cctx = NULL;
  if (OB_ISNULL(req) || OB_ISNULL(pkt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Easy request or pkt is NULL", KP(req), KP(pkt), K(ret));
  } else if (OB_ISNULL(req->ms) || OB_ISNULL(easy_conn = req->ms->c)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ms or connection is NULL", KP(req->ms), KP(easy_conn), K(ret));
  } else if (OB_ISNULL(ctx_set = static_cast<ObRpcCompressCtxSet *>(easy_conn->user_data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("compress ctx set is NULL", K(ret));
  } else {
    ObRpcCompressCCtx &compress_ctx = ctx_set->compress_ctx_;
    if (!compress_ctx.is_inited_) {
      ret = OB_NOT_INIT;
      LOG_ERROR("compress_ctx is not inited", K(ret));
    } else if (OB_ISNULL(compressor = compress_ctx.get_stream_compressor())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compressor from ctx is NULL",  K(ret));
    } else if (OB_ISNULL(cctx = compress_ctx.cctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compress ctx from ctx is NULL",  K(ret));
    } else {
      uint32_t rpc_pkt_size = static_cast<uint32_t>(pkt->get_encoded_size());
      int64_t net_pkt_size = rpc_pkt_size + OB_NET_HEADER_LENGTH;
      int64_t block_size = compress_ctx.block_size_;
      int64_t &ring_buffer_pos = compress_ctx.ring_buffer_pos_;
      int64_t ring_buffer_size = compress_ctx.ring_buffer_size_;
      char *ring_buffer = compress_ctx.ring_buffer_;

      //The header size should not exceed 31K
      int16_t rpc_pkt_header_size = static_cast<int16_t>(pkt->get_header_size() + OB_NET_HEADER_LENGTH);
      if (rpc_pkt_header_size >= block_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected rpc_pkt_header_size", K(rpc_pkt_header_size), K(block_size), K(*pkt), K(ret));
      } else if (net_pkt_size < block_size) {
        if (ring_buffer_pos + net_pkt_size > ring_buffer_size) {
          //LOG_INFO("YYY encode reset ring_buffer_pos", K(compress_ctx), K(net_pkt_size), K(ret));
          ring_buffer_pos = 0;
        }
        ObCompressSegmentPacketHeader header;
        int64_t header_size = header.get_encode_size();
        int64_t segment_start_pos = ring_buffer_pos;
        timeguard.click();
        MEMCPY(ring_buffer + ring_buffer_pos, ObRpcPacket::MAGIC_HEADER_FLAG, 4);
        timeguard.click();
        ring_buffer_pos += 4;
        if (OB_FAIL(encode_i32(ring_buffer, ring_buffer_size, ring_buffer_pos, rpc_pkt_size))) { // next 4 for packet content length
          LOG_WARN("failed to encode int32_t", K(rpc_pkt_size), K(ret));
        } else if (OB_FAIL(encode_i32(ring_buffer, ring_buffer_size, ring_buffer_pos, pkt->get_chid()))) {
          // next 4 for channel id
          LOG_WARN("failed to encode packet chid", K(ret));
        } else if (OB_FAIL(encode_i32(ring_buffer, ring_buffer_size, ring_buffer_pos, 0))) {
        //skip 4 bytes for reserved
          LOG_WARN("failed to encode reserved", K(ret));
        } else if (OB_FAIL(pkt->encode(ring_buffer, ring_buffer_size, ring_buffer_pos))) {
          LOG_ERROR("failed to encode packet", K(ret));
        } else {
          timeguard.click();
          char *header_buf = NULL;
          int64_t pos = 0;
          int64_t compressed_size = 0;
          if (OB_FAIL(encode_segment(req, compress_ctx, ring_buffer + segment_start_pos, net_pkt_size, header_size,
                                     header_buf, compressed_size))) {
            LOG_ERROR("failed to encode segment", K(ret), K(segment_start_pos), K(net_pkt_size), K(header_size));
          } else if (OB_FAIL(header.init(compress_ctx.compress_mode_, (int16_t)(0 == compressed_size ? net_pkt_size : compressed_size), (int16_t)net_pkt_size,
                                 0 != compressed_size))) {
            LOG_ERROR("failed to init header", K(header), K(compress_ctx), K(ret));
          } else if (OB_FAIL(header.encode(header_buf, header_size, pos))) {
            LOG_ERROR("failed to encode header", K(header), K(ret));
          } else {
            int32_t total_data_len_after_compress = (int32_t)(header_size + (0 == compressed_size ? net_pkt_size : compressed_size));
            full_size += total_data_len_after_compress;
            EVENT_ADD(RPC_STREAM_COMPRESS_COMPRESSED_SIZE, total_data_len_after_compress);
            EVENT_ADD(RPC_STREAM_COMPRESS_ORIGINAL_SIZE, net_pkt_size);
            EVENT_INC(RPC_STREAM_COMPRESS_COMPRESSED_PACKET_CNT);
          }
        }
      } else {
        ObCompressHeadPacketHeader head_packet_header;
        int16_t packet_cdata_comsumed = 0;
        int16_t head_packet_payload = 0;
        int16_t head_packet_origin_payload = 0;
        bool is_head_packet_compressed = false;
        int32_t total_data_len_before_compress = (int32_t)net_pkt_size;
        int32_t total_data_len_after_compress = 0;
        char *head_packet_header_buf = NULL;
        int64_t head_packet_full_size = 0;
        int64_t head_packet_header_size = head_packet_header.get_encode_size();
        int64_t compressed_size = 0;
        {
          //construct HeadPacket, headPacket's payload is block_size
          if (ring_buffer_pos > ring_buffer_size - block_size) {
            ring_buffer_pos = 0;
          }

          int64_t segment_start_pos = ring_buffer_pos;
          timeguard.click();
          MEMCPY(ring_buffer + ring_buffer_pos, ObRpcPacket::MAGIC_HEADER_FLAG, 4);
          timeguard.click();
          ring_buffer_pos += 4;
          if (OB_FAIL(encode_i32(ring_buffer, ring_buffer_size, ring_buffer_pos, rpc_pkt_size))) { // next 4 for packet content length
            LOG_ERROR("failed to encode rpc_pkt_size", K(rpc_pkt_size), K(ret));
          } else if (OB_FAIL(encode_i32(ring_buffer, ring_buffer_size, ring_buffer_pos, pkt->get_chid()))) {
            // next 4 for channel id
            LOG_ERROR("failed to encode chid", K(ret));
          } else if (OB_FAIL(encode_i32(ring_buffer, ring_buffer_size, ring_buffer_pos, 0))) {
            //skip 4 bytes for reserved
            LOG_ERROR("failed to encode reserved", K(ret));
          } else if (OB_FAIL(pkt->encode_header(ring_buffer, ring_buffer_size, ring_buffer_pos))) {
            LOG_ERROR("failed to encode packet header", K(ring_buffer_size), K(ring_buffer_pos), K(ret));
          } else {
            head_packet_origin_payload = static_cast<int16_t>(rpc_pkt_header_size);
            timeguard.click();
            if (pkt->get_clen() > 0) {
              packet_cdata_comsumed = static_cast<int16_t>(min(static_cast<int64_t>(pkt->get_clen()), block_size - rpc_pkt_header_size));
              MEMCPY(ring_buffer + ring_buffer_pos, const_cast<char *>(pkt->get_cdata()), packet_cdata_comsumed);
              timeguard.click();
              head_packet_origin_payload = static_cast<int16_t>(head_packet_origin_payload + packet_cdata_comsumed);
              ring_buffer_pos += packet_cdata_comsumed;
            }
            timeguard.click();
            if (OB_FAIL(ret)) {
              //do nothing
            } else if (OB_FAIL(encode_segment(req, compress_ctx, ring_buffer + segment_start_pos, head_packet_origin_payload,
                                       head_packet_header_size, head_packet_header_buf, compressed_size))) {
              LOG_ERROR("failed to encode segment", K(ret), K(segment_start_pos), K(head_packet_origin_payload), K(head_packet_header_size));
            } else {
              timeguard.click();
              is_head_packet_compressed = (0 != compressed_size);
              total_data_len_after_compress += (int32_t)(is_head_packet_compressed ?  compressed_size : head_packet_origin_payload);
              head_packet_payload = (int16_t)(is_head_packet_compressed? compressed_size : head_packet_origin_payload);
              head_packet_full_size += head_packet_header_size + head_packet_payload;
            }
          }
        }

        if (OB_SUCC(ret)) {
          //contruct following SegmentPacket
          int64_t remain_cdata_size = pkt->get_clen() - packet_cdata_comsumed;
          while (OB_SUCC(ret) && remain_cdata_size > 0) {
            ObCompressSegmentPacketHeader header;
            int64_t header_size = header.get_encode_size();
            int64_t segment_size = min(remain_cdata_size, block_size);
            if (ring_buffer_pos > ring_buffer_size - segment_size) {
              ring_buffer_pos = 0;
            }
            int64_t segment_start_pos = ring_buffer_pos;
            timeguard.click();
            MEMCPY(ring_buffer + ring_buffer_pos, const_cast<char *>(pkt->get_cdata()) + pkt->get_clen() - remain_cdata_size, segment_size);
            timeguard.click();
            ring_buffer_pos += segment_size;

            char *header_buf = NULL;
            int64_t compressed_size = 0;
            int64_t pos = 0;
            if (OB_FAIL(encode_segment(req, compress_ctx, ring_buffer + segment_start_pos, segment_size,
                                       header_size, header_buf, compressed_size))) {
              LOG_ERROR("failed to encode segment", K(ret), K(segment_start_pos), K(segment_size), K(header_size));
            } else if (header.init(compress_ctx.compress_mode_, (int16_t)(0 == compressed_size ? segment_size : compressed_size),
                                   (int16_t)segment_size, 0 != compressed_size)) {
              LOG_ERROR("failed to init header", K(header), K(ret));
            } else if (OB_FAIL(header.encode(header_buf, header_size, pos))) {
              LOG_ERROR("failed to encode header", K(header), K(ret));
            } else {
              timeguard.click();
              remain_cdata_size -= segment_size;
              total_data_len_after_compress += (int32_t)(0 == compressed_size ? segment_size : compressed_size);
              head_packet_full_size += header_size + (0 == compressed_size ? segment_size : compressed_size);
            }
          }
        }

        if (OB_SUCC(ret)) {
          int64_t pos = 0;
          timeguard.click();
          if (head_packet_header.init(compress_ctx.compress_mode_,
                                      static_cast<int32_t>(head_packet_full_size),
                                      total_data_len_before_compress,
                                      total_data_len_after_compress,
                                      head_packet_payload,
                                      head_packet_origin_payload,
                                      is_head_packet_compressed)) {
            LOG_ERROR("failed to init header", K(head_packet_header), K(ret));
          } else if (OB_FAIL(head_packet_header.encode(head_packet_header_buf, head_packet_header_size, pos))) {
            LOG_ERROR("failed to encode header", K(head_packet_header), K(ret));
          } else {
            timeguard.click();
            full_size += static_cast<int32_t>(head_packet_full_size);
            EVENT_ADD(RPC_STREAM_COMPRESS_COMPRESSED_SIZE, head_packet_full_size);
            EVENT_ADD(RPC_STREAM_COMPRESS_ORIGINAL_SIZE, total_data_len_before_compress);
            EVENT_INC(RPC_STREAM_COMPRESS_COMPRESSED_PACKET_CNT);
          }
        }
      }
    }
  }
  return ret;
}

int ObVirtualRpcProtocolProcessor::encode_raw_rpc_packet(ObTimeGuard &timeguard,
                                                         easy_request_t *req,
                                                         ObRpcPacket *&pkt,
                                                         int32_t &full_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req) || OB_ISNULL(pkt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("request or packet should not be NULL", KP(req), KP(pkt), K(ret));
  } else if (OB_ISNULL(req->ms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Easy message session(ms) or ms pool is NULL", "ms", "req->ms", K(ret));
  } else {
    uint32_t rpc_packet_size = static_cast<uint32_t>(pkt->get_encoded_size());
    uint32_t rpc_header_size = static_cast<uint32_t>(pkt->get_header_size());
    uint32_t ez_pkt_size = OB_NET_HEADER_LENGTH + rpc_packet_size;
    uint32_t ez_rpc_header_size = OB_NET_HEADER_LENGTH + rpc_header_size;

    if (ez_pkt_size > get_max_rpc_packet_size()) {
      // We find out the packet size would beyond max size of limitation
      // in OceanBase.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("packet size beyond limit", K(ret), K(ez_pkt_size), "limit", get_max_rpc_packet_size());
    } else {
      timeguard.click();
      ObRpcProxy::PCodeGuard pcode_guard(OB_RPC_STREAM_TEST_ENCODE_RAW_PCODE);
      {
        /*
         *                   RPC packet buffer format
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  |  ObRpcPacket  |  easy header |  RPC header  | RPC payload |
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *
         *  For RPC request packet, RPC payload is RPC request parameter. For RPC response
         *  packet, RPC payload are rcode + RPC result.
         *
         */
        easy_buf_t *ebuf = &(req->tx_ebuf);
        int64_t pos = 0;
        char *ez_rpc_header = reinterpret_cast<char*>(pkt + 1);

        if (OB_FAIL(pkt->encode_ez_header(ez_rpc_header, ez_rpc_header_size, pos))) {
          LOG_WARN("failed to encode easy packet header", K(ret));
        } else if (OB_FAIL(pkt->encode_header(ez_rpc_header, ez_rpc_header_size, pos))) {
          LOG_WARN("failed to encode rpc packet header", K(ret));
        } else {
          /*
           * In TX side, req->ms->pool is NULL, easy_session does not have dedicated pool. ebuf->args
           * is mainly used by easy_session_process_low_level. We need to change the algorithm of
           * easy_session_process_low_levellater later.
           *
           * In RX side, req->ms->pool does not make any sense. It will be changed to easy_request_t
           * by easy_buf_set_cleanup soon.
           */
          easy_buf_set_data(req->ms->pool, ebuf, ez_rpc_header, ez_rpc_header_size + pkt->get_clen());
          easy_request_addbuf(req, ebuf);
        }
      }
      timeguard.click();
      //ERRSIM: 10 injections and one error
      ret = OB_E(EventTable::EN_RPC_ENCODE_RAW_DATA_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        if (REACH_TIME_INTERVAL(1000000)) {
          LOG_ERROR("ERRSIM: fake encode raw packet err", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        full_size += ez_pkt_size;
        LOG_DEBUG("encode packet success packet code",
                  "pcode", pkt->get_pcode(), "connection", easy_connection_str(req->ms->c));
      }
    }
    timeguard.click();
  }
  return ret;
}

int ObVirtualRpcProtocolProcessor::decode_compressed_packet_data(ObTimeGuard &timeguard,
                                                                 easy_message_t *ms,
                                                                 const int64_t preceding_data_len,
                                                                 const int64_t decode_data_len,
                                                                 ObRpcPacket *&pkt)
{
  int ret = OB_SUCCESS;
  easy_connection_t *easy_conn = NULL;
  ObRpcCompressCtxSet *ctx_set = NULL;
  int64_t recv_len = 0;
  ObRpcProxy::PCodeGuard pcode_guard(OB_RPC_STREAM_TEST_DECODE_COMPRESS_PCODE);
  if (OB_ISNULL(ms) || OB_UNLIKELY(preceding_data_len < 0) || OB_UNLIKELY(decode_data_len <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(ms), K(preceding_data_len), K(decode_data_len), K(ret));
  } else if (OB_ISNULL(ms->pool) || OB_ISNULL(ms->input) || OB_ISNULL(easy_conn = ms->c)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ms pool is NULL or connection is NULL", KP(ms->pool), KP(ms->input), KP(easy_conn), K(ret));
  } else if (OB_ISNULL(ms->input->pos) || OB_ISNULL(ms->input->last)
             || (recv_len = ms->input->last - ms->input->pos) < (preceding_data_len + decode_data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid input", KP(ms->input->pos), KP(ms->input->last), K(recv_len),
              K(preceding_data_len), K(decode_data_len), K(ret));
  } else if (OB_ISNULL(ctx_set = static_cast<ObRpcCompressCtxSet *>(easy_conn->user_data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid rpc compress ctx set is NULL", KP(ctx_set), K(ret));
  } else {
    ObRpcCompressDCtx &decompress_ctx = ctx_set->decompress_ctx_;
    if (!decompress_ctx.is_inited_) {
      ret = OB_NOT_INIT;
      LOG_ERROR("decompress_ctx is not inited", K(ret));
    } else {
      char *data = ms->input->pos + preceding_data_len;
      int64_t data_len = decode_data_len;
      //The buf used to store the decompressed network packet, including an ObRpcPacket object space
      char *net_packet_buf = NULL;
      int64_t net_packet_buf_size = 0;
      int64_t net_packet_buf_pos = sizeof(ObRpcPacket);
      if (data[0] & (int8_t)0x80) {
        //single small packet
        int64_t pos = 0;
        ObCompressSegmentPacketHeader header;
        timeguard.click();
        if (OB_FAIL(header.decode(data, data_len, pos))) {
          LOG_WARN("failed to decode", K(ret));
        } else if (header.origin_size_ > get_max_rpc_packet_size()) {
          ret = OB_RPC_PACKET_TOO_LONG;
          LOG_WARN("obrpc packet payload exceed its limit", K(ret), K(header.origin_size_), "limit", get_max_rpc_packet_size());
        } else {
          timeguard.click();
          int32_t full_size = header.full_size_;
          int16_t compressed_size = static_cast<int16_t>(full_size - header.get_encode_size());
          int16_t original_size = header.origin_size_;
          net_packet_buf_size = original_size + sizeof(ObRpcPacket);
          if (OB_UNLIKELY(data_len != full_size)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid data", K(data_len), K(full_size), K(pos), K(header), K(ret));
          } else if (OB_UNLIKELY(NULL == (net_packet_buf = easy_alloc(ms->pool, net_packet_buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed allocate memory", K(net_packet_buf_size), K(header), K(ret));
          } else if (OB_FAIL(decode_segment(decompress_ctx,
                                            data,
                                            net_packet_buf,
                                            net_packet_buf_size,
                                            header.is_data_compressed(),
                                            compressed_size,
                                            original_size,
                                            pos,
                                            net_packet_buf_pos))) {
            LOG_WARN("failed to decode_segment", KP(data), KP(net_packet_buf),
                     K(compressed_size), K(pos), K(decompress_ctx), K(net_packet_buf_pos), K(header),
                     "is_data_compressed", header.is_data_compressed(), K(decompress_ctx), K(ret));
          } else {
            /*do nothing*/
          }
          timeguard.click();
        }
      } else {
        //handle big packet
        ObCompressHeadPacketHeader header;
        int64_t pos = 0;
        timeguard.click();
        if (OB_FAIL(header.decode(data, data_len, pos))) {
          LOG_WARN("failed to decode HeadPacketHeader", K(ret));
        } else if (header.total_data_len_before_compress_ > get_max_rpc_packet_size()) {
          ret = OB_RPC_PACKET_TOO_LONG;
          LOG_WARN("obrpc packet payload exceed its limit", K(ret), K(header.total_data_len_before_compress_), "limit", get_max_rpc_packet_size());
        } else if (OB_UNLIKELY(data_len != header.full_size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid data", K(data_len), K(pos), K(header), K(ret));
        } else {
          int32_t total_data_len_before_compress = header.total_data_len_before_compress_;
          int16_t compressed_size = header.compressed_size_;
          int16_t original_size = header.origin_size_;

          net_packet_buf_size = total_data_len_before_compress + sizeof(ObRpcPacket);
          timeguard.click();
          if (OB_UNLIKELY(NULL == (net_packet_buf = easy_alloc(ms->pool, net_packet_buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed allocate memory", K(net_packet_buf_size), K(header), K(ret));
          } else {
            timeguard.click();
            if (OB_FAIL(decode_segment(decompress_ctx,
                                       data,
                                       net_packet_buf,
                                       net_packet_buf_size,
                                       header.is_data_compressed(),
                                       compressed_size,
                                       original_size,
                                       pos,
                                       net_packet_buf_pos))) {
              LOG_WARN("failed to decode_segment", KP(data), KP(net_packet_buf),
                       K(compressed_size), K(pos), K(decompress_ctx), K(net_packet_buf_pos), K(ret));
            }

            timeguard.click();
            while (OB_SUCC(ret) && pos < data_len) {
              ObCompressSegmentPacketHeader seg_header;
              if (OB_FAIL(seg_header.decode(data, data_len, pos))) {
                LOG_ERROR("failed to decode", K(ret));
              } else {
                int32_t full_size = seg_header.full_size_;
                int16_t original_size = seg_header.origin_size_;

                int16_t compressed_size = static_cast<int16_t>(full_size - seg_header.get_encode_size());
                timeguard.click();
                if (OB_FAIL(decode_segment(decompress_ctx,
                                           data,
                                           net_packet_buf,
                                           net_packet_buf_size,
                                           seg_header.is_data_compressed(),
                                           compressed_size,
                                           original_size,
                                           pos,
                                           net_packet_buf_pos))) {
                  LOG_WARN("failed to decode_segment", KP(data), KP(net_packet_buf),
                           K(compressed_size), K(pos), K(decompress_ctx), K(net_packet_buf_pos), K(ret));
                } else {
                  timeguard.click();
                }
              }
            }
          }
        }
      }

      //ERRSIM: 10 injections once is the best
      ret = OB_E(EventTable::EN_RPC_DECODE_COMPRESS_DATA_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        if (REACH_TIME_INTERVAL(1000000)) {
          LOG_ERROR("ERRSIM: fake decode compress packet err", K(ret));
        }
      }

      //handle net_packet
      if (OB_SUCC(ret)) {
        if (OB_FAIL(decode_net_rpc_packet(timeguard, net_packet_buf, net_packet_buf_size, pkt))) {
          LOG_WARN("failed to decode net rpc packet", K(net_packet_buf_size), K(ret));
        } else {
          int64_t full_len = preceding_data_len + decode_data_len;
          ms->input->pos += full_len;
        }
      }
    }
  }
  return ret;
}

int ObVirtualRpcProtocolProcessor::decode_net_rpc_packet(ObTimeGuard &timeguard,
                                                         char *data, int64_t data_len, ObRpcPacket *&pkt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data) || OB_UNLIKELY(data_len <= OB_NET_HEADER_LENGTH + sizeof(ObRpcPacket))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(data), K(data_len), "rpc_packet_size", sizeof(ObRpcPacket), K(ret));
  } else {
    // decode net header (16 bytes)
    char *net_header = data + sizeof(ObRpcPacket);
    timeguard.click();
    uint8_t mflag[4] = { static_cast<uint8_t> (net_header[0]),
      static_cast<uint8_t> (net_header[1]),
      static_cast<uint8_t> (net_header[2]),
      static_cast<uint8_t> (net_header[3]) };
    uint32_t plen = bswap_32(*((uint32_t *)(net_header + 4)));
    uint32_t chid = bswap_32(*((uint32_t *)(net_header + 8)));
    // ignore 4 bytes for reserved

    timeguard.click();
    if (data_len != OB_NET_HEADER_LENGTH + plen + sizeof(ObRpcPacket)) {
      ret = OB_ERR_UNEXPECTED;
      hex_dump(net_header, OB_NET_HEADER_LENGTH, true, OB_LOG_LEVEL_INFO);
      LOG_WARN("invalid data", K(data_len), K(plen), "rpc_packet_size", sizeof(ObRpcPacket), K(ret));
    } else if (OB_UNLIKELY(0 != MEMCMP(&mflag[0], ObRpcPacket::MAGIC_HEADER_FLAG, 4))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("packet magic flag mismatch", K(ret));
    } else {
      timeguard.click();
      pkt = new (data) ObRpcPacket();
      pkt->set_chid(chid);
      int64_t rpc_packet_pos = sizeof(ObRpcPacket) + OB_NET_HEADER_LENGTH;
      char *pbuf = data + rpc_packet_pos;
      if (OB_FAIL(pkt->decode(pbuf, data_len - rpc_packet_pos))) {
        // decode packet header fail
        pkt = NULL;
        LOG_WARN("decode packet fail, close connection", K(ret));
      }
      timeguard.click();
    }
  }
  return ret;
}

int ObVirtualRpcProtocolProcessor::decode_raw_net_rpc_packet(common::ObTimeGuard &timeguard,
                                                             easy_message_t *ms,
                                                             const int64_t preceding_data_len,
                                                             ObRpcPacket *&pkt)
{
  int ret = OB_SUCCESS;

  pkt = NULL;

  ObRpcProxy::PCodeGuard pcode_guard(OB_RPC_STREAM_TEST_DECODE_RAW_PCODE);
  if (OB_ISNULL(ms) || OB_UNLIKELY(preceding_data_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arugment", KP(ms), K(preceding_data_len), K(ret));
  } else if (OB_ISNULL(ms->input) || OB_ISNULL(ms->pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", "input", ms->input, "pool", ms->pool, K(ret));
  } else {
    uint32_t recv_len = 0;
    if ((recv_len = static_cast<uint32_t>(ms->input->last - ms->input->pos)) >= preceding_data_len + OB_NET_HEADER_LENGTH) {
      char *&in_data = ms->input->pos;
      if (OB_ISNULL(in_data)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("in_data is NULL", KP(in_data), K(ret));
      } else {
        // decode net header (16 bytes)
        timeguard.click();
        char *net_header_data = ms->input->pos + preceding_data_len;
        uint8_t mflag[4] = { static_cast<uint8_t> (net_header_data[0]),
                                static_cast<uint8_t> (net_header_data[1]),
                                static_cast<uint8_t> (net_header_data[2]),
                                static_cast<uint8_t> (net_header_data[3]) };
        uint32_t plen = bswap_32(*((uint32_t *)(net_header_data + 4)));
        uint32_t chid = bswap_32(*((uint32_t *)(net_header_data + 8)));
        // ignore 4 bytes for reserved

        // net header length plus packet content length
        const uint32_t full_demanded_len = OB_NET_HEADER_LENGTH + plen + static_cast<uint32_t>(preceding_data_len);
        timeguard.click();
        if (0 != MEMCMP(&mflag[0], ObRpcPacket::MAGIC_HEADER_FLAG, 4)) {
          // magic header flag mismatch, close this connection with setting
          // EASY_ERROR to message status
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("packet magic flag mismatch, close connection", K(ret));
          ms->status = EASY_ERROR;
        } else if (plen > get_max_rpc_packet_size()) {
          ret = OB_RPC_PACKET_TOO_LONG;
          LOG_WARN("obrpc packet payload exceed its limit", K(ret), K(plen), "limit", get_max_rpc_packet_size());
        } else if (recv_len < full_demanded_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid recv_len ", K(recv_len), K(full_demanded_len), K(ret));
        } else {
          uint32_t alloc_size = static_cast<uint32_t>(sizeof (ObRpcPacket)) + plen;
          timeguard.click();
          char *buf = easy_alloc(ms->pool, alloc_size);
          if (OB_UNLIKELY(NULL == buf)) {
            // reject this connection, for fear that network buffer
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("no memory available, close connection", "ms status", ms->status, K(ret));
          } else {
            timeguard.click();
            pkt = new (buf) ObRpcPacket();
            pkt->set_chid(chid);
            char *pbuf = buf + sizeof(ObRpcPacket);
            MEMCPY(pbuf, net_header_data + OB_NET_HEADER_LENGTH, plen);
            timeguard.click();
            if (OB_FAIL(pkt->decode(pbuf, plen))) {
              // decode packet header fail
              LOG_WARN("decode packet fail, close connection", K(ret), "ms status", ms->status);
            }
            in_data += full_demanded_len;

            //ERRSIM: 10 injections once is the best
            ret = OB_E(EventTable::EN_RPC_ENCODE_RAW_DATA_ERR) OB_SUCCESS;
            if (OB_FAIL(ret)) {
              if (REACH_TIME_INTERVAL(1000000)) {
                LOG_ERROR("ERRSIM: fake decode raw packet err", K(ret));
              }
            }

          }
          timeguard.click();
        }
      }

    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid recv_len ", K(recv_len), K(preceding_data_len), K(ret));
    }
  }
  return ret;
}

int ObVirtualRpcProtocolProcessor::decode_segment(ObRpcCompressDCtx &ctx,
                                                  char *data,
                                                  char *net_packet_buf,
                                                  int64_t net_packet_buf_size,
                                                  bool is_data_compressed,
                                                  int16_t compressed_size,
                                                  int16_t original_size,
                                                  int64_t &pos,
                                                  int64_t &net_packet_buf_pos)
{
  int ret = OB_SUCCESS;
  void *&dctx = ctx.dctx_;
  common::ObStreamCompressor *compressor = NULL;
  char *ring_buffer = NULL;
  common::ObTimeGuard timeguard("ObVirtualRpcProtocolProcessor::decode_segment", DECODE_SEGMENT_COST_TIME);

  if (OB_ISNULL(data) || OB_ISNULL(net_packet_buf) || OB_UNLIKELY(compressed_size <= 0) || OB_ISNULL(dctx)
      || OB_UNLIKELY(original_size <= 0) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(net_packet_buf_pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(data), KP(net_packet_buf), KP(dctx),
             K(compressed_size), K(pos), K(original_size), K(net_packet_buf_pos), K(is_data_compressed), K(ret));
  } else if (OB_UNLIKELY(!is_data_compressed && compressed_size != original_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arguments", KP(data), KP(net_packet_buf), K(is_data_compressed), K(compressed_size),
             K(original_size), K(pos), K(net_packet_buf_pos), K(ret));
  } else if (OB_ISNULL(compressor = ctx.get_stream_compressor())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("compressor from ctx is NULL", K(ctx), K(ret));
  } else if (OB_ISNULL(ring_buffer = ctx.get_ring_buffer())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ring_buffer from ctx is NULL", K(ctx), K(ret));
  } else {
    int64_t &ring_buffer_pos = ctx.ring_buffer_pos_;
    int64_t ring_buffer_size = ctx.ring_buffer_size_;
    int64_t decompressed_size = 0;
    if (ring_buffer_pos + original_size > ring_buffer_size) {
      ring_buffer_pos = 0;
    }

    timeguard.click();
    if (OB_UNLIKELY(original_size > net_packet_buf_size - net_packet_buf_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("net packet buf is not enough", K(original_size),
                K(net_packet_buf_size), K(net_packet_buf_pos), K(ret));
    } else if (!is_data_compressed) {
      timeguard.click();
      MEMCPY(ring_buffer + ring_buffer_pos, data + pos, original_size);
      timeguard.click();
      if (OB_FAIL(compressor->insert_uncompressed_block(dctx, ring_buffer + ring_buffer_pos, original_size))) {
        LOG_ERROR("failed to insert_uncompressed_block", K(ring_buffer_pos), K(original_size), KP(dctx), K(ret));
      } else {
        LOG_INFO("data is not compressed", K(ctx), K(original_size), K(ret));
        timeguard.click();
        MEMCPY(net_packet_buf + net_packet_buf_pos, data + pos, original_size);
        timeguard.click();
        ring_buffer_pos += original_size;
        net_packet_buf_pos += original_size;
        pos += original_size;
      }
    } else if (OB_FAIL(compressor->stream_decompress(dctx,
                                                     data + pos,
                                                     compressed_size,
                                                     ring_buffer + ring_buffer_pos,
                                                     ring_buffer_size - ring_buffer_pos,
                                                     decompressed_size))) {
      LOG_WARN("failed to stream_decompress", K(dctx), K(compressed_size), K(pos), K(ret));
    } else if (OB_UNLIKELY(decompressed_size != original_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decompressed_size is not equal to original_size", K(decompressed_size), K(original_size), K(ret));
    } else {
      timeguard.click();
      MEMCPY(net_packet_buf + net_packet_buf_pos, ring_buffer + ring_buffer_pos, original_size);
      timeguard.click();
      ring_buffer_pos += original_size;
      net_packet_buf_pos += original_size;
      pos += compressed_size;
    }
  }
  return ret;
}


int ObVirtualRpcProtocolProcessor::encode_segment(easy_request_t *req,
                                                  ObRpcCompressCCtx &compress_ctx,
                                                  char *segment, int64_t segment_size,
                                                  int64_t header_size, char *&header_buf,
                                                  int64_t &compressed_size)
{
  int ret = OB_SUCCESS;

  int64_t dest_size = 0;
  char *buf = NULL;
  easy_pool_t *pool = NULL;
  ObStreamCompressor *compressor = compress_ctx.get_stream_compressor();
  void *&ctx = compress_ctx.cctx_;
  ObRpcProxy::PCodeGuard pcode_guard(OB_RPC_STREAM_TEST_ENCODE_SEGMENT_PCODE);
  common::ObTimeGuard timeguard("ObVirtualRpcProtocolProcessor::encode_segment", ENCODE_SEGMENT_COST_TIME);
  if (OB_ISNULL(req) || OB_ISNULL(req->ms) ||OB_ISNULL(pool = req->ms->pool) || OB_ISNULL(compressor)
      || OB_ISNULL(ctx) || OB_ISNULL(segment)
      || OB_UNLIKELY(segment_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(req), KP(compressor), KP(ctx), KP(segment), K(segment_size), K(ret));
  } else if (OB_FAIL(compressor->get_compress_bound_size(segment_size, dest_size))) {
    LOG_WARN("failed to get_compress_bound_size", K(segment_size), K(ret));
  } else if (NULL == (buf = easy_alloc(pool, header_size + dest_size +  sizeof(easy_buf_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate memory", K(dest_size), K(ret));
  } else {
    timeguard.click();
    easy_buf_t *cur_ebuf = reinterpret_cast<easy_buf_t*>(buf);
    header_buf = reinterpret_cast<char*>(cur_ebuf + 1);
    char *dest_buf = header_buf + header_size;
    int64_t &total_data_size_before_compress = compress_ctx.total_data_size_before_compress_;
    if (OB_FAIL(compressor->stream_compress(ctx, segment, segment_size, dest_buf, dest_size, compressed_size))) {
      LOG_WARN("failed to stream_compress", K(segment_size), K(dest_size), KP(segment), KP(buf), KP(dest_buf), K(ret));
    } else if (0 == compressed_size) {
      //Only the compressed_size of zstd will return OB_SUCCESS with a value of 0, indicating that the data is not compressible and special processing is required to send the data before compression
      timeguard.click();
      MEMCPY(dest_buf, segment, segment_size);
      easy_buf_set_data(pool, cur_ebuf, header_buf, static_cast<uint32_t>(header_size + segment_size));
      easy_request_addbuf(req, cur_ebuf);
      total_data_size_before_compress += segment_size;
    } else {
      //In order to keep the two compression algorithms consistent, the compressed data is still transmitted if the compressed length is longer
      timeguard.click();
      easy_buf_set_data(pool, cur_ebuf, header_buf, static_cast<uint32_t>(header_size + compressed_size));
      easy_request_addbuf(req, cur_ebuf);
      total_data_size_before_compress += segment_size;

      //ERRSIM: 10 injections once is the best
      ret = OB_E(EventTable::EN_RPC_ENCODE_SEGMENT_DATA_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        if (REACH_TIME_INTERVAL(1000000)) {
          LOG_ERROR("ERRSIM: fake encode segment err", K(ret));
        }
      }

    }

    timeguard.click();
  }
  return ret;
}

}//end of namespace obrpc
}//end of namespace oceanbase
