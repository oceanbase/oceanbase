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

#include "io/easy_io.h"
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/trace/ob_trace_event.h"
#include "common/data_buffer.h"
#include "rpc/obrpc/ob_rpc_req_context.h"
#include "rpc/obrpc/ob_rpc_stream_cond.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "rpc/obrpc/ob_irpc_extra_payload.h"
#include "rpc/obrpc/ob_rpc_processor_base.h"

using namespace oceanbase::common;

namespace oceanbase {

namespace obrpc {

void ObRpcProcessorBase::reuse()
{
  rpc::frame::ObReqProcessor::reuse();
  rpc_pkt_ = NULL;
}

ObRpcProcessorBase::~ObRpcProcessorBase()
{
  reuse();
  if (NULL != sc_) {
    // memory is allocated by arena.
    sc_->~ObRpcStreamCond();
    sc_ = NULL;
  }
}

int ObRpcProcessorBase::deserialize()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_pkt_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "rpc_pkt_ should not be NULL", K(ret));
  } else {
    UNIS_VERSION_GUARD(rpc_pkt_->get_unis_version());
    int64_t len = rpc_pkt_->get_clen();
    const char* ez_buf = rpc_pkt_->get_cdata();
    const common::ObCompressorType& compressor_type = rpc_pkt_->get_compressor_type();
    const int32_t original_len = rpc_pkt_->get_original_len();
    int64_t pos = 0;
    if (OB_ISNULL(ez_buf)) {
      ret = OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(WARN, "ez buf should not be NULL", K(ret));
    }
    if (OB_SUCC(ret)) {
      auto level = get_rpc_checksum_check_level();
      if (ObRpcCheckSumCheckLevel::INVALID == level) {
        ret = OB_ERR_UNEXPECTED;
      } else if (ObRpcCheckSumCheckLevel::FORCE == level) {
        ret = rpc_pkt_->verify_checksum();
      } else if (ObRpcCheckSumCheckLevel::OPTIONAL == level) {
        if (rpc_pkt_->get_checksum() != 0) {
          ret = rpc_pkt_->verify_checksum();
        }
      } else if (ObRpcCheckSumCheckLevel::DISABLE == level) {
        // do-nothing
      }
      if (OB_FAIL(ret)) {
        RPC_OBRPC_LOG(ERROR, "verify packet checksum fail", K(*rpc_pkt_), K(ret));
      }
    }

    // handle compression
    bool need_compressed = ObCompressorPool::get_instance().need_common_compress(compressor_type);
    if (OB_SUCC(ret) && need_compressed) {
      common::ObCompressor* compressor = NULL;
      int64_t dst_data_size = 0;
      if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type, compressor))) {
        RPC_OBRPC_LOG(WARN, "get_compressor failed", K(ret), K(compressor_type));
      } else if (NULL == (uncompressed_buf_ = static_cast<char*>(
                              common::ob_malloc(original_len, common::ObModIds::OB_RPC_PROCESSOR)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        RPC_OBRPC_LOG(WARN, "Allocate memory failed", K(original_len), K(compressor_type), K(ret));
      } else if (OB_FAIL(compressor->decompress(ez_buf, len, uncompressed_buf_, original_len, dst_data_size))) {
        RPC_OBRPC_LOG(WARN, "decompress failed", K(original_len), K(compressor_type), K(ret));
      } else if (dst_data_size != original_len) {
        ret = common::OB_ERR_UNEXPECTED;
        RPC_OBRPC_LOG(ERROR, "decompress len not match", K(ret), K(dst_data_size), K(original_len));
      } else {
        ez_buf = uncompressed_buf_;
        len = original_len;
      }
    }

    if (OB_SUCC(ret)) {
      if (preserve_recv_data_) {
        char* new_buf = static_cast<char*>(common::ob_malloc(len, common::ObModIds::OB_RPC_PROCESSOR));
        if (!new_buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          RPC_OBRPC_LOG(WARN, "Allocate memory error", K(ret));
        } else {
          MEMCPY(new_buf, ez_buf, len);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(decode_base(new_buf, len, pos))) {
          int pcode = m_get_pcode();
          RPC_OBRPC_LOG(WARN, "decode argument fail", K(pcode), K(ret));
          common::ob_free(new_buf);
        } else {
          if (OB_SUCC(ret) && len > pos) {
            if (!rpc_pkt_->has_disable_debugsync()) {
              if (OB_FAIL(ObIRpcExtraPayload::instance().deserialize(ez_buf, len, pos))) {
                RPC_OBRPC_LOG(WARN, "decode debug sync actions fail", K(ret));
              }
            }
          }
          if (OB_SUCC(ret) && len > pos) {
            lib::RuntimeContext* ctx = lib::get_runtime_context();
            OB_ASSERT(ctx != nullptr);
            if (rpc_pkt_->has_context()) {
              if (OB_FAIL(common::serialization::decode(ez_buf, len, pos, *ctx))) {
                RPC_OBRPC_LOG(WARN, "decode debug sync actions fail", K(len), K(pos), K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            preserved_buf_ = new_buf;
          } else {
            RPC_OBRPC_LOG(WARN, "Decode error", K(ret), K(len), K(pos));
            common::ob_free(new_buf);
          }
        }
      } else {
        ret = decode_base(ez_buf, len, pos);
        if (OB_SUCC(ret) && len > pos) {
          if (!rpc_pkt_->has_disable_debugsync()) {
            if (OB_FAIL(ObIRpcExtraPayload::instance().deserialize(ez_buf, len, pos))) {
              RPC_OBRPC_LOG(WARN, "decode debug sync actions fail", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && len > pos) {
          lib::RuntimeContext* ctx = lib::get_runtime_context();
          OB_ASSERT(ctx != nullptr);
          if (rpc_pkt_->has_context()) {
            if (OB_FAIL(common::serialization::decode(ez_buf, len, pos, *ctx))) {
              RPC_OBRPC_LOG(WARN, "decode debug sync actions fail", K(len), K(pos), K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
          RPC_OBRPC_LOG(WARN, "Decode error", K(ret), K(len), K(pos));
        }
      }
    }
  }

  return ret;
}

int ObRpcProcessorBase::serialize()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(using_buffer_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "using_buffer_ should not be NULL", K(ret));
  } else if (OB_FAIL(encode_base(
                 using_buffer_->get_data(), using_buffer_->get_capacity(), using_buffer_->get_position()))) {
    RPC_OBRPC_LOG(WARN, "encode data error", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObRpcProcessorBase::do_response(const Response& rsp)
{
  int ret = OB_SUCCESS;
  if (is_stream_end_) {
    RPC_OBRPC_LOG(DEBUG, "stream rpc end before");
  } else if (OB_ISNULL(req_)) {
    ret = OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "req is NULL", K(ret));
  } else if (OB_ISNULL(req_->get_request())) {
    ret = OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "req is NULL", K(ret));
  } else if (OB_ISNULL(rpc_pkt_)) {
    ret = OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "rpc pkt is NULL", K(ret));
  } else {
    // TODO: , make force_destroy_second as a configure item
    // static const int64_t RESPONSE_RESERVED_US = 20 * 1000 * 1000;
    // int64_t rts = static_cast<int64_t>(req_->get_request()->start_time) * 1000 * 1000;
    // todo(): get 'force destroy second' from eio?
    // if (rts > 0 && eio_->force_destroy_second > 0
    //     && ::oceanbase::common::ObTimeUtility::current_time() - rts + RESPONSE_RESERVED_US >
    //     eio_->force_destroy_second * 1000000) {
    //   _OB_LOG(ERROR, "pkt process too long time: pkt_receive_ts=%ld, pkt_code=%d", rts, pcode);
    // }
    // copy packet into req buffer
    ObRpcPacketCode pcode = rpc_pkt_->get_pcode();
    if (OB_SUCC(ret)) {
      if (!rpc_pkt_->unneed_response()) {
        ObRpcPacket* packet = rsp.pkt_;
        packet->set_pcode(pcode);
        packet->set_chid(rpc_pkt_->get_chid());
        packet->set_session_id(rsp.sessid_);
        packet->set_trace_id(common::ObCurTraceId::get());
        packet->set_resp();
        // The cluster_id of the response must be the src_cluster_id of the request
        packet->set_dst_cluster_id(rpc_pkt_->get_src_cluster_id());

        packet->set_request_arrival_time(req_->get_request_arrival_time());
        packet->set_arrival_push_diff(req_->get_arrival_push_diff());
        packet->set_push_pop_diff(req_->get_push_pop_diff());
        packet->set_pop_process_start_diff(req_->get_pop_process_start_diff());
        packet->set_process_start_end_diff(req_->get_process_start_end_diff());
        packet->set_process_end_response_diff(req_->get_process_end_response_diff());
        if (rsp.is_stream_) {
          if (!rsp.is_stream_last_) {
            packet->set_stream_next();
          } else {
            packet->set_stream_last();
          }
        }
        if (rsp.bad_routing_) {
          packet->set_bad_routing();
        }
        packet->set_unis_version(unis_version_);
        packet->calc_checksum();
        req_->get_request()->opacket = packet;
        EVENT_INC(RPC_PACKET_OUT);
        EVENT_ADD(RPC_PACKET_OUT_BYTES, packet->get_clen() + packet->get_header_size() + common::OB_NET_HEADER_LENGTH);
      }
    }
    // just set request retcode, wakeup in ObSingleServer::handlePacketQueue()
    req_->set_request_rtcode(EASY_OK);
    wakeup_request();
  }
  return ret;
}

void ObRpcProcessorBase::compress_result(
    const char* src_buf, int64_t src_len, char* dst_buf, int64_t dst_len, ObRpcPacket* pkt)
{
  int ret = common::OB_SUCCESS;
  common::ObCompressor* compressor = nullptr;
  int64_t real_len = 0;
  bool need_compress = true;
  if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(result_compress_type_, compressor))) {
  } else if (OB_FAIL(compressor->compress(src_buf, src_len, dst_buf, dst_len, real_len))) {
    need_compress = false;
  } else if (real_len >= src_len) {
    need_compress = false;
  } else {
  }
  RPC_OBRPC_LOG(
      DEBUG, "result compressed", K(ret), K(need_compress), K_(result_compress_type), K(src_len), K(real_len));
  if (OB_SUCC(ret) && need_compress) {
    pkt->set_content(dst_buf, real_len);
    pkt->set_compressor_type(result_compress_type_);
    pkt->set_original_len(static_cast<int32_t>(src_len));
  } else {
    // use the original uncompressed data
    MEMCPY(dst_buf, src_buf, src_len);
    pkt->set_content(dst_buf, src_len);
    pkt->set_compressor_type(common::INVALID_COMPRESSOR);
    pkt->set_original_len(0);
  }
}

int ObRpcProcessorBase::part_response(const int retcode, bool is_last)
{
  int ret = OB_SUCCESS;
  if (req_has_wokenup_ || OB_ISNULL(req_)) {
    ret = OB_INVALID_ARGUMENT;
    RPC_OBRPC_LOG(
        WARN, "invalid req, maybe stream rpc timeout", K(ret), K(retcode), K(is_last), K(req_has_wokenup_), KP_(req));
  } else {
    ObRpcResultCode rcode;
    rcode.rcode_ = retcode;
    // add warning buffer into result code buffer if rpc fails.
    common::ObWarningBuffer* wb = common::ob_get_tsi_warning_buffer();
    if (wb) {
      if (retcode != OB_SUCCESS) {
        (void)snprintf(rcode.msg_, common::OB_MAX_ERROR_MSG_LEN, "%s", wb->get_err_msg());
      }
      // always add warning buffer
      bool not_null = true;
      for (uint32_t idx = 0; OB_SUCC(ret) && not_null && idx < wb->get_readable_warning_count(); idx++) {
        const common::ObWarningBuffer::WarningItem* item = wb->get_warning_item(idx);
        if (item != NULL) {
          if (OB_FAIL(rcode.warnings_.push_back(*item))) {
            RPC_OBRPC_LOG(WARN, "Failed to add warning", K(ret));
          }
        } else {
          not_null = false;
        }
      }
    }

    int64_t content_size = m_get_encoded_length() + common::serialization::encoded_length(rcode);
    int64_t max_overflow_size = 0;
    if (common::ObCompressorPool::get_instance().need_common_compress(result_compress_type_)) {
      // allocate larger buffer for the compressor
      common::ObCompressor* compressor = nullptr;
      if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(result_compress_type_, compressor))) {
        RPC_OBRPC_LOG(WARN, "failed to get compressor", K(ret), K_(result_compress_type));
        max_overflow_size = 0;
      } else if (OB_FAIL(compressor->get_max_overflow_size(content_size, max_overflow_size))) {
        RPC_OBRPC_LOG(WARN, "get_max_overflow_size failed", K(ret), K(content_size), K(max_overflow_size));
        max_overflow_size = 0;
      }
    }
    char* buf = NULL;
    char* tmp_buf = NULL;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (content_size + max_overflow_size > common::OB_MAX_PACKET_LENGTH) {
      ret = common::OB_RPC_PACKET_TOO_LONG;
      RPC_OBRPC_LOG(WARN, "response content size bigger than OB_MAX_PACKET_LENGTH", K(ret));
    } else {
      // allocate memory from easy
      //[ ObRpcPacket ... ObDatabuffer ... serilized content ...]
      int64_t size = (content_size + max_overflow_size) + sizeof(common::ObDataBuffer) + sizeof(ObRpcPacket);
      buf = static_cast<char*>(easy_alloc(size));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        RPC_OBRPC_LOG(WARN, "allocate rpc data buffer fail", K(ret), K(size));
      } else {
        using_buffer_ = new (buf + sizeof(ObRpcPacket)) common::ObDataBuffer();
        if (common::ObCompressorPool::get_instance().need_common_compress(result_compress_type_)) {
          tmp_buf = static_cast<char*>(ob_malloc(content_size, common::ObModIds::OB_RPC_PROCESSOR));
          if (NULL != tmp_buf) {
            // If compressed, serialize to another memory first
            if (!using_buffer_->set_data(tmp_buf, content_size)) {
              ret = OB_INVALID_ARGUMENT;
              RPC_OBRPC_LOG(WARN, "invalid parameters", K(ret));
            }
          }
        }
        if (NULL == tmp_buf) {
          if (!(using_buffer_->set_data(buf + sizeof(ObRpcPacket) + sizeof(*using_buffer_), content_size))) {
            ret = OB_INVALID_ARGUMENT;
            RPC_OBRPC_LOG(WARN, "invalid parameters", K(ret));
          }
        }
      }
    }

    // serialize
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(using_buffer_)) {
        ret = OB_ERR_UNEXPECTED;
        RPC_OBRPC_LOG(ERROR, "using_buffer_ is NULL", K(ret));
      } else if (OB_FAIL(rcode.serialize(
                     using_buffer_->get_data(), using_buffer_->get_capacity(), using_buffer_->get_position()))) {
        RPC_OBRPC_LOG(WARN, "serialize result code fail", K(ret));
      } else {
        // also send result if process successfully.
        if (common::OB_SUCCESS == retcode) {
          if (OB_FAIL(serialize())) {
            RPC_OBRPC_LOG(WARN, "serialize result fail", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t sessid = sc_ ? sc_->sessid() : 0;
      ObRpcPacket* pkt = new (buf) ObRpcPacket();
      Response rsp(sessid, is_stream_, is_last, bad_routing_, pkt);
      if (common::ObCompressorPool::get_instance().need_common_compress(result_compress_type_) && NULL != tmp_buf) {
        // compress the serialized result buffer
        char* dst_buf = buf + sizeof(ObRpcPacket) + sizeof(*using_buffer_);
        compress_result(
            using_buffer_->get_data(), using_buffer_->get_position(), dst_buf, content_size + max_overflow_size, pkt);
      } else {
        pkt->set_content(using_buffer_->get_data(), using_buffer_->get_position());
      }
      if (OB_FAIL(do_response(rsp))) {
        RPC_OBRPC_LOG(WARN, "response data fail", K(ret));
      }
    }

    using_buffer_ = NULL;
    if (NULL != tmp_buf) {
      common::ob_free(tmp_buf);
      tmp_buf = NULL;
    }
  }
  return ret;
}

int ObRpcProcessorBase::flush(int64_t wait_timeout)
{
  int ret = OB_SUCCESS;
  is_stream_ = true;
  rpc::ObRequest* req = NULL;
  UNIS_VERSION_GUARD(unis_version_);

  if (nullptr == sc_) {
    sc_ = OB_NEWx(ObRpcStreamCond, (&lib::this_worker().get_sql_arena_allocator()), *sh_);
    if (nullptr == sc_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      RPC_OBRPC_LOG(WARN, "allocate stream condition object fail", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(rpc_pkt_) || is_stream_end_) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(WARN, "request is NULL, maybe wait timeout", K(ret), K(rpc_pkt_), K(is_stream_end_));
  } else if (rpc_pkt_ && rpc_pkt_->is_stream_last()) {
    ret = OB_ITER_END;
    RPC_OBRPC_LOG(WARN, "stream is end", K(ret), K(*rpc_pkt_));
  } else if (OB_FAIL(sc_->prepare())) {
    RPC_OBRPC_LOG(WARN, "prepare stream session fail", K(ret));
  } else if (OB_FAIL(part_response(common::OB_SUCCESS, false))) {
    RPC_OBRPC_LOG(WARN, "response part result to peer fail", K(ret));
  } else if (FALSE_IT({ NG_TRACE(transmit); })) {
  } else if (OB_FAIL(sc_->wait(req, wait_timeout))) {
    NG_TRACE(receive);
    req_ = NULL;  // wait fail, invalid req_
    reuse();
    is_stream_end_ = true;
    RPC_OBRPC_LOG(WARN, "wait next packet fail, set req_ to null", K(ret), K(wait_timeout));
  } else if (OB_ISNULL(req)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "Req should not be NULL", K(ret));
  } else {
    NG_TRACE(receive);
    reuse();
    set_ob_request(*req);
    if (!rpc_pkt_) {
      wakeup_request();
      is_stream_end_ = true;
      ret = OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(ERROR, "rpc packet is NULL in stream", K(ret));
    } else if (rpc_pkt_->is_stream_last()) {
      ret = OB_ITER_END;
    } else {
      // do nothing
    }
  }

  // here we don't care what exactly the packet is.

  return ret;
}

void ObRpcProcessorBase::cleanup()
{
  if (preserve_recv_data_) {
    if (preserved_buf_) {
      common::ob_free(preserved_buf_);
    } else {
      RPC_OBRPC_LOG(WARN, "preserved buffer is NULL, maybe alloc fail");
    }
  }

  if (uncompressed_buf_) {
    common::ob_free(uncompressed_buf_);
    uncompressed_buf_ = NULL;
  }

  // record
  // TODO: support streaming interface
  if (!is_stream_) {
    rpc::RpcStatPiece piece;
    piece.is_server_ = true;
    piece.size_ = pkt_size_;
    piece.net_time_ = get_receive_timestamp() - get_send_timestamp();
    piece.wait_time_ = get_enqueue_timestamp() - get_receive_timestamp();
    piece.queue_time_ = get_run_timestamp() - get_enqueue_timestamp();
    piece.process_time_ = common::ObTimeUtility::current_time() - get_run_timestamp();
    RPC_STAT(static_cast<ObRpcPacketCode>(m_get_pcode()), piece);
  }
}

}  // end of namespace obrpc
}  // end of namespace oceanbase
