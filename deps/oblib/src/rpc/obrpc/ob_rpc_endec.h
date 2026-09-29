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

#ifndef OCEANBASE_OBRPC_OB_RPC_ENDEC_H_
#define OCEANBASE_OBRPC_OB_RPC_ENDEC_H_
#include "rpc/obrpc/ob_rpc_opts.h"
#include "rpc/obrpc/ob_rpc_mem_pool.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/ash/ob_active_session_guard.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/trace/ob_trace.h"

extern "C" {
void* pn_send_alloc(uint64_t gtid, int64_t sz);
void pn_send_free(void* p);
};

namespace oceanbase
{
namespace obrpc
{
extern int64_t get_max_rpc_packet_size();
class ObRpcProxy;
int64_t calc_extra_payload_size();
int fill_extra_payload(ObRpcPacket& pkt, char* buf, int64_t len, int64_t &pos);
int init_packet(ObRpcProxy& proxy, ObRpcPacket& pkt, ObRpcPacketCode pcode, const ObRpcOpts &opts,
                const bool unneed_response);
common::ObCompressorType get_proxy_compressor_type(ObRpcProxy& proxy);
class ObRpcPreparedBody;
ObRpcPreparedBody *get_proxy_reusable_body(ObRpcProxy &proxy);
uint64_t get_proxy_tenant_id(ObRpcProxy &proxy);
int rpc_encode_prepared_req(ObRpcProxy &proxy, uint64_t gtid, ObRpcPacketCode pcode,
                            const ObRpcOpts &opts, const ObRpcPreparedBody &body,
                            bool unneed_resp, char *&req, int64_t &req_sz);

// One logical request, one synchronous fan-out. Each transport owns its own copy
// of this body; the object must not be shared between threads or different args.
class ObRpcPreparedBody
{
public:
  ObRpcPreparedBody();
  ~ObRpcPreparedBody();

  // Prepare once for identical args in this fan-out. A non-success return asks
  // the caller to use its original encoding path. nullptr selects the pool's compressor.
  template <typename T>
  int prepare(const T &args, ObRpcPacketCode pcode, uint64_t tenant_id,
              common::ObCompressorType compressor_type,
              common::ObCompressor *compressor = nullptr);

  bool matches(ObRpcPacketCode pcode, uint64_t tenant_id,
               common::ObCompressorType compressor_type) const;
  const char *get_data() const { return data_; }
  int64_t get_size() const { return size_; }
  int64_t get_original_size() const { return original_size_; }
  int64_t get_payload_size() const { return payload_size_; }
  bool is_compressed() const { return compressed_; }
  // Copy into caller-owned storage and a fresh packet; no ownership is transferred.
  int fill_packet(ObRpcPacket &pkt, char *dst, int64_t capacity) const;
  // These existing counters describe packets/bytes, not compressor invocations.
  void record_packet_stat(bool count_original_on_fallback = true) const;

private:
  enum State { EMPTY, READY, BYPASS };
  int finish_prepare(const ObRpcPacket &metadata, common::ObCompressor *compressor);
  void release_buffer();
  State state_;         // EMPTY: unprepared; READY: reusable; BYPASS: use original encoding.
  char *data_;          // Owned serialized request body, optionally compressed.
  int64_t size_;        // Body bytes copied into each outgoing packet.
  int64_t original_size_; // Serialized body size before compression.
  int64_t payload_size_;  // Original easy payload estimate plus compression overflow, for statistics.
  // Request metadata checked before reusing the prepared body.
  ObRpcPacketCode pcode_;
  uint64_t tenant_id_;
  common::ObCompressorType compressor_type_;
  bool compressed_;     // Whether data_ contains compressed bytes.
  // Extra-payload flags copied to each outgoing packet.
  bool has_context_;
  bool disable_debugsync_;
  bool has_trace_info_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcPreparedBody);
};

template <typename T>
int ObRpcPreparedBody::prepare(const T &args, ObRpcPacketCode pcode,
                              uint64_t tenant_id, common::ObCompressorType compressor_type,
                              common::ObCompressor *compressor)
{
  int ret = common::OB_SUCCESS;
  if (OBTRACE->is_inited()) {
    // Trace extra payload can change between destinations; preserve old semantics.
    state_ = BYPASS;
    ret = common::OB_NOT_SUPPORTED;
  } else if (READY == state_) {
    ret = matches(pcode, tenant_id, compressor_type)
        ? common::OB_SUCCESS
        : common::OB_STATE_NOT_MATCH;
    if (OB_SUCC(ret) && original_size_ > get_max_rpc_packet_size()) {
      ret = common::OB_RPC_PACKET_TOO_LONG;
    }
  } else if (BYPASS == state_) {
    ret = common::OB_NOT_SUPPORTED;
  } else {
    // Attempt preparation only once, including allocation/serialization failures.
    state_ = BYPASS;
    if (!common::ObCompressorPool::need_common_compress(compressor_type)) {
      ret = common::OB_NOT_SUPPORTED;
    } else {
      int64_t args_size = 0;
      int64_t extra_size = 0;
      int64_t limit = 0;
#ifdef ENABLE_SERIALIZATION_CHECK
      lib::begin_record_serialization();
#endif
      args_size = common::serialization::encoded_length(args);
#ifdef ENABLE_SERIALIZATION_CHECK
      lib::finish_record_serialization();
#endif
      extra_size = calc_extra_payload_size();
      limit = get_max_rpc_packet_size();
      if (args_size < 0 || extra_size < 0 || args_size > limit || extra_size > limit - args_size) {
        ret = common::OB_RPC_PACKET_TOO_LONG;
      } else if ((original_size_ = args_size + extra_size) <= 0 || original_size_ > INT32_MAX) {
        ret = common::OB_INVALID_ARGUMENT;
      } else if (OB_ISNULL(data_ = static_cast<char *>(
                   common::ob_malloc(original_size_, common::ObModIds::OB_RPC)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        int64_t pos = 0;
        ObRpcPacket metadata;
        payload_size_ = original_size_;
        pcode_ = pcode;
        tenant_id_ = tenant_id;
        compressor_type_ = compressor_type;
        if (OB_FAIL(common::serialization::encode(data_, original_size_, pos, args))) {
          // Keep the error for cleanup and let the caller encode without reuse.
        } else if (pos < 0 || pos > args_size) {
#ifdef ENABLE_SERIALIZATION_CHECK
          lib::begin_check_serialization();
          common::serialization::encoded_length(args);
          lib::finish_check_serialization();
#endif
          ret = common::OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(fill_extra_payload(metadata, data_, original_size_, pos))) {
          // Keep the error for cleanup and let the caller encode without reuse.
        } else if (pos < 0 || pos > original_size_) {
          ret = common::OB_ERR_UNEXPECTED;
        } else {
          // encoded_length may be an upper bound; never compress allocation slack.
          original_size_ = pos;
          if (OB_FAIL(finish_prepare(metadata, compressor))) {
            // Preparation stays bypassed after an allocation/compressor setup failure.
          } else {
            state_ = READY;
          }
        }
      }
      if (OB_FAIL(ret)) {
        RPC_OBRPC_LOG(DEBUG, "prepare reusable rpc body failed, use original encoding", K(ret), K(pcode));
        release_buffer();
      }
    }
  }
  return ret;
}

template <typename T>
int rpc_encode_unprepared_req(ObRpcProxy &proxy, uint64_t gtid, ObRpcPacketCode pcode,
                              const T &args, const ObRpcOpts &opts, char *&req, int64_t &req_sz,
                              bool unneed_resp, bool is_next, bool is_last, int64_t session_id)
{
  int ret = common::OB_SUCCESS;
  ObRpcPacket pkt;
  const int64_t header_sz = pkt.get_header_size();
  int64_t extra_payload_size = calc_extra_payload_size();
#ifdef ENABLE_SERIALIZATION_CHECK
  lib::begin_record_serialization();
  int64_t args_len = common::serialization::encoded_length(args);
  lib::finish_record_serialization();
#else
  int64_t args_len = common::serialization::encoded_length(args);
#endif
  int64_t payload_sz = extra_payload_size + args_len;
  char* header_buf  = NULL;
  char* payload_buf = NULL;
  if (payload_sz > get_max_rpc_packet_size()) {
    ret = common::OB_RPC_PACKET_TOO_LONG;
    RPC_OBRPC_LOG(ERROR, "obrpc packet payload execced its limit",
                  K(payload_sz), "limit", get_max_rpc_packet_size(), K(ret));
  } else {
    header_buf = (char*)pn_send_alloc(gtid, header_sz + payload_sz);
    payload_buf = header_buf + header_sz;
  }
  int64_t pos = 0;
  UNIS_VERSION_GUARD(opts.unis_version_);
  if (OB_FAIL(ret)) {
    // OB_RPC_PACKET_TOO_LONG
  } else if (NULL == header_buf) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WARN, "alloc buffer fail", K(payload_sz));
  } else if (OB_FAIL(common::serialization::encode(
                         payload_buf, payload_sz, pos, args))) {
    RPC_OBRPC_LOG(WARN, "serialize argument fail", K(pos), K(payload_sz), K(ret));
  } else if (OB_UNLIKELY(args_len < pos)) {
#ifdef ENABLE_SERIALIZATION_CHECK
    lib::begin_check_serialization();
    common::serialization::encoded_length(args);
    lib::finish_check_serialization();
#endif
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "arg encoded length greater than arg length", K(ret), K(payload_sz),
                  K(args_len), K(extra_payload_size), K(pos), K(pcode));
  } else if (OB_FAIL(fill_extra_payload(pkt, payload_buf, payload_sz, pos))) {
    RPC_OBRPC_LOG(WARN, "fill extra payload fail", K(ret), K(pos), K(payload_sz), K(args_len),
                  K(extra_payload_size), K(pcode));
  } else {
    const common::ObCompressorType &compressor_type = get_proxy_compressor_type(proxy);
    bool need_compressed = common::ObCompressorPool::get_instance().need_common_compress(compressor_type);
    if (need_compressed) {
      // compress
      EVENT_INC(RPC_COMPRESS_ORIGINAL_PACKET_CNT);
      EVENT_ADD(RPC_COMPRESS_ORIGINAL_SIZE, payload_sz);
      int tmp_ret = OB_SUCCESS;
      common::ObCompressor *compressor = NULL;
      char *compressed_buf = NULL;
      int64_t dst_data_size = 0;
      int64_t max_overflow_size = 0;
      if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type, compressor))) {
        RPC_OBRPC_LOG(WARN, "get_compressor failed", K(ret), K(compressor_type));
      } else if (OB_FAIL(compressor->get_max_overflow_size(payload_sz, max_overflow_size))) {
        RPC_OBRPC_LOG(WARN, "get_max_overflow_size failed", K(ret), K(payload_sz), K(max_overflow_size));
      } else if (NULL == (compressed_buf = static_cast<char *>(
                              common::ob_malloc(payload_sz + max_overflow_size, common::ObModIds::OB_RPC_PROCESSOR)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        RPC_OBRPC_LOG(WARN, "Allocate memory failed", K(ret));
      } else if (OB_SUCCESS !=
                 (tmp_ret = compressor->compress(
                      payload_buf, payload_sz, compressed_buf, payload_sz + max_overflow_size, dst_data_size))) {
        RPC_OBRPC_LOG(WARN, "compress failed", K(tmp_ret));
      } else if (dst_data_size >= payload_sz) {
      } else {
        RPC_OBRPC_LOG(DEBUG, "compress request success", K(compressor_type), K(dst_data_size), K(payload_sz));
        // replace buf
        pkt.set_compressor_type(compressor_type);
        pkt.set_original_len(static_cast<int32_t>(payload_sz));
        memcpy(payload_buf, compressed_buf, dst_data_size);
        payload_sz = dst_data_size;
        EVENT_INC(RPC_COMPRESS_COMPRESSED_PACKET_CNT);
        EVENT_ADD(RPC_COMPRESS_COMPRESSED_SIZE, dst_data_size);
      }
      if (NULL != compressed_buf) {
        ob_free(compressed_buf);
        compressed_buf = NULL;
      }
    }
    int64_t header_pos = 0;
    pkt.set_content(payload_buf, payload_sz);
    if (OB_FAIL(init_packet(proxy, pkt, pcode, opts, unneed_resp))) {
      RPC_OBRPC_LOG(WARN, "init packet fail", K(ret));
    } else {
      if (is_next) {
          pkt.set_stream_next();
      }
      if (is_last) {
          pkt.set_stream_last();
      }
      if (session_id) {
        pkt.set_session_id(session_id);
      }
      if (OB_FAIL(pkt.encode_header(header_buf, header_sz, header_pos))) {
        RPC_OBRPC_LOG(WARN, "encode header fail", K(ret));
      } else {
        req = header_buf;
        req_sz = header_sz + payload_sz;
      }
    }
  }
  if (OB_FAIL(ret) && NULL != header_buf) {
    pn_send_free(header_buf);
  }
  return ret;
}

template <typename T>
    int rpc_encode_req(
      ObRpcProxy& proxy,
      uint64_t gtid,
      ObRpcPacketCode pcode,
      const T& args,
      const ObRpcOpts& opts,
      char*& req,
      int64_t& req_sz,
      bool unneed_resp,
      bool is_next = false,
      bool is_last = false,
      int64_t session_id = 0
    )
{
  int ret = common::OB_SUCCESS;
  bool use_prepared = false;
  ObRpcPreparedBody *body = nullptr;
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_rpc_encode);
  body = get_proxy_reusable_body(proxy);
  // This branch is opt-in for ordinary PALF fan-out, never stream continuation.
  if (OB_NOT_NULL(body) && !is_next && !is_last && 0 == session_id) {
    int tmp_ret = common::OB_SUCCESS;
    tmp_ret = body->prepare(args, pcode, get_proxy_tenant_id(proxy),
                           get_proxy_compressor_type(proxy));
    if (common::OB_SUCCESS != tmp_ret) {
      // Preparation failure only disables reuse; preserve ret for the original path.
    } else {
      use_prepared = true;
    }
  }
  if (use_prepared) {
    // Once prepared encoding starts, its error is returned without an original-path retry.
    ret = rpc_encode_prepared_req(proxy, gtid, pcode, opts, *body, unneed_resp, req, req_sz);
  } else {
    ret = rpc_encode_unprepared_req(proxy, gtid, pcode, args, opts, req, req_sz,
                                    unneed_resp, is_next, is_last, session_id);
  }
  return ret;
}

template <typename T>
int rpc_decode_resp(const char* resp_buf, int64_t resp_sz, T& result, ObRpcPacket &pkt, ObRpcResultCode &rcode)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_rpc_decode);
  int ret = common::OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(pkt.decode(resp_buf, resp_sz))) {
    RPC_OBRPC_LOG(WARN, "decode packet fail", KP(resp_buf), K(resp_sz), K(pos));
  } else {
    UNIS_VERSION_GUARD(pkt.get_unis_version());
    const char* payload = pkt.get_cdata();
    int64_t limit = pkt.get_clen();
    if (OB_FAIL(rcode.deserialize(payload, limit, pos))) {
      rcode.rcode_ = common::OB_DESERIALIZE_ERROR;
      RPC_OBRPC_LOG(WARN, "deserialize result code fail", KP(payload), K(limit), K(resp_sz), K(pos));
    } else {
      if (rcode.rcode_ != common::OB_SUCCESS) {
        ret = rcode.rcode_;
      } else if (OB_FAIL(common::serialization::decode(payload, limit, pos, result))) {
        RPC_OBRPC_LOG(WARN, "deserialize result fail", KP(payload), K(limit), K(resp_sz), K(pos));
      } else {
        ret = rcode.rcode_;
      }
    }
  }
  return ret;
}

int rpc_decode_ob_packet(const char* buf, int64_t sz, ObRpcPacket& ret_pkt);
int rpc_encode_ob_packet(ObRpcMemPool& pool, ObRpcPacket* pkt, char*& buf, int64_t& sz, int64_t reserve_buf_size);

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_RPC_ENDEC_H_ */
