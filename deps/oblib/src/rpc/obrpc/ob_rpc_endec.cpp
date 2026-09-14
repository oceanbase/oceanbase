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
#include "ob_rpc_endec.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "lib/trace/ob_trace.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
namespace oceanbase
{
namespace obrpc
{
ObRpcPreparedBody *get_proxy_reusable_body(ObRpcProxy &proxy)
{
  return proxy.get_reusable_body();
}

uint64_t get_proxy_tenant_id(ObRpcProxy &proxy)
{
  return proxy.get_tenant();
}

ObRpcPreparedBody::ObRpcPreparedBody()
    : state_(EMPTY), data_(nullptr), size_(0), original_size_(0), payload_size_(0),
      pcode_(OB_INVALID_RPC_CODE), tenant_id_(OB_INVALID_TENANT_ID),
      compressor_type_(INVALID_COMPRESSOR), compressed_(false),
      has_context_(false), disable_debugsync_(false), has_trace_info_(false)
{
}

ObRpcPreparedBody::~ObRpcPreparedBody()
{
  release_buffer();
}

void ObRpcPreparedBody::release_buffer()
{
  if (OB_NOT_NULL(data_)) {
    ob_free(data_);
    data_ = nullptr;
  }
  size_ = 0;
  original_size_ = 0;
  payload_size_ = 0;
}

bool ObRpcPreparedBody::matches(ObRpcPacketCode pcode, uint64_t tenant_id,
                                ObCompressorType compressor_type) const
{
  return READY == state_ && pcode_ == pcode && tenant_id_ == tenant_id
      && compressor_type_ == compressor_type;
}

int ObRpcPreparedBody::finish_prepare(const ObRpcPacket &metadata, ObCompressor *compressor)
{
  int ret = OB_SUCCESS;
  int64_t overflow_size = 0;
  char *compressed_buf = nullptr;
  if (OB_ISNULL(compressor)
      && OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_type_, compressor))) {
    // Preserve setup failure; prepare() releases the body and disables reuse.
  } else if (OB_ISNULL(compressor) || original_size_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(compressor->get_max_overflow_size(payload_size_, overflow_size))) {
    // Preserve setup failure; prepare() releases the body and disables reuse.
  } else if (overflow_size < 0 || overflow_size > INT64_MAX - payload_size_
      || overflow_size > get_max_rpc_packet_size() - payload_size_) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_ISNULL(compressed_buf = static_cast<char *>(
               ob_malloc(payload_size_ + overflow_size, ObModIds::OB_RPC)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    int64_t compressed_size = 0;
    payload_size_ += overflow_size;
    const int compress_ret = compressor->compress(data_, original_size_, compressed_buf,
                                                  payload_size_, compressed_size);
    size_ = original_size_;
    if (OB_SUCCESS == compress_ret && compressed_size > 0 && compressed_size < original_size_) {
      ob_free(data_);
      data_ = compressed_buf;
      compressed_buf = nullptr;
      size_ = compressed_size;
      compressed_ = true;
    } else if (OB_SUCCESS != compress_ret) {
      // Preserve the serialized original body, including on all later cache hits.
      LOG_DEBUG("compress reusable rpc body failed, use original body", K(compress_ret), K_(pcode));
    }
    has_context_ = metadata.has_context();
    disable_debugsync_ = metadata.has_disable_debugsync();
    has_trace_info_ = metadata.has_trace_info();
  }
  if (OB_NOT_NULL(compressed_buf)) {
    ob_free(compressed_buf);
    compressed_buf = nullptr;
  }
  return ret;
}

int ObRpcPreparedBody::fill_packet(ObRpcPacket &pkt, char *dst, int64_t capacity) const
{
  int ret = OB_SUCCESS;
  if (READY != state_ || OB_ISNULL(dst) || capacity < size_ || OB_ISNULL(data_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(dst, data_, size_);
    pkt.set_content(dst, size_);
    pkt.set_compressor_type(compressed_ ? compressor_type_ : INVALID_COMPRESSOR);
    pkt.set_original_len(compressed_ ? static_cast<int32_t>(original_size_) : 0);
    // The caller supplies a fresh packet. These flags are independent.
    if (has_context_) {
      pkt.set_has_context();
    }
    if (disable_debugsync_) {
      pkt.set_disable_debugsync();
    }
    if (has_trace_info_) {
      pkt.set_has_trace_info();
    }
  }
  return ret;
}

void ObRpcPreparedBody::record_packet_stat(bool count_original_on_fallback) const
{
  EVENT_INC(RPC_COMPRESS_ORIGINAL_PACKET_CNT);
  EVENT_ADD(RPC_COMPRESS_ORIGINAL_SIZE, original_size_);
  if (compressed_) {
    EVENT_INC(RPC_COMPRESS_COMPRESSED_PACKET_CNT);
  }
  // PNIO historically does not add this counter for uncompressed fallback;
  // easy does. Preserve each backend's existing packet/byte accounting.
  if (compressed_ || count_original_on_fallback) {
    EVENT_ADD(RPC_COMPRESS_COMPRESSED_SIZE, size_);
  }
}

int rpc_encode_prepared_req(ObRpcProxy &proxy, uint64_t gtid, ObRpcPacketCode pcode,
                            const ObRpcOpts &opts, const ObRpcPreparedBody &body,
                            bool unneed_resp, char *&req, int64_t &req_sz)
{
  int ret = OB_SUCCESS;
  ObRpcPacket pkt;
  const int64_t header_size = pkt.get_header_size();
  char *buf = nullptr;
  req = nullptr;
  req_sz = 0;
  if (!body.matches(pcode, proxy.get_tenant(), proxy.get_compressor_type())) {
    ret = OB_STATE_NOT_MATCH;
  } else if (body.get_size() > get_max_rpc_packet_size()
      || body.get_size() > INT64_MAX - header_size) {
    ret = OB_RPC_PACKET_TOO_LONG;
  } else if (OB_ISNULL(buf = static_cast<char *>(
               pn_send_alloc(gtid, header_size + body.get_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(body.fill_packet(pkt, buf + header_size, body.get_size()))) {
    // Keep the error; the destination-specific send allocation is freed below.
  } else {
    int64_t pos = 0;
    body.record_packet_stat(false /* count_original_on_fallback */);
    if (OB_FAIL(init_packet(proxy, pkt, pcode, opts, unneed_resp))) {
      // Keep the error; do not retry the original encoding path after preparation.
    } else if (OB_FAIL(pkt.encode_header(buf, header_size, pos))) {
      // Keep the error; the destination-specific send allocation is freed below.
    } else {
      req = buf;
      req_sz = header_size + body.get_size();
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    pn_send_free(buf);
    buf = nullptr;
  }
  return ret;
}

int64_t calc_extra_payload_size()
{
  int64_t payload = 0;
  if (!g_runtime_enabled) {
    payload += ObIRpcExtraPayload::instance().get_serialize_size();
  } else {
    ObRuntimeContext& ctx = get_ob_runtime_context();
    payload += ctx.get_serialize_size();
  }
  if (OBTRACE->is_inited()) {
    payload += OBTRACE->get_serialize_size();
  }
  return payload;
}

int fill_extra_payload(ObRpcPacket& pkt, char* buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!g_runtime_enabled) {
    if (OB_FAIL(common::serialization::encode(
                    buf, len, pos, ObIRpcExtraPayload::instance()))) {
      LOG_WARN("serialize debug sync actions fail", K(ret), K(pos), K(len));
    }
  } else {
    ObRuntimeContext& ctx = get_ob_runtime_context();
    if (OB_FAIL(common::serialization::encode(buf, len, pos, ctx))) {
      LOG_WARN("serialize context fail", K(ret), K(pos), K(len));
    } else {
      pkt.set_has_context();
      pkt.set_disable_debugsync();
    }
  }
  if (OBTRACE->is_inited() && OB_SUCC(ret)) {
    if (OB_FAIL(common::serialization::encode(buf, len, pos, *OBTRACE))) {
      LOG_WARN("serialize failed", K(ret), K(buf), K(pos));
    } else {
      pkt.set_has_trace_info();
    }
  }
  return ret;
}

int init_packet(ObRpcProxy& proxy, ObRpcPacket& pkt, ObRpcPacketCode pcode, const ObRpcOpts &opts,
                const bool unneed_response)
{
  int ret = proxy.init_pkt(&pkt, pcode, opts, unneed_response);
  if (common::OB_INVALID_CLUSTER_ID == pkt.get_dst_cluster_id()) {
    pkt.set_dst_cluster_id(ObRpcNetHandler::CLUSTER_ID);
  }
  return ret;
}

int rpc_decode_ob_packet(const char* buf, int64_t sz, ObRpcPacket& pkt)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(pkt.decode(buf, sz))) {
    const int64_t fly_ts = ObTimeUtility::current_time() - pkt.get_timestamp();
    if (pkt.get_timestamp() > 0 && fly_ts > oceanbase::common::OB_MAX_PACKET_FLY_TS && TC_REACH_TIME_INTERVAL(100 * 1000)) {
      RPC_LOG_RET(WARN, common::OB_ERR_TOO_MUCH_TIME, "PNIO packet wait too much time between response and client_cb", "pcode", pkt.get_pcode(),
              "fly_ts", fly_ts, "send_timestamp", pkt.get_timestamp(), K(sz));
    }
  }
  return ret;
}

int rpc_encode_ob_packet(ObRpcMemPool& pool, ObRpcPacket* pkt, char*& buf, int64_t& sz, int64_t reserve_buf_size)
{
  int ret = common::OB_SUCCESS;
  int64_t pos = 0;
  int64_t encode_size = pkt->get_encoded_size();
  if (NULL == buf || encode_size > reserve_buf_size) {
    buf = (char*)pool.alloc(encode_size);
  }
  if (NULL == buf) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc encode buffer fail", K(encode_size));
  } else if (OB_FAIL(pkt->encode_header(buf, encode_size, pos))) {
    LOG_WARN("encode header fail", K(ret), KP(buf), K(encode_size));
  } else {
    memcpy(buf + pos, pkt->get_cdata(), pkt->get_clen());
    sz = encode_size;
  }
  return ret;
}

}; // end namespace obrpc
}; // end namespace oceanbase
