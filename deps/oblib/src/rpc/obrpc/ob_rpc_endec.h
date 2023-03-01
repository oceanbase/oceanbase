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

namespace oceanbase
{
namespace obrpc
{
class ObRpcProxy;
int64_t calc_extra_payload_size();
int fill_extra_payload(ObRpcPacket& pkt, char* buf, int64_t len, int64_t pos);
int init_packet(ObRpcProxy& proxy, ObRpcPacket& pkt, ObRpcPacketCode pcode, const ObRpcOpts &opts,
                const bool unneed_response);

template <typename T>
    int rpc_encode_req(
      ObRpcProxy& proxy,
      ObRpcMemPool& pool,
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
  ObRpcPacket pkt;
  const int64_t header_sz = pkt.get_header_size();
  const int64_t payload_sz = calc_extra_payload_size() + common::serialization::encoded_length(args);
#ifdef PERF_MODE
  const int64_t reserve_bytes_for_pnio = 200;
#else
  const int64_t reserve_bytes_for_pnio = 0;
#endif
  char* header_buf = (char*)pool.alloc(reserve_bytes_for_pnio + header_sz + payload_sz) + reserve_bytes_for_pnio;
  char* payload_buf = header_buf + header_sz;
  int64_t pos = 0;
  UNIS_VERSION_GUARD(opts.unis_version_);
  if (NULL == header_buf) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WARN, "alloc buffer fail", K(payload_sz));
  } else if (payload_sz > common::OB_MAX_RPC_PACKET_LENGTH) {
    ret = common::OB_RPC_PACKET_TOO_LONG;
    RPC_OBRPC_LOG(WARN, "obrpc packet payload execced its limit",
                  K(payload_sz), "limit", common::OB_MAX_RPC_PACKET_LENGTH, K(ret));
  } else if (OB_FAIL(common::serialization::encode(
                         payload_buf, payload_sz, pos, args))) {
    RPC_OBRPC_LOG(WARN, "serialize argument fail", K(pos), K(payload_sz), K(ret));
  } else if (OB_FAIL(fill_extra_payload(pkt, payload_buf, payload_sz, pos))) {
    RPC_OBRPC_LOG(WARN, "fill extra payload fail", K(ret), K(pos), K(payload_sz));
  } else {
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
    }
    if (OB_FAIL(pkt.encode_header(header_buf, header_sz, header_pos))) {
      RPC_OBRPC_LOG(WARN, "encode header fail", K(ret));
    } else {
      req = header_buf;
      req_sz = header_sz + payload_sz;
    }
  }
  return ret;
}

template <typename T>
int rpc_decode_resp(const char* resp_buf, int64_t resp_sz, T& result, ObRpcPacket &pkt, ObRpcResultCode &rcode)
{
  int ret = common::OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(pkt.decode(resp_buf, resp_sz))) {
    RPC_OBRPC_LOG(WARN, "decode packet fail", K(ret));
  } else {
    UNIS_VERSION_GUARD(pkt.get_unis_version());
    const char* payload = pkt.get_cdata();
    int64_t limit = pkt.get_clen();
    if (OB_FAIL(rcode.deserialize(payload, limit, pos))) {
      rcode.rcode_ = common::OB_DESERIALIZE_ERROR;
      RPC_OBRPC_LOG(WARN, "deserialize result code fail", K(ret));
    } else {
      if (rcode.rcode_ != common::OB_SUCCESS) {
        ret = rcode.rcode_;
      } else if (OB_FAIL(common::serialization::decode(payload, limit, pos, result))) {
        RPC_OBRPC_LOG(WARN, "deserialize result fail", K(ret));
      } else {
        ret = rcode.rcode_;
      }
    }
  }
  return ret;
}

int rpc_decode_ob_packet(ObRpcMemPool& pool, const char* buf, int64_t sz, ObRpcPacket*& ret_pkt);
int rpc_encode_ob_packet(ObRpcMemPool& pool, ObRpcPacket* pkt, char*& buf, int64_t& sz);

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_RPC_ENDEC_H_ */
