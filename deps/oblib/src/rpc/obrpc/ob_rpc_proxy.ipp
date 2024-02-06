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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_IPP_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_IPP_

#include "lib/compress/ob_compressor.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/serialization.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/trace/ob_trace.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "rpc/obrpc/ob_irpc_extra_payload.h"

namespace oceanbase
{
namespace obrpc
{

template <class pcodeStruct>
bool SSHandle<pcodeStruct>::has_more() const
{
  return has_more_;
}

template <class pcodeStruct>
int SSHandle<pcodeStruct>::get_more(typename pcodeStruct::Response &result)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  static const int64_t PAYLOAD_SIZE = 0;
  UNIS_VERSION_GUARD(opts_.unis_version_);

  int ret = OB_SUCCESS;

  ObReqTransport::Request  req;
  ObReqTransport::Result   r;

  if (OB_ISNULL(transport_)) {
    RPC_OBRPC_LOG(INFO, "transport_ is NULL, use poc_rpc", K(has_more_));
    const int64_t start_ts = common::ObTimeUtility::current_time();
    int64_t src_tenant_id = ob_get_tenant_id();
    auto &set = obrpc::ObRpcPacketSet::instance();
    const char* pcode_label = set.name_of_idx(set.idx_of_pcode(pcode_));
    ObRpcMemPool pool(src_tenant_id, pcode_label);
    ObSyncRespCallback cb(pool);
    char* pnio_req = NULL;
    int64_t pnio_req_sz = 0, resp_sz = 0;
    const char* resp = NULL;
    ObRpcPacket resp_pkt;
    sockaddr_in sock_addr;
    uint8_t thread_id = ObPocClientStub::balance_assign_tidx();
    uint64_t pnio_group_id = ObPocRpcServer::DEFAULT_PNIO_GROUP;
    int pn_err = 0;
    // TODO:@fangwu.lcc map proxy.group_id_ to pnio_group_id
    if (OB_LS_FETCH_LOG2 == pcode_) {
      pnio_group_id = ObPocRpcServer::RATELIMIT_PNIO_GROUP;
    }
    if (OB_FAIL(rpc_encode_req(proxy_, pool, pcode_, NULL, opts_, pnio_req, pnio_req_sz, false, true, false, sessid_))) {
      RPC_LOG(WARN, "rpc encode req fail", K(ret));
    } else if(OB_FAIL(ObPocClientStub::check_blacklist(dst_))) {
      RPC_LOG(WARN, "check_blacklist failed", K(ret));
    } else if (0 != (pn_err = pn_send(
        (pnio_group_id<<32) + thread_id,
        ObPocClientStub::obaddr2sockaddr(&sock_addr, dst_),
        pnio_req,
        pnio_req_sz,
        static_cast<int16_t>(set.idx_of_pcode(pcode_)),
        start_ts + proxy_.timeout(),
        ObSyncRespCallback::client_cb,
        &cb))) {
      ret = ObPocClientStub::translate_io_error(pn_err);
      RPC_LOG(WARN, "pnio post fail", K(pn_err));
    } else if (OB_FAIL(cb.wait())) {
      RPC_LOG(WARN, "stream rpc execute fail", K(ret), K(dst_));
    } else if (NULL == (resp = cb.get_resp(resp_sz))) {
      ret = common::OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "stream rpc execute success but resp is null", K(ret), K(dst_));
    } else if (OB_FAIL(rpc_decode_resp(resp, resp_sz, result, resp_pkt, rcode_))) {
      RPC_LOG(WARN, "rpc decode response fail", KP(resp), K(resp_sz), K(ret));
    } else if (rcode_.rcode_ != OB_SUCCESS) {
      ret = rcode_.rcode_;
      RPC_OBRPC_LOG(WARN, "execute rpc fail", K(ret));
    } else {
      has_more_ = resp_pkt.is_stream_next();
    }

  } else if (OB_FAIL(ObRpcProxy::create_request(pcode_, *transport_,
      req, dst_, PAYLOAD_SIZE, proxy_.timeout(), opts_.local_addr_, do_ratelimit_,
      is_bg_flow_, opts_.ssl_invited_nodes_, NULL))) {
    RPC_OBRPC_LOG(WARN, "create request fail", K(ret));
  } else if (NULL == req.pkt() || NULL == req.buf()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WARN, "request packet is NULL", K(ret));
  } else if (OB_FAIL(proxy_.init_pkt(req.pkt(), pcodeStruct::PCODE, opts_, false))) {
    RPC_OBRPC_LOG(WARN, "Init packet error", K(ret));
  } else {
    req.pkt()->set_stream_next();
    req.pkt()->set_session_id(sessid_);

    if (OB_FAIL(transport_->send(req, r))) {
      RPC_OBRPC_LOG(WARN, "send request fail", K(ret));
    } else if (OB_ISNULL(r.pkt()->get_cdata())) {
      ret = OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(WARN, "cdata should not be NULL", K(ret));
    } else {
      UNIS_VERSION_GUARD(r.pkt()->get_unis_version());
      const char   *buf = r.pkt()->get_cdata();
      int64_t len = r.pkt()->get_clen();

      // uncompress if necessary
      const common::ObCompressorType &compressor_type =  r.pkt()->get_compressor_type();
      char *uncompressed_buf = NULL;
      if (common::INVALID_COMPRESSOR != compressor_type) {
        // uncompress
        const int32_t original_len = r.pkt()->get_original_len();
        common::ObCompressor *compressor = NULL;
        int64_t dst_data_size = 0;
        if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type,
                                                                            compressor))) {
          RPC_OBRPC_LOG(WARN, "get_compressor failed", K(ret), K(compressor_type));
        } else if (NULL == (uncompressed_buf =
                            static_cast<char *>(common::ob_malloc(original_len, common::ObModIds::OB_RPC)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          RPC_OBRPC_LOG(WARN, "Allocate memory failed", K(ret));
        } else if (OB_FAIL(compressor->decompress(buf, len, uncompressed_buf, original_len, dst_data_size))) {
          RPC_OBRPC_LOG(WARN, "decompress failed", K(ret));
        } else if (dst_data_size != original_len) {
          ret = common::OB_ERR_UNEXPECTED;
          RPC_OBRPC_LOG(ERROR, "decompress len not match", K(ret), K(dst_data_size), K(original_len));
        } else {
          RPC_OBRPC_LOG(DEBUG, "uncompress result success", K(compressor_type), K(len), K(original_len));
          // replace buf
          buf = uncompressed_buf;
          len = original_len;
        }
      }

      int64_t pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WARN, "deserialize result code fail", K(ret));
      } else if (rcode_.rcode_ != OB_SUCCESS) {
        ret = rcode_.rcode_;
        RPC_OBRPC_LOG(WARN, "execute rpc fail", K(ret));
      } else if (OB_FAIL(common::serialization::decode(buf, len, pos, result))) {
        RPC_OBRPC_LOG(WARN, "deserialize result fail", K(ret));
      } else {
        has_more_ = r.pkt()->is_stream_next();
      }

      // free the uncompress buffer
      if (NULL != uncompressed_buf) {
        common::ob_free(uncompressed_buf);
        uncompressed_buf = NULL;
      }
    }
  }

  return ret;
}

template <class pcodeStruct>
int SSHandle<pcodeStruct>::abort()
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  static const int64_t PAYLOAD_SIZE = 0;

  int ret = OB_SUCCESS;

  ObReqTransport::Request  req;
  ObReqTransport::Result   r;

  if (OB_ISNULL(transport_)) {
    RPC_OBRPC_LOG(INFO, "transport_ is NULL, use poc_rpc", K(has_more_));
    const int64_t start_ts = common::ObTimeUtility::current_time();
    int64_t src_tenant_id = ob_get_tenant_id();
    auto &set = obrpc::ObRpcPacketSet::instance();
    const char* pcode_label = set.name_of_idx(set.idx_of_pcode(pcode_));
    ObRpcMemPool pool(src_tenant_id, pcode_label);
    ObSyncRespCallback cb(pool);
    char* pnio_req = NULL;
    int64_t pnio_req_sz = 0, resp_sz = 0;
    const char* resp = NULL;
    ObRpcPacket resp_pkt;
    sockaddr_in sock_addr;
    uint8_t thread_id = ObPocClientStub::balance_assign_tidx();
    uint64_t pnio_group_id = ObPocRpcServer::DEFAULT_PNIO_GROUP;
    int pn_err = 0;
    // TODO:@fangwu.lcc map proxy.group_id_ to pnio_group_id
    if (OB_LS_FETCH_LOG2 == pcode_) {
      pnio_group_id = ObPocRpcServer::RATELIMIT_PNIO_GROUP;
    }
    if (OB_FAIL(rpc_encode_req(proxy_, pool, pcode_, NULL, opts_, pnio_req, pnio_req_sz, false, false, true, sessid_))) {
      RPC_LOG(WARN, "rpc encode req fail", K(ret));
    } else if(OB_FAIL(ObPocClientStub::check_blacklist(dst_))) {
      RPC_LOG(WARN, "check_blacklist failed", K(ret));
    } else if (0 != (pn_err = pn_send(
        (pnio_group_id<<32) + thread_id,
        ObPocClientStub::obaddr2sockaddr(&sock_addr, dst_),
        pnio_req,
        pnio_req_sz,
        static_cast<int16_t>(set.idx_of_pcode(pcode_)),
        start_ts + proxy_.timeout(),
        ObSyncRespCallback::client_cb,
        &cb))) {
      ret = ObPocClientStub::translate_io_error(pn_err);
      RPC_LOG(WARN, "pnio post fail", K(pn_err));
    } else if (OB_FAIL(cb.wait())) {
      RPC_LOG(WARN, "stream rpc execute fail", K(ret), K(dst_));
    } else if (NULL == (resp = cb.get_resp(resp_sz))) {
      ret = common::OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "stream rpc execute success but resp is null", K(ret), K(dst_));
    } else {
      typename pcodeStruct::Response result;
      if (OB_FAIL(rpc_decode_resp(resp, resp_sz, result, resp_pkt, rcode_))) {
        RPC_LOG(WARN, "rpc decode response fail", KP(resp), K(resp_sz), K(ret));
      } else if (rcode_.rcode_ != OB_SUCCESS) {
        ret = rcode_.rcode_;
        RPC_OBRPC_LOG(WARN, "execute rpc fail", K(ret));
      } else {
        //do nothing
      }
      has_more_ = false;
    }

  } else if (OB_FAIL(ObRpcProxy::create_request(pcode_, *transport_,
      req, dst_, PAYLOAD_SIZE, proxy_.timeout(), opts_.local_addr_, do_ratelimit_,
      is_bg_flow_, opts_.ssl_invited_nodes_, NULL))) {
    RPC_OBRPC_LOG(WARN, "create request fail", K(ret));
  } else if (NULL == req.pkt() || NULL == req.buf()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WARN, "request packet is NULL", K(ret));
  } else if (OB_FAIL(proxy_.init_pkt(req.pkt(), pcodeStruct::PCODE, opts_, false))) {
    RPC_OBRPC_LOG(WARN, "Fail to init packet", K(ret));
  } else {
    req.pkt()->set_stream_last();
    req.pkt()->set_session_id(sessid_);

    if (OB_FAIL(transport_->send(req, r))) {
      RPC_OBRPC_LOG(WARN, "send request fail", K(ret));
    } else if (OB_ISNULL(r.pkt()->get_cdata())) {
      ret = OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(WARN, "cdata should not be NULL", K(ret));
    } else {
      int64_t       pos = 0;
      const char   *buf = r.pkt()->get_cdata();
      const int64_t len = r.pkt()->get_clen();

      if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WARN, "deserialize result code fail", K(ret));
      } else if (rcode_.rcode_ != OB_SUCCESS) {
        ret = rcode_.rcode_;
        RPC_OBRPC_LOG(WARN, "execute rpc fail", K(ret));
      } else {
        //do nothing
      }
      has_more_ = false;
    }
  }

  return ret;
}

template <class pcodeStruct>
const ObRpcResultCode &SSHandle<pcodeStruct>::get_result_code() const
{
  return rcode_;
}

template <class pcodeStruct>
int ObRpcProxy::AsyncCB<pcodeStruct>::decode(void *pkt)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  int ret   = OB_SUCCESS;

  if (OB_ISNULL(pkt)) {
    ret = OB_INVALID_ARGUMENT;
    RPC_OBRPC_LOG(WARN, "pkt should not be NULL", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObRpcPacket  *rpkt = reinterpret_cast<ObRpcPacket*>(pkt);
    const char   *buf  = rpkt->get_cdata();
    int64_t      len   = rpkt->get_clen();
    int64_t      pos   = 0;
    UNIS_VERSION_GUARD(rpkt->get_unis_version());
    char *uncompressed_buf = NULL;

    if (OB_FAIL(rpkt->verify_checksum())) {
      RPC_OBRPC_LOG(ERROR, "verify checksum fail", K(*rpkt), K(ret));
    }
    if (OB_SUCC(ret)) {
      const common::ObCompressorType &compressor_type = rpkt->get_compressor_type();
      if (common::INVALID_COMPRESSOR != compressor_type) {
        // uncompress
        const int32_t original_len = rpkt->get_original_len();
        common::ObCompressor *compressor = NULL;
        int64_t dst_data_size = 0;
        if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type, compressor))) {
          RPC_OBRPC_LOG(WARN, "get_compressor failed", K(ret), K(compressor_type));
        } else if (NULL == (uncompressed_buf =
                                   static_cast<char *>(common::ob_malloc(original_len, common::ObModIds::OB_RPC)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          RPC_OBRPC_LOG(WARN, "Allocate memory failed", K(ret));
        } else if (OB_FAIL(compressor->decompress(buf, len, uncompressed_buf, original_len, dst_data_size))) {
          RPC_OBRPC_LOG(WARN, "decompress failed", K(ret));
        } else if (dst_data_size != original_len) {
          ret = common::OB_ERR_UNEXPECTED;
          RPC_OBRPC_LOG(ERROR, "decompress len not match", K(ret), K(dst_data_size), K(original_len));
        } else {
          RPC_OBRPC_LOG(DEBUG, "uncompress result success", K(compressor_type), K(len), K(original_len));
          // replace buf
          buf = uncompressed_buf;
          len = original_len;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WARN, "decode result code fail", K(*rpkt), K(ret));
      } else if (rcode_.rcode_ != OB_SUCCESS) {
        // RPC_OBRPC_LOG(WARN, "execute rpc fail", K_(rcode));
      } else if (OB_FAIL(result_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WARN, "decode packet fail", K(ret));
      } else {
        // do nothing
      }
    }
    // free the uncompress buffer
    if (NULL != uncompressed_buf) {
      common::ob_free(uncompressed_buf);
      uncompressed_buf = NULL;
    }
  }
  return ret;
}
template <class pcodeStruct>
int ObRpcProxy::AsyncCB<pcodeStruct>::get_rcode()
{
  return rcode_.rcode_;
}

/*
template <class pcodeStruct>
void ObRpcProxy::AsyncCB<pcodeStruct>::check_request_rt(const bool force_print)
{
  if (force_print
      || req_->client_send_time - req_->client_start_time > REQUEST_ITEM_COST_RT
      || req_->client_connect_time - req_->client_send_time > REQUEST_ITEM_COST_RT
      || req_->client_write_time - req_->client_connect_time > REQUEST_ITEM_COST_RT
      || req_->request_arrival_time - req_->client_write_time > REQUEST_ITEM_COST_RT
      || req_->arrival_push_diff > REQUEST_ITEM_COST_RT
      || req_->push_pop_diff > REQUEST_ITEM_COST_RT
      || req_->pop_process_start_diff > REQUEST_ITEM_COST_RT
      || req_->process_start_end_diff > REQUEST_ITEM_COST_RT
      || req_->process_end_response_diff > REQUEST_ITEM_COST_RT
      || (req_->client_read_time - req_->request_arrival_time - req_->arrival_push_diff - req_->push_pop_diff
        - req_->pop_process_start_diff - req_->process_start_end_diff - req_->process_end_response_diff) > REQUEST_ITEM_COST_RT
      || req_->client_end_time - req_->client_read_time > REQUEST_ITEM_COST_RT) {

    if (TC_REACH_TIME_INTERVAL(100 * 1000)) {
      _RPC_OBRPC_LOG(INFO,
          "rpc time, packet_id :%lu, client_start_time :%ld, start_send_diff :%ld, "
          "send_connect_diff: %ld, connect_write_diff: %ld, request_fly_ts: %ld, "
          "arrival_push_diff: %d, push_pop_diff: %d, pop_process_start_diff :%d, "
          "process_start_end_diff: %d, process_end_response_diff: %d, response_fly_ts: %ld, "
          "read_end_diff: %ld, client_end_time :%ld",
          req_->packet_id,
          req_->client_start_time,
          req_->client_send_time - req_->client_start_time,
          req_->client_connect_time - req_->client_send_time,
          req_->client_write_time - req_->client_connect_time,
          req_->request_arrival_time - req_->client_write_time,
          req_->arrival_push_diff,
          req_->push_pop_diff,
          req_->pop_process_start_diff,
          req_->process_start_end_diff,
          req_->process_end_response_diff,
          req_->client_read_time - req_->request_arrival_time - req_->arrival_push_diff - req_->push_pop_diff
          - req_->pop_process_start_diff - req_->process_start_end_diff - req_->process_end_response_diff,
          req_->client_end_time - req_->client_read_time,
          req_->client_end_time);
    }
  }
}
*/

template <typename Input, typename Out>
int ObRpcProxy::rpc_call(ObRpcPacketCode pcode, const Input &args,
                         Out &result, Handle *handle, const ObRpcOpts &opts)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  int ret = OB_SUCCESS;
  UNIS_VERSION_GUARD(opts.unis_version_);

  const int64_t start_ts = ObTimeUtility::current_time();
  rpc::RpcStatPiece piece;

  if (!init_) {
    ret = OB_NOT_INIT;
    RPC_OBRPC_LOG(WARN, "Rpc proxy not inited", K(ret));
  } else if (!active_) {
    ret = OB_INACTIVE_RPC_PROXY;
    RPC_OBRPC_LOG(WARN, "Rpc proxy inactive", K(ret));
  } else {
    //do nothing
  }
  POC_RPC_INTERCEPT(send, dst_, pcode, args, result, handle, opts);

  int64_t pos = 0;
  const int64_t payload = calc_payload_size(common::serialization::encoded_length(args));
  ObReqTransport::Request req;

  if (OB_FAIL(ret)) {
  } else if (payload > get_max_rpc_packet_size()) {
    ret = OB_RPC_PACKET_TOO_LONG;
    RPC_OBRPC_LOG(WARN, "obrpc packet payload execced its limit",
            K(payload), "limit", get_max_rpc_packet_size(),
            K(ret));
  } else if (OB_ISNULL(transport_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(WARN, "transport_ should not be NULL", K(ret));
  } else if (OB_FAIL(ObRpcProxy::create_request(pcode, *transport_,
                     req, dst_, payload, timeout_, opts.local_addr_, do_ratelimit_,
                     is_bg_flow_, opts.ssl_invited_nodes_, NULL))) {
    RPC_OBRPC_LOG(WARN, "create request fail", K(ret));
  } else if (NULL == req.pkt() || NULL == req.buf()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WARN, "request packet is NULL", K(req), K(ret));
  } else if (OB_FAIL(common::serialization::encode(
                     req.buf(), payload, pos, args))) {
    RPC_OBRPC_LOG(WARN, "serialize argument fail", K(pos), K(payload), K(ret));
  } else if (OB_FAIL(fill_extra_payload(req, payload, pos))) {
    RPC_OBRPC_LOG(WARN, "fill extra payload fail", K(ret), K(pos), K(payload));
  } else if (OB_FAIL(init_pkt(req.pkt(), pcode, opts, false))) {
    RPC_OBRPC_LOG(WARN, "Init packet error", K(ret));
  } else {
    ObReqTransport::Result r;
    if (OB_FAIL(send_request(req, r))) {
      RPC_OBRPC_LOG(WARN, "send rpc request fail", K(pcode), K(ret));
    } else {
      const char *buf = r.pkt()->get_cdata();
      int64_t     len = r.pkt()->get_clen();

      const common::ObCompressorType &compressor_type =  r.pkt()->get_compressor_type();
      char *uncompressed_buf = NULL;
      if (common::INVALID_COMPRESSOR != compressor_type) {
        // uncompress
        const int32_t original_len = r.pkt()->get_original_len();
        common::ObCompressor *compressor = NULL;
        int64_t dst_data_size = 0;
        if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type,
                                                                            compressor))) {
          RPC_OBRPC_LOG(WARN, "get_compressor failed", K(ret), K(compressor_type));
        } else if (NULL == (uncompressed_buf =
                            static_cast<char *>(common::ob_malloc(original_len, common::ObModIds::OB_RPC)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          RPC_OBRPC_LOG(WARN, "Allocate memory failed", K(ret));
        } else if (OB_FAIL(compressor->decompress(buf, len, uncompressed_buf, original_len, dst_data_size))) {
          RPC_OBRPC_LOG(WARN, "decompress failed", K(ret));
        } else if (dst_data_size != original_len) {
          ret = common::OB_ERR_UNEXPECTED;
          RPC_OBRPC_LOG(ERROR, "decompress len not match", K(ret), K(dst_data_size), K(original_len));
        } else {
          RPC_OBRPC_LOG(INFO, "uncompress result success", K(compressor_type), K(len), K(original_len));
          // replace buf
          buf = uncompressed_buf;
          len = original_len;
        }
      }

      int64_t pos = 0;
      if (OB_SUCC(ret)) {
        UNIS_VERSION_GUARD(r.pkt()->get_unis_version());
        if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
          RPC_OBRPC_LOG(WARN, "deserialize result code fail", K(ret));
        } else {
          int wb_ret = OB_SUCCESS;
          if (rcode_.rcode_ != OB_SUCCESS) {
            ret = rcode_.rcode_;
            RPC_OBRPC_LOG(WARN, "execute rpc fail", K(ret), K_(dst));
          } else if (OB_FAIL(common::serialization::decode(buf, len, pos, result))) {
            RPC_OBRPC_LOG(WARN, "deserialize result fail", K(ret));
          } else {
            ret = rcode_.rcode_;
          }

          if (common::OB_SUCCESS == ret && NULL != handle) {
            handle->has_more_  = r.pkt()->is_stream_next();
            handle->dst_       = dst_;
            handle->sessid_    = r.pkt()->get_session_id();
            handle->opts_      = opts;
            handle->transport_ = transport_;
            handle->proxy_     = *this;
            handle->pcode_     = pcode;
            handle->do_ratelimit_ = do_ratelimit_;
            handle->is_bg_flow_   = is_bg_flow_;
          }
          if (common::OB_SUCCESS != (wb_ret = log_user_error_and_warn(rcode_))) {
            RPC_OBRPC_LOG(WARN, "fail to log user error and warn", K(ret), K(wb_ret), K((rcode_)));
          }
        }
      }
      // free the uncompress buffer
      if (NULL != uncompressed_buf) {
        common::ob_free(uncompressed_buf);
        uncompressed_buf = NULL;
      }
    }
  }

  piece.size_ = payload;
  piece.time_ = ObTimeUtility::current_time() - start_ts;
  if (OB_FAIL(ret)) {
    piece.failed_ = true;
    if (OB_TIMEOUT == ret) {
      piece.is_timeout_ = true;
    }
  }
  RPC_STAT(pcode, tenant_id_, piece);

  return ret;
}

template <class pcodeStruct>
int ObRpcProxy::rpc_post(const typename pcodeStruct::Request &args,
                         AsyncCB<pcodeStruct> *cb, const ObRpcOpts &opts)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  UNIS_VERSION_GUARD(opts.unis_version_);

  common::ObTimeGuard timeguard("rpc post", 10 * 1000);
  const int64_t start_ts = ObTimeUtility::current_time();

  if (!init_) {
    ret = OB_NOT_INIT;
    RPC_OBRPC_LOG(WARN, "rpc not inited", K(ret));
  } else if (!active_) {
    ret = OB_INACTIVE_RPC_PROXY;
    RPC_OBRPC_LOG(WARN, "rpc is inactive", K(ret));
  }
  POC_RPC_INTERCEPT(post, dst_, pcodeStruct::PCODE, args, cb, opts);

  ObReqTransport::Request req;
  int64_t pos = 0;
  const int64_t original_len = calc_payload_size(common::serialization::encoded_length(args));
  int64_t payload = original_len;
  int64_t max_overflow_size = 0;

  bool need_compressed = ObCompressorPool::get_instance().need_common_compress(compressor_type_);
  char *serialize_buf = NULL;
  common::ObCompressor *compressor = NULL;
  bool use_context = false;
  bool has_trace_info = false;
  if (OB_SUCC(ret) && need_compressed) {
    int64_t tmp_pos = 0;
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_type_,
            compressor))) {
      RPC_OBRPC_LOG(WARN, "get_compressor failed", K(ret), K(compressor_type_));
    } else if (OB_FAIL(compressor->get_max_overflow_size(payload, max_overflow_size))) {
      RPC_OBRPC_LOG(WARN, "get_max_overflow_size failed", K(ret), K(payload), K(max_overflow_size));
    } else if (NULL == (serialize_buf =
          static_cast<char *>(common::ob_malloc(payload, common::ObModIds::OB_RPC)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      RPC_OBRPC_LOG(WARN, "ob_malloc failed", K(ret), K(payload));
    } else if (OB_FAIL(common::serialization::encode(serialize_buf, payload, tmp_pos,
            args))) {
      RPC_OBRPC_LOG(WARN, "args serialize failed", K(ret));
    } else {
      if (!lib::g_runtime_enabled) {
        if (OB_FAIL(common::serialization::encode(
                        serialize_buf, payload, tmp_pos, ObIRpcExtraPayload::instance()))) {
          RPC_OBRPC_LOG(WARN, "serialize debug sync actions fail", K(ret), K(pos), K(payload));
        }
      } else {
        lib::ObRuntimeContext *ctx = &(lib::get_ob_runtime_context());
        if (ctx != nullptr) {
          if (OB_FAIL(common::serialization::encode(
                          serialize_buf, payload, tmp_pos, *ctx))) {
            RPC_OBRPC_LOG(WARN, "serialize context fail", K(ret), K(pos), K(payload));
          } else {
            use_context = true;
          }
        }
      }
      if (OBTRACE->is_inited() && OB_SUCC(ret)) {
        if (OB_FAIL(common::serialization::encode(serialize_buf, payload, tmp_pos, *OBTRACE))) {
          RPC_OBRPC_LOG(WARN, "serialize failed", K(ret), K(tmp_pos), K(payload));
        } else {
          has_trace_info = true;
        }
      }
      // source data length plus max overflow size is the maximum
      // possible size of compressed data.
      if (OB_SUCC(ret)) {
        payload += max_overflow_size;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (payload > get_max_rpc_packet_size()) {
    ret = OB_RPC_PACKET_TOO_LONG;
    RPC_OBRPC_LOG(WARN, "obrpc packet payload execced its limit",
                  K(ret), K(payload), "limit", get_max_rpc_packet_size());
  } else if (OB_ISNULL(transport_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(WARN, "transport_ should not be NULL", K(ret), KP_(transport));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRpcProxy::create_request(pcodeStruct::PCODE, *transport_,
                       req, dst_, payload, timeout_, opts.local_addr_, do_ratelimit_,
                       is_bg_flow_, opts.ssl_invited_nodes_, cb))) {
      RPC_OBRPC_LOG(WARN, "create request fail", K(ret));
    } else if (NULL == req.pkt()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      RPC_OBRPC_LOG(WARN, "request packet is NULL", K(ret));
    } else if (use_context) {
      req.pkt()->set_has_context();
      req.pkt()->set_disable_debugsync();
    } else if (has_trace_info) {
      req.pkt()->set_has_trace_info();
    }
    timeguard.click();
  }

  if (OB_SUCC(ret)) {
    req.s_->is_trace_time = is_trace_time_ ? 1 : 0;
    req.s_->max_process_handler_time = max_process_handler_time_;
    if (need_compressed) {
      EVENT_INC(RPC_COMPRESS_ORIGINAL_PACKET_CNT);
      EVENT_ADD(RPC_COMPRESS_ORIGINAL_SIZE, original_len);
      int64_t dst_data_size = 0;
      if (OB_SUCCESS != (tmp_ret = compressor->compress(serialize_buf, original_len, req.buf(), payload,
              dst_data_size))) {
        RPC_OBRPC_LOG(WARN, "compress failed", K(ret));
        need_compressed = false;
        EVENT_ADD(RPC_COMPRESS_COMPRESSED_SIZE, original_len);
      } else if (dst_data_size >= original_len) {
        need_compressed = false;
        EVENT_ADD(RPC_COMPRESS_COMPRESSED_SIZE, original_len);
      } else {
        req.pkt_->set_content(req.buf(), dst_data_size);
        req.pkt_->set_compressor_type(compressor_type_);
        req.pkt_->set_original_len(static_cast<int32_t>(original_len));

        EVENT_INC(RPC_COMPRESS_COMPRESSED_PACKET_CNT);
        EVENT_ADD(RPC_COMPRESS_COMPRESSED_SIZE, dst_data_size);
      }
    }

    if (!need_compressed) {
      if (OB_FAIL(common::serialization::encode(req.buf(), payload, pos, args))) {
        RPC_OBRPC_LOG(WARN, "serialize argument fail", K(ret));
      } else if (OB_FAIL(fill_extra_payload(req, payload, pos))) {
        RPC_OBRPC_LOG(WARN, "fill extra payload fail", K(ret), K(pos), K(payload));
      } else {
        /*
         * When compress mode is enabled here, payload value is (original_len + max_overflow_size).
         * If the data length after data compressed is not leass than original_len, we do not
         * use compression for this RPC packet, and the packet size should be original_len.
         * So we do set_content with pos, instead of payload.
         */
        req.pkt_->set_content(req.buf(), pos);
      }
    }
    timeguard.click();
  }

  if (OB_SUCC(ret)) {
    bool need_stream_compress = ObCompressorPool::get_instance().need_stream_compress(compressor_type_);
    if (need_stream_compress) {
      req.pkt_->set_compressor_type(compressor_type_);
    }

    auto *newcb = reinterpret_cast<AsyncCB<pcodeStruct>*>(req.cb());
    if (newcb) {
      newcb->set_args(args);
      newcb->set_dst(dst_);
      newcb->set_tenant_id(tenant_id_);
      newcb->set_timeout(timeout_);
      newcb->set_send_ts(start_ts);
      newcb->set_payload(payload);
    }
    req.set_async();
    if (OB_FAIL(init_pkt(req.pkt(), pcodeStruct::PCODE, opts, NULL == cb))) {
      RPC_OBRPC_LOG(WARN, "Init packet error", K(ret));
    }
    timeguard.click();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(transport_->post(req))) {
      ObRpcPacketCode pcode = pcodeStruct::PCODE;
      RPC_OBRPC_LOG(WARN, "post packet fail", K(pcode), K(ret));
      req.destroy();
    } else {
      //do nothing
    }
    timeguard.click();
  }

  static ObRpcPacketCode pcode = pcodeStruct::PCODE;
  if (NULL != serialize_buf) {
    ob_free(serialize_buf);
    serialize_buf = NULL;
  }

  NG_TRACE_EXT(post_packet, OB_Y(ret), OB_Y(pcode), OB_ID(addr), dst_);
  return ret;
}




} // end of namespace rpc
} // end of namespace oceanbase


#endif //OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_IPP_
