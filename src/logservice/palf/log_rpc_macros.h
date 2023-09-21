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

#ifndef OCEANBASE_LOGSERVICE_LOG_RPC_MACROS_
#define OCEANBASE_LOGSERVICE_LOG_RPC_MACROS_

#include "share/ob_occam_time_guard.h"

#define __RPC_PROCESS_CODE__(REQTYPE) \
      int ret = OB_SUCCESS;                                                                                       \
      LogRpcPacketImpl<REQTYPE> &rpc_packet = arg_;                                                               \
      const REQTYPE &req = rpc_packet.req_;                                                                       \
      const ObAddr server = rpc_packet.src_;                                                                      \
      int64_t palf_id = rpc_packet.palf_id_;                                                                      \
      if (OB_ISNULL(palf_env_impl_)                                                                               \
          && OB_FAIL(__get_palf_env_impl(rpc_pkt_->get_tenant_id(), palf_env_impl_, filter_ == NULL))) {          \
      } else if (OB_UNLIKELY(NULL != filter_ && true == (*filter_)(server))) {                                    \
        PALF_LOG(INFO, "need filter this packet", K(rpc_packet));                                                 \
      } else {                                                                                                    \
        LogRequestHandler handler(palf_env_impl_);                                                                \
        TIMEGUARD_INIT(PALF, 50_ms, 10_s);                                                                        \
        ret = handler.handle_request(palf_id, server, req);                                                       \
        PALF_LOG(TRACE, "Processor handle_request success", K(ret), K(palf_id), K(req), KP(filter_));             \
        EVENT_INC(ObStatEventIds::PALF_HANDLE_RPC_REQUEST_COUNT);                                                   \
      }                                                                                                           \
      return ret;                                                                                                 \

#define __DEFINE_RPC_PROCESSOR__(CLASS, PROXY, REQTYPE, PCODE, ELECTION_MSG)                                      \
  class CLASS : public obrpc::ObRpcProcessor<PROXY::ObRpc<PCODE>>                                                 \
  {                                                                                                               \
  public:                                                                                                         \
    CLASS() : filter_(NULL), palf_env_impl_(NULL) {}                                                              \
    virtual ~CLASS() {}                                                                                           \
    int process() { return process_impl_(); }                                                                     \
    template <bool FLAG = ELECTION_MSG, typename std::enable_if<FLAG, bool>::type = true>                         \
    int process_impl_()                                                                                           \
    {                                                                                                             \
      TIMEGUARD_INIT(ELECT, 50_ms, 10_s);                                                                         \
      __RPC_PROCESS_CODE__(REQTYPE)                                                                               \
    }                                                                                                             \
    template <bool FLAG = ELECTION_MSG, typename std::enable_if<!FLAG, bool>::type = true>                        \
    int process_impl_()                                                                                           \
    {                                                                                                             \
      __RPC_PROCESS_CODE__(REQTYPE)                                                                               \
    }                                                                                                             \
    void set_filter(void *filter)                                                                                 \
    {                                                                                                             \
      filter_ = reinterpret_cast<ObFunction<bool(const ObAddr &src)> *>(filter);                                  \
    }                                                                                                             \
    void set_palf_env_impl(void *palf_env_impl)                                                                   \
    {                                                                                                             \
      palf_env_impl_ = reinterpret_cast<IPalfEnvImpl *>(palf_env_impl);                                           \
    }                                                                                                             \
  private:                                                                                                        \
    ObFunction<bool(const ObAddr &src)> *filter_;                                                                 \
    IPalfEnvImpl *palf_env_impl_;                                                                                 \
  }

#define DEFINE_RPC_PROCESSOR(CLASS, PROXY, REQTYPE, PCODE) __DEFINE_RPC_PROCESSOR__(CLASS, PROXY, REQTYPE, PCODE, false)
#define DEFINE_ELECTION_RPC_PROCESSOR(CLASS, PROXY, REQTYPE, PCODE) __DEFINE_RPC_PROCESSOR__(CLASS, PROXY, REQTYPE, PCODE, true)

#define DECLARE_RPC_PROXY_POST_FUNCTION(PRIO, REQTYPE, PCODE)               \
  RPC_AP(PRIO post_packet, PCODE, (palf::LogRpcPacketImpl<palf::REQTYPE>)); \
  int post_packet(const common::ObAddr &dst, const palf::LogRpcPacketImpl<palf::REQTYPE> &pkt, const int64_t tenant_id,  \
                  const palf::PalfTransportCompressOptions &options)

#define DEFINE_RPC_PROXY_POST_FUNCTION(REQTYPE, PCODE)                                              \
  int LogRpcProxyV2::post_packet(const common::ObAddr &dst, const palf::LogRpcPacketImpl<palf::REQTYPE> &pkt, \
                                 const int64_t tenant_id, const palf::PalfTransportCompressOptions &options)      \
  {                                                                                                           \
    int ret = common::OB_SUCCESS;                                                                             \
    static obrpc::LogRpcCB<obrpc::PCODE> cb;                                                                  \
    if (options.enable_transport_compress_) {                                                                 \
      ret = this->to(dst)                                                                                     \
                .timeout(3000 * 1000)                                                                         \
                .trace_time(true)                                                                             \
                .max_process_handler_time(100 * 1000)                                                         \
                .by(tenant_id)                                                                                \
                .group_id(share::OBCG_CLOG)                                                                   \
                .compressed(options.transport_compress_func_)                                                 \
                .dst_cluster_id(src_cluster_id_)                                                              \
                .post_packet(pkt, &cb);                                                                       \
    } else {                                                                                                  \
      ret = this->to(dst)                                                                                     \
                .timeout(3000 * 1000)                                                                         \
                .trace_time(true)                                                                             \
                .max_process_handler_time(100 * 1000)                                                         \
                .by(tenant_id)                                                                                \
                .group_id(share::OBCG_CLOG)                                                                   \
                .dst_cluster_id(src_cluster_id_)                                                              \
                .post_packet(pkt, &cb);                                                                       \
    }                                                                                                         \
    return ret;                                                                                               \
  }
// ELECTION use unique message queue
// no need transport compress
#define DEFINE_RPC_PROXY_ELECTION_POST_FUNCTION(REQTYPE, PCODE)                                               \
  int LogRpcProxyV2::post_packet(const common::ObAddr &dst, const palf::LogRpcPacketImpl<palf::REQTYPE> &pkt, \
                                 const int64_t tenant_id, const palf::PalfTransportCompressOptions &options)      \
  {                                                                                                           \
    TIMEGUARD_INIT(ELECT, 50_ms, 10_s);                                                                       \
    int ret = common::OB_SUCCESS;                                                                             \
    static obrpc::LogRpcCB<obrpc::PCODE> cb;                                                                  \
    ret = this->to(dst)                                                                                       \
              .timeout(3000 * 1000)                                                                           \
              .trace_time(true)                                                                               \
              .max_process_handler_time(100 * 1000)                                                           \
              .by(tenant_id)                                                                                  \
              .group_id(share::OBCG_ELECTION)                                                                 \
              .dst_cluster_id(src_cluster_id_)                                                                \
              .post_packet(pkt, &cb);                                                                         \
    return ret;                                                                                               \
  }

#define DECLARE_SYNC_RPC_PROXY_POST_FUNCTION(PRIO, NAME, REQTYPE, RESPTYPE, PCODE)                                    \
  RPC_S(PRIO NAME, PCODE, (palf::LogRpcPacketImpl<palf::REQTYPE>), palf::LogRpcPacketImpl<palf::RESPTYPE>);           \
  int post_sync_packet(const common::ObAddr &dst, const int64_t tenant_id,                                            \
                       const palf::PalfTransportCompressOptions &options, const int64_t timeout_us,                       \
                       const palf::LogRpcPacketImpl<palf::REQTYPE> &pkt, palf::LogRpcPacketImpl<palf::RESPTYPE> &resp)

#define DEFINE_SYNC_RPC_PROXY_POST_FUNCTION(NAME, REQTYPE, RESPTYPE)                                                \
  int LogRpcProxyV2::post_sync_packet(const common::ObAddr &dst, const int64_t tenant_id,                           \
                                      const palf::PalfTransportCompressOptions &options,                                \
                                      const int64_t timeout_us,                                                     \
                                      const palf::LogRpcPacketImpl<palf::REQTYPE> &pkt,                             \
                                      palf::LogRpcPacketImpl<palf::RESPTYPE> &resp)                                 \
  {                                                                                                                 \
    int ret = common::OB_SUCCESS;                                                                                   \
    if (options.enable_transport_compress_) {                                                                       \
      ret = this->to(dst)                                                                                           \
                .timeout(timeout_us)                                                                                \
                .trace_time(true)                                                                                   \
                .max_process_handler_time(100 * 1000)                                                               \
                .by(tenant_id)                                                                                      \
                .group_id(share::OBCG_CLOG)                                                                         \
                .compressed(options.transport_compress_func_)                                                       \
                .dst_cluster_id(src_cluster_id_)                                                                    \
                .NAME(pkt, resp);                                                                                   \
    } else {                                                                                                        \
      ret = this->to(dst)                                                                                           \
                .timeout(timeout_us)                                                                                \
                .trace_time(true)                                                                                   \
                .max_process_handler_time(100 * 1000)                                                               \
                .by(tenant_id)                                                                                      \
                .group_id(share::OBCG_CLOG)                                                                         \
                .dst_cluster_id(src_cluster_id_)                                                                    \
                .NAME(pkt, resp);                                                                                   \
    }                                                                                                               \
    return ret;                                                                                                     \
  }

#define DEFINE_SYNC_RPC_PROCESSOR(CLASS, PROXY, REQTYPE, RESPTYPE, PCODE)                                         \
  class CLASS : public obrpc::ObRpcProcessor<PROXY::ObRpc<PCODE>>                                                 \
  {                                                                                                               \
  public:                                                                                                         \
    CLASS() : filter_(NULL), palf_env_impl_(NULL) {}                                                              \
    virtual ~CLASS() { filter_ = NULL, palf_env_impl_ = NULL; }                                                   \
    int process()                                                                                                 \
    {                                                                                                             \
      TIMEGUARD_INIT(PALF, 100_ms, 10_s);                                                                         \
      int ret = OB_SUCCESS;                                                                                       \
      LogRpcPacketImpl<REQTYPE> &rpc_packet = arg_;                                                               \
      const REQTYPE &req = rpc_packet.req_;                                                                       \
      const ObAddr server = rpc_packet.src_;                                                                      \
      int64_t palf_id = rpc_packet.palf_id_;                                                                      \
      RESPTYPE &resp = result_.req_;                                                                              \
      result_.palf_id_ = palf_id;                                                                                 \
      if (OB_ISNULL(palf_env_impl_)                                                                               \
          && CLICK_FAIL(__get_palf_env_impl(rpc_pkt_->get_tenant_id(), palf_env_impl_, filter_ == NULL))) {       \
        PALF_LOG(WARN, "__get_palf_env_impl failed", K(ret), KPC(rpc_pkt_));                                      \
      } else if (CLICK() && NULL != filter_ && true == (*filter_)(server)) {                                      \
        PALF_LOG(INFO, "need filter this packet", K(rpc_packet));                                                 \
      } else {                                                                                                    \
        CLICK();                                                                                                  \
        LogRequestHandler handler(palf_env_impl_);                                                                \
        ret = handler.handle_sync_request(palf_id, server, req, resp);                                            \
      }                                                                                                           \
                                                                                                                  \
      return ret;                                                                                                 \
    }                                                                                                             \
    void set_filter(void *filter)                                                                                 \
    {                                                                                                             \
      filter_ = reinterpret_cast<ObFunction<bool(const ObAddr &src)> *>(filter);                                  \
    }                                                                                                             \
    void set_palf_env_impl(void *palf_env_impl)                                                                   \
    {                                                                                                             \
      palf_env_impl_ = reinterpret_cast<IPalfEnvImpl *>(palf_env_impl);                                           \
    }                                                                                                             \
  private:                                                                                                        \
    ObFunction<bool(const ObAddr &src)> *filter_;                                                                 \
    IPalfEnvImpl *palf_env_impl_;                                                                                 \
  }

#endif // for OCEANBASE_LOGSERVICE_LOG_RPC_MACROS_
