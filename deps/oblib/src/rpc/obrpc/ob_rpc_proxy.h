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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/net/ob_addr.h"
#include "lib/runtime.h"
#include "rpc/frame/ob_req_transport.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
// macros of ObRpcProxy
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "rpc/obrpc/ob_rpc_processor.h"

namespace oceanbase {

namespace obrpc {
class Handle;

struct ObRpcOpts {
  uint64_t tenant_id_;
  ObRpcPriority pr_;             // priority of this RPC packet
  mutable bool is_stream_;       // is this RPC packet a stream packet?
  mutable bool is_stream_last_;  // is this RPC packet the last packet in stream?
  uint64_t unis_version_;
  common::ObAddr local_addr_;
  common::ObString ssl_invited_nodes_;

  ObRpcOpts()
      : tenant_id_(common::OB_INVALID_ID),
        pr_(ORPR_UNDEF),
        is_stream_(false),
        is_stream_last_(false),
        local_addr_(),
        ssl_invited_nodes_()
  {
    unis_version_ = lib::get_unis_global_compat_version();
  }
};

class ObRpcProxy {
public:
  class PCodeGuard;

public:
  static const int64_t MAX_RPC_TIMEOUT = 9000 * 1000;
  static common::ObAddr myaddr_;
  struct NoneT {
    int serialize(SERIAL_PARAMS) const
    {
      UNF_UNUSED_SER;
      return common::OB_SUCCESS;
    }
    int deserialize(DESERIAL_PARAMS)
    {
      UNF_UNUSED_DES;
      return common::OB_SUCCESS;
    }
    int64_t get_serialize_size() const
    {
      return 0;
    }
    TO_STRING_EMPTY();
  };

public:
  template <ObRpcPacketCode pcode, typename IGNORE = void>
  struct ObRpc {};

  // asynchronous callback
  template <class pcodeStruct>
  class AsyncCB : public rpc::frame::ObReqTransport::AsyncCB {
  protected:
    using Request = typename pcodeStruct::Request;
    using Response = typename pcodeStruct::Response;

  public:
    int decode(void* pkt);

    virtual void do_first();
    virtual void set_args(const Request& arg) = 0;
    virtual void destroy()
    {}

  protected:
    void check_request_rt(const bool force_print = false);

  protected:
    Response result_;
    ObRpcResultCode rcode_;
  };

public:
  ObRpcProxy();

  int init(const rpc::frame::ObReqTransport* transport, const common::ObAddr& dst = common::ObAddr());
  void destroy()
  {
    init_ = false;
  }
  bool is_inited() const
  {
    return init_;
  }
  void set_timeout(int64_t timeout)
  {
    timeout_ = timeout;
  }
  void set_trace_time(const bool is_trace_time)
  {
    is_trace_time_ = is_trace_time;
  }
  void set_max_process_handler_time(const uint32_t max_process_handler_time)
  {
    max_process_handler_time_ = max_process_handler_time;
  }
  void set_tenant(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_priv_tenant(uint64_t tenant_id)
  {
    priv_tenant_id_ = tenant_id;
  }
  void set_server(const common::ObAddr& dst)
  {
    dst_ = dst;
  }
  const common::ObAddr& get_server() const
  {
    return dst_;
  }
  void set_compressor_type(const common::ObCompressorType& compressor_type)
  {
    compressor_type_ = compressor_type;
  }
  void set_dst_cluster(int64_t dst_cluster_id)
  {
    dst_cluster_id_ = dst_cluster_id;
  }

  // when active is set as false, all RPC calls will simply return OB_INACTIVE_RPC_PROXY.
  void active(const bool active)
  {
    active_ = active;
  }

  int64_t timeout() const
  {
    return timeout_;
  }

  const ObRpcResultCode& get_result_code() const;

  int init_pkt(ObRpcPacket* pkt, ObRpcPacketCode pcode, const ObRpcOpts& opts, const bool unneed_response) const;

  // Calculate whole payload size, including RPC_EXTRA_SIZE and user
  // payload.
  int64_t calc_payload_size(int64_t user_payload);

  // Fill in extra payload for obrpc.
  int fill_extra_payload(rpc::frame::ObReqTransport::Request<ObRpcPacket>& req, int64_t len, int64_t& pos);

  //// example:
  //// without argument and result
  //
  // RPC_S(@PR5 rpc_name, pcode);
  //
  //// with argument but without result
  //
  // RPC_S(@PR5 rpc_name, pcode, (args));
  //
  //// without argument but with result
  //
  // RPC_S(@PR5 rpc_name, pcode, retult);
  //
  //// with both argument and result
  //
  // RPC_S(@PR5 rpc_name, pcode, (args), retult);
  //
  // Make sure 'args' and 'result' are serializable and deserializable.

  template <typename T>
  static int create_request(const obrpc::ObRpcPacketCode pcode, const rpc::frame::ObReqTransport& transport,
      rpc::frame::ObReqTransport::Request<T>& req, const common::ObAddr& addr, int64_t size, int64_t timeout,
      const common::ObAddr& local_addr, const common::ObString& ssl_invited_nodes,
      const rpc::frame::ObReqTransport::AsyncCB* cb = NULL);

protected:
  // we can definitely judge input or output argument by their
  // constant specifier since they're only called by our wrapper
  // function where input argument is always const-qualified whereas
  // output result is not.
  template <typename Input, typename Out>
  int rpc_call(ObRpcPacketCode pcode, const Input& args, Out& result, Handle* handle, const ObRpcOpts& opts);
  template <typename Input>
  int rpc_call(ObRpcPacketCode pcode, const Input& args, Handle* handle, const ObRpcOpts& opts);
  template <typename Output>
  int rpc_call(ObRpcPacketCode pcode, Output& result, Handle* handle, const ObRpcOpts& opts);
  int rpc_call(ObRpcPacketCode pcode, Handle* handle, const ObRpcOpts& opts);

  template <class pcodeStruct>
  int rpc_post(const typename pcodeStruct::Request& args, AsyncCB<pcodeStruct>* cb, const ObRpcOpts& opts);
  int rpc_post(ObRpcPacketCode pcode, rpc::frame::ObReqTransport::AsyncCB* cb, const ObRpcOpts& opts);

private:
  int send_request(const rpc::frame::ObReqTransport::Request<ObRpcPacket>& req,
      rpc::frame::ObReqTransport::Result<ObRpcPacket>& result) const;

  int log_user_error_and_warn(const ObRpcResultCode& rcode) const;

protected:
  const rpc::frame::ObReqTransport* transport_;
  common::ObAddr dst_;
  int64_t timeout_;
  uint64_t tenant_id_;
  uint64_t priv_tenant_id_;
  uint32_t max_process_handler_time_;
  common::ObCompressorType compressor_type_;
  int64_t dst_cluster_id_;
  bool init_;
  bool active_;
  bool is_trace_time_;
  ObRpcResultCode rcode_;
};

class ObRpcProxy::PCodeGuard {
public:
  PCodeGuard(const obrpc::ObRpcPacketCode pcode)
  {
    last_pcode_ = obrpc::current_pcode();
    obrpc::set_current_pcode(pcode);
  }
  ~PCodeGuard()
  {
    obrpc::set_current_pcode(last_pcode_);
  }

private:
  obrpc::ObRpcPacketCode last_pcode_;
};

// common handle
class Handle {
  friend class ObRpcProxy;

public:
  Handle();
  const common::ObAddr& get_dst_addr() const
  {
    return dst_;
  }

protected:
  bool has_more_;
  common::ObAddr dst_;
  int64_t sessid_;
  ObRpcOpts opts_;
  const rpc::frame::ObReqTransport* transport_;
  ObRpcProxy proxy_;
  ObRpcPacketCode pcode_;

private:
  DISALLOW_COPY_AND_ASSIGN(Handle);
};

// stream rpc handle
template <class pcodeStruct>
class SSHandle : public Handle {
  friend class ObRpcProxy;

public:
  bool has_more() const;
  int get_more(typename pcodeStruct::Response& result);
  int abort();
  const ObRpcResultCode& get_result_code() const;

protected:
  ObRpcResultCode rcode_;
};

// asynchronous store result rpc handle
// template <ObRpcPacketCode pcode>
// class AsyncSHandle
//     : public SSHandle<pcode>
// {
// public:
// };

}  // namespace obrpc
}  // end of namespace oceanbase

#include "rpc/obrpc/ob_rpc_proxy.ipp"

#define DEFINE_TO(CLS, ...)                                                                   \
  static constexpr auto OB_SUCCESS = ::oceanbase::common::OB_SUCCESS;                         \
  template <ObRpcPacketCode pcode, typename IGNORE = void>                                    \
  struct ObRpc {};                                                                            \
  template <ObRpcPacketCode pcode>                                                            \
  using Processor = ::oceanbase::obrpc::ObRpcProcessor<CLS::ObRpc<pcode>>;                    \
  template <ObRpcPacketCode pcode>                                                            \
  using SSHandle = ::oceanbase::obrpc::SSHandle<CLS::ObRpc<pcode>>;                           \
  template <ObRpcPacketCode pcode>                                                            \
  using AsyncCB = ::oceanbase::obrpc::ObRpcProxy::AsyncCB<CLS::ObRpc<pcode>>;                 \
  inline CLS to(const ::oceanbase::common::ObAddr& dst = ::oceanbase::common::ObAddr()) const \
  {                                                                                           \
    CLS proxy(*this);                                                                         \
    proxy.set_server(dst);                                                                    \
    return proxy;                                                                             \
  }                                                                                           \
  inline CLS& timeout(int64_t timeout)                                                        \
  {                                                                                           \
    set_timeout(timeout);                                                                     \
    return *this;                                                                             \
  }                                                                                           \
  inline CLS& by(uint64_t tenant_id)                                                          \
  {                                                                                           \
    set_tenant(tenant_id);                                                                    \
    return *this;                                                                             \
  }                                                                                           \
  inline CLS& as(uint64_t tenant_id)                                                          \
  {                                                                                           \
    set_priv_tenant(tenant_id);                                                               \
    return *this;                                                                             \
  }                                                                                           \
  inline CLS& trace_time(const bool is_trace_time)                                            \
  {                                                                                           \
    set_trace_time(is_trace_time);                                                            \
    return *this;                                                                             \
  }                                                                                           \
  inline CLS& max_process_handler_time(const uint32_t timestamp)                              \
  {                                                                                           \
    set_max_process_handler_time(timestamp);                                                  \
    return *this;                                                                             \
  }                                                                                           \
  inline CLS& compressed(const oceanbase::common::ObCompressorType& type)                     \
  {                                                                                           \
    set_compressor_type(type);                                                                \
    return *this;                                                                             \
  }                                                                                           \
  inline CLS& dst_cluster_id(int64_t dst_cluster_id)                                          \
  {                                                                                           \
    set_dst_cluster(dst_cluster_id);                                                          \
    return *this;                                                                             \
  }                                                                                           \
  explicit CLS(CLS* mock_proxy = NULL) : mock_proxy_(mock_proxy)                              \
  {                                                                                           \
    __VA_ARGS__;                                                                              \
  }                                                                                           \
  CLS* mock_proxy_

#endif  // OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_
