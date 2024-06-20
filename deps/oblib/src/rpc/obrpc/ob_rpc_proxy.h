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
#include "rpc/obrpc/ob_rpc_opts.h"

namespace oceanbase
{

namespace obrpc
{
class Handle;
class ObRpcProxy
{
public:
  class PCodeGuard;
public:
  static const int64_t MAX_RPC_TIMEOUT = 9000 * 1000;
  static const int8_t BACKGROUND_FLOW = 1;
  static common::ObAddr myaddr_;
  struct NoneT
  {
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
  template <ObRpcPacketCode pcode, typename IGNORE=void> struct
  ObRpc {};

  // asynchronous callback
  template <class pcodeStruct>
  class AsyncCB
      : public rpc::frame::ObReqTransport::AsyncCB
  {
  protected:
    using Request = typename pcodeStruct::Request;
    using Response = typename pcodeStruct::Response;

  public:
    AsyncCB(): rpc::frame::ObReqTransport::AsyncCB(pcodeStruct::PCODE) { cloned_ = false; }
    virtual ~AsyncCB() { reset_rcode(); }
    int decode(void *pkt);
    int get_rcode();
    void reset_rcode()
    {
      rcode_.reset();
    }
    void set_cloned(bool cloned)
    {
      cloned_ = cloned;
    }
    bool get_cloned()
    {
      return cloned_;
    }

    virtual void set_args(const Request &arg) = 0;
    virtual void destroy() {}

  protected:

  protected:
    /*
     * When the variable 'clone_' is true, it indicates that the derived class of AsyncCB realloctes
     * new memory and clone itself in its overwriten 'clone' virtual function. But in some cases, the
     * derived class reuses its original memory which is maintained by up-layer modules, and the value
     * of 'clone_' is false. Further, rcode_.warnings_ may reallocate and enlarge it internal memory
     * space when rpc packets deserealized. When clone_is false, the relocated memory in rcode_.warnings_
     * has to be freed in the destructor of class AsyncCB.
     */
    bool cloned_;
    Response result_;
    ObRpcResultCode rcode_;
  };

public:
  ObRpcProxy()
      : transport_(NULL), dst_(), transport_impl_(1), timeout_(MAX_RPC_TIMEOUT),
        tenant_id_(common::OB_SYS_TENANT_ID), group_id_(0),
        priv_tenant_id_(common::OB_INVALID_TENANT_ID),
        max_process_handler_time_(0), compressor_type_(common::INVALID_COMPRESSOR),
        src_cluster_id_(common::OB_INVALID_CLUSTER_ID),
        dst_cluster_id_(common::OB_INVALID_CLUSTER_ID), init_(false),
        active_(true), is_trace_time_(false), do_ratelimit_(false), is_bg_flow_(0), rcode_() {}
  virtual ~ObRpcProxy() = default;

  int init(const rpc::frame::ObReqTransport *transport,
           const common::ObAddr &dst = common::ObAddr());
  int init(const rpc::frame::ObReqTransport *transport,
           const int64_t src_cluster_id,
           const common::ObAddr &dst = common::ObAddr());
  void destroy()                                { init_ = false; }
  bool is_inited() const                        { return init_; }
  void set_timeout(int64_t timeout)             { timeout_ = timeout; }
  int64_t get_timeout() const                   { return timeout_; }
  void set_trace_time(const bool is_trace_time) { is_trace_time_ = is_trace_time; }
  void set_ratelimit(const bool do_ratelimit)   { do_ratelimit_ = do_ratelimit; }
  void set_bg_flow(const int8_t is_bg_flow)    { is_bg_flow_ = is_bg_flow;}
  void set_max_process_handler_time(const uint32_t max_process_handler_time)
  { max_process_handler_time_ = max_process_handler_time; }
  uint64_t get_tenant() const { return tenant_id_; }
  void set_tenant(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  int32_t get_group_id() const { return group_id_; }
  common::ObCompressorType  get_compressor_type() const { return compressor_type_; }
  void set_group_id(int32_t group_id) { group_id_ = group_id; }
  void set_priv_tenant(uint64_t tenant_id) { priv_tenant_id_ = tenant_id; }
  void set_server(const common::ObAddr &dst) { dst_ = dst; }
  const common::ObAddr &get_server() const { return dst_; }
  void set_compressor_type(const common::ObCompressorType &compressor_type) { compressor_type_ = compressor_type; }
  void set_dst_cluster(int64_t dst_cluster_id) { dst_cluster_id_ = dst_cluster_id; }
  void set_transport_impl(int transport_impl) { transport_impl_ = transport_impl; }
  void set_result_code(const ObRpcResultCode &retcode) {
    rcode_.rcode_ = retcode.rcode_;
    snprintf(rcode_.msg_, common::OB_MAX_ERROR_MSG_LEN, "%s", retcode.msg_);
    rcode_.warnings_.reset();
    rcode_.warnings_ = retcode.warnings_;
  }
  void set_handle_attr(Handle* handle, const ObRpcPacketCode& pcode, const ObRpcOpts& opts, bool is_stream_next, int64_t session_id, int64_t pkt_id, int64_t send_ts);

  bool need_increment_request_level(int pcode) const {
    return ((pcode > OB_SQL_PCODE_START && pcode < OB_SQL_PCODE_END)
            || pcode == OB_OUT_TRANS_LOCK_TABLE || pcode == OB_OUT_TRANS_UNLOCK_TABLE
            || pcode == OB_TABLE_LOCK_TASK
            || pcode == OB_HIGH_PRIORITY_TABLE_LOCK_TASK || pcode == OB_BATCH_TABLE_LOCK_TASK
            || pcode == OB_HIGH_PRIORITY_BATCH_TABLE_LOCK_TASK
            || pcode == OB_REGISTER_TX_DATA
            || pcode == OB_REFRESH_SYNC_VALUE || pcode == OB_CLEAR_AUTOINC_CACHE
            || pcode == OB_CLEAN_SEQUENCE_CACHE || pcode == OB_FETCH_TABLET_AUTOINC_SEQ_CACHE
            || pcode == OB_BATCH_GET_TABLET_AUTOINC_SEQ || pcode == OB_BATCH_SET_TABLET_AUTOINC_SEQ
            || pcode == OB_CALC_COLUMN_CHECKSUM_REQUEST || pcode == OB_REMOTE_WRITE_DDL_REDO_LOG
            || pcode == OB_REMOTE_WRITE_DDL_COMMIT_LOG || pcode == OB_REMOTE_WRITE_DDL_INC_COMMIT_LOG);
  }

  // when active is set as false, all RPC calls will simply return OB_INACTIVE_RPC_PROXY.
  void active(const bool active) { active_ = active; }

  int64_t timeout() const { return timeout_; }

  const ObRpcResultCode &get_result_code() const;

  int init_pkt(ObRpcPacket *pkt,
               ObRpcPacketCode pcode,
               const ObRpcOpts &opts,
               const bool unneed_response) const;

  // Calculate whole payload size, including RPC_EXTRA_SIZE and user
  // payload.
  int64_t calc_payload_size(int64_t user_payload);

  // Fill in extra payload for obrpc.
  int fill_extra_payload(
      rpc::frame::ObReqTransport::Request &req, int64_t len, int64_t &pos);


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

  static int create_request(
      const obrpc::ObRpcPacketCode pcode,
      const rpc::frame::ObReqTransport &transport,
      rpc::frame::ObReqTransport::Request &req, const common::ObAddr &addr,
      int64_t size, int64_t timeout,
      const common::ObAddr &local_addr,
      bool do_ratelimit,
      uint32_t is_bg_flow,
      const common::ObString &ssl_invited_nodes,
      const rpc::frame::ObReqTransport::AsyncCB *cb = NULL);
protected:
  // we can definitely judge input or output argument by their
  // constant specifier since they're only called by our wrapper
  // function where input argument is always const-qualified whereas
  // output result is not.
  template <typename Input, typename Out>
  int rpc_call(ObRpcPacketCode pcode,
               const Input &args,
               Out &result,
               Handle *handle,
               const ObRpcOpts &opts);

  template <class pcodeStruct>
  int rpc_post(const typename pcodeStruct::Request &args,
               AsyncCB<pcodeStruct> *cb,
               const ObRpcOpts &opts);
  int rpc_post(ObRpcPacketCode pcode,
               rpc::frame::ObReqTransport::AsyncCB *cb,
               const ObRpcOpts &opts);

private:
  int send_request(
      const rpc::frame::ObReqTransport::Request &req,
      rpc::frame::ObReqTransport::Result &result) const;

  int log_user_error_and_warn(const ObRpcResultCode &rcode) const;
protected:
  const rpc::frame::ObReqTransport *transport_;
  common::ObAddr dst_;
  int transport_impl_;
  int64_t timeout_;
  uint64_t tenant_id_;
  int32_t group_id_;
  uint64_t priv_tenant_id_;
  uint32_t max_process_handler_time_;
  common::ObCompressorType compressor_type_;
  int64_t src_cluster_id_;
  int64_t dst_cluster_id_;
  bool init_;
  bool active_;
  bool is_trace_time_;
  bool do_ratelimit_;
  int8_t is_bg_flow_;
  ObRpcResultCode rcode_;
};

class ObRpcProxy::PCodeGuard
{
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
  const common::ObAddr &get_dst_addr() const { return dst_; }

protected:
  bool has_more_;
  common::ObAddr dst_;
  int64_t sessid_;
  ObRpcOpts opts_;
  const rpc::frame::ObReqTransport *transport_;
  ObRpcProxy proxy_;
  ObRpcPacketCode pcode_;
  bool do_ratelimit_;
  int8_t is_bg_flow_;
  int64_t first_pkt_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(Handle);
};

// stream rpc handle
template <class pcodeStruct>
class SSHandle
    : public Handle {
  friend class ObRpcProxy;

public:
  bool has_more() const;
  int get_more(typename pcodeStruct::Response &result);
  int abort();
  const ObRpcResultCode &get_result_code() const;

protected:
  ObRpcResultCode rcode_;
};

extern ObRpcProxy::NoneT None;
// asynchronous store result rpc handle
// template <ObRpcPacketCode pcode>
// class AsyncSHandle
//     : public SSHandle<pcode>
// {
// public:
// };

} // end of namespace common
} // end of namespace oceanbase

#include "rpc/obrpc/ob_poc_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy.ipp"

#define DEFINE_TO(CLS, ...)                                             \
  static constexpr auto OB_SUCCESS = ::oceanbase::common::OB_SUCCESS;   \
  template <ObRpcPacketCode pcode, typename IGNORE=void>                \
  struct ObRpc {};                                                      \
  template <ObRpcPacketCode pcode>                                      \
  using Processor =                                                     \
      ::oceanbase::obrpc::ObRpcProcessor<CLS::ObRpc<pcode>>;            \
  template <ObRpcPacketCode pcode>                                      \
  using SSHandle =                                                      \
      ::oceanbase::obrpc::SSHandle<CLS::ObRpc<pcode>>;                  \
  template <ObRpcPacketCode pcode>                                      \
  using AsyncCB =                                                       \
      ::oceanbase::obrpc::ObRpcProxy::AsyncCB<CLS::ObRpc<pcode>>;       \
  inline CLS to(                                                        \
      const ::oceanbase::common::ObAddr &dst                            \
      = ::oceanbase::common::ObAddr())  const                           \
  {                                                                     \
    CLS proxy(*this);                                                   \
    proxy.set_server(dst);                                              \
    return proxy;                                                       \
  }                                                                     \
  inline CLS& transport(int transport_id)                               \
  {                                                                     \
    set_transport_impl(transport_id);                                   \
    return *this;                                                       \
  }                                                                     \
  inline CLS& timeout(int64_t timeout)                                  \
  {                                                                     \
    set_timeout(timeout);                                               \
    return *this;                                                       \
  }                                                                     \
  inline CLS& by(uint64_t tenant_id)                                    \
  {                                                                     \
    set_tenant(tenant_id);                                              \
    return *this;                                                       \
  }                                                                     \
  inline CLS& group_id(uint64_t group_id)                                    \
  {                                                                     \
    set_group_id(static_cast<int32_t>(group_id));                       \
    return *this;                                                       \
  }                                                                     \
  inline CLS& as(uint64_t tenant_id)                                    \
  {                                                                     \
    set_priv_tenant(tenant_id);                                         \
    return *this;                                                       \
  }                                                                     \
  inline CLS& trace_time(const bool is_trace_time)                      \
  {                                                                     \
    set_trace_time(is_trace_time);                                      \
    return *this;                                                       \
  }                                                                     \
  inline CLS& ratelimit(const bool do_ratelimit)                        \
  {                                                                     \
    set_ratelimit(do_ratelimit);                                        \
    return *this;                                                       \
  }                                                                     \
  inline CLS& bg_flow(const uint32_t is_bg_flow)                        \
  {                                                                     \
    set_bg_flow(is_bg_flow);                                            \
    return *this;                                                       \
  }                                                                     \
  inline CLS& max_process_handler_time(const uint32_t timestamp)        \
  {                                                                     \
    set_max_process_handler_time(timestamp);                            \
    return *this;                                                       \
  }                                                                     \
  inline CLS& compressed(const oceanbase::common::ObCompressorType &type) \
  {                                                                     \
    set_compressor_type(type);                                          \
    return *this;                                                       \
  }                                                                     \
  inline CLS& dst_cluster_id(int64_t dst_cluster_id)                    \
  {                                                                     \
    set_dst_cluster(dst_cluster_id);                                    \
    return *this;                                                       \
  }                                                                     \
  explicit CLS(CLS *mock_proxy=NULL)                                    \
      : mock_proxy_(mock_proxy)                                         \
  { __VA_ARGS__; }                                                      \
  CLS *mock_proxy_

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_
