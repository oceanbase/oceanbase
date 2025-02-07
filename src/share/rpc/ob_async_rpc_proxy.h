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

#ifndef OCEANBASE_RPC_OB_ASYNC_RPC_PROXY_H_
#define OCEANBASE_RPC_OB_ASYNC_RPC_PROXY_H_

#include "lib/lock/ob_thread_cond.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_errno.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace obrpc
{

template<ObRpcPacketCode PC, typename AsyncRpcProxy, typename RpcProxy>
class ObAsyncCB : public RpcProxy::template AsyncCB<PC>,
    public common::ObDLinkBase<ObAsyncCB<PC, AsyncRpcProxy, RpcProxy> >
{
  using AsyncCB = typename RpcProxy::template AsyncCB<PC>;
public:
  ObAsyncCB(AsyncRpcProxy &proxy) : proxy_(proxy) {}
  virtual ~ObAsyncCB() {}

  void set_args(const typename AsyncCB::Request &args) { UNUSED(args); }
  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const;

  int process();
  void on_timeout();
  void on_invalid();

  int get_ret_code() const { return AsyncCB::rcode_.rcode_; }
  const common::ObAddr &get_dst() const { return AsyncCB::dst_; }
  int64_t get_timeout() const { return AsyncCB::timeout_; }
  const typename AsyncCB::Response &get_result() const
  {
    return AsyncCB::result_;
  }

  TO_STRING_KV("dst", get_dst(), "ret_code", get_ret_code(),
      "result", get_result());
private:
  AsyncRpcProxy &proxy_;
};

template<ObRpcPacketCode PC, typename AsyncRpcProxy, typename RpcProxy>
rpc::frame::ObReqTransport::AsyncCB *ObAsyncCB<PC, AsyncRpcProxy, RpcProxy>::clone(
    const rpc::frame::SPAlloc &alloc) const
{
  UNUSED(alloc);
  return const_cast<rpc::frame::ObReqTransport::AsyncCB *>(
      static_cast<const rpc::frame::ObReqTransport::AsyncCB * const>(this));
}

template<ObRpcPacketCode PC, typename AsyncRpcProxy, typename RpcProxy>
int ObAsyncCB<PC, AsyncRpcProxy, RpcProxy>::process()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(proxy_.receive_response())) {
    RPC_LOG(WARN, "proxy_ receive_response failed", K(ret));
  }
  return ret;
}

template<ObRpcPacketCode PC, typename AsyncRpcProxy, typename RpcProxy>
void ObAsyncCB<PC, AsyncRpcProxy, RpcProxy>::on_timeout()
{
  int ret = common::OB_SUCCESS;
  RPC_LOG(WARN, "some error in rcode and enter on_timeout", K(AsyncCB::rcode_.rcode_));
  AsyncCB::rcode_.rcode_ = common::OB_TIMEOUT;
  if (OB_FAIL(proxy_.receive_response())) {
    RPC_LOG(WARN, "proxy_ receive_response failed", K(ret));
  }
}

template<ObRpcPacketCode PC, typename AsyncRpcProxy, typename RpcProxy>
void ObAsyncCB<PC, AsyncRpcProxy, RpcProxy>::on_invalid()
{
  int tmp_ret = common::OB_SUCCESS;
  AsyncCB::rcode_.rcode_ = common::OB_RPC_PACKET_INVALID;
  if (common::OB_SUCCESS != (tmp_ret = proxy_.receive_response())) {
    RPC_LOG_RET(WARN, tmp_ret, "proxy_ receive_response failed", K(tmp_ret));
  }
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
class ObAsyncRpcProxy
{
public:
  struct EmptyType {
  public:
    bool is_valid() const { return true; }
    TO_STRING_EMPTY();
  };
  ObAsyncRpcProxy(RpcProxy &rpc_proxy, const Func &func);
  virtual ~ObAsyncRpcProxy();

  void reuse();

  int call(const common::ObAddr &server,
           const int64_t timeout);
  int call(const common::ObAddr &server,
           const int64_t timeout,
           const RpcArg &arg);
  int call(const common::ObAddr &server,
           const int64_t timeout,
           const uint64_t tenant_id,
           const RpcArg &arg);
  int call(const common::ObAddr &server,
           const int64_t timeout,
           const int64_t cluster_id,
           const uint64_t tenant_id,
           const RpcArg &arg);
  int call(const common::ObAddr &server,
           const int64_t timeout,
           const int64_t cluster_id,
           const uint64_t tenant_id,
           const uint64_t group_id,
           const RpcArg &arg);

  // wait all asynchronous rpc finish, return fail if any rpc fail.
  int wait();
  // wait all asynchronous rpc finish and store it return code to %return_code_array
  int wait_all(common::ObIArray<int> &return_code_array);
  const common::ObIArray<RpcArg> &get_args() const { return args_; }
  const common::ObIArray<common::ObAddr> &get_dests() const { return dests_; }
  const common::ObIArray<const RpcResult *> &get_results() const { return results_; }
  int receive_response();

  int check_return_cnt(const int64_t return_cnt) const;
private:
  int call_rpc(const common::ObAddr &server, const int64_t timeout, const int64_t cluster_id,
               const uint64_t tenant_id, const RpcArg &arg, ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb);
  int call_rpc(const common::ObAddr &server, const int64_t timeout, const int64_t cluster_id,
               const uint64_t tenant_id, const uint64_t group_id, const RpcArg &arg,
               ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb);
  int call_rpc(const common::ObAddr &server, const int64_t timeout, const uint64_t tenant_id,
               const EmptyType &empty_obj, ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb);
  int wait(common::ObIArray<int> *return_code_array, const bool return_rpc_error);
  RpcProxy &rpc_proxy_;
  common::ObArray<RpcArg> args_;
  common::ObArray<common::ObAddr> dests_;
  common::ObArray<const RpcResult *> results_;
  Func func_;
  common::ObArenaAllocator allocator_;
  common::ObDList<ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> > cb_list_;
  int64_t response_count_;
  common::ObThreadCond cond_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAsyncRpcProxy);
};

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::ObAsyncRpcProxy(
    RpcProxy &rpc_proxy, const Func &func)
  : rpc_proxy_(rpc_proxy), args_(), results_(),
    func_(func), allocator_(common::ObModIds::OB_ASYNC_RPC_PROXY),
    cb_list_(), response_count_(0), cond_()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(cond_.init(common::ObWaitEventIds::ASYNC_RPC_PROXY_COND_WAIT))) {
    RPC_LOG(ERROR, "cond init failed", K(ret));
  }
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::~ObAsyncRpcProxy()
{
  reuse();
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
void ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::reuse()
{
  args_.reuse();
  dests_.reuse();
  results_.reuse();
  response_count_ = 0;
  ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb = cb_list_.get_first();
  ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *next = NULL;
  while (cb != cb_list_.get_header()) {
    next = cb->get_next();
    cb->~ObAsyncCB();
    cb = next;
  }
  cb_list_.clear();
  allocator_.reuse();
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::call(
    const common::ObAddr &server,
    const int64_t timeout)
{
  int ret = common::OB_SUCCESS;
  if (!server.is_valid() || timeout <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "invalid argument", K(server), K(timeout), K(ret));
  } else if (OB_FAIL(call(server, timeout, EmptyType()))) {
    RPC_LOG(WARN, "call failed", K(server), K(timeout), K(ret));
  }

  // do_call failed, outer code won't wait, we should wait rpc responses have sent
  if (OB_FAIL(ret)) {
    common::ObThreadCondGuard guard(cond_);
    while (response_count_ < cb_list_.get_size()) {
      cond_.wait();
    }
  }
  return ret;
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::call(
    const common::ObAddr &server,
    const int64_t timeout,
    const RpcArg &arg)
{
  return call(server, timeout, common::OB_INVALID_CLUSTER_ID, OB_SYS_TENANT_ID, 0, arg);
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::call(
    const common::ObAddr &server,
    const int64_t timeout,
    const uint64_t tenant_id,
    const RpcArg &arg)
{
  return call(server, timeout, common::OB_INVALID_CLUSTER_ID, tenant_id, 0, arg);
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::call(
    const common::ObAddr &server,
    const int64_t timeout,
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const RpcArg &arg)
{
  return call(server, timeout, cluster_id, tenant_id, 0, arg);
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::call(
    const common::ObAddr &server,
    const int64_t timeout,
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const uint64_t group_id,
    const RpcArg &arg)
{
  int ret = common::OB_SUCCESS;
  void *mem = NULL;
  if (!server.is_valid() || timeout <= 0 || !arg.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "invalid argument", K(server), K(timeout), K(arg), KR(ret));
  } else if (NULL == (mem = allocator_.alloc(sizeof(ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy>)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RPC_LOG(ERROR, "alloc memory failed",
        "size", sizeof(ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy>), KR(ret));
  } else {
    ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb = new (mem) ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy>(*this);
    if (!cb_list_.add_last(cb)) {
      ret = common::OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "cb_list add_last failed", KR(ret));
    } else {
      if (OB_FAIL(args_.push_back(arg))) {
        RPC_LOG(WARN, "push_back failed", K(arg), KR(ret));
      } else if (OB_FAIL(dests_.push_back(server))) {
        RPC_LOG(WARN, "push_back failed", K(server), KR(ret));
      } else if (0 == group_id && OB_FAIL(call_rpc(server, timeout, cluster_id, tenant_id, arg, cb))) {
        RPC_LOG(WARN, "call rpc func failed", K(server), K(timeout),
               K(cluster_id), K(tenant_id), K(arg), K(group_id), KR(ret));
      } else if (0 != group_id && OB_FAIL(call_rpc(server, timeout, cluster_id, tenant_id, group_id, arg, cb))) {
        RPC_LOG(WARN, "call rpc func failed", K(server), K(timeout),
               K(cluster_id), K(tenant_id), K(arg), K(group_id), KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // if send rpc failed, just call on_timeout to fill the result and add response count
      cb->on_timeout();
    }
  }

  // do_call failed, outer code won't wait, we should wait rpc responses have sent
  if (OB_FAIL(ret)) {
    common::ObThreadCondGuard guard(cond_);
    while (response_count_ < cb_list_.get_size()) {
      cond_.wait();
    }
  }

  return ret;
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::call_rpc(
    const common::ObAddr &server, const int64_t timeout,
    const int64_t cluster_id, const uint64_t tenant_id,
    const RpcArg &arg, ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb)
{
  int ret = common::OB_SUCCESS;
  if (!server.is_valid() || timeout <= 0 || !arg.is_valid() || NULL == cb) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "invalid argument", K(server), K(timeout), K(arg), KP(cb), KR(ret));
  } else if (common::OB_INVALID_CLUSTER_ID == cluster_id) {
    if (OB_FAIL((rpc_proxy_.to(server).by(tenant_id).timeout(timeout).*func_)(
        arg, cb, ObRpcOpts()))) {
      RPC_LOG(WARN, "call rpc func failed", K(server), K(timeout), K(arg), K(tenant_id), KR(ret));
    }
  } else {
    if (OB_FAIL((rpc_proxy_.to(server).dst_cluster_id(cluster_id)
                .by(tenant_id).timeout(timeout).*func_)(arg, cb, ObRpcOpts()))) {
      RPC_LOG(WARN, "call rpc func failed", K(server), K(timeout), K(arg),
              K(cluster_id), K(tenant_id), KR(ret));
    }
  }
  return ret;
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::call_rpc(
    const common::ObAddr &server, const int64_t timeout,
    const int64_t cluster_id, const uint64_t tenant_id,
    const uint64_t group_id, const RpcArg &arg,
    ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb)
{
  int ret = common::OB_SUCCESS;
  if (!server.is_valid() || timeout <= 0 || !arg.is_valid() || NULL == cb) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "invalid argument", K(server), K(timeout), K(arg), KP(cb), KR(ret));
  } else if (common::OB_INVALID_CLUSTER_ID == cluster_id) {
    if (OB_FAIL((rpc_proxy_.to(server).by(tenant_id).timeout(timeout).group_id(group_id).*func_)(
        arg, cb, ObRpcOpts()))) {
      RPC_LOG(WARN, "call rpc func failed", K(server), K(timeout), K(arg),
              K(tenant_id), K(group_id), KR(ret));
    }
  } else {
    if (OB_FAIL((rpc_proxy_.to(server).dst_cluster_id(cluster_id)
                .by(tenant_id).timeout(timeout).group_id(group_id).*func_)(arg, cb, ObRpcOpts()))) {
      RPC_LOG(WARN, "call rpc func failed", K(server), K(timeout), K(arg),
              K(cluster_id), K(tenant_id), K(group_id), KR(ret));
    }
  }
  return ret;
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::call_rpc(
    const common::ObAddr &server, const int64_t timeout, const uint64_t tenant_id,
    const EmptyType &empty_obj, ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb)
{
  UNUSED(empty_obj);
  int ret = common::OB_SUCCESS;
  if (!server.is_valid() || timeout <= 0 || NULL == cb) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "invalid argument", K(server), K(timeout), KP(cb), K(ret));
  } else if (OB_FAIL((rpc_proxy_.to(server).by(tenant_id).timeout(timeout).*func_)(
          cb, ObRpcOpts()))) {
    RPC_LOG(WARN, "call rpc func failed", K(server), K(timeout), K(tenant_id), K(ret));
  }
  return ret;
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::wait()
{
  common::ObIArray<int> *return_code_array = NULL;
  const bool return_rpc_error = true;
  return wait(return_code_array, return_rpc_error);
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::wait_all(common::ObIArray<int> &return_code_array)
{
  const bool return_rpc_error = false;
  return wait(&return_code_array, return_rpc_error);
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::wait(
    common::ObIArray<int> *return_code_array, const bool return_rpc_error)
{
  int ret = common::OB_SUCCESS;
  {
    common::ObThreadCondGuard guard(cond_);
    if (response_count_ < 0 || response_count_ > cb_list_.get_size()) {
      ret = common::OB_INNER_STAT_ERROR;
      RPC_LOG(WARN, "inner stat error", K_(response_count), "cb_count",
          cb_list_.get_size(), K(ret));
    } else {
      bool has_terminated = false;
      while (response_count_ < cb_list_.get_size()) {
        cond_.wait(1000);
        if (OB_UNLIKELY(rpc_proxy_.is_detect_session_killed()
                          && !has_terminated
                          && OB_ERR_SESSION_INTERRUPTED == THIS_WORKER.check_status())) {
          RPC_LOG(INFO, "check session killed, will terminate all rpc locally", K(response_count_), K(cb_list_.get_size()));
          int tmp_ret = OB_SUCCESS;
          int index = 0;
          ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb = cb_list_.get_first();
          while (common::OB_SUCCESS == tmp_ret && cb != cb_list_.get_header()) {
            if (NULL == cb) {
              tmp_ret = common::OB_ERR_UNEXPECTED;
              RPC_LOG_RET(WARN, tmp_ret, "cb is null", KP(cb));
            } else {
              RPC_LOG(INFO, "terminate the rpc of cb_list", K(cb->gtid_), K(cb->pkt_id_), K(index));
              int err = 0;
              if ((err = pn_terminate_pkt(cb->gtid_, cb->pkt_id_)) != 0) {
                tmp_ret = tranlate_to_ob_error(err);
                RPC_LOG_RET(WARN, tmp_ret, "pn_terminate_pkt failed", K(err));
              } else {
                cb = cb->get_next();
                ++index;
              }
            }
          }
          if (index == cb_list_.get_size()) {
            has_terminated = true;
          }
        }
      }

      // set results
      int index = 0;
      ObAsyncCB<PC, ObAsyncRpcProxy, RpcProxy> *cb = cb_list_.get_first();
      while (common::OB_SUCCESS == ret && cb != cb_list_.get_header()) {
        if (NULL == cb) {
          ret = common::OB_ERR_UNEXPECTED;
          RPC_LOG(WARN, "cb is null", KP(cb), K(ret));
        } else {
          const int rc = cb->get_ret_code();
          if (common::OB_SUCCESS != rc) {
            if (index <= (args_.count() -1)) {
              // ignore ret
              RPC_LOG(WARN, "execute rpc failed", K(rc), "server", cb->get_dst(), "timeout", cb->get_timeout(),
                  "packet code", PC, "arg", args_.at(index), K(ret));
            } else {
              // ignore ret
              RPC_LOG(WARN, "execute rpc failed and args_ count is not correct", K(rc), "server", cb->get_dst(), "timeout", cb->get_timeout(),
                  "packet code", PC, K(args_.count()), K(index), K(ret));
            }
          }
          if (NULL != return_code_array) {
            if (OB_FAIL(return_code_array->push_back(rc))) {
              RPC_LOG(WARN, "add return code failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (common::OB_SUCCESS != rc && return_rpc_error) {
              ret = rc;
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(results_.push_back(&cb->get_result()))) {
            RPC_LOG(WARN, "push_back failed", K(ret));
          } else {
            cb = cb->get_next();
            ++index;
          }
        }
      }
    }
  }
  return ret;
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::receive_response()
{
  int ret = common::OB_SUCCESS;
  common::ObThreadCondGuard guard(cond_);
  if (response_count_ < 0 || response_count_ >= cb_list_.get_size()) {
    ret = common::OB_INNER_STAT_ERROR;
    RPC_LOG(WARN, "inner stat error", K_(response_count), "cb_count", cb_list_.get_size(), K(ret));
  } else {
    ++response_count_;
    if (response_count_ == cb_list_.get_size()) {
      int tmp_ret = cond_.broadcast();
      if (common::OB_SUCCESS != tmp_ret) {
        RPC_LOG(WARN, "condition broadcast failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

template<ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func, typename RpcProxy>
int ObAsyncRpcProxy<PC, RpcArg, RpcResult, Func, RpcProxy>::check_return_cnt(
    const int64_t return_cnt) const
{
  int ret = common::OB_SUCCESS;
  if (return_cnt != args_.count()
      || return_cnt != dests_.count()
      || return_cnt != results_.count()) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "return cnt not match",
            KR(ret), K(return_cnt),
            "arg_cnt", args_.count(),
            "dest_cnt", dests_.count(),
            "result_cnt", results_.count());
  }
  return ret;
}

#define RPC_F(code, arg, result, name) \
  typedef obrpc::ObAsyncRpcProxy<code, arg, result, \
    int (obrpc::ObSrvRpcProxy::*)(const arg &, obrpc::ObSrvRpcProxy::AsyncCB<code> *, const obrpc::ObRpcOpts &), obrpc::ObSrvRpcProxy> name


}//end namespace obrpc
}//end namespace oceanbase

#endif //OCEANBASE_RPC_OB_ASYNC_RPC_PROXY_H_
