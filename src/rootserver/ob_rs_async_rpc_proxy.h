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

#ifndef OCEANBASE_ROOTSERVER_OB_RS_ASYNC_RPC_PROXY_H_
#define OCEANBASE_ROOTSERVER_OB_RS_ASYNC_RPC_PROXY_H_

#include "lib/lock/ob_thread_cond.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "rpc/obrpc/ob_rpc_proxy.h"

namespace oceanbase {
namespace rootserver {

template <obrpc::ObRpcPacketCode PC, typename AsyncRpcProxy>
class ObRsAsyncCB : public obrpc::ObSrvRpcProxy::AsyncCB<PC>,
                    public common::ObDLinkBase<ObRsAsyncCB<PC, AsyncRpcProxy> > {
  using AsyncCB = typename obrpc::ObSrvRpcProxy::AsyncCB<PC>;

public:
  ObRsAsyncCB(AsyncRpcProxy& proxy) : proxy_(proxy)
  {}
  virtual ~ObRsAsyncCB()
  {}

  void set_args(const typename AsyncCB::Request& args)
  {
    UNUSED(args);
  }
  rpc::frame::ObReqTransport::AsyncCB* clone(const rpc::frame::SPAlloc& alloc) const;

  int process();
  void on_timeout();
  void on_invalid();

  int get_ret_code() const
  {
    return AsyncCB::rcode_.rcode_;
  }
  const common::ObAddr& get_dst() const
  {
    return AsyncCB::dst_;
  }
  int64_t get_timeout() const
  {
    return AsyncCB::timeout_;
  }
  const typename AsyncCB::Response& get_result() const
  {
    return AsyncCB::result_;
  }

  TO_STRING_KV("dst", get_dst(), "ret_code", get_ret_code(), "result", get_result());

private:
  AsyncRpcProxy& proxy_;
};

template <obrpc::ObRpcPacketCode PC, typename AsyncRpcProxy>
rpc::frame::ObReqTransport::AsyncCB* ObRsAsyncCB<PC, AsyncRpcProxy>::clone(const rpc::frame::SPAlloc& alloc) const
{
  UNUSED(alloc);
  return const_cast<rpc::frame::ObReqTransport::AsyncCB*>(
      static_cast<const rpc::frame::ObReqTransport::AsyncCB* const>(this));
}

template <obrpc::ObRpcPacketCode PC, typename AsyncRpcProxy>
int ObRsAsyncCB<PC, AsyncRpcProxy>::process()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(proxy_.receive_response())) {
    RS_LOG(WARN, "proxy_ receive_response failed", K(ret));
  }
  return ret;
}

template <obrpc::ObRpcPacketCode PC, typename AsyncRpcProxy>
void ObRsAsyncCB<PC, AsyncRpcProxy>::on_timeout()
{
  int ret = common::OB_SUCCESS;
  AsyncCB::rcode_.rcode_ = common::OB_TIMEOUT;
  if (OB_FAIL(proxy_.receive_response())) {
    RS_LOG(WARN, "proxy_ receive_response failed", K(ret));
  }
}

template <obrpc::ObRpcPacketCode PC, typename AsyncRpcProxy>
void ObRsAsyncCB<PC, AsyncRpcProxy>::on_invalid()
{
  int tmp_ret = common::OB_SUCCESS;
  AsyncCB::rcode_.rcode_ = common::OB_RPC_PACKET_INVALID;
  if (common::OB_SUCCESS != (tmp_ret = proxy_.receive_response())) {
    RS_LOG(WARN, "proxy_ receive_response failed", K(tmp_ret));
  }
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
class ObRsAsyncRpcProxy {
public:
  struct EmptyType {
  public:
    bool is_valid() const
    {
      return true;
    }
    TO_STRING_EMPTY();
  };
  ObRsAsyncRpcProxy(obrpc::ObSrvRpcProxy& rpc_proxy, const Func& func);
  virtual ~ObRsAsyncRpcProxy();

  void reuse();

  int call(const common::ObAddr& server, const int64_t timeout);
  int call(const common::ObAddr& server, const int64_t timeout, const RpcArg& arg);
  int call(const common::ObAddr& server, const int64_t timeout, const uint64_t tenant_id, const RpcArg& arg);

  // wait all asynchronous rpc finish, return fail if any rpc fail.
  int wait();
  // wait all asynchronous rpc finish and store it return code to %return_code_array
  int wait_all(common::ObIArray<int>& return_code_array);
  const common::ObIArray<RpcArg>& get_args() const
  {
    return args_;
  }
  const common::ObIArray<common::ObAddr>& get_dests() const
  {
    return dests_;
  }
  const common::ObIArray<const RpcResult*>& get_results() const
  {
    return results_;
  }
  int receive_response();

private:
  int call_rpc(const common::ObAddr& server, const int64_t timeout, const uint64_t tenant_id, const RpcArg& arg,
      ObRsAsyncCB<PC, ObRsAsyncRpcProxy>* cb);
  int call_rpc(const common::ObAddr& server, const int64_t timeout, const uint64_t tenant_id,
      const EmptyType& empty_obj, ObRsAsyncCB<PC, ObRsAsyncRpcProxy>* cb);
  int wait(common::ObIArray<int>* return_code_array, const bool return_rpc_error);
  obrpc::ObSrvRpcProxy& rpc_proxy_;
  common::ObArray<RpcArg> args_;
  common::ObArray<common::ObAddr> dests_;
  common::ObArray<const RpcResult*> results_;
  Func func_;
  common::ObArenaAllocator allocator_;
  common::ObDList<ObRsAsyncCB<PC, ObRsAsyncRpcProxy> > cb_list_;
  int64_t response_count_;
  common::ObThreadCond cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRsAsyncRpcProxy);
};

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::ObRsAsyncRpcProxy(obrpc::ObSrvRpcProxy& rpc_proxy, const Func& func)
    : rpc_proxy_(rpc_proxy),
      args_(),
      results_(),
      func_(func),
      allocator_(common::ObModIds::OB_RS_ASYNC_RPC_PROXY),
      cb_list_(),
      response_count_(0),
      cond_()
{
  (void)cond_.init(common::ObWaitEventIds::ASYNC_RPC_PROXY_COND_WAIT);
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::~ObRsAsyncRpcProxy()
{
  reuse();
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
void ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::reuse()
{
  args_.reuse();
  results_.reuse();
  response_count_ = 0;
  ObRsAsyncCB<PC, ObRsAsyncRpcProxy>* cb = cb_list_.get_first();
  ObRsAsyncCB<PC, ObRsAsyncRpcProxy>* next = NULL;
  while (cb != cb_list_.get_header()) {
    next = cb->get_next();
    cb->~ObRsAsyncCB();
    cb = next;
  }
  cb_list_.clear();
  allocator_.reuse();
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::call(const common::ObAddr& server, const int64_t timeout)
{
  int ret = common::OB_SUCCESS;
  if (!server.is_valid() || timeout <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(server), K(timeout), K(ret));
  } else if (OB_FAIL(call(server, timeout, EmptyType()))) {
    RS_LOG(WARN, "call failed", K(server), K(timeout), K(ret));
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

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::call(
    const common::ObAddr& server, const int64_t timeout, const RpcArg& arg)
{
  return call(server, timeout, OB_SYS_TENANT_ID, arg);
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::call(
    const common::ObAddr& server, const int64_t timeout, const uint64_t tenant_id, const RpcArg& arg)
{
  int ret = common::OB_SUCCESS;
  void* mem = NULL;
  if (!server.is_valid() || timeout <= 0 || !arg.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(server), K(timeout), K(arg), K(ret));
  } else if (NULL == (mem = allocator_.alloc(sizeof(ObRsAsyncCB<PC, ObRsAsyncRpcProxy>)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RS_LOG(ERROR, "alloc memory failed", "size", sizeof(ObRsAsyncCB<PC, ObRsAsyncRpcProxy>), K(ret));
  } else {
    ObRsAsyncCB<PC, ObRsAsyncRpcProxy>* cb = new (mem) ObRsAsyncCB<PC, ObRsAsyncRpcProxy>(*this);
    if (!cb_list_.add_last(cb)) {
      ret = common::OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "cb_list add_last failed", K(ret));
    } else {
      if (OB_FAIL(args_.push_back(arg))) {
        RS_LOG(WARN, "push_back failed", K(arg), K(ret));
      } else if (OB_FAIL(dests_.push_back(server))) {
        RS_LOG(WARN, "push_back failed", K(server), K(ret));
      } else if (OB_FAIL(call_rpc(server, timeout, tenant_id, arg, cb))) {
        RS_LOG(WARN, "call rpc func failed", K(server), K(timeout), K(tenant_id), K(arg), K(ret));
      }
      if (OB_FAIL(ret)) {
        cb_list_.remove_last();
      }
    }
    if (OB_FAIL(ret)) {
      // free memory
      cb->~ObRsAsyncCB();
      allocator_.free(mem);
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

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::call_rpc(const common::ObAddr& server, const int64_t timeout,
    const uint64_t tenant_id, const RpcArg& arg, ObRsAsyncCB<PC, ObRsAsyncRpcProxy>* cb)
{
  int ret = common::OB_SUCCESS;
  if (!server.is_valid() || timeout <= 0 || !arg.is_valid() || NULL == cb) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(server), K(timeout), K(arg), KP(cb), K(ret));
  } else if (OB_FAIL((rpc_proxy_.to(server).by(tenant_id).timeout(timeout).*func_)(arg, cb, obrpc::ObRpcOpts()))) {
    RS_LOG(WARN, "call rpc func failed", K(server), K(timeout), K(arg), K(tenant_id), K(ret));
  }
  return ret;
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::call_rpc(const common::ObAddr& server, const int64_t timeout,
    const uint64_t tenant_id, const EmptyType& empty_obj, ObRsAsyncCB<PC, ObRsAsyncRpcProxy>* cb)
{
  UNUSED(empty_obj);
  int ret = common::OB_SUCCESS;
  if (!server.is_valid() || timeout <= 0 || NULL == cb) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(server), K(timeout), KP(cb), K(ret));
  } else if (OB_FAIL((rpc_proxy_.to(server).by(tenant_id).timeout(timeout).*func_)(cb, obrpc::ObRpcOpts()))) {
    RS_LOG(WARN, "call rpc func failed", K(server), K(timeout), K(tenant_id), K(ret));
  }
  return ret;
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::wait()
{
  common::ObIArray<int>* return_code_array = NULL;
  const bool return_rpc_error = true;
  return wait(return_code_array, return_rpc_error);
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::wait_all(common::ObIArray<int>& return_code_array)
{
  const bool return_rpc_error = false;
  return wait(&return_code_array, return_rpc_error);
}

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::wait(
    common::ObIArray<int>* return_code_array, const bool return_rpc_error)
{
  int ret = common::OB_SUCCESS;
  {
    common::ObThreadCondGuard guard(cond_);
    if (response_count_ < 0 || response_count_ > cb_list_.get_size()) {
      ret = common::OB_INNER_STAT_ERROR;
      RS_LOG(WARN, "inner stat error", K_(response_count), "cb_count", cb_list_.get_size(), K(ret));
    } else {
      while (response_count_ < cb_list_.get_size()) {
        cond_.wait();
      }

      // set results
      int index = 0;
      ObRsAsyncCB<PC, ObRsAsyncRpcProxy>* cb = cb_list_.get_first();
      while (common::OB_SUCCESS == ret && cb != cb_list_.get_header()) {
        if (NULL == cb) {
          ret = common::OB_ERR_UNEXPECTED;
          RS_LOG(WARN, "cb is null", KP(cb), K(ret));
        } else {
          const int rc = cb->get_ret_code();
          if (common::OB_SUCCESS != rc) {
            RS_LOG(WARN,
                "execute rpc failed",
                K(rc),
                "server",
                cb->get_dst(),
                "timeout",
                cb->get_timeout(),
                "packet code",
                PC,
                "arg",
                args_.at(index));
          }
          if (NULL != return_code_array) {
            if (OB_FAIL(return_code_array->push_back(rc))) {
              RS_LOG(WARN, "add return code failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (common::OB_SUCCESS != rc && return_rpc_error) {
              ret = rc;
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(results_.push_back(&cb->get_result()))) {
            RS_LOG(WARN, "push_back failed", K(ret));
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

template <obrpc::ObRpcPacketCode PC, typename RpcArg, typename RpcResult, typename Func>
int ObRsAsyncRpcProxy<PC, RpcArg, RpcResult, Func>::receive_response()
{
  int ret = common::OB_SUCCESS;
  common::ObThreadCondGuard guard(cond_);
  if (response_count_ < 0 || response_count_ >= cb_list_.get_size()) {
    ret = common::OB_INNER_STAT_ERROR;
    RS_LOG(WARN, "inner stat error", K_(response_count), "cb_count", cb_list_.get_size(), K(ret));
  } else {
    ++response_count_;
    if (response_count_ == cb_list_.get_size()) {
      int tmp_ret = cond_.broadcast();
      if (common::OB_SUCCESS != tmp_ret) {
        RS_LOG(WARN, "condition broadcast failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

#define RPC_F(code, arg, result, name)                                                                          \
  typedef ObRsAsyncRpcProxy<code,                                                                               \
      arg,                                                                                                      \
      result,                                                                                                   \
      int (obrpc::ObSrvRpcProxy::*)(const arg&, obrpc::ObSrvRpcProxy::AsyncCB<code>*, const obrpc::ObRpcOpts&)> \
      name

RPC_F(obrpc::OB_MINOR_FREEZE, obrpc::ObMinorFreezeArg, obrpc::Int64, ObMinorFreezeProxy);
RPC_F(obrpc::OB_PREPARE_MAJOR_FREEZE, obrpc::ObMajorFreezeArg, obrpc::Int64, ObPrepareMajorFreezeProxy);
RPC_F(obrpc::OB_COMMIT_MAJOR_FREEZE, obrpc::ObMajorFreezeArg, obrpc::Int64, ObCommitMajorFreezeProxy);
RPC_F(obrpc::OB_ABORT_MAJOR_FREEZE, obrpc::ObMajorFreezeArg, obrpc::Int64, ObAbortMajorFreezeProxy);
RPC_F(obrpc::OB_CREATE_PARTITION, obrpc::ObCreatePartitionArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_CREATE_PARTITION>::Response, ObCreatePartitionProxy);
RPC_F(obrpc::OB_GET_WRS_INFO, obrpc::ObGetWRSArg, obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_GET_WRS_INFO>::Response,
    ObGetWRSProxy);
RPC_F(obrpc::OB_CREATE_PARTITION_BATCH, obrpc::ObCreatePartitionBatchArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_CREATE_PARTITION_BATCH>::Response, ObCreatePartitionBatchProxy);
RPC_F(obrpc::OB_SWITCH_LEADER_LIST_ASYNC, obrpc::ObSwitchLeaderListArg, obrpc::Int64, ObSwitchLeaderListAsyncProxy);
RPC_F(obrpc::OB_GET_LEADER_CANDIDATES_ASYNC, obrpc::ObGetLeaderCandidatesArg, obrpc::ObGetLeaderCandidatesResult,
    ObGetLeaderCandidatesAsyncProxy);
RPC_F(obrpc::OB_GET_LEADER_CANDIDATES_ASYNC_V2, obrpc::ObGetLeaderCandidatesV2Arg, obrpc::ObGetLeaderCandidatesResult,
    ObGetLeaderCandidatesAsyncProxyV2);
RPC_F(obrpc::OB_CHECK_SCHEMA_VERSION_ELAPSED, obrpc::ObCheckSchemaVersionElapsedArg,
    obrpc::ObCheckSchemaVersionElapsedResult, ObCheckSchemaVersionElapsedProxy);
RPC_F(obrpc::OB_BATCH_SPLIT_PARTITION, obrpc::ObSplitPartitionArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_BATCH_SPLIT_PARTITION>::Response, ObSplitPartitionBatchProxy);
RPC_F(obrpc::OB_CHECK_CTX_CREATE_TIMESTAMP_ELAPSED, obrpc::ObCheckCtxCreateTimestampElapsedArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_CHECK_CTX_CREATE_TIMESTAMP_ELAPSED>::Response,
    ObCheckCtxCreateTimestampElapsedProxy);
RPC_F(obrpc::OB_PARTITION_STOP_WRITE, obrpc::Int64,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_PARTITION_STOP_WRITE>::Response, ObRpcStopWriteProxy);
RPC_F(obrpc::OB_PARTITION_CHECK_LOG, obrpc::Int64, obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_PARTITION_CHECK_LOG>::Response,
    ObRpcCheckLogProxy);
RPC_F(obrpc::OB_BATCH_START_ELECTION, obrpc::ObBatchStartElectionArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_BATCH_START_ELECTION>::Response, ObRpcBatchTranslateProxy);
RPC_F(obrpc::OB_BATCH_FLASHBACK, obrpc::ObBatchFlashbackArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_BATCH_FLASHBACK>::Response, ObRpcBatchFlashBackProxy);
RPC_F(obrpc::OB_BATCH_PREPARE_FLASHBACK, obrpc::ObBatchFlashbackArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_BATCH_PREPARE_FLASHBACK>::Response, ObRpcBatchPrepareFlashBackProxy);
RPC_F(obrpc::OB_NOTIFY_TENANT_SERVER_UNIT_RESOURCE, obrpc::TenantServerUnitConfig,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_NOTIFY_TENANT_SERVER_UNIT_RESOURCE>::Response,
    ObNotifyTenantServerResourceProxy);
RPC_F(obrpc::OB_BATCH_SET_MEMBER_LIST, obrpc::ObSetMemberListBatchArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_BATCH_SET_MEMBER_LIST>::Response, ObSetMemberListBatchProxy);
RPC_F(obrpc::OB_REACH_PARTITION_LIMIT, obrpc::ObReachPartitionLimitArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_REACH_PARTITION_LIMIT>::Response, ObReachPartitionLimitProxy);
RPC_F(obrpc::OB_CHECK_FROZEN_VERSION, obrpc::ObCheckFrozenVersionArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_CHECK_FROZEN_VERSION>::Response, ObCheckFrozenVersionProxy);
RPC_F(obrpc::OB_BATCH_WAIT_LEADER, obrpc::ObBatchCheckLeaderArg, obrpc::ObBatchCheckRes, ObRpcBatchWaitLeaderProxy);
RPC_F(obrpc::OB_BATCH_WRITE_CUTDATA_CLOG, obrpc::ObBatchWriteCutdataClogArg, obrpc::ObBatchCheckRes,
    ObRpcBatchCutdataProxy);
RPC_F(obrpc::OB_GET_MIN_SSTABLE_SCHEMA_VERSION, obrpc::ObGetMinSSTableSchemaVersionArg,
    obrpc::ObGetMinSSTableSchemaVersionRes, ObGetMinSSTableSchemaVersionProxy);
RPC_F(obrpc::OB_BATCH_GET_MEMBER_LIST_AND_LEADER, obrpc::ObLocationRpcRenewArg, obrpc::ObLocationRpcRenewResult,
    ObBatchRpcRenewLocProxy);
RPC_F(obrpc::OB_BATCH_GET_ROLE, obrpc::ObBatchGetRoleArg, obrpc::ObBatchGetRoleResult, ObBatchGetRoleProxy);
RPC_F(obrpc::OB_BATCH_GET_PROTECTION_LEVEL, obrpc::ObBatchCheckLeaderArg, obrpc::ObBatchCheckRes,
    ObBatchGetProtectionLevelProxy);
RPC_F(obrpc::OB_CHECK_NEED_OFFLINE_REPLICA, obrpc::ObTenantSchemaVersions, obrpc::ObGetPartitionCountResult,
    ObCheckNeedOfflineReplicaProxy);
RPC_F(obrpc::OB_CHECK_FLASHBACK_INFO_DUMP, obrpc::ObCheckFlashbackInfoArg, obrpc::ObCheckFlashbackInfoResult,
    ObCheckFlashbackInfoProxy);

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_RS_ASYNC_RPC_PROXY_H_
