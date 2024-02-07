/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "ob_table_load_control_rpc_struct.h"
#include "observer/table_load/ob_table_load_rpc_executor.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{
class ObDirectLoadControlPreBeginExecutor;
class ObDirectLoadControlConfirmBeginExecutor;
class ObDirectLoadControlPreMergeExecutor;
class ObDirectLoadControlStartMergeExecutor;
class ObDirectLoadControlCommitExecutor;
class ObDirectLoadControlAbortExecutor;
class ObDirectLoadControlGetStatusExecutor;
class ObDirectLoadControlHeartBeatExecutor;
/// trans
class ObDirectLoadControlPreStartTransExecutor;
class ObDirectLoadControlConfirmStartTransExecutor;
class ObDirectLoadControlPreFinishTransExecutor;
class ObDirectLoadControlConfirmFinishTransExecutor;
class ObDirectLoadControlAbandonTransExecutor;
class ObDirectLoadControlGetTransStatusExecutor;
class ObDirectLoadControlInsertTransExecutor;

class ObTableLoadControlRpcProxy
{
  static const int64_t DEFAULT_TIMEOUT_US = 10LL * 1000 * 1000; // 10s
public:
  template <ObDirectLoadControlCommandType pcode, typename IGNORE = void>
  struct ObTableLoadControlRpc
  {
  };

#define OB_DEFINE_TABLE_LOAD_CONTROL_RPC_CALL_1(prio, name, pcode, Arg)                       \
  int name(const Arg &arg)                                                                    \
  {                                                                                           \
    int ret = OB_SUCCESS;                                                                     \
    ObDirectLoadControlRequest request;                                                       \
    ObDirectLoadControlResult result;                                                         \
    request.command_type_ = pcode;                                                            \
    result.allocator_ = &allocator_;                                                          \
    if (OB_FAIL(request.set_arg(arg, allocator_))) {                                          \
      SERVER_LOG(WARN, "fail to set arg", K(ret), K(arg));                                    \
    } else if (OB_FAIL(rpc_proxy_.to(addr_)                                                   \
                         .timeout(timeout_)                                                   \
                         .by(tenant_id_)                                                      \
                         .group_id(ObTableLoadRpcPriority::HIGH_PRIO == prio                  \
                                     ? share::OBCG_DIRECT_LOAD_HIGH_PRIO                      \
                                     : share::OBCG_DEFAULT)                                   \
                         .direct_load_control(request, result))) {                            \
      SERVER_LOG(WARN, "fail to rpc call direct load control", K(ret), K_(addr), K(request)); \
    } else if (OB_UNLIKELY(result.command_type_ != pcode)) {                                  \
      ret = OB_ERR_UNEXPECTED;                                                                \
      SERVER_LOG(WARN, "unexpected command type", K(ret), K(request), K(result));             \
    } else if (OB_UNLIKELY(!result.res_content_.empty())) {                                   \
      ret = OB_ERR_UNEXPECTED;                                                                \
      SERVER_LOG(WARN, "unexpected non empty res content", K(ret), K(result));                \
    }                                                                                         \
    return ret;                                                                               \
  }

#define OB_DEFINE_TABLE_LOAD_CONTROL_RPC_CALL_2(prio, name, pcode, Arg, Res)                  \
  int name(const Arg &arg, Res &res)                                                          \
  {                                                                                           \
    int ret = OB_SUCCESS;                                                                     \
    ObDirectLoadControlRequest request;                                                       \
    ObDirectLoadControlResult result;                                                         \
    request.command_type_ = pcode;                                                            \
    result.allocator_ = &allocator_;                                                          \
    if (OB_FAIL(request.set_arg(arg, allocator_))) {                                          \
      SERVER_LOG(WARN, "fail to set arg", K(ret), K(arg));                                    \
    } else if (OB_FAIL(rpc_proxy_.to(addr_)                                                   \
                         .timeout(timeout_)                                                   \
                         .by(tenant_id_)                                                      \
                         .group_id(ObTableLoadRpcPriority::HIGH_PRIO == prio                  \
                                     ? share::OBCG_DIRECT_LOAD_HIGH_PRIO                      \
                                     : share::OBCG_DEFAULT)                                   \
                         .direct_load_control(request, result))) {                            \
      SERVER_LOG(WARN, "fail to rpc call direct load control", K(ret), K_(addr), K(request)); \
    } else if (OB_UNLIKELY(result.command_type_ != pcode)) {                                  \
      ret = OB_ERR_UNEXPECTED;                                                                \
      SERVER_LOG(WARN, "unexpected command type", K(ret), K(request), K(result));             \
    } else if (OB_FAIL(result.get_res(res))) {                                                \
      SERVER_LOG(WARN, "fail to get res", K(ret), K(result));                                 \
    }                                                                                         \
    return ret;                                                                               \
  }

#define OB_DEFINE_TABLE_LOAD_CONTROL_RPC_CALL(prio, name, pcode, ...)   \
  CONCAT(OB_DEFINE_TABLE_LOAD_CONTROL_RPC_CALL_, ARGS_NUM(__VA_ARGS__)) \
  (prio, name, pcode, __VA_ARGS__)

#define OB_DEFINE_TABLE_LOAD_CONTROL_RPC(prio, name, pcode, Processor, ...)                     \
  OB_DEFINE_TABLE_LOAD_RPC(ObTableLoadControlRpc, pcode, Processor, ObDirectLoadControlRequest, \
                           ObDirectLoadControlResult, __VA_ARGS__)                              \
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC_CALL(ObTableLoadRpcPriority::prio, name, pcode, __VA_ARGS__)

public:
  ObTableLoadControlRpcProxy(obrpc::ObSrvRpcProxy &rpc_proxy)
    : rpc_proxy_(rpc_proxy),
      allocator_("TLD_RpcProxy"),
      timeout_(DEFAULT_TIMEOUT_US),
      tenant_id_(MTL_ID())
  {
    allocator_.set_tenant_id(MTL_ID());
  }

  ObTableLoadControlRpcProxy &to(ObAddr addr)
  {
    addr_ = addr;
    return *this;
  }
  ObTableLoadControlRpcProxy &timeout(int64_t timeout)
  {
    timeout_ = timeout;
    return *this;
  }
  ObTableLoadControlRpcProxy &by(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
    return *this;
  }

  static int dispatch(const ObDirectLoadControlRequest &request, ObDirectLoadControlResult &result,
                      common::ObIAllocator &allocator);

  // pre_begin
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, pre_begin,
                                   ObDirectLoadControlCommandType::PRE_BEGIN,
                                   ObDirectLoadControlPreBeginExecutor,
                                   ObDirectLoadControlPreBeginArg);
  // confirm_begin
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, confirm_begin,
                                   ObDirectLoadControlCommandType::CONFIRM_BEGIN,
                                   ObDirectLoadControlConfirmBeginExecutor,
                                   ObDirectLoadControlConfirmBeginArg);
  // pre_merge
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, pre_merge,
                                   ObDirectLoadControlCommandType::PRE_MERGE,
                                   ObDirectLoadControlPreMergeExecutor,
                                   ObDirectLoadControlPreMergeArg);
  // start_merge
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, start_merge,
                                   ObDirectLoadControlCommandType::START_MERGE,
                                   ObDirectLoadControlStartMergeExecutor,
                                   ObDirectLoadControlStartMergeArg);
  // commit
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, commit, ObDirectLoadControlCommandType::COMMIT,
                                   ObDirectLoadControlCommitExecutor, ObDirectLoadControlCommitArg,
                                   ObDirectLoadControlCommitRes);
  // abort
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, abort, ObDirectLoadControlCommandType::ABORT,
                                   ObDirectLoadControlAbortExecutor, ObDirectLoadControlAbortArg,
                                   ObDirectLoadControlAbortRes);
  // get_status
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, get_status,
                                   ObDirectLoadControlCommandType::GET_STATUS,
                                   ObDirectLoadControlGetStatusExecutor,
                                   ObDirectLoadControlGetStatusArg,
                                   ObDirectLoadControlGetStatusRes);
  // heart_beat
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(HIGH_PRIO, heart_beat,
                                   ObDirectLoadControlCommandType::HEART_BEAT,
                                   ObDirectLoadControlHeartBeatExecutor,
                                   ObDirectLoadControlHeartBeatArg);

  /// trans
  // pre_start_trans
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, pre_start_trans,
                                   ObDirectLoadControlCommandType::PRE_START_TRANS,
                                   ObDirectLoadControlPreStartTransExecutor,
                                   ObDirectLoadControlPreStartTransArg);
  // confirm_start_trans
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, confirm_start_trans,
                                   ObDirectLoadControlCommandType::CONFIRM_START_TRANS,
                                   ObDirectLoadControlConfirmStartTransExecutor,
                                   ObDirectLoadControlConfirmStartTransArg);
  // pre_finish_trans
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, pre_finish_trans,
                                   ObDirectLoadControlCommandType::PRE_FINISH_TRANS,
                                   ObDirectLoadControlPreFinishTransExecutor,
                                   ObDirectLoadControlPreFinishTransArg);
  // confirm_finish_trans
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, confirm_finish_trans,
                                   ObDirectLoadControlCommandType::CONFIRM_FINISH_TRANS,
                                   ObDirectLoadControlConfirmFinishTransExecutor,
                                   ObDirectLoadControlConfirmFinishTransArg);
  // abandon_trans
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, abandon_trans,
                                   ObDirectLoadControlCommandType::ABANDON_TRANS,
                                   ObDirectLoadControlAbandonTransExecutor,
                                   ObDirectLoadControlAbandonTransArg);
  // get_trans_status
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, get_trans_status,
                                   ObDirectLoadControlCommandType::GET_TRANS_STATUS,
                                   ObDirectLoadControlGetTransStatusExecutor,
                                   ObDirectLoadControlGetTransStatusArg,
                                   ObDirectLoadControlGetTransStatusRes);
  OB_DEFINE_TABLE_LOAD_CONTROL_RPC(NORMAL_PRIO, insert_trans,
                                   ObDirectLoadControlCommandType::INSERT_TRANS,
                                   ObDirectLoadControlInsertTransExecutor,
                                   ObDirectLoadControlInsertTransArg);

private:
  obrpc::ObSrvRpcProxy &rpc_proxy_;
  ObArenaAllocator allocator_;
  ObAddr addr_;
  int64_t timeout_;
  uint64_t tenant_id_;
};

} // namespace observer
} // namespace oceanbase
