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

#include "ob_table_direct_load_rpc_struct.h"
#include "observer/table_load/ob_table_load_rpc_executor.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "share/table/ob_table_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{
class ObTableDirectLoadExecContext;
class ObTableDirectLoadBeginExecutor;
class ObTableDirectLoadCommitExecutor;
class ObTableDirectLoadAbortExecutor;
class ObTableDirectLoadGetStatusExecutor;
class ObTableDirectLoadInsertExecutor;
class ObTableDirectLoadHeartBeatExecutor;

class ObTableDirectLoadRpcProxy
{
  static const int64_t DEFAULT_TIMEOUT_US = 10LL * 1000 * 1000; // 10s
public:
  template <table::ObTableDirectLoadOperationType pcode, typename IGNORE = void>
  struct ObTableDirectLoadRpc
  {
  };

#define OB_DEFINE_TABLE_DIRECT_LOAD_RPC_CALL_1(prio, name, pcode, Arg)                \
  int name(const Arg &arg)                                                            \
  {                                                                                   \
    int ret = OB_SUCCESS;                                                             \
    table::ObTableDirectLoadRequest request;                                          \
    table::ObTableDirectLoadResult result;                                            \
    request.header_.operation_type_ = pcode;                                          \
    request.credential_ = credential_;                                                \
    result.allocator_ = &allocator_;                                                  \
    if (OB_FAIL(request.set_arg(arg, allocator_))) {                                  \
      SERVER_LOG(WARN, "fail to set arg", K(ret), K(arg));                            \
    } else if (OB_FAIL(rpc_proxy_.to(addr_)                                           \
                         .timeout(timeout_)                                           \
                         .by(tenant_id_)                                              \
                         .group_id(ObTableLoadRpcPriority::HIGH_PRIO == prio          \
                                     ? share::OBCG_DIRECT_LOAD_HIGH_PRIO              \
                                     : share::OBCG_DEFAULT)                           \
                         .direct_load(request, result))) {                            \
      SERVER_LOG(WARN, "fail to rpc call direct load", K(ret), K_(addr), K(request)); \
    } else if (OB_UNLIKELY(result.header_.operation_type_ != pcode)) {                \
      ret = OB_ERR_UNEXPECTED;                                                        \
      SERVER_LOG(WARN, "unexpected operation type", K(ret), K(request), K(result));   \
    } else if (OB_UNLIKELY(!result.res_content_.empty())) {                           \
      ret = OB_ERR_UNEXPECTED;                                                        \
      SERVER_LOG(WARN, "unexpected non empty res content", K(ret), K(result));        \
    }                                                                                 \
    return ret;                                                                       \
  }

#define OB_DEFINE_TABLE_DIRECT_LOAD_RPC_CALL_2(prio, name, pcode, Arg, Res)           \
  int name(const Arg &arg, Res &res)                                                  \
  {                                                                                   \
    int ret = OB_SUCCESS;                                                             \
    table::ObTableDirectLoadRequest request;                                          \
    table::ObTableDirectLoadResult result;                                            \
    request.header_.operation_type_ = pcode;                                          \
    request.credential_ = credential_;                                                \
    result.allocator_ = &allocator_;                                                  \
    if (OB_FAIL(request.set_arg(arg, allocator_))) {                                  \
      SERVER_LOG(WARN, "fail to set arg", K(ret), K(arg));                            \
    } else if (OB_FAIL(rpc_proxy_.to(addr_)                                           \
                         .timeout(timeout_)                                           \
                         .by(tenant_id_)                                              \
                         .group_id(ObTableLoadRpcPriority::HIGH_PRIO == prio          \
                                     ? share::OBCG_DIRECT_LOAD_HIGH_PRIO              \
                                     : share::OBCG_DEFAULT)                           \
                         .direct_load(request, result))) {                            \
      SERVER_LOG(WARN, "fail to rpc call direct load", K(ret), K_(addr), K(request)); \
    } else if (OB_UNLIKELY(result.header_.operation_type_ != pcode)) {                \
      ret = OB_ERR_UNEXPECTED;                                                        \
      SERVER_LOG(WARN, "unexpected operation type", K(ret), K(request), K(result));   \
    } else if (OB_FAIL(result.get_res(res))) {                                        \
      SERVER_LOG(WARN, "fail to get res", K(ret), K(result));                         \
    }                                                                                 \
    return ret;                                                                       \
  }

#define OB_DEFINE_TABLE_DIRECT_LOAD_RPC_CALL(prio, name, pcode, ...)   \
  CONCAT(OB_DEFINE_TABLE_DIRECT_LOAD_RPC_CALL_, ARGS_NUM(__VA_ARGS__)) \
  (prio, name, pcode, __VA_ARGS__)

#define OB_DEFINE_TABLE_DIRECT_LOAD_RPC(prio, name, pcode, Processor, ...)                  \
  OB_DEFINE_TABLE_LOAD_RPC(ObTableDirectLoadRpc, pcode, Processor,                          \
                           table::ObTableDirectLoadRequest, table::ObTableDirectLoadResult, \
                           __VA_ARGS__)                                                     \
  OB_DEFINE_TABLE_DIRECT_LOAD_RPC_CALL(ObTableLoadRpcPriority::prio, name, pcode, __VA_ARGS__)

public:
  ObTableDirectLoadRpcProxy(obrpc::ObTableRpcProxy &rpc_proxy)
    : rpc_proxy_(rpc_proxy),
      allocator_("TLD_RpcProxy"),
      timeout_(DEFAULT_TIMEOUT_US),
      tenant_id_(MTL_ID())
  {
    allocator_.set_tenant_id(MTL_ID());
  }

  ObTableDirectLoadRpcProxy &to(ObAddr addr)
  {
    addr_ = addr;
    return *this;
  }
  ObTableDirectLoadRpcProxy &timeout(int64_t timeout)
  {
    timeout_ = timeout;
    return *this;
  }
  ObTableDirectLoadRpcProxy &by(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
    return *this;
  }
  ObTableDirectLoadRpcProxy &set_credential(const ObString &credential)
  {
    credential_ = credential;
    return *this;
  }

  static int dispatch(ObTableDirectLoadExecContext &ctx,
                      const table::ObTableDirectLoadRequest &request,
                      table::ObTableDirectLoadResult &result);

  // begin
  OB_DEFINE_TABLE_DIRECT_LOAD_RPC(NORMAL_PRIO, begin, table::ObTableDirectLoadOperationType::BEGIN,
                                  ObTableDirectLoadBeginExecutor, ObTableDirectLoadBeginArg,
                                  ObTableDirectLoadBeginRes);
  // commit
  OB_DEFINE_TABLE_DIRECT_LOAD_RPC(NORMAL_PRIO, commit,
                                  table::ObTableDirectLoadOperationType::COMMIT,
                                  ObTableDirectLoadCommitExecutor, ObTableDirectLoadCommitArg);
  // abort
  OB_DEFINE_TABLE_DIRECT_LOAD_RPC(NORMAL_PRIO, abort, table::ObTableDirectLoadOperationType::ABORT,
                                  ObTableDirectLoadAbortExecutor, ObTableDirectLoadAbortArg);
  // get_status
  OB_DEFINE_TABLE_DIRECT_LOAD_RPC(NORMAL_PRIO, get_status,
                                  table::ObTableDirectLoadOperationType::GET_STATUS,
                                  ObTableDirectLoadGetStatusExecutor, ObTableDirectLoadGetStatusArg,
                                  ObTableDirectLoadGetStatusRes);
  // insert
  OB_DEFINE_TABLE_DIRECT_LOAD_RPC(NORMAL_PRIO, insert,
                                  table::ObTableDirectLoadOperationType::INSERT,
                                  ObTableDirectLoadInsertExecutor, ObTableDirectLoadInsertArg);

  // heart_beat
  OB_DEFINE_TABLE_DIRECT_LOAD_RPC(HIGH_PRIO, heartbeat,
                                  table::ObTableDirectLoadOperationType::HEART_BEAT,
                                  ObTableDirectLoadHeartBeatExecutor, ObTableDirectLoadHeartBeatArg,
                                  ObTableDirectLoadHeartBeatRes);

private:
  obrpc::ObTableRpcProxy &rpc_proxy_;
  ObArenaAllocator allocator_;
  ObAddr addr_;
  int64_t timeout_;
  uint64_t tenant_id_;
  ObString credential_;
};

} // namespace observer
} // namespace oceanbase
