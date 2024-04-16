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

#include "ob_table_load_resource_rpc_struct.h"
#include "observer/table_load/ob_table_load_rpc_executor.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{
class ObDirectLoadResourceApplyExecutor;
class ObDirectLoadResourceReleaseExecutor;
class ObDirectLoadResourceUpdateExecutor;
class ObDirectLoadResourceCheckExecutor;

class ObTableLoadResourceRpcProxy
{
public:
  static const int64_t DEFAULT_TIMEOUT_US = 10LL * 1000 * 1000; // 10s
  template <ObDirectLoadResourceCommandType pcode, typename IGNORE = void>
  struct ObTableLoadResourceRpc
  {
  };

#define OB_DEFINE_TABLE_LOAD_RESOURCE_RPC_CALL_1(name, pcode, Arg)                                    \
  int name(const Arg &arg)                                                                            \
  {                                                                                                   \
    int ret = OB_SUCCESS;                                                                             \
    ObDirectLoadResourceOpRequest request;                                                            \
    ObDirectLoadResourceOpResult result;                                                              \
    request.command_type_ = pcode;                                                                    \
    result.allocator_ = &allocator_;                                                                  \
    if (OB_FAIL(request.set_arg(arg, allocator_))) {                                                  \
      SERVER_LOG(WARN, "fail to set arg", K(ret), K(arg));                                            \
    } else if (OB_FAIL(rpc_proxy_.to(addr_)                                                           \
                                 .timeout(timeout_)                                                   \
                                 .by(tenant_id_)                                                      \
                                 .direct_load_resource(request, result))) {                           \
      SERVER_LOG(WARN, "fail to rpc call direct load resource", K(ret), K_(addr), K(arg));            \
    } else if (OB_UNLIKELY(result.command_type_ != pcode)) {                                          \
      ret = OB_ERR_UNEXPECTED;                                                                        \
      SERVER_LOG(WARN, "unexpected command type", K(ret), K(request), K(result));                     \
    } else if (OB_UNLIKELY(!result.res_content_.empty())) {                                           \
      ret = OB_ERR_UNEXPECTED;                                                                        \
      SERVER_LOG(WARN, "unexpected non empty res content", K(ret), K(result));                        \
    }                                                                                                 \
    return ret;                                                                                       \
  }

#define OB_DEFINE_TABLE_LOAD_RESOURCE_RPC_CALL_2(name, pcode, Arg, Res)                               \
  int name(const Arg &arg, Res &res)                                                                  \
  {                                                                                                   \
    int ret = OB_SUCCESS;                                                                             \
    ObDirectLoadResourceOpRequest request;                                                            \
    ObDirectLoadResourceOpResult result;                                                              \
    request.command_type_ = pcode;                                                                    \
    result.allocator_ = &allocator_;                                                                  \
    if (OB_FAIL(request.set_arg(arg, allocator_))) {                                                  \
      SERVER_LOG(WARN, "fail to set arg", K(ret), K(arg));                                            \
    } else if (OB_FAIL(rpc_proxy_.to(addr_)                                                           \
                                 .timeout(timeout_)                                                   \
                                 .by(tenant_id_)                                                      \
                                 .direct_load_resource(request, result))) {                           \
      SERVER_LOG(WARN, "fail to rpc call direct load resource", K(ret), K_(addr), K(arg));            \
    } else if (OB_UNLIKELY(result.command_type_ != pcode)) {                                          \
      ret = OB_ERR_UNEXPECTED;                                                                        \
      SERVER_LOG(WARN, "unexpected command type", K(ret), K(request), K(result));                     \
    } else if (OB_FAIL(result.get_res(res))) {                                                        \
      SERVER_LOG(WARN, "fail to get res", K(ret), K(result));                                         \
    }                                                                                                 \
    return ret;                                                                                       \
  }

#define OB_DEFINE_TABLE_LOAD_RESOURCE_RPC_CALL(name, pcode, ...)                                      \
  CONCAT(OB_DEFINE_TABLE_LOAD_RESOURCE_RPC_CALL_, ARGS_NUM(__VA_ARGS__))(name, pcode, __VA_ARGS__)

#define OB_DEFINE_TABLE_LOAD_RESOURCE_RPC(name, pcode, Processor, ...)                                \
  OB_DEFINE_TABLE_LOAD_RPC(ObTableLoadResourceRpc, pcode, Processor, ObDirectLoadResourceOpRequest,   \
                           ObDirectLoadResourceOpResult, __VA_ARGS__)                                 \
  OB_DEFINE_TABLE_LOAD_RESOURCE_RPC_CALL(name, pcode, __VA_ARGS__)

public:
  ObTableLoadResourceRpcProxy(obrpc::ObSrvRpcProxy &rpc_proxy)
    : rpc_proxy_(rpc_proxy),
      allocator_("TLD_RpcProxy"),
      timeout_(DEFAULT_TIMEOUT_US),
      tenant_id_(MTL_ID())
  {
    allocator_.set_tenant_id(MTL_ID());
  }

  ObTableLoadResourceRpcProxy &to(ObAddr addr)
  {
    addr_ = addr;
    return *this;
  }
  ObTableLoadResourceRpcProxy &timeout(int64_t timeout)
  {
    timeout_ = timeout;
    return *this;
  }
  ObTableLoadResourceRpcProxy &by(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
    return *this;
  }

  static int dispatch(const ObDirectLoadResourceOpRequest &request,
                      ObDirectLoadResourceOpResult &result,
                      common::ObIAllocator &allocator);

  // apply_resource
  OB_DEFINE_TABLE_LOAD_RESOURCE_RPC(apply_resource,
                                    ObDirectLoadResourceCommandType::APPLY,
                                    ObDirectLoadResourceApplyExecutor,
                                    ObDirectLoadResourceApplyArg,
                                    ObDirectLoadResourceOpRes);
  // release_resource
  OB_DEFINE_TABLE_LOAD_RESOURCE_RPC(release_resource,
                                    ObDirectLoadResourceCommandType::RELEASE,
                                    ObDirectLoadResourceReleaseExecutor,
                                    ObDirectLoadResourceReleaseArg);
  // update_resource
  OB_DEFINE_TABLE_LOAD_RESOURCE_RPC(update_resource,
                                    ObDirectLoadResourceCommandType::UPDATE,
                                    ObDirectLoadResourceUpdateExecutor,
                                    ObDirectLoadResourceUpdateArg);
  // check_resource
  OB_DEFINE_TABLE_LOAD_RESOURCE_RPC(check_resource,
                                    ObDirectLoadResourceCommandType::CHECK,
                                    ObDirectLoadResourceCheckExecutor,
                                    ObDirectLoadResourceCheckArg,
                                    ObDirectLoadResourceOpRes);

private:
  obrpc::ObSrvRpcProxy &rpc_proxy_;
  ObArenaAllocator allocator_;
  ObAddr addr_;
  int64_t timeout_;
  uint64_t tenant_id_;
};

#define TABLE_LOAD_RESOURCE_RPC_CALL(name, addr, arg, ...)                                                     \
  ({                                                                                                           \
    ObTableLoadResourceRpcProxy proxy(*GCTX.srv_rpc_proxy_);                                                   \
    ObTimeoutCtx ctx;                                                                                          \
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, ObTableLoadResourceRpcProxy::DEFAULT_TIMEOUT_US))) { \
      LOG_WARN("fail to set default timeout ctx", KR(ret));                                                    \
    } else if (OB_FAIL(proxy.to(addr)                                                                          \
                            .timeout(ctx.get_timeout())                                                        \
                            .by(MTL_ID())                                                                      \
                            .name(arg, ##__VA_ARGS__))) {                                                                \
      LOG_WARN("fail to rpc call " #name, KR(ret), K(addr), K(arg));                                           \
    }                                                                                                          \
  })

} // namespace observer
} // namespace oceanbase
