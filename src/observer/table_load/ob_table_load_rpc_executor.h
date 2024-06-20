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

#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
enum class ObTableLoadRpcPriority
{
  NORMAL_PRIO = 0,
  HIGH_PRIO = 1,
};

// template <typename pcode, typename IGNORE = void>
// struct ObTableLoadRpc
// {
// };

#define OB_DEFINE_TABLE_LOAD_RPC_STRUCT(RpcType, pcode, Processor, Request, Result, Arg, Res) \
  template <typename IGNORE>                                                                  \
  struct RpcType<pcode, IGNORE>                                                               \
  {                                                                                           \
    static constexpr auto PCODE = pcode;                                                      \
    typedef Processor ProcessorType;                                                          \
    typedef Request RequestType;                                                              \
    typedef Result ResultType;                                                                \
    typedef Arg ArgType;                                                                      \
    typedef Res ResType;                                                                      \
  };

#define OB_DEFINE_TABLE_LOAD_RPC_S1(RpcType, pcode, Processor, Request, Result, Arg) \
  OB_DEFINE_TABLE_LOAD_RPC_STRUCT(RpcType, pcode, Processor, Request, Result, Arg,   \
                                  obrpc::ObRpcProxy::NoneT)

#define OB_DEFINE_TABLE_LOAD_RPC_S2(RpcType, pcode, Processor, Request, Result, Arg, Res) \
  OB_DEFINE_TABLE_LOAD_RPC_STRUCT(RpcType, pcode, Processor, Request, Result, Arg, Res)

#define OB_DEFINE_TABLE_LOAD_RPC(RpcType, pcode, Processor, Request, Result, ...) \
  CONCAT(OB_DEFINE_TABLE_LOAD_RPC_S, ARGS_NUM(__VA_ARGS__))                       \
  (RpcType, pcode, Processor, Request, Result, __VA_ARGS__)

#define OB_TABLE_LOAD_RPC_PROCESS_WITHOUT_ARG(RpcType, pcode, request, result) \
  ({                                                                           \
    typename RpcType<pcode>::ProcessorType processor(request, result);         \
    if (OB_FAIL(processor.execute())) {                                        \
      SERVER_LOG(WARN, "fail to execute", K(ret));                             \
    }                                                                          \
  })

#define OB_TABLE_LOAD_RPC_PROCESS_WITH_ARG(RpcType, pcode, request, result, ...)    \
  ({                                                                                \
    typename RpcType<pcode>::ProcessorType processor(__VA_ARGS__, request, result); \
    if (OB_FAIL(processor.execute())) {                                             \
      SERVER_LOG(WARN, "fail to execute", K(ret));                                  \
    }                                                                               \
  })

#define OB_TABLE_LOAD_RPC_PROCESS_ARG0(RpcType, pcode, request, result) \
  OB_TABLE_LOAD_RPC_PROCESS_WITHOUT_ARG(RpcType, pcode, request, result)
#define OB_TABLE_LOAD_RPC_PROCESS_ARG1(RpcType, pcode, request, result, ...) \
  OB_TABLE_LOAD_RPC_PROCESS_WITH_ARG(RpcType, pcode, request, result, __VA_ARGS__)
#define OB_TABLE_LOAD_RPC_PROCESS_ARG2(RpcType, pcode, request, result, ...) \
  OB_TABLE_LOAD_RPC_PROCESS_WITH_ARG(RpcType, pcode, request, result, __VA_ARGS__)
#define OB_TABLE_LOAD_RPC_PROCESS_ARG3(RpcType, pcode, request, result, ...) \
  OB_TABLE_LOAD_RPC_PROCESS_WITH_ARG(RpcType, pcode, request, result, __VA_ARGS__)
#define OB_TABLE_LOAD_RPC_PROCESS_ARG4(RpcType, pcode, request, result, ...) \
  OB_TABLE_LOAD_RPC_PROCESS_WITH_ARG(RpcType, pcode, request, result, __VA_ARGS__)
#define OB_TABLE_LOAD_RPC_PROCESS_ARG5(RpcType, pcode, request, result, ...) \
  OB_TABLE_LOAD_RPC_PROCESS_WITH_ARG(RpcType, pcode, request, result, __VA_ARGS__)
#define OB_TABLE_LOAD_RPC_PROCESS_ARG6(RpcType, pcode, request, result, ...) \
  OB_TABLE_LOAD_RPC_PROCESS_WITH_ARG(RpcType, pcode, request, result, __VA_ARGS__)

#define OB_TABLE_LOAD_RPC_PROCESS(RpcType, pcode, request, result, ...) \
  CONCAT(OB_TABLE_LOAD_RPC_PROCESS_ARG, ARGS_NUM(__VA_ARGS__))          \
  (RpcType, pcode, request, result, ##__VA_ARGS__)

template <class Rpc>
class ObTableLoadRpcExecutor
{
  typedef typename Rpc::RequestType RequestType;
  typedef typename Rpc::ResultType ResultType;
  typedef typename Rpc::ArgType ArgType;
  typedef typename Rpc::ResType ResType;

public:
  ObTableLoadRpcExecutor(const RequestType &request, ResultType &result)
    : request_(request), result_(result)
  {
  }
  virtual ~ObTableLoadRpcExecutor() = default;
  int execute()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(deserialize())) {
      SERVER_LOG(WARN, "fail to do deserialize", K(ret));
    } else if (OB_FAIL(check_args())) {
      SERVER_LOG(WARN, "fail to check args", K(ret));
    } else if (OB_FAIL(process())) {
      SERVER_LOG(WARN, "fail to process", K(ret));
    } else if (OB_FAIL(set_result_header())) {
      SERVER_LOG(WARN, "fail to set result header", K(ret));
    } else if (OB_FAIL(serialize())) {
      SERVER_LOG(WARN, "fail to do serialize", K(ret));
    }
    return ret;
  }

protected:
  // deserialize arg from request
  virtual int deserialize() = 0;
  virtual int check_args() = 0;
  virtual int process() = 0;
  virtual int set_result_header() = 0;
  // serialize res to result
  virtual int serialize() = 0;

protected:
  const RequestType &request_;
  ResultType &result_;
  ArgType arg_;
  ResType res_;
};

} // namespace observer
} // namespace oceanbase
