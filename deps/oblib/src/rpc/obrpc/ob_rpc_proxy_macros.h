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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_MACROS_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_MACROS_

#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"

#define OROP_ const ObRpcOpts &opts = ObRpcOpts()
#define ORSSH_(pcode) SSHandle<pcode> &handle
#define ORACB_(pcode) AsyncCB<pcode> *cb

// packet priority s
#define PR1 )(ORPR1,
#define PR2 )(ORPR2,
#define PR3 )(ORPR3,
#define PR4 )(ORPR4,
#define PR5 )(ORPR5,
#define PR6 )(ORPR6,
#define PR7 )(ORPR7,
#define PR8 )(ORPR8,
#define PR9 )(ORPR9,
// for DDL
#define PRD )(ORPR_DDL,
// for LEASE
#define PRZ )(ORPR_UNDEF,
#define PR11 )(ORPR11,

#define OB_RPC_STRUCT(pcode, Input, Output)           \
  template <typename IGNORE>                          \
  struct ObRpc<pcode, IGNORE>                         \
  {                                                   \
    static constexpr auto PCODE = pcode;              \
    typedef INPUT_TYPE_(Input) Request;               \
    typedef OUTPUT_TYPE_(Output) Response;            \
  };

#define OB_DEFINE_RPC_STRUCT(pcode, Input, Output)     \
  template <typename IGNORE>                    \
  struct ObRpc<pcode, IGNORE> {                 \
    static constexpr auto PCODE = pcode;        \
    typedef Input Request;                      \
    typedef Output Response;                    \
  };

#define RPC_CALL_DISPATCH(name, ...)                             \
  if (mock_proxy_) {                                             \
    mock_proxy_->set_server(dst_);                               \
    return mock_proxy_->name(__VA_ARGS__);                       \
  } else {                                                       \
    return name ##_(args, result, opts);                         \
  }

#define OB_DEFINE_RPC_SYNC(name, pcode, prio, Input, Output)    \
  OB_DEFINE_RPC_STRUCT(pcode, Input, Output)                    \
  int name ## _(const Input& args, Output& result, OROP_)           \
  {                                                             \
    const static ObRpcPriority PR = prio;                       \
    int ret = common::OB_SUCCESS;                               \
    ObRpcOpts newopts = opts;                                   \
    if (newopts.pr_ == ORPR_UNDEF) {                            \
      newopts.pr_ = PR;                                         \
    }                                                                   \
    newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
    newopts.local_addr_ = GCTX.self_addr();                             \
    ret = rpc_call(pcode, args, result, NULL, newopts);                 \
    return ret;                                                 \
  }

#define OB_DEFINE_RPC_S2_(name, pcode, prio, Input, Output)     \
  OB_DEFINE_RPC_SYNC(name, pcode, prio, Input, Output);         \
  virtual int name(const Input& args, Output& result, OROP_) {  \
    RPC_CALL_DISPATCH(name, args, result, opts);                \
  }
#define OB_DEFINE_RPC_S2(name, pcode, prio, Input, Output) OB_DEFINE_RPC_S2_(name, pcode, prio, EXPAND Input, Output)

#define OB_DEFINE_RPC_S1_INPUT_(name, pcode, prio, Input) \
  OB_DEFINE_RPC_SYNC(name, pcode, prio, Input, NoneT);    \
  virtual int name(const Input& args, OROP_) {            \
    NoneT result;                                         \
    RPC_CALL_DISPATCH(name, args, opts);                  \
  }
#define OB_DEFINE_RPC_S1_INPUT(name, pcode, prio, Input) OB_DEFINE_RPC_S1_INPUT_(name, pcode, prio, EXPAND Input)
#define OB_DEFINE_RPC_S1_OUTPUT(name, pcode, prio, Output)  \
  OB_DEFINE_RPC_SYNC(name, pcode, prio, NoneT, Output);     \
  virtual int name(Output& result, OROP_) {                 \
    const NoneT args;                                       \
    RPC_CALL_DISPATCH(name, result, opts);                  \
  }
#define OB_DEFINE_RPC_S1(name, pcode, prio, InOut) IF_IS_PAREN(InOut, OB_DEFINE_RPC_S1_INPUT, OB_DEFINE_RPC_S1_OUTPUT)(name, pcode, prio, InOut)

#define OB_DEFINE_RPC_S0(name, pcode, prio)             \
  OB_DEFINE_RPC_SYNC(name, pcode, prio, NoneT, NoneT);  \
  virtual int name(OROP_) {                             \
    const NoneT args;                                   \
    NoneT result;                                       \
    RPC_CALL_DISPATCH(name, opts);                      \
  }

#define OB_DEFINE_RPC_S(prio, name, pcode, ...)                 \
  SELECT4(,                                                     \
          ## __VA_ARGS__,                                       \
          OB_DEFINE_RPC_S2,                                     \
          OB_DEFINE_RPC_S1,                                     \
          OB_DEFINE_RPC_S0) (name, pcode, prio, ## __VA_ARGS__)

#define RPC_S(args...) _CONCAT(OB_DEFINE_RPC, _S IGNORE_(args))

#define OB_DEFINE_RPC_STREAM(name, pcode, prio, Input, Output)          \
  OB_DEFINE_RPC_STRUCT(pcode, Input, Output);                           \
  int name ##_(const Input& args, Output& result, ORSSH_(pcode), OROP_) { \
    int ret = common::OB_SUCCESS;                                       \
    const static ObRpcPriority PR = prio;                               \
    ObRpcOpts newopts = opts;                                           \
    if (newopts.pr_ == ORPR_UNDEF) {                                    \
      newopts.pr_ = PR;                                                 \
    }                                                                   \
    newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
    newopts.local_addr_ = GCTX.self_addr();                             \
    ret = rpc_call(pcode, args, result, &handle, newopts);              \
    return ret;                                                         \
  }
// define synchronized stream interface
#define OB_DEFINE_RPC_SS2_(name, pcode, prio, Input, Output)            \
  OB_DEFINE_RPC_STREAM(name, pcode, prio, Input, Output);               \
  virtual int name(const Input& args, Output& result, ORSSH_(pcode), OROP_) { \
    return name ##_(args, result, handle, opts);                               \
  }
#define OB_DEFINE_RPC_SS2(name, pcode, prio, Input, Output) OB_DEFINE_RPC_SS2_(name, pcode, prio, EXPAND Input, Output)
#define OB_DEFINE_RPC_SS1_INPUT_(name, pcode, prio, Input)     \
  OB_DEFINE_RPC_STREAM(name, pcode, prio, Input, NoneT);      \
  virtual int name(const Input& args, ORSSH_(pcode), OROP_) { \
    NoneT result;                                             \
    return name ##_(args, result, handle, opts);              \
  }
#define OB_DEFINE_RPC_SS1_INPUT(name, pcode, prio, Input)  OB_DEFINE_RPC_SS1_INPUT_(name, pcode, prio, EXPAND Input)
#define OB_DEFINE_RPC_SS1_OUTPUT(name, pcode, prio, Output) \
  OB_DEFINE_RPC_STREAM(name, pcode, prio, NoneT, Output);   \
  virtual int name(Output& result, ORSSH_(pcode), OROP_) {  \
    NoneT args;                                             \
    return name ##_(args, result, handle, opts);            \
  }
#define OB_DEFINE_RPC_SS1(name, pcode, prio, InOut)  IF_IS_PAREN(InOut, OB_DEFINE_RPC_SS1_INPUT, OB_DEFINE_RPC_SS1_OUTPUT)(name, pcode, prio, InOut)

// Theoretically, stream rpc without argument or result is
// impossible. We add this SS0 interface just complete our rpc
// framework.
#define OB_DEFINE_RPC_SS0(name, pcode, prio)
#define OB_DEFINE_RPC_SS(prio, name, pcode, ...)                  \
  SELECT4(,                                                       \
          ## __VA_ARGS__,                                         \
          OB_DEFINE_RPC_SS2,                                      \
          OB_DEFINE_RPC_SS1,                                      \
          OB_DEFINE_RPC_SS0) (name, pcode, prio, ## __VA_ARGS__)

#define RPC_SS(args...) _CONCAT(OB_DEFINE_RPC, _SS IGNORE_(args))

// define asynchronous interface
#define OB_RPC_ASYNC_DISPATCH(name, ...)        \
  if (mock_proxy_) {                            \
    mock_proxy_->set_server(dst_);              \
    return mock_proxy_->name(__VA_ARGS__);      \
  } else {                                      \
    return name##_(args, cb, opts);          \
  }

#define OB_DEFINE_RPC_ASYNC(name, pcode, prio, Input, Output)             \
  OB_DEFINE_RPC_STRUCT(pcode, Input, Output);                           \
  int name##_(const Input& args, ORACB_(pcode), OROP_)                   \
  {                                                                     \
    const static ObRpcPriority PR = prio;                               \
    int ret = common::OB_SUCCESS;                                       \
    ObRpcOpts newopts = opts;                                           \
    if (newopts.pr_ == ORPR_UNDEF) {                                    \
      newopts.pr_ = PR;                                                 \
    }                                                                   \
    newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
    newopts.local_addr_ = GCTX.self_addr();                             \
    ret = rpc_post<ObRpc<pcode>>(args, cb, newopts);                    \
    return ret;                                                         \
  }

#define OB_DEFINE_RPC_AP2_(name, pcode, prio, Input, Output)     \
  OB_DEFINE_RPC_ASYNC(name, pcode, prio, Input, Output);        \
  virtual int name(const Input& args, ORACB_(pcode), OROP_)  {  \
    OB_RPC_ASYNC_DISPATCH(name, args, cb, opts);                \
  }
#define OB_DEFINE_RPC_AP2(name, pcode, prio, Input, Output) OB_DEFINE_RPC_AP2_(name, pcode, prio, EXPAND Input, Output)

#define OB_DEFINE_RPC_AP1_INPUT_(name, pcode, prio, Input)       \
  OB_DEFINE_RPC_ASYNC(name, pcode, prio, Input, NoneT);         \
  virtual int name(const Input& args, ORACB_(pcode), OROP_)  {  \
    OB_RPC_ASYNC_DISPATCH(name, args, cb, opts);                \
  }
#define OB_DEFINE_RPC_AP1_OUTPUT(name, pcode, prio, Output) \
  OB_DEFINE_RPC_ASYNC(name, pcode, prio, NoneT, Output);    \
  virtual int name(ORACB_(pcode), OROP_)  {                 \
    OB_RPC_ASYNC_DISPATCH(name, cb, opts);                  \
  }
#define OB_DEFINE_RPC_AP1_INPUT(name, pcode, prio, InOut) OB_DEFINE_RPC_AP1_INPUT_(name, pcode, prio, EXPAND InOut)
#define OB_DEFINE_RPC_AP1(name, pcode, prio, InOut)   IF_IS_PAREN(InOut, OB_DEFINE_RPC_AP1_INPUT, OB_DEFINE_RPC_AP1_OUTPUT)(name, pcode, prio, InOut)

#define OB_DEFINE_RPC_AP0(name, pcode, prio)            \
  OB_DEFINE_RPC_ASYNC(name, pcode, prio, NoneT, NoneT); \
  virtual int name(ORACB_(pcode), OROP_)  {             \
    OB_RPC_ASYNC_DISPATCH(name, cb, opts);              \
  }

#define SELECT4(a, b, c, d, ...) d
#define OB_DEFINE_RPC_AP(prio, name, pcode, ...)           \
  SELECT4(,                                                       \
          ## __VA_ARGS__,                                         \
          OB_DEFINE_RPC_AP2,                                      \
          OB_DEFINE_RPC_AP1,                                      \
          OB_DEFINE_RPC_AP0) (name, pcode, prio, ## __VA_ARGS__)

#define RPC_AP(args...) _CONCAT(OB_DEFINE_RPC, _AP IGNORE_(args))

#endif // _OB_RPC_PROXY_MACROS_H_
