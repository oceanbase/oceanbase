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

#define SELECT4(a, b, c, d, ...) d
#define SELECT5(a, b, c, d, e, ...) e

// AOR_, aka argument or result, accepts one argument which is a
// typename. If the argument is surrounded by parenthesis, then the
// type is represented as input argument type and result into "const
// Type & args", otherwise the argument will be treated as result type
// and produces "Type & result". Here's the explanation of expanding
// step by step:
//
//   AOR_((Type))
//     => CONCAT(IS_, NOT_CONST_P_ (Type)) & AOR_P_((Type))
//     => CONCAT(IS_, CONST_P_ Type) & CONCAT(IS_, RESULT_P_ (Type)) )
//     => IS_CONST_P_ Type & CONCAT(IS_, ARGS_P_) )
//     => const Type & IS_ARGS_P_ )
//     => const Type & args IGNORE_( )
//     => const Type & args
//
//   AOR_(Type)
//     => CONCAT(IS_, NOT_CONST_P_ Type) & AOR_P_(Type)
//     => IS_NOT_CONST_P_ Type & CONCAT(IS_, RESULT_P_ Type) )
//     => Type & IS_RESULT_P_ Type )
//     => Type & result IGNORE_( Type )
//     => Type & result
//
#define RPM_ARGS(T) const T& args
#define RPM_RESULT(T) T& result
#define AOR_(T) IF_PAREN(T, RPM_ARGS, RPM_RESULT)

// AOR_P_ is the core macro used by macro AOR_, return "args" if it's
// surrounded by parenthesis, "result" or not.
//
//   AOR_P_((Type)) => args
//   AOR_P_(Type) => result
//
#define RPM_ARGS_P(T) args
#define RPM_RESULT_P(T) result
#define AOR_P_(T) IF_PAREN(T, RPM_ARGS_P, RPM_RESULT_P)

// SWITCH_IN_OUT_(Type) => (Type)
// SWITCH_IN_OUT_((Type)) => Type
#define RPM2INPUT(T) (T)
#define RPM2OUPUT(T) T
#define SWITCH_IN_OUT_(T) IF_PAREN(T, RPM2OUTPUT, RPM2INPUT)

// INPUT_TYPE_((Type)) => Type
// INPUT_TYPE_(Type) => NoneT
// OUTPUT_TYPE_((Type)) => NoneT
// OUTPUT_TYPE_(Type) => Type
#define RPM_SELF_TYPE(T) T
#define RPM_NONE_TYPE(T) NoneT
#define INPUT_TYPE_(T) IF_PAREN(T, RPM_SELF_TYPE, RPM_NONE_TYPE)
#define OUTPUT_TYPE_(T) IF_PAREN(T, RPM_NONE_TYPE, RPM_SELF_TYPE)

// AP_AOR_(Type) => ,
// AP_AOR_((Type)) => const Type &args,
#define AP_IGNORE(T)
#define AP_INPUT(T) const T &args,
#define AP_AOR_(T) IF_PAREN(T, AP_INPUT, AP_IGNORE)

// AP_AOR_P_(Type) => ,
// AP_AOR_P_((Type)) => const Type &args,
#define AP_INPUT_P(T) args,
#define AP_AOR_P_(T) IF_PAREN(T, AP_INPUT_P, AP_IGNORE)

#define OROP_ const ObRpcOpts& opts = ObRpcOpts()
#define ORSSH_(pcode) SSHandle<pcode>& handle
#define ORACB_(pcode) AsyncCB<pcode>* cb

// packet priority level
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

#define OB_RPC_STRUCT(pcode, Input, Output) \
  template <typename IGNORE>                \
  struct ObRpc<pcode, IGNORE> {             \
    static constexpr auto PCODE = pcode;    \
    typedef INPUT_TYPE_(Input) Request;     \
    typedef OUTPUT_TYPE_(Output) Response;  \
  };

#define OB_DEFINE_RPC_S2(name, pcode, prio, Input, Output)                         \
  OB_RPC_STRUCT(pcode, Input, Output)                                              \
  virtual int name(AOR_(Input), AOR_(Output), OROP_)                               \
  {                                                                                \
    const static ObRpcPriority PR = prio;                                          \
    int ret = common::OB_SUCCESS;                                                  \
    if (mock_proxy_) {                                                             \
      mock_proxy_->set_server(dst_);                                               \
      ret = mock_proxy_->name(args, result, opts);                                 \
    } else {                                                                       \
      ObRpcOpts newopts = opts;                                                    \
      if (newopts.pr_ == ORPR_UNDEF) {                                             \
        newopts.pr_ = PR;                                                          \
      }                                                                            \
      newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
      newopts.local_addr_ = GCTX.self_addr_;                                       \
      ret = rpc_call(pcode, args, result, NULL, newopts);                          \
    }                                                                              \
    return ret;                                                                    \
  }

#define OB_DEFINE_RPC_S1(name, pcode, prio, InOut)                                 \
  OB_RPC_STRUCT(pcode, InOut, InOut)                                               \
  virtual int name(AOR_(InOut), OROP_)                                             \
  {                                                                                \
    const static ObRpcPriority PR = prio;                                          \
    int ret = common::OB_SUCCESS;                                                  \
    if (mock_proxy_) {                                                             \
      mock_proxy_->set_server(dst_);                                               \
      ret = mock_proxy_->name(AOR_P_(InOut), opts);                                \
    } else {                                                                       \
      ObRpcOpts newopts = opts;                                                    \
      if (newopts.pr_ == ORPR_UNDEF) {                                             \
        newopts.pr_ = PR;                                                          \
      }                                                                            \
      newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
      newopts.local_addr_ = GCTX.self_addr_;                                       \
      ret = rpc_call(pcode, AOR_P_(InOut), NULL, newopts);                         \
    }                                                                              \
    return ret;                                                                    \
  }

#define OB_DEFINE_RPC_S0(name, pcode, prio)                                        \
  OB_RPC_STRUCT(pcode, (NoneT), NoneT)                                             \
  virtual int name(OROP_)                                                          \
  {                                                                                \
    const static ObRpcPriority PR = prio;                                          \
    int ret = common::OB_SUCCESS;                                                  \
    if (mock_proxy_) {                                                             \
      mock_proxy_->set_server(dst_);                                               \
      ret = mock_proxy_->name(opts);                                               \
    } else {                                                                       \
      ObRpcOpts newopts = opts;                                                    \
      if (newopts.pr_ == ORPR_UNDEF) {                                             \
        newopts.pr_ = PR;                                                          \
      }                                                                            \
      newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
      newopts.local_addr_ = GCTX.self_addr_;                                       \
      ret = rpc_call(pcode, NULL, newopts);                                        \
    }                                                                              \
    return ret;                                                                    \
  }

#define OB_DEFINE_RPC_S(prio, name, pcode, ...) \
  SELECT4(, ##__VA_ARGS__, OB_DEFINE_RPC_S2, OB_DEFINE_RPC_S1, OB_DEFINE_RPC_S0)(name, pcode, prio, ##__VA_ARGS__)

#define RPC_S(args...) _CONCAT(OB_DEFINE_RPC, _S IGNORE_(args))

// define synchronized stream interface
#define OB_DEFINE_RPC_SS2(name, pcode, prio, Input, Output)                      \
  OB_RPC_STRUCT(pcode, Input, Output)                                            \
  virtual int name(AOR_(Input), AOR_(Output), ORSSH_(pcode), OROP_)              \
  {                                                                              \
    int ret = common::OB_SUCCESS;                                                \
    const static ObRpcPriority PR = prio;                                        \
    ObRpcOpts newopts = opts;                                                    \
    if (newopts.pr_ == ORPR_UNDEF) {                                             \
      newopts.pr_ = PR;                                                          \
    }                                                                            \
    newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
    newopts.local_addr_ = GCTX.self_addr_;                                       \
    ret = rpc_call(pcode, args, result, &handle, newopts);                       \
    return ret;                                                                  \
  }

#define OB_DEFINE_RPC_SS1(name, pcode, prio, InOut)                              \
  OB_RPC_STRUCT(pcode, InOut, InOut)                                             \
  virtual int name(AOR_(InOut), ORSSH_(pcode), OROP_)                            \
  {                                                                              \
    int ret = common::OB_SUCCESS;                                                \
    const static ObRpcPriority PR = prio;                                        \
    ObRpcOpts newopts = opts;                                                    \
    if (newopts.pr_ == ORPR_UNDEF) {                                             \
      newopts.pr_ = PR;                                                          \
    }                                                                            \
    newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
    newopts.local_addr_ = GCTX.self_addr_;                                       \
    ret = rpc_call(pcode, AOR_P_(InOut), &handle, newopts);                      \
    return ret;                                                                  \
  }

// Theoretically, stream rpc without argument or result is
// impossible. We add this SS0 interface just complete our rpc
// framework.
#define OB_DEFINE_RPC_SS0(name, pcode, prio)                                     \
  OB_RPC_STRUCT(pcode, (NoneT), NoneT)                                           \
  virtual int name(ORSSH_(pcode), OROP_)                                         \
  {                                                                              \
    int ret = common::OB_SUCCESS;                                                \
    const static ObRpcPriority PR = prio;                                        \
    ObRpcOpts newopts = opts;                                                    \
    if (newopts.pr_ == ORPR_UNDEF) {                                             \
      newopts.pr_ = PR;                                                          \
    }                                                                            \
    newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
    newopts.local_addr_ = GCTX.self_addr_;                                       \
    ret = rpc_call(pcode, &handle, newopts);                                     \
    return ret;                                                                  \
  }

#define OB_DEFINE_RPC_SS(prio, name, pcode, ...) \
  SELECT4(, ##__VA_ARGS__, OB_DEFINE_RPC_SS2, OB_DEFINE_RPC_SS1, OB_DEFINE_RPC_SS0)(name, pcode, prio, ##__VA_ARGS__)

#define RPC_SS(args...) _CONCAT(OB_DEFINE_RPC, _SS IGNORE_(args))

// define asynchronous interface
#define OB_DEFINE_RPC_AP2(name, pcode, prio, Input, Output)                        \
  OB_RPC_STRUCT(pcode, Input, Output)                                              \
  virtual int name(AOR_(Input), ORACB_(pcode), OROP_)                              \
  {                                                                                \
    const static ObRpcPriority PR = prio;                                          \
    int ret = common::OB_SUCCESS;                                                  \
    ObRpcOpts newopts = opts;                                                      \
    if (newopts.pr_ == ORPR_UNDEF) {                                               \
      newopts.pr_ = PR;                                                            \
    }                                                                              \
    if (mock_proxy_) {                                                             \
      mock_proxy_->set_server(dst_);                                               \
      ret = mock_proxy_->name(args, cb, newopts);                                  \
    } else {                                                                       \
      newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
      newopts.local_addr_ = GCTX.self_addr_;                                       \
      ret = rpc_post<ObRpc<pcode>>(args, cb, newopts);                             \
    }                                                                              \
    return ret;                                                                    \
  }

#define OB_DEFINE_RPC_AP1(name, pcode, prio, InOut)                                \
  OB_RPC_STRUCT(pcode, InOut, InOut)                                               \
  virtual int name(AP_AOR_(InOut) ORACB_(pcode), OROP_)                            \
  {                                                                                \
    const static ObRpcPriority PR = prio;                                          \
    int ret = common::OB_SUCCESS;                                                  \
    ObRpcOpts newopts = opts;                                                      \
    if (newopts.pr_ == ORPR_UNDEF) {                                               \
      newopts.pr_ = PR;                                                            \
    }                                                                              \
    if (mock_proxy_) {                                                             \
      mock_proxy_->set_server(dst_);                                               \
      ret = mock_proxy_->name(AP_AOR_P_(InOut) cb, newopts);                       \
    } else {                                                                       \
      newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
      newopts.local_addr_ = GCTX.self_addr_;                                       \
      ret = rpc_post<ObRpc<pcode>>(AP_AOR_P_(InOut) cb, newopts);                  \
    }                                                                              \
    return ret;                                                                    \
  }

#define OB_DEFINE_RPC_AP0(name, pcode, prio)                                       \
  OB_RPC_STRUCT(pcode, (NoneT), NoneT)                                             \
  virtual int name(ORACB_(pcode), OROP_)                                           \
  {                                                                                \
    const static ObRpcPriority PR = prio;                                          \
    int ret = common::OB_SUCCESS;                                                  \
    ObRpcOpts newopts = opts;                                                      \
    if (newopts.pr_ == ORPR_UNDEF) {                                               \
      newopts.pr_ = PR;                                                            \
    }                                                                              \
    if (mock_proxy_) {                                                             \
      mock_proxy_->set_server(dst_);                                               \
      ret = mock_proxy_->name(cb, newopts);                                        \
    } else {                                                                       \
      newopts.ssl_invited_nodes_ = GCONF._ob_ssl_invited_nodes.get_value_string(); \
      newopts.local_addr_ = GCTX.self_addr_;                                       \
      ret = rpc_post(pcode, cb, newopts);                                          \
    }                                                                              \
    return ret;                                                                    \
  }

#define OB_DEFINE_RPC_AP(prio, name, pcode, ...) \
  SELECT4(, ##__VA_ARGS__, OB_DEFINE_RPC_AP2, OB_DEFINE_RPC_AP1, OB_DEFINE_RPC_AP0)(name, pcode, prio, ##__VA_ARGS__)

#define RPC_AP(args...) _CONCAT(OB_DEFINE_RPC, _AP IGNORE_(args))

#endif  // _OB_RPC_PROXY_MACROS_H_
