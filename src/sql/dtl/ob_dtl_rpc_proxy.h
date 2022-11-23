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

#ifndef OB_DTL_RPC_PROXY_H
#define OB_DTL_RPC_PROXY_H

#include "lib/net/ob_addr.h"
#include "share/ob_scanner.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlRpcDataResponse
{
  OB_UNIS_VERSION(1);
public:
  ObDtlRpcDataResponse() : is_block_(false), recode_(OB_SUCCESS) {}
  TO_STRING_KV(K_(is_block));

public:
  bool is_block_;
  int recode_;
};

class ObDtlBCRpcDataResponse
{
  OB_UNIS_VERSION(1);
public:
  ObDtlBCRpcDataResponse() : resps_() {}
  TO_STRING_KV(K_(resps));

public:
  ObSEArray<ObDtlRpcDataResponse, 4> resps_;
};

struct ObDtlRpcChanArgs
{
  int64_t chid_;
  common::ObAddr peer_;

  TO_STRING_KV(K_(chid), K_(peer));
  OB_UNIS_VERSION(1);
};

class ObDtlSendArgs
{
public:
  ObDtlSendArgs() = default;
  ObDtlSendArgs(int64_t chid, const ObDtlLinkedBuffer &buffer)
      : chid_(chid), buffer_(buffer)
  {}
  int64_t chid_;
  ObDtlLinkedBuffer buffer_;
  TO_STRING_KV(K_(chid), KP(&buffer_));
private:
  OB_UNIS_VERSION(1);
};

class ObDtlBCSendArgs
{
public:
  ObDtlBCSendArgs()
      : args_(), bc_buffer_()
  {}
  ObSEArray<ObDtlSendArgs, 4> args_;
  ObDtlLinkedBuffer bc_buffer_;
  TO_STRING_KV(K(args_.count()));
private:
  OB_UNIS_VERSION(1);
};


}  // dtl
}  // sql

namespace obrpc {
class ObDtlRpcProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(ObDtlRpcProxy);

  // RPC_S(PR5 create_channel, OB_DTL_CREATE_CHANNEL, (ObDtlRpcChanArgs));
  // RPC_S(PR5 destroy_channel, OB_DTL_DESTROY_CHANNEL, (ObDtlRpcChanArgs));
  RPC_AP(@PR5 ap_send_message, OB_DTL_SEND, (sql::dtl::ObDtlSendArgs), sql::dtl::ObDtlRpcDataResponse);
  // use for broadcast
  RPC_AP(@PR5 ap_send_bc_message, OB_DTL_BC_SEND, (sql::dtl::ObDtlBCSendArgs), sql::dtl::ObDtlBCRpcDataResponse);
};

}  // obrpc
}  // oceanbase



#endif /* OB_DTL_RPC_PROXY_H */
