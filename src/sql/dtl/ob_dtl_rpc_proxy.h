/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  ObDtlSendArgs();
  ObDtlSendArgs(int64_t chid, const ObDtlLinkedBuffer &buffer);
  int64_t chid_;
  ObDtlLinkedBuffer buffer_;
  TO_STRING_KV(K_(chid), KP(&buffer_));
private:
  OB_UNIS_VERSION(1);
};

class ObDtlBCSendArgs
{
public:
  ObDtlBCSendArgs();
  ObSEArray<ObDtlSendArgs, 4> args_;
  ObDtlLinkedBuffer bc_buffer_;
  TO_STRING_KV(K(args_.count()));
private:
  OB_UNIS_VERSION(1);
};

// Send-path only: avoids copying ObDtlLinkedBuffer (~700B) per channel into the args array.
struct ObDtlSendArgRef {
  int64_t chid_;
  const ObDtlLinkedBuffer *buf_;  // non-owning, valid until ap_send_batch_message() returns
  TO_STRING_KV(K_(chid), KP(buf_));
};

class ObDtlBatchSendArgs
{
  OB_UNIS_VERSION(1);
public:
  ObDtlBatchSendArgs();
  // arg_refs_ and args_ serve different sides of the RPC: serialize reads arg_refs_,
  // deserialize writes args_, so receiver code needs no change.
  ObTMArray<ObDtlSendArgRef> arg_refs_;
  ObTMArray<ObDtlSendArgs>   args_;

  bool is_pure_control_msg_;
  ObDtlLinkedBuffer batch_buffer_;
  TO_STRING_KV(K(arg_refs_.count()), K(args_.count()), K(is_pure_control_msg_), KP(&batch_buffer_));
};

// Batch send response: per-channel response
class ObDtlBatchRpcDataResponse
{
  OB_UNIS_VERSION(1);
public:
  ObDtlBatchRpcDataResponse() : resps_(), is_pure_control_msg_(false), recode_() {}
  TO_STRING_KV(K_(resps));
public:
  ObTMArray<ObDtlRpcDataResponse> resps_;

  bool is_pure_control_msg_;
  int recode_;
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
  RPC_AP(@PR5 ap_send_batch_message, OB_DTL_BATCH_SEND, (sql::dtl::ObDtlBatchSendArgs), sql::dtl::ObDtlBatchRpcDataResponse);
};

}  // obrpc
}  // oceanbase



#endif /* OB_DTL_RPC_PROXY_H */
