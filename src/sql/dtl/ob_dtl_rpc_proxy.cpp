/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_dtl_channel.h"
#include "ob_dtl_rpc_proxy.h"

namespace oceanbase {
namespace sql {
namespace dtl {

ObDtlSendArgs::ObDtlSendArgs() = default;

ObDtlSendArgs::ObDtlSendArgs(int64_t chid, const ObDtlLinkedBuffer &buffer)
    : chid_(chid), buffer_(buffer) {}

ObDtlBCSendArgs::ObDtlBCSendArgs() : args_(), bc_buffer_() {}

ObDtlBatchSendArgs::ObDtlBatchSendArgs()
    : arg_refs_(), args_(), is_pure_control_msg_(false), batch_buffer_() {}

OB_SERIALIZE_MEMBER(ObDtlRpcDataResponse, is_block_, recode_);
OB_SERIALIZE_MEMBER(ObDtlRpcChanArgs, chid_, peer_);
OB_SERIALIZE_MEMBER(ObDtlSendArgs, chid_, buffer_);
OB_SERIALIZE_MEMBER(ObDtlBCSendArgs, args_, bc_buffer_);
OB_SERIALIZE_MEMBER(ObDtlBCRpcDataResponse, resps_);
OB_DEF_SERIALIZE(ObDtlBatchSendArgs)
{
  int ret = OB_SUCCESS;
  int64_t count = arg_refs_.count();
  OB_UNIS_ENCODE(count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    OB_UNIS_ENCODE(arg_refs_.at(i).chid_);
    if (OB_SUCC(ret) && OB_ISNULL(arg_refs_.at(i).buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_DTL_LOG(WARN, "ref buf is null", K(ret), K(i));
    } else {
      OB_UNIS_ENCODE(*(arg_refs_.at(i).buf_));
    }
  }
  OB_UNIS_ENCODE(is_pure_control_msg_);
  OB_UNIS_ENCODE(batch_buffer_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDtlBatchSendArgs)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret) && OB_FAIL(args_.prepare_allocate(count))) {
    SQL_DTL_LOG(WARN, "prepare allocate failed", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    OB_UNIS_DECODE(args_.at(i).chid_);
    OB_UNIS_DECODE(args_.at(i).buffer_);
  }
  OB_UNIS_DECODE(is_pure_control_msg_);
  OB_UNIS_DECODE(batch_buffer_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDtlBatchSendArgs)
{
  int64_t len = 0;
  int64_t count = arg_refs_.count();
  OB_UNIS_ADD_LEN(count);
  for (int64_t i = 0; i < count; ++i) {
    const ObDtlSendArgRef &ref = arg_refs_.at(i);
    OB_UNIS_ADD_LEN(ref.chid_);
    if (OB_NOT_NULL(ref.buf_)) {
      OB_UNIS_ADD_LEN(*ref.buf_);
    }
  }
  OB_UNIS_ADD_LEN(is_pure_control_msg_);
  OB_UNIS_ADD_LEN(batch_buffer_);
  return len;
}
OB_SERIALIZE_MEMBER(ObDtlBatchRpcDataResponse, resps_, is_pure_control_msg_, recode_);

}  // dtl
}  // sql
}  // oceanbase
