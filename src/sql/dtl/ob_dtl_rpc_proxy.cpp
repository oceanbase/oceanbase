/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_dtl_rpc_proxy.h"

namespace oceanbase {
namespace sql {
namespace dtl {

OB_SERIALIZE_MEMBER(ObDtlRpcDataResponse, is_block_, recode_);
OB_SERIALIZE_MEMBER(ObDtlRpcChanArgs, chid_, peer_);
OB_SERIALIZE_MEMBER(ObDtlSendArgs, chid_, buffer_);
OB_SERIALIZE_MEMBER(ObDtlBCSendArgs, args_, bc_buffer_);
OB_SERIALIZE_MEMBER(ObDtlBCRpcDataResponse, resps_);

}  // dtl
}  // sql
}  // oceanbase
