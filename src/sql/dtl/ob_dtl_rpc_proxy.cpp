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

#include "ob_dtl_rpc_proxy.h"

namespace oceanbase {
namespace sql {
namespace dtl {
int64_t ObDtlRpcDataResponse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_block));
  J_OBJ_END();
  return pos;
}
int64_t ObDtlBCRpcDataResponse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(resps));
  J_OBJ_END();
  return pos;
}
int64_t ObDtlRpcChanArgs::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(chid), K_(peer));
  J_OBJ_END();
  return pos;
}
int64_t ObDtlSendArgs::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(chid), KP(&buffer_));
  J_OBJ_END();
  return pos;
}
int64_t ObDtlBCSendArgs::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(args_.count()));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObDtlRpcDataResponse, is_block_, recode_);
OB_SERIALIZE_MEMBER(ObDtlRpcChanArgs, chid_, peer_);
OB_SERIALIZE_MEMBER(ObDtlSendArgs, chid_, buffer_);
OB_SERIALIZE_MEMBER(ObDtlBCSendArgs, args_, bc_buffer_);
OB_SERIALIZE_MEMBER(ObDtlBCRpcDataResponse, resps_);

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase
