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

#ifndef OCEANBASE_LOGSERVICE_LOG_RPC_PACKET_
#define OCEANBASE_LOGSERVICE_LOG_RPC_PACKET_

#include "lib/net/ob_addr.h"                            // ObAddr
#include "lib/utility/serialization.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"              // OB_UNIS_VERSION

namespace oceanbase
{
namespace palf
{
template <typename ReqType>
struct LogRpcPacketImpl {
public:
  LogRpcPacketImpl() : src_(),
                       palf_id_(),
                       req_() {}
  LogRpcPacketImpl(const common::ObAddr &src,
                   const int64_t palf_id,
                   const ReqType &req)
       : src_(src),
         palf_id_(palf_id),
         req_(req) {}
public:
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(src), K_(palf_id), K_(req));
public:
  common::ObAddr src_;
  int64_t palf_id_;
  ReqType req_;
};

template<typename ReqType>
DEFINE_SERIALIZE(LogRpcPacketImpl<ReqType>)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= buf_len || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(src_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, palf_id_))
             || OB_FAIL(req_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogRpcPacketImpl serialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

template<typename ReqType>
DEFINE_DESERIALIZE(LogRpcPacketImpl<ReqType>)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(src_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogRpcPacketImpl deserialize src failed", K(ret), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &palf_id_))) {
    PALF_LOG(ERROR, "LogRpcPacketImpl deserialize log_id failed", K(ret), K(new_pos));
  } else if (OB_FAIL(req_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogRpcPacketImpl deserialize req failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

template<typename ReqType>
DEFINE_GET_SERIALIZE_SIZE(LogRpcPacketImpl<ReqType>)
{
  int64_t size = 0;
  size += src_.get_serialize_size();
  size += serialization::encoded_length_i64(palf_id_);
  size += req_.get_serialize_size();
  return size;
}
} // end namespace obrpc
} // end namespace oceanbase

#endif
