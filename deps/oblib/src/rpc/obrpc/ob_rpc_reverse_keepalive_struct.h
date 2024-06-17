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

#ifndef OCEANBASE_OBSERVER_RPC_REVERSE_KEEPALIVE_STRUCT_H
#define OCEANBASE_OBSERVER_RPC_REVERSE_KEEPALIVE_STRUCT_H

#include "lib/utility/ob_unify_serialize.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace obrpc
{
struct ObRpcReverseKeepaliveArg
{
  OB_UNIS_VERSION(1);
public:
  ObRpcReverseKeepaliveArg()
   : dst_(), first_send_ts_(OB_INVALID_TIMESTAMP), pkt_id_(-1)
  {}
  ObRpcReverseKeepaliveArg(const ObAddr& dst, int64_t first_send_ts_, const int64_t pkt_id)
    : dst_(dst), first_send_ts_(first_send_ts_), pkt_id_(pkt_id)
  {}
  ~ObRpcReverseKeepaliveArg()
  {}
  bool is_valid() const
  {
    return dst_.is_valid() && pkt_id_ >= 0 && pkt_id_ <= INT32_MAX && first_send_ts_ > 0;
  }
  int assign(const ObRpcReverseKeepaliveArg &other);
  TO_STRING_KV(K_(dst), K_(pkt_id), K_(first_send_ts));

  ObAddr dst_;
  int64_t first_send_ts_;
  int64_t pkt_id_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcReverseKeepaliveArg);
};

struct ObRpcReverseKeepaliveResp
{
  OB_UNIS_VERSION(1);
public:
  ObRpcReverseKeepaliveResp() : ret_(OB_SUCCESS)
  {}
  ~ObRpcReverseKeepaliveResp()
  {}
  int assign(const ObRpcReverseKeepaliveResp &other)
  {
    ret_ = other.ret_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(ret));
  int32_t ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcReverseKeepaliveResp);
};
}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* !OCEANBASE_OBSERVER_RPC_REVERSE_KEEPALIVE_STRUCT_H */
