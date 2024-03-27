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

#ifndef OCEANBASE_OBSERVER_RPC_REVERSE_KEEPALIVE_H
#define OCEANBASE_OBSERVER_RPC_REVERSE_KEEPALIVE_H

#include "observer/ob_rpc_processor_simple.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class ObRpcReverseKeepAliveService
{
public:
  ObRpcReverseKeepAliveService() : init_(false), rpc_proxy_(NULL), map_attr_(), rpc_pkt_id_map_() {}
  int init(ObSrvRpcProxy *srv_rpc_proxy);
  int destroy();
  // called from the RPC receiver that needs to probe
  int receiver_probe(const ObRpcReverseKeepaliveArg& reverse_keepalive_arg);
  int sender_register(const int64_t pkt_id, int64_t pcode);
  int sender_unregister(const int64_t pkt_id);
  int check_status(const int64_t send_ts, const int64_t pkt_id);
  bool init_;
public:
  struct RpcPktID
  {
    int64_t rpc_pkt_id_;
    RpcPktID(const int64_t id) : rpc_pkt_id_(id) {}
    int64_t hash() const
    {
      return rpc_pkt_id_;
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    bool operator== (const RpcPktID &other) const
    {
      return rpc_pkt_id_ == other.rpc_pkt_id_;
    }
    TO_STRING_KV(K_(rpc_pkt_id));
  };
private:
  ObSrvRpcProxy *rpc_proxy_;
  ObMemAttr map_attr_;
  common::ObLinearHashMap<RpcPktID, int64_t> rpc_pkt_id_map_;
};
extern ObRpcReverseKeepAliveService rpc_reverse_keepalive_instance;
}; // end namespace obrpc
namespace observer
{
OB_DEFINE_PROCESSOR_S(Srv, OB_RPC_REVERSE_KEEPALIVE, ObRpcReverseKeepaliveP);

}; // end of namespace observer
}; // end namespace oceanbase

#endif /* !OCEANBASE_OBSERVER_RPC_REVERSE_KEEPALIVE_H */
