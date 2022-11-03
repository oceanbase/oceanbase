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

#ifndef OCEANBASE_RPC_OB_BLACKLIST_PROXY_H_
#define OCEANBASE_RPC_OB_BLACKLIST_PROXY_H_
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace obrpc
{

struct ObBlacklistReq
{
  OB_UNIS_VERSION_V(1);
public:
  ObBlacklistReq() { reset(); }
  ObBlacklistReq(const common::ObAddr &sender, const int64_t send_ts)
    : sender_(sender), send_timestamp_(send_ts) {}
  ~ObBlacklistReq() { reset(); }

  void reset();
  bool is_valid() const { return sender_.is_valid(); }
  const common::ObAddr &get_sender() const { return sender_; }
  int64_t get_send_timestamp() const { return send_timestamp_; }

  TO_STRING_KV(K_(sender), K_(send_timestamp));

  common::ObAddr sender_;
  int64_t send_timestamp_;
};

struct ObBlacklistResp
{
  OB_UNIS_VERSION_V(1);
public:
  ObBlacklistResp() { reset(); }
  ObBlacklistResp(const common::ObAddr &sender, const int64_t req_send_ts,
                  const int64_t recv_ts, const int64_t server_start_time)
    : sender_(sender), req_send_timestamp_(req_send_ts),
      req_recv_timestamp_(recv_ts), server_start_time_(server_start_time) {}
  ~ObBlacklistResp() { reset(); }

  void reset();
  bool is_valid() const { return sender_.is_valid(); }
  const common::ObAddr &get_sender() const { return sender_; }
  int64_t get_req_send_timestamp() const { return req_send_timestamp_; }
  int64_t get_req_recv_timestamp() const { return req_recv_timestamp_; }
  int64_t get_server_start_time() const { return server_start_time_; }

  TO_STRING_KV(K_(sender), K_(req_send_timestamp), K_(req_recv_timestamp));

  common::ObAddr sender_;
  int64_t req_send_timestamp_;
  int64_t req_recv_timestamp_;
  int64_t server_start_time_;
};

class ObBlacklistRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObBlacklistRpcProxy);
  RPC_AP(PR1 post_request, OB_SERVER_BLACKLIST_REQ, (ObBlacklistReq));
  RPC_AP(PR1 post_response, OB_SERVER_BLACKLIST_RESP, (ObBlacklistResp));
  int send_req(const common::ObAddr &dst, const int64_t dst_cluster_id, const ObBlacklistReq &req);
  int send_resp(const common::ObAddr &dst, const int64_t dst_cluster_id, const ObBlacklistResp &resp);
private:
  static const int64_t BLACK_LIST_MSG_TIMEOUT = 10 * 1000 * 1000;
};

};
};


#endif /* OCEANBASE_RPC_OB_BLACKLIST_PROXY_H_ */

