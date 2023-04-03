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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_NET_HANDLER_
#define OCEANBASE_RPC_OBRPC_OB_RPC_NET_HANDLER_

#include "lib/ob_define.h"
#include "rpc/frame/ob_req_handler.h"
#include "rpc/obrpc/ob_rpc_protocol_processor.h"
#include "rpc/obrpc/ob_rpc_compress_protocol_processor.h"

namespace oceanbase
{
namespace obrpc
{

class ObRpcNetHandler
    : public rpc::frame::ObReqHandler
{
public:
  ObRpcNetHandler()
  {
    ez_handler_.decode         = oceanbase::easy::decode;
    ez_handler_.encode         = oceanbase::easy::encode;
    ez_handler_.get_packet_id  = oceanbase::easy::get_packet_id;
    ez_handler_.set_trace_info = oceanbase::easy::set_trace_info;
    ez_handler_.on_connect     = oceanbase::easy::on_connect;
    ez_handler_.on_disconnect  = oceanbase::easy::on_disconnect;
    ez_handler_.new_keepalive_packet    = oceanbase::easy::new_keepalive_packet;
  }

  int try_decode_keepalive(easy_message_t *ms, int &result);
  void *decode(easy_message_t *m);
  int encode(easy_request_t *r, void *packet);
  uint64_t get_packet_id(easy_connection_t *c, void *packet);
  void set_trace_info(easy_request_t *req, void *packet);
  int on_connect(easy_connection_t *c);
  int on_disconnect(easy_connection_t *c);
  int on_idle(easy_connection_t *c);

public:
  static int64_t CLUSTER_ID;
  static uint64_t CLUSTER_NAME_HASH;
protected:
  char *easy_alloc(easy_pool_t *pool, int64_t size) const;
private:
  ObRpcProtocolProcessor rpc_processor_;
  ObRpcCompressProtocolProcessor rpc_compress_processor_;

  DISALLOW_COPY_AND_ASSIGN(ObRpcNetHandler);
}; // end of class ObRpcNetHandler

} // end of namespace obrpc
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_NET_HANDLER_
