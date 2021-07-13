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

#ifndef _OCEABASE_OBSERVER_OB_SRV_NETWORK_FRAME_H_
#define _OCEABASE_OBSERVER_OB_SRV_NETWORK_FRAME_H_

#include "rpc/frame/ob_net_easy.h"
#include "rpc/frame/ob_req_handler.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "observer/mysql/obsm_handler.h"
#include "observer/ob_srv_xlator.h"
#include "observer/ob_srv_deliver.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace rpc {
namespace frame {
class ObReqTranslator;
}
}  // namespace rpc
}  // namespace oceanbase

namespace oceanbase {
namespace observer {

class ObSrvNetworkFrame {
public:
  enum { NET_IO_NORMAL_GID = 0, NET_IO_HP_GID = 64, NET_IO_BATCH_GID = 72 };
  explicit ObSrvNetworkFrame(ObGlobalContext& gctx);

  virtual ~ObSrvNetworkFrame();

  int init();
  void destroy();
  int start();
  int mysql_shutdown();
  int rpc_shutdown();
  int high_prio_rpc_shutdown();
  int batch_rpc_shutdown();
  void wait();
  int stop();

  int reload_config();
  int reload_ssl_config();
  static uint64_t get_ssl_file_hash(bool& file_exist);
  ObSrvDeliver& get_deliver()
  {
    return deliver_;
  }
  int get_proxy(obrpc::ObRpcProxy& proxy);
  rpc::frame::ObReqTransport* get_req_transport();
  rpc::frame::ObReqTransport* get_high_prio_req_transport();
  rpc::frame::ObReqTransport* get_batch_rpc_req_transport();
  inline rpc::frame::ObReqTranslator& get_xlator();

private:
  ObGlobalContext& gctx_;

  ObSrvXlator xlator_;
  rpc::frame::ObReqQHandler request_qhandler_;

  // generic deliver
  ObSrvDeliver deliver_;

  // rpc handler
  obrpc::ObRpcHandler rpc_handler_;
  ObSMHandler mysql_handler_;

  rpc::frame::ObNetEasy net_;
  rpc::frame::ObReqTransport* rpc_transport_;
  rpc::frame::ObReqTransport* high_prio_rpc_transport_;
  rpc::frame::ObReqTransport* mysql_transport_;
  rpc::frame::ObReqTransport* batch_rpc_transport_;
  uint64_t last_ssl_info_hash_;

  DISALLOW_COPY_AND_ASSIGN(ObSrvNetworkFrame);
};  // end of class ObSrvNetworkFrame

// inline functions
inline rpc::frame::ObReqTranslator& ObSrvNetworkFrame::get_xlator()
{
  return xlator_;
}

}  // end of namespace observer
}  // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_SRV_NETWORK_FRAME_H_ */
