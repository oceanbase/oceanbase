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
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "observer/mysql/obsm_handler.h"/*  */
#include "observer/ob_srv_xlator.h"
#include "observer/ob_srv_deliver.h"
#include "observer/ob_server_struct.h"
#include "observer/net/ob_ingress_bw_alloc_service.h"
#include "observer/ob_srv_rpc_handler.h"

namespace oceanbase {
namespace rpc {
namespace frame {
class ObReqTranslator;
}}}

namespace oceanbase
{
namespace observer
{

class ObSrvNetworkFrame
{
public:
  enum { NET_IO_NORMAL_GID = 0, NET_IO_HP_GID = 64, NET_IO_BATCH_GID = 72 };
  explicit ObSrvNetworkFrame(ObGlobalContext &gctx);

  virtual ~ObSrvNetworkFrame();

  int init();
  void destroy();
  int start();
  int mysql_shutdown();
  int rpc_shutdown();
  int high_prio_rpc_shutdown();
  int batch_rpc_shutdown();
  int unix_rpc_shutdown();
  void sql_nio_stop();
  void rpc_stop();
  void wait();
  int stop();

  int reload_config();
  int reload_ssl_config();
  static int extract_expired_time(const char *const cert_file, int64_t &expired_time);
  static uint64_t get_ssl_file_hash(const char *intl_file[3], const char *sm_file[5], bool &file_exist);
  ObSMHandler &get_mysql_handler() { return mysql_handler_; }
  ObSrvDeliver& get_deliver() { return deliver_; }
  int get_proxy(obrpc::ObRpcProxy &proxy);
  rpc::frame::ObReqTransport *get_req_transport();
  rpc::frame::ObReqTransport *get_high_prio_req_transport();
  rpc::frame::ObReqTransport *get_batch_rpc_req_transport();
  inline rpc::frame::ObReqTranslator &get_xlator();
  rpc::frame::ObNetEasy *get_net_easy();
  void set_ratelimit_enable(int ratelimit_enabled);
  int reload_sql_thread_config();
  int reload_tenant_sql_thread_config(const uint64_t tenant_id);

  int reload_mysql_login_thread_config() {
    int cnt = deliver_.get_mysql_login_thread_count_to_set(static_cast<int32_t>(GCONF.sql_login_thread_count));
    return deliver_.set_mysql_login_thread_count(cnt);
  }
  static int reload_rpc_auth_method();

  rootserver::ObIngressBWAllocService *get_ingress_service();
  int net_endpoint_register(const ObNetEndpointKey &endpoint_key, int64_t expire_time);
  int net_endpoint_predict_ingress(const ObNetEndpointKey &endpoint_key, int64_t &predicted_bw);
  int net_endpoint_set_ingress(const ObNetEndpointKey &endpoint_key, int64_t assigned_bw);

private:
  ObGlobalContext &gctx_;

  ObSrvXlator xlator_;
  rpc::frame::ObReqQHandler request_qhandler_;

  // generic deliver
  ObSrvDeliver deliver_;

  // rpc handler
  ObSrvRpcHandler rpc_handler_;
  ObSMHandler mysql_handler_;
  rootserver::ObIngressBWAllocService ingress_service_;

  rpc::frame::ObNetEasy net_;
  rpc::frame::ObReqTransport *rpc_transport_;
  rpc::frame::ObReqTransport *high_prio_rpc_transport_;
  rpc::frame::ObReqTransport *mysql_transport_;
  rpc::frame::ObReqTransport *batch_rpc_transport_;
  uint64_t last_ssl_info_hash_;
  int64_t standby_fetchlog_bw_limit_;
  uint64_t standby_fetchlog_bytes_;
  int64_t standby_fetchlog_time_;

  DISALLOW_COPY_AND_ASSIGN(ObSrvNetworkFrame);
}; // end of class ObSrvNetworkFrame

// inline functions
inline
rpc::frame::ObReqTranslator &
ObSrvNetworkFrame::get_xlator() {
  return xlator_;
}

static int get_default_net_thread_count()
{
  int cnt = 1;
  int cpu_num = static_cast<int>(get_cpu_count());

  if (cpu_num <= 4) {
    cnt = 1;
  } else if (cpu_num <= 8) {
    cnt = 2;
  } else if (cpu_num <= 16) {
    cnt = 4;
  } else if (cpu_num <= 32) {
    cnt = 7;
  } else {
    cnt = max(8, cpu_num / 6);
  }
  return cnt;
}

} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_SRV_NETWORK_FRAME_H_ */
