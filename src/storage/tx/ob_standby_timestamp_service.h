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

#ifndef OCEANBASE_TRANSACTION_OB_STANDBY_TIMESTAMP_SERVICE_
#define OCEANBASE_TRANSACTION_OB_STANDBY_TIMESTAMP_SERVICE_

#include "lib/thread/thread_mgr_interface.h" 
#include "ob_gts_rpc.h"
#include "logservice/ob_log_base_type.h"

namespace oceanbase
{

namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}

namespace common
{
class ObMySQLProxy;
}

namespace obrpc
{
class ObGtsRpcResult;
}

namespace transaction
{
class ObGtsRequest;

class ObStandbyTimestampService : public logservice::ObIRoleChangeSubHandler, public lib::TGRunnable
{
public:
  ObStandbyTimestampService() : inited_(false), last_id_(OB_INVALID_VERSION), tenant_id_(OB_INVALID_ID),
                                epoch_(OB_INVALID_TIMESTAMP), tg_id_(-1),
                                switch_to_leader_ts_(OB_INVALID_TIMESTAMP),
                                print_error_log_interval_(3 * 1000 * 1000),
                                print_id_log_interval_(3 * 1000 * 1000) {}
  virtual ~ObStandbyTimestampService() { destroy(); }

  int init(rpc::frame::ObReqTransport *req_transport);
  static int mtl_init(ObStandbyTimestampService *&sts);
  int start();
  void stop();
  void wait();
  void destroy();

  void run1();

  int switch_to_follower_gracefully();
  void switch_to_follower_forcedly();
  int resume_leader();
  int switch_to_leader();
  
  int handle_request(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result);
  int check_leader(bool &leader);
  int get_number(int64_t &gts);
  int64_t get_last_id() const { return last_id_; }
  void get_virtual_info(int64_t &ts_value, common::ObRole &role, int64_t &proposal_id);
  TO_STRING_KV(K_(inited), K_(last_id), K_(tenant_id), K_(epoch), K_(self), K_(switch_to_leader_ts));
private:
  int query_and_update_last_id();
  int handle_local_request_(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result);
private:
  bool inited_;
  int64_t last_id_;
  uint64_t tenant_id_;
  int64_t epoch_;
  int tg_id_;
  ObGtsResponseRpc rpc_;
  common::ObAddr self_;
  int64_t switch_to_leader_ts_;
  common::ObTimeInterval print_error_log_interval_;
  common::ObTimeInterval print_id_log_interval_;
};


}
}
#endif
