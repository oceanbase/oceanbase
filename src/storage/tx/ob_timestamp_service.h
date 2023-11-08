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

#ifndef OCEANBASE_TRANSACTION_OB_TIMESTAMP_SERVICE_
#define OCEANBASE_TRANSACTION_OB_TIMESTAMP_SERVICE_

#include "ob_id_service.h"
#include "ob_gts_rpc.h"
#include "logservice/palf/election/utils/election_common_define.h"

namespace oceanbase
{

namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}

namespace obrpc
{
class ObGtsRpcResult;
}

namespace transaction
{
class ObGtsRequest;

class ObTimestampService : public ObIDService
{
public:
  ObTimestampService() {}
  ~ObTimestampService() {}
  int init(rpc::frame::ObReqTransport *req_transport);
  static int mtl_init(ObTimestampService *&timestamp_service);
  int start() { return rpc_.start(); }
  void stop() { rpc_.stop(); }
  void wait() { rpc_.wait(); }
  void destroy()
  {
    reset();
    rpc_.destroy();
  }
  // nano second
  static const int64_t TIMESTAMP_PREALLOCATED_RANGE = palf::election::MAX_LEASE_TIME * 1000;
  static const int64_t PREALLOCATE_RANGE_FOR_SWITHOVER =  2 * TIMESTAMP_PREALLOCATED_RANGE;
  int handle_request(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result);
  int get_timestamp(int64_t &gts);
  int switch_to_follower_gracefully();
  void switch_to_follower_forcedly();
  int resume_leader();
  int switch_to_leader();
  int64_t get_limited_id() const { return limited_id_; }
  static SCN get_sts_start_scn(const SCN &max_sys_ls_scn)
  { return SCN::plus(max_sys_ls_scn, PREALLOCATE_RANGE_FOR_SWITHOVER); };
  void get_virtual_info(int64_t &ts_value, common::ObRole &role, int64_t &proposal_id);
private:
  ObGtsResponseRpc rpc_;
  // last timestamp retrieved from gts leader，updated periodically, nanosecond
  int64_t last_gts_;
  // the time of last request，updated periodically, nanosecond
  int64_t last_request_ts_;
  // the lock of checking the gts service's advancing speed, used in get_timestamp to avoid
  // concurrent threads all pushing the gts ahead
  int64_t check_gts_speed_lock_;
  int handle_local_request_(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result);
};

}
}
#endif
