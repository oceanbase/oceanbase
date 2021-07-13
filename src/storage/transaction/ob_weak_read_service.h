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

#ifndef OCEANBASE_TRANSACTION_OB_WRS_SERVICE_
#define OCEANBASE_TRANSACTION_OB_WRS_SERVICE_

#include "share/ob_thread_pool.h"          // ObThreadPool
#include "rpc/obrpc/ob_rpc_result_code.h"  // ObRpcResultCode
#include "lib/net/ob_addr.h"               // ObAddr

#include "ob_i_weak_read_service.h"           // ObIWeakReadService
#include "ob_tenant_weak_read_service.h"      // ObTenantWeakReadService
#include "ob_weak_read_service_rpc_define.h"  // obrpc::
#include "ob_weak_read_service_rpc.h"         // ObWrsRpc

namespace oceanbase {
namespace storage {
class ObIPartitionGroup;
}
namespace rpc {
namespace frame {
class ObReqTransport;
}
}  // namespace rpc

namespace transaction {

class ObWeakReadService : public ObIWeakReadService, public share::ObThreadPool {
public:
  ObWeakReadService() : inited_(false), wrs_rpc_(), server_version_epoch_tstamp_(0)
  {}
  ~ObWeakReadService()
  {
    destroy();
  }
  int init(const rpc::frame::ObReqTransport* transport);
  void destroy();
  int start();
  void stop();
  void wait();

public:
  void run1();

public:
  /// get SERVER level weak read version
  int get_server_version(const uint64_t tenant_id, int64_t& version) const;
  /// get CLUSTER level weak read version
  int get_cluster_version(const uint64_t tenant_id, int64_t& version);

  void check_server_can_start_service(bool& can_start_service, int64_t& min_wrs) const;

  ///////////// RPC process functions /////////////////
  void process_get_cluster_version_rpc(const uint64_t tenant_id, const obrpc::ObWrsGetClusterVersionRequest& req,
      obrpc::ObWrsGetClusterVersionResponse& res);

  void process_cluster_heartbeat_rpc(const uint64_t tenant_id, const obrpc::ObWrsClusterHeartbeatRequest& req,
      obrpc::ObWrsClusterHeartbeatResponse& res);

  void process_cluster_heartbeat_rpc_cb(const uint64_t tenant_id, const obrpc::ObRpcResultCode& rcode,
      const obrpc::ObWrsClusterHeartbeatResponse& res, const common::ObAddr& dst);

  ObIWrsRpc& get_wrs_rpc()
  {
    return wrs_rpc_;
  }

private:
  int update_server_version_epoch_tstamp_(const int64_t cur_time);
  int scan_all_partitions_(int64_t& valid_user_part_count, int64_t& skip_user_part_count,
      int64_t& valid_inner_part_count, int64_t& skip_inner_part_count);
  int handle_partition_(storage::ObIPartitionGroup& part, bool& need_skip, bool& is_user_part);
  int get_tenant_service_(const uint64_t tenant_id, ObTenantWeakReadService*& wrts);
  int handle_all_tenant_();

private:
  bool inited_;
  ObWrsRpc wrs_rpc_;

  // epoch of current server level weak read version
  // update with every partition scan, increase only
  //
  int64_t server_version_epoch_tstamp_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_WRS_SERVICE_
