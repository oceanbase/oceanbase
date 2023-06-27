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

#include <stdint.h>
#include "share/scn.h"

#ifndef OCEANBASE_TRANSACTION_OB_I_WRS_SERVICE_
#define OCEANBASE_TRANSACTION_OB_I_WRS_SERVICE_

namespace oceanbase
{
namespace common { class ObAddr; }

namespace obrpc
{
class ObWrsGetClusterVersionRequest;
class ObWrsGetClusterVersionResponse;
class ObWrsClusterHeartbeatRequest;
class ObWrsClusterHeartbeatResponse;
struct ObRpcResultCode;
}

namespace transaction
{

// Weak Read Service Level
//
// "Weak Read Service" is a service of weakly consistent reading version
// that is, what level of weakly consistent reading version is provided
enum ObWeakReadServiceLevel
{
  WRS_LEVEL_CLUSTER = 0,    // CLUSTER level weak read, support monotonic reading across servers within the CLUSTER
  WRS_LEVEL_REGION = 1,     // [TODO] REGION level weak read, support monotonic reading across servers within the REGION
  WRS_LEVEL_ZONE = 2,       // [TODO] ZONE level weak read, support monotonic reading across servers within the ZONE

  WRS_LEVEL_SERVER = 10,    // SERVER level weak read, only guarantee monotonic reading in a single server
  WRS_LEVEL_MAX
};

// The maximum GAP time between MIN and MAX version when persist WRS version
// FIXME: Considering that follower readable version maybe stuck, WRS version can not be pushed casually
// Currently maintain <min_version, max_version>，when WRS service switch，max_version will be pushed up,
// That is, even all cluster readable version are stucked, WRS version will be pushed up when WRS service switch leader.
//
// For this reason, temporarily modify the GAP policy to ensure that the wrs version is not pushed up when the service is switched
//static const int64_t WRS_VERSION_GAP_FOR_PERSISTENCE = 1 * 1000 * 1000L;
static const int64_t WRS_VERSION_GAP_FOR_PERSISTENCE = 0;

const char *wrs_level_to_str(const int level);

class ObIWrsRpc;
class ObIWeakReadService
{
public:
  virtual ~ObIWeakReadService() {}

public:
  /// get SERVER level wrs version
  ///
  /// @param [in]  tenant_id   target tenant ID
  /// @param [in]  is_inner    is inner table or not
  /// @param [out] version     wrs version
  virtual int get_server_version(const uint64_t tenant_id, share::SCN &version) const = 0;

  /// get CLUSTER level wrs version
  ///
  /// @param [in]  tenant_id   target tenant ID
  /// @param [out] version     wrs version
  virtual int get_cluster_version(const uint64_t tenant_id, share::SCN &version) = 0;

  /// check tenant can start service or not
  virtual int check_tenant_can_start_service(const uint64_t tenant_id,
                                             bool &can_start_service,
                                             share::SCN &version) const = 0;

  /// get RPC instance
  virtual ObIWrsRpc &get_wrs_rpc() = 0;

  /// process get cluster version RPC
  virtual void process_get_cluster_version_rpc(const uint64_t tenant_id,
      const obrpc::ObWrsGetClusterVersionRequest &req,
      obrpc::ObWrsGetClusterVersionResponse &res) = 0;

  /// process cluster heartbeat RPC
  virtual void process_cluster_heartbeat_rpc(const uint64_t tenant_id,
      const obrpc::ObWrsClusterHeartbeatRequest &req,
      obrpc::ObWrsClusterHeartbeatResponse &res) = 0;

  /// process cluster heartbeat RPC callback
  virtual void process_cluster_heartbeat_rpc_cb(const uint64_t tenant_id,
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObWrsClusterHeartbeatResponse &res,
      const common::ObAddr &dst) = 0;

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
};

} // transaction
} // oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_I_WRS_SERVICE_
