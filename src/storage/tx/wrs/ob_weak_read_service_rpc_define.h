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

#ifndef OCEANBASE_TRANSACTION_OB_WEAK_READ_SERVICE_RPC_DEFINE_H_
#define OCEANBASE_TRANSACTION_OB_WEAK_READ_SERVICE_RPC_DEFINE_H_

#include "lib/utility/ob_unify_serialize.h"     // OB_UNIS_VERSION
#include "lib/net/ob_addr.h"                    // ObAddr
#include "rpc/obrpc/ob_rpc_proxy.h"             // ObRpcProxy
#include "rpc/obrpc/ob_rpc_proxy_macros.h"      // RPC_*
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace obrpc
{

struct ObWrsGetClusterVersionRequest
{
  common::ObAddr req_server_;

  ObWrsGetClusterVersionRequest() : req_server_() {}

  void set(const common::ObAddr &svr)
  {
    req_server_ = svr;
  }

  TO_STRING_KV(K_(req_server));

  OB_UNIS_VERSION(1);
};

struct ObWrsGetClusterVersionResponse
{
  int     err_code_;                // error code
  share::SCN version_;                 // weak read version
  int64_t version_duration_us_;     // weak read version duration time, for future 'version on Lease' compat

  ObWrsGetClusterVersionResponse() : err_code_(0), version_(), version_duration_us_(0) {}
  void set(const int err, const share::SCN version, const int64_t version_duration_us = 0)
  {
    err_code_ = err;
    version_ = version;
    version_duration_us_ = version_duration_us;
  }

  TO_STRING_KV(K_(err_code), K_(version), K_(version_duration_us));

  OB_UNIS_VERSION(1);
};

// Cluster Heartbeat Requst Struct
struct ObWrsClusterHeartbeatRequest
{
  common::ObAddr  req_server_;          // Who I am
  share::SCN        version_;             // What my weak read version is
  int64_t         valid_part_count_;    // How many valid partition I have
  int64_t         total_part_count_;    // How many partition I have
  int64_t         generate_timestamp_;  // server version generation timestamp

  ObWrsClusterHeartbeatRequest() :
      req_server_(),
      version_(),
      valid_part_count_(0),
      total_part_count_(0),
      generate_timestamp_(0)
  {}

  void set(const common::ObAddr &svr,
      const share::SCN version,
      const int64_t valid_part_count,
      const int64_t total_part_count,
      const int64_t generate_timestamp)
  {
    req_server_ = svr;
    version_ = version;
    valid_part_count_ = valid_part_count;
    total_part_count_ = total_part_count;
    generate_timestamp_ = generate_timestamp;
  }

  TO_STRING_KV(K_(req_server), K_(version), K_(valid_part_count), K_(total_part_count),
      K_(generate_timestamp));

  OB_UNIS_VERSION(1);
};

struct ObWrsClusterHeartbeatResponse
{
  int err_code_;

  void set(const int err_code) { err_code_ = err_code; }

  TO_STRING_KV(K_(err_code));

  OB_UNIS_VERSION(1);
};

class ObWrsRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObWrsRpcProxy);

  // get CLUSTER level weak read version
  RPC_S(PR1 get_weak_read_cluster_version, OB_WRS_GET_CLUSTER_VERSION,
      (ObWrsGetClusterVersionRequest), ObWrsGetClusterVersionResponse);

  // post Cluster level heartbeat request info, AP RPC
  RPC_AP(PR5 post_weak_read_cluster_heartbeat, OB_WRS_CLUSTER_HEARTBEAT,
      (ObWrsClusterHeartbeatRequest), ObWrsClusterHeartbeatResponse);
};

///////////////////////////////// RPC process functions /////////////////////////////////////
class ObWrsGetClusterVersionP : public ObWrsRpcProxy::Processor<OB_WRS_GET_CLUSTER_VERSION>
{
public:
  explicit ObWrsGetClusterVersionP(transaction::ObIWeakReadService *wrs) : wrs_(wrs)
  {}
  virtual ~ObWrsGetClusterVersionP() {}
protected:
  int process();
private:
  transaction::ObIWeakReadService *wrs_;
};

class ObWrsClusterHeartbeatP : public ObWrsRpcProxy::Processor<OB_WRS_CLUSTER_HEARTBEAT>
{
public:
  explicit ObWrsClusterHeartbeatP(transaction::ObIWeakReadService *wrs) : wrs_(wrs)
  {}
  virtual ~ObWrsClusterHeartbeatP() {}
protected:
  int process();
private:
  transaction::ObIWeakReadService *wrs_;
};

} // obrpc
} // oceanbase

#endif
