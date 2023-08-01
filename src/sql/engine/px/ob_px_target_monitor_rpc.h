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

#ifndef __SQL_ENG_PX_TARGET_MONITOR_RPC_H__
#define __SQL_ENG_PX_TARGET_MONITOR_RPC_H__

#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{

struct ObPxRpcAddrTarget
{
  OB_UNIS_VERSION(1);
public:
  ObPxRpcAddrTarget() : addr_(), target_() {}
  ObPxRpcAddrTarget(ObAddr addr, int64_t target) : addr_(addr), target_(target) {}
  ObAddr addr_;
  int64_t target_;
  TO_STRING_KV(K_(addr), K_(target));
};

// The follower reports the usage of the local target to the leader, 
// and receives the current statistics of the global target usage of the leader
class ObPxRpcFetchStatArgs {
  OB_UNIS_VERSION(1);
public:
  ObPxRpcFetchStatArgs() : tenant_id_(OB_INVALID_ID), follower_version_(OB_INVALID_ID),
    addr_target_array_(), need_refresh_all_(false) {}
  ObPxRpcFetchStatArgs(uint64_t tenant_id, uint64_t ver, uint64_t refresh_all) :
    tenant_id_(tenant_id), follower_version_(ver),
    addr_target_array_(), need_refresh_all_(refresh_all) {}
  ~ObPxRpcFetchStatArgs() { }

  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() { return tenant_id_; }

  void set_version(uint64_t version) { follower_version_ = version; }
  uint64_t get_version() { return follower_version_; }
  void set_need_refresh_all(bool v) { need_refresh_all_ = v; }
  bool need_refresh_all() { return need_refresh_all_; }

  int push_local_target_usage(const ObAddr &server, int64_t local_usage)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(addr_target_array_.push_back(ObPxRpcAddrTarget(server, local_usage)))) {
      COMMON_LOG(WARN, "push_back addr failed", K(server), K(local_usage));
    }
    return ret;
  }

  TO_STRING_KV(K_(tenant_id), K_(follower_version));
public:
  uint64_t tenant_id_;
  // Check if the version_ of the follower and the leader match. 
  // If match, start to exchange statistics.
  // If don’t match, the follower needs to synchronize the version number of the leader and reinitialize the local statistics.
  uint64_t follower_version_;
  // The following structure was originally hashmap<ObAddr, int64_t>,
  // value represents the number of targets used since the last report
  // Because hashmap does not support serialization, so that it is transform to array
  ObSEArray<ObPxRpcAddrTarget, 100> addr_target_array_;
  bool need_refresh_all_;
};


class ObPxRpcFetchStatResponse {
  OB_UNIS_VERSION(1);
public:
  ObPxRpcFetchStatResponse() : addr_target_array_() {}
  ~ObPxRpcFetchStatResponse() { }

  void set_status(uint64_t status) { status_ = status; }
  uint64_t get_status() { return status_; }

  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() { return tenant_id_; }

  void set_version(uint64_t version) { leader_version_ = version; }
  uint64_t get_version() { return leader_version_; }

  int push_peer_target_usage(const ObAddr &server, int64_t peer_usage)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(addr_target_array_.push_back(ObPxRpcAddrTarget(server, peer_usage)))) {
      COMMON_LOG(WARN, "push_back addr failed", K(server), K(peer_usage));
    }
    return ret;
  }

  TO_STRING_KV(K_(status), K_(tenant_id), K_(leader_version));
public:
  // Used to notify the follower of the results of this report,
  // such as success, need to roll back (timeout/exception), not master
  uint64_t status_;
  uint64_t tenant_id_;
  // when the follower version number does not match the leader version number, it is used to push up the followor version number
  uint64_t leader_version_;
  // The following structure was originally hashmap<ObAddr, int64_t>,
  // value represents the the global target usage
  // Because hashmap does not support serialization, so that it is transform to array
  ObSEArray<ObPxRpcAddrTarget, 100> addr_target_array_;
};


}
}

#endif /* __SQL_ENG_PX_TARGET_MONITOR_RPC_H__ */
//// end of header file
