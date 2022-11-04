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

#define USING_LOG_PREFIX TABLELOCK

#include "ob_table_lock_rpc_client.h"
#include "share/ob_ls_id.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_srv_network_frame.h"

namespace oceanbase
{
using namespace share;

namespace transaction
{
namespace tablelock
{

static inline int get_ls_leader(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const int64_t abs_timeout_ts,
    ObAddr &addr)
{
  int ret = OB_SUCCESS;
  ObLocationService *location_service = GCTX.location_service_;
  if (OB_ISNULL(location_service)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location_service not inited", K(ret));
  } else if (OB_FAIL(location_service->get_leader_with_retry_until_timeout(
      cluster_id,
      tenant_id,
      ls_id,
      addr,
      abs_timeout_ts))) {
    LOG_WARN("failed to get ls leader with retry until timeout",
        K(ret), K(cluster_id), K(tenant_id), K(ls_id), K(addr), K(abs_timeout_ts));
  } else {
    LOG_DEBUG("get ls leader from location_service",
        K(ret), K(cluster_id), K(tenant_id), K(ls_id), K(addr), K(abs_timeout_ts));
  }
  return ret;
}

int ObTableLockRpcClient::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_lock_rpc_proxy_.init(GCTX.net_frame_->get_req_transport(),
                                         GCTX.self_addr()))) {
    LOG_WARN("failed to init rpc proxy", K(ret));
  }
  return ret;
}

ObTableLockRpcClient &ObTableLockRpcClient::get_instance()
{
  static ObTableLockRpcClient instance_;
  return instance_;
}

int ObTableLockRpcClient::lock_table(
    const uint64_t table_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader;
  int64_t rpc_timeout_us = (0 == timeout_us) ? DEFAULT_TIMEOUT_US : timeout_us;
  int64_t abs_timeout_ts = ObTimeUtility::current_time() + rpc_timeout_us;
  if(!is_lock_mode_valid(lock_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lock_mode));
  } else if (OB_FAIL(get_ls_leader(
      GCONF.cluster_id,
      tenant_id,
      LOCK_SERVICE_LS,
      abs_timeout_ts,
      leader))) {
    LOG_WARN("fail to get leader", K(ret), K(tenant_id), K(LOCK_SERVICE_LS), K(abs_timeout_ts), K(leader));
  } else {
    ObOutTransLockTableRequest arg;
    arg.table_id_ = table_id;
    arg.lock_mode_ = lock_mode;
    arg.lock_owner_ = lock_owner;
    arg.timeout_us_ = timeout_us;
    if (OB_FAIL(table_lock_rpc_proxy_.to(leader)
                                      .by(tenant_id)
                                      .timeout(rpc_timeout_us)
                                      .lock_table(arg))) {
      LOG_WARN("lock_table rpc request failed", K(ret), K(arg), K(leader),
               K(tenant_id), K(rpc_timeout_us));
    }
  }

  LOG_DEBUG("finish ObTableLockRpcClient ::lock_table", K(ret));
  return ret;
}

int ObTableLockRpcClient::unlock_table(
    const uint64_t table_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader;
  int64_t rpc_timeout_us = (0 == timeout_us) ? DEFAULT_TIMEOUT_US : timeout_us;
  int64_t abs_timeout_ts = ObTimeUtility::current_time() + rpc_timeout_us;
  if(!is_lock_mode_valid(lock_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lock_mode));
  } else if (OB_FAIL(get_ls_leader(
      GCONF.cluster_id,
      tenant_id,
      LOCK_SERVICE_LS,
      abs_timeout_ts,
      leader))) {
    LOG_WARN("fail to get leader", K(ret), K(tenant_id), K(LOCK_SERVICE_LS), K(leader), K(abs_timeout_ts));
  } else {
    ObOutTransUnLockTableRequest arg;
    arg.table_id_ = table_id;
    arg.lock_mode_ = lock_mode;
    arg.lock_owner_ = lock_owner;
    arg.timeout_us_ = timeout_us;
    if (OB_FAIL(table_lock_rpc_proxy_.to(leader)
                                     .by(tenant_id)
                                     .timeout(rpc_timeout_us)
                                     .unlock_table(arg))) {
      LOG_WARN("unlock_table rpc request failed", K(ret), K(arg), K(leader),
               K(tenant_id), K(rpc_timeout_us));
    }
  }

  LOG_DEBUG("finish ObTableLockRpcClient ::unlock_table", K(ret));
  return ret;
}

}
}
}
