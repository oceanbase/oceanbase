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

#define USING_LOG_PREFIX  TRANS

#include "ob_weak_read_service.h"

#include "share/rc/ob_context.h"                             // WITH_CONTEXT
#include "share/rc/ob_tenant_base.h"                         // MTL
#include "share/config/ob_server_config.h"                   // ObServerConfig
#include "lib/container/ob_array.h"                          // ObArray
#include "lib/thread/ob_thread_name.h"
#include "observer/omt/ob_multi_tenant.h"                    // TenantIdList
#include "ob_weak_read_util.h"                               //ObWeakReadUtil
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace obrpc;
using namespace share;

namespace transaction
{
int ObWeakReadService::init(const rpc::frame::ObReqTransport *transport)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(wrs_rpc_.init(transport, *this))) {
    LOG_WARN("init tenant map fail", KR(ret));
  } else {
    inited_ = true;
    LOG_INFO("[WRS] weak read service init succ");
  }
  return ret;
}

void ObWeakReadService::destroy()
{
  LOG_INFO("[WRS] weak read service begin destroy");
  if (inited_) {
    stop();
    wait();
    inited_ = false;
  }
  LOG_INFO("[WRS] weak read service destroy succ");
}

int ObWeakReadService::start()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[WRS] weak read service thread start");
  return ret;
}

void ObWeakReadService::stop()
{
  LOG_INFO("[WRS] weak read service thread stop");
}

void ObWeakReadService::wait()
{
  LOG_INFO("[WRS] weak read service thread wait");
}

int ObWeakReadService::get_server_version(const uint64_t tenant_id, SCN &version) const
{
  int ret = OB_SUCCESS;
  // switch tenant
  MTL_SWITCH(tenant_id) {
    ObTenantWeakReadService *twrs = MTL(ObTenantWeakReadService *);
    if (OB_ISNULL(twrs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("MTL ObTenantWeakReadService object is NULL", K(twrs), K(tenant_id), KR(ret));
    } else {
      version = twrs->get_server_version();
    }
  } else {
    LOG_WARN("change tenant context fail when get weak read service server version",
        KR(ret), K(tenant_id));
  }

  if (OB_SUCC(ret) && !version.is_valid()) {
    int old_ret = ret;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get server version succ, but version is not valid snapshot version", K(ret), K(old_ret),
        K(tenant_id), K(version));
  }
  LOG_DEBUG("[WRS] get_server_version", K(ret), K(tenant_id), K(version));

  return ret;
}

int ObWeakReadService::get_cluster_version(const uint64_t tenant_id, SCN &version)
{
  int ret = OB_SUCCESS;
  // switch tenant
  MTL_SWITCH(tenant_id) {
    ObTenantWeakReadService *twrs = MTL(ObTenantWeakReadService *);
    if (OB_ISNULL(twrs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("MTL ObTenantWeakReadService object is NULL", K(twrs), K(tenant_id), KR(ret));
    } else if (OB_FAIL(twrs->get_cluster_version(version))) {
      LOG_WARN("get tenant weak read service cluster version fail", KR(ret), K(tenant_id));
    } else {
      // success
    }
  } else {
    LOG_WARN("change tenant context fail when get weak read service cluster version",
        KR(ret), K(tenant_id));
  }

  if (OB_SUCC(ret) && !version.is_valid()) {
    int old_ret = ret;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get cluster version succ, but version is not valid snapshot version", K(ret), K(old_ret),
        K(tenant_id), K(version));
  }
  LOG_INFO("[WRS] get_cluster_version", K(ret), K(tenant_id), K(version));

  return ret;
}

int ObWeakReadService::check_tenant_can_start_service(const uint64_t tenant_id,
                                                      bool &can_start_service,
                                                      SCN &version) const
{
  int ret = OB_SUCCESS;
  SCN gts_scn;
  ObLSID ls_id;
  SCN min_version;

  MTL_SWITCH(tenant_id) {
    ObTenantWeakReadService *twrs = MTL(ObTenantWeakReadService *);
    if (OB_ISNULL(twrs)) {
      ret = OB_ERR_UNEXPECTED;
      FLOG_ERROR("MTL ObTenantWeakReadService object is NULL", K(twrs), K(tenant_id), KR(ret));
    } else if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, NULL, gts_scn))) {
      FLOG_WARN("[WRS] [OBSERVER_NOTICE] get gts scn error", K(ret), K(tenant_id));
    } else if (OB_FAIL(twrs->check_can_start_service(gts_scn,
                                                     can_start_service,
                                                     min_version,
                                                     ls_id))) {
      FLOG_WARN("get tenant weak read service cluster version fail", KR(ret), K(tenant_id));
    } else if (!can_start_service) {
      version = min_version;
    } else {
      // success
    }
  } else {
    // tenant not exist
    can_start_service = true;
    FLOG_WARN("change tenant context fail when get weak read service cluster version",
        KR(ret), K(tenant_id));
  }

  if (can_start_service) {
    FLOG_INFO("[WRS] [OBSERVER_NOTICE] current tenant start service successfully",
        K(tenant_id),
        "target_ts", (gts_scn.is_valid() ? gts_scn.convert_to_ts() : 0),
        "min_ts", (min_version.is_valid() ? min_version.convert_to_ts() : 0),
        "delta_us", ((min_version.is_valid() && gts_scn.is_valid()) ? (gts_scn.convert_to_ts() - min_version.convert_to_ts()) : 0));
  } else {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      int64_t tmp_version = min_version.is_valid() ? min_version.convert_to_ts() : 0;
      FLOG_INFO("[WRS] [OBSERVER_NOTICE] waiting log replay... ",
          K(ret),
          K(tenant_id),
          K(can_start_service),
          "min_version", tmp_version,
          "delta", ObTimeUtility::current_time() - tmp_version,
          "slowest_ls_id", ls_id);
    }
  }
  return ret;
}

void ObWeakReadService::process_get_cluster_version_rpc(const uint64_t tenant_id,
    const obrpc::ObWrsGetClusterVersionRequest &req,
    obrpc::ObWrsGetClusterVersionResponse &res)
{
  int ret = OB_SUCCESS;
  SCN version;

  MTL_SWITCH(tenant_id) {
    ObTenantWeakReadService *twrs = NULL;
    if (OB_ISNULL(twrs = MTL(ObTenantWeakReadService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL ObTenantWeakReadService is NULL", KR(ret), K(twrs), K(tenant_id));
    } else if (OB_FAIL(twrs->process_get_cluster_version_rpc(version))) {
      LOG_WARN("tenant weak read service process get cluster version RPC fail", KR(ret),
          K(tenant_id), K(req), KPC(twrs));
    } else {
      // success
    }
  } else {
    LOG_WARN("change tenant context fail when process get cluster version RPC", KR(ret),
        K(tenant_id), K(req));
  }

  // set response
  res.set(ret, version);
}

void ObWeakReadService::process_cluster_heartbeat_rpc(const uint64_t tenant_id,
    const obrpc::ObWrsClusterHeartbeatRequest &req,
    obrpc::ObWrsClusterHeartbeatResponse &res)
{
  int ret = OB_SUCCESS;

  MTL_SWITCH(tenant_id) {
    ObTenantWeakReadService *twrs = NULL;
    if (OB_ISNULL(twrs = MTL(ObTenantWeakReadService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL ObTenantWeakReadService is NULL", KR(ret), K(twrs), K(tenant_id));
    } else if (OB_FAIL(twrs->process_cluster_heartbeat_rpc(req.req_server_, req.version_,
        req.valid_part_count_, req.total_part_count_, req.generate_timestamp_))) {
      LOG_WARN("tenant weak read service process cluster heartbeat RPC fail", KR(ret),
          K(tenant_id), K(req), KPC(twrs));
    } else {
      // success
    }
  } else {
    LOG_WARN("change tenant context fail when process cluster heartbeat RPC", KR(ret),
        K(tenant_id), K(req));
  }

  // set respnse
  res.set(ret);
}

void ObWeakReadService::process_cluster_heartbeat_rpc_cb(const uint64_t tenant_id,
    const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObWrsClusterHeartbeatResponse &res,
    const common::ObAddr &dst)
{
  int ret = OB_SUCCESS;

  MTL_SWITCH(tenant_id) {
    ObTenantWeakReadService *twrs = NULL;
    if (OB_ISNULL(twrs = MTL(ObTenantWeakReadService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL ObTenantWeakReadService is NULL", KR(ret), K(twrs), K(tenant_id));
    } else {
      // get response ret
      int ret_code = rcode.rcode_;
      if (OB_SUCCESS == ret_code) {
        ret_code = res.err_code_;
      }
      // handle response ret
      twrs->process_cluster_heartbeat_rpc_cb(rcode, dst);
    }
  } else {
    LOG_WARN("change tenant context fail when process cluster heartbeat rpc cb", KR(ret),
        K(tenant_id), K(res), K(rcode), K(dst));
  }
}

} // transaction
} // oceanbase
