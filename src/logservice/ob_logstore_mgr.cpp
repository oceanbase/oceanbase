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

#define USING_LOG_PREFIX CLOG
#include "ob_logstore_mgr.h"
#include "lib/time/ob_time_utility.h"           // ObTimeUtility
#include "lib/utility/ob_macro_utils.h"         // OB_UNLIKELY
#include "lib/utility/utility.h"                // ObTimeGuard
#include "share/ob_errno.h"                     // errno
#include "share/config/ob_server_config.h"      // GCONF
#include "rpc/obrpc/ob_rpc_net_handler.h"       // CLUSTER_ID
#include "grpc/ob_grpc_keepalive.h"             // grpc_keepalive
#include "src/logservice/ob_log_grpc_adapter.h"

namespace oceanbase
{
using namespace share;

namespace logservice
{

void LogstoreServiceInfo::reset()
{
  memory_limit_ = 0;
  memory_used_ = 0;
  shm_limit_ = 0;
  shm_used_ = 0;
}

ObLogstoreMgr::ObLogstoreMgr()
    : logstore_service_addr_(),
      grpc_adapter_(NULL),
      logstore_compatible_version_(-1),
      is_inited_(false)
{
}

ObLogstoreMgr::~ObLogstoreMgr()
{
  destroy();
}

int ObLogstoreMgr::init()
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  char *buf = nullptr;
  common::ObAddr logstore_service_addr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(get_logstore_service_addr_(logstore_service_addr))) {
    CLOG_LOG(WARN, "get_logstore_service_addr_ failed", K(ret), KPC(this));
  } else if (!logstore_service_addr.is_valid()) {
    // logstore_service_addr is invalid, skip init
  } else if (FALSE_IT(grpc_adapter_ = OB_NEW(ObLogGrpcAdapter, "GRPC"))) {
  } else if (OB_ISNULL(grpc_adapter_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "fail to allocate memory", K(ret));
  } else if (OB_FAIL(grpc_adapter_->init(logstore_service_addr, GRPC_TIMEOUT_US, cluster_id))) {
    CLOG_LOG(ERROR, "fail to init grpc_adapter_", K(ret), K(logstore_service_addr), K(cluster_id));
  } else {
    logstore_service_addr_ = logstore_service_addr;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogstoreMgr init success", KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObLogstoreMgr::destroy()
{
  CLOG_LOG_RET(WARN, OB_SUCCESS, "ObLogstoreMgr destroy", KPC(this));
  is_inited_ = false;
  logstore_compatible_version_ = -1;
  logstore_service_addr_.reset();
  if (NULL != grpc_adapter_) {
    OB_DELETE(ObLogGrpcAdapter, "GRPC", grpc_adapter_);
    grpc_adapter_ = NULL;
  }
}

int ObLogstoreMgr::get_logstore_service_status(ObAddr &logstore_service_addr, bool &is_active, int64_t &last_active_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_logstore_service_addr_(logstore_service_addr))) {
    CLOG_LOG(ERROR, "get_logstore_service_addr_ failed", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool is_in_blacklist =false;
    if (OB_SUCCESS != (tmp_ret = obgrpc::get_grpc_ka_instance().in_black(logstore_service_addr,
      is_in_blacklist, last_active_ts))) {
      // in_black may return err code(-4016) when arg server has not been detected.
      // we can ignore err code here, and keep default value for is_active.
      CLOG_LOG(WARN, "grpc client in_black failed", K(tmp_ret), K(logstore_service_addr));
    }
    is_active = !is_in_blacklist;
    CLOG_LOG(INFO, "get_logstore_service_status", K(ret), K(logstore_service_addr), K(is_active), K(last_active_ts));
  }
  return ret;
}

int ObLogstoreMgr::get_logstore_service_info(LogstoreServiceInfo &logstore_info)
{
  int ret = OB_SUCCESS;
  newlogstorepb::GetLogStoreInfoReq req;
  newlogstorepb::GetLogStoreInfoResp resp;
  req.set_epoch(1);  // mock unused epoch
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(grpc_adapter_->get_log_store_info(req, resp))) {
    CLOG_LOG(ERROR, "grpc call get_log_store_info failed", K(ret));
  } else {
    // 除了网络错误，get_log_store_info 预期只会返回OB_SUCCESS
    assert(OB_SUCCESS == resp.ret_code());
    logstore_info.memory_limit_ = resp.memory_limit();
    logstore_info.memory_used_ = resp.memory_used();
    logstore_info.shm_limit_ = resp.shm_limit();
    logstore_info.shm_used_ = resp.shm_used();
    CLOG_LOG(INFO, "get_logstore_service_info success", K(ret), K(logstore_info));
  }
  return ret;
}

int ObLogstoreMgr::get_logstore_service_addr_(common::ObAddr &logstore_addr)
{
  int ret = OB_SUCCESS;
  const ObString &logstore_addr_str = GCONF._ob_logstore_service_addr.str();
  if (logstore_addr_str.empty()) {
    CLOG_LOG(WARN, "invalid config param _ob_logstore_service_addr", K(ret), K(logstore_addr_str));
  } else if (OB_FAIL(logstore_addr.parse_from_string(logstore_addr_str))) {
    CLOG_LOG(ERROR, "failed to parse from string", K(ret), K(logstore_addr_str));
  } else {
    // do nothing
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
