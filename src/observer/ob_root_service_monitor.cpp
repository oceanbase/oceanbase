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

#define USING_LOG_PREFIX SERVER

#include "ob_root_service_monitor.h"

#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "rootserver/ob_root_service.h"
#include "observer/ob_server_struct.h"
#include "share/ob_server_status.h"
#include "lib/thread/ob_thread_name.h"
#include "logservice/ob_log_service.h"
namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace rootserver;
using namespace storage;
namespace observer
{
ObRootServiceMonitor::ObRootServiceMonitor(ObRootService &root_service,
                                           share::ObRsMgr &rs_mgr)
  : inited_(false),
    root_service_(root_service),
    fail_count_(0),
    rs_mgr_(rs_mgr)
{
}

ObRootServiceMonitor::~ObRootServiceMonitor()
{
  if (inited_) {
    stop();
  }
}

int ObRootServiceMonitor::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    FLOG_WARN("init twice", KR(ret));
  } else {
    const int thread_count = 1;
    this->set_thread_count(thread_count);
    inited_ = true;
  }
  return ret;
}

void ObRootServiceMonitor::run1()
{
  int ret = OB_SUCCESS;
  ObRSThreadFlag rs_work;
  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("not init", KR(ret));
  } else {
    FLOG_INFO("root service monitor thread start");
    lib::set_thread_name("RSMonitor");
    while (!has_set_stop()) {
      if (OB_FAIL(monitor_root_service())) {
        FLOG_WARN("monitor root service failed", KR(ret));
      }
      if (!has_set_stop()) {
        ob_usleep(MONITOR_ROOT_SERVICE_INTERVAL_US);
      }
    }
    FLOG_INFO("root service monitor thread exit");
  }
}

int ObRootServiceMonitor::start()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    ret = OB_ERR_UNEXPECTED;
    FLOG_WARN("start root service monitor thread failed", KR(ret));
  }
  return ret;
}

void ObRootServiceMonitor::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("not init", KR(ret));
  } else if (!has_set_stop()) {
    share::ObThreadPool::stop();
  }
}


int ObRootServiceMonitor::monitor_root_service()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("not init", KR(ret));
  } else {
    const uint64_t tenant_id = OB_SYS_TENANT_ID;
    MTL_SWITCH(tenant_id) {
      ObRole role = FOLLOWER;
      bool palf_exist = false;
      int64_t proposal_id = 0;  // unused
      palf::PalfHandleGuard palf_handle_guard;
      logservice::ObLogService *log_service = nullptr;
      if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
        ret = OB_ERR_UNEXPECTED;
        FLOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(log_service->check_palf_exist(SYS_LS, palf_exist))) {
        FLOG_WARN("fail to check palf exist", KR(ret), K(tenant_id), K(SYS_LS));
      } else if (!palf_exist) {
        // bypass
      } else if (OB_FAIL(log_service->open_palf(SYS_LS, palf_handle_guard))) {
        FLOG_WARN("open palf failed", KR(ret), K(tenant_id), K(SYS_LS));
      } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
        FLOG_WARN("get role failed", KR(ret), K(tenant_id));
      }
      if (OB_FAIL(ret)) {
      } else if (root_service_.is_stopping()) {
        //need exit
        if (OB_FAIL(root_service_.stop_service())) {
          FLOG_WARN("root_service stop_service failed", KR(ret));
        }
      } else if (root_service_.is_need_stop()) {
        FLOG_INFO("root service is starting, stop_service need wait");
      } else {
        if (is_strong_leader(role)) {
          if (root_service_.in_service()) {
            //already started or is starting
            //nothing todo
          } else if (!root_service_.can_start_service()) {
            LOG_ERROR("bug here. root service can not start service");
          } else {
            DEBUG_SYNC(BEFORE_START_RS);
            if (OB_FAIL(try_start_root_service())) {
              FLOG_WARN("fail to start root_service", KR(ret));
            }
          }
        } else {
          // possible follower or doesn't have role yet
          //DEBUG_SYNC(BEFORE_STOP_RS);
          //leader does not exist.
          if (!root_service_.is_start()) {
            //nothing todo
          } else {
            if (OB_FAIL(root_service_.revoke_rs())) {
              FLOG_WARN("fail to revoke rootservice", KR(ret));
              if (root_service_.is_need_stop()) {
                //nothing todo
              } else if (root_service_.is_stopping()) {
                if (OB_FAIL(root_service_.stop_service())) {
                  FLOG_WARN("root_service stop_service failed", KR(ret));
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                FLOG_WARN("inalid root service status", KR(ret));
              }
            }
          }
        }
      }
    } else {
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        ret = OB_SUCCESS;
      } else {
        FLOG_WARN("fail to get tenant", KR(ret), "tenant_id", OB_SYS_TENANT_ID);
      }
    }
  }
  return ret;
}

int ObRootServiceMonitor::try_start_root_service()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("try start root service begin");
  ObArray<ObAddr> rs_list;
  const int64_t cluster_id = GCONF.cluster_id;
  bool need_to_wait = false;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const bool check_ls_service = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    FLOG_WARN("ObRootServiceMonitor is not inited", KR(ret));
  } else if (OB_UNLIKELY(!rs_mgr_.is_inited())) {
    ret = OB_NOT_INIT;
    FLOG_WARN("rs_mgr_ is not inited", KR(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    FLOG_WARN("GCTX.srv_rpc_proxy_ is null", KR(ret));
  } else if (OB_FAIL(rs_mgr_.construct_initial_server_list(check_ls_service, rs_list))) {
    FLOG_WARN("fail to construct initial server list", KR(ret));
  } else if (rs_list.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    FLOG_WARN("rs_list should not has no member", KR(ret), K(rs_list), "count", rs_list.count());
  } else {
    int64_t timeout = GCONF.rpc_timeout;
    int64_t count = 0;
    rootserver::ObGetRootserverRoleProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_root_server_status);
    ObDetectMasterRsArg arg;
    for (int64_t i = 0; i < rs_list.count(); i++) {
      ObAddr &addr = rs_list.at(i);
      if (!addr.is_valid() || GCTX.self_addr() == addr) {
        // do nothing, no need to check self
      } else if (OB_SUCCESS != (tmp_ret = arg.init(addr, cluster_id))) {
        // cluster_id is useless, but we want to reuse ObDetectMasterRsArg here
        need_to_wait = true;
        FLOG_WARN("need to wait because fail to init arg", KR(tmp_ret), K(addr), K(cluster_id));
      } else if (FALSE_IT(count++)) {
        // shall never be here
      } else if (OB_SUCCESS != (tmp_ret = proxy.call(addr, timeout, OB_SYS_TENANT_ID, arg))) {
        need_to_wait = true;
        FLOG_WARN("need to wait because fail to send rpc", KR(tmp_ret), K(addr), K(timeout), K(arg));
      }
    }

    ObArray<int> return_ret_array;
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_ret_array))) {
      // ignore ret
      need_to_wait = true;
      FLOG_WARN("need to wait because wait batch result failed", KR(tmp_ret));
    } else if (return_ret_array.count() != count) {
      //ignore ret
      need_to_wait = true;
      FLOG_WARN("need to wait because send rpc count should match return rpc count", K(count),
               "return_ret_array count", return_ret_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_ret_array.count(); i++) {
        int return_ret = return_ret_array.at(i);
        const ObAddr &addr = proxy.get_dests().at(i);
        if (OB_SUCCESS == return_ret) {
          // need to check that server is leader or !status::init
          const ObGetRootserverRoleResult *result = proxy.get_results().at(i);
          if (OB_ISNULL(result)) {
            //ignore ret
            need_to_wait = true;
            FLOG_WARN("need to wait because result is null", KR(ret), K(addr));
          } else if (is_strong_leader(result->get_role())) {
            ret = OB_NOT_MASTER;
            FLOG_WARN("a new leader may elected", KR(ret), K(addr),
                     "status", result->get_status(), "role", result->get_role());
          } else if (result->get_status() != status::INIT) {
            //old rs may not stop
            need_to_wait = true;
            FLOG_WARN("need to wait because another root server already exist", K(addr),
                     "status", result->get_status(), "role", result->get_role());
          }
        } else {
          need_to_wait = true;
          FLOG_WARN("need to wait because failed to get result", KR(ret), K(addr));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // OB_FAIL(ret) will occur only if any server said its role is leader,
      // that leader may newly elected, only in this case we do not start rootservice
    } else {
      // if OB_SUCC(ret) then we have to check whether wait a while before start rootservice
      // need_to_wait will be set to true in these cases:
      //   (1) fail to ask other servers in member_list
      //   (2) success to receive rpc result and its status is NOT init, old rs may not stop
      if (need_to_wait && !has_set_stop()) {
        ob_usleep(2 * MONITOR_ROOT_SERVICE_INTERVAL_US);
      }
      if (OB_FAIL(root_service_.start_service())) {
        FLOG_WARN("root_service start_service failed", KR(ret));
      }
    }
  }
  FLOG_INFO("try start root service finish", KR(ret));
  return ret;
}

}//end namespace observer
}//end namespace oceanbase
