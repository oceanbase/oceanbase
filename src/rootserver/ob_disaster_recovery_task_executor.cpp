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

#define USING_LOG_PREFIX RS

#include "ob_disaster_recovery_task_executor.h"

#include "share/ob_debug_sync.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ob_cluster_version.h"
#include "ob_rs_event_history_table_operator.h"
#include "ob_server_manager.h"
#include "ob_disaster_recovery_task_mgr.h"
#include "ob_disaster_recovery_task.h"
#include "observer/ob_server.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{

int ObDRTaskExecutor::init(
    share::ObLSTableOperator &lst_operator,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    ObServerManager &server_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    lst_operator_ = &lst_operator;
    rpc_proxy_ = &rpc_proxy;
    server_mgr_ = &server_mgr;
    inited_ = true;
  }
  return ret;
}

int ObDRTaskExecutor::execute(
    const ObDRTask &task,
    int &ret_code,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr is null", KR(ret), KP(server_mgr_));
  } else {
    const ObAddr &dst_server = task.get_dst_server();
    bool is_dst_server_alive = false;
    if (OB_FAIL(server_mgr_->check_server_alive(dst_server, is_dst_server_alive))) {
      LOG_WARN("fail to check server alive", KR(ret), K(dst_server));
    } else if (!is_dst_server_alive) {
      ret = OB_REBALANCE_TASK_CANT_EXEC;
      ret_comment = ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_SERVER_NOT_ALIVE;
      LOG_WARN("dst server not alive", KR(ret), K(dst_server));
    } else if (OB_ISNULL(lst_operator_) || OB_ISNULL(rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lst_operator or rpc_proxy is null",
               KR(ret), KP(lst_operator_), KP(rpc_proxy_));
    } else if (OB_FAIL(task.check_before_execute(*lst_operator_, ret_comment))) {
      LOG_WARN("fail to check before execute", KR(ret));
    } else {
      DEBUG_SYNC(BEFORE_SEND_DRTASK_RPC);
      if (OB_FAIL(task.execute(*rpc_proxy_, ret_code, ret_comment))) {
        LOG_WARN("fail to execute task", KR(ret));
      }
    }
    FLOG_INFO("[DRTASK_NOTICE] finish send disaster recovery task", KR(ret), K(task));
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
