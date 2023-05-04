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

#include "ob_all_server_task.h"

#include "lib/profile/ob_trace_id.h"
#include "share/config/ob_server_config.h"
#include "share/ob_server_status.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_disaster_recovery_task_mgr.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"
#include "share/ob_all_server_tracer.h"
#include "ob_heartbeat_service.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;

ObAllServerTask::ObAllServerTask(ObServerManager &server_manager,
                                 ObDRTaskMgr &disaster_recovery_task_mgr,
                                 const ObAddr &server,
                                 bool with_rootserver)
  : server_manager_(server_manager),
    disaster_recovery_task_mgr_(disaster_recovery_task_mgr),
    server_(server),
    with_rootserver_(with_rootserver)
{
}

ObAllServerTask::~ObAllServerTask()
{
}

int ObAllServerTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!ObHeartbeatService::is_service_enabled()) {
    if (OB_ISNULL(ObCurTraceId::get_trace_id())) {
      //Prevent the current trace_id from being overwritten
      ObCurTraceId::init(GCONF.self_addr_);
    }
    THIS_WORKER.set_timeout_ts(INT64_MAX);
    if (!ObRootServiceRoleChecker::is_rootserver()) {
      ret = OB_NOT_MASTER;
      LOG_WARN("not master", K(ret));
    } else if (OB_FAIL(server_manager_.adjust_server_status(
            server_, disaster_recovery_task_mgr_, with_rootserver_))) {
      LOG_WARN("fail to adjust server status", K(ret), K(server_));
    }
    if (OB_TMP_FAIL(SVR_TRACER.refresh())) {
      LOG_WARN("fail to refresh all server tracer", KR(ret), KR(tmp_ret));
    }
  } else {
    LOG_TRACE("no need to do ObAllServerTask in version >= 4.2");
  }
  return ret;
}

int64_t ObAllServerTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask *ObAllServerTask::deep_copy(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObAllServerTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(ObAllServerTask))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null or buf size not large enough", "buf", OB_P(buf), K(buf_size),
        "need size", sizeof(ObAllServerTask), K(ret));
  } else {
    task = new(buf) ObAllServerTask(server_manager_,
        disaster_recovery_task_mgr_, server_, with_rootserver_);
  }
  return task;
}

}//end namespace rootserver
}//end namespace oceanbase
