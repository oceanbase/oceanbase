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

#include "ob_sys_ddl_util.h"
#include "rootserver/ob_ddl_service_launcher.h" // for ObDDLServiceLauncher
#include "share/ob_ddl_common.h" // for ObDDLUtil
namespace oceanbase
{
namespace rootserver
{
int ObSysDDLReplicaBuilderUtil::push_task(ObAsyncTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.omt_));
  } else if (OB_UNLIKELY(!GCTX.omt_->has_tenant(OB_SYS_TENANT_ID))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("local server does not have SYS tenant resource", KR(ret));
  } else if (!ObDDLServiceLauncher::is_ddl_service_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl service not started", KR(ret));
  } else {
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      rootserver::ObDDLScheduler* sys_ddl_scheduler = MTL(rootserver::ObDDLScheduler*);
      if (OB_ISNULL(sys_ddl_scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys ddl scheduler service is null", KR(ret), KP(sys_ddl_scheduler));
      } else if (OB_FAIL(sys_ddl_scheduler->get_ddl_builder().push_task(task))) {
        LOG_WARN("add task to ddl builder failed", KR(ret));
      }
    }
  }
  return ret;
}
} // end name space rootserver
} // end namespace oceanbase
