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

#include "log_rpc_processor.h"
#include "palf_env.h"
#include "log_define.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace palf
{

int __get_palf_env_impl(uint64_t tenant_id, IPalfEnvImpl *&palf_env_impl, const bool need_check_tenant_id)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = nullptr;
  PalfEnv *palf_env = nullptr;
   if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "get_log_service failed", K(ret));
	} else if (OB_ISNULL(palf_env = log_service->get_palf_env())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "get_palf_env failed", K(ret));
  } else if (OB_ISNULL(palf_env_impl = palf_env->get_palf_env_impl())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "get_palf_env_impl failed", K(ret), KP(log_service), KP(palf_env_impl));
  } else if (need_check_tenant_id && tenant_id != palf_env_impl->get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "tenant_id is not same as palf_env", K(ret), K(tenant_id), "tenant_id_in_palf", palf_env_impl->get_tenant_id());
  } else {
    // do nothing
  }
	return ret;
}

} // end namespace palf
} // end namespace oceanbase
