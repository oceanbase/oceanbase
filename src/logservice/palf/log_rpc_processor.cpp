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

#include "logservice/palf/log_rpc_processor.h"
#include "logservice/ob_log_service.h"
#include "logservice/palf/palf_env.h"

namespace oceanbase
{
namespace palf
{

int __get_palf_env_impl(uint64_t tenant_id, PalfEnvImpl *&palf_env_impl)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = nullptr;
  PalfEnv *palf_env = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "mtl id not match", K(tenant_id), K(MTL_ID()), K(ret));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "get_log_service failed", K(ret));
	} else if (OB_ISNULL(palf_env = log_service->get_palf_env())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "get_palf_env failed", K(ret));
  } else if (OB_ISNULL(palf_env_impl = palf_env->get_palf_env_impl())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "get_palf_env_impl failed", K(ret), KP(log_service), KP(palf_env_impl));
  } else {
    // do nothing
  }
	return ret;
}

} // end namespace palf
} // end namespace oceanbase
