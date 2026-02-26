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

#include "ipalf_env.h"
#include "logservice/palf/palf_env.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "logservice/libpalf/libpalf_env.h"
#endif

namespace oceanbase
{
namespace ipalf
{
int create_palf_env(PalfEnvCreateParams *params, palf::PalfEnv *&palf_env) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(palf::PalfEnv::create_palf_env(params, palf_env))) {
        PALF_LOG(WARN, "create palf env failed", K(ret), K(params));
    } else {
        PALF_LOG(INFO, "create palf env success", K(ret), K(params));
    }
    return ret;
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
int create_palf_env(LibPalfEnvCreateParams *params, libpalf::LibPalfEnv *&libpalf_env) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(libpalf::LibPalfEnv::create_palf_env(params, libpalf_env))) {
        PALF_LOG(WARN, "create libpalf env failed", K(ret), K(params));
    } else {
        PALF_LOG(INFO, "create libpalf env success", K(ret), K(params));
    }
    return ret;
}
#endif

void destroy_palf_env(IPalfEnv *&ipalf_env) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ipalf_env)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "ipalf env is null", K(ipalf_env));
    } else {
        MTL_DELETE(IPalfEnv, "ipalf_env", ipalf_env);
        PALF_LOG(INFO, "destroy palf env success");
    }
}

} // end namespace ipalf
} // end namespace oceanbase