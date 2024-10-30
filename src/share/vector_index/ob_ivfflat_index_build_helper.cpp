/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON
#include "share/vector_index/ob_ivfflat_index_build_helper.h"

namespace oceanbase {
namespace share {
/*
 * ObIvfflatIndexBuildHelper Impl
 */
int ObIvfflatIndexBuildHelper::init(const int64_t tenant_id,
                                    const int64_t lists,
                                    const ObVectorDistanceType distance_type) {
  int ret = ObIvfIndexBuildHelper::init(tenant_id, lists, distance_type);
  if (OB_SUCC(ret)) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_FAIL(center_helper_.init(tenant_id, lists, distance_type,
                  tenant_config->vector_ivfflat_iters_count,
                   tenant_config->vector_ivfflat_elkan))) {
      LOG_WARN("failed to init center helper");
    }
    is_init_ = true;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
