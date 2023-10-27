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
#define USING_LOG_PREFIX SHARE
#include "share/restore/ob_recover_table_util.h"
#include "share/ob_cluster_version.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "lib/worker.h"

using namespace oceanbase;
using namespace share;

int ObRecoverTableUtil::check_compatible(const uint64_t target_tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  // 1. check sys tenant data version
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
    LOG_WARN("fail to get sys data version", K(ret));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Tenant COMPATIBLE is below 4.2.1.0, recover table is not supported", K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Tenant COMPATIBLE is below 4.2.1.0, recover table is");
  }

  // 2. check target meta tenant data version
  if (FAILEDx(GET_MIN_DATA_VERSION(gen_meta_tenant_id(target_tenant_id), data_version))) {
    LOG_WARN("fail to get meta tenant data version", K(ret), K(target_tenant_id));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Tenant COMPATIBLE is below 4.2.1.0, recover table is not supported", K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Tenant COMPATIBLE is below 4.2.1.0, recover table is");
  }

  // 3. check target user tenant data version
  if (FAILEDx(GET_MIN_DATA_VERSION(target_tenant_id, data_version))) {
    LOG_WARN("fail to get user tenant data version", K(ret), K(target_tenant_id));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Tenant COMPATIBLE is below 4.2.1.0, recover table is not supported", K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Tenant COMPATIBLE is below 4.2.1.0, recover table is");
  }
  return ret;
}