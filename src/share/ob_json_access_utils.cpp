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
 * This file contains implementation for lob_access_utils.
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_json_access_utils.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/json_type/ob_json_base.h"

namespace oceanbase
{
using namespace common;
namespace share
{

int ObJsonWrapper::get_raw_binary(ObIJsonBase *j_base, ObString &result, ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  uint64_t tenant_data_version = 0;
  uint64_t tenant_id = MTL_ID();
  if (tenant_id == 0) {
    tenant_id = OB_SYS_TENANT_ID;
    LOG_INFO("get tenant id zero");
  }
  if (OB_ISNULL(allocator) || OB_ISNULL(j_base)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator or j_base is null", K(ret), KP(allocator), KP(j_base));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (! ((DATA_VERSION_4_2_2_0 <= tenant_data_version && tenant_data_version < DATA_VERSION_4_3_0_0) || tenant_data_version >= DATA_VERSION_4_3_1_0)) {
    if (OB_FAIL(j_base->get_raw_binary_v0(result, allocator))) {
      LOG_WARN("get raw binary fail", K(ret), K(tenant_data_version), K(tenant_id));
    }
  } else if (OB_FAIL(j_base->get_raw_binary(result, allocator))) {
    LOG_WARN("get raw binary fail", K(ret), K(tenant_data_version), K(tenant_id));
  }
  return ret;
}


} // end namespace share
} // end namespace oceanbase
