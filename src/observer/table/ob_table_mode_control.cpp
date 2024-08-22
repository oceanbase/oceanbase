/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_table_mode_control.h"

using namespace oceanbase::common;
using namespace oceanbase::table;

int ObTableModeCtrl::check_mode(ObKvModeType tenant_mode, ObTableEntityType entity_type)
{
  int ret = OB_SUCCESS;

  switch (tenant_mode) {
    case ObKvModeType::ALL: {
      // all mode is supported
      break;
    }
    case ObKvModeType::TABLEAPI: {
      if (entity_type != ObTableEntityType::ET_KV && entity_type != ObTableEntityType::ET_DYNAMIC) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "As the ob_kv_mode variable has been set to 'TABLEAPI', your current interfaces");
        LOG_WARN("mode not matched", K(ret), K(entity_type), K(tenant_mode));
      }
      break;
    }
    case ObKvModeType::HBASE: {
      if (entity_type != ObTableEntityType::ET_HKV && entity_type != ObTableEntityType::ET_DYNAMIC) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "As the ob_kv_mode variable has been set to 'HBASE', your current interfaces");
        LOG_WARN("mode not matched", K(ret), K(entity_type), K(tenant_mode));
      }
      break;
    }
    case ObKvModeType::REDIS: {
      // OBKV-Redis model is not supported in 433
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "As the ob_kv_mode variable has been set to 'REDIS', your current interfaces");
      LOG_WARN("redis is not supported now", K(ret), K(entity_type), K(tenant_mode));
      break;
    }
    case ObKvModeType::NONE: {
      // all modes are not supported
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "As the ob_kv_mode variable has been set to 'NONE', your current interfaces");
      LOG_WARN("all OBKV modes are not supported", K(ret), K(entity_type), K(tenant_mode));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown ob_kv_mode", K(ret), K(tenant_mode));
      break;
    }
  }

  return ret;
}
