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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_ls_getter.h"
#include "lib/oblog/ob_log_module.h"        // LOG_ERROR
#include "ob_log_instance.h"

namespace oceanbase
{
namespace libobcdc
{
ObLogLsGetter::ObLogLsGetter() :
    is_inited_(false),
    tenant_ls_ids_cache_()
{
}

ObLogLsGetter::~ObLogLsGetter()
{
  destroy();
}

int ObLogLsGetter::init(const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogLsGetter has been inited", KR(ret));
  } else if (OB_FAIL(tenant_ls_ids_cache_.init("TenantLSIDs"))) {
    LOG_ERROR("tenant_ls_ids_cache_ init failed", KR(ret));
  } else {
    ARRAY_FOREACH_N(tenant_ids, idx, count) {
      const uint64_t tenant_id = tenant_ids.at(idx);

      if (OB_SYS_TENANT_ID == tenant_id
          || is_meta_tenant(tenant_id)) {
        // do nothing
      } else if (OB_FAIL(query_and_set_tenant_ls_info_(tenant_id))) {
        LOG_ERROR("query_and_set_tenant_ls_info_ failed", KR(ret), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

void ObLogLsGetter::destroy()
{
  if (is_inited_) {
    tenant_ls_ids_cache_.destroy();
    is_inited_ = false;
  }
}

int ObLogLsGetter::get_ls_ids(
    const uint64_t tenant_id,
    common::ObIArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  LSIDArray ls_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLsGetter has not been inited", KR(ret));
  } else if (OB_FAIL(tenant_ls_ids_cache_.get(tenant_id, ls_ids))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("tenant_ls_ids_cache_ get failed", KR(ret), K(tenant_id), K(ls_ids));
    } else {
      ret = OB_SUCCESS;
      // query and set
      if (OB_FAIL(query_and_set_tenant_ls_info_(tenant_id))) {
        LOG_ERROR("query_and_set_tenant_ls_info_ failed", KR(ret), K(tenant_id));
      }
    }
  } else {
    ARRAY_FOREACH_N(ls_ids, idx, count) {
      if (OB_FAIL(ls_id_array.push_back(ls_ids.at(idx)))) {
        LOG_ERROR("ls_id_array push_back failed", KR(ret), K(ls_id_array));
      }
    }
  }

  return ret;
}

int ObLogLsGetter::query_and_set_tenant_ls_info_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObLogSysTableHelper::TenantLSIDs tenant_ls_ids;
  LSIDArray ls_ids;

  if (OB_FAIL(query_tenant_ls_info_(tenant_id, tenant_ls_ids))) {
    LOG_ERROR("query_tenant_ls_info_ failed", KR(ret), K(tenant_id));
  } else {
    ARRAY_FOREACH_N(tenant_ls_ids, ls_ids_idx, ls_ids_count) {
      if (OB_FAIL(ls_ids.push_back(tenant_ls_ids.at(ls_ids_idx)))) {
        LOG_ERROR("ls_ids push_back failed", KR(ret), K(ls_ids), K(tenant_ls_ids));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_ls_ids_cache_.insert(tenant_id, ls_ids))) {
        LOG_ERROR("tenant_ls_ids_cache_ insert failed", KR(ret), K(tenant_id), K(ls_ids));
      } else {
        LOG_INFO("tenant_ls_ids_cache_ insert succ", K(tenant_id), K(ls_ids));
      }
    } // OB_SUCC
  }

  return ret;
}

int ObLogLsGetter::query_tenant_ls_info_(
    const uint64_t tenant_id,
    ObLogSysTableHelper::TenantLSIDs &tenant_ls_ids)
{
  int ret = OB_SUCCESS;
  IObLogSysTableHelper *systable_helper = TCTX.systable_helper_;
  tenant_ls_ids.reset();
  bool done = false;

  if (OB_ISNULL(systable_helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("systable_helper is NULL", KR(ret));
  } else {
    while (! done && OB_SUCCESS == ret) {
      if (OB_FAIL(systable_helper->query_tenant_ls_info(tenant_id, tenant_ls_ids))) {
        LOG_WARN("systable_helper query_tenant_ls_info fail", KR(ret), K(tenant_id), K(tenant_ls_ids));
      } else {
        done = true;
      }

      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
        ob_usleep(100L * 1000L);
      }
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
