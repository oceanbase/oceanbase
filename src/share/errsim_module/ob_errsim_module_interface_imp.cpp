
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
#define USING_LOG_PREFIX COMMON
#include "ob_errsim_module_interface_imp.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "lib/ob_define.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_tenant_errsim_event_mgr.h"

using namespace oceanbase::share;
namespace oceanbase {
namespace common {

int build_tenant_errsim_moulde(
    const uint64_t tenant_id,
    const int64_t config_version,
    const common::ObArray<ObFixedLengthString<ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH>> &module_array,
    const int64_t percentage)
{
  int ret = OB_SUCCESS;
  const uint64_t tmp_tenant_id = is_virtual_tenant_id(tenant_id) ? MTL_ID() : tenant_id;

  if (OB_INVALID_ID == tmp_tenant_id || config_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tenant module get invalid argument", K(ret), K(tmp_tenant_id), K(config_version));
  } else if (is_virtual_tenant_id(tmp_tenant_id) || OB_INVALID_TENANT_ID == tmp_tenant_id) {
    //do nothing
  } else {
    MTL_SWITCH(tmp_tenant_id) {
      ObTenantErrsimModuleMgr *errsim_module_mgr = nullptr;
      if (OB_ISNULL(errsim_module_mgr = MTL(ObTenantErrsimModuleMgr *))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "errsim module mgr should not be NULL", K(ret), KP(errsim_module_mgr));
      } else if (OB_FAIL(errsim_module_mgr->build_tenant_moulde(tmp_tenant_id, config_version, module_array, percentage))) {
        LOG_WARN("failed to build tenant module", K(ret), K(tmp_tenant_id), K(config_version));
      }
    }
  }
  return ret;
}

bool is_errsim_module(
    const uint64_t tenant_id,
    const ObErrsimModuleType::TYPE &type)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  const uint64_t tmp_tenant_id = is_virtual_tenant_id(tenant_id) ? MTL_ID() : tenant_id;
  if (OB_INVALID_ID == tmp_tenant_id || !ObErrsimModuleTypeHelper::is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is errsim module get invalid argument", K(ret), K(tenant_id), K(tmp_tenant_id), K(type));
  } else if (is_virtual_tenant_id(tmp_tenant_id) || OB_INVALID_TENANT_ID == tmp_tenant_id) {
    b_ret = false;
  } else if (ObErrsimModuleType::ERRSIM_MODULE_NONE == type) {
    b_ret = false;
  } else {
    MTL_SWITCH(tmp_tenant_id) {
      ObTenantErrsimModuleMgr *errsim_module_mgr = nullptr;
      if (OB_ISNULL(errsim_module_mgr = MTL(ObTenantErrsimModuleMgr *))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "errsim module mgr should not be NULL", K(ret), KP(errsim_module_mgr));
      } else {
        b_ret = errsim_module_mgr->is_errsim_module(type);
      }
    }
  }
  return b_ret;
}

int add_tenant_errsim_event(
    const uint64_t tenant_id,
    const ObTenantErrsimEvent &event)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  const uint64_t tmp_tenant_id = is_virtual_tenant_id(tenant_id) ? MTL_ID() : tenant_id;
  if (OB_INVALID_ID == tmp_tenant_id || !event.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is errsim module get invalid argument", K(ret), K(tmp_tenant_id), K(event));
  } else if (is_virtual_tenant_id(tmp_tenant_id) || OB_INVALID_TENANT_ID == tmp_tenant_id) {
    //do nothing
  } else {
    MTL_SWITCH(tmp_tenant_id) {
      ObTenantErrsimEventMgr *errsim_event_mgr = nullptr;
      if (OB_ISNULL(errsim_event_mgr = MTL(ObTenantErrsimEventMgr *))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "errsim event mgr should not be NULL", K(ret), KP(errsim_event_mgr));
      } else if (OB_FAIL(errsim_event_mgr->add_tenant_event(event))) {
        LOG_WARN("failed to add tenant event", K(ret), K(event));
      }
    }
  }
  return b_ret;
}


} // common
} // oceanbase
