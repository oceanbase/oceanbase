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
#include "ob_tenant_errsim_module_mgr.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace lib;

namespace share
{

ObTenantErrsimModuleMgr::ObTenantErrsimModuleMgr()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      lock_(),
      config_version_(0),
      is_whole_module_(false),
      module_set_(),
      percentage_(0)
{
}

ObTenantErrsimModuleMgr::~ObTenantErrsimModuleMgr()
{
}

int ObTenantErrsimModuleMgr::mtl_init(ObTenantErrsimModuleMgr *&errsim_module_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(errsim_module_mgr->init(tenant_id))) {
    LOG_WARN("failed to init errsim module mgr", K(ret), KP(errsim_module_mgr));
  }
  return ret;
}

void ObTenantErrsimModuleMgr::destroy()
{
  module_set_.destroy();
}

int ObTenantErrsimModuleMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const ObMemAttr bucket_attr(tenant_id, "ErrsimModuleSet");

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant errsim module mgr init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tenant errsim module mgr get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(module_set_.create(MAX_BUCKET_NUM, bucket_attr))) {
    LOG_WARN("failed to create module set", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_whole_module_ = false;
    is_inited_ = true;
  }
  return ret;
}

bool ObTenantErrsimModuleMgr::is_errsim_module(
    const ObErrsimModuleType::TYPE &type)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant errsim module mgr do not init", K(ret));
  } else if (!ObErrsimModuleTypeHelper::is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is errsim module get invalid argument", K(ret), K(type));
  } else {
    common::SpinRLockGuard guard(lock_);
    const int64_t percentage = ObRandom::rand(0, 100);
    if (percentage > percentage_) {
      b_ret = false;
    } else if (is_whole_module_) {
      b_ret = true;
    } else {
      ObErrsimModuleType module_type(type);
      const int32_t hash_ret = module_set_.exist_refactored(module_type);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        b_ret = false;
      } else if (OB_HASH_EXIST == hash_ret) {
        b_ret = true;
      } else {
        b_ret = false;
        LOG_ERROR("failed to check module type exist", K(hash_ret));
      }
    }
  }
  return b_ret;
}

int ObTenantErrsimModuleMgr::build_tenant_moulde(
    const uint64_t tenant_id,
    const int64_t config_version,
    const ModuleArray &module_array,
    const int64_t percentage)
{
  int ret = OB_SUCCESS;
  char type_buf[ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH] = "";
  ObErrsimModuleType::TYPE type = ObErrsimModuleType::ERRSIM_MODULE_MAX;
  const int32_t flag = 1;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant errsim module mgr do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || config_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tenant module get invalid argument", K(ret), K(tenant_id), K(config_version));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (config_version <= config_version_) {
      //do nothing
    } else {
      is_whole_module_ = false;
      module_set_.reuse();
      percentage_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < module_array.size(); ++i) {
        const ErrsimModuleString &string = module_array.at(i);
        type = ObErrsimModuleTypeHelper::get_type(string.ptr());
        ObErrsimModuleType module_type(type);
        if (!module_type.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("errsim module type is unexpected", K(ret), K(module_type), K(type_buf));
        } else if (ObErrsimModuleType::ERRSIM_MODULE_ALL == module_type.type_) {
          is_whole_module_ = true;
        } else if (OB_FAIL(module_set_.set_refactored(module_type, flag))) {
          LOG_WARN("failed to set module set", K(ret), K(module_type));
        } else {
          LOG_INFO("succeed set module", K(module_type), K(tenant_id));
        }
      }

      if (OB_SUCC(ret)) {
        percentage_ = percentage;
      }
    }
  }
  return ret;
}


} //share
} //oceanbase
