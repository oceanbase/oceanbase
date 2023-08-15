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
#include "ob_tenant_errsim_event_mgr.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace lib;

namespace share
{

ObTenantErrsimEventMgr::ObTenantErrsimEventMgr()
    : is_inited_(false),
      lock_(),
      event_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("TErrsimEvent", MTL_ID()))
{
}

ObTenantErrsimEventMgr::~ObTenantErrsimEventMgr()
{
}

int ObTenantErrsimEventMgr::mtl_init(ObTenantErrsimEventMgr *&errsim_event_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(errsim_event_mgr->init())) {
    LOG_WARN("failed to init errsim event mgr", K(ret), KP(errsim_event_mgr));
  }
  return ret;
}

void ObTenantErrsimEventMgr::destroy()
{
  event_array_.destroy();
}

int ObTenantErrsimEventMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant errsim event mgr init twice", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTenantErrsimEventMgr::add_tenant_event(
    const ObTenantErrsimEvent &event)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant errsim event mgr do not init", K(ret));
  } else if (!event.is_valid()) {
    LOG_WARN("add tenant event get invalid argument", K(ret), K(event));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(event_array_.push_back(event))) {
      LOG_WARN("failed to add tenant event", K(ret), K(event));
    }
  }
  return ret;
}


} //share
} //oceanbase
