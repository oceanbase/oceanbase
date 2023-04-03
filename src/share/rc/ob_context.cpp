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
#include "share/rc/ob_context.h"
#include "lib/coro/co_var.h"
#include "lib/rc/ob_rc.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
namespace oceanbase
{
namespace lib
{
uint64_t current_resource_owner_id()
{
  return CURRENT_ENTITY(RESOURCE_OWNER)->get_owner_id();
}
} // end of namespace lib

namespace share
{

uint64_t ObTenantSpace::get_tenant_id() const
{
  return tenant_->id();
}

int ObTenantSpace::guard_init_cb(const ObTenantSpace &tenant_space, char *buf, bool &is_inited)
{
  int ret = OB_SUCCESS;
  is_inited = false;
  if (&tenant_space == &ObTenantSpace::root()) {
    // do-nothing
  } else {
    lib::Worker::CompatMode mode = THIS_WORKER.get_compatibility_mode();
    *reinterpret_cast<lib::Worker::CompatMode*>(buf) = mode;
    mode = tenant_space.get_tenant()->get<lib::Worker::CompatMode>();
    THIS_WORKER.set_compatibility_mode(mode);
    is_inited = true;
  }
  return ret;
}

void ObTenantSpace::guard_deinit_cb(const ObTenantSpace &tenant_space, char *buf)
{
  UNUSEDx(tenant_space);
  lib::Worker::CompatMode mode = *reinterpret_cast<lib::Worker::CompatMode*>(buf);
  THIS_WORKER.set_compatibility_mode(mode);
}

ObTenantSpace &ObTenantSpace::root()
{
  static ObTenantSpace *root = nullptr;
  if (OB_UNLIKELY(nullptr == root)) {
    static lib::ObMutex mutex;
    lib::ObMutexGuard guard(mutex);
    if (nullptr == root) {
      static ObTenantSpace tmp(nullptr);
      int ret = tmp.init();
      abort_unless(OB_SUCCESS == ret);
      root = &tmp;
    }
  }
  return *root;
}

ObResourceOwner &ObResourceOwner::root()
{
  static ObResourceOwner *root = nullptr;
  if (OB_UNLIKELY(nullptr == root)) {
    static lib::ObMutex mutex;
    lib::ObMutexGuard guard(mutex);
    if (nullptr == root) {
      static ObResourceOwner tmp(common::OB_SERVER_TENANT_ID);
      int ret = tmp.init();
      abort_unless(OB_SUCCESS == ret);
      root = &tmp;
    }
  }
  return *root;
}

ObTenantSpaceFetcher::ObTenantSpaceFetcher(const uint64_t tenant_id)
  : ret_(OB_SUCCESS),
    entity_(nullptr)
{
  int ret = common::OB_SUCCESS;
  ObTenantSpace *tmp = nullptr;
  if (OB_FAIL(get_tenant_ctx_with_tenant_lock(tenant_id, handle_, tmp))) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      SHARE_LOG(WARN, "get tenant ctx failed", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tmp)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "null ptr", K(ret), K(tenant_id));
  } else {
    entity_ = tmp;
  }
  if (OB_FAIL(ret) && ret == OB_IN_STOP_STATE) {
    ret = OB_TENANT_NOT_IN_SERVER;
  }
  ret_ = ret;
}

ObTenantSpaceFetcher::~ObTenantSpaceFetcher()
{
  if (entity_ != nullptr && entity_->get_tenant() != nullptr) {
    entity_->get_tenant()->unlock(handle_);
    entity_ = nullptr;
  }
}


} // end of namespace share
} // end of namespace oceanbase
