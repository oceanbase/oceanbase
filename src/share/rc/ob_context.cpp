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
namespace oceanbase {
namespace lib {
uint64_t current_tenant_id()
{
  return CURRENT_ENTITY(TENANT_SPACE)->get_tenant_id();
}
uint64_t current_resource_owner_id()
{
  return CURRENT_ENTITY(RESOURCE_OWNER)->get_owner_id();
}
}  // end of namespace lib

namespace share {

uint64_t ObTenantSpace::get_tenant_id() const
{
  return tenant_->id();
}

int ObTenantSpace::guard_init_cb(const ObTenantSpace& tenant_space, char* buf, bool& is_inited)
{
  int ret = OB_SUCCESS;
  is_inited = false;
  if (&tenant_space == &ObTenantSpace::root()) {
    // do-nothing
  } else {
    ObWorker::CompatMode mode = THIS_WORKER.get_compatibility_mode();
    *reinterpret_cast<ObWorker::CompatMode*>(buf) = mode;
    mode = tenant_space.get_tenant()->get<ObWorker::CompatMode>();
    THIS_WORKER.set_compatibility_mode(mode);
    is_inited = true;
  }
  return ret;
}

void ObTenantSpace::guard_deinit_cb(const ObTenantSpace& tenant_space, char* buf)
{
  UNUSEDx(tenant_space);
  ObWorker::CompatMode mode = *reinterpret_cast<ObWorker::CompatMode*>(buf);
  THIS_WORKER.set_compatibility_mode(mode);
}

ObTenantSpace& ObTenantSpace::root()
{
  static ObTenantSpace* root = nullptr;
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

ObResourceOwner& ObResourceOwner::root()
{
  static ObResourceOwner* root = nullptr;
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

int ObTableSpace::init()
{
  int ret = common::OB_SUCCESS;
  ObTenantSpace* entity = nullptr;
  ObLDHandle handle;
  if (common::is_sys_table(table_id_)) {
    compat_mode_ = ObWorker::CompatMode::MYSQL;
  } else if (OB_FAIL(get_tenant_ctx_with_tenant_lock(common::extract_tenant_id(table_id_), handle, entity))) {
  } else {
    compat_mode_ = entity->get_tenant()->get<ObWorker::CompatMode>();
    entity->get_tenant()->unlock(handle);
  }
  return ret;
}

int ObTableSpace::guard_init_cb(const ObTableSpace& table_space, char* buf, bool& is_inited)
{
  int ret = OB_SUCCESS;
  is_inited = false;
  if (&table_space == &ObTableSpace::root()) {
    // do-nothing
  } else {
    ObWorker::CompatMode mode = THIS_WORKER.get_compatibility_mode();
    *reinterpret_cast<ObWorker::CompatMode*>(buf) = mode;
    mode = table_space.get_compat_mode();
    THIS_WORKER.set_compatibility_mode(mode);
    is_inited = true;
  }
  return ret;
}

void ObTableSpace::guard_deinit_cb(const ObTableSpace& table_space, char* buf)
{
  UNUSEDx(table_space);
  THIS_WORKER.set_compatibility_mode(*reinterpret_cast<ObWorker::CompatMode*>(buf));
}

ObTableSpace& ObTableSpace::root()
{
  static ObTableSpace* root = nullptr;
  if (OB_UNLIKELY(nullptr == root)) {
    static lib::ObMutex mutex;
    lib::ObMutexGuard guard(mutex);
    if (nullptr == root) {
      static ObTableSpace tmp(common::combine_id(common::OB_SYS_TENANT_ID, 1));
      int ret = tmp.init();
      abort_unless(OB_SUCCESS == ret);
      root = &tmp;
    }
  }
  return *root;
}

int get_compat_mode_with_table_id(const uint64_t table_id, ObWorker::CompatMode& compat_mode)
{
  int ret = OB_SUCCESS;
  CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, table_id)
  {
    compat_mode = THIS_WORKER.get_compatibility_mode();
    if (ObWorker::CompatMode::INVALID == compat_mode) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "compat mode can not be invalid", K(ret), K(table_id));
    }
  }
  else
  {
    if (OB_TENANT_NOT_IN_SERVER == ret) {
      ret = OB_EAGAIN;
    }
    SHARE_LOG(WARN, "failed to swith table context", K(ret), K(table_id));
  }
  return ret;
}

ObTenantSpaceFetcher::ObTenantSpaceFetcher(const uint64_t tenant_id) : ret_(OB_SUCCESS), entity_(nullptr)
{
  int ret = common::OB_SUCCESS;
  ObTenantSpace* tmp = nullptr;
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
  ret_ = ret;
}

ObTenantSpaceFetcher::~ObTenantSpaceFetcher()
{
  if (entity_ != nullptr && entity_->get_tenant() != nullptr) {
    entity_->get_tenant()->unlock(handle_);
    entity_ = nullptr;
  }
}

}  // end of namespace share
}  // end of namespace oceanbase
