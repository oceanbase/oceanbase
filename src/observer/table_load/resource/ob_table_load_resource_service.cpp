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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/resource/ob_table_load_resource_service.h"
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_schema.h"
#include "share/location_cache/ob_location_struct.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;
using namespace share::schema;
using namespace table;
using namespace omt;
using namespace common::hash;

/**
 * ObTableLoadResourceService
 */

ObTableLoadResourceService::~ObTableLoadResourceService()
{
  obsys::ObWLockGuard w_guard(rw_lock_);
  ob_delete(resource_manager_);
}

int ObTableLoadResourceService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }

  return ret;
}

int ObTableLoadResourceService::mtl_init(ObTableLoadResourceService *&service)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(service));
  } else if (OB_FAIL(service->init(tenant_id))) {
    LOG_WARN("fail to init resource service", KR(ret), K(tenant_id));
  }

  return ret;
}

void ObTableLoadResourceService::stop()
{
  obsys::ObRLockGuard r_guard(rw_lock_);
  if (OB_NOT_NULL(resource_manager_)) {
    LOG_INFO("resource_manager_ start to stop", K_(tenant_id));
    resource_manager_->stop();
  }
  LOG_INFO("resource_service finish to stop", K_(tenant_id));
}

void ObTableLoadResourceService::wait()
{
  obsys::ObRLockGuard r_guard(rw_lock_);
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(resource_manager_)) {
    LOG_INFO("resource_manager_ start to wait", K_(tenant_id));
    if (OB_FAIL(resource_manager_->wait())) {
      LOG_WARN("fail to wait", KR(ret), K_(tenant_id));
    }
  }
  LOG_INFO("resource_service finish to wait", K_(tenant_id));
}

void ObTableLoadResourceService::destroy()
{
  obsys::ObRLockGuard r_guard(rw_lock_);
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(resource_manager_)) {
    LOG_INFO("resource_manager_ start to destroy", K_(tenant_id));
    resource_manager_->destroy();
  }
  LOG_INFO("resource_service finish to destroy", K_(tenant_id));
}

int ObTableLoadResourceService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  ObMutexGuard switch_guard(switch_lock_);
  int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K_(tenant_id));
  } else {
    if (OB_ISNULL(resource_manager_)) {
      obsys::ObWLockGuard w_guard(rw_lock_);
      if (OB_FAIL(alloc_resource_manager())) {
        LOG_WARN("fail to alloc resource_manager", KR(ret), K_(tenant_id));
      }
    } else {
      obsys::ObRLockGuard r_guard(rw_lock_);
      ret = resource_manager_->resume();
      LOG_INFO("resource_service finish to resume",KR(ret), K_(tenant_id));
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("resource_manager: switch_to_leader", KR(ret), K_(tenant_id), K(cost_us), KP_(resource_manager));

  return ret;
}

int ObTableLoadResourceService::switch_to_follower_gracefully() {
  int ret = OB_SUCCESS;
  LOG_INFO("switch_to_follower_gracefully", K_(tenant_id));
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("fail to switch to follower", KR(ret));
  }

  return ret;
}

void ObTableLoadResourceService::switch_to_follower_forcedly() {
  int ret = OB_SUCCESS;
  LOG_INFO("switch_to_follower_forcedly", K_(tenant_id));
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("fail to switch to follower", KR(ret));
  }
}

int ObTableLoadResourceService::alloc_resource_manager()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  int64_t len = sizeof(ObTableLoadResourceManager);
  if (FAILEDx(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret));
  } else if (OB_NOT_NULL(resource_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resource_manager_ is not null", K_(tenant_id), KR(ret), KP_(resource_manager));
  } else if (nullptr == (buf = common::ob_malloc(len, ObMemAttr(tenant_id_, "tenant_rm_mgr")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(tenant_id), K(len));
  } else if (FALSE_IT(resource_manager_ = new(buf) ObTableLoadResourceManager())) {
    // impossible
  } else if (OB_FAIL(resource_manager_->init())) {
    LOG_WARN("fail to init resource_manager", K_(tenant_id), KR(ret));
  } else if (OB_FAIL(resource_manager_->start())) {
    LOG_WARN("fail to start resource_manager", K_(tenant_id), KR(ret));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succ to alloc resource_manager", K_(tenant_id), KP_(resource_manager));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(delete_resource_manager())) {
      LOG_WARN("fail to delete tenant major resource manager", KR(tmp_ret), K_(tenant_id));
    }
    buf = nullptr;
  }

  return ret;
}

int ObTableLoadResourceService::delete_resource_manager()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(resource_manager_)) {
    // no need to delete
  } else {
    resource_manager_->stop();
    if (OB_FAIL(resource_manager_->wait())) {
      LOG_WARN("fail to wait", KR(ret), K_(tenant_id));
    } else {
      resource_manager_->destroy();
      LOG_INFO("succ to delete resource_manager", K_(tenant_id));
    }
  }

  // ignore ret
  if (OB_NOT_NULL(resource_manager_)) {
    ob_delete(resource_manager_);
    resource_manager_ = nullptr;
  }
  LOG_INFO("finish to delete resource_manager", KR(ret), K_(tenant_id));

  return ret;
}

int ObTableLoadResourceService::inner_switch_to_follower()
{
  int ret = OB_SUCCESS;
  ObMutexGuard switch_guard(switch_lock_);
  obsys::ObRLockGuard r_guard(rw_lock_);
  const int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_NOT_NULL(resource_manager_)) {
    resource_manager_->pause();
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("resource_manager: switch_to_follower", KR(ret), K_(tenant_id), K(cost_us), KP_(resource_manager));

  return ret;
}

int ObTableLoadResourceService::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  }

  return ret;
}

int ObTableLoadResourceService::check_tenant()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObTenant *tenant = nullptr;
  if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL !=
                         tenant->get_unit_status())) {
    ret = OB_ERR_UNEXPECTED_UNIT_STATUS;
    LOG_WARN("unit status not normal", KR(ret), K(tenant->get_unit_status()));
  }

  return ret;
}

int ObTableLoadResourceService::get_leader_addr(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObAddr &leader)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
      GCONF.cluster_id, tenant_id, ls_id, leader, GET_LEADER_RETRY_TIMEOUT))) {
    LOG_WARN("fail to get ls location leader", KR(ret), K(tenant_id), K(ls_id));
    if (is_location_service_renew_error(ret)) {
      ret = OB_EAGAIN;
    }
  }

  return ret;
}

int ObTableLoadResourceService::local_apply_resource(ObDirectLoadResourceApplyArg &arg, ObDirectLoadResourceOpRes &res)
{
  int ret = OB_SUCCESS;
  ObTableLoadResourceService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadResourceService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load resource service", KR(ret));
  } else if(OB_ISNULL(service->resource_manager_)) {
    ret = OB_EAGAIN;
    LOG_WARN("resource_manager_ is null", KR(ret));
  } else {
    ret = service->resource_manager_->apply_resource(arg, res);
  }

  return ret;
}

int ObTableLoadResourceService::local_release_resource(ObDirectLoadResourceReleaseArg &arg)
{
  int ret = OB_SUCCESS;
  ObTableLoadResourceService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadResourceService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load resource service", KR(ret));
  } else if(OB_ISNULL(service->resource_manager_)) {
    ret = OB_EAGAIN;
    LOG_WARN("resource_manager_ is null", KR(ret));
  } else {
    ret = service->resource_manager_->release_resource(arg);
  }

  return ret;
}

int ObTableLoadResourceService::local_update_resource(ObDirectLoadResourceUpdateArg &arg)
{
  int ret = OB_SUCCESS;
  ObTableLoadResourceService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadResourceService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load resource service", KR(ret));
  } else if(OB_ISNULL(service->resource_manager_)) {
    ret = OB_EAGAIN;
    LOG_WARN("resource_manager_ is null", KR(ret));
  } else {
    ret = service->resource_manager_->update_resource(arg);
  }

  return ret;
}

int ObTableLoadResourceService::apply_resource(ObDirectLoadResourceApplyArg &arg, ObDirectLoadResourceOpRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), KR(ret));
  } else {
    ObAddr leader;
    if (OB_FAIL(get_leader_addr(arg.tenant_id_, share::SYS_LS, leader))) {
      LOG_WARN("fail to get leader addr", KR(ret));
    } else if (ObTableLoadUtils::is_local_addr(leader)) {
      ret = local_apply_resource(arg, res);
    } else {
      TABLE_LOAD_RESOURCE_RPC_CALL(apply_resource, leader, arg, res);
    }
  }

  return ret;
}

int ObTableLoadResourceService::release_resource(ObDirectLoadResourceReleaseArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), KR(ret));
  } else {
    ObAddr leader;
    if (OB_FAIL(get_leader_addr(arg.tenant_id_, share::SYS_LS, leader))) {
      LOG_WARN("fail to get leader addr", KR(ret));
    } else if (ObTableLoadUtils::is_local_addr(leader)) {
      ret = local_release_resource(arg);
    } else {
      TABLE_LOAD_RESOURCE_RPC_CALL(release_resource, leader, arg);
    }
  }

  return ret;
}

int ObTableLoadResourceService::update_resource(ObDirectLoadResourceUpdateArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), KR(ret));
  } else {
    ObAddr leader;
    if (OB_FAIL(get_leader_addr(arg.tenant_id_, share::SYS_LS, leader))) {
      LOG_WARN("fail to get leader addr", KR(ret));
    } else if (ObTableLoadUtils::is_local_addr(leader)) {
      ret = local_update_resource(arg);
    } else {
      TABLE_LOAD_RESOURCE_RPC_CALL(update_resource, leader, arg);
    }
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
