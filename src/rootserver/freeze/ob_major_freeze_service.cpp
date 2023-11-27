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

#define USING_LOG_PREFIX RS_COMPACTION

#include "rootserver/freeze/ob_major_freeze_service.h"
#include "rootserver/freeze/ob_tenant_major_freeze.h"
#include "share/ob_all_server_tracer.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace rootserver
{
using namespace share;

int ObMajorFreezeService::init(const uint64_t tenant_id)
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

ObMajorFreezeService::~ObMajorFreezeService()
{
  SpinWLockGuard w_guard(rw_lock_);
  ob_delete(tenant_major_freeze_);
}

int ObMajorFreezeService::switch_to_leader()
{
  ObRecursiveMutexGuard switch_guard(switch_lock_);
  int64_t start_time_us = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K_(tenant_id));
  } else {
    if (OB_ISNULL(tenant_major_freeze_)) {
      SpinWLockGuard w_guard(rw_lock_);
      if (OB_FAIL(alloc_tenant_major_freeze())) {
        LOG_WARN("fail to alloc tenant_major_freeze", KR(ret), K_(tenant_id));
      }
    } else {
      SpinRLockGuard r_guard(rw_lock_);
      tenant_major_freeze_->resume();
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("major_freeze: switch_to_leader", KR(ret), K_(tenant_id), K(cost_us), KP_(tenant_major_freeze));

  return ret;
}

int ObMajorFreezeService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  LOG_INFO("switch_to_follower_gracefully", K_(tenant_id));
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("fail to switch to follower", KR(ret));
  }
  return ret;
}

void ObMajorFreezeService::switch_to_follower_forcedly()
{
  int ret = OB_SUCCESS;
  LOG_INFO("switch_to_follower_forcedly", K_(tenant_id));
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("fail to switch to follower", KR(ret));
  }
}

int ObMajorFreezeService::inner_switch_to_follower()
{
  ObRecursiveMutexGuard switch_guard(switch_lock_);
  SpinRLockGuard r_guard(rw_lock_);
  const int64_t start_time_us = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tenant_major_freeze_)) {
    tenant_major_freeze_->pause();
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("major_freeze: switch_to_follower", KR(ret), K_(tenant_id), K(cost_us), KP_(tenant_major_freeze));
  return ret;
}

int ObMajorFreezeService::alloc_tenant_major_freeze()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  int64_t len = sizeof(ObTenantMajorFreeze);
  bool is_primary_service = true;
  ObMajorFreezeServiceType service_type = get_service_type();
  if ((service_type <= ObMajorFreezeServiceType::SERVICE_TYPE_INVALID)
      || (service_type >= ObMajorFreezeServiceType::SERVICE_TYPE_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected major freeze service type", KR(ret), K(service_type), K_(tenant_id));
  } else {
    is_primary_service = (ObMajorFreezeServiceType::SERVICE_TYPE_PRIMARY == service_type) ? true : false;
  }

  if (FAILEDx(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret));
  } else if (OB_NOT_NULL(tenant_major_freeze_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_major_freeze is not null", K_(tenant_id), KR(ret), KP_(tenant_major_freeze));
  } else if (nullptr == (buf = common::ob_malloc(len, ObMemAttr(tenant_id_, "tenant_mf_mgr")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(tenant_id), K(len));
  } else if (FALSE_IT(tenant_major_freeze_ = new(buf) ObTenantMajorFreeze(tenant_id_))) {
    // impossible
  } else if (OB_FAIL(tenant_major_freeze_->init(is_primary_service, *GCTX.sql_proxy_,
             *GCTX.config_, *GCTX.schema_service_, ObAllServerTracer::get_instance()))) {
    LOG_WARN("fail to init tenant_major_freeze", K_(tenant_id), KR(ret), K(is_primary_service));
  } else if (OB_FAIL(tenant_major_freeze_->start())) {
    LOG_WARN("fail to start tenant_major_freeze", K_(tenant_id), KR(ret), K(is_primary_service));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succ to alloc tenant_major_freeze", K_(tenant_id), KP_(tenant_major_freeze),
             K(is_primary_service));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(delete_tenant_major_freeze())) {
      LOG_WARN("fail to delete tenant major freeze", KR(tmp_ret), K_(tenant_id), K(is_primary_service));
    }
    buf = nullptr;
  }
  return ret;
}

int ObMajorFreezeService::delete_tenant_major_freeze()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_major_freeze_)) {
    // no need to delete
  } else if (FALSE_IT(tenant_major_freeze_->stop())) {
  } else if (OB_FAIL(tenant_major_freeze_->wait())) {
    LOG_WARN("fail to wait", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(tenant_major_freeze_->destroy())) {
    LOG_WARN("fail to destroy", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("succ to delete tenant_major_freeze", K_(tenant_id));
  }

  // ignore ret
  if (OB_NOT_NULL(tenant_major_freeze_)) {
    ob_delete(tenant_major_freeze_);
    tenant_major_freeze_ = nullptr;
  }

  LOG_INFO("finish to delete tenant_major_freeze", KR(ret), K_(tenant_id));

  return ret;
}

int ObMajorFreezeService::launch_major_freeze()
{
  int ret = OB_SUCCESS;
  bool can_launch = ATOMIC_BCAS(&is_launched_, false, true);

  if (!can_launch) {
    // 'sync operation' of launch_major_freeze not finish
    ret = OB_MAJOR_FREEZE_NOT_FINISHED;
    LOG_WARN("previous major freeze not finish, please wait", KR(ret), K_(is_launched));
  } else {
    ObRecursiveMutexGuard guard(lock_);
    SpinRLockGuard r_guard(rw_lock_);
    if (OB_ISNULL(tenant_major_freeze_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_major_freeze is null", KR(ret), K_(tenant_id), KP_(tenant_major_freeze));
    } else if (OB_FAIL(tenant_major_freeze_->launch_major_freeze())) {
      // 'async operation' of launch_major_freeze not finish
      if ((OB_MAJOR_FREEZE_NOT_FINISHED != ret) && (OB_FROZEN_INFO_ALREADY_EXIST != ret)) {
        LOG_WARN("fail to launch_major_freeze", KR(ret), K_(tenant_id));
      }
    }
    ATOMIC_STORE(&is_launched_, false); // set is as false no matter its previous value.
  }

  return ret;
}

int ObMajorFreezeService::suspend_merge()
{
  ObRecursiveMutexGuard guard(lock_);
  SpinRLockGuard r_guard(rw_lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_major_freeze_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_major_freeze is null", KR(ret), KP_(tenant_major_freeze));
  } else if (OB_FAIL(tenant_major_freeze_->suspend_merge())) {
    LOG_WARN("fail to suspend_merge", KR(ret));
  }
  return ret;
}

int ObMajorFreezeService::resume_merge()
{
  ObRecursiveMutexGuard guard(lock_);
  SpinRLockGuard r_guard(rw_lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_major_freeze_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_major_freeze is null", KR(ret), KP_(tenant_major_freeze));
  } else if (OB_FAIL(tenant_major_freeze_->resume_merge())) {
    LOG_WARN("fail to resume_merge", KR(ret));
  }
  return ret;
}

int ObMajorFreezeService::clear_merge_error()
{
  ObRecursiveMutexGuard guard(lock_);
  SpinRLockGuard r_guard(rw_lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_major_freeze_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_major_freeze is null", KR(ret));
  } else if (OB_FAIL(tenant_major_freeze_->clear_merge_error())) {
    LOG_WARN("fail to clear_merge_error", KR(ret));
  }
  return ret;
}

int ObMajorFreezeService::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  }
  return ret;
}

void ObMajorFreezeService::stop()
{
  LOG_INFO("major_freeze_service start to stop", K_(tenant_id));
  ObRecursiveMutexGuard guard(lock_);
  SpinRLockGuard r_guard(rw_lock_);
  if (OB_NOT_NULL(tenant_major_freeze_)) {
    LOG_INFO("tenant_major_freeze_ start to stop", K_(tenant_id));
    tenant_major_freeze_->stop();
  }
  LOG_INFO("major_freeze_service finish to stop", K_(tenant_id));
}

void ObMajorFreezeService::wait()
{
  LOG_INFO("major_freeze_service start to wait", K_(tenant_id));
  ObRecursiveMutexGuard guard(lock_);
  SpinRLockGuard r_guard(rw_lock_);
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tenant_major_freeze_)) {
    LOG_INFO("tenant_major_freeze_ start to wait", K_(tenant_id));
    if (OB_FAIL(tenant_major_freeze_->wait())) {
      LOG_WARN("fail to wait", KR(ret), K_(tenant_id));
    }
  }
  LOG_INFO("major_freeze_service finish to wait", K_(tenant_id));
}

void ObMajorFreezeService::destroy()
{
  LOG_INFO("major_freeze_service start to destroy", K_(tenant_id));
  ObRecursiveMutexGuard guard(lock_);
  SpinRLockGuard r_guard(rw_lock_);
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tenant_major_freeze_)) {
    LOG_INFO("tenant_major_freeze_ start to destroy", K_(tenant_id));
    if (OB_FAIL(tenant_major_freeze_->destroy())) {
      LOG_WARN("fail to destroy", KR(ret), K_(tenant_id));
    }
  }
  LOG_INFO("major_freeze_service finish to destroy", K_(tenant_id));
}

bool ObMajorFreezeService::is_paused() const
{
  bool is_paused = true;
  if (OB_NOT_NULL(tenant_major_freeze_)) {
    is_paused = tenant_major_freeze_->is_paused();
  }
  // if tenant_major_freeze_ is null, treat it as paused
  return is_paused;
}

int ObMajorFreezeService::get_uncompacted_tablets(
    ObArray<ObTabletReplica> &uncompacted_tablets,
    ObArray<uint64_t> &uncompacted_table_ids) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    if (OB_ISNULL(tenant_major_freeze_)) {
      ret = OB_LEADER_NOT_EXIST;
      LOG_WARN("tenant_major_freeze is null", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(tenant_major_freeze_->get_uncompacted_tablets(uncompacted_tablets, uncompacted_table_ids))) {
      LOG_WARN("fail to get uncompacted tablets", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////
ObPrimaryMajorFreezeService::ObPrimaryMajorFreezeService() : ObMajorFreezeService()
{}

ObPrimaryMajorFreezeService::~ObPrimaryMajorFreezeService()
{}

int ObPrimaryMajorFreezeService::mtl_init(ObPrimaryMajorFreezeService *&service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(service->init(MTL_ID()))) {
    LOG_WARN("fail to init primary major freeze service", KR(ret));
  }
  return ret;
}

ObMajorFreezeServiceType ObPrimaryMajorFreezeService::get_service_type() const
{
  return ObMajorFreezeServiceType::SERVICE_TYPE_PRIMARY;
}

///////////////////////////////////////////////////////////////////////////////
ObRestoreMajorFreezeService::ObRestoreMajorFreezeService() : ObMajorFreezeService()
{}

ObRestoreMajorFreezeService::~ObRestoreMajorFreezeService()
{}

int ObRestoreMajorFreezeService::mtl_init(ObRestoreMajorFreezeService *&service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(service->init(MTL_ID()))) {
    LOG_WARN("fail to init restore major freeze service", KR(ret));
  }
  return ret;
}

ObMajorFreezeServiceType ObRestoreMajorFreezeService::get_service_type() const
{
  return ObMajorFreezeServiceType::SERVICE_TYPE_RESTORE;
}

} // end namespace rootserver
} // end namespace oceanbase
