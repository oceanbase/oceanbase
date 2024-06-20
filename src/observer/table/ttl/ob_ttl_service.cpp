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
#include "observer/table/ttl/ob_ttl_service.h"
#include "observer/table/ttl/ob_tenant_ttl_manager.h"
using namespace oceanbase::observer;
namespace oceanbase
{
namespace table
{
int ObTTLService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  FLOG_INFO("ttl service: init", KR(ret), K_(tenant_id));
  return ret;
}
ObTTLService::~ObTTLService()
{
}

int ObTTLService::switch_to_leader()
{
  int64_t start_time_us = ObTimeUtility::current_time();
  FLOG_INFO("ttl_service: start to switch_to_leader", K_(tenant_id), K(start_time_us));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K_(tenant_id));
  } else {
    if (OB_ISNULL(tenant_ttl_mgr_)) {
      if (OB_FAIL(alloc_tenant_ttl_mgr())) {
        LOG_WARN("fail to alloc tenant_ttl_mgr", KR(ret), K_(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      if (!has_start_) {
        if (OB_FAIL(tenant_ttl_mgr_->start())) {
          LOG_WARN("fail to start tenant_ttl_mgr", K_(tenant_id), KR(ret));
        } else {
          has_start_ = true;
        }
      } else {
        tenant_ttl_mgr_->resume();
      }
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("ttl_service: finish switch_to_leader", KR(ret), K_(tenant_id), K(cost_us), KP_(tenant_ttl_mgr));
  return ret;
}

int ObTTLService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  inner_switch_to_follower();
  return ret;
}

void ObTTLService::switch_to_follower_forcedly()
{
  inner_switch_to_follower();
}

void ObTTLService::inner_switch_to_follower()
{
  FLOG_INFO("ttl_service: switch_to_follower", K_(tenant_id));
  const int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_NOT_NULL(tenant_ttl_mgr_)) {
    tenant_ttl_mgr_->pause();
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("ttl_service: switch_to_follower", K_(tenant_id), K(cost_us), KP_(tenant_ttl_mgr));
}

int ObTTLService::launch_ttl_task(const obrpc::ObTTLRequestArg &req)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret));
  } else {
    if (OB_ISNULL(tenant_ttl_mgr_)) {
      ret = OB_EAGAIN;
      LOG_WARN("tenant_ttl_mgr is null, need retry", KR(ret), K_(tenant_id), KP_(tenant_ttl_mgr));
    } else if (OB_FAIL(tenant_ttl_mgr_->handle_user_ttl(req))) {
      LOG_WARN("fail to handle user ttl", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

void ObTTLService::stop()
{
  FLOG_INFO("ttl_service: start to stop", K_(tenant_id));
  if (OB_NOT_NULL(tenant_ttl_mgr_)) {
    LOG_INFO("tenant_ttl_mgr start to stop", K_(tenant_id));
    tenant_ttl_mgr_->stop();
  }
  FLOG_INFO("ttl_service: finish to stop", K_(tenant_id));
}

void ObTTLService::wait()
{
  FLOG_INFO("ttl_service: start to wait", K_(tenant_id));
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tenant_ttl_mgr_)) {
    LOG_INFO("tenant_ttl_mgr start to wait", K_(tenant_id));
    tenant_ttl_mgr_->wait();
  }
  FLOG_INFO("ttl_service: finish to wait", K_(tenant_id));
}
void ObTTLService::destroy()
{
  FLOG_INFO("ttl_service: start to destroy", K_(tenant_id));
  delete_tenant_ttl_mgr();
  FLOG_INFO("ttl_service: finish to destroy", K_(tenant_id));
}
int ObTTLService::alloc_tenant_ttl_mgr()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  int64_t len = sizeof(ObTenantTTLManager);
  ObMemAttr attr(MTL_ID(), "tenant_ttl_mgr");
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret));
  } else if (OB_NOT_NULL(tenant_ttl_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_ttl_mgr is not null", K_(tenant_id), KR(ret), KP_(tenant_ttl_mgr));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObTenantTTLManager), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(tenant_id), K(len));
  } else if (FALSE_IT(tenant_ttl_mgr_ = new(buf)ObTenantTTLManager())) {
  } else if (OB_FAIL(tenant_ttl_mgr_->init(tenant_id_, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to init tenant_ttl_mgr", K_(tenant_id), KR(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to alloc tenant_ttl_mgr", K_(tenant_id), KP_(tenant_ttl_mgr));
  } else {
    delete_tenant_ttl_mgr();
  }
  return ret;
}

void ObTTLService::delete_tenant_ttl_mgr()
{
  if (OB_ISNULL(tenant_ttl_mgr_)) {
    // no need to delete
  } else {
    tenant_ttl_mgr_->destroy();
    OB_DELETE(ObTenantTTLManager, "tenant_ttl_mgr", tenant_ttl_mgr_);
    tenant_ttl_mgr_ = nullptr;
  }
  LOG_INFO("finish to delete tenant_ttl_mgr", K_(tenant_id));
}

int ObTTLService::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObTTLService::mtl_init(ObTTLService *&service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(service->init(MTL_ID()))) {
    LOG_WARN("fail to ini ttl service", KR(ret));
  }
  return ret;
}
}
}
