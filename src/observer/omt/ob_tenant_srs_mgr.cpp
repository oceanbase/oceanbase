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

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_tenant_srs_mgr.h"
#include "lib/thread/thread_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::common;

namespace oceanbase
{

namespace omt
{

ObTenantSrsMgr::ObTenantSrsMgr()
    : allocator_("TenantSrs"), sql_proxy_(nullptr), self_(),
      rwlock_(ObLatchIds::SRS_LOCK), tenant_srs_map_(),
      add_tenant_task_(this), del_tenant_task_(this),
      is_inited_(false), is_sys_load_completed_(false),
      schema_service_(nullptr), nonexist_tenant_srs_(allocator_), infinite_plane_()
{
}

ObTenantSrsMgr::~ObTenantSrsMgr()
{
}

ObTenantSrsMgr &ObTenantSrsMgr::get_instance()
{
  static ObTenantSrsMgr ob_tenant_srs_mgr;
  return ob_tenant_srs_mgr;
}

void ObTenantSrsMgr::AddTenantTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  uint32_t delay = DEFAULT_PERIOD;
  if (OB_ISNULL(tenant_srs_mgr_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_ERROR("failed to do add tenant srs task, tenant srs mgr is null", K(ret));
  } else if (!tenant_srs_mgr_->is_sys_schema_ready()) {
    delay = BOOTSTRAP_PERIOD;
  } else if (OB_FAIL(tenant_srs_mgr_->try_to_add_new_tenants())) {
    LOG_WARN("failed to update tenants srs", K(ret));
  }
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::SRS_MGR, *this, delay, false))) {
    LOG_ERROR("failed to schedule add tenant srs task", K(ret));
  }
}

void ObTenantSrsMgr::DeleteTenantTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_srs_mgr_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_ERROR("failed to do add tenant srs task, tenant srs mgr is null", K(ret));
  } else if (!tenant_srs_mgr_->is_sys_schema_ready()) {
    // do nothing
  } else if (OB_FAIL(tenant_srs_mgr_->remove_nonexist_tenants())) {
    LOG_WARN("failed to add nonexist tenants to del list", K(ret));
  } else if (OB_FAIL(tenant_srs_mgr_->delete_nonexist_tenants_srs())) {
    LOG_WARN("failed to delete nonexist tenants", K(ret));
  }

  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::SRS_MGR, *this, DEFAULT_PERIOD, false))) {
    LOG_ERROR("failed to schedule add tenant srs task", K(ret));
  }
}

int ObTenantSrsMgr::init(ObMySQLProxy *sql_proxy, const ObAddr &server,
                         share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_START(lib::TGDefIDs::SRS_MGR))) {
    LOG_WARN("fail to init timer", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
    self_ = server;
    schema_service_ = schema_service;
    is_inited_ = true;
    infinite_plane_.minX_ = INT32_MIN;
    infinite_plane_.minY_ = INT32_MIN;
    infinite_plane_.maxX_ = INT32_MAX;
    infinite_plane_.maxY_ = INT32_MAX;
    if (OB_FAIL(add_tenant_srs(OB_SYS_TENANT_ID))) {
      LOG_WARN("add sys tenant srs info failed", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::SRS_MGR, add_tenant_task_, 0, false))) {
      LOG_WARN("schedule add tenant srs task failed", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::SRS_MGR, del_tenant_task_, 0, false))) {
      LOG_WARN("schedule del tenant srs task failed", K(ret));
    }
  }
  return ret;
}

int ObTenantSrsMgr::get_srs_bounds(uint64_t srid, const ObSrsItem *srs_item,
                                   const ObSrsBoundsItem *&bounds_item)
{
  int ret = OB_SUCCESS;
  if (srid == 0) {
    bounds_item = &infinite_plane_;
  } else if (OB_ISNULL(srs_item)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_ERROR("srs item is null", K(ret));
  } else {
    const ObSrsBoundsItem *tmp_bounds = srs_item->get_bounds();
    if (isnan(tmp_bounds->minX_) || isnan(tmp_bounds->minY_)
        || isnan(tmp_bounds->maxX_) || isnan(tmp_bounds->maxY_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid bounds info", K(ret), K(srid), K(srs_item->get_srid()), K(*tmp_bounds));
    } else {
      bounds_item = tmp_bounds;
    }
  }
  return ret;
}

int ObTenantSrsMgr::remove_nonexist_tenants()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> nonexist_tenants_id;
  {
    share::schema::ObSchemaGetterGuard sys_schema_guard;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID,
                                                                sys_schema_guard))) {
      LOG_WARN("get sys tenant schema guard failed", K(ret));
    }
    DRWLock::RDLockGuard guard(rwlock_);
    TenantSrsMap::const_iterator it = tenant_srs_map_.begin();
    for (; OB_SUCC(ret) && it != tenant_srs_map_.end(); it++) {
      bool is_dropped = false;
      if (OB_FAIL(sys_schema_guard.check_if_tenant_has_been_dropped(it->first, is_dropped))) {
        LOG_WARN("check if tenant has been dropped failed", K(ret));
      } else if (is_dropped && OB_FAIL(nonexist_tenants_id.push_back(it->first))) {
        LOG_WARN("push back failed", K(ret), K(it->first));
      }
    }
  }
  for (uint64_t i = 0; i < nonexist_tenants_id.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(move_tenant_srs_to_nonexist_list(nonexist_tenants_id.at(i)))) {
      LOG_WARN("failed to move tenant srs to nonexist list", K(ret), K(nonexist_tenants_id.at(i)));
    }
  }
  return ret;
}

int ObTenantSrsMgr::delete_nonexist_tenants_srs()
{
  int ret = OB_SUCCESS;
  FOREACH_X(iter, nonexist_tenant_srs_, OB_SUCC(ret)) {
    ObTenantSrs *tenant_srs = *iter;
    if (OB_ISNULL(tenant_srs)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("tenant srs in recycle list is null", K(ret));
    } else {
      if (OB_FAIL(tenant_srs->cancle_update_task())) {
        LOG_WARN("failed to cancle tenant srs update task", K(ret), K(tenant_srs->tenant_id()));
      } else {
        tenant_srs->recycle_old_snapshots();
        tenant_srs->recycle_last_snapshots();
        if (tenant_srs->get_snapshots_size() == 0) {
          if (OB_FAIL(nonexist_tenant_srs_.erase(iter))) {
            LOG_WARN("failed to erase tenant srs from nonexist list", K(ret));
          } else {
            tenant_srs->~ObTenantSrs();
            allocator_.free(tenant_srs);
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantSrsMgr::move_tenant_srs_to_nonexist_list(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantSrs *tenant_srs = nullptr;
  DRWLock::WRLockGuard guard(rwlock_);
  if (is_virtual_tenant_id(tenant_id)) {
    // do nothing
  } else if (OB_FAIL(tenant_srs_map_.get_refactored(tenant_id, tenant_srs))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get tenant tenant_srs failed", K(tenant_id), K(ret));
    }
  } else if (OB_ISNULL(tenant_srs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("tenant srs is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_srs_map_.erase_refactored(tenant_id))) {
    LOG_WARN("erase tenant tenant_srs failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(nonexist_tenant_srs_.push_back(tenant_srs))) {
    LOG_WARN("push back tenant_srs failed", K(ret));
  } else {
    LOG_INFO("drop tenant srs push back succeed", K(tenant_srs->tenant_id()));
  }
  return ret;
}

int ObTenantSrsMgr::try_to_add_new_tenants()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> new_tenants_id;
  {
    share::schema::ObSchemaGetterGuard sys_schema_guard;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("schema service is null", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID,
                                                                sys_schema_guard))) {
      LOG_WARN("get sys tenant schema guard failed", K(ret));
    } else if (OB_FAIL(sys_schema_guard.get_tenant_ids(new_tenants_id))) {
      LOG_WARN("get tenant ids failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_tenants_srs(new_tenants_id))) {
    LOG_WARN("failed to add tenants srs", K(ret), K(new_tenants_id));
  }
  return ret;
}

int ObTenantSrsMgr::add_tenants_srs(const common::ObIArray<uint64_t> &new_tenants_id)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < new_tenants_id.count() && OB_SUCC(ret); i++) {
    uint64_t tenant_id = new_tenants_id.at(i);
    if (OB_FAIL(add_tenant_srs(tenant_id))) {
      LOG_WARN("failed to add tenant srs", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObTenantSrsMgr::add_tenant_srs(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantSrs *const *old_tenant_srs = NULL;
  DRWLock::WRLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant srs mgr not inited", K(ret));
  } else if (is_virtual_tenant_id(tenant_id)
             || OB_NOT_NULL(old_tenant_srs = tenant_srs_map_.get(tenant_id))) {
    LOG_INFO("try to add exist tenant or virtual tenant", K(tenant_id));
  } else {
    void *buf = allocator_.alloc(sizeof(ObTenantSrs));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for tenant srs", K(ret));
    } else {
      ObTenantSrs *new_tenant_srs = new (buf) ObTenantSrs(&allocator_, tenant_id);
      if (OB_FAIL(new_tenant_srs->init(this))) {
        LOG_WARN("failed to init new tenant srs", K(ret));
      } else if (OB_FAIL(tenant_srs_map_.set_refactored(tenant_id, new_tenant_srs, 1))) {
        LOG_WARN("failed to set new tenant srs", K(ret), K(tenant_id));
      }
      if (OB_FAIL(ret)) {
        if (OB_FAIL(new_tenant_srs->cancle_update_task())) {
          LOG_WARN("failed to cancle update srs task", K(ret), K(tenant_id));
        } else {
          new_tenant_srs->~ObTenantSrs();
          allocator_.free(new_tenant_srs);
        }
      }
    }
  }

  return OB_SUCCESS;
}

int ObTenantSrsMgr::refresh_tenant_srs(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_LIKELY(OB_INVALID_ID != tenant_id)) {
    share::schema::ObSchemaGetterGuard sys_schema_guard;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID,
                                                                sys_schema_guard))) {
      LOG_WARN("get sys tenant schema guard failed", K(ret));
    } else if (OB_FAIL(sys_schema_guard.check_tenant_exist(tenant_id, is_exist))) {
      LOG_WARN("get tenant ids failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_exist && OB_FAIL(add_tenant_srs(tenant_id))) {
    LOG_WARN("add tenant timezone failed", K(ret));
  }
  return ret;
}

int ObTenantSrsMgr::get_tenant_guard_inner(uint64_t tenant_id, ObSrsCacheGuard &srs_guard)
{
  int ret = OB_SUCCESS;
  ObTenantSrs *tenant_srs = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (OB_FAIL(tenant_srs_map_.get_refactored(tenant_id, tenant_srs))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get tenant srs", K(ret), K(tenant_id));
    }
  } else if (OB_FAIL(tenant_srs->try_get_last_snapshot(srs_guard))) {
    LOG_WARN("failed to get last srs snapshot", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantSrsMgr::get_tenant_srs_guard(uint64_t tenant_id, ObSrsCacheGuard &srs_guard)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_tenant_guard_inner(tenant_id, srs_guard))) {
    if (ret == OB_HASH_NOT_EXIST) {
      if (OB_FAIL(refresh_tenant_srs(tenant_id))) {
        LOG_WARN("update srs tenant map failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(get_tenant_guard_inner(tenant_id, srs_guard))) {
        LOG_WARN("failed to get tenant srs", K(ret), K(tenant_id));
      }
    } else if (ret == OB_ERR_EMPTY_QUERY) {
      ret = OB_ERR_SRS_EMPTY;
      LOG_WARN("srs table might be empty", K(ret), K(tenant_id));
      LOG_USER_ERROR(OB_ERR_SRS_EMPTY);
    } else {
      LOG_WARN("failed to get tenant srs", K(ret), K(tenant_id));
    }
  }
  return ret;
}

bool ObTenantSrsMgr::is_sys_schema_ready()
{
  return schema_service_->is_sys_full_schema();
}

void ObTenantSrsMgr::destroy()
{
  tenant_srs_map_.destroy();
}


}  // namespace omt
}  // namespace oceanbase