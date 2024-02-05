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

#define USING_LOG_PREFIX STORAGE

#include "storage/tenant_snapshot/ob_ls_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_mgr.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_mgr.h"

namespace oceanbase
{
namespace storage
{

int ObTenantSnapshotMgr::init(ObLSSnapshotMgr* ls_snapshot_mgr,
                              ObTenantMetaSnapshotHandler* meta_handler)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  if (OB_UNLIKELY(IS_INIT)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ob_tenant_snapshot_mgr has inited", KR(ret));
  } else if (OB_ISNULL(ls_snapshot_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_snapshot_mgr is nullptr", KR(ret));
  } else if (OB_ISNULL(meta_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_handler is nullptr", KR(ret));
  } else if (OB_FAIL(tenant_snapshot_map_.init("TSMap", tenant_id))) {
    LOG_WARN("fail to init tenant snapshot map", KR(ret));
  } else {
    ls_snapshot_mgr_ = ls_snapshot_mgr;
    meta_handler_ = meta_handler;
    is_inited_ = true;
    LOG_INFO("tenant snapshot manager thread init succ", KPC(this));
  }

  return ret;
}

class ObTenantSnapshotStopFunctor
{
public:
  bool operator()(const ObTenantSnapshotID &tenant_snapshot_id, ObTenantSnapshot* tenant_snapshot)
  {
    int ret = OB_SUCCESS;

    if (!tenant_snapshot_id.is_valid() || OB_ISNULL(tenant_snapshot)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), KP(tenant_snapshot));
    } else {
      tenant_snapshot->stop();
    }
    return true;
  }
};

void ObTenantSnapshotMgr::stop()
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotStopFunctor fn;
  if (OB_FAIL(tenant_snapshot_map_.for_each(fn))) {
    LOG_ERROR("fail to tenant snapshot stop", KR(ret));
  }
}

class ObTenantSnapshotDestroyFunctor
{
public:
  bool operator()(const ObTenantSnapshotID &tenant_snapshot_id, ObTenantSnapshot* tenant_snapshot)
  {
    int ret = OB_SUCCESS;
    bool bool_ret = false;

    if (!tenant_snapshot_id.is_valid() || OB_ISNULL(tenant_snapshot)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), KP(tenant_snapshot));
    } else {
      tenant_snapshot->destroy();
      bool_ret = true;
    }
    return bool_ret;
  }
};

void ObTenantSnapshotMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ObTenantSnapshotDestroyFunctor fn;
    tenant_snapshot_map_.remove_if(fn);

    int64_t value_cnt = tenant_snapshot_map_.get_alloc_handle().get_value_cnt();
    if (0 != value_cnt) {
      LOG_ERROR("tenant snapshot value cnt is not zero, memory leak", K(value_cnt));
    }

    int64_t map_cnt = tenant_snapshot_map_.count();
    if (0 == map_cnt) {
      tenant_snapshot_map_.destroy();
      LOG_INFO("tenant snapshot mgr destroy succ");
    } else {
      LOG_ERROR("tenant snapshot map cnt is not zero", K(map_cnt));
    }

    is_inited_ = false;
  }
}

int ObTenantSnapshotMgr::get_tenant_snapshot(const ObTenantSnapshotID &tenant_snapshot_id,
                                             ObTenantSnapshot *&tenant_snapshot)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshot *tmp_tenant_snapshot = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotMgr has not been inited.", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is invalid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(tenant_snapshot_map_.get(tenant_snapshot_id, tmp_tenant_snapshot))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TENANT_SNAPSHOT_NOT_EXIST;
    } else {
      LOG_WARN("fail to get tenant_snapshot", KR(ret), K(tenant_snapshot_id));
    }
  } else if (OB_ISNULL(tmp_tenant_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tmp_tenant_snapshot is nullptr", KR(ret), K(tenant_snapshot_id));
  } else {
    tenant_snapshot = tmp_tenant_snapshot;
  }

  return ret;
}

int ObTenantSnapshotMgr::acquire_tenant_snapshot(const ObTenantSnapshotID &tenant_snapshot_id,
                                                 ObTenantSnapshot *&tenant_snapshot)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshot *tmp_tenant_snapshot = nullptr;
  bool is_existed = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotMgr has not been inited.", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is invalid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(get_tenant_snapshot(tenant_snapshot_id, tmp_tenant_snapshot))) {
    if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_existed = false;
    } else {
      LOG_WARN("fail to get tenant snapshot", KR(ret), K(tenant_snapshot_id));
    }
  } else {
    tenant_snapshot = tmp_tenant_snapshot;
    is_existed = true;
  }

  if (OB_SUCC(ret) && !is_existed) {
    if (OB_FAIL(create_tenant_snapshot_(tenant_snapshot_id, tmp_tenant_snapshot))) {
      if (OB_ENTRY_EXIST == ret) {
        LOG_INFO("create a existed tenant snapshot, maybe some concurrent request is processed",
            KR(ret), K(tenant_snapshot_id));
      } else {
        LOG_WARN("fail to create tenant snapshot", KR(ret), K(tenant_snapshot_id));
      }
    } else {
      tenant_snapshot = tmp_tenant_snapshot;
    }
  }

  return ret;
}

int ObTenantSnapshotMgr::revert_tenant_snapshot(ObTenantSnapshot *tenant_snapshot)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotMgr has not been inited", KR(ret), KPC(this));
  } else if (OB_ISNULL(tenant_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant snapshot is nullptr", KR(ret));
  } else {
    tenant_snapshot_map_.revert(tenant_snapshot);
  }
  return ret;
}

int ObTenantSnapshotMgr::create_tenant_snapshot_(const ObTenantSnapshotID &tenant_snapshot_id,
                                                 ObTenantSnapshot *&tenant_snapshot)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  ObTenantSnapshot* tmp_tenant_snapshot = nullptr;
  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is invalid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(tenant_snapshot_map_.alloc_value(tmp_tenant_snapshot))) {
    LOG_WARN("failed to alloc tenant snapshot", KR(ret));
  } else if (OB_ISNULL(tmp_tenant_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc tenant snapshot", KR(ret));
  } else if (OB_FAIL(tmp_tenant_snapshot->init(tenant_snapshot_id, ls_snapshot_mgr_, meta_handler_))) {
    LOG_WARN("failed to init tenant snapshot", KR(ret), KPC(this));
  } else if (OB_FAIL(tenant_snapshot_map_.insert_and_get(tenant_snapshot_id, tmp_tenant_snapshot))) {
    if (OB_ENTRY_EXIST == ret) {
      LOG_INFO("create a existed tenant snapshot, maybe some concurrent request is processed",
          KR(ret), K(tenant_snapshot_id));
    } else {
      LOG_WARN("failed to insert_and_get tenant snapshot", KR(ret), K(tenant_snapshot_id));
    }
  } else {
    tenant_snapshot = tmp_tenant_snapshot;
  }

  if (OB_FAIL(ret) && tmp_tenant_snapshot != nullptr) {
    tenant_snapshot_map_.free_value(tmp_tenant_snapshot);
  }
  return ret;
}

int ObTenantSnapshotMgr::del_tenant_snapshot(const ObTenantSnapshotID& tenant_snapshot_id)
{
  int ret = OB_SUCCESS;

  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is invalid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(tenant_snapshot_map_.del(tenant_snapshot_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("del from map failed", KR(ret), K(tenant_snapshot_id));
    }
  }

  return ret;
}

class ObCheckTenantSnapshotStoppedFunctor
{
public:
  ObCheckTenantSnapshotStoppedFunctor() : has_stopped_tenant_snapshot_(false) {}
  ~ObCheckTenantSnapshotStoppedFunctor() {}

  bool operator()(const ObTenantSnapshotID &tenant_snapshot_id, ObTenantSnapshot* tenant_snapshot)
  {
    int ret = OB_SUCCESS;

    if (!tenant_snapshot_id.is_valid() || OB_ISNULL(tenant_snapshot)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(tenant_snapshot_id), KP(tenant_snapshot));
    } else {
      if (tenant_snapshot->is_stopped()) {
        has_stopped_tenant_snapshot_ = true;
      }
    }
    return true;
  }
public:
  bool has_stopped_tenant_snapshot_;
};

int ObTenantSnapshotMgr::has_tenant_snapshot_stopped(bool& has_tenant_snapshot_stopped)
{
  int ret = OB_SUCCESS;

  ObCheckTenantSnapshotStoppedFunctor fn;

  if (OB_FAIL(tenant_snapshot_map_.for_each(fn))) {
    LOG_ERROR("fail to check tenant snapshot is stopped", KR(ret));
  } else {
    has_tenant_snapshot_stopped = fn.has_stopped_tenant_snapshot_;
  }
  return ret;
}

int ObTenantSnapshotMgr::get_tenant_snapshot_cnt(int64_t& cnt)
{
  int ret = OB_SUCCESS;

  cnt = INT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotMgr has not been inited.", KR(ret), KPC(this));
  } else {
    cnt = tenant_snapshot_map_.count();
  }

  return ret;
}

}
}
