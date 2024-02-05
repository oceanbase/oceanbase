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

#include "storage/tenant_snapshot/ob_ls_snapshot_mgr.h"

namespace oceanbase
{
namespace storage
{

int ObLSSnapshotMgr::init(ObTenantMetaSnapshotHandler* meta_handler)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = MTL_ID();
  const char *OB_LS_SNAPSHOT_MGR = "LSSnapshotMgr";
  const int64_t SNAPSHOT_ALLOC_TOTAL_LIMIT = 1024 * 1024 * 1024;

  if (OB_UNLIKELY(IS_INIT)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSSnapshotMgr has inited", KR(ret));
  } else if (OB_ISNULL(meta_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_handler is unexpected nullptr", KR(ret));
  } else if (OB_FAIL(ls_snapshot_map_.init("LSSnapMap", tenant_id))) {
    LOG_WARN("fail to init ls snapshot map", KR(ret));
  } else if (OB_FAIL(build_ctx_allocator_.init(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                 OB_LS_SNAPSHOT_MGR,
                                                 tenant_id,
                                                 SNAPSHOT_ALLOC_TOTAL_LIMIT))) {
    LOG_WARN("fail to init tenant snapshot allocator", KR(ret));
  } else {
    meta_handler_ = meta_handler;
    is_inited_ = true;
    LOG_INFO("ObLSSnapshotMgr init succ", KPC(this));
  }

  return ret;
}

void ObLSSnapshotMgr::destroy()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    int64_t value_cnt = ls_snapshot_map_.get_alloc_handle().get_value_cnt();
    if (0 != value_cnt) {
      LOG_ERROR("ls snapshot value cnt is not zero, memory leak", K(value_cnt));
    }

    int64_t map_cnt = ls_snapshot_map_.count();
    if (0 == map_cnt) {
      ls_snapshot_map_.destroy();
      build_ctx_allocator_.destroy();
      LOG_INFO("ls snapshot mgr destroy succ");
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls snapshot map cnt is not zero", KR(ret), K(map_cnt));
    }
    is_inited_ = false;
  }
}

int ObLSSnapshotMgr::get_ls_snapshot(const ObTenantSnapshotID &tenant_snapshot_id,
                                     const ObLSID &ls_id,
                                     ObLSSnapshot *&ls_snapshot)
{
  int ret = OB_SUCCESS;
  ObLSSnapshot *tmp_ls_snapshot = nullptr;
  ObLSSnapshotMapKey ls_snapshot_key(tenant_snapshot_id, ls_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSSnapshotMgr not inited", KR(ret));
  } else if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is not valid", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_snapshot_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_snapshot_key is not valid", KR(ret), K(ls_snapshot_key));
  } else if (OB_FAIL(ls_snapshot_map_.get(ls_snapshot_key, tmp_ls_snapshot))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get ls snapshot", KR(ret), K(tenant_snapshot_id), K(ls_id));
    }
  } else if (OB_ISNULL(tmp_ls_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_snapshot is unexpected null", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else {
    ls_snapshot = tmp_ls_snapshot;
  }

  return ret;
}

int ObLSSnapshotMgr::create_ls_snapshot_(const ObTenantSnapshotID &tenant_snapshot_id,
                                         const ObLSID &ls_id,
                                         ObLSSnapshot *&ls_snapshot)
{
  int ret = OB_SUCCESS;
  ObLSSnapshotMapKey ls_snapshot_key(tenant_snapshot_id, ls_id);

  ObLSSnapshot* tmp_ls_snapshot = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSSnapshotMgr not inited", KR(ret));
  } else if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is not valid", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_snapshot_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_snapshot_key is not valid", KR(ret), K(ls_snapshot_key));
  } else if (OB_FAIL(ls_snapshot_map_.alloc_value(tmp_ls_snapshot))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc tenant snapshot", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (OB_ISNULL(tmp_ls_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc tenant snapshot", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (OB_FAIL(tmp_ls_snapshot->init(tenant_snapshot_id,
                                           ls_id,
                                           &build_ctx_allocator_,
                                           meta_handler_))) {
    LOG_WARN("fail to init tenant snapshot", KR(ret), K(ls_snapshot_key));
  } else if (OB_FAIL(ls_snapshot_map_.insert_and_get(ls_snapshot_key, tmp_ls_snapshot))) {
    LOG_WARN("fail to insert_and_get tenant snapshot", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else {
    ls_snapshot = tmp_ls_snapshot;
  }

  if (OB_FAIL(ret) && tmp_ls_snapshot != nullptr) {
    ls_snapshot_map_.free_value(tmp_ls_snapshot);
  }
  return ret;
}

int ObLSSnapshotMgr::acquire_ls_snapshot(const ObTenantSnapshotID &tenant_snapshot_id,
                                         const ObLSID &ls_id,
                                         ObLSSnapshot *&ls_snapshot)
{
  int ret = OB_SUCCESS;
  ObLSSnapshot *tmp_ls_snapshot = nullptr;
  bool is_existed = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSSnapshotMgr has not been inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is invalid", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is invalid", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (OB_FAIL(get_ls_snapshot(tenant_snapshot_id, ls_id, tmp_ls_snapshot))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_existed = false;
    } else {
      LOG_WARN("fail to get tenant snapshot", KR(ret), K(tenant_snapshot_id), K(ls_id));
    }
  } else {
    ls_snapshot = tmp_ls_snapshot;
    is_existed = true;
  }

  if (OB_SUCC(ret) && !is_existed) {
    if (OB_FAIL(create_ls_snapshot_(tenant_snapshot_id, ls_id, tmp_ls_snapshot))) {
      LOG_WARN("fail to create ls snapshot", KR(ret), K(tenant_snapshot_id), K(ls_id));
    } else {
     ls_snapshot = tmp_ls_snapshot;
    }
  }

  return ret;
}

int ObLSSnapshotMgr::revert_ls_snapshot(ObLSSnapshot *ls_snapshot)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSSnapshotMgr not inited", KR(ret));
  } else if (OB_ISNULL(ls_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls snapshot is nullptr", KR(ret));
  } else {
    ls_snapshot_map_.revert(ls_snapshot);
  }

  return ret;
}

int ObLSSnapshotMgr::del_ls_snapshot(const ObTenantSnapshotID& tenant_snapshot_id,
                                     const ObLSID& ls_id)
{
  int ret = OB_SUCCESS;
  ObLSSnapshotMapKey ls_snapshot_key(tenant_snapshot_id, ls_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSSnapshotMgr not inited", KR(ret));
  } else if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is not valid", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_snapshot_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_snapshot_key is not valid", KR(ret), K(ls_snapshot_key));
  } else if (OB_FAIL(ls_snapshot_map_.del(ls_snapshot_key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("fail to del ls_snapshot from ls_snapshot_map_",
          KR(ret), K(tenant_snapshot_id), K(ls_id));
    }
  }

  return ret;
}

}
}
