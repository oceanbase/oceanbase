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
#include "ob_sstable_merge_info_mgr.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/ob_sstable_struct.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
/**
 * ------------------------------------------------------------------ObSSTableMergeInfoIter-------------------------------------------------------------
 */
ObSSTableMergeInfoIterator::ObSSTableMergeInfoIterator()
  : all_tenants_(NULL/*allocator*/, ObModIds::OB_TENANT_ID_LIST),
    tenant_idx_(0),
    cur_tenant_id_(OB_INVALID_TENANT_ID),
    major_info_idx_(0),
    major_info_cnt_(0),
    minor_info_idx_(0),
    minor_info_cnt_(0),
    is_opened_(false)
{
}

ObSSTableMergeInfoIterator::~ObSSTableMergeInfoIterator()
{
}

int ObSSTableMergeInfoIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(unused_tenant_guard);
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObSSTableMergeInfoIterator has been opened", K(ret));
  } else if (!::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    all_tenants_.push_back(tenant_id);
  } else {
    GCTX.omt_->get_tenant_ids(all_tenants_);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(next_tenant(unused_tenant_guard))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to init tenant iterator", K(ret), K_(minor_info_idx));
    }
  } else {
    is_opened_ = true;
    major_info_idx_ = 0;
    minor_info_idx_ = 0;
  }
  return ret;
}

int ObSSTableMergeInfoIterator::next_tenant(share::ObTenantSwitchGuard &tenant_guard)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (tenant_idx_ >= all_tenants_.size()) {
      ret = OB_ITER_END;
    } else if (FALSE_IT(cur_tenant_id_ = all_tenants_[tenant_idx_++])) {
    } else if (is_virtual_tenant_id(cur_tenant_id_)) {
      // skip virtual tenant
    } else if (OB_FAIL(tenant_guard.switch_to(cur_tenant_id_))) {
      if (OB_TENANT_NOT_IN_SERVER != ret) {
        STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(cur_tenant_id_));
      } else {
        ret = OB_SUCCESS;
        continue;
      }
    } else {
      major_info_idx_ = 0;
      minor_info_idx_ = 0;
      major_info_cnt_ = MTL(storage::ObTenantSSTableMergeInfoMgr *)->get_major_info_array_cnt();
      minor_info_cnt_ = MTL(storage::ObTenantSSTableMergeInfoMgr *)->get_minor_info_array_cnt();
      break;
    }
  }
  return ret;
}

int ObSSTableMergeInfoIterator::get_next_merge_info(ObSSTableMergeInfo &merge_info)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSTableMergeInfoIterator has not been inited", K(ret));
  } else if (OB_FAIL(tenant_guard.switch_to(cur_tenant_id_))) {
    STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(cur_tenant_id_));
  }
  while (OB_SUCC(ret)) {
    if (major_info_idx_ < major_info_cnt_) {
      if (OB_FAIL(MTL(ObTenantSSTableMergeInfoMgr *)->get_major_info(major_info_idx_, merge_info))) {
        STORAGE_LOG(WARN, "Fail to get merge info", K(ret), K_(major_info_idx));
      } else {
        major_info_idx_++;
        break;
      }
    } else if (minor_info_idx_ < minor_info_cnt_) {
      if (OB_FAIL(MTL(ObTenantSSTableMergeInfoMgr *)->get_minor_info(minor_info_idx_, merge_info))) {
        STORAGE_LOG(WARN, "Fail to get merge info", K(ret), K_(minor_info_idx));
      } else {
        minor_info_idx_++;
        break;
      }
    } else if (OB_FAIL(next_tenant(tenant_guard))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to next tenant", K(ret), K_(minor_info_idx));
      }
    }
  } // end of while
  return ret;
}

void ObSSTableMergeInfoIterator::reset()
{
  all_tenants_.reset();
  tenant_idx_ = 0;
  cur_tenant_id_ = OB_INVALID_TENANT_ID;
  major_info_idx_ = 0;
  major_info_cnt_ = 0;
  minor_info_idx_ = 0;
  minor_info_cnt_ = 0;
  is_opened_ = false;
}

/**
 * ------------------------------------------------------------------ObTenantSSTableMergeInfoMgr---------------------------------------------------------------
 */
ObTenantSSTableMergeInfoMgr::ObTenantSSTableMergeInfoMgr()
  : is_inited_(false),
    allocator_(SET_USE_UNEXPECTED_500(ObModIds::OB_SSTABLE_MERGE_INFO), OB_MALLOC_BIG_BLOCK_SIZE),
    major_merge_infos_(allocator_),
    minor_merge_infos_(allocator_)
{
}


ObTenantSSTableMergeInfoMgr::~ObTenantSSTableMergeInfoMgr()
{
  destroy();
}

int ObTenantSSTableMergeInfoMgr::mtl_init(ObTenantSSTableMergeInfoMgr *&sstable_merge_info)
{
  return sstable_merge_info->init();
}

int ObTenantSSTableMergeInfoMgr::init(const int64_t memory_limit)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSSTableMergeInfo has already been initiated", K(ret));
  } else if (OB_UNLIKELY(memory_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(memory_limit));
  } else {
    void *buf = NULL;
    int64_t max_info_cnt = min(GMEMCONF.get_server_memory_limit(), memory_limit) / (sizeof(ObSSTableMergeInfo));
    if (max_info_cnt < 2) {
      max_info_cnt = 2;
    }

    const int64_t info_max_cnt = MAX(max_info_cnt / 2, 1);
    if (OB_FAIL(major_merge_infos_.init(info_max_cnt))) {
      STORAGE_LOG(WARN, "Fail to init major merge infos", K(ret), K(info_max_cnt));
    } else if (OB_FAIL(minor_merge_infos_.init(info_max_cnt))) {
      STORAGE_LOG(WARN, "Fail to alloc minor merge infos", K(ret), K(info_max_cnt));
    } else {
      is_inited_ = true;
      STORAGE_LOG(INFO, "Success to init ObTenantSSTableMergeInfoMgr", K(info_max_cnt));
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObTenantSSTableMergeInfoMgr::destroy()
{
  major_merge_infos_.destroy();
  minor_merge_infos_.destroy();
  allocator_.reset();
  is_inited_ = false;
  STORAGE_LOG(INFO, "ObTenantSSTableMergeInfoMgr is destroyed");
}

int ObTenantSSTableMergeInfoMgr::add_sstable_merge_info(ObSSTableMergeInfo &merge_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantSSTableMergeInfoMgr is not initialized", K(ret));
  } else if (OB_UNLIKELY(!merge_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "merge info is invalid", K(ret), K(merge_info));
  } else if (merge_info.is_major_merge_type()) {
    if (OB_FAIL(major_merge_infos_.add(merge_info))) {
      STORAGE_LOG(WARN, "Fail to add into major merge info manager", K(ret), K(merge_info));
    }
  } else if (OB_FAIL(minor_merge_infos_.add(merge_info))) {
    STORAGE_LOG(WARN, "Fail to add into minor merge info manager", K(ret), K(merge_info));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("success to add into merge info manager", K(ret), K(merge_info));
  }

  return ret;
}

int ObTenantSSTableMergeInfoMgr::get_major_info(const int64_t idx, ObSSTableMergeInfo &merge_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTenantSSTableMergeInfoMgr has not been inited", K(ret));
  } else if (OB_FAIL(major_merge_infos_.get(idx, merge_info))) {
    STORAGE_LOG(WARN, "failed to get info", K(ret), K(idx));
  }
  return ret;
}

int ObTenantSSTableMergeInfoMgr::get_minor_info(const int64_t idx, ObSSTableMergeInfo &merge_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTenantSSTableMergeInfoMgr has not been inited", K(ret));
  } else if (OB_FAIL(minor_merge_infos_.get(idx, merge_info))) {
    STORAGE_LOG(WARN, "failed to get info", K(ret), K(idx));
  }
  return ret;
}

}//storage
}//oceanbase
