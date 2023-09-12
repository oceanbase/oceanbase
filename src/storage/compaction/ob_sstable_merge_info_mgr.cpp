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
#include "storage/compaction/ob_compaction_diagnose.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
/**
 * ------------------------------------------------------------------ObTenantSSTableMergeInfoMgr---------------------------------------------------------------
 */
ObTenantSSTableMergeInfoMgr::ObTenantSSTableMergeInfoMgr()
  : is_inited_(false),
    major_info_pool_(),
    minor_info_pool_()
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

int64_t ObTenantSSTableMergeInfoMgr::cal_max()
{
  const uint64_t tenant_id = MTL_ID();
  int64_t max_size = std::min(lib::get_tenant_memory_limit(tenant_id) * MEMORY_PERCENTAGE / 100,
                          static_cast<int64_t>(POOL_MAX_SIZE));
  return max_size;
}

int ObTenantSSTableMergeInfoMgr::get_next_info(compaction::ObIDiagnoseInfoMgr::Iterator &major_iter,
      compaction::ObIDiagnoseInfoMgr::Iterator &minor_iter,
      ObSSTableMergeInfo &info, char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(major_iter.get_next(&info, buf, buf_len))) {
    if (OB_ITER_END == ret) {
      if (OB_FAIL(minor_iter.get_next(&info, buf, buf_len))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next minor sstable merge info", K(ret));
        }
      }
    } else {
      STORAGE_LOG(WARN, "failed to get next major sstable merge info", K(ret));
    }
  }
  return ret;
}

int ObTenantSSTableMergeInfoMgr::init(const int64_t page_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTenantSSTableMergeInfoMgr has already been initiated", K(ret));
  } else {
    int64_t max_size = cal_max();
    if (OB_FAIL(major_info_pool_.init(false,
                                      MTL_ID(),
                                      "MajorMerge",
                                      page_size,
                                      max_size * (100 - MINOR_MEMORY_PERCENTAGE) / 100))) {
      STORAGE_LOG(WARN, "failed to init major info pool", K(ret));
    } else if (OB_FAIL(minor_info_pool_.init(false,
                                      MTL_ID(),
                                      "MinorMerge",
                                      page_size,
                                      max_size * MINOR_MEMORY_PERCENTAGE / 100))) {
      STORAGE_LOG(WARN, "failed to init minor info pool", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    reset();
  }
  return ret;
}

void ObTenantSSTableMergeInfoMgr::destroy()
{
  if (IS_INIT) {
    reset();
  }
}

void ObTenantSSTableMergeInfoMgr::reset()
{
  major_info_pool_.destroy();
  minor_info_pool_.destroy();
  is_inited_ = false;
  STORAGE_LOG(INFO, "ObTenantSSTableMergeInfoMgr destroy finish");
}

int ObTenantSSTableMergeInfoMgr::open_iter(compaction::ObIDiagnoseInfoMgr::Iterator &major_iter,
      compaction::ObIDiagnoseInfoMgr::Iterator &minor_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantSSTableMergeInfoMgr is not initialized", K(ret));
  } else if (OB_FAIL(major_info_pool_.open_iter(major_iter))) {
    STORAGE_LOG(WARN, "failed to open major iter", K(ret));
  } else if (OB_FAIL(minor_info_pool_.open_iter(minor_iter))) {
    STORAGE_LOG(WARN, "failed to open minor iter", K(ret));
  }
  return ret;
}

int ObTenantSSTableMergeInfoMgr::set_max(int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantSSTableMergeInfoMgr is not init", K(ret));
  } else if (OB_FAIL(major_info_pool_.set_max(max_size * (100 - MINOR_MEMORY_PERCENTAGE) / 100))) {
    STORAGE_LOG(WARN, "failed to resize major info pool", K(ret), "max_size",
        max_size * (100 - MINOR_MEMORY_PERCENTAGE) / 100);
  } else if (OB_FAIL(minor_info_pool_.set_max(max_size * MINOR_MEMORY_PERCENTAGE / 100))) {
    STORAGE_LOG(WARN, "failed to resize minor info pool", K(ret), "max_size",
        max_size * MINOR_MEMORY_PERCENTAGE / 100);
  }
  return ret;
}

int ObTenantSSTableMergeInfoMgr::gc_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantSSTableMergeInfoMgr is not init", K(ret));
  } else if (OB_FAIL(major_info_pool_.gc_info())) {
    STORAGE_LOG(WARN, "failed to gc major info pool", K(ret));
  } else if (OB_FAIL(minor_info_pool_.gc_info())) {
    STORAGE_LOG(WARN, "failed to gc minor info pool", K(ret));
  }
  return ret;
}

int ObTenantSSTableMergeInfoMgr::size()
{
  int size = 0;
  if (IS_INIT) {
    size = minor_info_pool_.size() + major_info_pool_.size();
  }
  return size;
}

int ObTenantSSTableMergeInfoMgr::add_sstable_merge_info(ObSSTableMergeInfo &input_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantSSTableMergeInfoMgr is not initialized", K(ret));
  } else if (OB_UNLIKELY(!input_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(input_info));
  } else {
    compaction::ObIDiagnoseInfoMgr *info_pool = &minor_info_pool_;
    if (input_info.is_major_merge_type()) {
      info_pool = &major_info_pool_;
    }
    if (OB_FAIL(info_pool->alloc_and_add(0, &input_info))) {
      STORAGE_LOG(WARN, "failed to add sstable merge info", K(ret), K(input_info));
    }
  }
  return ret;
}
}//storage
}//oceanbase
