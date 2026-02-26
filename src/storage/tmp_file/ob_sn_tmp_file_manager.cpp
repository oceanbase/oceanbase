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

#include "storage/tmp_file/ob_sn_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{
int64_t ObSNTenantTmpFileManager::current_fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
int64_t ObSNTenantTmpFileManager::current_dir_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;

ObSNTenantTmpFileManager::ObSNTenantTmpFileManager()
  : ObITenantTmpFileManager(),
    write_cache_(),
    tmp_file_block_manager_()
{
}

ObSNTenantTmpFileManager::~ObSNTenantTmpFileManager()
{
}

int ObSNTenantTmpFileManager::init_sub_module_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tmp_file_block_manager_.init(tenant_id_))) {
    LOG_WARN("fail to init tenant tmp file block manager", KR(ret));
  } else if (OB_FAIL(write_cache_.init(&tmp_file_block_manager_))) {
    LOG_WARN("fail to init write cache", KR(ret));
  } else {
    LOG_INFO("ObSNTenantTmpFileManager init successful", K(tenant_id_), KP(this));
  }

  return ret;
}

int ObSNTenantTmpFileManager::start_sub_module_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_cache_.start())) {
    LOG_WARN("fail to init tmp file write cache", KR(ret), K(tenant_id_));
  } else {
    is_running_ = true;
  }
  LOG_INFO("ObSNTenantTmpFileManager start over", KR(ret), K(tenant_id_), KP(this));
  return ret;
}

int ObSNTenantTmpFileManager::stop_sub_module_()
{
  int ret = OB_SUCCESS;
  write_cache_.stop();
  LOG_INFO("ObSNTenantTmpFileManager stop successful", K(tenant_id_), KP(this));
  return ret;
}

int ObSNTenantTmpFileManager::wait_sub_module_()
{
  int ret = OB_SUCCESS;
  write_cache_.wait();
  LOG_INFO("ObSNTenantTmpFileManager wait successful", K(tenant_id_), KP(this));
  return ret;
}

int ObSNTenantTmpFileManager::destroy_sub_module_()
{
  int ret = OB_SUCCESS;
  write_cache_.destroy();
  tmp_file_block_manager_.destroy();
  LOG_INFO("ObSNTenantTmpFileManager destroy", K(tenant_id_), KP(this));
  return ret;
}

int ObSNTenantTmpFileManager::alloc_dir(int64_t &dir_id)
{
  int ret = OB_SUCCESS;
  dir_id = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObSNTenantTmpFileManager is not running", KR(ret), K(is_running_));
  } else {
    dir_id = ATOMIC_AAF(&current_dir_id_, 1);
  }

  LOG_DEBUG("alloc dir over", KR(ret), K(dir_id), K(lbt()));
  return ret;
}

int ObSNTenantTmpFileManager::open(
    int64_t &fd,
    const int64_t &dir_id,
    const char* const label)
{
  int ret = OB_SUCCESS;
  fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  void *buf = nullptr;
  ObSharedNothingTmpFile *tmp_file = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObSNTenantTmpFileManager is not running", KR(ret), K(is_running_));
  } else if (OB_ISNULL(buf = tmp_file_allocator_.alloc(sizeof(ObSharedNothingTmpFile),
                                                       lib::ObMemAttr(tenant_id_, "SNTmpFile")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for tmp file",
             KR(ret), K(tenant_id_), K(sizeof(ObSharedNothingTmpFile)));
  } else if (FALSE_IT(tmp_file = new (buf) ObSharedNothingTmpFile())) {
  } else if (FALSE_IT(fd = ATOMIC_AAF(&current_fd_, 1))) {
  } else if (OB_FAIL(tmp_file->init(tenant_id_, fd, dir_id,
                                    &tmp_file_block_manager_,
                                    &write_cache_,
                                    &callback_allocator_,
                                    label))) {
    LOG_WARN("fail to init tmp file", KR(ret), K(fd), K(dir_id));
  } else if (OB_FAIL(files_.insert(ObTmpFileKey(fd), tmp_file))) {
    LOG_WARN("fail to set refactored to tmp file map", KR(ret), K(fd), KP(tmp_file));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_file)) {
    tmp_file->~ObSharedNothingTmpFile();
    tmp_file_allocator_.free(tmp_file);
    tmp_file = nullptr;
  }

  LOG_INFO("open a tmp file over", KR(ret), K(fd), K(dir_id), KP(tmp_file), KP(this), K(lbt()));
  return ret;
}

// Get tmp file and increase its refcnt
int ObSNTenantTmpFileManager::get_tmp_file(const int64_t fd, ObSNTmpFileHandle &file_handle) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObSNTenantTmpFileManager is not running", KR(ret), K(is_running_));
  } else if (OB_FAIL(files_.get(ObTmpFileKey(fd), file_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("tmp file does not exist", KR(ret), K(fd));
    } else {
      LOG_WARN("fail to get tmp file", KR(ret), K(fd));
    }
  } else if (OB_ISNULL(file_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tmp file pointer", KR(ret), K(fd), KP(file_handle.get()));
  }

  return ret;
}

int ObSNTenantTmpFileManager::get_macro_block_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
// XXX This function must still be available after the tenant is stopped and before it is destroyed.
//  } else if (OB_UNLIKELY(!is_running())) {
//    ret = OB_NOT_RUNNING;
//    LOG_WARN("ObSNTenantTmpFileManager is not running", KR(ret), K(is_running_));
  } else if (OB_FAIL(tmp_file_block_manager_.get_macro_block_list(macro_id_list))) {
    LOG_WARN("fail to get macro block id list", KR(ret));
  }

  LOG_INFO("get tmp file macro block list", KR(ret), K(macro_id_list.count()));
  return ret;
}

int ObSNTenantTmpFileManager::get_tmp_file_disk_usage(int64_t &disk_data_size, int64_t &occupied_disk_size)
{
  int ret = OB_SUCCESS;
  int64_t used_page_num = 0;
  int64_t macro_block_count = 0;
  disk_data_size = 0;
  occupied_disk_size = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
// XXX This function must still be available after the tenant is stopped and before it is destroyed.
//  } else if (OB_UNLIKELY(!is_running())) {
//    ret = OB_NOT_RUNNING;
//    LOG_WARN("ObSNTenantTmpFileManager is not running", KR(ret), K(is_running_));
  } else if (OB_FAIL(tmp_file_block_manager_.get_block_usage_stat(used_page_num, macro_block_count))) {
    LOG_WARN("fail to get block usage stat", KR(ret));
  } else {
    disk_data_size = used_page_num * ObTmpFileGlobal::PAGE_SIZE;
    occupied_disk_size = macro_block_count * ObTmpFileGlobal::SN_BLOCK_SIZE;
  }

  LOG_INFO("get tmp file macro block count", KR(ret), K(used_page_num), K(macro_block_count));
  return ret;
}

int ObSNTenantTmpFileManager::get_suggested_max_tmp_file_num(int64_t& suggested_max_tmp_file_num,
                const int64_t write_cache_size_expected_reside_in_memory)
{
  int ret = OB_SUCCESS;
  suggested_max_tmp_file_num = 0;

  if (write_cache_size_expected_reside_in_memory <= 0) {
    int64_t& write_cache_size_expected_reside_in_memory_mutable_ref =
                  const_cast<int64_t&>(write_cache_size_expected_reside_in_memory);
    write_cache_size_expected_reside_in_memory_mutable_ref = 16 * 1024; // 16KB
  }

  int64_t memory_limit = write_cache_.get_memory_limit();
  if (memory_limit <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected memory limit", KR(ret), K(memory_limit));
  } else {
    suggested_max_tmp_file_num = memory_limit / write_cache_size_expected_reside_in_memory;
    LOG_INFO("suggested max tmp file num by tmp file write buffer pool memory limit",
        KR(ret), K(memory_limit), K(suggested_max_tmp_file_num));
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
