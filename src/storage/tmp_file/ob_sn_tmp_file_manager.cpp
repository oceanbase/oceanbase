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
#include "storage/tmp_file/ob_tmp_file_cache.h"

namespace oceanbase
{
namespace tmp_file
{
ObSNTenantTmpFileManager::ObSNTenantTmpFileManager()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    last_access_tenant_config_ts_(-1),
    last_meta_mem_limit_(META_DEFAULT_LIMIT),
    tmp_file_allocator_(),
    callback_allocator_(),
    wbp_index_cache_allocator_(),
    wbp_index_cache_bucket_allocator_(),
    files_(),
    tmp_file_block_manager_(),
    page_cache_controller_(tmp_file_block_manager_),
    current_fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    current_dir_id_(ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID)
{
}

ObSNTenantTmpFileManager::~ObSNTenantTmpFileManager()
{
  destroy();
}

int ObSNTenantTmpFileManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSNTenantTmpFileManager init twice", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_ = MTL_ID()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(files_.init("TmpFileMap", tenant_id_))) {
    LOG_WARN("fail to init tmp files map", KR(ret));
  } else if (FALSE_IT(refresh_meta_memory_limit())) {
  } else if (OB_FAIL(tmp_file_block_manager_.init(tenant_id_, last_meta_mem_limit_))) {
    LOG_WARN("fail to init tenant tmp file block manager", KR(ret));
  } else if (OB_FAIL(tmp_file_allocator_.init(common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                              ObModIds::OB_TMP_FILE_MANAGER, tenant_id_,
                                              last_meta_mem_limit_))) {
    LOG_WARN("fail to init tmp file allocator", KR(ret), K(tenant_id_), K(last_meta_mem_limit_));
  } else if (OB_FAIL(callback_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                              OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                              ObMemAttr(tenant_id_, "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init callback allocator", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(wbp_index_cache_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                     OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                     ObMemAttr(tenant_id_, "TmpFileIndCache",
                                                     ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init wbp index cache allocator", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(wbp_index_cache_bucket_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                            OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                            ObMemAttr(tenant_id_, "TmpFileIndCBkt",
                                                            ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init wbp index cache bucket allocator", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(page_cache_controller_.init(*this))) {
    LOG_WARN("fail to init page cache controller", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("ObSNTenantTmpFileManager init successful", K(tenant_id_), KP(this));
  }
  return ret;
}

int ObSNTenantTmpFileManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(page_cache_controller_.start())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to start page cache controller background threads", KR(ret));
  } else {
    LOG_INFO("ObSNTenantTmpFileManager start successful", K(tenant_id_), KP(this));
  }
  return ret;
}

void ObSNTenantTmpFileManager::stop()
{
  page_cache_controller_.stop();
  LOG_INFO("ObSNTenantTmpFileManager stop successful", K(tenant_id_), KP(this));
}

void ObSNTenantTmpFileManager::wait()
{
  page_cache_controller_.wait();
  LOG_INFO("ObSNTenantTmpFileManager wait successful", K(tenant_id_), KP(this));
}

void ObSNTenantTmpFileManager::destroy()
{
  last_access_tenant_config_ts_ = -1;
  last_meta_mem_limit_ = META_DEFAULT_LIMIT;
  page_cache_controller_.destroy();
  files_.destroy();
  tmp_file_block_manager_.destroy();
  tmp_file_allocator_.reset();
  callback_allocator_.reset();
  wbp_index_cache_allocator_.reset();
  wbp_index_cache_bucket_allocator_.reset();
  is_inited_ = false;
  current_fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  current_dir_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;

  LOG_INFO("ObSNTenantTmpFileManager destroy", K(tenant_id_), KP(this));
}

int ObSNTenantTmpFileManager::alloc_dir(int64_t &dir_id)
{
  int ret = OB_SUCCESS;
  dir_id = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else {
    dir_id = ATOMIC_AAF(&current_dir_id_, 1);
  }

  LOG_DEBUG("alloc dir over", KR(ret), K(dir_id), K(lbt()));
  return ret;
}

int ObSNTenantTmpFileManager::open(int64_t &fd, const int64_t &dir_id, const char* const label)
{
  int ret = OB_SUCCESS;
  fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  void *buf = nullptr;
  ObSharedNothingTmpFile *tmp_file = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(buf = tmp_file_allocator_.alloc(sizeof(ObSharedNothingTmpFile),
                                                       lib::ObMemAttr(tenant_id_, "SNTmpFile")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for tmp file",
             KR(ret), K(tenant_id_), K(sizeof(ObSharedNothingTmpFile)));
  } else if (FALSE_IT(tmp_file = new (buf) ObSharedNothingTmpFile())) {
  } else if (FALSE_IT(fd = ATOMIC_AAF(&current_fd_, 1))) {
  } else if (OB_FAIL(tmp_file->init(tenant_id_, fd, dir_id,
                                    &tmp_file_block_manager_, &callback_allocator_,
                                    &wbp_index_cache_allocator_, &wbp_index_cache_bucket_allocator_,
                                    &page_cache_controller_, label))) {
    LOG_WARN("fail to init tmp file", KR(ret), K(fd), K(dir_id));
  } else if (OB_FAIL(files_.insert(ObTmpFileKey(fd), tmp_file))) {
    LOG_WARN("fail to set refactored to tmp file map", KR(ret), K(fd), KP(tmp_file));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_file)) {
    tmp_file->~ObSharedNothingTmpFile();
    tmp_file_allocator_.free(tmp_file);
    tmp_file = nullptr;
  }

  LOG_INFO("open a tmp file over", KR(ret), K(fd), K(dir_id), KP(tmp_file), K(lbt()));
  return ret;
}

int ObSNTenantTmpFileManager::remove(const int64_t fd)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle tmp_file_handle;
  int64_t start_remove_ts = ObTimeUtility::current_time();
  LOG_INFO("remove a tmp file start", KR(ret), K(start_remove_ts), K(fd), K(lbt()));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(files_.erase(ObTmpFileKey(fd), tmp_file_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("erase non-exist tmp file", K(fd), K(lbt()));
    } else {
      LOG_WARN("fail to erase tmp file", KR(ret), K(fd), K(lbt()));
    }
  } else if (OB_ISNULL(tmp_file_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), KP(tmp_file_handle.get()), K(fd));
  } else if (OB_FAIL(tmp_file_handle.get()->delete_file())) {
    LOG_WARN("fail to delete tmp file", KR(ret), K(fd), K(lbt()));
  } else {
    ObSharedNothingTmpFile *tmp_file = tmp_file_handle.get();

    int64_t LOG_WARN_TIMEOUT = 10 * 1000 * 1000;
    int64_t LOG_ERROR_TIMEOUT = 60 * 1000 * 1000;
    while(!tmp_file->can_remove())
    {
      if (start_remove_ts + LOG_ERROR_TIMEOUT < ObTimeUtility::current_time()) {
        LOG_ERROR("wait thread release reference too long",
            K(start_remove_ts), KP(tmp_file), KPC(tmp_file), K(lbt()));
        sleep(10); // 10s
      } else if (start_remove_ts + LOG_WARN_TIMEOUT < ObTimeUtility::current_time()) {
        LOG_WARN("wait thread release reference too long",
            K(start_remove_ts), KP(tmp_file), KPC(tmp_file), K(lbt()));
        sleep(2); // 2s
      } else {
        usleep(100 * 1000); // 100ms
      }
    }
    tmp_file_handle.reset();
    tmp_file_allocator_.free(tmp_file);
  }

  LOG_INFO("remove a tmp file over", KR(ret), K(start_remove_ts), K(fd), K(lbt()));
  return ret;
}

void ObSNTenantTmpFileManager::refresh_meta_memory_limit()
{
  int ret = OB_SUCCESS;
  const int64_t last_access_ts = ATOMIC_LOAD(&last_access_tenant_config_ts_);
  int64_t mem_limit = META_DEFAULT_LIMIT;

  if (last_access_ts < 0 || common::ObClockGenerator::getClock() - last_access_ts > REFRESH_CONFIG_INTERVAL) {
    omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id_));
    const int64_t tenant_mem_limit = lib::get_tenant_memory_limit(tenant_id_);
    if (!config.is_valid() || 0 == tenant_mem_limit || INT64_MAX == tenant_mem_limit) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get tenant config or tenant memory", KR(ret), K(tenant_id_), K(tenant_mem_limit));
    } else {
      const int64_t last_memory_limit = ATOMIC_LOAD(&last_meta_mem_limit_);
      const int64_t limit_percentage_config = config->_temporary_file_meta_memory_limit_percentage;
      const int64_t limit_percentage = 0 == limit_percentage_config ? 70 : limit_percentage_config;
      mem_limit = tenant_mem_limit * limit_percentage / 100;
      if (OB_UNLIKELY(mem_limit <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("memory limit isn't more than 0", KR(ret), K(mem_limit));
      } else if (FALSE_IT(ATOMIC_STORE(&last_access_tenant_config_ts_, common::ObClockGenerator::getClock()))) {
      } else if (mem_limit != last_memory_limit) {
        ATOMIC_STORE(&last_meta_mem_limit_, mem_limit);
        tmp_file_allocator_.set_total_limit(mem_limit);
        tmp_file_block_manager_.get_block_allocator().set_total_limit(mem_limit);
        share::ObTaskController::get().allow_next_syslog();
        LOG_INFO("succeed to refresh tmp file meta memory limit",
            K(tenant_id_), K(last_memory_limit), K(mem_limit), KP(this));
      }
    }
  }
}

int ObSNTenantTmpFileManager::aio_read(const ObTmpFileIOInfo &io_info, ObSNTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle tmp_file_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(io_handle.is_valid() && !io_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(io_handle));
  } else if (FALSE_IT(io_handle.reset())) {
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_read(io_info))) {
    LOG_WARN("fail to init io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->aio_pread(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio pread", KR(ret), K(io_info));
  } else {
    tmp_file_handle.get()->set_read_stats_vars(io_handle.get_io_ctx().is_unaligned_read(),
                                               io_info.size_);
  }

  LOG_DEBUG("aio_read a tmp file over", KR(ret), K(io_info), K(io_handle), KPC(tmp_file_handle.get()));
  return ret;
}

int ObSNTenantTmpFileManager::aio_pread(const ObTmpFileIOInfo &io_info,
                                      const int64_t offset,
                                      ObSNTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle tmp_file_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(io_handle.is_valid() && !io_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(io_handle));
  } else if (FALSE_IT(io_handle.reset())) {
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_pread(io_info, offset))) {
    LOG_WARN("fail to init io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->aio_pread(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio pread", KR(ret), K(io_info));
  } else {
    tmp_file_handle.get()->set_read_stats_vars(io_handle.get_io_ctx().is_unaligned_read(),
                                               io_info.size_);
  }

  LOG_DEBUG("aio_pread a tmp file over", KR(ret), K(io_info), K(offset), K(io_handle), KPC(tmp_file_handle.get()));
  return ret;
}

int ObSNTenantTmpFileManager::read(const ObTmpFileIOInfo &io_info, ObSNTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle tmp_file_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(io_handle.is_valid() && !io_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(io_handle));
  } else if (FALSE_IT(io_handle.reset())) {
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_read(io_info))) {
    LOG_WARN("fail to init io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->aio_pread(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio pread", KR(ret), K(io_info));
  } else {
    tmp_file_handle.get()->set_read_stats_vars(io_handle.get_io_ctx().is_unaligned_read(),
                                               io_info.size_);
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait", KR(tmp_ret), K(io_info));
    }
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }

  LOG_DEBUG("read a tmp file over", KR(ret), K(io_info), K(io_handle), KPC(tmp_file_handle.get()));
  return ret;
}

int ObSNTenantTmpFileManager::pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObSNTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle tmp_file_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(io_handle.is_valid() && !io_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(io_handle));
  } else if (FALSE_IT(io_handle.reset())) {
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_pread(io_info, offset))) {
    LOG_WARN("fail to init io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->aio_pread(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio pread", KR(ret), K(io_info));
  } else {
    tmp_file_handle.get()->set_read_stats_vars(io_handle.get_io_ctx().is_unaligned_read(),
                                               io_info.size_);
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait", KR(tmp_ret), K(io_info));
    }
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }

  LOG_DEBUG("pread a tmp file over", KR(ret), K(io_info), K(offset), K(io_handle), KPC(tmp_file_handle.get()));
  return ret;
}

int ObSNTenantTmpFileManager::aio_write(const ObTmpFileIOInfo &io_info, ObSNTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle tmp_file_handle;
  io_handle.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(io_info));
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_write(io_info))) {
    LOG_WARN("fail to init io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->aio_write(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio write", KR(ret), K(io_info));
  }

  LOG_DEBUG("aio_write a tmp file over", KR(ret), K(io_info), K(io_handle), KPC(tmp_file_handle.get()));
  return ret;
}

// tmp file is always buffer writing, there are no io tasks need to be waited
int ObSNTenantTmpFileManager::write(const ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObSNTmpFileIOHandle io_handle;

  if (OB_FAIL(aio_write(io_info, io_handle))) {
    LOG_WARN("fail to aio write", KR(ret), K(io_info));
  }

  LOG_DEBUG("write a tmp file over", KR(ret), K(io_info), K(io_handle));
  return ret;
}

// attention:
// currently truncate() only release memory, but not release disk space.
// we will support to release disk space in future.
int ObSNTenantTmpFileManager::truncate(const int64_t fd, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle tmp_file_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(offset));
  } else if (OB_FAIL(get_tmp_file(fd, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", KR(ret), K(fd));
  } else if (OB_FAIL(tmp_file_handle.get()->truncate(offset))) {
    LOG_WARN("fail to truncate", KR(ret), K(fd), K(offset));
  } else {
    LOG_INFO("truncate a tmp file over", KR(ret), K(fd), K(offset));
  }
  return ret;
}

// Get tmp file and increase its refcnt
int ObSNTenantTmpFileManager::get_tmp_file(const int64_t fd, ObTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(files_.get(ObTmpFileKey(fd), file_handle))) {
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

int ObSNTenantTmpFileManager::get_tmp_file_size(const int64_t fd, int64_t &size)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle tmp_file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(get_tmp_file(fd, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", KR(ret), K(fd));
  } else {
    size = tmp_file_handle.get()->get_file_size();
  }

  LOG_DEBUG("get tmp file size", KR(ret), K(fd), K(size));
  return ret;
}

int ObSNTenantTmpFileManager::get_macro_block_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tmp_file_block_manager_.get_macro_block_list(macro_id_list))) {
    LOG_WARN("fail to get macro block id list", KR(ret));
  }

  LOG_INFO("get tmp file macro block list", KR(ret), K(macro_id_list.count()));
  return ret;
}

int ObSNTenantTmpFileManager::get_macro_block_count(int64_t &macro_block_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tmp_file_block_manager_.get_macro_block_count(macro_block_count))) {
    LOG_WARN("fail to get macro block id count", KR(ret));
  }

  LOG_INFO("get tmp file macro block count", KR(ret), K(macro_block_count));
  return ret;
}

bool ObSNTenantTmpFileManager::CollectTmpFileKeyFunctor::operator()(
     const ObTmpFileKey &key, const ObTmpFileHandle &tmp_file_handle)
{
  int ret = OB_SUCCESS;
  ObSharedNothingTmpFile *tmp_file_ptr = NULL;

  if (OB_ISNULL(tmp_file_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tmp file pointer", KR(ret), K(key), KP(tmp_file_handle.get()));
  } else if (OB_FAIL(fds_.push_back(key.fd_))) {
    LOG_WARN("failed to push back", KR(ret), K(key));
  }
  return OB_SUCCESS == ret;
}

int ObSNTenantTmpFileManager::get_tmp_file_fds(ObIArray<int64_t> &fd_arr)
{
  int ret = OB_SUCCESS;
  CollectTmpFileKeyFunctor func(fd_arr);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(files_.for_each(func))) {
    LOG_WARN("fail to collect tmp file fds", KR(ret));
  }

  return ret;
}

int ObSNTenantTmpFileManager::get_tmp_file_info(const int64_t fd, ObSNTmpFileInfo &tmp_file_info)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(get_tmp_file(fd, file_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("tmp file not exist", KR(ret), K(fd));
    } else {
      LOG_WARN("fail to get tmp file", KR(ret), K(fd));
    }
  } else if (OB_ISNULL(file_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tmp file pointer", KR(ret), K(fd), KP(file_handle.get()));
  } else if (OB_FAIL(file_handle.get()->copy_info_for_virtual_table(tmp_file_info))) {
    LOG_WARN("failed to copy info for virtual table", KR(ret), K(fd), KPC(file_handle.get()));
  }

  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
