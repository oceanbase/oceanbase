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

#include "storage/tmp_file/ob_i_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{
ObITenantTmpFileManager::ObITenantTmpFileManager()
  : is_inited_(false),
    is_running_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    tmp_file_allocator_(),
    callback_allocator_(),
    files_()
{
}

ObITenantTmpFileManager::~ObITenantTmpFileManager()
{
}

int ObITenantTmpFileManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_INIT)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObITenantTmpFileManager init twice", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_ = MTL_ID()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(files_.init("TmpFileMap", tenant_id_))) {
    LOG_WARN("fail to init tmp files map", KR(ret));
  } else if (OB_FAIL(tmp_file_allocator_.init(common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                              ObModIds::OB_TMP_FILE_MANAGER, tenant_id_,
                                              INT64_MAX))) {
    LOG_WARN("fail to init tmp file allocator", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(callback_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                              OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                              ObMemAttr(tenant_id_, "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init callback allocator", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(init_sub_module_())) {
    LOG_WARN("fail to init sub module", KR(ret), K(tenant_id_));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObITenantTmpFileManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObITenantTmpFileManager has already been started", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(start_sub_module_())) {
    LOG_WARN("fail to start sub module", KR(ret), K(tenant_id_));
  } else {
    is_running_ = true;
  }

  return ret;
}

void ObITenantTmpFileManager::stop()
{
  int ret = OB_SUCCESS;
  if (is_running_) {
    if (OB_FAIL(stop_sub_module_())) {
      LOG_WARN("fail to stop sub module", KR(ret), K(tenant_id_));
    } else {
      is_running_ = false;
    }
  }
}

void ObITenantTmpFileManager::wait()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wait_sub_module_())) {
    LOG_WARN("fail to wait sub module", KR(ret), K(tenant_id_));
  }
}

void ObITenantTmpFileManager::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    if (OB_FAIL(destroy_sub_module_())) {
      LOG_WARN("fail to destroy sub module", KR(ret), K(tenant_id_));
    } else {
      int64_t curr_file_cnt = files_.count();
      if (OB_UNLIKELY(curr_file_cnt > 0)) {
        int ret = OB_SUCCESS;
        TmpFileMap::BlurredIterator iter(files_);
        while (OB_SUCC(ret)) {
          ObTmpFileKey unused_key(ObTmpFileGlobal::INVALID_TMP_FILE_FD);
          ObITmpFileHandle handle;
          if (OB_FAIL(iter.next(unused_key, handle))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next tmp file", KR(ret), K(tenant_id_));
            }
          } else {
            // resource leak
            LOG_ERROR("the tmp file has not been removed when tmp file mgr is destroying", KPC(handle.get()));
          }
        } // end while

        int64_t new_file_cnt = files_.count();
        if (OB_UNLIKELY(new_file_cnt != curr_file_cnt)) {
          LOG_ERROR("there are some operation for tmp files when tmp file mgr is destroying", K(tenant_id_), K(curr_file_cnt));
        }
      }
      files_.destroy();
      tmp_file_allocator_.reset();
      callback_allocator_.reset();
      is_inited_ = false;
    }
  }

  LOG_INFO("ObITenantTmpFileManager destroy", K(tenant_id_), KP(this));
}

int ObITenantTmpFileManager::remove(const int64_t fd)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  int64_t start_remove_ts = ObTimeUtility::current_time();
  LOG_INFO("remove a tmp file start", KR(ret), K(start_remove_ts), K(fd), K(lbt()));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  // } else if (OB_UNLIKELY(!is_running())) {
  //   // some modules remove tmp file when they are destroying.
  //   // at this time, the tmp file mgr is not running because of stop().
  //   // thus, we need to support this case
  //   ret = OB_NOT_RUNNING;
  //   LOG_WARN("ObITenantTmpFileManager is not running", KR(ret), K(is_running_));
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
    ObITmpFile *tmp_file = tmp_file_handle.get();

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
        ob_usleep(100 * 1000); // 100ms
      }
    }
    if (OB_FAIL(tmp_file->release_resource())) {
      LOG_ERROR("fail to release resource", KR(ret), KP(tmp_file), KPC(tmp_file), K(lbt()));
    } else {
      tmp_file_handle.reset();
      tmp_file_allocator_.free(tmp_file);
    }
  }

  LOG_INFO("remove a tmp file over", KR(ret), K(start_remove_ts), K(fd), K(lbt()));
  return ret;
}

int ObITenantTmpFileManager::aio_read(const uint64_t tenant_id,
                                      const ObTmpFileIOInfo &io_info,
                                      ObTmpFileIOHandle &io_handle,
                                      ObITmpFileHandle* file_handle_hint)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  ObITmpFileHandle* tmp_file_handle_ptr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(MTL_ID() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id not match", KR(ret), K(tenant_id), K(MTL_ID()));
  } else if (OB_UNLIKELY(io_handle.is_valid() && !io_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(io_handle));
  } else if (FALSE_IT(io_handle.reset())) {
  } else if (OB_FAIL(get_tmp_file_(io_info.fd_, file_handle_hint, tmp_file_handle_ptr, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_read(tenant_id, io_info))) {
    LOG_WARN("fail to init io handle", KR(ret), K(tenant_id), K(io_info));
  } else if (OB_FAIL(tmp_file_handle_ptr->get()->aio_pread(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio pread", KR(ret), K(io_info), KPC(tmp_file_handle_ptr->get()));
  } else {
    tmp_file_handle_ptr->get()->set_read_stats_vars(io_handle.get_io_ctx(), io_info.size_);
  }

  LOG_DEBUG("aio_read a tmp file over", KR(ret), K(io_info), K(io_handle), KP(tmp_file_handle_ptr));
  return ret;
}

int ObITenantTmpFileManager::aio_pread(const uint64_t tenant_id,
                                       const ObTmpFileIOInfo &io_info,
                                       const int64_t offset,
                                       ObTmpFileIOHandle &io_handle,
                                       ObITmpFileHandle* file_handle_hint)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  ObITmpFileHandle* tmp_file_handle_ptr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(MTL_ID() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id not match", KR(ret), K(tenant_id), K(MTL_ID()));
  } else if (OB_UNLIKELY(io_handle.is_valid() && !io_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(io_handle));
  } else if (FALSE_IT(io_handle.reset())) {
  } else if (OB_FAIL(get_tmp_file_(io_info.fd_, file_handle_hint, tmp_file_handle_ptr, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_pread(tenant_id, io_info, offset))) {
    LOG_WARN("fail to init io handle", KR(ret), K(tenant_id), K(io_info));
  } else if (OB_FAIL(tmp_file_handle_ptr->get()->aio_pread(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio pread", KR(ret), K(io_info), KPC(tmp_file_handle_ptr->get()));
  } else {
    tmp_file_handle_ptr->get()->set_read_stats_vars(io_handle.get_io_ctx(), io_info.size_);
  }

  LOG_DEBUG("aio_pread a tmp file over", KR(ret), K(io_info), K(offset), K(io_handle), KP(tmp_file_handle_ptr));
  return ret;
}

int ObITenantTmpFileManager::read(const uint64_t tenant_id,
                                  const ObTmpFileIOInfo &io_info,
                                  ObTmpFileIOHandle &io_handle,
                                  ObITmpFileHandle* file_handle_hint)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  ObITmpFileHandle* tmp_file_handle_ptr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(MTL_ID() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id not match", KR(ret), K(tenant_id), K(MTL_ID()));
  } else if (OB_UNLIKELY(io_handle.is_valid() && !io_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(io_handle));
  } else if (FALSE_IT(io_handle.reset())) {
  } else if (OB_FAIL(get_tmp_file_(io_info.fd_, file_handle_hint, tmp_file_handle_ptr, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_read(tenant_id, io_info))) {
    LOG_WARN("fail to init io handle", KR(ret), K(tenant_id), K(io_info));
  } else if (OB_FAIL(tmp_file_handle_ptr->get()->aio_pread(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio pread", KR(ret), K(io_info), KPC(tmp_file_handle_ptr->get()));
  } else {
    tmp_file_handle_ptr->get()->set_read_stats_vars(io_handle.get_io_ctx(), io_info.size_);
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait", KR(tmp_ret), K(io_info));
    }
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }

  LOG_DEBUG("read a tmp file over", KR(ret), K(io_info), K(io_handle), KP(tmp_file_handle_ptr));
  return ret;
}

int ObITenantTmpFileManager::pread(const uint64_t tenant_id,
                                   const ObTmpFileIOInfo &io_info,
                                   const int64_t offset,
                                   ObTmpFileIOHandle &io_handle,
                                   ObITmpFileHandle* file_handle_hint)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  ObITmpFileHandle* tmp_file_handle_ptr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(MTL_ID() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id not match", KR(ret), K(tenant_id), K(MTL_ID()));
  } else if (OB_UNLIKELY(io_handle.is_valid() && !io_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(io_handle));
  } else if (FALSE_IT(io_handle.reset())) {
  } else if (OB_FAIL(get_tmp_file_(io_info.fd_, file_handle_hint, tmp_file_handle_ptr, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(io_handle.init_pread(tenant_id, io_info, offset))) {
    LOG_WARN("fail to init io handle", KR(ret), K(tenant_id), K(io_info));
  } else if (OB_FAIL(tmp_file_handle_ptr->get()->aio_pread(io_handle.get_io_ctx()))) {
    LOG_WARN("fail to aio pread", KR(ret), K(io_info), KPC(tmp_file_handle_ptr->get()));
  } else {
    tmp_file_handle_ptr->get()->set_read_stats_vars(io_handle.get_io_ctx(), io_info.size_);
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait", KR(tmp_ret), K(io_info));
      if (OB_TIMEOUT == tmp_ret) {
        io_handle.reset();
      }
    }
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }

  LOG_DEBUG("pread a tmp file over", KR(ret), K(io_info), K(offset), K(io_handle), KP(tmp_file_handle_ptr));
  return ret;
}

// tmp file is always buffer writing, there are no io tasks need to be waited
int ObITenantTmpFileManager::write(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info,
                                   ObITmpFileHandle* file_handle_hint)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  ObITmpFileHandle* tmp_file_handle_ptr = nullptr;
  ObTmpFileIOWriteCtx ctx;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio write, invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(MTL_ID() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id not match", KR(ret), K(tenant_id), K(MTL_ID()));
  } else if (OB_FAIL(get_tmp_file_(io_info.fd_, file_handle_hint, tmp_file_handle_ptr, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(ctx.init(io_info.fd_, io_info.io_desc_,
                              io_info.io_timeout_ms_))) {
    LOG_WARN("failed to init io context", KR(ret), K(io_info));
  } else if (OB_FAIL(ctx.prepare(io_info.buf_, io_info.size_))) {
    LOG_WARN("fail to prepare write context", KR(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle_ptr->get()->write(ctx))) {
    LOG_WARN("fail to write", KR(ret), K(io_info), KPC(tmp_file_handle_ptr->get()));
  } else {
    tmp_file_handle_ptr->get()->set_write_stats_vars(ctx);
  }

  LOG_DEBUG("write a tmp file over", KR(ret), K(io_info), K(ctx));
  return ret;
}

int ObITenantTmpFileManager::truncate(const int64_t fd,
                                      const int64_t offset,
                                      ObITmpFileHandle* file_handle_hint)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  ObITmpFileHandle* tmp_file_handle_ptr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(fd == ObTmpFileGlobal::INVALID_TMP_FILE_FD || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(offset), K(fd));
  } else if (OB_FAIL(get_tmp_file_(fd, file_handle_hint, tmp_file_handle_ptr, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", KR(ret), K(fd));
  } else if (OB_FAIL(tmp_file_handle_ptr->get()->truncate(offset))) {
    LOG_WARN("fail to truncate", KR(ret), K(fd), K(offset), KP(tmp_file_handle_ptr));
  } else {
    LOG_DEBUG("truncate a tmp file over", KR(ret), K(fd), K(offset));
  }
  return ret;
}

// Get tmp file and increase its refcnt
int ObITenantTmpFileManager::get_tmp_file(const int64_t fd, ObITmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(fd == ObTmpFileGlobal::INVALID_TMP_FILE_FD)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd));
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

int ObITenantTmpFileManager::get_tmp_file_(const int64_t fd,
                                           ObITmpFileHandle* file_handle_hint,
                                           ObITmpFileHandle*& file_handle_ptr,
                                           ObITmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(fd == ObTmpFileGlobal::INVALID_TMP_FILE_FD)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd));
  }

  if (OB_SUCC(ret)) {
    if (file_handle_hint != nullptr) {
      if (!file_handle_hint->is_inited() || OB_ISNULL(file_handle_hint->get())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(fd), KPC(file_handle_hint));
      } else if (file_handle_hint->get()->get_fd() != fd) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument",
            KR(ret), K(fd), KPC(file_handle_hint), KPC(file_handle_hint->get()));
      } else {
        file_handle_ptr = file_handle_hint;
      }
    } else {
      if (OB_FAIL(files_.get(ObTmpFileKey(fd), file_handle))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_WARN("tmp file does not exist", KR(ret), K(fd));
        } else {
          LOG_WARN("fail to get tmp file", KR(ret), K(fd));
        }
      } else {
        file_handle_ptr = &file_handle;
      }
    }
  }

  if (OB_SUCC(ret) && (OB_ISNULL(file_handle_ptr) || OB_ISNULL(file_handle_ptr->get()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tmp file pointer",
        KR(ret), K(fd), KP(file_handle_hint), KP(file_handle_ptr));
  }

  return ret;
}

int ObITenantTmpFileManager::get_tmp_file_size(const int64_t fd,
                                               int64_t &size,
                                               ObITmpFileHandle* file_handle_hint)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  ObITmpFileHandle* tmp_file_handle_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(get_tmp_file_(fd, file_handle_hint, tmp_file_handle_ptr, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", KR(ret), K(fd));
  } else {
    size = tmp_file_handle_ptr->get()->get_file_size();
  }

  LOG_DEBUG("get tmp file size", KR(ret), K(fd), K(size));
  return ret;
}

bool ObITenantTmpFileManager::CollectTmpFileKeyFunctor::operator()(
     const ObTmpFileKey &key, const ObITmpFileHandle &tmp_file_handle)
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

int ObITenantTmpFileManager::get_tmp_file_fds(ObIArray<int64_t> &fd_arr)
{
  int ret = OB_SUCCESS;
  CollectTmpFileKeyFunctor func(fd_arr);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(files_.for_each(func))) {
    LOG_WARN("fail to collect tmp file fds", KR(ret));
  }

  return ret;
}

int ObITenantTmpFileManager::get_tmp_file_info(const int64_t fd, ObTmpFileBaseInfo &tmp_file_info)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
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

void ObITenantTmpFileManager::set_compressible_info(const int64_t fd,
                                                     const OB_TMP_FILE_TYPE file_type,
                                                     const int64_t compressible_fd,
                                                     const void* compressible_file)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle file_handle;
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
  } else {
    file_handle.get()->set_compressible_info(file_type, compressible_fd, compressible_file);
  }
}

}  // end namespace tmp_file
}  // end namespace oceanbase
