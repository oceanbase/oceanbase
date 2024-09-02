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
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace tmp_file
{
int ObTenantTmpFileManager::mtl_init(ObTenantTmpFileManager *&manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to mtl init tmp file manager, null pointer argument", KR(ret), KP(manager));
  } else if (OB_FAIL(manager->init())) {
    LOG_WARN("fail to init ObTenantTmpFileManager", KR(ret));
  }
  return ret;
}

ObTenantTmpFileManager &ObTenantTmpFileManager::get_instance()
{
  int ret = OB_SUCCESS;
  ObTenantTmpFileManager *tmp_file_manager = MTL(tmp_file::ObTenantTmpFileManager *);
  if (OB_ISNULL(tmp_file_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObTenantTmpFileManager is null, please check whether MTL module is normal", KR(ret));
    ob_abort();
  }

  return *tmp_file_manager;
}

int ObTenantTmpFileManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantTmpFileManager init twice", K(ret), K(is_inited_));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.init())) {
      LOG_WARN("fail to init ss tmp file manager", KR(ret));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.init())) {
      LOG_WARN("fail to init sn tmp file manager", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  LOG_INFO("ObTenantTmpFileManager init success", KR(ret), K(MTL_ID()), K(GCTX.is_shared_storage_mode()));
  return ret;
}

int ObTenantTmpFileManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.start())) {
      LOG_WARN("fail to start ss tmp file manager", KR(ret));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.start())) {
      LOG_WARN("fail to start sn tmp file manager", KR(ret));
    }
  }
  LOG_INFO("ObTenantTmpFileManager start success", KR(ret), K(MTL_ID()), K(GCTX.is_shared_storage_mode()));
  return ret;
}

void ObTenantTmpFileManager::stop()
{
  if (!GCTX.is_shared_storage_mode()) {
    sn_file_manager_.stop();
  }
#ifdef OB_BUILD_SHARED_STORAGE
  else {
    ss_file_manager_.stop();
  }
#endif
  LOG_INFO("ObTenantTmpFileManager stop success", K(MTL_ID()), K(GCTX.is_shared_storage_mode()));
}

void ObTenantTmpFileManager::wait()
{
  if (!GCTX.is_shared_storage_mode()) {
    sn_file_manager_.wait();
  }
#ifdef OB_BUILD_SHARED_STORAGE
  else {
    ss_file_manager_.wait();
  }
#endif
  LOG_INFO("ObTenantTmpFileManager wait success", K(MTL_ID()), K(GCTX.is_shared_storage_mode()));
}

void ObTenantTmpFileManager::destroy()
{
  if (!GCTX.is_shared_storage_mode()) {
    sn_file_manager_.destroy();
  }
#ifdef OB_BUILD_SHARED_STORAGE
  else {
    ss_file_manager_.destroy();
  }
#endif
  is_inited_ = false;
  LOG_INFO("ObTenantTmpFileManager destroy success", K(MTL_ID()), K(GCTX.is_shared_storage_mode()));
}

int ObTenantTmpFileManager::alloc_dir(int64_t &dir_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.alloc_dir(dir_id))) {
      LOG_WARN("fail to alloc dir in ss tmp file manager", KR(ret), K(dir_id));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.alloc_dir(dir_id))) {
      LOG_WARN("fail to alloc dir in sn tmp file manager", KR(ret), K(dir_id));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::open(int64_t &fd, const int64_t &dir_id, const char* const label)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.open(fd, dir_id))) {
      LOG_WARN("fail to open file in ss tmp file manager", KR(ret), K(fd), K(dir_id));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.open(fd, dir_id))) {
      LOG_WARN("fail to open file in sn tmp file manager", KR(ret), K(fd), K(dir_id));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::remove(const int64_t fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.remove(fd))) {
      LOG_WARN("fail to remove file in ss tmp file manager", KR(ret), K(fd));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.remove(fd))) {
      LOG_WARN("fail to remove file in sn tmp file manager", KR(ret), K(fd));
    }
  }
  return ret;
}


int ObTenantTmpFileManager::aio_read(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.aio_read(io_info, io_handle.get_ss_handle()))) {
      LOG_WARN("fail to read file in ss tmp file manager", KR(ret), K(io_info));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.aio_read(io_info, io_handle.get_sn_handle()))) {
      LOG_WARN("fail to read file in sn tmp file manager", KR(ret), K(io_info));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::aio_pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.aio_pread(io_info, offset, io_handle.get_ss_handle()))) {
      LOG_WARN("fail to read file in ss tmp file manager", KR(ret), K(io_info), K(offset));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.aio_pread(io_info, offset, io_handle.get_sn_handle()))) {
      LOG_WARN("fail to read file in sn tmp file manager", KR(ret), K(io_info), K(offset));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::read(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.read(io_info, io_handle.get_ss_handle()))) {
      LOG_WARN("fail to read file in ss tmp file manager", KR(ret), K(io_info));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.read(io_info, io_handle.get_sn_handle()))) {
      LOG_WARN("fail to read file in sn tmp file manager", KR(ret), K(io_info));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.pread(io_info, offset, io_handle.get_ss_handle()))) {
      LOG_WARN("fail to read file in ss tmp file manager", KR(ret), K(io_info), K(offset));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.pread(io_info, offset, io_handle.get_sn_handle()))) {
      LOG_WARN("fail to read file in sn tmp file manager", KR(ret), K(io_info), K(offset));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::aio_write(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.aio_write(io_info, io_handle.get_ss_handle()))) {
      LOG_WARN("fail to write file in ss tmp file manager", KR(ret), K(io_info));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.aio_write(io_info, io_handle.get_sn_handle()))) {
      LOG_WARN("fail to write file in sn tmp file manager", KR(ret), K(io_info));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::write(const ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.write(io_info))) {
      LOG_WARN("fail to write file in ss tmp file manager", KR(ret), K(io_info));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.write(io_info))) {
      LOG_WARN("fail to write file in sn tmp file manager", KR(ret), K(io_info));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::truncate(const int64_t fd, const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.truncate(fd, offset))) {
      LOG_WARN("fail to truncate file in ss tmp file manager", KR(ret), K(fd), K(offset));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.truncate(fd, offset))) {
      LOG_WARN("fail to truncate file in sn tmp file manager", KR(ret), K(fd), K(offset));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::get_tmp_file_size(const int64_t fd, int64_t &file_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_file_manager_.get_tmp_file_size(fd, file_size))) {
      LOG_WARN("fail to get tmp file size in ss tmp file manager", KR(ret), K(fd));
    }
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.get_tmp_file_size(fd, file_size))) {
      LOG_WARN("fail to get tmp file size in sn tmp file manager", KR(ret), K(fd));
    }
  }
  return ret;
}

int ObTenantTmpFileManager::get_tmp_file_fds(ObIArray<int64_t> &fd_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to get_tmp_file_fds in ss mode", KR(ret), K(fd_arr));
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.get_tmp_file_fds(fd_arr))) {
      LOG_WARN("fail to get tmp file fds in sn tmp file manager", KR(ret), K(fd_arr));
    }
  }

  return ret;
}

int ObTenantTmpFileManager::get_tmp_file_info(const int64_t fd, ObSNTmpFileInfo &tmp_file_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTmpFileManager has not been inited", KR(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to get_tmp_file_info in ss mode", KR(ret), K(fd));
#endif
  } else {
    if (OB_FAIL(sn_file_manager_.get_tmp_file_info(fd, tmp_file_info))) {
      LOG_WARN("fail to get tmp file info in sn tmp file manager", KR(ret), K(fd));
    }
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
