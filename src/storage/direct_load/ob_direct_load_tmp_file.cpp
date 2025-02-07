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

#include "storage/direct_load/ob_direct_load_tmp_file.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadTmpFileHandle
 */

ObDirectLoadTmpFileHandle::ObDirectLoadTmpFileHandle()
  : tmp_file_(nullptr)
{
}

ObDirectLoadTmpFileHandle::~ObDirectLoadTmpFileHandle()
{
  reset();
}

void ObDirectLoadTmpFileHandle::reset()
{
  if (is_valid()) {
    int64_t ref_count = tmp_file_->dec_ref_count();
    if (ref_count == 0) {
      tmp_file_->get_file_mgr()->put_file(tmp_file_);
    }
    tmp_file_ = nullptr;
  }
}

int ObDirectLoadTmpFileHandle::assign(const ObDirectLoadTmpFileHandle &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_file(other.tmp_file_))) {
    LOG_WARN("fail to set file", KR(ret));
  }
  return ret;
}

int ObDirectLoadTmpFileHandle::set_file(ObDirectLoadTmpFile *tmp_file)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(nullptr == tmp_file || !tmp_file->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(tmp_file));
  } else {
    tmp_file->inc_ref_count();
    tmp_file_ = tmp_file;
  }
  return ret;
}

/**
 * ObDirectLoadTmpFilesHandle
 */

ObDirectLoadTmpFilesHandle::ObDirectLoadTmpFilesHandle()
{
  tmp_file_list_.set_tenant_id(MTL_ID());
}

ObDirectLoadTmpFilesHandle::~ObDirectLoadTmpFilesHandle()
{
  reset();
}

void ObDirectLoadTmpFilesHandle::reset()
{
  for (int64_t i = 0; i < tmp_file_list_.count(); ++i) {
    ObDirectLoadTmpFile *tmp_file = tmp_file_list_.at(i);
    int64_t ref_count = tmp_file->dec_ref_count();
    if (ref_count == 0) {
      tmp_file->get_file_mgr()->put_file(tmp_file);
    }
  }
  tmp_file_list_.reset();
}

int ObDirectLoadTmpFilesHandle::assign(const ObDirectLoadTmpFilesHandle &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(tmp_file_list_.assign(other.tmp_file_list_))) {
    LOG_WARN("fail to assign tmp file list", KR(ret));
  } else {
    for (int64_t i = 0; i < tmp_file_list_.count(); ++i) {
      tmp_file_list_.at(i)->inc_ref_count();
    }
  }
  return ret;
}

int ObDirectLoadTmpFilesHandle::add_file(ObDirectLoadTmpFile *tmp_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == tmp_file || !tmp_file->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(tmp_file));
  } else {
    if (OB_FAIL(tmp_file_list_.push_back(tmp_file))) {
      LOG_WARN("fail to push back", KR(ret));
    } else {
      tmp_file->inc_ref_count();
    }
  }
  return ret;
}

int ObDirectLoadTmpFilesHandle::add(const ObDirectLoadTmpFileHandle &tmp_file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tmp_file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tmp_file_handle));
  } else {
    ObDirectLoadTmpFile *tmp_file = tmp_file_handle.get_file();
    if (OB_FAIL(add_file(tmp_file))) {
      LOG_WARN("fail to add file", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTmpFilesHandle::add(const ObDirectLoadTmpFilesHandle &tmp_files_handle)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_files_handle.count(); ++i) {
    ObDirectLoadTmpFile *tmp_file = tmp_files_handle.tmp_file_list_.at(i);
    if (OB_FAIL(add_file(tmp_file))) {
      LOG_WARN("fail to add file", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTmpFilesHandle::get_file(int64_t idx,
                                         ObDirectLoadTmpFileHandle &tmp_file_handle) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx >= tmp_file_list_.count())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("idx overflow", KR(ret), K(idx), K(tmp_file_list_.count()));
  } else if (OB_FAIL(tmp_file_handle.set_file(tmp_file_list_.at(idx)))) {
    LOG_WARN("fail to set file", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadTmpFileIOHandle
 */

ObDirectLoadTmpFileIOHandle::ObDirectLoadTmpFileIOHandle()
  : tmp_file_(nullptr), is_cancel_(false)
{
}

ObDirectLoadTmpFileIOHandle::~ObDirectLoadTmpFileIOHandle()
{
  reset();
}

void ObDirectLoadTmpFileIOHandle::reset()
{
  tmp_file_ = nullptr;
  file_io_handle_.reset();
  file_handle_.reset();
  io_info_.reset();
  is_cancel_ = false;
}

int ObDirectLoadTmpFileIOHandle::open(const ObDirectLoadTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  ObDirectLoadTmpFile *tmp_file = file_handle.get_file();
  int64_t file_size = 0;
  if (OB_UNLIKELY(!file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(file_handle));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
                     MTL_ID(), tmp_file->get_file_id().fd_, file_size))) {
    LOG_WARN("fail to get tmp file size", KR(ret), KPC(tmp_file));
  } else if (OB_UNLIKELY(file_size != tmp_file->get_file_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file size", KR(ret), K(file_size), KPC(tmp_file));
  } else {
    reset();
    if (OB_FAIL(file_handle_.assign(file_handle))) {
      LOG_WARN("fail to assign file handle", KR(ret));
    } else {
      tmp_file_ = tmp_file;
      io_info_.dir_id_ = tmp_file_->get_file_id().dir_id_;
      io_info_.fd_ = tmp_file_->get_file_id().fd_;
      io_info_.io_desc_.set_sys_module_id(ObIOModule::DIRECT_LOAD_IO);
      io_info_.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    }
  }
  return ret;
}

int ObDirectLoadTmpFileIOHandle::check_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("tmp file io canceled", KR(ret));
  } else if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
    ret = OB_TIMEOUT;
    LOG_WARN("worker timeout", KR(ret), K(THIS_WORKER.get_timeout_ts()));
  }
  return ret;
}

int ObDirectLoadTmpFileIOHandle::pread(char *buf, int64_t size, int64_t offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("tmp file not set", KR(ret));
  } else if (OB_UNLIKELY(nullptr == buf || size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(size), K(offset));
  } else if (OB_UNLIKELY(offset + size > tmp_file_->get_file_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read out of file size", KR(ret), KPC_(tmp_file), K(size), K(offset));
  } else {
    int64_t retry_cnt = 0;
    io_info_.size_ = size;
    io_info_.buf_ = buf;
    io_info_.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    io_info_.disable_page_cache_ = true;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(MTL_ID(), io_info_, offset, file_io_handle_))) {
        LOG_WARN("fail to do pread from tmp file", KR(ret), K_(io_info), K(offset));
        if (OB_LIKELY(is_retry_err(ret))) {
          if (++retry_cnt <= MAX_RETRY_CNT) {
            ret = OB_SUCCESS;
            LOG_INFO("retry pread tmp file", K(retry_cnt), K_(io_info), K(size), K(offset));
          }
        } else if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected read out of file size", KR(ret), K_(io_info), K(size), K(offset));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObDirectLoadTmpFileIOHandle::write(char *buf, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("tmp file not set", KR(ret));
  } else if (OB_UNLIKELY(nullptr == buf || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(size));
  } else {
    int64_t retry_cnt = 0;
    io_info_.buf_ = buf;
    io_info_.size_ = size;
    io_info_.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_INDEX_BUILD_WRITE);
    io_info_.disable_page_cache_ = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      }
      // TODO(suzhi.yt): 先保留原来的调用, aio_write提交成功就相当于写成功了
      else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.aio_write(MTL_ID(), io_info_, file_io_handle_))) {
        LOG_WARN("fail to do aio write to tmp file", KR(ret), K_(io_info));
        if (OB_LIKELY(is_retry_err(ret))) {
          if (++retry_cnt <= MAX_RETRY_CNT) {
            ret = OB_SUCCESS;
            int64_t new_file_size = 0;
            if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
                        MTL_ID(), tmp_file_->get_file_id().fd_, new_file_size))) {
              LOG_WARN("fail to get tmp file size", KR(ret), KPC_(tmp_file));
            } else {
              const int64_t write_size = new_file_size - tmp_file_->get_file_size();
              tmp_file_->inc_file_size(write_size);
              io_info_.buf_ += write_size;
              io_info_.size_ -= write_size;
              if (io_info_.size_ > 0) {
                LOG_INFO("retry aio write tmp file", K(retry_cnt), K_(io_info));
              } else {
                break;
              }
            }
          }
        }
      } else {
        tmp_file_->inc_file_size(io_info_.size_);
        break;
      }
    }
  }
  return ret;
}

int ObDirectLoadTmpFileIOHandle::aio_pread(char *buf, int64_t size, int64_t offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("tmp file not set", KR(ret));
  } else if (OB_UNLIKELY(nullptr == buf || size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(size), K(offset));
  } else if (OB_UNLIKELY(offset + size > tmp_file_->get_file_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read out of file size", KR(ret), KPC_(tmp_file), K(size), K(offset));
  } else {
    int64_t retry_cnt = 0;
    io_info_.size_ = size;
    io_info_.buf_ = buf;
    io_info_.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    io_info_.disable_page_cache_ = true;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.aio_pread(MTL_ID(), io_info_, offset, file_io_handle_))) {
        LOG_WARN("fail to do aio pread from tmp file", KR(ret), K_(io_info), K(offset));
        if (OB_LIKELY(is_retry_err(ret))) {
          if (++retry_cnt <= MAX_RETRY_CNT) {
            ret = OB_SUCCESS;
            LOG_INFO("retry aio pread tmp file", K(retry_cnt), K_(io_info), K(offset));
          }
        } else if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected read out of file size", KR(ret), KPC_(tmp_file), K(size), K(offset));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObDirectLoadTmpFileIOHandle::aio_write(char *buf, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("tmp file not set", KR(ret));
  } else if (OB_UNLIKELY(nullptr == buf || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(size));
  } else {
    int64_t retry_cnt = 0;
    io_info_.size_ = size;
    io_info_.buf_ = buf;
    io_info_.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_INDEX_BUILD_WRITE);
    io_info_.disable_page_cache_ = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      }
      // aio_write提交成功就相当于写成功了
      else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.aio_write(MTL_ID(), io_info_, file_io_handle_))) {
        LOG_WARN("fail to do aio write to tmp file", KR(ret), K_(io_info));
        if (OB_LIKELY(is_retry_err(ret))) {
          if (++retry_cnt <= MAX_RETRY_CNT) {
            ret = OB_SUCCESS;
            int64_t new_file_size = 0;
            if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
                        MTL_ID(), tmp_file_->get_file_id().fd_, new_file_size))) {
              LOG_WARN("fail to get tmp file size", KR(ret), KPC_(tmp_file));
            } else {
              const int64_t write_size = new_file_size - tmp_file_->get_file_size();
              tmp_file_->inc_file_size(write_size);
              io_info_.buf_ += write_size;
              io_info_.size_ -= write_size;
              if (io_info_.size_ > 0) {
                LOG_INFO("retry aio write tmp file", K(retry_cnt), K_(io_info));
              } else {
                break;
              }
            }
          }
        }
      } else {
        tmp_file_->inc_file_size(io_info_.size_);
        break;
      }
    }
  }
  return ret;
}

int ObDirectLoadTmpFileIOHandle::wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("tmp file not set", KR(ret));
  } else {
    int64_t retry_cnt = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (file_io_handle_.is_valid() && OB_FAIL(file_io_handle_.wait())) {
        LOG_WARN("fail to wait io finish", KR(ret));
        if (OB_LIKELY(is_retry_err(ret))) {
          if (++retry_cnt <= MAX_RETRY_CNT) {
            ret = OB_SUCCESS;
            LOG_INFO("retry wait tmp file io finish", K(retry_cnt), KPC_(tmp_file),
                     K_(file_io_handle));
          }
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObDirectLoadTmpFileIOHandle::seal()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("tmp file not set", KR(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.seal(MTL_ID(), io_info_.fd_))) {
    LOG_WARN("failed to seal tmp file", KR(ret), K_(io_info));
  }
  return ret;
}

/**
 * ObDirectLoadTmpFileManager
 */

ObDirectLoadTmpFileManager::ObDirectLoadTmpFileManager()
  : is_inited_(false)
{
}

ObDirectLoadTmpFileManager::~ObDirectLoadTmpFileManager()
{
}

int ObDirectLoadTmpFileManager::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadTmpFileManager init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(file_allocator_.init("TLD_FilePool", tenant_id))) {
      LOG_WARN("fail to init allocator", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadTmpFileManager::alloc_dir(int64_t &dir_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTmpFileManager not init", KR(ret), KP(this));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id))) {
    LOG_WARN("fail to alloc dir", KR(ret));
  }
  return ret;
}

int ObDirectLoadTmpFileManager::alloc_file(int64_t dir_id,
                                           ObDirectLoadTmpFileHandle &tmp_file_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTmpFileManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(dir_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(dir_id));
  } else {
    ObDirectLoadTmpFile *tmp_file = nullptr;
    ObDirectLoadTmpFileId file_id;
    file_id.dir_id_ = dir_id;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(MTL_ID(), file_id.fd_, file_id.dir_id_))) {
      LOG_WARN("fail to open file", KR(ret));
    } else if (OB_ISNULL(tmp_file = file_allocator_.alloc(this, file_id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc tmp file", KR(ret));
    } else if (OB_FAIL(tmp_file_handle.set_file(tmp_file))) {
      LOG_WARN("fail to set file", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != tmp_file) {
        file_allocator_.free(tmp_file);
        tmp_file = nullptr;
      }
      if (file_id.is_valid()) {
        FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(MTL_ID(), file_id.fd_);
      }
    }
  }
  return ret;
}

void ObDirectLoadTmpFileManager::put_file(ObDirectLoadTmpFile *tmp_file)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTmpFileManager not init", KR(ret), KP(this));
  } else if (OB_ISNULL(tmp_file) || OB_UNLIKELY(!tmp_file->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(tmp_file));
  } else {
    const int64_t ref_count = tmp_file->get_ref_count();
    if (0 == ref_count) {
      FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(MTL_ID(), tmp_file->get_file_id().fd_);
      file_allocator_.free(tmp_file);
    } else {
      LOG_ERROR("tmp file ref count must be zero", K(ref_count));
    }
  }
}

} // namespace storage
} // namespace oceanbase
