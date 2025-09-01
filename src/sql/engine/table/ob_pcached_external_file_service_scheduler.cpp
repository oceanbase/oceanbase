/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "share/backup/ob_backup_io_adapter.h"
#include "ob_pcached_external_file_service_scheduler.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "share/ob_thread_mgr.h"
#include "ob_pcached_external_file_service.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace sql
{

/**
 * Check if the macro_id is valid for external table operations
 *
 * In SN (Shared Nothing) mode, macro_id does not have storage object type distinction,
 * so we only check if the macro_id is valid.
 * In SS (Shared Storage) mode, we additionally check if the storage object type
 * is EXTERNAL_TABLE_FILE to ensure it's specifically for external table operations.
 *
 * @param macro_id The macro block ID to validate
 * @return true if the macro_id is valid for external table operations, false otherwise
 */
bool is_valid_external_macro_id(const blocksstable::MacroBlockId &macro_id)
{
  return macro_id.is_valid()
      && (!GCTX.is_shared_storage_mode() || ObStorageObjectType::EXTERNAL_TABLE_FILE == macro_id.storage_object_type());
}

int ext_async_read_from_object_storage(
    const common::ObString &url,
    const common::ObObjectStorageInfo *info,
    char *read_buf,
    const int64_t read_offset,
    const int64_t read_size,
    common::ObIOHandle &read_handle)
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);
  ObIODevice *device_handle = nullptr;
  ObIOFd fd;
  ObIOInfo io_info;

  if (OB_ISNULL(info) || OB_ISNULL(read_buf)
      || OB_UNLIKELY(url.empty() || !info->is_valid() || read_offset < 0 || read_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(url), KPC(info), KP(read_buf), K(read_offset), K(read_size));
  } else if (OB_FAIL(ObExternalIoAdapter::open_with_access_type(
      device_handle, fd, info, url,
      ObStorageAccessType::OB_STORAGE_ACCESS_READER,
      ObStorageIdMod::get_default_external_id_mod()))) {
    LOG_WARN("fail to open fd for read", KR(ret),
        K(url), KPC(info), KP(read_buf), K(read_offset), K(read_size));
  } else if (OB_FAIL(ObExternalIoAdapter::basic_init_read_info(
      *device_handle, fd,
      read_buf, read_offset, read_size, OB_INVALID_ID/*sys_module_id*/, io_info))) {
    LOG_WARN("fail to init read info", KR(ret),
        K(url), KPC(info), KP(read_buf), K(read_offset), K(read_size));
  } else if (FALSE_IT(io_info.flag_.set_need_close_dev_and_fd())) {
  } else if (OB_FAIL(ObExternalIoAdapter::async_pread_with_io_info(io_info, read_handle))) {
    LOG_WARN("fail to async pread", KR(ret),
        K(url), KPC(info), KP(read_buf), K(read_offset), K(read_size));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(device_handle)) {
    ObExternalIoAdapter::close_device_and_fd(device_handle, fd);
  }
  return ret;
}

int ext_async_write_to_local(
    const char *write_buf,
    const int64_t write_size,
    ObStorageObjectHandle &write_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_buf) || OB_UNLIKELY(write_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(write_buf), K(write_size));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = write_size;
    write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.set_is_write_cache(false); // preread macro is read cache

    if (OB_FAIL(write_handle.async_write(write_info))) {
      LOG_WARN("fail to async write", KR(ret),
          K(write_info), KP(write_buf), K(write_size), K(write_handle));
    }
  }
  return ret;
}

/*-----------------------------------------ObExternalDataPrefetchTaskInfo-----------------------------------------*/
ObExternalDataPrefetchTaskInfo::ObExternalDataPrefetchTaskInfo()
    : TaskInfoWithRWHandle(),
      allocator_(),
      operation_type_(PrefetchOperationType::MAX_TYPE),
      macro_id_(),
      storage_info_(nullptr),
      read_offset_(-1),
      data_(nullptr)
{
  ObMemAttr attr(MTL_ID(), "ExtPrefetchTask");
  SET_IGNORE_MEM_VERSION(attr);
  allocator_.set_attr(attr);
  url_[0] = '\0';
  buf_size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
}

int ObExternalDataPrefetchTaskInfo::assign(const ObExternalDataPrefetchTaskInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(other));
    } else if (OB_FAIL(TaskInfoWithRWHandle::assign(other))) {
      LOG_WARN("fail to assign base", KR(ret), K(other));
    } else {
      operation_type_ = other.operation_type_;
      macro_id_ = other.macro_id_;
    }

    if (OB_FAIL(ret)) {
    } else if (PrefetchOperationType::READ_THEN_WRITE == operation_type_) {
      if (OB_FAIL(other.storage_info_->clone(allocator_, storage_info_))) {
        LOG_WARN("fail to deep clone storage info", KR(ret), K(other));
      } else {
        MEMCPY(url_, other.url_, sizeof(url_));
        read_offset_ = other.read_offset_;
      }
    } else if (PrefetchOperationType::DIRECT_WRITE == operation_type_) {
      if (OB_ISNULL(data_ = static_cast<char *>(allocator_.alloc(other.buf_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(other));
      } else {
        MEMCPY(data_, other.data_, other.buf_size_);
      }
    }
  }
  return ret;
}

void ObExternalDataPrefetchTaskInfo::reset()
{
  // data_ is allocated by allocator_, no need to free it
  data_ = nullptr;
  if (OB_NOT_NULL(storage_info_)) {
    OB_DELETEx(ObObjectStorageInfo, &allocator_, storage_info_);
    storage_info_ = nullptr;
  }
  url_[0] = '\0';
  read_offset_ = -1;
  macro_id_.reset();
  operation_type_ = PrefetchOperationType::MAX_TYPE;
  allocator_.reset();
  TaskInfoWithRWHandle::reset();
}

bool ObExternalDataPrefetchTaskInfo::is_valid() const
{
  bool bret = is_valid_external_macro_id(macro_id_)
      && buf_size_ == OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  if (!bret) {
  } else if (PrefetchOperationType::READ_THEN_WRITE == operation_type_) {
    bret &= (url_[0] != '\0'
      && OB_NOT_NULL(storage_info_) && storage_info_->is_valid()
      && read_offset_ >= 0
      && TaskInfoWithRWHandle::is_valid());
  } else if (PrefetchOperationType::DIRECT_WRITE == operation_type_) {
    bret &= (OB_NOT_NULL(data_) && ObStorageIOPipelineTaskInfo::is_valid());
    if (bret && state_ >= TASK_WRITE_DONE) {
      bret &= write_handle_.is_valid();
    }
  } else {
    bret = false;
  }

  return bret;
}

// internal func, skips validation check
int ObExternalDataPrefetchTaskInfo::refresh_read_state_()
{
  int ret = OB_SUCCESS;
  if (TASK_READ_IN_PROGRESS == state_) {
    if (PrefetchOperationType::DIRECT_WRITE == operation_type_) {
      state_ = TASK_READ_DONE;
    } else {
      if (OB_FAIL(TaskInfoWithRWHandle::refresh_read_state_())) {
        LOG_WARN("fail to refresh read state", KR(ret), KPC(this));
      }
    }
  }
  return ret;
}

int ObExternalDataPrefetchTaskInfo::init(
    const common::ObString &url,
    const common::ObObjectStorageInfo *info,
    const int64_t read_offset,
    const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this), K(url), KPC(info), K(read_offset), K(macro_id));
  } else if (OB_ISNULL(info)
      || OB_UNLIKELY(url.empty() || read_offset < 0 || !macro_id.is_valid() || !info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(url), KPC(info), K(read_offset), K(macro_id));
  } else if (OB_FAIL(set_macro_block_id(write_handle_, macro_id))) {
    LOG_WARN("fail to set macro id", KR(ret), K(url), KPC(info), K(read_offset), K(macro_id));
  } else if (OB_FAIL(databuff_printf(url_, sizeof(url_), "%.*s", url.length(), url.ptr()))) {
    LOG_WARN("fail to deep copy url", KR(ret), K(url), KPC(info), K(read_offset), K(macro_id));
  } else if (OB_FAIL(info->clone(allocator_, storage_info_))) {
    LOG_WARN("fail to deep copy storage info", KR(ret),
        K(url), KPC(info), K(read_offset), K(macro_id));
  } else {
    read_offset_ = read_offset;
    macro_id_ = macro_id;
    operation_type_ = PrefetchOperationType::READ_THEN_WRITE;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret),
          K(url), KPC(info), K(read_offset), K(macro_id), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObExternalDataPrefetchTaskInfo::init(
    const char *data,
    const int64_t data_size,
    const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(macro_id));
  } else if (OB_ISNULL(data)
      || OB_UNLIKELY(data_size != buf_size_ || !is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data), K(data_size), K(macro_id));
  } else if (OB_FAIL(set_macro_block_id(write_handle_, macro_id))) {
    LOG_WARN("fail to set macro id", KR(ret), K(macro_id));
  } else if (OB_ISNULL(data_ = static_cast<char *>(allocator_.alloc(data_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(data_size));
  } else {
    MEMCPY(data_, data, data_size);
    macro_id_ = macro_id;
    operation_type_ = PrefetchOperationType::DIRECT_WRITE;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(data), K(data_size), K(macro_id), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

/*-----------------------------------------ObSSExternalDataPrefetchTaskInfo-----------------------------------------*/
uint64_t ObSSExternalDataPrefetchTaskInfo::hash() const
{
  return macro_id_.hash();
}

/*-----------------------------------------ObSNExternalDataPrefetchTaskInfo-----------------------------------------*/
ObSNExternalDataPrefetchTaskInfo::ObSNExternalDataPrefetchTaskInfo()
    : ObExternalDataPrefetchTaskInfo(),
      content_digest_(MTL_ID()),
      modify_time_(0)
{
}

int ObSNExternalDataPrefetchTaskInfo::assign(const ObSNExternalDataPrefetchTaskInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(other));
    } else if (OB_FAIL(ObExternalDataPrefetchTaskInfo::assign(other))) {
      LOG_WARN("fail to assign base", KR(ret), K(other));
    } else if (OB_FAIL(content_digest_.assign(other.content_digest_))) {
      LOG_WARN("fail to assign content digest", KR(ret), K(other));
    } else {
      MEMCPY(url_, other.url_, sizeof(url_));
      read_offset_ = other.read_offset_;
      modify_time_ = other.modify_time_;
    }
  }
  return ret;
}

void ObSNExternalDataPrefetchTaskInfo::reset()
{
  modify_time_ = 0;
  content_digest_.reset();
  ObExternalDataPrefetchTaskInfo::reset();
}

bool ObSNExternalDataPrefetchTaskInfo::is_valid() const
{
  return url_[0] != '\0'
      && read_offset_ >= 0
      && (!content_digest_.empty() || modify_time_ > 0)
      && ObExternalDataPrefetchTaskInfo::is_valid();
}

int ObSNExternalDataPrefetchTaskInfo::init(
    const common::ObString &url,
    const common::ObObjectStorageInfo *info,
    const int64_t read_offset,
    const blocksstable::MacroBlockId &macro_id,
    const storage::ObExtFileVersion &file_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this),
        K(url), KPC(info), K(read_offset), K(macro_id), K(file_version));
  } else if (OB_ISNULL(info)
      || OB_UNLIKELY(url.empty() || !info->is_valid() || read_offset < 0)
      || OB_UNLIKELY(!file_version.is_valid() || !is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(url), KPC(info), K(read_offset), K(macro_id), K(file_version));
  } else if (OB_FAIL(content_digest_.set(file_version.content_digest()))) {
    LOG_WARN("fail to assign content digest", KR(ret),
        K(url), KPC(info), K(read_offset), K(macro_id), K(file_version));
  } else {
    modify_time_ = file_version.modify_time();

    if (OB_FAIL(ObExternalDataPrefetchTaskInfo::init(url, info, read_offset, macro_id))) {
      LOG_WARN("fail to init base", KR(ret),
          K(url), KPC(info), K(read_offset), K(macro_id), K(file_version));
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSNExternalDataPrefetchTaskInfo::init(
    const char *data,
    const int64_t data_size,
    const common::ObString &url,
    const int64_t read_offset,
    const blocksstable::MacroBlockId &macro_id,
    const storage::ObExtFileVersion &file_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this), K(macro_id), K(file_version), K(url), K(read_offset));
  } else if (OB_ISNULL(data) || OB_UNLIKELY(url.empty() || read_offset < 0)
      || OB_UNLIKELY(!file_version.is_valid())
      || OB_UNLIKELY(data_size != buf_size_ || !is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        KP(data), K(data_size), K(macro_id), K(file_version), K(url), K(read_offset));
  } else if (OB_FAIL(databuff_printf(url_, sizeof(url_), "%.*s", url.length(), url.ptr()))) {
    LOG_WARN("fail to deep copy url", KR(ret), K(url), K(read_offset), K(macro_id));
  } else if (OB_FAIL(content_digest_.set(file_version.content_digest()))) {
    LOG_WARN("fail to assign content digest", KR(ret),
        K(url), K(read_offset), K(macro_id), K(file_version));
  } else {
    read_offset_ = read_offset;
    modify_time_ = file_version.modify_time();

    if (OB_FAIL(ObExternalDataPrefetchTaskInfo::init(data, data_size, macro_id))) {
      LOG_WARN("fail to init base", KR(ret), K(url), K(read_offset),
          KP(data), K(data_size), K(macro_id), K(file_version));
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

uint64_t ObSNExternalDataPrefetchTaskInfo::hash() const
{
  uint64_t hash_val = content_digest_.hash();
  hash_val = murmurhash(url_, sizeof(url_), hash_val);
  hash_val = murmurhash(&read_offset_, sizeof(read_offset_), hash_val);
  hash_val = murmurhash(&modify_time_, sizeof(modify_time_), hash_val);
  return hash_val;
}

/*-----------------------------------------ObExternalDataPrefetchPipeline-----------------------------------------*/
template<typename TaskInfoType_>
ObExternalDataPrefetchPipeline<TaskInfoType_>::ObExternalDataPrefetchPipeline()
    : ObStorageIOPipeline<TaskInfoType_>()
{}

template<typename TaskInfoType_>
ObExternalDataPrefetchPipeline<TaskInfoType_>::~ObExternalDataPrefetchPipeline()
{}

template<typename TaskInfoType_>
int ObExternalDataPrefetchPipeline<TaskInfoType_>::init(const uint64_t tenant_id)
{
  return ObStorageIOPipeline<TaskInfoType_>::basic_init_(tenant_id);
}

template<typename TaskInfoType_>
int ObExternalDataPrefetchPipeline<TaskInfoType_>::on_task_failed_(TaskType &task)
{
  // when task failed, task may be invalid, remove it from running map whatever
  // so do not check task.is_valid() here
  int ret = OB_SUCCESS;
  ObPCachedExternalFileService *ext_file_service = nullptr;
  if (OB_FAIL(ObStorageIOPipeline<TaskInfoType_>::check_init_and_stop_())) {
  } else if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is NULL", KR(ret), K(task));
  } else if (OB_FAIL(ext_file_service->finish_running_prefetch_task(task.info_.hash()))) {
    LOG_WARN("fail to finish running prefetch task", KR(ret), K(task));
  }
  return ret;
}

template<typename TaskInfoType_>
int ObExternalDataPrefetchPipeline<TaskInfoType_>::on_task_succeeded_(TaskType &task)
{
  // when task succeeded, task must be valid, no need to check task.is_valid() here
  int ret = OB_SUCCESS;
  ObPCachedExternalFileService *ext_file_service = nullptr;
  if (OB_FAIL(ObStorageIOPipeline<TaskInfoType_>::check_init_and_stop_())) {
  } else if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is NULL", KR(ret), K(task));
  } else if (OB_FAIL(ext_file_service->finish_running_prefetch_task(task.info_.hash()))) {
    LOG_WARN("fail to finish running prefetch task", KR(ret), K(task));
  }
  return ret;
}

template<typename TaskInfoType_>
int ObExternalDataPrefetchPipeline<TaskInfoType_>::do_async_read_(TaskType &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageIOPipeline<TaskInfoType_>::check_init_and_stop_())) {
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (ObExternalDataPrefetchTaskInfo::PrefetchOperationType::DIRECT_WRITE ==
      task.info_.operation_type_) {
    // do nothing
  } else if (OB_FAIL(ext_async_read_from_object_storage(
      task.info_.url_,
      task.info_.storage_info_,
      task.buf_,
      task.info_.read_offset_,
      task.info_.get_buf_size(),
      task.info_.read_handle_.get_io_handle()))) {
    LOG_WARN("fail to async read from object storage", KR(ret), K(task));
  }
  return ret;
}

template<typename TaskInfoType_>
int ObExternalDataPrefetchPipeline<TaskInfoType_>::do_async_write_(TaskType &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageIOPipeline<TaskInfoType_>::check_init_and_stop_())) {
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else {
    const char *buf = nullptr;
    int64_t data_size = -1;
    if (ObExternalDataPrefetchTaskInfo::PrefetchOperationType::READ_THEN_WRITE ==
        task.info_.operation_type_) {
      buf = task.info_.read_handle_.get_buffer();
      data_size = task.info_.read_handle_.get_data_size();
    } else if (ObExternalDataPrefetchTaskInfo::PrefetchOperationType::DIRECT_WRITE ==
        task.info_.operation_type_) {
      buf = task.info_.data_;
      data_size = task.info_.get_buf_size();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operation type", KR(ret), K(task));
    }

    if (OB_SUCC(ret)) {
      const int64_t aligned_data_size = upper_align(data_size, DIO_ALIGN_SIZE);
      if (OB_UNLIKELY(aligned_data_size > task.info_.get_buf_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected data size", KR(ret), K(task), K(data_size), K(aligned_data_size));
      } else if (OB_FAIL(ext_async_write_to_local(
            buf, aligned_data_size, task.info_.write_handle_))) {
        LOG_WARN("fail to async write to local", KR(ret),
            K(task), KP(buf), K(data_size), K(aligned_data_size));
      }
    }
  }
  return ret;
}

template<typename TaskInfoType>
int ObExternalDataPrefetchPipeline<TaskInfoType>::do_complete_task_(TaskType &task)
{
  // do nothing
  return OB_SUCCESS;
}

/*-----------------------------------------ObSSExternalDataPrefetchPipeline-----------------------------------------*/
template<>
int ObExternalDataPrefetchPipeline<ObSSExternalDataPrefetchTaskInfo>::on_task_failed_(
    TaskType &task)
{
  // when task failed, task may be invalid, remove it from running map whatever
  // so do not check task.is_valid() here
  int ret = OB_SUCCESS;
  ObPCachedExternalFileService *ext_file_service = nullptr;
  if (OB_FAIL(ObStorageIOPipeline<ObSSExternalDataPrefetchTaskInfo>::check_init_and_stop_())) {
  } else if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is NULL", KR(ret), K(task));
    // TODO @fangdan 支持根据macro id删除
  // } else if (OB_FAIL(ext_file_service->path_map_.erase_orphaned_path(
  //     ObPCachedExtMacroKey(task.info_.url_, 0, task.info_.read_offset_)))) {
    // In SS mode, macros are pre-inserted into the path map. If disk write times out and the task fails,
    // the old I/O operations may still be in progress. When the same block appears again for prefetch,
    // it could cause concurrent writes to the same file, leading to data corruption.
    // Therefore, we need to remove the allocated macro from the path map here.
    // During deletion, we are still in the pipeline execution process, so there won't be concurrent writes.
    // Concurrent reads from upper layers at this time are cache misses and won't read from disk.
    // After deletion, when upper layers read this block of external file data, a new macro will be allocated
    // and the prefetch task will write to a new file.
    LOG_WARN("fail to erase macro from path map", KR(ret), K(task));
  } else if (OB_FAIL(ext_file_service->finish_running_prefetch_task(task.info_.hash()))) {
    LOG_WARN("fail to finish running prefetch task", KR(ret), K(task));
  }
  return ret;
}

template class ObExternalDataPrefetchPipeline<ObSSExternalDataPrefetchTaskInfo>;

/*-----------------------------------------ObSNExternalDataPrefetchPipeline-----------------------------------------*/
template<>
int ObExternalDataPrefetchPipeline<ObSNExternalDataPrefetchTaskInfo>::do_complete_task_(
    TaskType &task)
{
  int ret = OB_SUCCESS;
  ObPCachedExternalFileService *ext_file_service = nullptr;
  const ObString content_digest(task.info_.content_digest_.length(), task.info_.content_digest_.ptr());
  ObPCachedExtMacroKey macro_key(
      task.info_.url_, content_digest, task.info_.modify_time_, task.info_.read_offset_);
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is NULL", KR(ret), K(task));
  } else {
    uint32_t data_size = 0;
    if (ObExternalDataPrefetchTaskInfo::PrefetchOperationType::READ_THEN_WRITE ==
        task.info_.operation_type_) {
      data_size = task.info_.read_handle_.get_data_size();
    } else if (ObExternalDataPrefetchTaskInfo::PrefetchOperationType::DIRECT_WRITE ==
        task.info_.operation_type_) {
      data_size = task.info_.get_buf_size();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operation type", KR(ret), K(task));
    }

    if (FAILEDx(ext_file_service->cache_macro(macro_key, task.info_.write_handle_, data_size))) {
      LOG_WARN("fail to cache macro", KR(ret), K(macro_key), K(task), K(data_size));
    }
  }
  return ret;
}

template class ObExternalDataPrefetchPipeline<ObSNExternalDataPrefetchTaskInfo>;

/*-----------------------------------------ObPCachedExtServicePrefetchTimerTask-----------------------------------------*/
template<typename PipelineType>
ObPCachedExtServicePrefetchTimerTask<PipelineType>::ObPCachedExtServicePrefetchTimerTask()
    : is_inited_(false),
      pipeline_()
{
}

template<typename PipelineType>
void ObPCachedExtServicePrefetchTimerTask<PipelineType>::destroy()
{
  is_inited_ = false;
  pipeline_.destroy();
}

template<typename PipelineType>
int ObPCachedExtServicePrefetchTimerTask<PipelineType>::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPCachedExtServicePrefetchTimerTask init twice", KR(ret), KPC(this));
  } else if (OB_FAIL(pipeline_.init(tenant_id))) {
    LOG_WARN("fail to init pipeline", KR(ret), KPC(this));
  } else {
    is_inited_ = true;
  }
  return ret;
}

template<typename PipelineType>
void ObPCachedExtServicePrefetchTimerTask<PipelineType>::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard trace_id_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtServicePrefetchTimerTask not init", KR(ret));
  } else if (OB_FAIL(pipeline_.process())) {
    LOG_WARN("fail to process pipeline", KR(ret), KPC(this));
  }
  LOG_DEBUG("success to process pipeline", KR(ret), KPC(this));
}

template<typename PipelineType>
int ObPCachedExtServicePrefetchTimerTask<PipelineType>::add_task(
    const ObSSExternalDataPrefetchTaskInfo &task_info)
{
  return OB_NOT_SUPPORTED;
}

template<typename PipelineType>
int ObPCachedExtServicePrefetchTimerTask<PipelineType>::add_task(
    const ObSNExternalDataPrefetchTaskInfo &task_info)
{
  return OB_NOT_SUPPORTED;
}

template<typename PipelineType>
void ObPCachedExtServicePrefetchTimerTask<PipelineType>::stop()
{
  pipeline_.stop();
  pipeline_.cancel();
}

template<>
int ObPCachedExtServicePrefetchTimerTask<ObSSExternalDataPrefetchPipeline>::add_task(
    const ObSSExternalDataPrefetchTaskInfo &task_info)
{
  return pipeline_.add_task(task_info);
}
template class ObPCachedExtServicePrefetchTimerTask<ObSSExternalDataPrefetchPipeline>;

template<>
int ObPCachedExtServicePrefetchTimerTask<ObSNExternalDataPrefetchPipeline>::add_task(
    const ObSNExternalDataPrefetchTaskInfo &task_info)
{
  return pipeline_.add_task(task_info);
}
template class ObPCachedExtServicePrefetchTimerTask<ObSNExternalDataPrefetchPipeline>;

/*-----------------------------------------ObPCachedExtServiceExpireTask-----------------------------------------*/
ObPCachedExtServiceExpireTask::ObPCachedExtServiceExpireTask()
{
}

ObPCachedExtServiceExpireTask::~ObPCachedExtServiceExpireTask()
{
}

void ObPCachedExtServiceExpireTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard trace_id_guard;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  ObPCachedExternalFileService *ext_file_service = nullptr;
  int64_t actual_expire_num = 0;
  const int64_t expire_before_time_us = start_time_us - EXPIRATION_DURATION_US;
  if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is NULL", KR(ret));
  } else if (OB_FAIL(ext_file_service->expire_cached_macro(
      expire_before_time_us, actual_expire_num))) {
    LOG_WARN("fail to expire external file cache", KR(ret), K(expire_before_time_us));
  }

  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObPCachedExtServiceExpireTask: finish expire task", KR(ret),
      K(used_time_us), K(start_time_us), K(actual_expire_num));
}

/*-----------------------------------------ObPCachedExtServiceCleanupTask-----------------------------------------*/
ObPCachedExtServiceCleanupTask::ObPCachedExtServiceCleanupTask()
{
}

ObPCachedExtServiceCleanupTask::~ObPCachedExtServiceCleanupTask()
{
}

void ObPCachedExtServiceCleanupTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard trace_id_guard;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  ObPCachedExternalFileService *ext_file_service = nullptr;
  if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is NULL", KR(ret));
  } else if (OB_FAIL(ext_file_service->cleanup_orphaned_path())) {
    LOG_WARN("fail to cleanup orphaned path", KR(ret));
  }

  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObPCachedExtServiceCleanupTask: finish cleanup task", KR(ret),
      K(used_time_us), K(start_time_us));
}

/*-----------------------------------------ObPCachedExtServiceStatTask-----------------------------------------*/
ObPCachedExtServiceStatTask::ObPCachedExtServiceStatTask()
{
}

ObPCachedExtServiceStatTask::~ObPCachedExtServiceStatTask()
{
}

void ObPCachedExtServiceStatTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard trace_id_guard;
  ObPCachedExternalFileService *ext_file_service = nullptr;
  ObStorageCacheStat cache_stat;
  ObStorageCacheHitStat hit_stat;
  if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is NULL", KR(ret));
  } else if (OB_FAIL(ext_file_service->get_cache_stat(cache_stat))) {
    LOG_WARN("fail to get cache stat", KR(ret));
  } else if (OB_FAIL(ext_file_service->get_hit_stat(hit_stat))) {
    LOG_WARN("fail to get cache hit stat", KR(ret));
  }
  LOG_INFO("ObPCachedExtServiceStatTask: finish stat task", KR(ret), K(cache_stat), K(hit_stat));
}

/*-----------------------------------------ObPCachedExtServiceScheduler-----------------------------------------*/
ObPCachedExtServiceScheduler::ObPCachedExtServiceScheduler()
    : is_inited_(false),
      is_stopped_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      tg_id_(-1),
      ss_prefetch_timer_(),
      sn_prefetch_timer_(),
      prefetch_timer_(GCTX.is_shared_storage_mode()
          ? static_cast<ObPCachedExtServicePrefetchTimerBase&>(ss_prefetch_timer_)
          : static_cast<ObPCachedExtServicePrefetchTimerBase&>(sn_prefetch_timer_)),
      cleanup_task_(),
      stat_task_()
{
}

int ObPCachedExtServiceScheduler::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::PCachedExtServiceScheduler, tg_id_))) {
    LOG_WARN("fail to init timer", KR(ret));
  } else if (OB_FAIL(prefetch_timer_.init(tenant_id))) {
    LOG_WARN("fail to init prefetch task", KR(ret));
  } else {
    is_stopped_ = false;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObPCachedExtServiceScheduler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TG_ID == tg_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tg id", KR(ret), K(tg_id_));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer", KR(ret), K(tg_id_));
  } else if (OB_FAIL(schedule_task_(prefetch_timer_, PREFETCH_TASK_SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule refresh policy task", KR(ret), K(tg_id_));
  } else if (OB_FAIL(schedule_task_(cleanup_task_, CLEANUP_TASK_SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule cleanup task", KR(ret), K(tg_id_));
  } else if (OB_FAIL(schedule_task_(stat_task_, STAT_TASK_SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule stat task", KR(ret), K(tg_id_));
  }
  return ret;
}

void ObPCachedExtServiceScheduler::stop()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    prefetch_timer_.stop();
    TG_STOP(tg_id_);
    is_stopped_ = true;
  }
}

void ObPCachedExtServiceScheduler::wait()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
}

void ObPCachedExtServiceScheduler::destroy()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
    is_inited_ = false;
    is_stopped_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    tg_id_ = OB_INVALID_TG_ID;
    prefetch_timer_.destroy();
  }
}

int ObPCachedExtServiceScheduler::schedule_task_(
    ObTimerTask &task, const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtServiceScheduler not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObPCachedExtServiceScheduler is stopped", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("interval_us invalid", KR(ret), KPC(this), K(interval_us));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task, interval_us, true/*repeat*/))) {
    LOG_WARN("fail to schedule refresh tablet svr task",
        KR(ret), KPC(this), K(interval_us), K(task));
  }
  LOG_INFO("finish scheduling timer task", KR(ret), K(task), K(interval_us));
  return ret;
}

} // sql
} // oceanbase