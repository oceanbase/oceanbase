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

#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "observer/ob_server_struct.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/blocksstable/ob_shared_storage_tmp_file.h"
#endif

namespace oceanbase
{
namespace blocksstable
{

/* -------------------------- ObTmpFileWaitTG --------------------------- */

void ObTmpFileWaitTG::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(tmp_file::ObTenantTmpFileManager *)->get_ss_file_manager().exec_wait_task_once())) {
    LOG_WARN("fail to exec wait task once", K(ret));
  }
}

/* -------------------------- ObTmpFileRemoveTG --------------------------- */

void ObTmpFileRemoveTG::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(tmp_file::ObTenantTmpFileManager *)->get_ss_file_manager().exec_remove_task_once())) {
    LOG_WARN("fail to exec remove task once", K(ret));
  }
}

/* -------------------------- ObTmpFileHandle --------------------------- */

ObSSTenantTmpFileManager::ObTmpFileHandle::ObTmpFileHandle(ObITmpFile *tmp_file)
  : ptr_(tmp_file)
{
  if (ptr_ != nullptr) {
    ptr_->inc_ref_cnt();
  }
}

ObSSTenantTmpFileManager::ObTmpFileHandle::ObTmpFileHandle(const ObSSTenantTmpFileManager::ObTmpFileHandle &handle)
  : ptr_(nullptr)
{
  operator=(handle);
}

ObSSTenantTmpFileManager::ObTmpFileHandle::~ObTmpFileHandle()
{
  reset();
}

ObSSTenantTmpFileManager::ObTmpFileHandle &
ObSSTenantTmpFileManager::ObTmpFileHandle::operator=(const ObSSTenantTmpFileManager::ObTmpFileHandle &other)
{
  if (other.get() != ptr_) {
    reset();
    ptr_ = other.get();
    if (ptr_ != nullptr) {
      ptr_->inc_ref_cnt();
    }
  }
  return *this;
}

void ObSSTenantTmpFileManager::ObTmpFileHandle::reset()
{
  int ret = OB_SUCCESS;
  if (ptr_ != nullptr) {
    int64_t new_ref_cnt = -1;
    ptr_->dec_ref_cnt(&new_ref_cnt);
    if (new_ref_cnt == 0) {
      // if temporary file's refcnt decrease to 0, it should be unlink from lru list and released
      if (OB_FAIL(MTL(tmp_file::ObTenantTmpFileManager *)->get_ss_file_manager().remove_from_lru_list(ptr_))) {
        LOG_ERROR("fail to reset temporary file", K(ret), K(ptr_->get_fd()));
      }
      LOG_INFO("shared storage temporary file destroy", K(ret), K(ptr_->get_fd()));
      ptr_->~ObITmpFile();
      MTL(tmp_file::ObTenantTmpFileManager *)->get_ss_file_manager().tmp_file_allocator_.free(ptr_);
    }
    ptr_ = nullptr;
  }
}

void ObSSTenantTmpFileManager::ObTmpFileHandle::set_obj(ObITmpFile *tmp_file)
{
  reset();
  ptr_ = tmp_file;
  ptr_->inc_ref_cnt();
}

int ObSSTenantTmpFileManager::ObTmpFileHandle::assign(const ObSSTenantTmpFileManager::ObTmpFileHandle &tmp_file_handle)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_NOT_NULL(tmp_file_handle.get())) {
    set_obj(tmp_file_handle.get());
  }
  return ret;
}

/* -------------------------- ObTmpFileAsyncWaitTaskQueue --------------------------- */

ObSSTenantTmpFileManager::ObTmpFileAsyncWaitTaskQueue::ObTmpFileAsyncWaitTaskQueue()
    : queue_(), queue_length_(0), pending_free_data_size_(0)
{
}

ObSSTenantTmpFileManager::ObTmpFileAsyncWaitTaskQueue::~ObTmpFileAsyncWaitTaskQueue()
{
}

int ObSSTenantTmpFileManager::ObTmpFileAsyncWaitTaskQueue::push(
    ObTmpFileAsyncFlushWaitTaskHandle *task_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_.push(task_handle))) {
    LOG_WARN("fail to push async wait task", K(ret), K(task_handle));
  } else {
    const int64_t queue_length = ATOMIC_AAF(&queue_length_, 1);
    const int64_t this_task_flush_page_nums = task_handle->wait_task_->flushed_page_nums_;
    const int64_t pending_free_data_size =
        ATOMIC_AAF(&pending_free_data_size_,
                   this_task_flush_page_nums * (8 * 1024 /* page size 8KB */));
    LOG_INFO("wait task enqueue", KPC(task_handle), K(queue_length),
             K(this_task_flush_page_nums), K(pending_free_data_size_));
  }
  return ret;
}

int ObSSTenantTmpFileManager::ObTmpFileAsyncWaitTaskQueue::pop(
    ObTmpFileAsyncFlushWaitTaskHandle *&task_handle)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link *node = nullptr;
  if (OB_FAIL(queue_.pop(node))) {
    LOG_WARN("fail to pop async wait task", K(ret),
             K(ATOMIC_LOAD(&queue_length_)),
             K(ATOMIC_LOAD(&pending_free_data_size_)));
  } else if (FALSE_IT(task_handle = static_cast<ObTmpFileAsyncFlushWaitTaskHandle *>(node))) {
  } else {
    const int64_t queue_length = ATOMIC_SAF(&queue_length_, 1);
    const int64_t this_task_flush_page_nums = task_handle->wait_task_->flushed_page_nums_;
    const int64_t pending_free_data_size =
        ATOMIC_SAF(&pending_free_data_size_,
                   this_task_flush_page_nums * (8 * 1024 /* page size 8KB */));
    LOG_INFO("wait task dequeue", KPC(task_handle), K(queue_length),
             K(this_task_flush_page_nums), K(pending_free_data_size));
  }
  return ret;
}

/* -------------------------- ObTmpFileAsyncRemoveTask --------------------------- */

ObSSTenantTmpFileManager::ObTmpFileAsyncRemoveTask::ObTmpFileAsyncRemoveTask(
    const MacroBlockId &tmp_file_id, const int64_t length)
    : tmp_file_id_(tmp_file_id), length_(length)
{
}

int ObSSTenantTmpFileManager::ObTmpFileAsyncRemoveTask::exec_remove() const
{
  int ret = OB_SUCCESS;

  ObTimeGuard time_guard("ss_tmp_file_remove", 4 * 1000 * 1000 /* 4s */);

#ifdef OB_BUILD_SHARED_STORAGE
  if (length_ == INT64_MAX) {
    // Reboot GC, we don't know temporary file length.
    if (OB_FAIL(MTL(ObTenantFileManager *)->delete_tmp_file(tmp_file_id_))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to delete previous temporary file before this reboot", K(ret), K(tmp_file_id_));
      }
    }
  } else {
    // Normal delete, we know temporary file length.
    if (OB_FAIL(MTL(ObTenantFileManager *)->delete_tmp_file(tmp_file_id_, length_))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to delete temporary file", K(ret), K(tmp_file_id_), K(length_));
      }
    }
  }
#endif

  time_guard.click("remove_ss_tmp_file");

  return ret;
}

/* -------------------------- ObTmpFileAsyncRemoveTaskQueue --------------------------- */

ObSSTenantTmpFileManager::ObTmpFileAsyncRemoveTaskQueue::
    ObTmpFileAsyncRemoveTaskQueue()
    : queue_(), queue_length_(0)
{
}

ObSSTenantTmpFileManager::ObTmpFileAsyncRemoveTaskQueue::~ObTmpFileAsyncRemoveTaskQueue()
{
}

int ObSSTenantTmpFileManager::ObTmpFileAsyncRemoveTaskQueue::push(ObTmpFileAsyncRemoveTask * remove_task)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(queue_.push(remove_task))) {
    LOG_WARN("fail to push remove task", K(ret));
  } else {
    const int64_t queue_length = ATOMIC_AAF(&queue_length_, 1);
    LOG_INFO("remove task enqueue", KPC(remove_task), K(queue_length));
  }

  return ret;
}

int ObSSTenantTmpFileManager::ObTmpFileAsyncRemoveTaskQueue::pop(ObTmpFileAsyncRemoveTask *& remove_task)
{
  int ret = OB_SUCCESS;

  ObSpLinkQueue::Link *node = nullptr;
  remove_task = nullptr;
  if (OB_FAIL(queue_.pop(node))) {
    LOG_WARN("fail to pop remove task", K(ret));
  } else if (FALSE_IT(remove_task = static_cast<ObTmpFileAsyncRemoveTask *>(node))) {
  } else {
    const int64_t queue_length = ATOMIC_SAF(&queue_length_, 1);
    LOG_INFO("remove task dequeue", KPC(remove_task), K(queue_length));
  }

  return ret;
}

/* -------------------------- ObTmpFileDiskUsageCalculator --------------------------- */

bool ObSSTenantTmpFileManager::ObTmpFileDiskUsageCalculator::operator()(
    const ObTmpFileKey &key, ObSSTenantTmpFileManager::ObTmpFileHandle &tmp_file_handle)
{
  UNUSED(key);
  int64_t tmp_file_flushed_size = 0;
  tmp_file_handle.get()->get_file_disk_usage_size(tmp_file_flushed_size);
  disk_data_size_ += tmp_file_flushed_size;
  occupied_disk_size_ += common::upper_align(
      tmp_file_flushed_size, ObSSTmpWriteBufferPool::PAGE_SIZE);
  return true;  // never ends prematurely.
}

/* -------------------------- ObSSTenantTmpFileManager --------------------------- */

ObSSTenantTmpFileManager::ObSSTenantTmpFileManager()
  : files_(),
    tmp_file_allocator_(),
    callback_allocator_(),
    wait_task_allocator_(),
    remove_task_allocator_(),
    lru_list_array_(),
    lru_lock_(common::ObLatchIds::TMP_FILE_MGR_LOCK),
    wbp_(),
    wait_task_queue_(),
    remove_task_queue_(),
    aflush_tg_id_(OB_INVALID_INDEX),
    aremove_tg_id_(OB_INVALID_INDEX),
    wait_task_(),
    remove_task_(),
    first_tmp_file_id_(INVALID_TMP_FILE_FD),
    is_stopped_(true),
    is_inited_(false)
{
}

ObSSTenantTmpFileManager::~ObSSTenantTmpFileManager()
{
  destroy();
}

int ObSSTenantTmpFileManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTenantTmpFileManager init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(files_.init("TmpFileMap", MTL_ID()))) {
    LOG_WARN("fail to init temporary files map", K(ret));
  } else if (OB_FAIL(tmp_file_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                              OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                              ObMemAttr(MTL_ID(), "TmpFileMgrTFA", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init temporary file allocator", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(callback_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                              OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                              ObMemAttr(MTL_ID(), "TmpFileMgrCA", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init callback allocator", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(wait_task_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                               OB_MALLOC_NORMAL_BLOCK_SIZE,
                                               ObMemAttr(MTL_ID(), "TmpFileMgrWTA", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init wait task allocator", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(remove_task_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                 OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                 ObMemAttr(MTL_ID(), "TmpFileMgrRTA", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init remove task allocator", K(ret), K(MTL_ID()));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTmpFileAFlush, aflush_tg_id_))) {
    LOG_WARN("fail to create async wait thread", K(ret));
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTmpFileARemove, aremove_tg_id_))) {
    LOG_WARN("fail to create async remove thread", K(ret));
#endif
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(wbp_.init())) {
    LOG_WARN("fail to init ObSSTmpWriteBufferPool", K(ret));
  // TODO: sn and ss both init kvcache, after sn rebuild finished, merge these two.
  } else if (OB_SUCC(ret) || OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTenantTmpFileManager::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to start temporary file manager, already started", K(ret), K(is_stopped_));
  // TODO: ss and sn should merge, and remove this ifdef and GCTX check.
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(TG_START(aflush_tg_id_))) {
      LOG_WARN("fail to start async wait thread", K(ret), K(aflush_tg_id_));
    } else if (OB_FAIL(TG_SCHEDULE(aflush_tg_id_, wait_task_, 10 * 1000 /* 10 ms */, true /* repeat */))) {
      LOG_WARN("fail to schedule async wait thread", KR(ret), K(aflush_tg_id_));
    } else if (OB_FAIL(TG_START(aremove_tg_id_))) {
      LOG_WARN("fail to start async remove thread", K(ret), K(aremove_tg_id_));
    } else if (OB_FAIL(TG_SCHEDULE(aremove_tg_id_, remove_task_, 5 * 1000 * 1000 /* 5 s */, true /* repeat */))) {
      LOG_WARN("fail to schedule async remove thread", KR(ret), K(aremove_tg_id_));
    } else if (OB_FAIL(get_first_tmp_file_id())) {
      LOG_WARN("fail to exec reboot gc once", K(ret));
    }
#endif
  }
  if (OB_SUCC(ret)) {
    is_stopped_ = false;
  }
  return ret;
}

int ObSSTenantTmpFileManager::wait()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX != aflush_tg_id_) {
    TG_WAIT(aflush_tg_id_);
  }
  if (OB_INVALID_INDEX != aremove_tg_id_) {
    TG_WAIT(aremove_tg_id_);
  }
  // Make sure all wait task finish.
  while (OB_SUCC(ret) && wait_task_queue_.get_queue_length() != 0) {
    if (OB_FAIL(exec_wait_task_once())) {
      LOG_WARN("fail to exec wait task once", K(ret));
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  // Make sure all remove task finish.
  while (OB_SUCC(ret) && remove_task_queue_.get_queue_length() != 0) {
    if (OB_FAIL(exec_remove_task_once())) {
      LOG_WARN("fail to exec remove task once", K(ret));
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_UNLIKELY(wait_task_queue_.get_queue_length() != 0 || remove_task_queue_.get_queue_length() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected task queue length during tmp file manager mtl wait",
              K(ret), K(wait_task_queue_.get_queue_length()),
              K(remove_task_queue_.get_queue_length()));
  }
  return ret;
}

int ObSSTenantTmpFileManager::stop()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX != aflush_tg_id_) {
    TG_STOP(aflush_tg_id_);
  }
  if (OB_INVALID_INDEX != aremove_tg_id_) {
    TG_STOP(aremove_tg_id_);
  }
  is_stopped_ = true;
  return ret;
}

void ObSSTenantTmpFileManager::destroy()
{
  int ret = OB_SUCCESS;
  // Make sure all wait task finish.
  if (OB_UNLIKELY(wait_task_queue_.get_queue_length() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected wait task queue length during tmp file manager destroy",
        K(ret), K(wait_task_queue_.get_queue_length()));
    ret = OB_SUCCESS;
  }
  // Clear file map and destroy wbp.
  files_.destroy();
  wbp_.destroy();
  // Make sure all remove task finish.
  if (OB_UNLIKELY(remove_task_queue_.get_queue_length() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected remove task queue length during tmp file manager destroy",
        K(ret), K(remove_task_queue_.get_queue_length()));
    ret = OB_SUCCESS;
  }
  // Destroy async_wait and async_remove thread.
  if (OB_INVALID_INDEX != aflush_tg_id_) {
    TG_DESTROY(aflush_tg_id_);
    aflush_tg_id_ = OB_INVALID_INDEX;
  }
  if (OB_INVALID_INDEX != aremove_tg_id_) {
    TG_DESTROY(aremove_tg_id_);
    aremove_tg_id_ = OB_INVALID_INDEX;
  }
  // Reset all allocators.
  tmp_file_allocator_.reset();
  callback_allocator_.reset();
  wait_task_allocator_.reset();
  remove_task_allocator_.reset();

  is_inited_ = false;

  FLOG_INFO(
      "destroy tmp file manager", KP(this), K(wait_task_queue_.is_empty()),
      K(wait_task_queue_.get_queue_length()), K(remove_task_queue_.is_empty()),
      K(remove_task_queue_.get_queue_length()));
}

int ObSSTenantTmpFileManager::alloc_dir(int64_t &dir)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to alloc dir", K(ret));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc dir, not running", K(ret), K(is_stopped_));
  } else {
    if (GCTX.is_shared_storage_mode()) { // Shared Storage
      dir = SHARE_STORAGE_DIR_ID;
    } else { // Shared Nothing
      // TODO: finish this
    }
  }
  return ret;
}

int ObSSTenantTmpFileManager::open(int64_t &fd, const int64_t &dir)
{
  int ret = OB_SUCCESS;
  fd = INVALID_TMP_FILE_FD;

  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to open, not running", K(ret), K(is_stopped_));
  } else if (OB_FAIL(set_tmp_file(fd, dir))) {
    LOG_WARN("fail to set tmp file", K(ret), K(fd), K(dir));
  } else {
    LOG_INFO("open a temporary file", K(fd), K(dir));
  }

  return ret;
}

int ObSSTenantTmpFileManager::remove(const int64_t fd)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (OB_FAIL(erase_tmp_file(fd)) && ret != OB_ENTRY_NOT_EXIST) {
    LOG_WARN("fail to remove temporary file", K(ret), K(fd));
  }

  return ret;
}

int ObSSTenantTmpFileManager::remove_tenant_file(const uint64_t tenant_id)
{
  int ret = OB_ERR_UNEXPECTED;;
  LOG_WARN("un-implement, remove_tenant_file", K(ret), K(tenant_id));
  return ret;
}

int ObSSTenantTmpFileManager::erase_tmp_file(const int64_t fd)
{
  int ret = OB_SUCCESS;
  // hold a file_handle to guarantee ref_cnt of file must not be 0 in the function of map;
  // otherwise, dead lock might happened when remove file thread
  // and flush thread work at the same time (caused by lru_lock_ and the bucket lock of map )
  ObSSTenantTmpFileManager::ObTmpFileHandle file_handle;
  if (OB_FAIL(files_.erase(ObTmpFileKey(fd), file_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("erase already non-exist temporary file", K(fd));
    } else {
      LOG_WARN("fail to erase tmp file", KR(ret), K(fd));
    }
  } else {
    file_handle.reset();
    LOG_INFO("erase temporary file", K(fd));
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSSTenantTmpFileManager::set_shared_storage_tmp_file(
    int64_t &fd, const int64_t &dir /* no use in shared storage mode */)
{
  int ret = OB_SUCCESS;

  void *buf = nullptr;
  ObSSTenantTmpFileManager::ObTmpFileHandle handle;
  ObSharedStorageTmpFile *tmp_file = nullptr;
  MacroBlockId tmp_file_id;
  tmp_file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tmp_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  ObStorageObjectOpt opt;
  opt.set_ss_tmp_file_object_opt();
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, tmp_file_id))) {
    LOG_WARN("fail to allocate temporary file id from object manager", K(ret), K(opt));
  } else if (OB_ISNULL(buf = tmp_file_allocator_.alloc(sizeof(ObSharedStorageTmpFile),
                                                       lib::ObMemAttr(MTL_ID(), "SSTmpFile")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for share storage temporary file",
             K(ret), K(sizeof(ObSharedStorageTmpFile)));
  } else if (FALSE_IT(tmp_file = new (buf) ObSharedStorageTmpFile())) {
  } else if (FALSE_IT(tmp_file->set_id(tmp_file_id))) {
  } else if (FALSE_IT(handle.set_obj(tmp_file))) {
  } else if (OB_FAIL(files_.insert_or_update(ObTmpFileKey(tmp_file_id.second_id()), handle))) {
    LOG_WARN("fail to set refactored to temporary file map", K(ret),
             K(tmp_file_id.second_id()), KP(tmp_file));
  } else {
    fd = tmp_file_id.second_id();
    LOG_INFO("succeed to set a share storage temporary file",
             K(tmp_file_id.second_id()), KP(tmp_file), K(files_.count()), K(common::lbt()));
  }

  return ret;
}
#endif

int ObSSTenantTmpFileManager::set_shared_nothing_tmp_file(int64_t &fd, const int64_t &dir)
{
  int ret = OB_SUCCESS;
  ret = OB_ERR_UNEXPECTED;
  LOG_WARN("set shared nothing temporary file is un-implement", K(ret), K(fd), K(dir));
  return ret;
}

int ObSSTenantTmpFileManager::set_tmp_file(int64_t &fd, const int64_t &dir)
{
  int ret = OB_SUCCESS;

  // Init temporary file and set into temporary file manager, return file descriptor and dir id through argument.
  if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
    // Shared Storage
    if (OB_FAIL(set_shared_storage_tmp_file(fd, dir))) {
      LOG_WARN("fail to set shared storage temporary file", K(ret), K(fd), K(dir));
    }
#endif
  } else {
    // Shared Nothing
    if (OB_FAIL(set_shared_nothing_tmp_file(fd, dir))) {
      LOG_WARN("fail to set shared nothing temporary file", K(ret), K(fd), K(dir));
    }
  }

  return ret;
}

int ObSSTenantTmpFileManager::update_meta_data(ObTmpFileAsyncFlushWaitTask & wait_task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t fd = wait_task.fd_;
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  if (OB_FAIL(get_tmp_file(fd, tmp_file_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tmp file handle", K(ret), K(fd));
    }
  } else {
    if (OB_FAIL(tmp_file_handle.get()->update_meta_data(wait_task))) {
      LOG_ERROR("fail to update meta data", K(ret), K(fd), KP(&wait_task));
    }
    // Regardless of whether `update_meta_data` succeeds or fails, this
    // temporary file should be put back into the LRU list to ensure it can be
    // flushed to disk later.
    if (OB_TMP_FAIL(flushing_put_into_lru_list(tmp_file_handle.get()))) {
      LOG_ERROR("fail to put temporary file into lru list", K(tmp_ret), K(tmp_file_handle.get()->get_fd()));
    }
  }
  return ret;
}

int ObSSTenantTmpFileManager::get_tmp_file(const int64_t fd, ObSSTenantTmpFileManager::ObTmpFileHandle &handle)
{
  int ret = OB_SUCCESS;

  // Get temporary file and increase refcnt, return through handle.
  if (OB_FAIL(files_.get(ObTmpFileKey(fd), handle)) && ret != OB_ENTRY_NOT_EXIST) {
    LOG_WARN("fail to get tmp file", K(ret), K(fd));
  } else if (ret == OB_ENTRY_NOT_EXIST) {
    LOG_WARN("fail to get tmp file, not exist", K(ret), K(fd));
  } else if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tmp file pointer", K(ret), K(fd), KP(handle.get()));
  }

  return ret;
}

int ObSSTenantTmpFileManager::get_tmp_file_size(const int64_t fd, int64_t &size)
{
  int ret = OB_SUCCESS;
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (OB_FAIL(get_tmp_file(fd, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", K(ret), K(fd));
  } else {
    tmp_file_handle.get()->get_file_size(size);
  }
  return ret;
}

int ObSSTenantTmpFileManager::get_tmp_file_disk_usage(int64_t & disk_data_size, int64_t & occupied_disk_size)
{
  int ret = OB_SUCCESS;
  ObTmpFileDiskUsageCalculator op;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (OB_FAIL(files_.for_each(op))) {
    LOG_WARN("fail to for each in tmp file map", K(ret));
  } else {
    disk_data_size = op.get_disk_data_size();
    occupied_disk_size = op.get_occupied_disk_size();
  }
  return ret;
}

int ObSSTenantTmpFileManager::wait_task_enqueue(ObTmpFileAsyncFlushWaitTask *task)
{
  int ret = OB_SUCCESS;

  char * buff = nullptr;
  ObTmpFileAsyncFlushWaitTaskHandle * task_handle = nullptr;

  if (OB_ISNULL(buff = static_cast<char *>(wait_task_allocator_.alloc(
                    sizeof(ObTmpFileAsyncFlushWaitTaskHandle))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc task handle", K(ret));
  } else if (FALSE_IT(task_handle = new (buff) ObTmpFileAsyncFlushWaitTaskHandle(task))) {
  } else if (OB_FAIL(wait_task_queue_.push(task_handle))) {
    LOG_WARN("fail to push wait task", K(ret), K(task));
  }

  if (OB_FAIL(ret) && task_handle != nullptr) {
    wait_task_allocator_.free(task_handle);
  }

  return ret;
}

int ObSSTenantTmpFileManager::exec_wait_task_once()
{
  int ret = OB_SUCCESS;

  int64_t i = 0;
  const int64_t queue_size = wait_task_queue_.get_queue_length();
  ObTmpFileAsyncFlushWaitTaskHandle * task_handle = nullptr;
  // Perform the wait task once for all tasks in the queue.
  for (; OB_SUCC(ret) && i < queue_size && !wait_task_queue_.is_empty(); ++i) {
    task_handle = nullptr;
    if (OB_FAIL(wait_task_queue_.pop(task_handle))) {
      LOG_WARN("fail to pop wait task queue", K(ret), K(i), K(queue_size));
    } else if (OB_FAIL(task_handle->get()->exec_wait())) {
      LOG_ERROR("fail to wait task finish", K(ret), KPC(task_handle->get()));
      // Swallow error code to continue other wait tasks.
      ret = OB_SUCCESS;
    }
    if (task_handle != nullptr) {
      // Regardless of whether the IO wait is successful, resources are
      // released. For failed IO, it is treated as if nothing happened.
      LOG_INFO("async wait task succeed exec_wait", K(task_handle->get()->fd_),
                K(queue_size), KPC(task_handle->get()));
      task_handle->~ObTmpFileAsyncFlushWaitTaskHandle();
      wait_task_allocator_.free(task_handle);
    }
  }

  return ret;
}

int ObSSTenantTmpFileManager::remove_task_enqueue(const MacroBlockId &tmp_file_id, const int64_t length)
{
  int ret = OB_SUCCESS;

  char * buf = nullptr;
  ObTmpFileAsyncRemoveTask * remove_task = nullptr;

  if (OB_ISNULL(buf = static_cast<char *>(remove_task_allocator_.alloc(sizeof(ObTmpFileAsyncRemoveTask))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate ObTmpFileAsyncRemoveTask", K(ret));
  } else if (FALSE_IT(remove_task = new (buf) ObTmpFileAsyncRemoveTask(tmp_file_id, length))) {
  } else if (OB_FAIL(remove_task_queue_.push(remove_task))) {
    LOG_WARN("fail to push async remove task", K(ret), K(tmp_file_id), K(length));
  }

  if (OB_FAIL(ret) && remove_task != nullptr) {
    remove_task_allocator_.free(remove_task);
  }

  return ret;
}

int ObSSTenantTmpFileManager::exec_reboot_gc_once()
{
  int ret = OB_SUCCESS;

#ifdef OB_BUILD_SHARED_STORAGE
  ObArray<MacroBlockId> list_tmp_file_res;
  if (OB_FAIL(MTL(ObTenantFileManager *)->list_tmp_file(list_tmp_file_res))) {
    LOG_WARN("fail to list tmp file", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < list_tmp_file_res.count(); ++i) {
      const MacroBlockId &curr_tmp_file_id = list_tmp_file_res.at(i);
      if (curr_tmp_file_id.second_id() < first_tmp_file_id_) {
        if (OB_FAIL(remove_task_enqueue(curr_tmp_file_id, INT64_MAX))) {
          LOG_ERROR("fail to remove task enqueue in reboot gc, irrecoverable error",
              K(ret), K(first_tmp_file_id_), K(curr_tmp_file_id));
          ret = OB_SUCCESS;
        } else {
          LOG_INFO("previous temporary file enqueue", K(curr_tmp_file_id));
        }
      } else if (curr_tmp_file_id.second_id() == first_tmp_file_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected tmp file id", K(ret), K(first_tmp_file_id_), K(curr_tmp_file_id));
      } else {
        // new temporary file after this reboot, do nothing.
      }
    }
  }
  FLOG_INFO("finish reboot gc", K(ret), K(first_tmp_file_id_));
#endif

  return ret;
}

int ObSSTenantTmpFileManager::exec_remove_task_once()
{
  int ret = OB_SUCCESS;

  // Execute reboot gc.
  if (INVALID_TMP_FILE_FD != first_tmp_file_id_) {
    if (OB_FAIL(exec_reboot_gc_once())) {
      LOG_WARN("fail to exec reboot gc once", K(ret), K(first_tmp_file_id_));
    } else {
      LOG_INFO("succeed to exec reboot gc", K(first_tmp_file_id_));
      first_tmp_file_id_ = INVALID_TMP_FILE_FD;
    }
  }

  // Continue async remove.
  ret = OB_SUCCESS;
  int64_t i = 0;
  const int64_t queue_size = remove_task_queue_.get_queue_length();
  ObTmpFileAsyncRemoveTask * remove_task = nullptr;
  // Perform the remove task once for all tasks in the queue.
  for (; OB_SUCC(ret) && i < queue_size && !remove_task_queue_.is_empty(); ++i) {
    remove_task = nullptr;
    if (OB_FAIL(remove_task_queue_.pop(remove_task))) {
      LOG_WARN("fail to top remove task queue", K(ret), K(i), K(queue_size));
    } else if (OB_FAIL(remove_task->exec_remove())) {
      LOG_WARN("fail to exec remove task", K(ret), KPC(remove_task));
      // Swallow error code to continue other remove tasks.
      ret = OB_SUCCESS;
      // Push back failed remove task to queue again, expect next batch succeed.
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(remove_task_queue_.push(remove_task))) {
        LOG_WARN("fail to push failed remove task, remove task lease", K(tmp_ret), KPC(remove_task));
      }
    } else {
      LOG_INFO("async remove task succeed exec_remove", K(i), K(queue_size), KPC(remove_task), K(first_tmp_file_id_));
      remove_task->~ObTmpFileAsyncRemoveTask();
      remove_task_allocator_.free(remove_task);
    }
  }

  return ret;
}

int ObSSTenantTmpFileManager::aio_read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;

  // Remove temporary file from lru list before read. And add to lru list after read.
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->aio_read(io_info, handle)) && ret != OB_ITER_END) {
    LOG_WARN("fail to aio read", K(ret), K(io_info));
  }

  return ret;
}

int ObSSTenantTmpFileManager::aio_pread(const tmp_file::ObTmpFileIOInfo &io_info,
                                      const int64_t offset,
                                      ObSSTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;

  // Remove temporary file from lru list before read. And add to lru list after read.
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->aio_pread(io_info, offset, handle)) && ret != OB_ITER_END) {
    LOG_WARN("fail to aio read", K(ret), K(io_info));
  }

  return ret;
}

int ObSSTenantTmpFileManager::read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;

  // Remove temporary file from lru list before read. And add to lru list after read.
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->read(io_info, handle)) && ret != OB_ITER_END) {
    LOG_WARN("fail to aio read", K(ret), K(io_info));
  }

  return ret;
}

int ObSSTenantTmpFileManager::pread(const tmp_file::ObTmpFileIOInfo &io_info, const int64_t offset, ObSSTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;

  // Remove temporary file from lru list before read. And add to lru list after read.
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio read, invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->pread(io_info, offset, handle)) && ret != OB_ITER_END) {
    LOG_WARN("fail to aio read", K(ret), K(io_info));
  }

  return ret;
}

int ObSSTenantTmpFileManager::aio_write(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;

  // Remove temporary file from lru list before write. And add to lru list after write.
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->aio_write(io_info, handle))) {
    LOG_WARN("fail to aio write", K(ret), K(io_info));
  }

  return ret;
}

int ObSSTenantTmpFileManager::write(const tmp_file::ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;

  // Remove temporary file from lru list before read. And add to lru list after read.
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to write, invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->write(io_info))) {
    LOG_WARN("fail to write", K(ret), K(io_info));
  }

  return ret;
}

int ObSSTenantTmpFileManager::seek(const int64_t fd, const int64_t offset, const int whence)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("un-implement, seek", K(ret), K(fd));
  return ret;
}

int ObSSTenantTmpFileManager::truncate(const int64_t fd, const int64_t offset)
{
  int ret = OB_SUCCESS;
  /* In scenarios with shared storage, there's no need to rigorously consider
   * the issue of temporary file space usage, so `truncate` can be implemented
   * as a no-op. */
  return ret;
}

int ObSSTenantTmpFileManager::sync(const int64_t fd, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;

  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", K(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to remove, not running", K(ret), K(is_stopped_));
  } else if (OB_FAIL(get_tmp_file(fd, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", K(ret), K(fd));
  } else if (OB_FAIL(tmp_file_handle.get()->sync(timeout_ms))) {
    LOG_WARN("fail to sync temporary file", K(ret), K(fd), K(timeout_ms));
  }

  return ret;
}

void ObSSTenantTmpFileManager::print_lru_list_info(ObIArray<int64_t> &array)
{
  SpinRLockGuard guard(lru_lock_);
  ObSSTenantTmpFileManager::TmpFileLRUList *list = nullptr;
  int32_t LOTS = LRUListIndex::DATA_PAGE_COUNT_LOTS;
  int32_t LESS = LRUListIndex::DATA_PAGE_COUNT_LESS;
  ObArray<int64_t> lots_array;
  ObArray<int64_t> less_array;

  int64_t lots_size = 0;
  list = &lru_list_array_[LOTS];
  for (ObITmpFile *p = list->get_last(); p != list->get_header(); p = p->get_prev()) {
    lots_size++;
    lots_array.push_back(p->get_fd());
  }

  int64_t less_size = 0;
  list = &lru_list_array_[LESS];
  for (ObITmpFile *p = list->get_last(); p != list->get_header(); p = p->get_prev()) {
    less_size++;
    less_array.push_back(p->get_fd());
  }

  array.at(LRUListIndex::DATA_PAGE_COUNT_LOTS) = lots_size;
  array.at(LRUListIndex::DATA_PAGE_COUNT_LESS) = less_size;

  LOG_INFO("print lru list info", K(lots_size), K(less_size), K(lots_array), K(less_array));
}

int ObSSTenantTmpFileManager::remove_from_lru_list(ObITmpFile *file)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(lru_lock_);

  if (OB_UNLIKELY(OB_ISNULL(file))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null temporary file", K(ret), KP(file));
  } else {
    file->unlink();
    LOG_INFO("remove from lru list while writing or destroying", K(ret), K(file->get_fd()));
  }
  return ret;
}

int ObSSTenantTmpFileManager::put_into_lru_list(ObITmpFile *file)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(lru_lock_);

  if (OB_UNLIKELY(OB_ISNULL(file))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null temporary file", K(ret), KP(file));
  } else if (file->is_flushing()) {
    // do nothing
    FLOG_INFO("no need to put into lru list while writing, flushing now", K(ret), K(file->get_fd()));
  } else if (OB_FAIL(inner_put_into_lru_list(file))) {
    LOG_ERROR("fail to put into lru list", K(ret), K(file->get_fd()), K(file->is_flushing()));
  }
  return ret;
}

int ObSSTenantTmpFileManager::get_first_tmp_file_id()
{
  int ret = OB_SUCCESS;

#ifdef OB_BUILD_SHARED_STORAGE
  MacroBlockId max_tmp_file_id;
  max_tmp_file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  max_tmp_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  ObStorageObjectOpt opt;
  opt.set_ss_tmp_file_object_opt();
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, max_tmp_file_id))) {
    LOG_WARN("fail to get current max tmp file id", K(ret), K(opt));
  } else {
    first_tmp_file_id_ = max_tmp_file_id.second_id();
  }
#endif

  return ret;
}

int ObSSTenantTmpFileManager::flushing_remove_from_lru_list(ObITmpFile *file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(file))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null temporary file", K(ret), KP(file));
  } else if (OB_UNLIKELY(file->is_flushing())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected flushing status, should be false", K(ret),
              K(file->get_fd()), K(file->is_flushing()));
  } else if (OB_FAIL(file->set_is_flushing())) {
    LOG_ERROR("fail to set flushing tag", K(ret), K(file->get_fd()));
  } else {
    file->unlink();
    FLOG_INFO("remove from lru list while flushing", K(ret), K(file->get_fd()));
  }
  return ret;
}

int ObSSTenantTmpFileManager::flushing_put_into_lru_list(ObITmpFile *file)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(lru_lock_);

  if (OB_UNLIKELY(OB_ISNULL(file))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null temporary file", K(ret), KP(file));
  } else if (OB_UNLIKELY(!file->is_flushing())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected flushing status, should be true", K(ret),
              K(file->get_fd()), K(file->is_flushing()));
  } else if (OB_FAIL(file->set_not_flushing())) {
    LOG_ERROR("fail to set not flushing tag", K(ret), K(file->get_fd()));
  } else if (OB_FAIL(inner_put_into_lru_list(file))) {
    LOG_ERROR("fail to put into lru list", K(ret), K(file->get_fd()), K(file->is_flushing()));
  }
  return ret;
}

int ObSSTenantTmpFileManager::inner_put_into_lru_list(ObITmpFile *file)
{
  /* This function must be used under the lru list lock protection. */
  int ret = OB_SUCCESS;
  int list_index = -1;
  int64_t page_nums = -1;
  // Based on the metadata info of this temporary file, determine which list enters.
  if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
    page_nums =
        (static_cast<ObSharedStorageTmpFile *>(file))
            ->get_data_page_nums(false /* including incomplete pages */);
    if (page_nums > 0 && page_nums < ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS) {
      list_index = LRUListIndex::DATA_PAGE_COUNT_LESS;
    } else if (page_nums < UINT32_MAX &&
               page_nums >= ObSSTmpWriteBufferPool::BLOCK_PAGE_NUMS) {
      list_index = LRUListIndex::DATA_PAGE_COUNT_LOTS;
    }
#endif
  } else {
    // TODO: finish this
  }
  file->unlink();
  if (list_index >= 0 && list_index < LRUListIndex::LRU_LIST_INDEX_MAX) {
    // Put this temporary file into its lru list.
    if (false == lru_list_array_[list_index].add_first(file)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to put into lru list", K(ret), K(file->get_fd()),
                K(page_nums), K(list_index));
    }
  }
  FLOG_INFO("put into lru list", K(ret), K(file->get_fd()),
            K(file->is_flushing()), K(page_nums), K(list_index));
  return ret;
}

int ObSSTenantTmpFileManager::wash(const int64_t expect_wash_size, ObSSTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;

  if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
    // Shared Storage
    if (OB_FAIL(shared_storage_wash(expect_wash_size, io_handle))) {
      LOG_WARN("fail to wash in shared storage mode", K(ret), K(expect_wash_size), K(io_handle));
    }
#endif
  } else {
    // Shared Nothing
    if (OB_FAIL(shared_nothing_wash(expect_wash_size, io_handle))) {
      LOG_WARN("fail to wash in shared nothing mode", K(ret), K(expect_wash_size));
    }
  }

  return ret;
}

int64_t ObSSTenantTmpFileManager::get_pending_free_data_size() const
{
  return wait_task_queue_.get_pending_free_data_size();
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSSTenantTmpFileManager::shared_storage_wash(const int64_t expect_wash_size, ObSSTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  int begin_lru_list_index = LRUListIndex::DATA_PAGE_COUNT_LOTS;
  int end_lru_list_index = LRUListIndex::DATA_PAGE_COUNT_LESS;
  int64_t wash_size = 0;
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  ObSSTenantTmpFileManager::TmpFileLRUList *list = nullptr;

  // Fetch LRU lock and traverse LRU list, find temporary file to execute flush operation.
  SpinWLockGuard guard(lru_lock_);
  for (int i = begin_lru_list_index;
       OB_SUCC(ret) && i <= end_lru_list_index && wash_size < expect_wash_size;
       ++i) {
    bool flush_as_block = false;
    int64_t free_size = 0;
    if (i == LRUListIndex::DATA_PAGE_COUNT_LOTS) {
      flush_as_block = true;
    }
    list = &(lru_list_array_[i]);
    while (OB_SUCC(ret) &&
           wash_size < expect_wash_size &&
           list->get_last() != list->get_header()) {
      if (OB_FAIL(get_tmp_file(list->get_last()->get_fd(), tmp_file_handle)) &&
          ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("fail to get temporary file in wash", K(ret), K(list->get_last()->get_fd()));
      } else if (ret == OB_HASH_NOT_EXIST) {
        // This temporary file may be in the process of removing, but it is still waiting for
        // the LRU lock which held here to remove itself from the list.
        list->remove_last();
        ret = OB_SUCCESS;
      } else if (OB_ISNULL(tmp_file_handle.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get temporary file", K(ret), KP(tmp_file_handle.get()));
      } else if (OB_FAIL(flushing_remove_from_lru_list(tmp_file_handle.get()))) {
        LOG_WARN("fail to remove from lru list without lock when flushing",
                 K(ret), K(tmp_file_handle.get()->get_fd()));
      } else {
        // Release the LRU lock during the process of flushing temporary files to disk.
        lru_lock_.unlock();
        ObSharedStorageTmpFile * ss_tmp_file = static_cast<ObSharedStorageTmpFile *>(tmp_file_handle.get());
        if (OB_FAIL(ss_tmp_file->flush(flush_as_block, free_size, io_handle))) {
          LOG_ERROR("fail to flush temporary file", K(ret), K(tmp_file_handle.get()->get_fd()));
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(flushing_put_into_lru_list(tmp_file_handle.get()))) {
            LOG_ERROR("fail to put back to lru list", K(tmp_ret), K(ret), K(tmp_file_handle.get()->get_fd()));
          }
        }
        wash_size += free_size;
        FLOG_INFO("wash shared storage temporary file", K(ret),
                  KPC(tmp_file_handle.get()), K(flush_as_block), K(free_size));
        tmp_file_handle.reset();
        lru_lock_.wrlock();
      }
    }
  }
  LOG_INFO("ObSSTenantTmpFileManager wash, shared storage mode", K(ret), K(expect_wash_size), K(wash_size));

  return ret;
}
#endif

int ObSSTenantTmpFileManager::shared_nothing_wash(const int64_t expect_wash_size, ObSSTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  // TODO: finish this
  ret = OB_ERR_UNEXPECTED;
  LOG_WARN("fail to shared nothing wash, un-implement", K(ret), K(expect_wash_size), K(io_handle));
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase