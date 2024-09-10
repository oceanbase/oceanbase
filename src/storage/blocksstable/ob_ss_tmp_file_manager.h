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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SS_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SS_TMP_FILE_MANAGER_H_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "storage/blocksstable/ob_i_tmp_file.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "storage/blocksstable/ob_tmp_file_cache.h"
#include "storage/blocksstable/ob_ss_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"

namespace oceanbase
{
namespace blocksstable
{

// Async wait thread task
class ObTmpFileWaitTG : public common::ObTimerTask
{
public:
  ObTmpFileWaitTG() {}
  virtual ~ObTmpFileWaitTG() {}
  virtual void runTimerTask() override;
};

// Async remove thread task
class ObTmpFileRemoveTG : public common::ObTimerTask
{
public:
  ObTmpFileRemoveTG() {}
  virtual ~ObTmpFileRemoveTG() {}
  virtual void runTimerTask() override;
};

class ObSSTenantTmpFileManager final : public ObITmpFileManager
{
public:
  static const int64_t SHARE_STORAGE_DIR_ID = 1;
  static const int64_t INVALID_TMP_FILE_FD = -1;

public:
  typedef common::ObDList<ObITmpFile> TmpFileLRUList;
  enum LRUListIndex
  {
    DATA_PAGE_COUNT_LOTS = 0,
    DATA_PAGE_COUNT_LESS = 1,
    LRU_LIST_INDEX_MAX
  };

  struct ObTmpFileKey final
  {
    explicit ObTmpFileKey(const int64_t fd) : fd_(fd) {}
    OB_INLINE int hash(uint64_t &hash_val) const
    {
      hash_val = murmurhash(&fd_, sizeof(uint64_t), 0);
      return OB_SUCCESS;
    }
    OB_INLINE bool operator==(const ObTmpFileKey &other) const { return fd_ == other.fd_; }
    int64_t fd_;
  };

  class ObTmpFileHandle final
  {
  public:
    ObTmpFileHandle() : ptr_(nullptr) {}
    ObTmpFileHandle(ObITmpFile *tmp_file);
    ObTmpFileHandle(const ObSSTenantTmpFileManager::ObTmpFileHandle &handle);
    ~ObTmpFileHandle();
    ObTmpFileHandle & operator=(const ObSSTenantTmpFileManager::ObTmpFileHandle &other);
    OB_INLINE ObITmpFile * get() const {return ptr_; }
    void reset();
    void set_obj(ObITmpFile *tmp_file);
    int assign(const ObSSTenantTmpFileManager::ObTmpFileHandle &tmp_file_handle);

  private:
    ObITmpFile *ptr_;
  };

  class ObTmpFileAsyncWaitTaskQueue
  {
  public:
    ObTmpFileAsyncWaitTaskQueue();
    ~ObTmpFileAsyncWaitTaskQueue();
    int push(ObTmpFileAsyncFlushWaitTaskHandle *task_handle);
    int pop(ObTmpFileAsyncFlushWaitTaskHandle *&task_handle);
    OB_INLINE bool is_empty() const { return queue_.is_empty(); }
    OB_INLINE int64_t get_queue_length() const { return ATOMIC_LOAD(&queue_length_); }
    OB_INLINE int64_t get_pending_free_data_size() const { return ATOMIC_LOAD(&pending_free_data_size_); }

  private:
    DISALLOW_COPY_AND_ASSIGN(ObTmpFileAsyncWaitTaskQueue);

  private:
    common::ObSpLinkQueue queue_;
    int64_t queue_length_;
    int64_t pending_free_data_size_;
  };

  class ObTmpFileAsyncRemoveTask : public common::ObDLink
  {
  public:
    explicit ObTmpFileAsyncRemoveTask(const MacroBlockId &tmp_file_id, const int64_t length);
    ~ObTmpFileAsyncRemoveTask() = default;
    int exec_remove() const;
    TO_STRING_KV(K_(tmp_file_id), K_(length));

  private:
    const MacroBlockId tmp_file_id_;
    const int64_t length_;
  };

  class ObTmpFileAsyncRemoveTaskQueue
  {
  public:
    ObTmpFileAsyncRemoveTaskQueue();
    ~ObTmpFileAsyncRemoveTaskQueue();
    int push(ObTmpFileAsyncRemoveTask * remove_task);
    int pop(ObTmpFileAsyncRemoveTask *& remove_task);
    OB_INLINE bool is_empty() const { return queue_.is_empty(); }
    OB_INLINE int64_t get_queue_length() const { return ATOMIC_LOAD(&queue_length_); }

  private:
    DISALLOW_COPY_AND_ASSIGN(ObTmpFileAsyncRemoveTaskQueue);

  private:
    common::ObSpLinkQueue queue_;
    int64_t queue_length_;
  };

private:
  class ObTmpFileDiskUsageCalculator final
  {
  public:
    ObTmpFileDiskUsageCalculator() : disk_data_size_(0), occupied_disk_size_(0) {}
    bool operator()(const ObTmpFileKey &key,
                    ObSSTenantTmpFileManager::ObTmpFileHandle &tmp_file_handle);
    OB_INLINE int64_t get_disk_data_size() const { return disk_data_size_; }
    OB_INLINE int64_t get_occupied_disk_size() const { return occupied_disk_size_; }

  private:
    int64_t disk_data_size_;
    int64_t occupied_disk_size_;
  };

public:
  ObSSTenantTmpFileManager();
  ~ObSSTenantTmpFileManager() override;
  static int mtl_init(ObSSTenantTmpFileManager *&manager);
  int init() override;
  int start() override;
  int wait() override;
  int stop() override;
  void destroy() override;
  int alloc_dir(int64_t &dir) override;
  int open(int64_t &fd, const int64_t &dir) override;
  int aio_read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) override;
  int aio_pread(const tmp_file::ObTmpFileIOInfo &io_info, const int64_t offset, ObSSTmpFileIOHandle &handle) override;
  int read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) override;
  int pread(const tmp_file::ObTmpFileIOInfo &io_info, const int64_t offset, ObSSTmpFileIOHandle &handle) override;
  // NOTE:
  //   only support append write.
  int aio_write(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) override;
  // NOTE:
  //   only support append write.
  int write(const tmp_file::ObTmpFileIOInfo &io_info) override;
  int seek(const int64_t fd, const int64_t offset, const int whence) override;
  // the data before the offset is released
  int truncate(const int64_t fd, const int64_t offset) override;
  int sync(const int64_t fd, const int64_t timeout_ms) override;
  int remove(const int64_t fd) override;
  int remove_tenant_file(const uint64_t tenant_id) override;
  int get_tmp_file_size(const int64_t fd, int64_t &size) override;
  int get_tmp_file_disk_usage(int64_t & disk_data_size, int64_t & occupied_disk_size);

public:
  int get_tmp_file(const int64_t fd, ObSSTenantTmpFileManager::ObTmpFileHandle &handle);
  int wait_task_enqueue(ObTmpFileAsyncFlushWaitTask *task);
  int exec_wait_task_once();
  int remove_task_enqueue(const MacroBlockId &tmp_file_id, const int64_t length);
  int exec_remove_task_once();
  int update_meta_data(ObTmpFileAsyncFlushWaitTask & wait_task);
  int wash(const int64_t expect_wash_size, ObSSTmpFileIOHandle &io_handle);
  int64_t get_pending_free_data_size() const;
  OB_INLINE ObIAllocator * get_callback_allocator() { return &callback_allocator_; }
  OB_INLINE ObIAllocator * get_wait_task_allocator() { return &wait_task_allocator_; }
  OB_INLINE ObSSTmpWriteBufferPool * get_write_buffer_pool() { return &wbp_; }
  int remove_from_lru_list(ObITmpFile *file);
  int put_into_lru_list(ObITmpFile *file);

private:
  OB_INLINE bool is_running() const { return !is_stopped_; }
  int get_first_tmp_file_id();
  int exec_reboot_gc_once();
  int flushing_remove_from_lru_list(ObITmpFile *file);
  int flushing_put_into_lru_list(ObITmpFile *file);
  int shared_nothing_wash(const int64_t expect_wash_size, ObSSTmpFileIOHandle &io_handle);
  int set_tmp_file(int64_t &fd, const int64_t &dir);
  int set_shared_nothing_tmp_file(int64_t &fd, const int64_t &dir);
#ifdef OB_BUILD_SHARED_STORAGE
  int shared_storage_wash(const int64_t expect_wash_size, ObSSTmpFileIOHandle &io_handle);
  int set_shared_storage_tmp_file(int64_t &fd, const int64_t &dir);
#endif
  int erase_tmp_file(const int64_t fd);
  int inner_put_into_lru_list(ObITmpFile *file);
  void print_lru_list_info(ObIArray<int64_t> &array);

private:
  const static int8_t LRU_LIST_NUM = 3;

private:
  common::ObLinearHashMap<ObTmpFileKey, ObSSTenantTmpFileManager::ObTmpFileHandle> files_;
  common::ObFIFOAllocator tmp_file_allocator_;
  common::ObFIFOAllocator callback_allocator_;
  common::ObFIFOAllocator wait_task_allocator_;
  common::ObFIFOAllocator remove_task_allocator_;
  TmpFileLRUList lru_list_array_[LRUListIndex::LRU_LIST_INDEX_MAX];
  common::SpinRWLock lru_lock_;
  ObSSTmpWriteBufferPool wbp_;
  ObTmpFileAsyncWaitTaskQueue wait_task_queue_;
  ObTmpFileAsyncRemoveTaskQueue remove_task_queue_;
  int aflush_tg_id_;
  int aremove_tg_id_;
  ObTmpFileWaitTG wait_task_;
  ObTmpFileRemoveTG remove_task_;
  int64_t first_tmp_file_id_;
  bool is_stopped_;
  bool is_inited_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SS_TMP_FILE_MANAGER_H_