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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_I_TMP_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_I_TMP_H_

#include "lib/list/ob_dlist.h"
#include "lib/container/ob_se_array.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_tmp_file_cache.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"

namespace oceanbase
{
namespace blocksstable
{

class ObTmpFileAsyncFlushWaitTaskHandle;

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSTmpFileFlushContext;
#endif

struct ObTmpFileAsyncFlushWaitTask
{
public:
  friend class ObTmpFileAsyncFlushWaitTaskHandle;

public:
  ObTmpFileAsyncFlushWaitTask();
  ~ObTmpFileAsyncFlushWaitTask();
  int init(const int64_t fd, const int64_t length, const uint32_t begin_page_id, const ObMemAttr attr);
#ifdef OB_BUILD_SHARED_STORAGE
  int push_back_io_task(const ObSSTmpFileFlushContext &ctx);
#endif
  int exec_wait();
  OB_INLINE int64_t get_current_begin_page_id() const { return current_begin_page_id_; }
  OB_INLINE int64_t get_flushed_page_nums() const { return flushed_page_nums_; }
  OB_INLINE int64_t get_update_meta_data_page_nums() const { return succeed_wait_page_nums_; }
  int cond_broadcast();
  int cond_wait();
  TO_STRING_KV(K_(fd), K_(current_length), K_(current_begin_page_id),
               K_(flushed_offset), K_(io_tasks), K_(flushed_page_nums),
               K_(succeed_wait_page_nums), K_(ref_cnt), K_(wait_has_finished),
               K_(is_inited));

private:
  int release_io_tasks();
  OB_INLINE void inc_ref_cnt()
  {
    ATOMIC_INC(&ref_cnt_);
  }
  OB_INLINE void dec_ref_cnt(int32_t *new_ref_cnt = nullptr)
  {
    int32_t ref_cnt = -1;
    ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
    if (OB_NOT_NULL(new_ref_cnt)) {
      *new_ref_cnt = ref_cnt;
    }
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileAsyncFlushWaitTask);

public:
  int64_t fd_;
  int64_t current_length_;
  uint32_t current_begin_page_id_;
  int64_t flushed_offset_;
  ObArray<std::pair<ObStorageObjectHandle *, char *>> io_tasks_;
  int64_t flushed_page_nums_;
  int64_t succeed_wait_page_nums_;

private:
  ObThreadCond cond_;
  int32_t ref_cnt_;
  bool wait_has_finished_;
  bool is_inited_;
};

struct ObTmpFileAsyncFlushWaitTaskHandle : public common::ObDLink
{
public:
  ObTmpFileAsyncFlushWaitTaskHandle();
  ObTmpFileAsyncFlushWaitTaskHandle(ObTmpFileAsyncFlushWaitTask *wait_task);
  ~ObTmpFileAsyncFlushWaitTaskHandle();
  void set_wait_task(ObTmpFileAsyncFlushWaitTask *wait_task);
  void reset();
  OB_INLINE ObTmpFileAsyncFlushWaitTask * get() { return wait_task_; }
  int wait();
  TO_STRING_KV(KP_(wait_task));

public:
  ObTmpFileAsyncFlushWaitTask *wait_task_;
};

class ObSSTmpFileIOHandle final
{
public:
  struct ObIOReadHandle final
  {
    ObIOReadHandle();
    ObIOReadHandle(const ObStorageObjectHandle &handle, char *buf,
                   const int64_t offset, const int64_t size);
    ~ObIOReadHandle();
    ObIOReadHandle(const ObIOReadHandle &other);
    ObIOReadHandle &operator=(const ObIOReadHandle &other);
    TO_STRING_KV(K_(handle), K_(offset), K_(size), KP_(buf));
    ObStorageObjectHandle handle_;
    char *buf_;
    int64_t offset_;
    int64_t size_;
  };

  struct ObPageCacheHandle final
  {
    ObPageCacheHandle();
    ObPageCacheHandle(const ObTmpPageValueHandle &page_handle, char *buf, const int64_t offset,
        const int64_t size);
    ~ObPageCacheHandle();
    ObPageCacheHandle(const ObPageCacheHandle &other);
    ObPageCacheHandle &operator=(const ObPageCacheHandle &other);
    TO_STRING_KV(K_(page_handle), K_(offset), K_(size), KP_(buf));
    ObTmpPageValueHandle page_handle_;
    char *buf_;
    int64_t offset_;
    int64_t size_;
  };

  struct ObBlockCacheHandle final
  {
    ObBlockCacheHandle();
    ObBlockCacheHandle(const ObTmpBlockValueHandle &block_handle, char *buf, const int64_t offset,
        const int64_t size);
    ~ObBlockCacheHandle();
    ObBlockCacheHandle(const ObBlockCacheHandle &other);
    ObBlockCacheHandle &operator=(const ObBlockCacheHandle &other);
    TO_STRING_KV(K_(block_handle), K_(offset), K_(size), KP_(buf));
    ObTmpBlockValueHandle block_handle_;
    char *buf_;       // memcpy target address
    int64_t offset_;  // offset in this page
    int64_t size_;    // read size in this page
  };

  ObSSTmpFileIOHandle();
  ~ObSSTmpFileIOHandle();
  OB_INLINE char *get_buffer() { return buf_; }
  OB_INLINE int64_t get_data_size() const { return size_; }
  OB_INLINE bool is_disable_page_cache() const { return disable_page_cache_; }
  OB_INLINE int64_t get_expect_read_size() const { return expect_read_size_; }
  OB_INLINE int64_t get_expect_write_size() const { return expect_write_size_; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE ObTmpFileAsyncFlushWaitTaskHandle &get_async_flush_wait_handle()
  {
    return wait_task_handle_;
  }
  int prepare_read(
      const int64_t read_size,
      const int64_t read_offset,
      const common::ObIOFlag io_flag,
      char *read_buf,
      const int64_t fd,
      const int64_t dir_id,
      const uint64_t tenant_id,
      const bool disable_page_cache);
  int prepare_write(
      char *write_buf,
      const int64_t write_size,
      const common::ObIOFlag io_flag,
      const int64_t fd,
      const int64_t dir_id,
      const uint64_t tenant_id);
  OB_INLINE void add_data_size(const int64_t size) { size_ += size; }
  OB_INLINE void sub_data_size(const int64_t size) { size_ -= size; }
  OB_INLINE void set_update_offset_in_file() { update_offset_in_file_ = true; }
  OB_INLINE bool need_update_offset_in_file() { return update_offset_in_file_; }
  // get read offset in file
  OB_INLINE int64_t get_last_read_offset() const { return last_read_offset_; }
  OB_INLINE void add_last_read_offset(const int64_t size) { last_read_offset_ += size; }
  OB_INLINE void set_last_read_offset(const int64_t last_read_offset)
  {
    last_read_offset_ = last_read_offset;
  }
  OB_INLINE common::ObIOFlag get_io_desc() const { return io_flag_; }
  OB_INLINE bool is_read() const { return is_read_; }
  int wait();
  void reset();
  bool is_valid() const;
  common::ObIArray<ObSSTmpFileIOHandle::ObIOReadHandle> &get_io_handles()
  {
    return io_handles_;
  }
  common::ObIArray<ObSSTmpFileIOHandle::ObPageCacheHandle> &get_page_cache_handles()
  {
    return page_cache_handles_;
  }
  common::ObIArray<ObSSTmpFileIOHandle::ObBlockCacheHandle> &get_block_cache_handles()
  {
    return block_cache_handles_;
  }
  int record_block_id(const int64_t block_it);

  int64_t get_last_extent_id() const;
  void set_last_extent_id(const int64_t last_extent_id);

  TO_STRING_KV(KP_(buf), K_(size), K_(is_read), K_(has_wait), K_(expect_read_size),
      K_(last_read_offset), K_(io_flag), K_(update_offset_in_file), K_(wait_task_handle));

private:
  int wait_write_finish(const int64_t timeout_ms);
  int wait_read_finish(const int64_t timeout_ms);
  int do_read_wait(const int64_t timeout_ms);

#ifdef OB_BUILD_SHARED_STORAGE
  int shared_storage_wait_read_finish(const int64_t timeout_ms);
  int do_shared_storage_read_wait();
  int do_shared_storage_write_wait();
#endif

private:
  common::ObSEArray<ObSSTmpFileIOHandle::ObIOReadHandle, 1> io_handles_;
  common::ObSEArray<ObSSTmpFileIOHandle::ObPageCacheHandle, 1> page_cache_handles_;
  common::ObSEArray<ObSSTmpFileIOHandle::ObBlockCacheHandle, 1> block_cache_handles_;
  common::hash::ObHashSet<int64_t> write_block_ids_;
  int64_t fd_;
  int64_t dir_id_;
  uint64_t tenant_id_;
  char *buf_;
  int64_t size_;  // has read or to write size
  bool is_read_;
  bool has_wait_;
  bool is_finished_;
  bool disable_page_cache_;
  int ret_code_;
  int64_t expect_read_size_;
  int64_t expect_write_size_;
  int64_t last_read_offset_; // file offset
  common::ObIOFlag io_flag_;
  bool update_offset_in_file_;
  int64_t last_fd_;
  int64_t last_extent_id_;
  ObTmpFileAsyncFlushWaitTaskHandle wait_task_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTmpFileIOHandle);
};

class ObITmpFileManager
{
public:
  ObITmpFileManager() {}
  virtual ~ObITmpFileManager() {}
  virtual int init() = 0;
  virtual int start() = 0;
  virtual int wait() = 0;
  virtual int stop() = 0;
  virtual void destroy() = 0;
  virtual int alloc_dir(int64_t &dir) = 0;
  virtual int open(int64_t &fd, const int64_t &dir) = 0;
  virtual int aio_read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) = 0;
  virtual int aio_pread(const tmp_file::ObTmpFileIOInfo &io_info, const int64_t offset, ObSSTmpFileIOHandle &handle) = 0;
  virtual int read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) = 0;
  virtual int pread(const tmp_file::ObTmpFileIOInfo &io_info, const int64_t offset, ObSSTmpFileIOHandle &handle) = 0;
  virtual int aio_write(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) = 0;
  virtual int write(const tmp_file::ObTmpFileIOInfo &io_info) = 0;
  virtual int seek(const int64_t fd, const int64_t offset, const int whence) = 0;
  virtual int truncate(const int64_t fd, const int64_t offset) = 0;
  virtual int sync(const int64_t fd, const int64_t timeout_ms) = 0;
  virtual int remove(const int64_t fd) = 0;
  virtual int remove_tenant_file(const uint64_t tenant_id) = 0;
  virtual int get_tmp_file_size(const int64_t fd, int64_t &file_size) = 0;
};

class ObITmpFile : public ObDLinkBase<ObITmpFile>
{
public:
  ObITmpFile() : ref_cnt_(0), is_flushing_(false) {}
  virtual ~ObITmpFile() {};
  virtual int aio_read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) = 0;
  virtual int aio_pread(const tmp_file::ObTmpFileIOInfo &io_info, const int64_t offset, ObSSTmpFileIOHandle &handle) = 0;
  virtual int read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) = 0;
  virtual int pread(const tmp_file::ObTmpFileIOInfo &io_info, const int64_t offset, ObSSTmpFileIOHandle &handle) = 0;
  virtual int aio_write(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &handle) = 0;
  virtual int write(const tmp_file::ObTmpFileIOInfo &io_info) = 0;
  virtual int sync(const int64_t timeout_ms) = 0;

public:
  virtual int continue_read(const tmp_file::ObTmpFileIOInfo &io_info, ObSSTmpFileIOHandle &io_handle) = 0;
  virtual int64_t get_fd() const = 0;
  virtual void get_file_size(int64_t &file_size) = 0;
  virtual void get_file_disk_usage_size(int64_t &disk_usage_size) = 0;
  virtual int update_meta_data(ObTmpFileAsyncFlushWaitTask & wait_task) = 0;

public:
  int inc_ref_cnt();
  int dec_ref_cnt(int64_t *new_ref_cnt = nullptr);
  bool is_flushing();
  int set_is_flushing();
  int set_not_flushing();

private:
  int64_t ref_cnt_;
  bool is_flushing_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_I_TMP_H_