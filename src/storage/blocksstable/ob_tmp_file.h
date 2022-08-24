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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_H_

#include "ob_store_file.h"
#include "storage/ob_resource_map.h"
#include "lib/container/ob_se_array.h"
#include "ob_tmp_file_store.h"
#include "storage/ob_tenant_file_struct.h"

namespace oceanbase {
namespace blocksstable {

class ObTmpFile;
class ObTmpFileExtent;

struct ObTmpFileIOInfo {
public:
  ObTmpFileIOInfo();
  virtual ~ObTmpFileIOInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(fd), K_(dir_id), K_(size), K_(tenant_id), KP_(buf), K_(io_desc));
  int64_t fd_;
  int64_t dir_id_;
  int64_t size_;
  uint64_t tenant_id_;
  char* buf_;
  common::ObIODesc io_desc_;
};

class ObTmpFileIOHandle {
public:
  struct ObIOReadHandle {
    ObIOReadHandle();
    ObIOReadHandle(const ObMacroBlockHandle& macro_handle, char* buf, const int64_t offset, const int64_t size);
    virtual ~ObIOReadHandle();
    ObIOReadHandle(const ObIOReadHandle& other);
    ObIOReadHandle& operator=(const ObIOReadHandle& other);
    TO_STRING_KV(K_(macro_handle), K_(offset), K_(size), KP_(buf));
    ObMacroBlockHandle macro_handle_;
    char* buf_;
    int64_t offset_;
    int64_t size_;
  };

  struct ObPageCacheHandle {
    ObPageCacheHandle();
    ObPageCacheHandle(const ObTmpPageValueHandle& page_handle, char* buf, const int64_t offset, const int64_t size);
    virtual ~ObPageCacheHandle();
    ObPageCacheHandle(const ObPageCacheHandle& other);
    ObPageCacheHandle& operator=(const ObPageCacheHandle& other);
    TO_STRING_KV(K_(page_handle), K_(offset), K_(size), KP_(buf));
    ObTmpPageValueHandle page_handle_;
    char* buf_;
    int64_t offset_;
    int64_t size_;
  };

  struct ObBlockCacheHandle {
    ObBlockCacheHandle();
    ObBlockCacheHandle(const ObTmpBlockValueHandle& block_handle, char* buf, const int64_t offset, const int64_t size);
    virtual ~ObBlockCacheHandle();
    ObBlockCacheHandle(const ObBlockCacheHandle& other);
    ObBlockCacheHandle& operator=(const ObBlockCacheHandle& other);
    TO_STRING_KV(K_(block_handle), K_(offset), K_(size), KP_(buf));
    ObTmpBlockValueHandle block_handle_;
    char* buf_;
    int64_t offset_;
    int64_t size_;
  };

  ObTmpFileIOHandle();
  virtual ~ObTmpFileIOHandle();
  OB_INLINE char* get_buffer()
  {
    return buf_;
  }
  OB_INLINE int64_t get_data_size()
  {
    return size_;
  }
  int prepare_read(const int64_t read_size, const int64_t read_offset, const common::ObIODesc &io_flag, char *read_buf,
      ObTmpFile *file);
  int prepare_write(char *write_buf, const int64_t write_size, ObTmpFile *file);
  OB_INLINE void add_data_size(const int64_t size)
  {
    size_ += size;
  }
  OB_INLINE void sub_data_size(const int64_t size)
  {
    size_ -= size;
  }
  OB_INLINE void set_update_offset_in_file()
  {
    update_offset_in_file_ = true;
  }
  OB_INLINE void set_last_read_offset(const int64_t last_read_offset)
  {
    last_read_offset_ = last_read_offset;
  }
  int wait(const int64_t timeout_ms);
  void reset();
  bool is_valid();
  common::ObIArray<ObTmpFileIOHandle::ObIOReadHandle>& get_io_handles()
  {
    return io_handles_;
  }
  common::ObIArray<ObTmpFileIOHandle::ObPageCacheHandle>& get_page_cache_handles()
  {
    return page_cache_handles_;
  }
  common::ObIArray<ObTmpFileIOHandle::ObBlockCacheHandle>& get_block_cache_handles()
  {
    return block_cache_handles_;
  }
  OB_INLINE int64_t get_last_read_offset() const
  {
    return last_read_offset_;
  }

  TO_STRING_KV(KP_(buf), K_(size), K_(is_read), K_(has_wait), K_(expect_read_size), K_(last_read_offset), K_(io_flag),
      K_(update_offset_in_file));

private:
  int do_wait(const int64_t timeout_ms);

private:
  ObTmpFile* tmp_file_;
  common::ObSEArray<ObTmpFileIOHandle::ObIOReadHandle, 1> io_handles_;
  common::ObSEArray<ObTmpFileIOHandle::ObPageCacheHandle, 1> page_cache_handles_;
  common::ObSEArray<ObTmpFileIOHandle::ObBlockCacheHandle, 1> block_cache_handles_;
  char* buf_;
  int64_t size_;  // has read or to write size.
  bool is_read_;
  bool has_wait_;
  int64_t expect_read_size_;
  int64_t last_read_offset_;  // only for more than 8MB read.
  common::ObIODesc io_flag_;
  bool update_offset_in_file_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileIOHandle);
};

class ObTmpFileExtent {
public:
  explicit ObTmpFileExtent(ObTmpFile* file);
  virtual ~ObTmpFileExtent();
  virtual int read(
      const ObTmpFileIOInfo& io_info, const int64_t offset, const int64_t size, char* buf, ObTmpFileIOHandle& handle);
  virtual int write(const ObTmpFileIOInfo& io_info, int64_t& size, char*& buf);
  void reset();
  OB_INLINE bool is_closed() const
  {
    return is_closed_;
  }
  bool is_valid();
  bool close(bool force = false);
  bool close(int32_t& free_page_start_id, int32_t& free_page_nums, bool force = false);
  void unclose(const int32_t page_nums = -1);
  bool is_alloced() const
  {
    return is_alloced_;
  }
  OB_INLINE void set_global_offset(const int64_t g_offset_start, const int64_t g_offset_end);
  OB_INLINE void get_global_offset(int64_t& g_offset_start, int64_t& g_offset_end) const;
  OB_INLINE int64_t get_global_end() const
  {
    return g_offset_end_;
  }
  OB_INLINE int64_t get_global_start() const
  {
    return g_offset_start_;
  }
  OB_INLINE void alloced()
  {
    is_alloced_ = true;
  }
  OB_INLINE void set_start_page_id(const int32_t start_page_id)
  {
    start_page_id_ = start_page_id;
  }
  OB_INLINE int32_t get_start_page_id() const
  {
    return start_page_id_;
  }
  OB_INLINE void set_page_nums(const int32_t page_nums)
  {
    page_nums_ = page_nums;
  }
  OB_INLINE int32_t get_page_nums() const
  {
    return page_nums_;
  }
  OB_INLINE void set_block_id(const int64_t block_id)
  {
    block_id_ = block_id;
  }
  OB_INLINE int64_t get_block_id() const
  {
    return block_id_;
  }
  OB_INLINE void set_offset(const int64_t offset)
  {
    offset_ = offset;
  }
  OB_INLINE int32_t get_offset() const
  {
    return offset_;
  }
  OB_INLINE ObTmpFile& get_owner()
  {
    return *owner_;
  }
  TO_STRING_KV(K_(is_alloced), K_(fd), K_(g_offset_start), K_(g_offset_end), KP_(owner), K_(start_page_id),
      K_(page_nums), K_(block_id), K_(offset), K_(is_closed));

private:
  bool is_alloced_;
  int64_t fd_;
  int64_t g_offset_start_;
  int64_t g_offset_end_;
  ObTmpFile* owner_;
  int32_t start_page_id_;
  int32_t page_nums_;
  int64_t block_id_;
  int32_t offset_;
  common::SpinRWLock lock_;
  bool is_closed_;  // only if close, this extent cannot be used.
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileExtent);
};

class ObTmpFileMeta {
public:
  explicit ObTmpFileMeta() : fd_(-1), dir_id_(-1), allocator_(NULL), extents_()
  {}
  virtual ~ObTmpFileMeta();
  int clear();
  int init(const int64_t fd, const int64_t dir_id, common::ObIAllocator* allocator);
  ObTmpFileExtent* get_last_extent();
  common::ObIArray<ObTmpFileExtent*>& get_extents()
  {
    return extents_;
  }
  int push_back_extent(ObTmpFileExtent* extent)
  {
    return extents_.push_back(extent);
  }
  int pop_back_extent(ObTmpFileExtent*& extent)
  {
    return extents_.pop_back(extent);
  }
  void pop_back_extent()
  {
    extents_.pop_back();
  }
  int deep_copy(const ObTmpFileMeta& other);
  OB_INLINE int64_t get_fd() const
  {
    return fd_;
  }
  OB_INLINE int64_t get_dir_id() const
  {
    return dir_id_;
  }
  TO_STRING_KV(K_(fd), K_(dir_id), K_(extents));

private:
  int64_t fd_;
  int64_t dir_id_;
  common::ObIAllocator* allocator_;
  ExtentArray extents_;  // b-tree is better
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileMeta);
};

class ObTmpFile {
public:
  enum FileWhence {
    SET_SEEK = 0,
    CUR_SEEK,
  };
  ObTmpFile();
  virtual ~ObTmpFile();
  int init(const int64_t fd, const int64_t dir_id, common::ObIAllocator& allocator);
  int aio_read(const ObTmpFileIOInfo& io_info, ObTmpFileIOHandle& handle);
  int aio_pread(const ObTmpFileIOInfo& io_info, const int64_t offset, ObTmpFileIOHandle& handle);
  int read(const ObTmpFileIOInfo& io_info, const int64_t timeout_ms, ObTmpFileIOHandle& handle);
  int pread(const ObTmpFileIOInfo& io_info, const int64_t offset, const int64_t timeout_ms, ObTmpFileIOHandle& handle);
  int aio_write(const ObTmpFileIOInfo& io_info, ObTmpFileIOHandle& handle);
  int write(const ObTmpFileIOInfo& io_info, const int64_t timeout_ms);
  int seek(const int64_t offset, const int whence);
  int clear();
  int64_t get_dir_id() const;
  uint64_t get_tenant_id() const;
  int64_t get_fd() const;
  int sync(const int64_t timeout_ms);
  int deep_copy(char* buf, const int64_t buf_len, ObTmpFile*& value) const;
  inline int64_t get_deep_copy_size() const;
  void get_file_size(int64_t &file_size);
  // only for ObTmpFileIOHandle, once more than READ_SIZE_PER_BATCH read.
  int once_aio_read_batch(
      const ObTmpFileIOInfo &io_info, const bool need_update_offset, int64_t &offset, ObTmpFileIOHandle &handle);

  TO_STRING_KV(K_(file_meta), K_(is_big), K_(tenant_id), K_(is_inited));

private:
  int write_file_extent(const ObTmpFileIOInfo &io_info, ObTmpFileExtent *file_extent, int64_t &size, char *&buf);
  int aio_read_without_lock(const ObTmpFileIOInfo &io_info, int64_t &offset, ObTmpFileIOHandle &handle);
  int once_aio_read_batch_without_lock(const ObTmpFileIOInfo &io_info, int64_t &offset, ObTmpFileIOHandle &handle);
  int64_t small_file_prealloc_size();
  int64_t big_file_prealloc_size();
  int64_t find_first_extent(const int64_t offset);

private:
  // NOTE:
  // 1.The pre-allocated macro should satisfy the following inequality:
  //      SMALL_FILE_MAX_THRESHOLD < BIG_FILE_PREALLOC_EXTENT_SIZE < block size
  static const int64_t SMALL_FILE_MAX_THRESHOLD = 4;
  static const int64_t BIG_FILE_PREALLOC_EXTENT_SIZE = 8;
  static const int64_t READ_SIZE_PER_BATCH = 8 * 1024 * 1024;  // 8MB

  ObTmpFileMeta file_meta_;
  bool is_big_;
  uint64_t tenant_id_;
  int64_t offset_;  // read offset
  common::ObIAllocator *allocator_;
  int64_t last_extent_id_;
  int64_t last_extent_min_offset_;
  int64_t last_extent_max_offset_;
  common::SpinRWLock lock_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObTmpFile);
};

class ObTmpFileHandle : public storage::ObResourceHandle<ObTmpFile> {
public:
  ObTmpFileHandle();
  virtual ~ObTmpFileHandle();
  virtual void reset() override;

private:
  friend class ObTmpFileManager;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileHandle);
};

class ObTmpFileManager {
public:
  static ObTmpFileManager& get_instance();
  int init();
  int start();
  int alloc_dir(int64_t& dir);
  int open(int64_t& fd, int64_t& dir);
  // NOTE:
  //   default order read, if want to read random, should be seek first.
  int aio_read(const ObTmpFileIOInfo& io_info, ObTmpFileIOHandle& handle);
  int aio_pread(const ObTmpFileIOInfo& io_info, const int64_t offset, ObTmpFileIOHandle& handle);
  // NOTE:
  //   default order read, if want to read random, should be seek first.
  int read(const ObTmpFileIOInfo& io_info, const int64_t timeout_ms, ObTmpFileIOHandle& handle);
  int pread(const ObTmpFileIOInfo& io_info, const int64_t offset, const int64_t timeout_ms, ObTmpFileIOHandle& handle);
  // NOTE:
  //   only support order write.
  int aio_write(const ObTmpFileIOInfo& io_info, ObTmpFileIOHandle& handle);
  // NOTE:
  //   only support order write.
  int write(const ObTmpFileIOInfo& io_info, const int64_t timeout_ms);
  // only for read:
  // 1. whence == SET_SEEK, inner offset = offset;
  // 2. whence == CUR_SEEK, inner offset -= offset;
  int seek(const int64_t fd, const int64_t offset, const int whence);
  // NOTE:
  //   remove file and all of block in this file, after not used file, should be called in case
  //   of block leak.
  int remove(const int64_t fd);
  int remove_tenant_file(const uint64_t tenant_id);

  int get_all_tenant_id(common::ObIArray<uint64_t> &tenant_ids);

  int sync(const int64_t fd, const int64_t timeout_ms);

  void destroy();
  int dec_handle_ref(ObTmpFileHandle& handle);
  OB_INLINE blocksstable::ObStorageFile* get_storage_file()
  {
    return storage_file_.file_;
  }

private:
  class RmTenantTmpFileOp {
  public:
    RmTenantTmpFileOp(const uint64_t tenant_id, common::ObIArray<int64_t>* fd_list)
        : tenant_id_(tenant_id), fd_list_(fd_list)
    {}
    ~RmTenantTmpFileOp() = default;
    int operator()(common::hash::HashMapPair<int64_t, ObTmpFile *> &entry)
    {
      int ret = OB_SUCCESS;
      ObTmpFile* tmp_file = entry.second;
      if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_) || OB_ISNULL(fd_list_) || OB_ISNULL(tmp_file)) {
        ret = common::OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid argument", K(ret));
      } else if (tmp_file->get_tenant_id() == tenant_id_) {
        if (OB_FAIL(fd_list_->push_back(tmp_file->get_fd()))) {
          STORAGE_LOG(WARN, "fd_list_ push back failed", K(ret));
        }
      }
      return ret;
    }

  private:
    const uint64_t tenant_id_;
    common::ObIArray<int64_t>* fd_list_;
  };

private:
  ObTmpFileManager();
  virtual ~ObTmpFileManager();
  int get_next_dir(int64_t& next_dir);
  int get_next_fd(int64_t& next_fd);
  void next_value(int64_t& current_val, int64_t& next_val);

private:
  static const int64_t DEFAULT_BUCKET_NUM = 10243L;
  static const int64_t TOTAL_LIMIT = 15 * 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t BLOCK_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  storage::ObResourceMap<int64_t, ObTmpFile> files_;
  ObStorageFileWithRef storage_file_;
  ObStorageFileHandle file_handle_;
  int64_t next_fd_;
  int64_t next_dir_;
  common::SpinRWLock rm_file_lock_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObTmpFileManager);
};

#define FILE_MANAGER_INSTANCE_V2 (::oceanbase::blocksstable::ObTmpFileManager::get_instance())

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_H_
