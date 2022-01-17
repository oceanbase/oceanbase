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

#ifndef OB_STORE_FILE_SYSTEM_H_
#define OB_STORE_FILE_SYSTEM_H_

#include "lib/io/ob_io_manager.h"
#include "lib/ob_define.h"
#include "ob_block_sstable_struct.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/ob_file_system_router.h"
#include "storage/ob_macro_meta_replay_map.h"
#include "storage/blocksstable/ob_macro_block_checker.h"

namespace oceanbase {
namespace storage {
class ObTenantFileKey;
class ObPartitionService;
class ObIPartitionGroupGuard;
}  // namespace storage
namespace blocksstable {
class ObMacroBlockReadInfo;
class ObMacroBlockWriteInfo;
class ObMacroBlockHandle;
class ObMacroBlocksHandle;
class ObMacroBlockInfo;
class ObBadBlockInfo;
class ObStoreFileSystem;
class ObStoreFileSystemWrapper;
class ObStorageFileHandle;

struct ObServerWorkingDir {
public:
  enum class DirStatus { NORMAL = 0, RECOVERING = 1, RECOVERED = 2, TEMPORARY = 3, DELETING = 4, MAX };

  ObServerWorkingDir();
  ~ObServerWorkingDir() = default;
  ObServerWorkingDir(const common::ObAddr svr, DirStatus status);

  void reset();
  bool is_valid() const;
  int to_path_string(char* path, const int64_t path_size) const;

  OB_INLINE const common::ObAddr& get_svr_addr() const
  {
    return svr_addr_;
  }
  OB_INLINE DirStatus get_status() const
  {
    return status_;
  }
  OB_INLINE bool is_normal() const
  {
    return DirStatus::NORMAL == status_;
  }

  TO_STRING_KV(K(svr_addr_), K(start_ts_), K(status_));

public:
  common::ObAddr svr_addr_;
  int64_t start_ts_;
  DirStatus status_;

  static const char* SVR_WORKING_DIR_PREFIX[(int32_t)DirStatus::MAX];
};

class ObFileSystemInspectBadBlockTask : public common::ObTimerTask {
public:
  ObFileSystemInspectBadBlockTask();
  virtual ~ObFileSystemInspectBadBlockTask();
  virtual void runTimerTask();
  void destroy();

private:
  int check_macro_block(const ObMacroBlockInfoPair& pair, const storage::ObTenantFileKey& file_key);
  int check_data_block(const MacroBlockId& macro_id, const blocksstable::ObFullMacroBlockMeta& full_meta,
      const storage::ObTenantFileKey& file_key);
  void inspect_bad_block();
  bool has_inited();

private:
  static const int64_t ACCESS_TIME_INTERVAL;
  static const int64_t MIN_OPEN_BLOCKS_PER_ROUND;
  static const int64_t MAX_SEARCH_COUNT_PER_ROUND;
  int64_t last_partition_idx_;
  int64_t last_sstable_idx_;
  int64_t last_macro_idx_;
  ObSSTableMacroBlockChecker macro_checker_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFileSystemInspectBadBlockTask);
};

struct ObStorageFileWithRef final {
public:
  ObStorageFileWithRef() : ref_cnt_(0), file_(nullptr)
  {}
  ~ObStorageFileWithRef() = default;
  void inc_ref();
  void dec_ref();
  int64_t get_ref();
  TO_STRING_KV(K_(ref_cnt), K_(file));
  int64_t ref_cnt_;
  blocksstable::ObStorageFile* file_;
};

struct ObStorageFileHandle final {
public:
  ObStorageFileHandle();
  ~ObStorageFileHandle();
  void reset();
  void set_storage_file_with_ref(ObStorageFileWithRef& file_with_ref);
  int assign(const ObStorageFileHandle& other);
  bool is_valid() const
  {
    return nullptr != file_with_ref_;
  }
  blocksstable::ObStorageFile* get_storage_file()
  {
    return nullptr == file_with_ref_ ? nullptr : file_with_ref_->file_;
  }
  TO_STRING_KV(KP_(file_with_ref));
  ObStorageFileWithRef* file_with_ref_;
};

struct ObStorageFilesHandle final {
public:
  static const int64_t DEFAULT_FILE_CNT = 100;
  typedef common::ObSEArray<ObStorageFileWithRef*, DEFAULT_FILE_CNT> FileArray;
  ObStorageFilesHandle();
  ~ObStorageFilesHandle();
  void reset();
  int add_storage_file(ObStorageFileWithRef& file);
  int get_storage_file(const int64_t idx, blocksstable::ObStorageFile*& file);
  int64_t get_count() const
  {
    return file_array_.count();
  }
  TO_STRING_KV(K_(file_array));

private:
  FileArray file_array_;
};

class ObStorageFile {
public:
  enum class FileType {
    SERVER_ROOT = 0,
    TMP_FILE,
    TENANT_DATA,  // abandon this after support OFS block interface
    TENANT_DATA_INFO,
    MAX
  };

  virtual ~ObStorageFile() = default;

  virtual int init(const common::ObAddr& svr_addr, const FileType file_type) = 0;
  virtual int init(
      const common::ObAddr& svr_addr, const uint64_t tenant_id, const int64_t file_id, const FileType file_type) = 0;
  virtual void reset();

  // only for improving merge performance.
  void reserve_ref_cnt_map_for_merge();
  int inc_ref(const MacroBlockId& macro_id);
  int dec_ref(const MacroBlockId& macro_id);

  virtual int open(const int flags) = 0;
  virtual int close() = 0;
  virtual int unlink() = 0;
  virtual int fsync() = 0;
  virtual int read_super_block(ObISuperBlock& super_block) = 0;
  virtual int write_super_block(const ObISuperBlock& super_block) = 0;
  virtual int async_read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle) = 0;
  virtual int async_write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle) = 0;
  virtual int write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle) = 0;
  virtual int read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle) = 0;

  OB_INLINE FileType get_file_type() const
  {
    return file_type_;
  }
  OB_INLINE uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  OB_INLINE int get_fd() const
  {
    return fd_;
  }
  OB_INLINE storage::ObMacroMetaReplayMap& get_replay_map()
  {
    return replay_map_;
  }
  void set_fd(const int fd)
  {
    fd_ = fd;
  }
  virtual const char* get_path() const = 0;
  virtual int64_t get_file_id() const
  {
    return file_id_;
  }
  virtual void enable_mark_and_sweep();
  virtual void disable_mark_and_sweep();
  virtual bool get_mark_and_sweep_status();

  VIRTUAL_TO_STRING_KV(K(file_type_), K(fd_), K(tenant_id_), K(file_id_), K(is_inited_));

protected:
  ObStorageFile();
  int init_base(const FileType file_type, const uint64_t tenant_id);

  struct BlockInfo {
    int32_t ref_cnt_;
    int64_t access_time_;
    BlockInfo() : ref_cnt_(0), access_time_(0)
    {}
    void reset()
    {
      ref_cnt_ = 0;
      access_time_ = 0;
    }
    TO_STRING_KV(K_(ref_cnt), K_(access_time));
  };

  typedef common::ObLinearHashMap<MacroBlockId, BlockInfo> MacroBlockInfo;

protected:
  static const int64_t LOCK_BUCKET_CNT = 1000;
  uint64_t tenant_id_;
  int64_t file_id_;
  FileType file_type_;
  int fd_;
  bool is_inited_;
  // structure to manage macro block reference count in the file
  MacroBlockInfo macro_block_info_;  // for block reference count
  common::ObBucketLock lock_;
  storage::ObMacroMetaReplayMap replay_map_;  // used when replay slog

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageFile);
};

struct ObStoreFileWriteInfo {
  MacroBlockId block_id_;
  const char* buf_;
  int64_t size_;
  common::ObIODesc io_desc_;
  common::ObIOCallback* io_callback_;

  ObStoreFileCtx* ctx_;
  // reuse_block_ctx_ are only used when reuse old block id
  const ObMacroBlockCtx* reuse_block_ctx_;
  ObStorageFile* store_file_;

  ObStoreFileWriteInfo()
      : block_id_(),
        buf_(NULL),
        size_(0),
        io_desc_(),
        io_callback_(NULL),
        ctx_(NULL),
        reuse_block_ctx_(NULL),
        store_file_(nullptr)
  {}
  OB_INLINE bool is_valid() const;
  OB_INLINE bool is_reuse_macro_block() const
  {
    return NULL != reuse_block_ctx_;
  }
  TO_STRING_KV(
      K_(block_id), KP_(buf), K_(size), K_(io_desc), KP_(io_callback), K_(ctx), K_(reuse_block_ctx), KP_(store_file));
};

struct ObStoreFileReadInfo {
  const ObMacroBlockCtx* macro_block_ctx_;
  int64_t offset_;
  int64_t size_;
  common::ObIODesc io_desc_;
  common::ObIOCallback* io_callback_;
  ObStorageFile* store_file_;

  ObStoreFileReadInfo()
      : macro_block_ctx_(NULL), offset_(0), size_(0), io_desc_(), io_callback_(NULL), store_file_(nullptr)
  {}
  OB_INLINE bool is_valid() const;
  TO_STRING_KV(K_(offset), K_(size), K_(io_desc), KP_(io_callback), K_(macro_block_ctx), KP_(store_file));
};

struct ObDiskStat final {
  int64_t disk_idx_;
  int64_t install_seq_;
  int64_t create_ts_;
  int64_t finish_ts_;
  int64_t percent_;
  const char* status_;
  char alias_name_[common::MAX_PATH_SIZE];

  ObDiskStat();
  TO_STRING_KV(K_(disk_idx), K_(install_seq), K_(create_ts), K_(finish_ts), K_(percent), K_(status), K_(alias_name));
};

struct ObDiskStats final {
  common::ObArray<ObDiskStat> disk_stats_;
  int64_t data_num_;
  int64_t parity_num_;

  ObDiskStats();
  void reset();
  TO_STRING_KV(K_(data_num), K_(parity_num), K_(disk_stats));
};

class ObStoreFileSystem {
public:
  static const int64_t RESERVED_MACRO_BLOCK_INDEX = 2;  // used for super block macro blocks
public:
  ObStoreFileSystem();
  virtual ~ObStoreFileSystem()
  {}

  virtual int async_write(const ObStoreFileWriteInfo& write_info, common::ObIOHandle& io_handle);
  virtual int async_read(const ObStoreFileReadInfo& read_info, common::ObIOHandle& io_handle);
  virtual int fsync();
  virtual int64_t get_total_data_size() const;
  virtual int free_file(const ObStoreFileCtx* ctx);
  virtual int init_file_ctx(const ObStoreFileType& file_type, blocksstable::ObStoreFileCtx& file_ctx) const;

  virtual int add_disk(
      const common::ObString& diskgroup_name, const common::ObString& disk_path, const common::ObString& alias_name);
  virtual int drop_disk(const common::ObString& diskgroup_name, const common::ObString& alias_name);
  virtual int get_disk_status(ObDiskStats& disk_stats);
  virtual bool is_disk_full() const;

  // storage 3.0 interface
  virtual int alloc_file(ObStorageFile*& file);
  virtual int free_file(ObStorageFile*& file);

  // used for block reference count management
  // alloc_block to increase ref cnt is implied in asyn_write
  // used in when block ref cnt in PG decreased to 0
  virtual int unlink_block(const ObStorageFile& file, const MacroBlockId& macro_id);
  // used in replay
  virtual int link_block(const ObStorageFile& file, const MacroBlockId& macro_id);
  // used in migration and major sstable sharing
  virtual int link_blocks(const ObStorageFile& src_file, const common::ObIArray<MacroBlockId>& src_macro_block_ids,
      ObStorageFile& dest_file, ObMacroBlocksHandle& dest_macro_blocks_handle);

  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual int64_t get_macro_block_size() const;
  virtual int64_t get_total_macro_block_count() const;
  virtual int64_t get_free_macro_block_count() const;
  virtual int64_t get_used_macro_block_count() const;
  virtual ObStorageFileHandle& get_server_root_handle()
  {
    return server_root_handle_;
  }
  virtual int get_marker_status(ObMacroBlockMarkerStatus& status);

  // used in report bad block.
  virtual int get_macro_block_info(
      const storage::ObTenantFileKey& file_key, const MacroBlockId& macro_block_id, ObMacroBlockInfo& macro_block_info);
  virtual int report_bad_block(
      const MacroBlockId& macro_block_id, const int64_t error_type, const char* error_msg, const char* file_path);
  virtual int get_bad_block_infos(common::ObArray<ObBadBlockInfo>& bad_block_infos);

  // Local storage file has no physical entity so it uses file system interface to read/write disk.
  // And write will update cached super_block_ in memory
  // OFS storage file has real file entity so it uses file interface to read/write disk.
  // And write will update cached super_block_ in memory
  virtual int write_server_super_block(const ObServerSuperBlock& super_block);
  virtual int read_server_super_block(ObServerSuperBlock& super_block);
  virtual int read_old_super_block(ObSuperBlockV2& super_block)
  {
    UNUSED(super_block);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_super_block_version(int64_t& super_block_version)
  {
    UNUSED(super_block_version);
    return common::OB_NOT_SUPPORTED;
  }

  OB_INLINE storage::ObPartitionService& get_partition_service()
  {
    return *partition_service_;
  }
  OB_INLINE const ObServerSuperBlock& get_server_super_block() const
  {
    return super_block_;
  }
  virtual int resize_file(const int64_t new_data_file_size, const int64_t new_data_file_disk_percentage);

  VIRTUAL_TO_STRING_KV("ObStoreFileSystem", "empty");

protected:
  // file system can only be inited and destroy through ObStoreFileSystemWrapper
  friend class ObStoreFileSystemWrapper;
  friend class ObFileSystemInspectBadBlockTask;
  virtual int init(const ObStorageEnv& storage_env, storage::ObPartitionService& partition_service);
  virtual void destroy();

protected:
  storage::ObPartitionService* partition_service_;
  ObServerSuperBlock super_block_;  // read only memory cache
  ObStorageFileWithRef svr_root_file_with_ref_;
  ObStorageFileHandle server_root_handle_;
  bool is_inited_;
};

class ObStoreFileSystemWrapper final {
public:
  static ObStoreFileSystem& get_instance();
  static int init(const ObStorageEnv& storage_env, storage::ObPartitionService& partition_service);
  static void destroy();

private:
  static ObStoreFileSystem* fs_instance_;
};

template <class STORAGE_FILE>
class ObStorageFileAllocator final {
public:
  ObStorageFileAllocator();
  virtual ~ObStorageFileAllocator();
  void destroy();

  int alloc_file(STORAGE_FILE*& file);
  int free_file(STORAGE_FILE*& file);

  TO_STRING_KV(K(free_file_cnt_), K(used_file_cnt_));

private:
  static const int64_t MAX_FILE_CNT = 30000;
  STORAGE_FILE* free_files_[MAX_FILE_CNT];
  int64_t free_file_cnt_;
  int64_t used_file_cnt_;
  common::ObArenaAllocator allocator_;
  lib::ObMutex lock_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageFileAllocator);
};

template <class STORAGE_FILE>
ObStorageFileAllocator<STORAGE_FILE>::ObStorageFileAllocator()
    : free_files_(), free_file_cnt_(0), used_file_cnt_(0), allocator_(common::ObModIds::OB_STORE_FILE_SYSTEM), lock_()
{
  MEMSET(free_files_, 0, MAX_FILE_CNT * sizeof(ObStorageFile*));
}

template <class STORAGE_FILE>
ObStorageFileAllocator<STORAGE_FILE>::~ObStorageFileAllocator()
{
  destroy();
}

template <class STORAGE_FILE>
void ObStorageFileAllocator<STORAGE_FILE>::destroy()
{
  allocator_.clear();
  free_file_cnt_ = 0;
  used_file_cnt_ = 0;
  MEMSET(free_files_, 0, MAX_FILE_CNT * sizeof(ObStorageFile*));
}

template <class STORAGE_FILE>
int ObStorageFileAllocator<STORAGE_FILE>::alloc_file(STORAGE_FILE*& file)
{
  int ret = common::OB_SUCCESS;
  file = nullptr;
  void* buf = nullptr;

  if (nullptr != file) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "file ptr is not null", K(ret), KP(file));
  }

  if (OB_SUCC(ret)) {
    lib::ObMutexGuard guard(lock_);
    if (used_file_cnt_ >= MAX_FILE_CNT) {
      ret = common::OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "too many storage files", K(ret), K(used_file_cnt_), K(free_file_cnt_));
    } else if (free_file_cnt_ > 0) {
      file = free_files_[free_file_cnt_ - 1];
      if (OB_ISNULL(file)) {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "file is null", K(ret), KP(file));
      } else {
        file->reset();
        --free_file_cnt_;
        ++used_file_cnt_;
      }
    } else {
      if (nullptr == (buf = allocator_.alloc(sizeof(STORAGE_FILE)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "allocate memory fail", K(ret), KP(buf));
      } else {
        file = new (buf) STORAGE_FILE();
        file->reset();
        ++used_file_cnt_;
      }
    }
  }
  return ret;
}

template <class STORAGE_FILE>
int ObStorageFileAllocator<STORAGE_FILE>::free_file(STORAGE_FILE*& file)
{
  int ret = common::OB_SUCCESS;

  if (nullptr == file) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "file ptr is null", K(ret), KP(file));
  }

  if (OB_SUCC(ret)) {
    lib::ObMutexGuard guard(lock_);
    if (used_file_cnt_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "used file count not right", K(ret), K(used_file_cnt_));
    } else if (free_file_cnt_ >= MAX_FILE_CNT) {
      ret = common::OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "too many free files", K(ret), K(used_file_cnt_), K(free_file_cnt_));
    } else {
      file->reset();
      free_files_[free_file_cnt_] = file;
      ++free_file_cnt_;
      --used_file_cnt_;
    }
  }
  return ret;
}

bool ObStoreFileWriteInfo::is_valid() const
{
  bool bool_ret = true;

  if (!io_desc_.is_valid() || NULL == ctx_) {
    bool_ret = false;
  } else if (NULL != reuse_block_ctx_) {  // reuse block
    if (NULL != buf_ || 0 != size_) {
      bool_ret = false;
    }
  } else {  // not reuse block
    if (NULL == buf_ || 0 >= size_) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool ObStoreFileReadInfo::is_valid() const
{
  return OB_NOT_NULL(macro_block_ctx_) && offset_ >= 0 && size_ > 0 && io_desc_.is_valid();
}

}  // namespace blocksstable
}  // namespace oceanbase
#define OB_FILE_SYSTEM (::oceanbase::blocksstable::ObStoreFileSystemWrapper::get_instance())

#endif /* OB_STORE_FILE_SYSTEM_H_ */
