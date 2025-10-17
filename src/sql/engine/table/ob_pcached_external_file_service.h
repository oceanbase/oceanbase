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

#ifndef SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_H_
#define SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_H_

#include "lib/hash/ob_hashmap.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "ob_external_data_access_mgr.h"
#include "lib/container/ob_se_array.h"
#include "ob_pcached_external_file_service_scheduler.h"
#include "share/ob_ptr_handle.h"
#include "storage/macro_cache/ob_storage_disk_space_meta.h"

namespace oceanbase
{
namespace sql
{

class ObPCachedExtMacroKey
{
public:
  ObPCachedExtMacroKey();
  ObPCachedExtMacroKey(
      const common::ObString &url,
      const common::ObString &content_digest,
      const int64_t modify_time,
      const int64_t offset);
  bool is_valid() const;
  int64_t offset_idx() const;
  int64_t block_start_offset() const;
  int assign(const ObPCachedExtMacroKey &other);

  const common::ObString &url() const { return url_; }
  const storage::ObExtFileVersion &file_version() const { return file_version_; }
  int64_t offset() const { return offset_; }

  TO_STRING_KV(K(url_), K(file_version_), K(offset_));

private:
  common::ObString url_;
  storage::ObExtFileVersion file_version_;
  int64_t offset_;
};

using ObExternalFileReversedPathMap =
    common::hash::ObHashMap<blocksstable::MacroBlockId, ObPCachedExtMacroKey>;

class ObExternalFilePathMap final
{
private:
  // do not hold url memory
  struct PathMapKey
  {
    PathMapKey() : url_(), content_digest_(), modify_time_(-1) {}
    PathMapKey(
        const common::ObString &url,
        const common::ObString &content_digest,
        const int64_t modify_time)
        : url_(url), content_digest_(content_digest), modify_time_(modify_time)
    {}
    PathMapKey(const ObPCachedExtMacroKey &macro_key)
        : url_(macro_key.url()),
          content_digest_(macro_key.file_version().content_digest()),
          modify_time_(macro_key.file_version().modify_time())
    {}

    void reset()
    {
      url_.reset();
      content_digest_.reset();
      modify_time_ = -1;
    }

    bool is_valid() const
    {
      return !url_.empty() && (!content_digest_.empty() || modify_time_ > 0);
    }

    bool operator==(const PathMapKey &other) const
    {
      return modify_time_ == other.modify_time_
          && url_ == other.url_
          && content_digest_ == other.content_digest_;
    }

    int hash(uint64_t &hash_val) const
    {
      hash_val = 0;
      hash_val = url_.hash();
      const int64_t content_digest_hash = content_digest_.hash();
      hash_val = common::murmurhash(&content_digest_hash, sizeof(content_digest_hash), hash_val);
      hash_val = common::murmurhash(&modify_time_, sizeof(modify_time_), hash_val);
      return OB_SUCCESS;
    }

    TO_STRING_KV(K(url_), K(content_digest_), K(modify_time_));

    common::ObString url_;
    common::ObString content_digest_;
    int64_t modify_time_;
  };

  struct MacroMapKey
  {
    MacroMapKey() : path_id_(UINT64_MAX), offset_idx_(-1) {}
    MacroMapKey(const uint64_t path_id, const int64_t offset_idx)
        : path_id_(path_id), offset_idx_(offset_idx)
    {}

    bool is_valid() const
    {
      return path_id_ < UINT64_MAX && offset_idx_ >= 0;
    }

    bool operator==(const MacroMapKey &other) const
    {
      return path_id_ == other.path_id_ && offset_idx_ == other.offset_idx_;
    }

    int hash(uint64_t &hash_val) const
    {
      hash_val = 0;
      hash_val = common::murmurhash(&path_id_, sizeof(path_id_), hash_val);
      hash_val = common::murmurhash(&offset_idx_, sizeof(offset_idx_), hash_val);
      return OB_SUCCESS;
    }

    TO_STRING_KV(K(path_id_), K(offset_idx_));

    uint64_t path_id_;
    int64_t offset_idx_;
  };

  //if key already exist, execute update_callback.
  template <typename KeyType_, typename ValueType_>
  class GetExistingValueUpdateCallback
  {
  public:
    GetExistingValueUpdateCallback(ValueType_ &value)
        : is_exist_(false), value_(value)
    {}

    void operator()(const common::hash::HashMapPair<KeyType_, ValueType_> &entry)
    {
      is_exist_ = true;
      value_ = entry.second;
    };

    bool is_exist() const { return is_exist_; }

  private:
    bool is_exist_;
    ValueType_ &value_;
  };

  template <typename KeyType_, typename ValueType_>
  class EmptySetCallback
  {
  public:
    EmptySetCallback() {}

    int operator()(const common::hash::HashMapPair<KeyType_, ValueType_> &entry)
    {
      return OB_SUCCESS;
    };
  };

  class PathMapKeyDeletePred final
  {
  public:
    PathMapKeyDeletePred() :
        shadow_copied_url_(), shadow_copied_content_digest_(), ret_(OB_SUCCESS) {}
    bool operator()(const common::hash::HashMapPair<PathMapKey, uint64_t> &entry)
    {
      shadow_copied_url_.assign_ptr(entry.first.url_.ptr(), entry.first.url_.length());
      shadow_copied_content_digest_.assign_ptr(
          entry.first.content_digest_.ptr(), entry.first.content_digest_.length());
      ret_ = OB_SUCCESS;
      return true;
    }
    int get_ret() const { return ret_; }
    common::ObString shadow_copied_url_;
    common::ObString shadow_copied_content_digest_;
    int ret_;
  };

  template <typename ValueType_, typename KeyType_>
  class ConstructReversedMapCB final
  {
  public:
    ConstructReversedMapCB(common::hash::ObHashMap<ValueType_, KeyType_> &reversed_map)
        : reversed_map_(reversed_map)
    {}
    int operator()(const common::hash::HashMapPair<KeyType_, ValueType_> &entry)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(reversed_map_.set_refactored(entry.second, entry.first))) {
        if (OB_HASH_EXIST == ret) {
          OB_LOG(ERROR, "fail to set reversed map", KR(ret), K(entry.second), K(entry.first));
        } else {
          OB_LOG(WARN, "fail to set reversed map", KR(ret), K(entry.second), K(entry.first));
        }
      }
      return ret;
    }

    common::hash::ObHashMap<ValueType_, KeyType_> &reversed_map_;
  };

  template <typename KeyType_, typename ValueType_>
  class CopyMapCB final
  {
  public:
    CopyMapCB(common::hash::ObHashMap<KeyType_, ValueType_> &copied_map)
        : copied_map_(copied_map)
    {}
    int operator()(const common::hash::HashMapPair<KeyType_, ValueType_> &entry)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(copied_map_.set_refactored(entry.first, entry.second))) {
        if (OB_HASH_EXIST == ret) {
          OB_LOG(ERROR, "fail to set copied map", KR(ret), K(entry.second), K(entry.first));
        } else {
          OB_LOG(WARN, "fail to set copied map", KR(ret), K(entry.second), K(entry.first));
        }
      }
      return ret;
    }

    common::hash::ObHashMap<KeyType_, ValueType_> &copied_map_;
  };

public:
  ObExternalFilePathMap();
  ~ObExternalFilePathMap();

  int init(const uint64_t tenant_id);
  void destroy();
  int64_t bucket_count() const;

  int get(
      const ObPCachedExtMacroKey &macro_key,
      blocksstable::MacroBlockId &macro_id) const;
  int get_or_generate(
      const ObPCachedExtMacroKey &macro_key,
      blocksstable::MacroBlockId &macro_id);
  // alloc a macro block but do not put into map
  int alloc_macro(
      const ObPCachedExtMacroKey &macro_key,
      blocksstable::ObStorageObjectHandle &macro_handle);
  int overwrite(
      const ObPCachedExtMacroKey &macro_key,
      const blocksstable::ObStorageObjectHandle &macro_handle);

  int build_reversed_path_mapping(ObExternalFileReversedPathMap &reversed_path_map);
  int erase_orphaned_macro_id(const ObPCachedExtMacroKey &macro_key);

private:
  int generate_path_id_(uint64_t &path_id);
  int generate_macro_id_(
      const MacroMapKey &macro_map_key,
      blocksstable::ObStorageObjectHandle &macro_handle);
  int get_or_generate_path_id_(
      const PathMapKey &path_map_key,
      uint64_t &path_id);
  int get_or_generate_macro_id_(
      const MacroMapKey &macro_map_key,
      blocksstable::MacroBlockId &macro_id);

  int cleanup_orphaned_path_entries_(
      const common::hash::ObHashMap<uint64_t, PathMapKey> &reversed_path_id_map,
      const common::hash::ObHashSet<uint64_t> &path_id_in_both_map,
      int64_t &erased_path_num, int64_t &erased_path_mem_size);

private:
  static const int64_t N_WAY = 16;
  static const int64_t DEFAULT_BLOCK_SIZE = 128 * 1024LL;

  bool is_inited_;
  common::ObBlockAllocMgr mem_limiter_;
  common::ObVSliceAlloc allocator_;
  // path url + modify_time -> path id
  common::hash::ObHashMap<PathMapKey, uint64_t> path_id_map_;
  // path id + offset_idx -> macro block id
  common::hash::ObHashMap<MacroMapKey, blocksstable::MacroBlockId> macro_id_map_;
  // TODO @fangdan: change to a persistent sequence id
  // to avoid sequence number conflicts after process restart
  uint64_t sn_path_id_;
};

class ObPCachedExtLRU;
class ObPCachedExtLRUListEntry : public common::ObDLinkBase<ObPCachedExtLRUListEntry>
{
public:
  ObPCachedExtLRUListEntry(
      ObPCachedExtLRU &lru,
      const blocksstable::MacroBlockId &macro_id,
      const uint32_t data_size);
  virtual ~ObPCachedExtLRUListEntry();

  void reset();
  bool is_valid() const;
  static bool is_valid_data_size(const uint32_t data_size);

  void inc_ref_count();
  void dec_ref_count();
  bool inc_and_check_access_cnt();
  void set_last_access_time_us(const int64_t last_access_time_us);
  int64_t get_last_access_time_us() const;
  void set_macro_id(const blocksstable::MacroBlockId &macro_id);
  const blocksstable::MacroBlockId &get_macro_id() const;
  void set_data_size(const uint32_t data_size);
  uint32_t get_data_size() const;

  TO_STRING_KV(KP(&lru_), KP(&lock_), K(ref_cnt_), K(access_cnt_), K(last_access_time_us_),
      K(macro_id_), K(data_size_), KP(prev_), KP(next_));

private:
  static const int64_t UPDATE_LRU_LIST_THRESHOLD = 5;

  ObPCachedExtLRU &lru_;
  common::SpinRWLock lock_;
  int32_t ref_cnt_;
  uint32_t access_cnt_;
  uint64_t last_access_time_us_;
  blocksstable::MacroBlockId macro_id_;
  uint32_t data_size_;

  DISALLOW_COPY_AND_ASSIGN(ObPCachedExtLRUListEntry);
};
using ObPCachedExtLRUListEntryHandle = storage::ObPtrHandle<ObPCachedExtLRUListEntry>;

class ObPCachedExtLRU final
{
public:
  using ObPCachedExtLRUMap =
      common::hash::ObHashMap<blocksstable::MacroBlockId, ObPCachedExtLRUListEntryHandle>;
  using ObPCachedExtLRUMapPairType =
      common::hash::HashMapPair<blocksstable::MacroBlockId, ObPCachedExtLRUListEntryHandle>;
  using ObPCachedExtLRUDList = common::ObDList<ObPCachedExtLRUListEntry>;

public:
  ObPCachedExtLRU();
  ~ObPCachedExtLRU();
  void destroy();
  int init(const uint64_t tenant_id);

  int put(const blocksstable::ObStorageObjectHandle &macro_handle, const uint32_t data_size);
  int get(
      const blocksstable::MacroBlockId &macro_id,
      bool &is_exist,
      blocksstable::ObStorageObjectHandle &macro_handle,
      uint32_t &data_size);
  int exist(const blocksstable::MacroBlockId &macro_id, bool &is_exist) const;
  int erase(const blocksstable::MacroBlockId &macro_id, bool &is_erased);
  // @param force_evict Whether to force eviction, ignoring MIN_RETENTION_DURATION_US
  //     - false: Normal eviction mode, only evict entries that exceed MIN_RETENTION_DURATION_US
  //     - true: Force eviction mode, ignore MIN_RETENTION_DURATION_US
  int evict(
      const int64_t expect_evict_num,
      int64_t &actual_evict_num,
      const bool force_evict = false);
  int expire(const int64_t expire_before_time_us, int64_t &actual_expire_num);

  int64_t cached_macro_count() const
  {
    return get_lru_entry_alloc_cnt_();
  }
  int64_t disk_size_allocated() const
  {
    return cached_macro_count() * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  }
  int64_t disk_size_in_use() const
  {
    return ATOMIC_LOAD(&disk_size_in_use_);
  }

private:
  int construct_lru_entry_handle_(
      const blocksstable::MacroBlockId &macro_id,
      const uint32_t data_size,
      ObPCachedExtLRUListEntryHandle &entry_handle);
  void delete_lru_entry_(ObPCachedExtLRUListEntry *lru_entry);
  void destroy_macro_map_();

  void update_lru_entry_alloc_cnt_(const int64_t delta)
  {
    ATOMIC_AAF(&lru_entry_alloc_cnt_, delta);
  }
  int64_t get_lru_entry_alloc_cnt_() const
  {
    return ATOMIC_LOAD(&lru_entry_alloc_cnt_);
  }

private:
  class ObPCachedExtLRUBaseCB
  {
  public:
    explicit ObPCachedExtLRUBaseCB(ObPCachedExtLRU &lru);
    virtual ~ObPCachedExtLRUBaseCB();
    int get_ret() const;
  protected:
    ObPCachedExtLRU &lru_;
    int ret_;
  };

  class ObPCachedExtLRUSetCB final : public ObPCachedExtLRUBaseCB
  {
  public:
    explicit ObPCachedExtLRUSetCB(ObPCachedExtLRU &lru);
    virtual ~ObPCachedExtLRUSetCB();
    int operator()(const ObPCachedExtLRUMapPairType &map_entry);
  private:
    DISALLOW_COPY_AND_ASSIGN(ObPCachedExtLRUSetCB);
  };

  class ObPCachedExtLRUDeletePred : public ObPCachedExtLRUBaseCB
  {
  public:
    explicit ObPCachedExtLRUDeletePred(ObPCachedExtLRU &lru);
    virtual ~ObPCachedExtLRUDeletePred();
    bool operator()(const ObPCachedExtLRUMapPairType &map_entry);
  private:
    DISALLOW_COPY_AND_ASSIGN(ObPCachedExtLRUDeletePred);
  };

  class ObPCachedExtLRUReadCB final : public ObPCachedExtLRUBaseCB
  {
  public:
    ObPCachedExtLRUReadCB(ObPCachedExtLRU &lru, blocksstable::ObStorageObjectHandle &macro_handle);
    virtual ~ObPCachedExtLRUReadCB();
    void operator()(const ObPCachedExtLRUMapPairType &map_entry);
    uint32_t get_data_size() const { return data_size_; }
  private:
    blocksstable::ObStorageObjectHandle &macro_handle_;
    uint32_t data_size_;
    DISALLOW_COPY_AND_ASSIGN(ObPCachedExtLRUReadCB);
  };

  friend class ObPCachedExtLRUListEntry;
  friend class ObPCachedExtLRUSetCB;
  friend class ObPCachedExtLRUDeletePred;
  friend class ObPCachedExtLRUReadCB;

private:
  int add_into_lru_list_(ObPCachedExtLRUListEntry &lru_entry);
  int remove_from_lru_list_(ObPCachedExtLRUListEntry &lru_entry);
  int update_lru_list_(ObPCachedExtLRUListEntry &lru_entry);

private:
  static constexpr const char *EXT_LRU_ALLOC_TAG = "PCachedExtLRU";
  static const int64_t MIN_RETENTION_DURATION_US = 30LL * common::S_US; // 30s

private:
  // Lock order: Always acquire macro_map_'s bucket locks before this lock_
  // lock_ is used for lru_list_
  lib::ObMutex lock_;
  bool is_inited_;
  common::ObSmallAllocator lru_entry_allocator_;
  int64_t lru_entry_alloc_cnt_;
  ObPCachedExtLRUMap macro_map_;
  ObPCachedExtLRUDList lru_list_;
  int64_t disk_size_in_use_;

  DISALLOW_COPY_AND_ASSIGN(ObPCachedExtLRU);
};

class ObExternalFileDiskSpaceMgr final
{
private:
  class ObExternalFileCheckDiskUsageTask final : public common::ObTimerTask
  {
  public:
    ObExternalFileCheckDiskUsageTask() {}
    virtual ~ObExternalFileCheckDiskUsageTask() {}
    virtual void runTimerTask() override;
  };

public:
  static ObExternalFileDiskSpaceMgr &get_instance();

  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  void update_cached_macro_count(const int64_t delta);
  int64_t cached_macro_count() const;
  int64_t disk_size_allocated() const;
  int check_disk_space(bool &is_full) const;
  int check_disk_usage_and_evict_if_needed();

private:
  // check internal + external disk usage
  // use MAX(MAX_DISK_USAGE_PERCENT, GCONF.external_table_disk_cache_max_percentage)
  // as the max disk usage percentage
  bool is_total_disk_usage_exceed_limit_() const;
  // check external disk usage satisfies the GCONF.external_table_disk_cache_max_percentage
  bool is_external_file_disk_space_full_() const;

  // tenant_id -> macro count limit to maintain after eviction
  using TenantMacroNumMap = common::hash::ObHashMap<uint64_t, int64_t>;
  int cal_tenant_macro_count_limit_(
      TenantMacroNumMap &tenant_macro_num_map,
      const int64_t expect_total_evict_num) const;
  int evict_cached_macro_by_tenant_(
      const uint64_t tenant_id,
      const int64_t macro_count_limit,
      const bool force_evict,
      int64_t &actual_evict_num);

private:
  // When the disk usage exceeds EVICT_TRIGGER_PERCENT, start to evict
  // Every evict operation will evict MIN(EVICT_BATCH_SIZE_LIMIT, EVICT_BATCH_SIZE_RATIO * total_macro_count) macros
  static const int64_t EVICT_TRIGGER_PERCENT = 80;
  // when the disk usage exceeds MAX_DISK_USAGE_PERCENT, new macro allocation is not allowed
  static const int64_t MAX_DISK_USAGE_PERCENT = 90;
  // Maximum number of macro blocks to evict in a single eviction operation
  static const int64_t EVICT_BATCH_SIZE_LIMIT = 1000;
  // Percentage of macro blocks to evict in a single eviction operation (in percentage points)
  static const int64_t EVICT_BATCH_SIZE_RATIO = 5;  // 5%

  static const int64_t CHECK_DISK_USAGE_INTERVAL_US = 2LL * common::S_US; // 2s

  ObExternalFileDiskSpaceMgr();
  ~ObExternalFileDiskSpaceMgr();

private:
  bool is_inited_;
  int tg_id_;
  int64_t cached_macro_count_;
  ObExternalFileCheckDiskUsageTask check_disk_usage_task_;
};

class ObPCachedExternalFileService
{
public:
  ObPCachedExternalFileService();
  virtual ~ObPCachedExternalFileService();

  int init(const uint64_t tenant_id);
  static int mtl_init(ObPCachedExternalFileService *&accesser);
  int start();
  void stop();
  void wait();
  void destroy();

  int async_read(
     const ObExternalAccessFileInfo &external_file_info,
     const ObExternalReadInfo &external_read_info,
     blocksstable::ObStorageObjectHandle &io_handle);

  int ss_add_prefetch_task(
      const common::ObString &url,
      const common::ObObjectStorageInfo *access_info,
      const int64_t offset_idx,
      const blocksstable::MacroBlockId &macro_id);
  int ss_add_prefetch_task(
      const char *data,
      const int64_t data_size,
      const blocksstable::MacroBlockId &macro_id);
  int sn_add_prefetch_task(
      const ObPCachedExtMacroKey &macro_key,
      const common::ObObjectStorageInfo *access_info);
  int sn_add_prefetch_task(
      const ObPCachedExtMacroKey &macro_key,
      const char *data,
      const int64_t data_size);
  int finish_running_prefetch_task(const uint64_t task_hash_key);

  int get_cached_macro(
      const ObPCachedExtMacroKey &macro_key,
      const int64_t offset,
      const int64_t read_size,
      bool &is_cached,
      blocksstable::ObStorageObjectHandle &macro_handle);
  int check_macro_cached(
      const ObPCachedExtMacroKey &macro_key,
      bool &is_cached) const;
  int alloc_macro(
      const ObPCachedExtMacroKey &macro_key,
      blocksstable::ObStorageObjectHandle &macro_handle);
  int cache_macro(
      const ObPCachedExtMacroKey &macro_key,
      const blocksstable::ObStorageObjectHandle &macro_handle,
      const uint32_t data_size);

  int expire_cached_macro(const int64_t expire_before_time_us, int64_t &actual_expire_num);
  // @param force_evict Whether to force eviction, ignoring MIN_RETENTION_DURATION_US
  //     - false: Normal eviction mode, only evict entries that exceed MIN_RETENTION_DURATION_US
  //     - true: Force eviction mode, ignore MIN_RETENTION_DURATION_US
  int evict_cached_macro(
      const int64_t expect_evict_num,
      int64_t &actual_evict_num,
      const bool force_evict = false);
  int cleanup_orphaned_path();

  static bool is_read_range_valid(const int64_t offset, const int64_t size);
  int get_cache_stat(storage::ObStorageCacheStat &cache_stat) const;
  int get_hit_stat(storage::ObStorageCacheHitStat &hit_stat) const;
  int get_io_callback_allocator(common::ObIAllocator *&allocator);

private:
  int check_init_and_stop_() const;

  int ss_async_read_(
      const ObExternalAccessFileInfo &external_file_info,
      const ObExternalReadInfo &external_read_info,
      blocksstable::ObStorageObjectHandle &io_handle);

  int sn_async_read_(
      const ObExternalAccessFileInfo &external_file_info,
      const ObExternalReadInfo &external_read_info,
      blocksstable::ObStorageObjectHandle &io_handle);

  int async_read_from_object_storage_(
      const common::ObString &url,
      const common::ObObjectStorageInfo *access_info,
      const ObExternalReadInfo &external_read_info,
      common::ObIOHandle &io_handle) const;

  int async_read_object_manager_(
      const common::ObString &url,
      const common::ObObjectStorageInfo *access_info,
      const ObExternalReadInfo &external_read_info,
      const blocksstable::MacroBlockId &macro_id,
      blocksstable::ObStorageObjectHandle &io_handle) const;

private:
  class SNAddPrefetchTaskSetCb final
  {
  public:
    SNAddPrefetchTaskSetCb(const ObPCachedExtMacroKey &macro_key);
    int operator()(const common::hash::HashMapPair<uint64_t, bool> &entry);
  private:
    const ObPCachedExtMacroKey &macro_key_;
    DISALLOW_COPY_AND_ASSIGN(SNAddPrefetchTaskSetCb);
  };

private:
  static const int64_t IO_CALLBACK_MEM_LIMIT = 128LL * common::MB; // 128MB
  static const int64_t MAX_RUNNING_PREFETCH_TASK_NUM = 64;

  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  common::ObConcurrentFIFOAllocator io_callback_allocator_;
  ObExternalFilePathMap path_map_;
  ObPCachedExtLRU lru_;
  ObPCachedExtServiceScheduler timer_task_scheduler_;
  // Tracks currently running prefetch tasks to avoid duplicates
  // - SS mode: key = hash(macro_id)
  // - SN mode: key = hash(url + access_info + offset_idx)
  // Using hash values unifies keys across modes. Although hash collisions may occur,
  // the map capacity is very small (MAX_RUNNING_PREFETCH_TASK_NUM = 64), making
  // collision probability extremely low. Even if collisions happen, missing one
  // prefetch task has minimal impact on performance.
  common::hash::ObHashMap<uint64_t, bool> running_prefetch_tasks_map_;
  storage::ObStorageCacheHitStat hit_stat_;
};

class ObExternalRemoteIOCallback : public common::ObIOCallback
{
public:
  ObExternalRemoteIOCallback();
  virtual ~ObExternalRemoteIOCallback();

  static int construct_io_callback(
      const blocksstable::ObStorageObjectReadInfo *read_info,
      common::ObIOInfo &io_info);
  static int construct_io_callback(
      const ObExternalAccessFileInfo &external_file_info,
      const ObExternalReadInfo &external_read_info,
      ObExternalReadInfo &new_external_read_info);
  static void free_io_callback_and_detach_original(common::ObIOCallback *&io_callback);

  // During initialization, ObExternalRemoteIOCallback clones the provided URL and access info.
  // The memory needed for cloning is allocated using the `allocator`
  int init(
      common::ObIAllocator *allocator,
      common::ObIOCallback *original_callback,
      char *user_data_buf,
      const int64_t offset_idx,
      const common::ObString &url,
      const common::ObObjectStorageInfo *access_info,
      const blocksstable::MacroBlockId &macro_id);
  int init(
      common::ObIAllocator *allocator,
      common::ObIOCallback *original_callback,
      char *user_data_buf,
      const int64_t offset_idx,
      const common::ObString &url,
      const common::ObObjectStorageInfo *access_info,
      const common::ObString &content_digest,
      const int64_t modify_time);

  virtual common::ObIAllocator *get_allocator() override { return allocator_; }
  virtual const char *get_data() override
  {
    return OB_NOT_NULL(ori_callback_) ? ori_callback_->get_data() : user_data_buf_;
  }
  virtual int64_t size() const override
  {
    return sizeof(ObExternalRemoteIOCallback);
  }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
  {
    UNUSED(io_data_buffer);
    UNUSED(data_size);
    return OB_NOT_SUPPORTED;
  }
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  virtual const char *get_cb_name() const override
  {
    return "ObExternalRemoteIOCallback";
  }

  common::ObIOCallback *clear_original_io_callback()
  {
    common::ObIOCallback *original_io_callback = ori_callback_;
    ori_callback_ = nullptr;
    return original_io_callback;
  }

  VIRTUAL_TO_STRING_KV(K(is_inited_), KP(allocator_), KPC(ori_callback_),
      KP(user_data_buf_), K(offset_idx_), K(url_), KPC(access_info_),
      K(macro_id_), K(content_digest_), K(modify_time_));

private:
  // During initialization, ObExternalRemoteIOCallback clones the provided URL and access info.
  // The memory needed for cloning is allocated using the `allocator`
  int base_init_(
      common::ObIAllocator *allocator,
      common::ObIOCallback *original_callback,
      char *user_data_buf,
      const int64_t offset_idx,
      const common::ObString &url,
      const common::ObObjectStorageInfo *access_info);

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;
  common::ObIOCallback *ori_callback_;
  char *user_data_buf_;
  int64_t offset_idx_;
  common::ObString url_;
  common::ObObjectStorageInfo *access_info_;
  blocksstable::MacroBlockId macro_id_; // for SS mode
  common::ObSmallString<64> content_digest_; // for SN mode
  int64_t modify_time_; // for SN mode
};

} // sql
} // oceanbase

#define OB_EXTERNAL_FILE_DISK_SPACE_MGR (oceanbase::sql::ObExternalFileDiskSpaceMgr::get_instance())

#endif // SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_H_