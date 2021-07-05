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

#ifndef OCEANBASE_CLOG_OB_FILE_ID_CACHE_H_
#define OCEANBASE_CLOG_OB_FILE_ID_CACHE_H_

#include "lib/allocator/ob_small_allocator.h"
#include "lib/container/ob_seg_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "ob_info_block_handler.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
class ObIlogAccessor;
// FileIdCache is used to manage InfoBlock information of all ilog files, and organize them into
// memory structure for query.
//
// The InfoBlock of the ilog file records each partition_key and its min_log_id, max_log_id,
// min_submit_timestamp, max_submit_timestamp in this ilog file,
//
// For version after 2.1(include 2.1), it's only recore the start_offset of the first ilog of
// this partition in the ilog file.
//
// For version before 2.1, the start_offset corresponds to the start_offset_index of the first
// ilog of this partition in the loaded ilog cache, OB_INVALID_OFFSET means it has not been loaded
// yet.
//
// The ilog of same partition may be existed in different ilog files, Firstly, FileIdCache organized
// by partition_key, and each partition_key corresponds to an ordered list, which can support binary
// search.
//
// For example,
// There are three ilog file on disk:
// f1.InfoBlock: {(p1,1), (p2,100), (p3,50)}
// f2.InfoBlock: {(p1,4), (p2,150) }
// f3.InfoBlock: {(p1,6), (p2,200), (p3,5000)}
//
// The memory structure is:
// p1 -> [(1,f1), (4,f2), (6,f3)]
// p2 -> [(100,f1), (150,f2), (200,f3)]
// p3 -> [(50,f1), (5000,f3)]
//
// When querying (p2, 170), first find the ordered list corresponding to p2, and then, use binary search.
class Log2File {
public:
  Log2File()
      : file_id_(common::OB_INVALID_FILE_ID),
        // For version after 2.1(include 2.1), start_offset correspond to the offset
        // of the first ilog of this partition in ilog file
        // For version before 2.1, the start_offset correspond to the start_offset_index
        // of the first ilog of this partition in ilog cache
        start_offset_(OB_INVALID_OFFSET),
        min_log_id_(common::OB_INVALID_ID),
        max_log_id_(common::OB_INVALID_ID),
        min_log_timestamp_(common::OB_INVALID_TIMESTAMP),
        max_log_timestamp_(common::OB_INVALID_TIMESTAMP)
  {}

  ~Log2File()
  {
    reset();
  }

public:
  file_id_t get_file_id() const
  {
    return file_id_;
  }
  offset_t get_start_offset() const
  {
    return start_offset_;
  }
  uint64_t get_min_log_id() const
  {
    return min_log_id_;
  }
  uint64_t get_max_log_id() const
  {
    return max_log_id_;
  }
  int64_t get_min_log_timestamp() const
  {
    return min_log_timestamp_;
  }
  int64_t get_max_log_timestamp() const
  {
    return max_log_timestamp_;
  }

  void reset()
  {
    file_id_ = common::OB_INVALID_FILE_ID;
    start_offset_ = OB_INVALID_OFFSET;
    min_log_id_ = common::OB_INVALID_ID;
    max_log_id_ = common::OB_INVALID_ID;
    min_log_timestamp_ = common::OB_INVALID_TIMESTAMP;
    max_log_timestamp_ = common::OB_INVALID_TIMESTAMP;
  }

  void reset(const file_id_t file_id, const offset_t start_offset, const uint64_t min_log_id, const uint64_t max_log_id,
      const int64_t min_log_timestamp, const int64_t max_log_timestamp)
  {
    file_id_ = file_id;
    start_offset_ = start_offset;
    min_log_id_ = min_log_id;
    max_log_id_ = max_log_id;
    min_log_timestamp_ = min_log_timestamp;
    max_log_timestamp_ = max_log_timestamp;
  }

  void set_min_log_id(const uint64_t min_log_id)
  {
    min_log_id_ = min_log_id;
  }
  void set_min_log_timestamp(const int64_t timestamp)
  {
    min_log_timestamp_ = timestamp;
  }

  int backfill(const offset_t start_offset)
  {
    int ret = common::OB_SUCCESS;
    ATOMIC_STORE(&start_offset_, start_offset);
    return ret;
  }

  const Log2File& operator=(const Log2File& that)
  {
    reset(that.get_file_id(),
        that.get_start_offset(),
        that.get_min_log_id(),
        that.get_max_log_id(),
        that.get_min_log_timestamp(),
        that.get_max_log_timestamp());
    return *this;
  }

  bool operator==(const Log2File& that) const
  {
    return file_id_ == that.file_id_ && start_offset_ == that.start_offset_ && min_log_id_ == that.min_log_id_ &&
           max_log_id_ == that.max_log_id_ && min_log_timestamp_ == that.min_log_timestamp_ &&
           max_log_timestamp_ == that.max_log_timestamp_;
  }

  bool is_valid() const
  {
    return common::OB_INVALID_FILE_ID != file_id_ && common::OB_INVALID_ID != min_log_id_ &&
           common::OB_INVALID_TIMESTAMP != min_log_timestamp_;
  }

  bool operator<=(const Log2File& that) const
  {
    bool bool_ret = false;
    if (common::OB_INVALID_ID != min_log_id_ && common::OB_INVALID_ID != that.min_log_id_) {
      bool_ret = (min_log_id_ <= that.min_log_id_);
    } else if (common::OB_INVALID_TIMESTAMP != min_log_timestamp_ &&
               common::OB_INVALID_TIMESTAMP != that.min_log_timestamp_) {
      bool_ret = (min_log_timestamp_ <= that.min_log_timestamp_);
    } else {
      CSR_LOG(ERROR, "Log2File item is invalid, cannot compare", KPC(this), K(that));
    }
    return bool_ret;
  }

  bool contain_log_id(const uint64_t log_id) const
  {
    bool is_contained = false;
    if (OB_UNLIKELY(!is_valid()) || OB_UNLIKELY(common::OB_INVALID_ID == log_id)) {
      CSR_LOG(ERROR,
          "Log2File is invalid or log_id is invalid",
          K(log_id),
          K(file_id_),
          K(min_log_id_),
          K(min_log_timestamp_));
      is_contained = false;
    } else {
      is_contained = (min_log_id_ <= log_id);

      if (is_contained && common::OB_INVALID_ID != get_max_log_id()) {
        is_contained = (log_id <= get_max_log_id());
      }
    }
    return is_contained;
  }

  bool contain_timestamp(const int64_t timestamp) const
  {
    bool is_contained = false;
    if (OB_UNLIKELY(!is_valid()) || OB_UNLIKELY(common::OB_INVALID_TIMESTAMP == timestamp)) {
      CSR_LOG(ERROR,
          "Log2File is invalid or timestamp is invalid",
          K(timestamp),
          K(file_id_),
          K(min_log_id_),
          K(min_log_timestamp_));
      is_contained = false;
    } else {
      is_contained = (min_log_timestamp_ <= timestamp);

      if (is_contained && common::OB_INVALID_TIMESTAMP != get_max_log_timestamp()) {
        is_contained = (timestamp <= get_max_log_timestamp());
      }
    }
    return is_contained;
  }

  void get_max_log_info(uint64_t& log_id, int64_t& timestamp) const
  {
    log_id = get_max_log_id();
    timestamp = get_max_log_timestamp();
  }

  void get_min_log_info(uint64_t& log_id, int64_t& timestamp) const
  {
    log_id = min_log_id_;
    timestamp = min_log_timestamp_;
  }

  // Determine whether target_item is the next consecutive item
  bool is_preceding_to(const Log2File& target_item) const
  {
    bool bool_ret = false;
    if (common::OB_INVALID_ID != get_max_log_id() && common::OB_INVALID_ID != target_item.min_log_id_) {
      bool_ret = ((get_max_log_id() + 1) == target_item.min_log_id_);
    }
    return bool_ret;
  }

  TO_STRING_KV(K_(file_id), K_(start_offset), K_(min_log_id), "max_log_id", get_max_log_id(), K_(min_log_timestamp),
      "max_log_timestamp", get_max_log_timestamp());

private:
  file_id_t file_id_;
  offset_t start_offset_;
  uint64_t min_log_id_;
  uint64_t max_log_id_;
  int64_t min_log_timestamp_;
  int64_t max_log_timestamp_;
};

struct ObLogBasePos {
public:
  ObLogBasePos()
  {
    reset();
  }
  ~ObLogBasePos()
  {
    reset();
  }
  void reset()
  {
    file_id_ = common::OB_INVALID_FILE_ID;
    base_offset_ = OB_INVALID_OFFSET;
  }
  TO_STRING_KV(K(file_id_), K(base_offset_));

public:
  file_id_t file_id_;
  offset_t base_offset_;
};

class ObFileIdCache;

class ObIFileIdCachePurgeStrategy {
public:
  ObIFileIdCachePurgeStrategy()
  {}
  virtual ~ObIFileIdCachePurgeStrategy()
  {}
  virtual bool can_purge(const Log2File& log_2_file, const common::ObPartitionKey& pkey) const = 0;
  virtual bool is_valid() const = 0;
  virtual bool need_wait(bool& need_print_error) const = 0;
  DECLARE_VIRTUAL_TO_STRING = 0;
};

class ObFileIdCachePurgeByFileId : public ObIFileIdCachePurgeStrategy {
public:
  explicit ObFileIdCachePurgeByFileId(const file_id_t min_file_id, ObFileIdCache& file_id_cache);
  ~ObFileIdCachePurgeByFileId();
  bool can_purge(const Log2File& log_2_file, const common::ObPartitionKey& unused_pkey) const final;
  bool is_valid() const final;
  bool need_wait(bool& need_print_error) const final;
  TO_STRING_KV(K(min_file_id_));

private:
  file_id_t min_file_id_;
  ObFileIdCache& file_id_cache_;
};

class ObFileIdCachePurgeByTimestamp : public ObIFileIdCachePurgeStrategy {
public:
  explicit ObFileIdCachePurgeByTimestamp(const int64_t max_decided_trans_version, const int64_t min_timestamp,
      ObIlogAccessor& ilog_accessor, ObFileIdCache& file_id_cache);
  ~ObFileIdCachePurgeByTimestamp();
  bool can_purge(const Log2File& log_2_file, const common::ObPartitionKey& pkey) const final;
  bool is_valid() const final;
  bool need_wait(bool& need_print_error) const final;
  TO_STRING_KV(K(max_decided_trans_version_), K(min_timestamp_));

private:
  int64_t max_decided_trans_version_;
  int64_t min_timestamp_;
  ObIlogAccessor& ilog_accessor_;
  ObFileIdCache& file_id_cache_;
};

class ObFileIdList {
public:
  class BackFillFunctor {
  public:
    BackFillFunctor() : err_(common::OB_SUCCESS), file_id_(common::OB_INVALID_FILE_ID), start_offset_(OB_INVALID_OFFSET)
    {}
    int init(const file_id_t file_id, const offset_t start_offset);
    void operator()(Log2File& item);
    int get_err() const
    {
      return err_;
    }
    TO_STRING_KV(K(err_), K(file_id_), K(start_offset_));

  private:
    int err_;
    file_id_t file_id_;
    offset_t start_offset_;
  };

  class IPurgeChecker {
  public:
    virtual bool should_purge(const Log2File& log_2_file) const = 0;
    virtual bool is_valid() const = 0;
    DECLARE_VIRTUAL_TO_STRING = 0;
  };
  // purge min
  // should_purge return true if min_log_id > top_item.file_id_
  class PurgeChecker : public IPurgeChecker {
  public:
    explicit PurgeChecker(const common::ObPartitionKey& pkey, ObIFileIdCachePurgeStrategy& purge_strategy)
        : partition_key_(pkey), purge_strategy_(purge_strategy)
    {}
    bool is_valid() const
    {
      return purge_strategy_.is_valid();
    }
    bool should_purge(const Log2File& log_2_file) const;
    TO_STRING_KV(K(purge_strategy_), K(partition_key_));

  private:
    common::ObPartitionKey partition_key_;
    ObIFileIdCachePurgeStrategy& purge_strategy_;
  };
  // Keep ObFileIdList::append atomic, clear broken file id items
  // should_purge return true if top_item.file_id_ == broken_file_id_
  // Because loading an InfoBlock involves multiple partitions, if only load a part of them,
  // then all of this load must be cleaned up
  class ClearBrokenFunctor : public IPurgeChecker {
  public:
    explicit ClearBrokenFunctor(const file_id_t file_id) : broken_file_id_(file_id)
    {}
    bool is_valid() const
    {
      return common::OB_INVALID_FILE_ID != broken_file_id_;
    }
    bool should_purge(const Log2File& log_2_file) const;
    TO_STRING_KV(K(broken_file_id_));

  private:
    file_id_t broken_file_id_;
  };

public:
  ObFileIdList();
  ~ObFileIdList()
  {
    destroy();
  }

public:
  int init(common::ObSmallAllocator* seg_array_allocator, common::ObSmallAllocator* seg_item_allocator,
      common::ObSmallAllocator* log2file_list_allocator, common::ObSmallAllocator* list_item_allocator,
      ObFileIdCache* file_id_cache);
  void destroy();

  int locate(const common::ObPartitionKey& pkey, const int64_t target_value, const bool locate_by_log_id,
      Log2File& prev_item, Log2File& next_item);
  int append(const ObPartitionKey& pkey, const file_id_t file_id, const offset_t start_offset,
      const uint64_t min_log_id, const uint64_t max_log_id, const int64_t min_log_timestamp,
      const int64_t max_log_timestamp);
  int undo_append(const file_id_t broken_file_id, bool& empty);
  int backfill(const uint64_t min_log_id, const file_id_t file_id, const offset_t start_offset);
  int purge(const common::ObPartitionKey& key, ObIFileIdCachePurgeStrategy& purge_strategy, bool& empty);
  int purge_preceding_items(const common::ObPartitionKey& pkey);
  int get_log_base_pos(ObLogBasePos& base_pos) const;
  uint64_t get_min_continuous_log_id() const
  {
    return min_continuous_log_id_;
  }
  int get_max_continuous_log_id(const common::ObPartitionKey& pkey, uint64_t& max_continuous_log_id);
  int get_front_log2file_max_timestamp(int64_t& front_log2file_max_timestamp) const;
  TO_STRING_KV(K(is_inited_), K(min_continuous_log_id_), K(base_pos_));
  static const int64_t SEG_STEP = 256;
  static const int64_t SEG_COUNT = 500;
  static const int64_t NEED_USE_SEG_ARRAY_THRESHOLD = 50;

private:
  int purge_(const bool is_front_end, IPurgeChecker& checker, bool& empty);
  int purge_preceding_items_(const common::ObPartitionKey& pkey, const Log2File& last_item);
  // The caller guarantees that the function will not be executed concurrently
  int prepare_container_();
  int move_item_to_seg_array_(common::ObISegArray<Log2File>* tmp_container_ptr) const;

private:
  bool is_inited_;
  bool use_seg_array_;
  uint64_t min_continuous_log_id_;
  ObFileIdCache* file_id_cache_;
  ObLogBasePos base_pos_;

  common::ObISegArray<Log2File>* container_ptr_;
  common::ObSmallAllocator* seg_array_allocator_;
  common::ObSmallAllocator* seg_item_allocator_;
  common::ObSmallAllocator* log2file_list_allocator_;
  common::ObSmallAllocator* list_item_allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFileIdList);
};

class ObFileIdCache {
public:
  ObFileIdCache();
  ~ObFileIdCache();

public:
  int init(const int64_t server_seq, const common::ObAddr& addr, ObIlogAccessor* ilog_accessor);
  void destroy();

  int locate(const common::ObPartitionKey& pkey, const int64_t target_value, const bool locate_by_log_id,
      Log2File& prev_item, Log2File& next_item);
  int append(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map);
  int backfill(const common::ObPartitionKey& pkey, const uint64_t min_log_id, const file_id_t file_id,
      const offset_t start_offset);
  int purge(ObIFileIdCachePurgeStrategy& purge_strategy);
  int ensure_log_continuous(const common::ObPartitionKey& pkey, const uint64_t log_id);
  int add_partition_needed(const common::ObPartitionKey& pkey, const uint64_t last_replay_log_id);
  file_id_t get_curr_max_file_id() const
  {
    return ATOMIC_LOAD(&curr_max_file_id_);
  }
  int64_t get_next_can_purge_log2file_timestamp() const
  {
    return ATOMIC_LOAD(&next_can_purge_log2file_timestamp_);
  }
  int get_clog_base_pos(const common::ObPartitionKey& pkey, file_id_t& file_id, offset_t& offset) const;
  // Attention: this interface doesn't consider the format of version which before 2.1
  int get_cursor_from_file(
      const common::ObPartitionKey& pkey, const uint64_t log_id, const Log2File& item, ObLogCursorExt& log_cursor);

private:
  class AppendInfoFunctor {
  public:
    int init(const file_id_t file_id, ObFileIdCache* cache);
    bool operator()(const common::ObPartitionKey& pkey, const IndexInfoBlockEntry& entry);
    int get_err() const
    {
      return err_;
    }
    TO_STRING_KV(K(file_id_), K(err_));

  private:
    int err_;
    file_id_t file_id_;
    ObFileIdCache* cache_;
  };
  // Ensure that the loading process is atomic
  class ObUndoAppendFunctor {
  public:
    explicit ObUndoAppendFunctor(const file_id_t broken_file_id) : broken_file_id_(broken_file_id)
    {}
    bool operator()(const common::ObPartitionKey& pkey, ObFileIdList* list);
    const common::ObPartitionArray& get_dead_pkeys() const
    {
      return dead_pkeys_;
    }
    TO_STRING_KV(K(broken_file_id_), K(dead_pkeys_));

  private:
    file_id_t broken_file_id_;
    common::ObPartitionArray dead_pkeys_;
  };
  class ObPurgeFunctor {
  public:
    explicit ObPurgeFunctor(ObIFileIdCachePurgeStrategy& purge_strategy)
        : purge_strategy_(purge_strategy), next_can_purge_log2file_timestamp_(common::OB_INVALID_TIMESTAMP)
    {}
    ~ObPurgeFunctor()
    {
      next_can_purge_log2file_timestamp_ = common::OB_INVALID_TIMESTAMP;
    }
    bool operator()(const common::ObPartitionKey& pkey, ObFileIdList* list);
    const common::ObPartitionArray& get_dead_pkeys() const
    {
      return dead_pkeys_;
    }
    int64_t get_next_can_purge_log2file_timestamp() const
    {
      return next_can_purge_log2file_timestamp_;
    }

  private:
    ObIFileIdCachePurgeStrategy& purge_strategy_;
    common::ObPartitionArray dead_pkeys_;
    int64_t next_can_purge_log2file_timestamp_;
  };
  class LogContinuousFunctor {
  public:
    explicit LogContinuousFunctor(const uint64_t log_id, const uint64_t memstore_min_log_id)
        : log_id_(log_id), memstore_min_log_id_(memstore_min_log_id), is_continuous_(false), next_item_()
    {}
    int operator()(const Log2File& log2file_item);
    bool is_continuous() const
    {
      return is_continuous_;
    }

  private:
    uint64_t log_id_;
    uint64_t memstore_min_log_id_;
    bool is_continuous_;
    Log2File next_item_;
  };
  class DestroyListFunctor {
  public:
    explicit DestroyListFunctor(common::ObSmallAllocator& list_allocator) : list_allocator_(list_allocator)
    {}
    int operator()(const common::ObPartitionKey& pkey, const ObFileIdList* list)
    {
      int ret = common::OB_SUCCESS;
      UNUSED(pkey);
      if (OB_ISNULL(list)) {
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        ObFileIdList* ls = const_cast<ObFileIdList*>(list);
        ls->~ObFileIdList();
        list_allocator_.free(ls);
        ls = NULL;
      }
      return common::OB_SUCCESS == ret;
    }

  private:
    common::ObSmallAllocator& list_allocator_;
  };

private:
  int append_(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map);
  int append_(const common::ObPartitionKey& pkey, const file_id_t file_id, const offset_t start_offset,
      const uint64_t min_log_id, const uint64_t max_log_id, const int64_t min_log_timestamp,
      const int64_t max_log_timestamp);
  int append_new_list_(const common::ObPartitionKey& pkey, const file_id_t file_id, const offset_t start_offset,
      const uint64_t min_log_id, const uint64_t max_log_id, const int64_t min_log_timestamp,
      const int64_t max_log_timestamp);
  int do_append_(const common::ObPartitionKey& pkey, const file_id_t file_id, const offset_t start_offset,
      const uint64_t min_log_id, const uint64_t max_log_id, const int64_t min_log_timestamp,
      const int64_t max_log_timestamp);
  int undo_append_(const file_id_t broken_file_id);
  int check_need_filter_partition_(const common::ObPartitionKey& pkey, const uint64_t max_log_id, bool& need_filter);

private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;

  RWLock rwlock_;
  bool is_inited_;
  bool need_filter_partition_;
  file_id_t curr_max_file_id_;
  int64_t next_can_purge_log2file_timestamp_;
  int64_t server_seq_;
  ObIlogAccessor* ilog_accessor_;
  common::ObAddr addr_;
  common::ObSmallAllocator list_allocator_;           // allocator for per-pkey file_id_list
  common::ObSmallAllocator seg_array_allocator_;      // allocator for SegArray
  common::ObSmallAllocator seg_item_allocator_;       // allocator for Log2File items(seg)
  common::ObSmallAllocator log2file_list_allocator_;  // allocator for Log2FileList
  common::ObSmallAllocator list_item_allocator_;      // allocator for Log2File items(list)
  common::ObLinearHashMap<common::ObPartitionKey, ObFileIdList*> map_;
  common::ObLinearHashMap<common::ObPartitionKey, uint64_t> filter_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFileIdCache);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_FILE_ID_CACHE_H_
