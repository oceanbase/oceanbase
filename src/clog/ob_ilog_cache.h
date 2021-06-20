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

#ifndef OCEANBASE_CLOG_ILOG_CACHE_
#define OCEANBASE_CLOG_ILOG_CACHE_

#include "lib/allocator/ob_qsync.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"
#include "lib/container/ob_se_array.h"
#include "ob_ilog_per_file_cache.h"

namespace oceanbase {
namespace clog {

/*
 * The ObIlogCache is used to manager the content of ilog file,
 * essentially, it's an in-memory cache.
 *
 * The main data struct of ObIlogCache is a map, which the key
 * is file_id_t(fild_id_t is a discriptor of one ilog file),
 * and the value is ObIlogPerFileCacheNode.
 *
 * Concurrency control:
 * The load operation involves reading disk, therefore it's a time-consuming operation.
 * There are three status of ObIlogPerFileCacheNode, NODE_STATUS_INVALID, NODE_STATUS_LOADING, NODE_STATUS_READY.
 * Firstly, allocating a node, label it as NODE_STATUS_LOADING, and insert it into map.
 * Secondly, reading ilog file from disk.
 * Lastly, label it as NODE_STATUS_READY.
 *
 * Analysis of several concurrency problems
 * 1. After the insertion of map, before finishing the load operation, meanwhile,
 *    it's need to backfill the status of ObIlogPerFileCacheNode into ready,
 *    the node may be not exist, in this case, we destroy the object which has
 *    been loaded.
 * 2. Same as above, wash thread delete the placeholder node, there is may be a new placehodler node,
 *    at this time, the above loading has finished. To identify two different nodes, a monotonically
 *    increasing sequence number is used to mark them
 * 3. To prevent the node which being accessed to be destoryed, we use reference count to protect
 *    node, the initial reference count of each node which has inserted into hash_map is 1.
 */

class ObILogEngine;

// --- ilog_per_file_cache wrapper
enum NodeStatus {
  NODE_STATUS_INVALID = 0,  // invalid
  NODE_STATUS_LOADING = 1,  // this ilog file is loading
  NODE_STATUS_READY = 2,    // ready to be queried
};

// wrap ObIlogPerFileCache and its allocator
struct ObIlogPerFileCacheWrapper {
  ObIlogPerFileCache cache_;  // cache_ is inited after file is loaded
  common::PageArena<> page_arena_;
  int64_t last_access_ts_;  // use more powerful statistics TODO
  int64_t ref_cnt_;         // reference count of the cache

  ObIlogPerFileCacheWrapper()
      : cache_(), page_arena_(), last_access_ts_(common::ObTimeUtility::current_time()), ref_cnt_(1)
  {}
  inline void set_mod_and_tenant(const char* label, const uint64_t tenant_id)
  {
    page_arena_.set_label(label);
    page_arena_.set_tenant_id(tenant_id);
  }

  // Preloaded files, should give this file more time, avoid ealry wash
  void be_kind_to_preload()
  {
    const int64_t PRELOAD_KIND_INTERVAL = 90 * 1000 * 1000;  // 90 second
    last_access_ts_ += PRELOAD_KIND_INTERVAL;
  }
  void inc_ref()
  {
    ATOMIC_INC(&ref_cnt_);
  }
  int64_t dec_ref()
  {
    return ATOMIC_AAF(&ref_cnt_, -1);
  }
};

// value in map
class ObIlogPerFileCacheNode {
public:
  ObIlogPerFileCacheNode();
  ~ObIlogPerFileCacheNode();
  bool is_valid() const
  {
    return NODE_STATUS_INVALID != ATOMIC_LOAD(&node_status_) && NULL != cache_wrapper_;
  }
  void reset();
  NodeStatus get_status() const
  {
    return ATOMIC_LOAD(&node_status_);
  }
  ObIlogPerFileCacheWrapper* get_cache_wrapper() const
  {
    return cache_wrapper_;
  }
  common::ObSmallAllocator* get_wrapper_allocator() const
  {
    return wrapper_allocator_;
  }
  int64_t get_seq() const
  {
    return seq_;
  }
  int set_cache_wrapper(ObIlogPerFileCacheWrapper* cache_wrapper, common::ObSmallAllocator* wrapper_allocator);

  // switch status, specific api for later complicated switch maybe
  inline int mark_loading(const file_id_t file_id);
  inline void mark_invalid(const file_id_t file_id);
  inline int mark_ready(const file_id_t file_id);
  inline bool is_ready() const
  {
    return (ATOMIC_LOAD(&node_status_) == NODE_STATUS_READY);
  }

  ObIlogPerFileCacheNode& operator=(const ObIlogPerFileCacheNode& node);
  void inc_wrapper_ref()
  {
    if (NULL != cache_wrapper_) {
      cache_wrapper_->inc_ref();
    }
  }
  void dec_wrapper_ref()
  {
    if (NULL != cache_wrapper_ && 0 == cache_wrapper_->dec_ref()) {
      destroy_wrapper();
    }
  }
  TO_STRING_KV("node_status", ATOMIC_LOAD(&node_status_), KP_(cache_wrapper), KP(wrapper_allocator_), K_(seq));

private:
  void destroy_wrapper();
  static int64_t alloc_seq()
  {
    static int64_t s_seq = 0;
    return ATOMIC_AAF(&s_seq, 1);
  }

private:
  NodeStatus node_status_;
  int64_t seq_;
  ObIlogPerFileCacheWrapper* cache_wrapper_;
  common::ObSmallAllocator* wrapper_allocator_;
};

struct ObIlogCacheConfig {
  int64_t hold_file_count_limit_;
  // TODO other config

  ObIlogCacheConfig() : hold_file_count_limit_(0)
  {}
  bool is_valid() const
  {
    return hold_file_count_limit_ > 0;
  }
  TO_STRING_KV(K(hold_file_count_limit_));
};

struct ObIlogCacheStatistic {
  int64_t hold_file_count_;  // refine: loading, deleting etc.

  ObIlogCacheStatistic() : hold_file_count_(0)
  {}
  int64_t inc_hold_file_count()
  {
    return ATOMIC_AAF(&hold_file_count_, 1);
  }
  int64_t dec_hold_file_count()
  {
    return ATOMIC_AAF(&hold_file_count_, -1);
  }
  int64_t get_hold_file_count() const
  {
    return ATOMIC_LOAD(&hold_file_count_);
  }
  void reset()
  {
    ATOMIC_STORE(&hold_file_count_, 0);
  }
  TO_STRING_KV(K(hold_file_count_));
};

typedef common::ObSEArray<file_id_t, 16> FileIdArray;

struct NodeItem {
  file_id_t file_id_;
  ObIlogPerFileCacheNode node_obj_;

  TO_STRING_KV(K(file_id_), K(node_obj_));
};
typedef common::ObSEArray<NodeItem, 128> NodeItemArray;

class ObIlogCache {
private:
  class NodeReadyMarker {
  public:
    explicit NodeReadyMarker(const int64_t seq) : err_(common::OB_SUCCESS), owner_seq_(seq), owner_match_(true)
    {}
    bool operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node);
    int get_err()
    {
      return err_;
    }
    const ObIlogPerFileCacheNode& get_marked_node() const
    {
      return node_;
    }
    TO_STRING_KV(K(err_), K(owner_seq_), K(owner_match_));

  private:
    int err_;
    int64_t owner_seq_;
    bool owner_match_;             // for debug
    ObIlogPerFileCacheNode node_;  // the node that is marked
  };

  class GetNodeFunctor {
  public:
    GetNodeFunctor() : err_(common::OB_SUCCESS)
    {}
    bool operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node);
    int get_err()
    {
      return err_;
    }
    const ObIlogPerFileCacheNode& get_node() const
    {
      return node_;
    }
    TO_STRING_KV(K(err_));

  private:
    int err_;
    ObIlogPerFileCacheNode node_;
  };

  class EraseNodeFunctor {
  public:
    EraseNodeFunctor() : err_(common::OB_SUCCESS)
    {}
    bool operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node);
    int get_err()
    {
      return err_;
    }
    TO_STRING_KV(K(err_));

  private:
    int err_;
  };

  class VictimPicker {
  public:
    VictimPicker()
        : err_(common::OB_SUCCESS),
          count_(0),
          victim_(common::OB_INVALID_FILE_ID),
          min_access_ts_(LARGE_ENOUGH_TIMESTAMP)
    {}
    bool operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node);
    inline file_id_t get_victim_file_id() const
    {
      return victim_;
    }
    inline int get_err() const
    {
      return err_;
    }
    inline int get_count() const
    {
      return count_;
    }
    inline int64_t get_min_access_ts() const
    {
      return min_access_ts_;
    }
    TO_STRING_KV(K(err_), K(count_), K(victim_), K(min_access_ts_));

  private:
    static const int64_t LARGE_ENOUGH_TIMESTAMP = 5000000000000000;  // 2128-06-11 16:53:20
  private:
    int err_;
    int count_;
    file_id_t victim_;
    int64_t min_access_ts_;
  };

  class ExpiredNodesPicker {
  public:
    explicit ExpiredNodesPicker(FileIdArray& expired_file_id_arr)
        : err_(common::OB_SUCCESS), expired_file_id_arr_(expired_file_id_arr)
    {}
    bool operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node);
    inline int get_err() const
    {
      return err_;
    }
    const FileIdArray& get_expired_file_id_arr() const
    {
      return expired_file_id_arr_;
    }
    TO_STRING_KV(K(err_), K(expired_file_id_arr_));

  private:
    static const int64_t EXPIRED_INVTERVAL = 600L * 1000L * 1000L;  // 10 min
  private:
    int err_;
    FileIdArray& expired_file_id_arr_;
  };

  class AllFileIdGetter {
  public:
    explicit AllFileIdGetter(FileIdArray& all_file_id_arr) : err_(common::OB_SUCCESS), all_file_id_arr_(all_file_id_arr)
    {}
    bool operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node);
    inline int get_err() const
    {
      return err_;
    }
    TO_STRING_KV(K(err_), K(all_file_id_arr_));

  private:
    int err_;
    FileIdArray& all_file_id_arr_;
  };

  class RemoveNodeFunctor {
  public:
    RemoveNodeFunctor() : count_(0)
    {}
    ~RemoveNodeFunctor()
    {
      count_ = 0;
    }
    bool operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node);

  private:
    int count_;
  };

  // ObLinearHashMap need KeyType to be a struct/class, so wrap it
  struct FileIdType {
    file_id_t file_id_;

    // intentionally no explicit
    FileIdType(file_id_t file_id) : file_id_(file_id)
    {}
    operator file_id_t() const
    {
      return file_id_;
    }
    bool operator==(const FileIdType that) const
    {
      return file_id_ == that.file_id_;
    }
    uint64_t hash() const
    {
      return common::murmurhash(&file_id_, sizeof(file_id_), 0);
    }
  };

  class NodeReporter {
  public:
    NodeReporter() : arr_()
    {}
    ~NodeReporter()
    {
      arr_.destroy();
    }
    bool operator()(const file_id_t file_id, ObIlogPerFileCacheNode& node_obj)
    {
      NodeItem node_item;
      node_item.file_id_ = file_id;
      node_item.node_obj_ = node_obj;
      return common::OB_SUCCESS == (arr_.push_back(node_item));
    }
    DECLARE_TO_STRING;

  private:
    NodeItemArray arr_;
  };

public:
  ObIlogCache() : is_inited_(false), wrapper_allocator_(), node_map_(), config_(), statistic_(), pf_cache_builder_(NULL)
  {}
  int init(const ObIlogCacheConfig& config, ObIlogPerFileCacheBuilder* pf_cache_builder);
  void destroy();
  int get_cursor(const file_id_t file_id, const common::ObPartitionKey& pkey, const uint64_t query_log_id,
      const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index,
      ObGetCursorResult& result, ObIlogStorageQueryCost& csr_cost);
  int prepare_cache_node(const file_id_t file_id);
  int force_wash();
  int timer_wash();
  int report();
  int admin_wash();
  int admin_wash(const file_id_t file_id);

  int locate_by_timestamp(const file_id_t file_id, const common::ObPartitionKey& pkey, const int64_t start_ts,
      const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index, uint64_t& target_log_id,
      int64_t& target_log_timestamp);

private:
  int do_locate_by_timestamp_(const file_id_t file_id, const common::ObPartitionKey& pkey, const int64_t start_ts,
      const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index, uint64_t& target_log_id,
      int64_t& target_log_timestamp, ObIlogStorageQueryCost& csr_cost);
  int locate_from_node_by_ts_(const file_id_t file_id, ObIlogPerFileCacheNode& target_node,
      const common::ObPartitionKey& pkey, const int64_t start_ts, const uint64_t min_log_id, const uint64_t max_log_id,
      const offset_t start_offset_index, uint64_t& target_log_id, int64_t& target_log_timestamp);
  int prepare_cache_node(const file_id_t file_id, ObIlogPerFileCacheNode& target_node, ObIlogStorageQueryCost& csr_cost,
      const bool is_preload = false);
  int get_cursor_from_node(const ObIlogPerFileCacheNode& target_node, const common::ObPartitionKey& pkey,
      const uint64_t query_log_id, const uint64_t min_log_id, const uint64_t max_log_id,
      const offset_t start_offset_index, ObGetCursorResult& result);
  int query_from_node(const file_id_t file_id, ObIlogPerFileCacheNode& target_node, const common::ObPartitionKey& pkey,
      const uint64_t query_log_id, const uint64_t min_log_id, const uint64_t max_log_id,
      const offset_t start_offset_index, ObGetCursorResult& result);
  int do_get_cursor(const file_id_t file_id, const common::ObPartitionKey& pkey, const uint64_t query_log_id,
      const uint64_t min_log_id, const uint64_t max_log_id, const offset_t start_offset_index,
      ObGetCursorResult& result, ObIlogStorageQueryCost& csr_cost);
  int prepare_loading(const file_id_t file_id, const bool is_preload, ObIlogPerFileCacheNode& node);
  int sync_load_ilog_file(
      const file_id_t file_id, ObIlogPerFileCacheNode& target_node, ObIlogStorageQueryCost& csr_cost);
  int get_victim_file_id(file_id_t& victim_file_id, int64_t& min_access_ts);
  int wash_victim(const file_id_t victim_file_id);
  int get_expired_arr(FileIdArray& file_id_arr);
  int get_all_file_id_arr(FileIdArray& all_file_id_arr);

private:
  static const file_id_t PRELOAD_AHEAD = 5;

private:
  bool is_inited_;
  common::ObSmallAllocator wrapper_allocator_;
  common::ObLinearHashMap<FileIdType, ObIlogPerFileCacheNode> node_map_;
  ObIlogCacheConfig config_;
  ObIlogCacheStatistic statistic_;
  ObIlogPerFileCacheBuilder* pf_cache_builder_;
};
}  // namespace clog
}  // namespace oceanbase

#endif
