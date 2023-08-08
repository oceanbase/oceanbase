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

#ifndef SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_OP_H_
#define SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_OP_H_

#include "sql/engine/join/ob_join_op.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/join/ob_hash_join_basic.h"
#include "lib/container/ob_bit_set.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "lib/container/ob_2d_array.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "lib/lock/ob_scond.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"

namespace oceanbase
{
namespace sql
{

struct ObHashTableSharedTableInfo
{
  int64_t sqc_thread_count_;

  ObSpinLock lock_;
  common::SimpleCond cond_;
  int64_t process_cnt_;
  int64_t close_cnt_;
  int64_t init_val_;
  int64_t sync_val_;

  int ret_;

  bool read_null_in_naaj_;
  bool non_preserved_side_is_not_empty_;

  int64_t total_memory_row_count_;
  int64_t total_memory_size_;
  int64_t open_cnt_;
  int open_ret_;
};

class ObHashJoinInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
private:
  enum SyncValueMode {
    MIN_MODE,
    MAX_MODE,
    FIRST_MODE,
    MAX
  };
  using EventPred = std::function<void(int64_t n_times)>;
public:
  ObHashJoinInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec),
      shared_hj_info_(0),
      task_id_(0)
  {}
  virtual ~ObHashJoinInput() {}
  virtual int init(ObTaskInfo &task_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(task_info);
    return ret;
  }

  int sync_wait(ObExecContext &ctx, int64_t &sys_event, EventPred pred, bool ignore_interrupt = false, bool is_open = false);
  int64_t get_sync_val()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->sync_val_);
  }
  int64_t get_total_memory_row_count()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->total_memory_row_count_);
  }
  int64_t get_total_memory_size()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->total_memory_size_);
  }
  int64_t get_null_in_naaj()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->read_null_in_naaj_);
  }
  int64_t get_non_preserved_side_naaj()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->non_preserved_side_is_not_empty_);
  }
  void sync_basic_info(int64_t n_times, int64_t row_count, int64_t input_size)
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    if (0 == n_times) {
      ATOMIC_SET(&shared_hj_info->total_memory_row_count_, row_count);
      ATOMIC_SET(&shared_hj_info->total_memory_size_, input_size);
    } else {
      ATOMIC_AAF(&shared_hj_info->total_memory_row_count_, row_count);
      ATOMIC_AAF(&shared_hj_info->total_memory_size_, input_size);
    }
    OB_LOG(DEBUG, "set basic info", K(shared_hj_info->total_memory_row_count_),
      K(shared_hj_info->total_memory_size_));
  }
  void sync_info_for_naaj(int64_t n_times, bool null_in_naal, bool non_preserved_side_naaj)
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    if (0 == n_times) {
      ATOMIC_SET(&shared_hj_info->read_null_in_naaj_, null_in_naal);
      ATOMIC_SET(&shared_hj_info->non_preserved_side_is_not_empty_, non_preserved_side_naaj);
    } else {
      if (!ATOMIC_LOAD(&shared_hj_info->read_null_in_naaj_)) {
        ATOMIC_SET(&shared_hj_info->read_null_in_naaj_, null_in_naal);
      }
      if (!ATOMIC_LOAD(&shared_hj_info->non_preserved_side_is_not_empty_)) {
        ATOMIC_SET(&shared_hj_info->non_preserved_side_is_not_empty_, non_preserved_side_naaj);
      }
    }
    OB_LOG(DEBUG, "set basic info", K(shared_hj_info->total_memory_row_count_),
      K(shared_hj_info->total_memory_size_));
  }

  int64_t &get_sqc_thread_count()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return shared_hj_info->sqc_thread_count_;
  }
  int64_t &get_process_cnt()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return shared_hj_info->process_cnt_;
  }
  int64_t &get_close_cnt()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return shared_hj_info->close_cnt_;
  }

  int64_t &get_open_cnt()
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    return shared_hj_info->open_cnt_;
  }

  ObHashTableSharedTableInfo *get_shared_hj_info()
  {
    return reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
  }
  void set_error_code(int in_ret)
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    ATOMIC_SET(&shared_hj_info->ret_, in_ret);
  }

  void set_open_ret(int in_ret)
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    ATOMIC_SET(&shared_hj_info->open_ret_, in_ret);
  }

  virtual void reset() override
  {
    if (0 != shared_hj_info_) {
      ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
      int64_t task_cnt = shared_hj_info->sqc_thread_count_;

      shared_hj_info->total_memory_row_count_ = 0;
      shared_hj_info->total_memory_size_ = 0;
    }
  }
  int init_shared_hj_info(ObIAllocator &alloc, int64_t task_cnt)
  {
    int ret = OB_SUCCESS;
    ObHashTableSharedTableInfo *shared_hj_info = nullptr;
    if (OB_ISNULL(shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(alloc.alloc(sizeof(ObHashTableSharedTableInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      shared_hj_info_ = reinterpret_cast<int64_t>(shared_hj_info);
      shared_hj_info->sqc_thread_count_ = task_cnt;

      shared_hj_info->process_cnt_ = 0;
      shared_hj_info->close_cnt_ = 0;
      shared_hj_info->open_cnt_ = 0;
      shared_hj_info->ret_ = OB_SUCCESS;
      shared_hj_info->open_ret_ = OB_SUCCESS;
      shared_hj_info->read_null_in_naaj_ = false;
      new (&shared_hj_info->cond_)common::SimpleCond(common::ObWaitEventIds::SQL_SHARED_HJ_COND_WAIT);
      new (&shared_hj_info->lock_)ObSpinLock(common::ObLatchIds::SQL_SHARED_HJ_COND_LOCK);
      reset();
    }
    return ret;
  }

  void sync_set_min(int64_t n_times, int64_t val)
  {
    sync_set_val(n_times, val, SyncValueMode::MIN_MODE);
  }
  void sync_set_max(int64_t n_times, int64_t val)
  {
    sync_set_val(n_times, val, SyncValueMode::MAX_MODE);
  }
  void sync_set_first(int64_t n_times, int64_t val)
  {
    sync_set_val(n_times, val, SyncValueMode::FIRST_MODE);
  }
  void sync_set_val(int64_t n_times, int64_t val, SyncValueMode val_mode)
  {
    ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
    if (0 == n_times) {
      // the first
      ATOMIC_SET(&shared_hj_info->init_val_, val);
    } else {
      // others
      if (SyncValueMode::MIN_MODE == val_mode) {
        // set min
        if (ATOMIC_LOAD(&shared_hj_info->init_val_) > val) {
          ATOMIC_SET(&shared_hj_info->init_val_, val);
        }
      } else if (SyncValueMode::MAX_MODE == val_mode) {
        // set max
        if (ATOMIC_LOAD(&shared_hj_info->init_val_) < val) {
          ATOMIC_SET(&shared_hj_info->init_val_, val);
        }
      } else if (SyncValueMode::FIRST_MODE == val_mode) {
      } else {
        OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "the value mode is not supported", K(val_mode));
      }
      if (n_times + 1 >= shared_hj_info->sqc_thread_count_) {
        // last time, set final value
        ATOMIC_SET(&shared_hj_info->sync_val_, shared_hj_info->init_val_);
      }
    }
    OB_LOG(TRACE, "sync cur part_count", K(n_times),
      K(shared_hj_info->init_val_),
      K(shared_hj_info->sync_val_));
  }

  void set_task_id(int64_t task_id) { task_id_ = task_id; }
  int64_t get_task_id() { return task_id_; }
public:
  uint64_t shared_hj_info_;
  int64_t task_id_;
};

class ObHashJoinSpec : public ObJoinSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObHashJoinSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  // all_exprs组成:(all_left_exprs keys, all_right_exprs keys)
  // 前面的all_xxx_exprs keys没有去重
  // 同理hash_funcs也保存了left和right的join key对应hash function
  // 为什么保存left和right分别保存，因为可能存在类型不一致情况，如sint = usint
  ExprFixedArray equal_join_conds_;
  ExprFixedArray all_join_keys_;
  common::ObHashFuncs all_hash_funcs_;
  bool can_prob_opt_;
  //is null aware anti join
  bool is_naaj_;
  //is single null aware anti join
  bool is_sna_;
  bool is_shared_ht_;
  // record which equal cond is null safe equal
  common::ObFixedArray<bool, common::ObIAllocator> is_ns_equal_cond_;
};

// hash join has no expression result overwrite problem:
//  LEFT: is block, do not care the overwrite.
//  RIGHT: overwrite with blank_right_row() in JS_FILL_LEFT state, right child also iterated end.

class ObHashJoinOp : public ObJoinOp
{
public:
  // EN_HASH_JOIN_OPTION switches:
  uint64_t HJ_TP_OPT_ENABLED = 1;
  uint64_t HJ_TP_OPT_ENABLE_CACHE_AWARE = 2;
  uint64_t HJ_TP_OPT_ENABLE_BLOOM_FILTER = 4;

  ObHashJoinOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashJoinOp() {}

  using BucketFunc = std::function<int64_t(int64_t, int64_t)>;
  using NextFunc = std::function<int (const ObHashJoinStoredJoinRow *&right_read_row)>;
private:
  enum HashJoinDrainMode
  {
    NONE_DRAIN,
    BUILD_HT,
    RIGHT_DRAIN,
    MAX_DRAIN
  };
  enum HJState
  {
    INIT,
    NORMAL,
    NEXT_BATCH
  };
  enum HJProcessor
  {
    NONE = 0,
    NEST_LOOP = 1,
    RECURSIVE = 2,
    IN_MEMORY = 4
  };
  enum ObJoinState {
    JS_JOIN_END,
    JS_READ_RIGHT,
    JS_READ_HASH_ROW,
    JS_LEFT_ANTI_SEMI, // for anti_semi
    JS_FILL_LEFT, // for left_outer,full_outer
    JS_STATE_COUNT
  };
  enum ObFuncType {
    FT_ITER_GOING = 0,
    FT_ITER_END,
    FT_TYPE_COUNT
  };
  enum HJLoopState {
    LOOP_START,
    LOOP_GOING,
    LOOP_RECURSIVE,
    LOOP_END
  };
private:

  struct HTBucket
  {
    // keep trivial constructor make ObSegmentArray use memset to construct arrays.
    HTBucket() = default;
    union {
      struct {
        uint64_t hash_value_:63;
        uint64_t used_:1;
      };
      uint64_t val_;
    };

    uint64_t stored_row_;

    ObHashJoinStoredJoinRow *get_stored_row() const
    {
      return reinterpret_cast<ObHashJoinStoredJoinRow *>(stored_row_);
    }

    void set_stored_row(ObHashJoinStoredJoinRow *sr)
    {
      stored_row_ = reinterpret_cast<uint64_t>(sr);
    }

    int64_t get_hash_value() const
    {
      return hash_value_;
    }
    void set_hash_value(uint64_t hash_value)
    {
      hash_value_ = hash_value;
    }

    bool get_used() const
    {
      return used_;
    }
    void set_used(bool used)
    {
      used_ = used;
    }

    TO_STRING_KV(K_(hash_value), K_(stored_row), K_(used));
  };

  // Open addressing hash table implement:
  //
  //   buckets:
  //   .......
  //   +----------------------------+
  //   | 0          | NULL          |
  //   +----------------------------+        +----------+       +----------+
  //   | hash_value | store_row_ptr |------->| store_row|------>| store_row|
  //   +----------------------------+        +----------+       +----------+
  //   | 0          | NULL          |
  //   +----------------------------+
  //   ......
  //
  // Buckets is array of <hash_value, store_row_ptr> pair, store rows linked in one bucket are
  // the same hash value.
  //
  struct PartHashJoinTable
  {
    PartHashJoinTable()
        : buckets_(nullptr),
          nbuckets_(0),
          row_count_(0),
          collisions_(0),
          used_buckets_(0),
          inited_(false),
          ht_alloc_(nullptr),
          magic_(0)
    {
    }

    // Get stored row list which has the same hash value.
    // return NULL if not found.
    inline ObHashJoinStoredJoinRow *get(const uint64_t hash_val)
    {
      HTBucket tmp_bucket;
      tmp_bucket.hash_value_ = hash_val;
      uint64_t mask = nbuckets_ - 1;
      uint64_t pos = tmp_bucket.hash_value_ & mask;
      ObHashJoinStoredJoinRow *sr = NULL;
      HTBucket *bucket = &buckets_->at(pos);
      if (bucket->used_) {
        do {
          if (bucket->hash_value_ == tmp_bucket.hash_value_) {
            sr = bucket->get_stored_row();
            break;
          }
          // next bucket
          ++bucket;
          ++pos;
          if (OB_UNLIKELY(pos == ((pos >> bit_cnt_) << bit_cnt_) || pos == nbuckets_)) {
            pos = (pos & mask);
            bucket = &buckets_->at(pos);
          }
          // hash table must has empty bucket
          // so we don't judge that the count is greater than bucket number
        } while (bucket->used_);
      }
      return sr;
    }

    void get(uint64_t hash_val, HTBucket *&bkt)
    {
      HTBucket tmp_bucket;
      tmp_bucket.hash_value_ = hash_val;
      uint64_t mask = nbuckets_ - 1;
      uint64_t pos = tmp_bucket.hash_value_ & mask;
      bkt = NULL;
      for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
        // FIXME bin.lb: use hash_value_ == 0 as the sentinel?
        HTBucket &bucket = buckets_->at(pos);
        if (!bucket.used_) {
          break;
        }
        if (bucket.hash_value_ == tmp_bucket.hash_value_) {
          bkt = &bucket;
          break;
        }
      }
    }

    // performance critical, do not double check the parameters
    void set(const uint64_t hash_val, ObHashJoinStoredJoinRow *sr)
    {
      HTBucket tmp_bucket;
      tmp_bucket.hash_value_ = hash_val;
      uint64_t mask = nbuckets_ - 1;
      uint64_t pos = tmp_bucket.hash_value_ & mask;
      for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
        HTBucket &bucket = buckets_->at(pos);
        if (bucket.hash_value_ == tmp_bucket.hash_value_) {
          sr->set_next(bucket.get_stored_row());
          bucket.set_stored_row(sr);
          bucket.used_ = true;
          break;
        } else if (!bucket.used_) {
          used_buckets_ += 1;
          bucket.hash_value_ = tmp_bucket.hash_value_;
          bucket.set_stored_row(sr);
          bucket.used_ = true;
          sr->set_next(NULL);
          break;
        }
        collisions_ += 1;
      }
    }

    // lock-free hash table
    inline void atomic_set(const uint64_t hash_val, ObHashJoinStoredJoinRow *sr,
      int64_t used_buckets, int64_t collisions)
    {
      HTBucket new_bucket;
      new_bucket.hash_value_ = hash_val;
      new_bucket.used_ = true;
      new_bucket.set_stored_row(sr);
      uint64_t mask = nbuckets_ - 1;
      uint64_t pos = new_bucket.hash_value_ & mask;
      bool added = false;
      HTBucket old_bucket;
      uint64_t old_val;
      uint64_t old_store_row;
      for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
        HTBucket &bucket = buckets_->at(pos);
        do {
          old_val = ATOMIC_LOAD(&bucket.val_);
          old_bucket.val_ = old_val;
          if (!old_bucket.used_) {
            if (ATOMIC_BCAS(&bucket.val_, old_val, new_bucket.val_)) {
              // write hash_value and used_ flag successfully
              // then write sr
              ++used_buckets;
              old_store_row = ATOMIC_LOAD(&bucket.stored_row_);
              sr->set_next(reinterpret_cast<ObHashJoinStoredJoinRow *>(old_store_row));
              if (ATOMIC_BCAS(&bucket.stored_row_, old_store_row, new_bucket.stored_row_)) {
                added = true;
              }
            }
          } else if (old_val == new_bucket.val_) {
            old_store_row = ATOMIC_LOAD(&bucket.stored_row_);
            sr->set_next(reinterpret_cast<ObHashJoinStoredJoinRow *>(old_store_row));
            if (ATOMIC_BCAS(&bucket.stored_row_, old_store_row, new_bucket.stored_row_)) {
              added = true;
            }
          } else {
            break;
          }
        } while (!added);
        if (added) {
          break;
        }
        ++collisions;
      }
    }
    // mark delete, can not add row again after delete
    void del(const uint64_t hash_val, ObHashJoinStoredJoinRow *sr)
    {
      HTBucket tmp_bucket;
      tmp_bucket.hash_value_ = hash_val;
      uint64_t mask = nbuckets_ - 1;
      uint64_t pos = tmp_bucket.hash_value_ & mask;
      for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
        HTBucket &bucket = buckets_->at(pos);
        if (!bucket.used_) {
          break;
        }
        if (bucket.hash_value_ == tmp_bucket.hash_value_) {
          if (NULL != bucket.get_stored_row()) {
            if (sr == bucket.get_stored_row()) {
              bucket.set_stored_row(sr->get_next());
              --row_count_;
            } else {
              auto head = bucket.get_stored_row();
              auto s = bucket.get_stored_row()->get_next();
              while (NULL != s) {
                if (s == sr) {
                  head->set_next(s->get_next());
                  --row_count_;
                  break;
                }
                head = s;
                s = s->get_next();
              }
            }
          }
          break;
        }
      }
    }

    void reset()
    {
      if (OB_NOT_NULL(buckets_)) {
        buckets_->reset();
      }
      nbuckets_ = 0;
      collisions_ = 0;
      used_buckets_ = 0;
    }
    int init(ObIAllocator &alloc);
    void free(ObIAllocator *alloc)
    {
      reset();
      if (OB_NOT_NULL(buckets_)) {
        buckets_->destroy();
        alloc->free(buckets_);
        buckets_ = nullptr;
      }
      if (OB_NOT_NULL(ht_alloc_)) {
        ht_alloc_->reset();
        ht_alloc_->~ModulePageAllocator();
        alloc->free(ht_alloc_);
        ht_alloc_ = nullptr;
      }
      inited_ = false;
    }
    using BucketArray =
      common::ObSegmentArray<HTBucket, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>;

    static const int64_t MAGIC_CODE = 0x123654abcd134;
    BucketArray *buckets_;
    int64_t nbuckets_;
    int64_t bit_cnt_;
    int64_t row_count_;
    int64_t collisions_;
    int64_t used_buckets_;
    bool inited_;
    ModulePageAllocator *ht_alloc_;
    int64_t magic_;
  };

  struct HistItem
  {
    int64_t hash_value_;
    ObHashJoinStoredJoinRow *store_row_;
    TO_STRING_KV(K_(hash_value), K(static_cast<void*>(store_row_)));
  };
  struct ResultItem
  {
    HistItem left_;
    HistItem right_;
    bool is_match_;
  };
  class HashJoinHistogram
  {
  public:
    HashJoinHistogram()
      : h1_(nullptr), h2_(nullptr), prefix_hist_count_(nullptr), prefix_hist_count2_(nullptr),
        hist_alloc_(nullptr), enable_bloom_filter_(false), bloom_filter_(nullptr), alloc_(nullptr),
        row_count_(0), bucket_cnt_(0)
      {}
    ~HashJoinHistogram()
    {
      reset();
    }
    void reset()
    {
      if (OB_NOT_NULL(alloc_)) {
        if (OB_NOT_NULL(h1_)) {
          h1_->reset();
          alloc_->free(h1_);
          h1_ = nullptr;
        }
        if (OB_NOT_NULL(h2_)) {
          h2_->reset();
          alloc_->free(h2_);
          h2_ = nullptr;
        }
        if (OB_NOT_NULL(prefix_hist_count_)) {
          prefix_hist_count_->reset();
          alloc_->free(prefix_hist_count_);
          prefix_hist_count_ = nullptr;
        }
        if (OB_NOT_NULL(prefix_hist_count2_)) {
          prefix_hist_count2_->reset();
          alloc_->free(prefix_hist_count2_);
          prefix_hist_count2_ = nullptr;
        }
        if (OB_NOT_NULL(bloom_filter_)) {
          bloom_filter_->~ObGbyBloomFilter();
          alloc_->free(bloom_filter_);
          bloom_filter_ = nullptr;
        }
        if (OB_NOT_NULL(hist_alloc_)) {
          hist_alloc_->reset();
          hist_alloc_->~ModulePageAllocator();
          alloc_->free(hist_alloc_);
          hist_alloc_ = nullptr;
        }
      }
      alloc_ = nullptr;
      row_count_ = 0;
      bucket_cnt_ = 0;
      enable_bloom_filter_ = false;
    }

    OB_INLINE int64_t get_bucket_idx(const uint64_t hash_value)
    { return hash_value & (bucket_cnt_ - 1); }
    int init(ObIAllocator *alloc, int64_t row_count, int64_t bucket_cnt, bool enable_bloom_filter);
    static int64_t calc_memory_size(int64_t row_count)
    {
      return next_pow2(row_count * RATIO_OF_BUCKETS) * (sizeof(HistItem) * 2 + sizeof(int64_t));
    }
    bool empty() const { return 0 == row_count_; }
    int reorder_histogram(BucketFunc bucket_func);
    int calc_prefix_histogram();
    void switch_histogram();
    void switch_prefix_hist_count();
  public:
    using HistItemArray =
      common::ObSegmentArray<HistItem, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>;
    using HistPrefixArray =
      common::ObSegmentArray<int64_t, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>;
    HistItemArray *h1_;
    HistItemArray *h2_;
    HistPrefixArray *prefix_hist_count_;
    HistPrefixArray *prefix_hist_count2_;
    ModulePageAllocator *hist_alloc_;
    bool enable_bloom_filter_;
    ObGbyBloomFilter *bloom_filter_;
    ObIAllocator *alloc_;
    int64_t row_count_;
    int64_t bucket_cnt_;
  };
  class PartitionSplitter
  {
  public:
    PartitionSplitter()
      : alloc_(nullptr), part_count_(0), hj_parts_(nullptr),
        max_level_(0), part_shift_(0), level1_bit_(0), level2_bit_(0),
        level_one_part_count_(0), level_two_part_count_(0),
        part_histogram_(), total_row_count_(0) {}
    ~PartitionSplitter()
    {
      reset();
    }
    void reset()
    {
      part_count_ = 0;
      hj_parts_ = nullptr;
      max_level_ = 0;
      part_shift_ = 0;
      level1_bit_ = 0;
      level2_bit_ = 0;
      level_one_part_count_ = 0;
      level_two_part_count_ = 0;
      part_histogram_.reset();
      total_row_count_ = 0;
      alloc_ = nullptr;
    }

    void set_part_count(int64_t part_shift, int64_t level1_part_count, int64_t level2_part_count)
    {
      OB_ASSERT(0 != level1_part_count);
      part_shift_ = part_shift;
      level_one_part_count_ = level1_part_count;
      level_two_part_count_ = level2_part_count;
      level1_bit_ = __builtin_ctz(level1_part_count);
      level2_bit_ = (0 != level2_part_count) ? __builtin_ctz(level2_part_count) : 0;
    }
    bool is_valid() { return nullptr != hj_parts_; }
    int init(ObIAllocator *alloc,
            int64_t part_count,
            ObHashJoinPartition *hj_parts,
            int64_t max_level,
            int64_t part_shift,
            int64_t level1_part_count,
            int64_t level2_part_count);
    int repartition_by_part_array(const int64_t part_level);
    int repartition_by_part_histogram(const int64_t part_level);
    int build_hash_table_by_part_hist(
        HashJoinHistogram *all_part_hists, bool enable_bloom_filter);
    int build_hash_table_by_part_array(HashJoinHistogram *all_part_hists, bool enable_bloom_filter);

    OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
    {
      int64_t part_idx = 0;
      if (0 == level_two_part_count_) {
        part_idx = get_part_level_one_idx(hash_value);
      } else {
        int64_t level1_part_idx = get_part_level_one_idx(hash_value);
        part_idx = get_part_level_two_idx(hash_value);
        part_idx = part_idx + level1_part_idx * level_two_part_count_;
      }
      return part_idx;
    }
    OB_INLINE int64_t get_part_level_one_idx(const uint64_t hash_value)
    { return (hash_value >> part_shift_) & (level_one_part_count_ - 1); }
    OB_INLINE int64_t get_part_level_two_idx(const uint64_t hash_value)
    { return ((hash_value >> part_shift_) >> level1_bit_) & (level_two_part_count_ - 1); }
    OB_INLINE bool is_level_one(int64_t part_level) { return 1 == part_level; }
    OB_INLINE bool get_total_row_count() { return total_row_count_; }
  public:
    ObIAllocator *alloc_;
    int64_t part_count_;
    ObHashJoinPartition *hj_parts_;
    int64_t max_level_;
    int64_t part_shift_;
    int64_t level1_bit_;
    int64_t level2_bit_;
    int64_t level_one_part_count_;
    int64_t level_two_part_count_;
    HashJoinHistogram part_histogram_;
    int64_t total_row_count_;
  };
public:
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_drain_exch() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  virtual int inner_close() override;

  PartHashJoinTable &get_hash_table()
  {
    return hash_table_;
  }

private:
  void calc_cache_aware_partition_count();
  int recursive_postprocess();
  int insert_batch_row(const int64_t cur_partition_in_memory);
  int insert_all_right_row(const int64_t row_count);
  int dump_remain_part_for_cache_aware();
  OB_INLINE int64_t get_part_level_one_idx(const uint64_t hash_value)
  { return (hash_value >> part_shift_) & (level1_part_count_ - 1); }
  OB_INLINE int64_t get_part_level_two_idx(const uint64_t hash_value)
  { return ((hash_value >> part_shift_) >> level1_bit_) & (level2_part_count_ - 1); }
  OB_INLINE int64_t get_cache_aware_part_idx(const uint64_t hash_value)
  {
    int64_t part_idx = 0;
    if (0 == level2_part_count_) {
      part_idx = get_part_level_one_idx(hash_value);
    } else {
      int64_t level1_part_idx = get_part_level_one_idx(hash_value);
      part_idx = get_part_level_two_idx(hash_value);
      part_idx = part_idx + level1_part_idx * level2_part_count_;
    }
    return part_idx;
  }
  void init_system_parameters();
  inline int64_t get_level_one_part(int64_t hash_val)
  { return hash_val & (level1_part_count_ - 1); }
  inline int init_mem_context(uint64_t tenant_id);
  void part_rescan();
  int part_rescan(bool reset_all);
  void reset();
  void reset_base();

  int inner_join_read_hashrow_func_going();
  int other_join_read_hashrow_func_going();

  int inner_join_read_hashrow_func_end();
  int other_join_read_hashrow_func_end();

  int set_hash_function(int8_t hash_join_hasher);

  int next();
  int join_end_operate();
  int join_end_func_end();
  int get_next_left_row();
  int get_next_left_row_na();
  int reuse_for_next_chunk();
  int load_next();
  int build_hash_table_for_nest_loop(int64_t &num_left_rows);
  int nest_loop_process(bool &need_not_read_right);
  int64_t calc_partition_count(
  int64_t input_size, int64_t part_size, int64_t max_part_count);
  int64_t calc_partition_count_by_cache_aware(
            int64_t row_count, int64_t max_part_count, int64_t global_mem_bound_size);
  int64_t calc_max_data_size(const int64_t extra_memory_size);
  int get_max_memory_size(int64_t input_size);
  int64_t calc_bucket_number(const int64_t row_count);
  int calc_basic_info(bool global_info = false);
  int get_processor_type();
  int build_hash_table_in_memory(int64_t &num_left_rows);
  int in_memory_process(bool &need_not_read_right);
  int init_join_partition();
  int force_dump(bool for_left);
  void update_remain_data_memory_size(
    int64_t row_count,
    int64_t total_mem_size, bool &need_dump);
  bool need_more_remain_data_memory_size(
    int64_t row_count,
    int64_t total_mem_size,
    double &data_ratio);
  int update_remain_data_memory_size_periodically(int64_t row_count, bool &need_dump, bool force_update = false);
  int dump_build_table(int64_t row_count, bool force_update = false);
  int split_partition(int64_t &num_left_rows);
  int prepare_hash_table();
  void trace_hash_table_collision(int64_t row_cnt);
  int build_hash_table_for_recursive();
  int split_partition_and_build_hash_table(int64_t &num_left_rows);
  int recursive_process(bool &need_not_read_right);
  int adaptive_process(bool &need_not_read_right);
  int get_next_right_row();
  int get_next_right_row_na();
  int read_right_operate();
  int calc_hash_value(
      const ObIArray<ObExpr*> &join_keys,
      const ObIArray<ObHashFunc> &hash_funcs,
      uint64_t &hash_value,
      bool is_left_side,
      bool &null_skip);
  int calc_right_hash_value();
  int finish_dump(bool for_left, bool need_dump, bool force = false);
  int read_right_func_end();
  int calc_equal_conds(bool &is_match);
  int read_hashrow();
  int dump_probe_table();
  int read_hashrow_func_going();
  int read_hashrow_func_end();
  int find_next_matched_tuple(ObHashJoinStoredJoinRow *&tuple);
  int left_anti_semi_operate();
  int left_anti_semi_going();
  int left_anti_naaj_going();
  int left_anti_semi_end();
  int find_next_unmatched_tuple(ObHashJoinStoredJoinRow *&tuple);
  int fill_left_operate();
  int convert_exprs(
      const ObHashJoinStoredJoinRow *store_row, const ObIArray<ObExpr*> &exprs, bool &has_fill);
  int fill_left_going();
  int fill_left_end();
  int join_rows_with_right_null();
  int join_rows_with_left_null();
  int only_join_left_row();
  int only_join_right_row();
  int dump_remain_partition();
  int update_dumped_partition_statistics(bool is_left);

  int save_last_right_row();
  int restore_last_right_row();
  int get_next_batch_right_rows();
  int get_match_row(bool &is_matched);
  int get_next_right_row_for_batch(NextFunc next_func);
private:
  // **** for vectorized *****
  int fill_partition_batch(int64_t &num_left_rows);
  int fill_partition_batch_opt(int64_t &num_left_rows);
  int get_next_left_row_batch(bool is_from_row_store,
                              const ObBatchRows *&child_brs);
  int get_next_left_row_batch_na(bool is_from_row_store, const ObBatchRows *&child_brs);
  int get_next_right_batch();
  int get_next_right_batch_na();
  int calc_hash_value_batch(const ObIArray<ObExpr*> &join_keys,
                            const ObBatchRows *brs,
                            const bool is_from_row_store,
                            uint64_t *hash_vals,
                            const ObHashJoinStoredJoinRow **store_rows,
                            bool is_left_side);
  int read_hashrow_batch();
  int read_hashrow_batch_for_left_semi_anti();
  int convert_right_exprs_batch_one(int64_t batch_idx);
  int convert_exprs_batch_one(const ObHashJoinStoredJoinRow *store_row,
                              const ObIArray<ObExpr*> &exprs);
  int inner_join_read_hashrow_going_batch();
  int inner_join_read_hashrow_end_batch();
  int join_rows_with_left_null_batch_one(int64_t batch_idx);
  int left_anti_semi_read_hashrow_going_batch();
  int left_anti_semi_read_hashrow_end_batch();
  int right_anti_semi_read_hashrow_going_batch();
  int right_anti_semi_read_hashrow_end_batch();
  int outer_join_read_hashrow_going_batch();
  int outer_join_read_hashrow_end_batch();
  int fill_left_join_result_batch();
  int dump_right_row_batch_one(int64_t part_idx, int64_t batch_idx);
  void set_output_eval_info();
  int calc_part_idx_batch(uint64_t *hash_vals, const ObBatchRows &child_brs);
  // **** for vectorized end ***

  /********** for shared hash table hash join ***********/
  int set_shared_info();
  int sync_wait_processor_type();
  int sync_wait_part_count();
  int sync_wait_cur_dumped_partition_idx();
  int sync_wait_basic_info(uint64_t &build_ht_thread_ptr);
  int sync_wait_init_build_hash(const uint64_t build_ht_thread_ptr);
  int sync_wait_finish_build_hash();
  int sync_wait_fetch_next_batch();
  int sync_check_early_exit(bool &early_exit);
  int sync_set_early_exit();
  int do_sync_wait_all();
  int sync_wait_close();
  int sync_wait_open();
  /********** end for shared hash table hash join *******/
private:
  using PredFunc = std::function<bool(int64_t)>;
  int fill_partition(int64_t &num_left_rows);
  OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
  { return (hash_value >> part_shift_) & (part_count_ - 1); }
  OB_INLINE bool top_part_level() { return 0 == part_level_; }
  void set_processor(HJProcessor p) { hj_processor_ = p; }
  OB_INLINE  bool need_right_bitset() const
  {
    return (RIGHT_OUTER_JOIN == MY_SPEC.join_type_ || FULL_OUTER_JOIN == MY_SPEC.join_type_ ||
      RIGHT_SEMI_JOIN == MY_SPEC.join_type_ || RIGHT_ANTI_JOIN == MY_SPEC.join_type_);
  }
  OB_INLINE bool all_dumped() { return -1 == cur_dumped_partition_; }
  OB_INLINE int64_t get_mem_used() { return nullptr == mem_context_ ? 0 : mem_context_->used(); }
  // memory used for dump that it's really used
  OB_INLINE int64_t get_cur_mem_used()
  {
    return get_mem_used()
      - (nullptr != hash_table_.buckets_ ? hash_table_.buckets_->mem_used() : 0)
      - dumped_fixed_mem_size_;
  }
  OB_INLINE int64_t get_data_mem_used() { return sql_mem_processor_.get_data_size(); }
  OB_INLINE bool need_dump(int64_t mem_used)
  {
    return mem_used > sql_mem_processor_.get_mem_bound() * buf_mgr_->get_data_ratio();
  }
  OB_INLINE bool need_dump()
  {
    return nullptr != buf_mgr_
      ? get_cur_mem_used() > sql_mem_processor_.get_mem_bound() * buf_mgr_->get_data_ratio()
      : get_cur_mem_used() > sql_mem_processor_.get_mem_bound();
  }
  int64_t get_need_dump_size(int64_t mem_used)
  {
    return mem_used - sql_mem_processor_.get_mem_bound() * buf_mgr_->get_data_ratio() + 2 * 1024 * 1024;
  }
  OB_INLINE bool all_in_memory(int64_t size) const
  { return size < remain_data_memory_size_; }
  void clean_batch_mgr()
  {
    if (nullptr != batch_mgr_) {
      batch_mgr_->reset();
      if (left_batch_ != NULL) {
        batch_mgr_->free(left_batch_);
        left_batch_ = NULL;
      }
      if (right_batch_ != NULL) {
        batch_mgr_->free(right_batch_);
        right_batch_ = NULL;
      }
    }
  }
  void reset_nest_loop()
  {
    nth_nest_loop_ = 0;
    cur_nth_row_ = 0;
    nth_right_row_ = -1;
    reset_statistics();
  }
  void reset_statistics()
  {
    bitset_filter_cnt_ = 0;
    probe_cnt_ = 0;
    hash_equal_cnt_ = 0;
    hash_link_cnt_ = 0;
  }
  // 这里可能会放大，暂时这样
  int64_t get_extra_memory_size() const
  {
    int64_t bucket_cnt = profile_.get_bucket_size();
    const int64_t DEFAULT_EXTRA_SIZE = 2 * 1024 * 1024;
    int64_t res = bucket_cnt * (sizeof(HTBucket));
    return  res < 0 ? DEFAULT_EXTRA_SIZE : res;
  }

  void clean_nest_loop_chunk()
  {
    hash_table_.reset();
    if (nullptr != bloom_filter_) {
      bloom_filter_->reset();
    }
    right_bit_set_.reset();
    alloc_->reuse();
    reset_nest_loop();
    nest_loop_state_ = LOOP_START;
  }
  OB_INLINE void mark_return() { need_return_ = true; }
  int init_bloom_filter(ObIAllocator &alloc, int64_t bucket_cnt);
  void free_bloom_filter();

  int asyn_dump_partition(int64_t dumped_size,
                      bool is_left,
                      bool dump_all,
                      int64_t start_dumped_part_idx,
                      PredFunc pred);

  bool can_use_cache_aware_opt();
  int read_hashrow_normal();
  int read_hashrow_for_cache_aware(NextFunc next_func);
  int init_histograms(HashJoinHistogram *&part_histograms, int64_t part_count);
  int partition_and_build_histograms();
  int repartition(PartitionSplitter &part_splitter,
        HashJoinHistogram *&part_histograms,
        ObHashJoinPartition *hj_parts,
        bool is_build_side);
  int get_next_probe_partition();
  inline bool check_right_need_dump(int64_t part_idx)
  {
    return hj_part_array_[part_idx].is_dumped() || (is_shared_ && part_idx > cur_dumped_partition_);
  }
  inline bool is_left_naaj() { return LEFT_ANTI_JOIN == MY_SPEC.join_type_ && MY_SPEC.is_naaj_; }
  inline bool is_right_naaj() { return RIGHT_ANTI_JOIN == MY_SPEC.join_type_ && MY_SPEC.is_naaj_; }
  inline bool is_left_naaj_na() { return is_left_naaj() && !MY_SPEC.is_sna_; }
  inline bool is_right_naaj_na() { return is_right_naaj() && !MY_SPEC.is_sna_; }
  inline bool is_left_naaj_sna() { return is_left_naaj() && MY_SPEC.is_sna_; }
  inline bool is_right_naaj_sna() { return is_right_naaj() && MY_SPEC.is_sna_; }
  int check_join_key_for_naaj(const bool is_left, bool &is_null);
  int check_join_key_for_naaj_batch_output(const int64_t batch_size);
  int check_join_key_for_naaj_batch(const bool is_left,
                                    const int64_t batch_size,
                                    bool &has_null,
                                    const ObBatchRows *child_brs);
private:
  typedef int (ObHashJoinOp::*ReadFunc)();
  typedef int (ObHashJoinOp::*state_function_func_type)();
  typedef int (ObHashJoinOp::*state_operation_func_type)();
  typedef int (ObHashJoinOp::*Get_next_right_row)();
  typedef int (ObHashJoinOp::*Get_next_left_row)();
  typedef int (ObHashJoinOp::*Get_next_right_batch)();
  typedef int (ObHashJoinOp::*Get_next_left_batch)(bool is_from_row_store,
                                                   const ObBatchRows *&child_brs);
  static const int64_t RATIO_OF_BUCKETS = 2;
  // min row count for estimated row count
  static const int64_t MIN_ROW_COUNT = 10000;
  // max row count for estimated row count
  static const int64_t MAX_ROW_COUNT = 1000000;
  // max memory size limit --unused
  static const int64_t DEFAULT_MEM_LIMIT = 100 * 1024 * 1024;

  static const int64_t CACHE_AWARE_PART_CNT = 128;
  static const int64_t BATCH_RESULT_SIZE = 512;
  static const int64_t INIT_LTB_SIZE = 64;
  static const int64_t MIN_PART_COUNT = 8;
  static const int64_t PAGE_SIZE = ObChunkDatumStore::BLOCK_SIZE;
  static const int64_t MIN_MEM_SIZE = (MIN_PART_COUNT + 1) * PAGE_SIZE;
  // 目前最大层次为4，通过高位4个字节作为recursive处理，超过partition level采用nest loop方式处理
  static const int64_t MAX_PART_LEVEL = 4;
  static const int64_t PART_SPLIT_LEVEL_ONE = 1;
  static const int64_t PART_SPLIT_LEVEL_TWO = 2;

  static const int8_t ENABLE_HJ_NEST_LOOP = 0x01;
  static const int8_t ENABLE_HJ_RECURSIVE = 0x02;
  static const int8_t ENABLE_HJ_IN_MEMORY = 0x04;
  static const int8_t HJ_PROCESSOR_MASK =
                          ENABLE_HJ_NEST_LOOP | ENABLE_HJ_RECURSIVE | ENABLE_HJ_IN_MEMORY;
  static int8_t HJ_PROCESSOR_ALGO;

  static const int8_t DEFAULT_MURMUR_HASH = 0x01;
  static const int8_t ENABLE_WY_HASH = 0x02;
  static const int8_t ENABLE_XXHASH64 = 0x04;
  static const int8_t HASH_FUNCTION_MASK = DEFAULT_MURMUR_HASH | ENABLE_WY_HASH | ENABLE_XXHASH64;

  // hard code seed, 24bit max prime number
  static const int64_t HASH_SEED = 16777213;
  // about 120M
  static const int64_t MAX_NEST_LOOP_RIGHT_ROW_COUNT = 1000000000;
  static bool TEST_NEST_LOOP_TO_RECURSIVE;

  // make PART_COUNT and MAX_PAGE_COUNT configurable by unittest
  static int64_t PART_COUNT;
  static int64_t MAX_PAGE_COUNT;
  static const int64_t MIN_BATCH_ROW_CNT_NESTLOOP = 256;
  // 之前hash join ctx变量
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
  HJState hj_state_;
  HJProcessor hj_processor_;
  ObHashJoinBufMgr *buf_mgr_;
  ObHashJoinBatchMgr *batch_mgr_;
  ObHashJoinBatch *left_batch_;
  ObHashJoinBatch *right_batch_;
  common::ObHashFuncs tmp_hash_funcs_;
  common::ObArrayHelper<common::ObHashFunc> left_hash_funcs_;
  common::ObArrayHelper<common::ObHashFunc> right_hash_funcs_;
  common::ObArrayHelper<ObExpr*> left_join_keys_;
  common::ObArrayHelper<ObExpr*> right_join_keys_;
  ReadFunc going_func_;
  ReadFunc end_func_;
  int32_t part_level_;
  int32_t part_shift_;
  int64_t part_count_;
  bool force_hash_join_spill_;
  int8_t hash_join_processor_;
  int64_t tenant_id_;
  int64_t input_size_;
  int64_t total_extra_size_;
  int64_t predict_row_cnt_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  // 之前part hash join ctx变量 ，主要是一些reset和rescan设置对不同变量进行处理，这里暂时直接隔开
  ObJoinState state_;
  uint64_t cur_right_hash_value_; // cur right row's hash_value
  bool right_has_matched_; // if cur right row has matched
  bool tuple_need_join_;  // for left_semi, left_anti_semi
  bool first_get_row_;
  HashJoinDrainMode drain_mode_;
  int64_t cur_bkid_; // for left,anti
  int64_t remain_data_memory_size_;
  int64_t nth_nest_loop_;
  int64_t cur_nth_row_;
  int64_t dumped_fixed_mem_size_;
  PartHashJoinTable hash_table_;
  PartHashJoinTable *cur_hash_table_;
  ObHashJoinStoredJoinRow *cur_tuple_; // null or last matched tuple
  common::ObNewRow cur_left_row_; // like cur_row_ in operator, get row from rowstore
  lib::MemoryContext mem_context_;
  common::ObIAllocator *alloc_; // for buckets
  ModulePageAllocator *bloom_filter_alloc_;
  ObGbyBloomFilter *bloom_filter_;
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator>
                                                                                    right_bit_set_;
  int64_t nth_right_row_;
  int64_t ltb_size_;
  int64_t l2_cache_size_;
  int64_t price_per_row_;
  int64_t max_partition_count_per_level_;
  int64_t cur_dumped_partition_;
  int32_t batch_round_;
  HJLoopState nest_loop_state_;
  bool is_shared_;
  bool is_last_chunk_;
  bool has_right_bitset_;
  ObHashJoinPartition *hj_part_array_;
  ObHashJoinPartition *right_hj_part_array_;
  // store row read from partition
  const ObHashJoinStoredJoinRow *left_read_row_;
  const ObHashJoinStoredJoinRow *right_read_row_;
  bool postprocessed_left_;
  bool has_fill_right_row_;
  bool has_fill_left_row_;
  ObChunkDatumStore::ShadowStoredRow right_last_row_;
  bool need_return_;
  bool iter_end_;
  bool opt_cache_aware_;
  bool has_right_material_data_;
  bool enable_bloom_filter_;
  HashJoinHistogram *part_histograms_;
  int64_t cur_full_right_partition_;
  bool right_iter_end_;
  int64_t cur_bucket_idx_;
  int64_t max_bucket_idx_;
  bool enable_batch_;
  int64_t level1_bit_;
  int64_t level1_part_count_;
  int64_t level2_part_count_;
  PartitionSplitter right_splitter_;
  HashJoinHistogram *cur_left_hist_;
  HashJoinHistogram *cur_right_hist_;
  int64_t cur_probe_row_idx_;
  int64_t max_right_bucket_idx_;

  // statistics
  int64_t probe_cnt_;
  int64_t bitset_filter_cnt_;
  int64_t hash_link_cnt_;
  int64_t hash_equal_cnt_;

  //***** for vectorized ***
  int64_t max_output_cnt_;
  const ObHashJoinStoredJoinRow **hj_part_stored_rows_;
  uint64_t *hash_vals_;
  const ObBatchRows *right_brs_;
  const ObHashJoinStoredJoinRow **right_hj_part_stored_rows_;
  bool right_read_from_stored_;
  uint64_t *right_hash_vals_;
  ObHashJoinStoredJoinRow **cur_tuples_;
  int64_t right_batch_traverse_cnt_;
  ObBatchRows child_brs_; // used for get_next_batch from datum store
  ObHashJoinStoredJoinRow **hj_part_added_rows_;
  uint16_t *part_selectors_;
  uint16_t *part_selector_sizes_;

  // store matched rows in selector, initialized in calc_hash_value_batch()
  uint16_t *right_selector_;
  uint16_t right_selector_cnt_;

  // ***** end vectorized ***
  // if we read null value in naaj, may break loop drictly
  bool read_null_in_naaj_;
  Get_next_right_row get_next_right_row_func_;
  Get_next_left_row get_next_left_row_func_;
  Get_next_right_batch get_next_right_batch_func_;
  Get_next_left_batch get_next_left_batch_func_;
  bool non_preserved_side_is_not_empty_;
  int64_t null_random_hash_value_;
  /*
  *for inner join && semi join, we can skip all rows which contain null join key
  *for left outer join, we can skip null join key on right side, random left side hash value
  *for right outer join, we can skip null join key on left side, random right side hash value
  *for full outer join, we do not skip null join key, only random its both side hash value
  *for anti join, we do not skip null join key && not change its hash value
  */
  bool skip_left_null_;
  bool skip_right_null_;
  ObChunkDatumStore::IterationAge iter_age_;
};

inline int ObHashJoinOp::init_mem_context(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_mem_attr(tenant_id, common::ObModIds::OB_ARENA_HASH_JOIN,
                     common::ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    } else {
      alloc_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_OP_H_ */
