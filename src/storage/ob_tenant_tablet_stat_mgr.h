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

#ifndef OCEANBASE_STORAGE_TENANT_TABLET_STAT_MGR_H_
#define OCEANBASE_STORAGE_TENANT_TABLET_STAT_MGR_H_

#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "lib/hash/ob_hashmap.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/list/ob_dlist.h"

namespace oceanbase
{

namespace storage
{

struct ObTransNodeDMLStat
{
public:
  ObTransNodeDMLStat() { reset(); }
  ~ObTransNodeDMLStat() { reset(); }
  void reset() { MEMSET(this, 0, sizeof(*this)); }
  bool empty() const;
  void atomic_inc(const ObTransNodeDMLStat &other);
  int64_t get_dml_count() const { return insert_row_count_ + update_row_count_ + delete_row_count_; }

  TO_STRING_KV(K_(insert_row_count), K_(update_row_count), K_(delete_row_count));
public:
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t delete_row_count_;
};


struct ObTabletStatKey
{
public:
  ObTabletStatKey() : ls_id_(), tablet_id_() {}
  ObTabletStatKey(const int64_t ls_id, const uint64_t tablet_id);
  ObTabletStatKey(const share::ObLSID ls_id, const common::ObTabletID tablet_id);
  ~ObTabletStatKey();
  void reset();
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool is_valid() const;
  bool operator == (const ObTabletStatKey &other) const;
  bool operator != (const ObTabletStatKey &other) const;
  TO_STRING_KV(K_(ls_id), K_(tablet_id));

  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};


struct ObTabletStat
{
public:
  ObTabletStat() { reset(); }
  ~ObTabletStat() = default;
  void reset() { MEMSET(this, 0, sizeof(ObTabletStat)); }
  bool is_valid() const;
  bool check_need_report() const;
  int64_t get_total_merge_row_count() const { return insert_row_cnt_ + update_row_cnt_ + delete_row_cnt_; }
  ObTabletStat& operator=(const ObTabletStat &other);
  ObTabletStat& operator+=(const ObTabletStat &other);
  ObTabletStat& archive(int64_t factor);
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(query_cnt), K_(merge_cnt), K_(scan_logical_row_cnt),
               K_(scan_physical_row_cnt), K_(scan_micro_block_cnt), K_(pushdown_micro_block_cnt),
               K_(exist_row_total_table_cnt), K_(exist_row_read_table_cnt), K_(insert_row_cnt),
               K_(update_row_cnt), K_(delete_row_cnt));

public:
  static constexpr int64_t QUERY_REPORT_INEFFICIENT_THRESHOLD = 3;
  static constexpr int64_t MERGE_REPORT_MIN_ROW_CNT = 1000;
public:
  int64_t ls_id_;
  uint64_t tablet_id_;
  uint32_t query_cnt_;
  uint32_t merge_cnt_;
  uint64_t scan_logical_row_cnt_;
  uint64_t scan_physical_row_cnt_;
  uint64_t scan_micro_block_cnt_;
  uint64_t pushdown_micro_block_cnt_;
  uint64_t exist_row_total_table_cnt_;
  uint64_t exist_row_read_table_cnt_;
  uint64_t insert_row_cnt_;
  uint64_t update_row_cnt_;
  uint64_t delete_row_cnt_;
};


struct ObTabletStatAnalyzer
{
public:
  ObTabletStatAnalyzer() = default;
  ~ObTabletStatAnalyzer() = default;
  bool is_hot_tablet() const;
  bool is_insert_mostly() const;
  bool is_update_or_delete_mostly() const;
  bool has_slow_query() const;
  TO_STRING_KV(K_(tablet_stat), K_(is_small_tenant), K_(boost_factor));
public:
  static constexpr int64_t ACCESS_FREQUENCY = 5;
  static constexpr int64_t BASE_FACTOR = 10;
  static constexpr int64_t LOAD_THRESHOLD = 7;
  static constexpr int64_t TOMBSTONE_THRESHOLD = 3;
  static constexpr int64_t QUERY_BASIC_ROW_CNT = 1000;
  static constexpr int64_t QUERY_BASIC_MICRO_BLOCK_CNT = 10;
  static constexpr int64_t QUERY_BASIC_ITER_TABLE_CNT = 5;
  static constexpr int64_t MERGE_BASIC_ROW_CNT = 10000;
public:
  ObTabletStat tablet_stat_;
  int64_t boost_factor_;
  bool is_small_tenant_;
};


struct ObTenantSysStat
{
public:
  ObTenantSysStat();
  ~ObTenantSysStat() = default;
  void reset();
  bool is_small_tenant() const;
  bool is_full_cpu_usage() const;
  TO_STRING_KV(K_(cpu_usage_percentage), K_(min_cpu_cnt), K_(max_cpu_cnt), K_(memory_hold), K_(memory_limit));

public:
  static constexpr double EPS = 1e-9;
  double cpu_usage_percentage_;
  double min_cpu_cnt_;
  double max_cpu_cnt_;
  int64_t memory_hold_;
  int64_t memory_limit_;
};


template<uint32_t SIZE>
class ObTabletStatBucket
{
public:
  ObTabletStatBucket(const uint32_t step)
    : head_idx_(0), curr_idx_(SIZE - 1), refresh_cnt_(0), step_(step) {}
  ~ObTabletStatBucket() {}
  void reset();
  OB_INLINE int64_t count() const { return curr_idx_ - head_idx_ + 1; }
  void add(const ObTabletStat &tablet_stat);
  bool retire_and_switch(ObTabletStat &old_stat);
  void refresh(ObTabletStat &stat, bool &has_retired_stat);
  void get_tablet_stat(ObTabletStat &tablet_stat) const;
  uint32_t get_idx(const uint32_t &idx) const { return idx % SIZE; }
  TO_STRING_KV(K_(units), K_(head_idx), K_(curr_idx), K_(refresh_cnt), K_(step));

public:
  ObTabletStat units_[SIZE];
  uint32_t head_idx_;
  uint32_t curr_idx_;
  uint32_t refresh_cnt_;
  uint32_t step_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletStatBucket);
};

template<uint32_t SIZE>
void ObTabletStatBucket<SIZE>::reset()
{
  for (int64_t i = 0; i < SIZE; ++i) {
    units_[i].reset();
  }
  head_idx_ = 0;
  curr_idx_ = SIZE - 1;
  refresh_cnt_ = 0;
}

template<uint32_t SIZE>
void ObTabletStatBucket<SIZE>::add(const ObTabletStat &stat)
{
  units_[get_idx(curr_idx_)] += stat;
}

template<uint32_t SIZE>
bool ObTabletStatBucket<SIZE>::retire_and_switch(ObTabletStat &old_stat)
{
  bool need_retire = (0 == refresh_cnt_ % step_);

  if (need_retire) { // retire head unit and switch cur unit
    old_stat = units_[get_idx(head_idx_)];
    units_[get_idx(head_idx_)].reset();
    ++head_idx_;
    ++curr_idx_;
  }
  return need_retire;
}

template<uint32_t SIZE>
void ObTabletStatBucket<SIZE>::refresh(ObTabletStat &stat, bool &has_retired_stat)
{
  ++refresh_cnt_;

  if (has_retired_stat) {
    add(stat);
    has_retired_stat = false;
  }
  has_retired_stat = retire_and_switch(stat);
}

template<uint32_t SIZE>
void ObTabletStatBucket<SIZE>::get_tablet_stat(ObTabletStat &tablet_stat) const
{
  for (int64_t i = 0; i < SIZE; ++i) {
    tablet_stat += units_[i];
  }
}


class ObTabletStream
{
public:
  ObTabletStream();
  virtual ~ObTabletStream();
  void reset();
  void add_stat(const ObTabletStat &stat);
  void refresh();

  template <uint32_t SIZE>
  int get_bucket_tablet_stat(
      const ObTabletStatBucket<SIZE> &bucket,
      common::ObIArray<ObTabletStat> &tablet_stats) const;
  int get_all_tablet_stat(common::ObIArray<ObTabletStat> &tablet_stats) const;
  OB_INLINE ObTabletStatKey& get_tablet_stat_key() { return key_; }
  OB_INLINE void get_latest_stat(ObTabletStat &tablet_stat) const { curr_buckets_.get_tablet_stat(tablet_stat); }
  TO_STRING_KV(K_(key), K_(curr_buckets), K_(latest_buckets), K_(past_buckets));

private:
  static constexpr uint32_t CURR_BUCKET_CNT = 8;
  static constexpr uint32_t LATEST_BUCKET_CNT = 4;
  static constexpr uint32_t PAST_BUCKET_CNT = 4;
  static constexpr uint32_t CURR_BUCKET_STEP = 1; // 2min for each unit, total 16min
  static constexpr uint32_t LATEST_BUCKET_STEP = 16; // 32min for each unit, total 128min
  static constexpr uint32_t PAST_BUCKET_STEP = 32; // 64min for each unit, total 256min

  ObTabletStatKey key_;
  ObTabletStatBucket<CURR_BUCKET_CNT> curr_buckets_;
  ObTabletStatBucket<LATEST_BUCKET_CNT> latest_buckets_;
  ObTabletStatBucket<PAST_BUCKET_CNT> past_buckets_;
};


class ObTabletStreamNode : public ObDLinkBase<ObTabletStreamNode>
{
public:
  explicit ObTabletStreamNode(const int64_t flag = 0)
    : stream_(), flag_(flag) {}
  ~ObTabletStreamNode() { reset(); }
  void reset() { stream_.reset(); }
  TO_STRING_KV(K_(stream), K_(flag));

public:
  ObTabletStream stream_;
  const int64_t flag_;
};


class ObTabletStreamPool
{
public:
  typedef common::ObFixedQueue<ObTabletStreamNode> FreeList;
  typedef common::ObDList<ObTabletStreamNode> LruList;
  enum NodeAllocType: int64_t {
    FIXED_ALLOC = 0,
    DYNAMIC_ALLOC
  };

  ObTabletStreamPool();
  ~ObTabletStreamPool();
  void destroy();
  int init(const int64_t max_free_list_num,
           const int64_t up_limit_node_num);
  int alloc(ObTabletStreamNode *&node, bool &is_retired);
  void free(ObTabletStreamNode *node);
  bool add_lru_list(ObTabletStreamNode *node) { return lru_list_.add_first(node); }
  bool remove_lru_list(ObTabletStreamNode *node) { return lru_list_.remove(node); }
  bool update_lru_list(ObTabletStreamNode *node) { return lru_list_.move_to_first(node); }
  OB_INLINE int64_t get_free_num() const { return free_list_.get_total(); }
  OB_INLINE int64_t get_allocated_num() const { return (max_free_list_num_ - get_free_num()) + allocated_dynamic_num_; }
  TO_STRING_KV(K_(max_free_list_num), K_(max_dynamic_node_num), K_(allocated_dynamic_num));

private:
  common::ObFIFOAllocator dynamic_allocator_;
  common::ObArenaAllocator free_list_allocator_;
  FreeList free_list_;
  LruList lru_list_;
  int64_t max_free_list_num_;
  int64_t max_dynamic_node_num_;
  int64_t allocated_dynamic_num_;
  bool is_inited_;
};


class ObTenantTabletStatMgr
{
public:
  static int mtl_init(ObTenantTabletStatMgr* &tablet_stat_mgr);
  ObTenantTabletStatMgr();
  virtual ~ObTenantTabletStatMgr();
  int init(const int64_t tenant_id);
  bool is_inited() const { return is_inited_; }
  // int start();
  void wait();
  void stop();
  void destroy();
  void reset();

  int report_stat(
      const ObTabletStat &stat,
      bool &succ_report);
  int get_latest_tablet_stat(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      ObTabletStat &tablet_stat);
  int get_history_tablet_stats(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      common::ObIArray<ObTabletStat> &tablet_stats);
  int get_tablet_analyzer(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      ObTabletStatAnalyzer &analyzer);
  int get_sys_stat(ObTenantSysStat &sys_stat);
  void process_stats();
  void refresh_all(const int64_t step);
private:
  class TabletStatUpdater : public common::ObTimerTask
  {
  public:
    TabletStatUpdater(ObTenantTabletStatMgr &mgr) : mgr_(mgr) {}
    virtual ~TabletStatUpdater() {}
    virtual void runTimerTask();
  private:
    ObTenantTabletStatMgr &mgr_;
  };

private:
  int update_tablet_stream(const ObTabletStat &report_stat);
  int fetch_node(ObTabletStreamNode *&node);
private:
  typedef common::hash::ObHashMap<ObTabletStatKey,
                          ObTabletStreamNode *,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<ObTabletStatKey>,
                          common::hash::equal_to<ObTabletStatKey>,
                          common::hash::SimpleAllocer<typename common::hash::HashMapTypes<ObTabletStatKey, ObTabletStreamNode *>::AllocType>,
                          common::hash::NormalPointer,
                          common::ObMalloc,
                          1 /*disable auto expansion */> TabletStreamMap;

  static constexpr int64_t TABLET_STAT_PROCESS_INTERVAL = 5 * 1000L * 1000L; //5s
  static constexpr int64_t CHECK_INTERVAL = 120L * 1000L * 1000L; //120s
  static constexpr int64_t CHECK_RUNNING_TIME_INTERVAL = 120L * 1000L * 1000L; //120s
  static constexpr int64_t CHECK_SYS_STAT_INTERVAL = 10 * 1000LL * 1000LL; //10s
  static constexpr int32_t DEFAULT_MAX_FREE_STREAM_CNT = 5000;
  static constexpr int32_t DEFAULT_UP_LIMIT_STREAM_CNT = 20000;
  static constexpr int32_t DEFAULT_BUCKET_NUM = 1543; // should be a prime to guarantee the bucket nums of hashmap and bucketlock are equal
  static constexpr int32_t DEFAULT_MAX_PENDING_CNT = 40000;
  static constexpr int32_t MAX_REPORT_RETRY_CNT = 5;

  TabletStatUpdater report_stat_task_;
  ObTabletStreamPool stream_pool_;
  TabletStreamMap stream_map_;
  common::ObBucketLock bucket_lock_;
  ObTabletStat report_queue_[DEFAULT_MAX_PENDING_CNT]; // 12 * 8 * 40000 bytes
  uint64_t report_cursor_;
  uint64_t pending_cursor_;
  int report_tg_id_;
  bool is_inited_;
};

template <uint32_t SIZE>
int ObTabletStream::get_bucket_tablet_stat(
    const ObTabletStatBucket<SIZE> &bucket,
    common::ObIArray<ObTabletStat> &tablet_stats) const
{
  int ret = OB_SUCCESS;
  int64_t idx = bucket.head_idx_;

  for (int64_t i = 0; OB_SUCC(ret) && i < bucket.count(); ++i) {
    int64_t curr_idx = bucket.get_idx(idx);
    if (OB_FAIL(tablet_stats.push_back(bucket.units_[curr_idx]))) {
      STORAGE_LOG(WARN, "failed to add tablet stat", K(ret), K(idx));
    }
    ++idx;
  }
  return ret;
}

#define CHECK_SCHEDULE_TIME_INTERVAL(interval, step) \
  ({ \
    bool bret = false; \
    RLOCAL_STATIC(int64_t, last_time) = ::oceanbase::common::ObTimeUtility::fast_current_time(); \
    int64_t cur_time = ::oceanbase::common::ObTimeUtility::fast_current_time(); \
    int64_t old_time = last_time; \
    step = 0; \
    step = (cur_time - old_time) / interval; \
    if (0 == step) { \
    } else if (old_time == ATOMIC_CAS(&last_time, old_time, cur_time)) \
    { \
      bret = true; \
    } \
    bret; \
  })

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_TENANT_TABLET_STAT_MGR_H_ */
