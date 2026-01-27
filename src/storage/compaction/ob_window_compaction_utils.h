/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_WINDOW_COMPACTION_UTILS_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_WINDOW_COMPACTION_UTILS_H_

#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/compaction/ob_compaction_util.h"

namespace oceanbase
{
namespace compaction
{

struct ObTabletCompactionScoreDynamicInfo
{
public:
  ObTabletCompactionScoreDynamicInfo();
  virtual ~ObTabletCompactionScoreDynamicInfo() = default;
  void reset();
  int assign(const ObTabletCompactionScoreDynamicInfo &other);
  void gen_info(const int64_t compat_version, char* buf, const int64_t buf_len, int64_t &pos) const;
  bool is_valid() const { return cg_merge_batch_cnt_ > 0; }
  TO_STRING_KV(K_(queuing_mode), K_(is_hot_tablet), K_(is_insert_mostly), K_(is_update_or_delete_mostly), K_(has_slow_query),
               K_(has_accumulated_delete), K_(need_recycle_mds), K_(need_progressive_merge), K_(cg_merge_batch_cnt),
               K_(query_cnt), K_(read_amplification_factor));
  OB_UNIS_VERSION(1);
public:
  static constexpr uint32_t TCSDI_ONE_BIT = 1;
  static constexpr uint32_t TCSDI_TEN_BITS = 10;
  static constexpr uint32_t TCSDI_ONE_BYTE = 8;
  static constexpr uint32_t TCSDI_RESERVED_BITS = 7;
  static constexpr uint32_t DYNAMIC_INFO_VERSION = 0;
  static constexpr uint32_t DYNAMIC_INFO_VERSION_LATEST = DYNAMIC_INFO_VERSION;
public:
  union {
    uint32_t info_; // for alignment
    struct {
      uint32_t queuing_mode_               : TCSDI_ONE_BYTE;
      uint32_t is_hot_tablet_              : TCSDI_ONE_BIT;
      uint32_t is_insert_mostly_           : TCSDI_ONE_BIT;
      uint32_t is_update_or_delete_mostly_ : TCSDI_ONE_BIT;
      uint32_t has_slow_query_             : TCSDI_ONE_BIT;
      uint32_t has_accumulated_delete_     : TCSDI_ONE_BIT;
      uint32_t need_recycle_mds_           : TCSDI_ONE_BIT;
      uint32_t need_progressive_merge_     : TCSDI_ONE_BIT;
      uint32_t cg_merge_batch_cnt_         : TCSDI_TEN_BITS; // OceanBase support 4096 columns at most, at most 410 batch when co merge (DEFAULT_CG_MERGE_BATCH_SIZE = 10)
      uint32_t reserved_                   : TCSDI_RESERVED_BITS;
    };
  };
  uint32_t query_cnt_;
  double read_amplification_factor_;
};

struct ObTabletCompactionScoreDecisionInfo
{
public:
  ObTabletCompactionScoreDecisionInfo();
  virtual ~ObTabletCompactionScoreDecisionInfo() = default;
  void reset();
  bool is_valid() const { return major_snapshot_version_ > 0; }
  TO_STRING_KV(K_(dynamic_info), K_(base_inc_row_cnt), K_(tablet_snapshot_version), K_(major_snapshot_version));

public:
  ObTabletCompactionScoreDynamicInfo dynamic_info_;
  uint64_t base_inc_row_cnt_;
  int64_t tablet_snapshot_version_;
  int64_t major_snapshot_version_;
};

using ObTabletCompactionScoreKey = storage::ObTabletStatKey; // ls_id + tablet_id

struct ObTabletCompactionScore : public ObDLinkBase<ObTabletCompactionScore>
{
public:
  enum CompactStatus : uint8_t
  {
    COMPACT_STATUS_WAITING = 0,
    COMPACT_STATUS_READY = 1,
    COMPACT_STATUS_LOG_SUBMITTED = 2,
    COMPACT_STATUS_FINISHED = 3,
    COMPACT_STATUS_MAX
  };
public:
  ObTabletCompactionScore();
  virtual ~ObTabletCompactionScore() { /*do nothing*/ }
  void reset();
  bool is_valid() const { return key_.is_valid() && score_ > 0 && decision_info_.is_valid() && compact_status_ < COMPACT_STATUS_MAX; }
  void update(const ObTabletCompactionScoreKey &key,
              const ObTabletCompactionScoreDecisionInfo &decision_info,
              const int64_t score);
  void update_with(const ObTabletCompactionScoreDecisionInfo &decision_info,
                   const int64_t score);
  OB_INLINE int64_t get_weighted_size() const { return decision_info_.dynamic_info_.cg_merge_batch_cnt_; }
  OB_INLINE int64_t get_major_snapshot_version() const { return decision_info_.major_snapshot_version_; }
  OB_INLINE ObTabletCompactionScoreKey get_key() const { return key_; }
  TO_STRING_KV(K_(key), K_(decision_info), K_(score), K_(pos_at_priority_queue), K_(add_timestamp));
public:
  // compaction status management
  OB_INLINE bool is_waiting_status() const { return COMPACT_STATUS_WAITING == compact_status_; }
  OB_INLINE bool is_ready_status() const { return COMPACT_STATUS_READY == compact_status_; }
  OB_INLINE bool is_log_submitted_status() const { return COMPACT_STATUS_LOG_SUBMITTED == compact_status_; }
  OB_INLINE bool is_finished_status() const { return COMPACT_STATUS_FINISHED == compact_status_; }
  OB_INLINE void set_ready_status() { compact_status_ = COMPACT_STATUS_READY; }
  OB_INLINE void set_log_submitted_status() { compact_status_ = COMPACT_STATUS_LOG_SUBMITTED; }
  OB_INLINE void set_finished_status() { compact_status_ = COMPACT_STATUS_FINISHED; }
public:
  static const char *CompactStatusStr[COMPACT_STATUS_MAX];
  static const char *get_compact_status_str(const CompactStatus compact_status);
public:
  ObTabletCompactionScoreKey key_;
  ObTabletCompactionScoreDecisionInfo decision_info_;
  int64_t score_;
  int64_t pos_at_priority_queue_;
  int64_t add_timestamp_; // used both in priority queue and candidate list
  CompactStatus compact_status_;
  uint32_t magic_; // use padding size, don't affect the alignment
};

class ObWindowCompactionMemoryContext
{
public:
  ObWindowCompactionMemoryContext();
  ~ObWindowCompactionMemoryContext();
  void purge() { allocator_.purge(); }
  int alloc_score(ObTabletCompactionScore *&score);
  void free_score(ObTabletCompactionScore *&score);
  ObIAllocator &get_allocator() { return allocator_; }
  OB_INLINE bool is_score_valid(ObTabletCompactionScore *score)
  { return OB_NOT_NULL(score) && score->is_valid() && CONTAINER_SCORE_MAGIC == score->magic_; }
public:
  static constexpr uint32_t CONTAINER_SCORE_MAGIC = 0x0B1428571;
private:
  common::ObVSliceAlloc allocator_; // ObVSliceAlloc mainly optimizes small block allocation
private:
  DISALLOW_COPY_AND_ASSIGN(ObWindowCompactionMemoryContext);
};

class ObWindowCompactionScoreTracker final
{
public:
  enum UpsertResult : uint8_t
  {
    INSERTED = 0,
    UPDATED = 1,
    SKIPED = 2,
    MAX_REULST
  };
  struct SetCallback final
  {
  public:
    SetCallback(UpsertResult &res) : res_(res) {}
    ~SetCallback() = default;
    int operator()(const hash::HashMapPair<ObTabletID, int64_t> &pair) {
      res_ = UpsertResult::INSERTED;
      return OB_SUCCESS;
    }
  public:
    UpsertResult &res_;
  };
  class UpdateCallback
  {
  public:
    UpdateCallback(const int64_t new_val, UpsertResult &res): new_val_(new_val), res_(res) {}
    ~UpdateCallback() = default;
    int operator()(hash::HashMapPair<ObTabletID, int64_t> &pair);
  private:
    int64_t new_val_;
    UpsertResult &res_;
  };
  struct TabletRecord
  {
  public:
    int append_itself(ObSqlString &records) const;
    TO_STRING_KV(K_(timestamp), K_(tablet_id), K_(inc_row_cnt), K_(dynamic_info), K_(score));
  public:
    int64_t timestamp_;
    int64_t tablet_id_;
    int64_t inc_row_cnt_;
    ObTabletCompactionScoreDynamicInfo dynamic_info_;
    int64_t score_;
  };
public:
  ObWindowCompactionScoreTracker();
  ~ObWindowCompactionScoreTracker() { reset(); }
  int init(const int64_t merge_start_time);
  void reset();
  int upsert(
      const ObTabletID &tablet_id,
      const storage::ObTabletStatAnalyzer *analyzer,
      const ObTabletCompactionScoreDecisionInfo &decision_info,
      const int64_t new_score);
  int finish(const ObTabletID &tablet_id);
private:
  int track_record_(
    const ObTabletID &tablet_id,
    const storage::ObTabletStatAnalyzer *analyzer,
    const ObTabletCompactionScoreDecisionInfo &decision_info,
    const int64_t new_score,
    const UpsertResult result);
  int dump_records_();
private:
  typedef AllocatorWrapper<hash::HashMapTypes<ObTabletID, int64_t>::AllocType, ObArenaAllocator> HashAllocator;
  typedef common::hash::ObHashMap<ObTabletID,
                                  int64_t,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<ObTabletID>,
                                  common::hash::equal_to<ObTabletID>,
                                  HashAllocator,
                                  common::hash::NormalPointer,
                                  ObWrapperAllocator> ScoreMap;
  const static int64_t DEFAULT_BUCKET_NUM = 49999; // should be a prime to guarantee the distribution of hash values are uniform
  const static int64_t MAX_RECORD_NUM = 128;
  const static char* RECORD_SCHEMA_STR;
private:
  bool is_inited_;
  int64_t merge_start_time_;
  ObArenaAllocator allocator_;
  HashAllocator hash_allocator_;
  ObWrapperAllocator bucket_allocator_;
  ScoreMap score_map_;
  ObArray<TabletRecord> record_array_;
};

class ObWindowCompactionBaseContainer
{
public:
  friend class ObWindowCompactionBaseContainerGuard;
public:
  ObWindowCompactionBaseContainer(ObWindowCompactionMemoryContext &mem_ctx,
                                  ObWindowCompactionScoreTracker &score_tracker)
    : lock_(common::ObLatchIds::WINDOW_COMPACTION_BASE_CONTAINER_LOCK),
      mem_ctx_(mem_ctx),
      score_tracker_(score_tracker)
  {}
  virtual ~ObWindowCompactionBaseContainer() {}
  OB_INLINE bool is_score_valid(ObTabletCompactionScore *score) const { return mem_ctx_.is_score_valid(score); }
public:
  typedef common::hash::ObHashMap<ObTabletCompactionScoreKey,
                                  ObTabletCompactionScore *,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<ObTabletCompactionScoreKey>,
                                  common::hash::equal_to<ObTabletCompactionScoreKey>,
                                  common::hash::SimpleAllocer<typename common::hash::HashMapTypes<ObTabletCompactionScoreKey, ObTabletCompactionScore *>::AllocType>,
                                  common::hash::NormalPointer,
                                  common::ObMalloc,
                                  1 /*disable auto expansion */> TabletScoreMap;
  static constexpr int32_t DEFAULT_BUCKET_NUM = 1543; // should be a prime to guarantee the bucket nums of hashmap and bucketlock are equal
protected:
  obsys::ObRWLock<> lock_;
  ObWindowCompactionMemoryContext &mem_ctx_;
  ObWindowCompactionScoreTracker &score_tracker_;
};

struct ObWindowCompactionQueueInfo
{
public:
  ObWindowCompactionQueueInfo() { MEMSET(this, 0, sizeof(ObWindowCompactionQueueInfo)); }
  ~ObWindowCompactionQueueInfo() = default;
  int info_to_string(ObSqlString &queue_info) const;
  TO_STRING_KV(K_(length), K_(max_score), K_(min_score), K_(mid_score), K_(threshold), K_(avg_score));
public:
  int64_t length_;
  int64_t max_score_;
  int64_t min_score_;
  int64_t mid_score_;
  int64_t threshold_;
  double avg_score_;
};

class ObWindowCompactionPriorityQueue : public ObWindowCompactionBaseContainer
{
public:
  friend class ObWindowCompactionPriorityQueueIterator;
public:
  struct ObTabletCompactionScoreMaxHeapCmp
  {
  public:
    int get_error_code() { return OB_SUCCESS; }
    bool operator()(const ObTabletCompactionScore *lhs, const ObTabletCompactionScore *rhs) { return lhs->score_ < rhs->score_; }
  };
public:
  ObWindowCompactionPriorityQueue(ObWindowCompactionMemoryContext &mem_ctx, ObWindowCompactionScoreTracker &score_tracker);
  virtual ~ObWindowCompactionPriorityQueue() { (void) destroy(); }
  int destroy();
  int init();
  int process_tablet_stat_analyzer(
      storage::ObTabletHandle &tablet_handle,
      const storage::ObTabletStatAnalyzer &analyzer);
  int process_tablet_stat(
      storage::ObTablet &tablet,
      const storage::ObTabletStatAnalyzer *analyzer = nullptr);
  int try_insert_or_update_tablet_score(
      const storage::ObTablet &tablet,
      const ObTabletCompactionScoreDecisionInfo &info,
      const int64_t score);
  int push(ObTabletCompactionScore *&score); // if push successfully, score will be set nullptr
  int check_and_pop(const int64_t max_size, ObTabletCompactionScore *&score);
  int fetch_info(ObWindowCompactionQueueInfo &info);
  OB_INLINE bool empty() const { return score_prio_queue_.empty(); }
  TO_STRING_KV(K_(is_inited), "size", score_prio_queue_.count(), "avg_score", get_average_score()); // size may be stale, but it's ok, doesn't affect correctness.
private:
  int inner_push(ObTabletCompactionScore *&score);
  int inner_check_exist_and_get_score(
      const ObTabletCompactionScoreKey &key,
      bool &exist,
      ObTabletCompactionScore *&score);
  int inner_update_with_new_score(
      ObTabletCompactionScore *tablet_score,
      const ObTabletCompactionScoreDecisionInfo &decision_info,
      const int64_t score);
  int inner_push_new_score(
      const ObTabletCompactionScoreKey &key,
      const ObTabletCompactionScoreDecisionInfo &decision_info,
      const int64_t score);
  int inner_fetch_info(ObWindowCompactionQueueInfo &info);
  int check_admission(const int64_t score);
  void update_avg_score_on_success(const int64_t score, const bool is_push);
  OB_INLINE bool is_score_valid_in_queue(ObTabletCompactionScore *score) const
  { return is_score_valid(score) && score->pos_at_priority_queue_ >= 0; }
  OB_INLINE int64_t get_average_score() const { return static_cast<int64_t>(avg_score_); }
private:
  typedef common::ObRemovableHeap<ObTabletCompactionScore *,
                                  ObTabletCompactionScoreMaxHeapCmp,
                                  &ObTabletCompactionScore::pos_at_priority_queue_> TabletCompactionScorePriorityQueue;
private:
  static constexpr int64_t BASE_THRESHOLD = 50000;   // Stage 1: free admission, the default value of compaction_schedule_tablet_batch_cnt
  static constexpr int64_t THRESHOLD_MAX = 200000;   // Stage 4: size overflow
private:
  bool is_inited_;
  ObTabletCompactionScoreMaxHeapCmp max_heap_cmp_;
  TabletScoreMap score_map_;
  TabletCompactionScorePriorityQueue score_prio_queue_;
  double avg_score_; // for size control while million-level tablets
private:
  DISALLOW_COPY_AND_ASSIGN(ObWindowCompactionPriorityQueue);
};

// Only operated by WindowLoop thread, so do not need to add any lock
class ObWindowCompactionReadyList : public ObWindowCompactionBaseContainer
{
public:
  friend class ObWindowCompactionReadyListGuard;
public:
  ObWindowCompactionReadyList(ObWindowCompactionMemoryContext &mem_ctx, ObWindowCompactionScoreTracker &score_tracker);
  virtual ~ObWindowCompactionReadyList() { (void) destroy(); }
  int destroy();
  int init();
  OB_INLINE int64_t get_remaining_space() const { return max_capacity_ - weighted_size_; }
  OB_INLINE int64_t get_current_weighted_size() const { return weighted_size_; }
  OB_INLINE int64_t get_max_capacity() const { return max_capacity_; }
  int get_candidate_list(ObIArray<ObTabletCompactionScore *> &candidates, const int64_t ts_threshold);
  int add(ObTabletCompactionScore *&score);
  int remove(ObTabletCompactionScore *&candidate); // protected by ObWindowCompactionBaseContainerGuard
  void update_max_capacity(const int64_t finished_weighted_size);
  TO_STRING_KV(K_(is_inited), "size", candidate_map_.size(), K_(max_capacity), K_(weighted_size), K_(waiting_score_cnt));
private:
  int inner_add_candidate(ObTabletCompactionScore *&score);
  int inner_check_exist_and_get_candidate(
      const ObTabletCompactionScoreKey &key,
      bool &exist,
      ObTabletCompactionScore *&candidate);
  int inner_update_candidate_with_new_score(
      ObTabletCompactionScore *candidate,
      ObTabletCompactionScore *&score);
private:
  static constexpr int64_t DEFAULT_CANDIDATE_WINDOW_SIZE = 1500;
  static constexpr int64_t MAX_CANDIDATE_WINDOW_SIZE = 12000;
public:
  bool is_inited_;
  TabletScoreMap candidate_map_;
  int64_t max_capacity_;
  int64_t weighted_size_;
  int64_t waiting_score_cnt_; // only used for statistic
private:
  DISALLOW_COPY_AND_ASSIGN(ObWindowCompactionReadyList);
};

class ObWindowCompactionUtils
{
public:
  static int calculate_tablet_compaction_score(
      const storage::ObTabletStatAnalyzer *analyzer,
      storage::ObTablet &tablet,
      bool &need_window_compaction,
      ObTabletCompactionScoreDecisionInfo &decision_info,
      int64_t &score);
private:
  static constexpr uint64_t DEFENSIVE_TIMESTAMP = 1262275200000000000L; // 2010-01-01 00:00:00
  static constexpr int64_t INC_ROW_CNT_THRESHOLD = 10 * 1000; // 10k
  static constexpr int64_t NS_PER_DAY = 1000L * 1000L * 1000L * 3600L * 24L;
private:
  static void multiply_tablet_compaction_score_with_analyzer(
    const storage::ObTabletStatAnalyzer &analyzer,
    ObTabletCompactionScoreDecisionInfo &decision_info,
    double &score_value);
  static int check_need_recycle_truncate_info(
      storage::ObTablet &tablet,
      bool &need_recycle_truncate_info);
  static int get_inc_row_cnt_factor(
      storage::ObTablet &tablet,
      const bool need_recycle_truncate_info,
      int64_t &factor);
  static int estimate_days_by_sstable(
    storage::ObTablet &tablet,
    int64_t &days);
  static int64_t calculate_days_since_timestamp(const int64_t timestamp);
};

class ObWindowCompactionBaseContainerGuard
{
public:
  ObWindowCompactionBaseContainerGuard(ObWindowCompactionBaseContainer &container);
  virtual ~ObWindowCompactionBaseContainerGuard() { release(); }
  int acquire();
  void release();
protected:
  obsys::ObRWLock<> &lock_;
  bool is_locked_;
  DISALLOW_COPY_AND_ASSIGN(ObWindowCompactionBaseContainerGuard);
};


class ObWindowCompactionPriorityQueueIterator : public ObWindowCompactionBaseContainerGuard
{
public:
  ObWindowCompactionPriorityQueueIterator(ObWindowCompactionPriorityQueue &priority_queue);
  virtual ~ObWindowCompactionPriorityQueueIterator() { destroy(); }
  int init();
  void destroy();
  int get_next(ObTabletCompactionScore *&score);
  int get_queue_summary(ObSqlString &queue_summary) const;
private:
  ObWindowCompactionPriorityQueue &queue_;
  int64_t idx_;
  int64_t count_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObWindowCompactionPriorityQueueIterator);
};

class ObWindowCompactionReadyListIterator : public ObWindowCompactionBaseContainerGuard
{
public:
  ObWindowCompactionReadyListIterator(ObWindowCompactionReadyList &ready_list);
  virtual ~ObWindowCompactionReadyListIterator() { /*do nothing*/ }
  int init();
  void destroy();
  int get_next(ObTabletCompactionScore *&score);
  int get_list_summary(ObSqlString &list_summary) const;
private:
  ObWindowCompactionReadyList &list_;
  ObWindowCompactionReadyList::TabletScoreMap::iterator iter_;
  ObWindowCompactionReadyList::TabletScoreMap::iterator end_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObWindowCompactionReadyListIterator);
};

} // namespace compaction
} // namespace oceanbase
#endif