//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_COMPACTION_TIME_GUARD_H_
#define OB_SHARE_COMPACTION_COMPACTION_TIME_GUARD_H_
#include "share/ob_occam_time_guard.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace compaction
{

/*
 * ObCompactionTimeGuard refers to the implementation of from ObOccamTimeGuard.
 * For example, you have 3 enum events {e0, e1, e2}
 * If you want to record the time cost of event e1, you can use click(e1), then event_times_[1] will accumulate the time cost of e1.
 *
 * ObCompactionTimeGuard
 * -- ObRSCompactionTimeGuard
 * -- ObScheduleCompactionTimeGuard
 * -- ObStorageCompactionTimeGuard
 */
class ObCompactionTimeGuard
{
public:
  enum ObCompactionTimeGuardType : uint8_t
  {
    BASE_COMPACT_TIME_GUARD = 0,
    RS_COMPACT_TIME_GUARD = 1,
    SCHEDULE_COMPACT_TIME_GUARD = 2,
    STORAGE_COMPACT_TIME_GUARD = 3,
    MAX_COMPACT_TIME_GUARD
  };
public:
  const static uint64_t WARN_THRESHOLD = 30L * 1000 * 1000; // 30s
  const static uint16_t CAPACITY = 16;
  ObCompactionTimeGuard(const ObCompactionTimeGuardType gurad_type = BASE_COMPACT_TIME_GUARD,
                        const uint64_t warn_threshold = WARN_THRESHOLD,
                        const char *mod = "")
    : guard_type_(gurad_type),
      warn_threshold_(warn_threshold),
      log_mod_(mod),
      capacity_(get_max_event_count(gurad_type)),
      size_(0),
      last_click_ts_(common::ObTimeUtility::current_time()),
      add_time_(common::ObTimeUtility::current_time())
  {
    reuse();
  }
  virtual ~ObCompactionTimeGuard();
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  void reuse();
  void add_time_guard(const ObCompactionTimeGuard &other);
  ObCompactionTimeGuard & operator=(const ObCompactionTimeGuard &other);
  OB_INLINE bool is_empty() const { return 0 == size_; }
  // set the dag add_time as the first click time
  OB_INLINE void set_last_click_ts(const int64_t time)
  {
    last_click_ts_ = time;
    add_time_ = time;
  }
  OB_INLINE uint32_t get_specified_cost_time(const int64_t event) const {
    uint32_t ret_val = 0;
    if (OB_LIKELY(event < size_)) {
      ret_val = event_times_[event];
    }
    return ret_val;
  }
  bool click(const uint16_t event);
  // copy from ObOccamTimeGuard
  void fmt_ts_to_meaningful_str(
       char *buf,
       const int64_t buf_len,
       int64_t &pos,
       const char *lvalue,
       const int64_t ts) const;
public:
  template <typename E>
  static constexpr uint16_t event_idx(E e) { return static_cast<uint16_t>(e); }
  static uint16_t get_max_event_count(const ObCompactionTimeGuardType guard_type);
public:
  ObCompactionTimeGuardType guard_type_;
  const uint64_t warn_threshold_;
  const char * log_mod_;
  uint16_t capacity_; // equal with CAPACITY, used for child class to check the array boundary
  uint16_t size_;
  int64_t last_click_ts_;
  int64_t add_time_;
  uint64_t event_times_[CAPACITY];
};

struct ObRSCompactionTimeGuard : public ObCompactionTimeGuard
{
public:
  ObRSCompactionTimeGuard()
    : ObCompactionTimeGuard(RS_COMPACT_TIME_GUARD, UINT64_MAX, "[RS] ")
  {}
  virtual ~ObRSCompactionTimeGuard() {}
  enum CompactionEvent : uint16_t {
    PREPARE_UNFINISH_TABLE_IDS = 0,
    GET_TABLET_LS_PAIRS,
    GET_TABLET_META_TABLE,
    CKM_VERIFICATION,
    COMPACTION_EVENT_MAX,
  };
  virtual int64_t to_string(char *buf, const int64_t buf_len) const override;
private:
  const static char *CompactionEventStr[];
  static const char *get_comp_event_str(const enum CompactionEvent event);
};
struct ObCompactionScheduleTimeGuard : public ObCompactionTimeGuard
{
public:
  ObCompactionScheduleTimeGuard()
    : ObCompactionTimeGuard(SCHEDULE_COMPACT_TIME_GUARD, UINT64_MAX, "[STORAGE] ")
  {}
  virtual ~ObCompactionScheduleTimeGuard() {}
  enum CompactionEvent : uint16_t {
    // medium scheduler
    GET_TABLET = 0,
    UPDATE_TABLET_REPORT_STATUS,
    READ_MEDIUM_INFO,
    SCHEDULE_NEXT_MEDIUM,
    SCHEDULE_TABLET_MEDIUM,
    FAST_FREEZE,
    // medium checker
    SEARCH_META_TABLE,
    CHECK_META_TABLE,
    SEARCH_CHECKSUM,
    CHECK_CHECKSUM,
    SCHEDULER_NEXT_ROUND,
    COMPACTION_EVENT_MAX
  };
  virtual int64_t to_string(char *buf, const int64_t buf_len) const override;
private:
  const static char *CompactionEventStr[];
  static const char *get_comp_event_str(const enum CompactionEvent event);
};

struct ObStorageCompactionTimeGuard : public ObCompactionTimeGuard
{
public:
  ObStorageCompactionTimeGuard()
    : ObCompactionTimeGuard(STORAGE_COMPACT_TIME_GUARD, COMPACTION_WARN_THRESHOLD_RATIO, "[STORAGE] ")
  {}
  virtual ~ObStorageCompactionTimeGuard() {}
  enum CompactionEvent : uint16_t {
    DAG_WAIT_TO_SCHEDULE = 0,
    COMPACTION_POLICY,
    PRE_PROCESS_TX_TABLE,
    GET_PARALLEL_RANGE,
    EXECUTE,
    CREATE_SSTABLE,
    UPDATE_UPPER_TRANS,
    UPDATE_TABLET,
    RELEASE_MEMTABLE,
    SCHEDULE_OTHER_COMPACTION,
    DAG_FINISH,
    COMPACTION_EVENT_MAX
  };
  virtual int64_t to_string(char *buf, const int64_t buf_len) const override;
private:
  const static char *CompactionEventStr[];
  static const char *get_comp_event_str(const enum CompactionEvent event);
  static const int64_t COMPACTION_WARN_THRESHOLD_RATIO = 60 * 1000L * 1000L; // 1 min
  static constexpr float COMPACTION_SHOW_PERCENT_THRESHOLD = 0.1;
  static const int64_t COMPACTION_SHOW_TIME_THRESHOLD = 1 * 1000L * 1000L; // 1s
};


} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_COMPACTION_TIME_GUARD_H_
