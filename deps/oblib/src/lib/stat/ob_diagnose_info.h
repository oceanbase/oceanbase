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

#ifndef OB_DIAGNOSE_INFO_H_
#define OB_DIAGNOSE_INFO_H_

#include "lib/wait_event/ob_wait_event.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_stat_template.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_lib_config.h"
#include "lib/thread/thread.h"
// #include "lib/stat/ob_diagnostic_info_guard.h"

namespace oceanbase
{
namespace common
{
static const int16_t SESSION_WAIT_HISTORY_CNT = 10;
typedef ObStatArray<ObWaitEventStat, WAIT_EVENTS_TOTAL> ObWaitEventStatArray;
typedef ObStatArray<ObStatEventAddStat, ObStatEventIds::STAT_EVENT_ADD_END> ObStatEventAddStatArray;
typedef ObStatArray<ObStatEventSetStat, ObStatEventIds::STAT_EVENT_SET_END - ObStatEventIds::STAT_EVENT_ADD_END -1> ObStatEventSetStatArray;

class ObDiagnosticInfo;

struct ObLatchStat
{
  ObLatchStat();
  int add(const ObLatchStat &other);
  void reset();
  // uint64_t addr_;
  // uint64_t id_;
  // uint64_t level_;
  // uint64_t hash_;
  uint64_t gets_;
  uint64_t misses_;
  uint64_t sleeps_;
  uint64_t immediate_gets_;
  uint64_t immediate_misses_;
  uint64_t spin_gets_;
  uint64_t wait_time_;
};

typedef ObStatArray<ObLatchStat, ObLatchIds::LATCH_END> ObStatLatchArray;

struct ObLatchStatArray
{
public:
  ObLatchStatArray(ObIAllocator *allocator = NULL);
  ~ObLatchStatArray();
  int add(const ObLatchStatArray &other);
  int add(ObStatLatchArray &other)
  {
    int ret = OB_SUCCESS;
    ObLatchStat *cur = nullptr;
    for (int i = 0; i < ObLatchIds::LATCH_END; i++) {
      cur = other.get(i);
      if (cur->gets_ || cur->spin_gets_) {
        ObLatchStat *target = get_or_create_item(i);
        if (OB_NOT_NULL(target)) {
          ret = target->add(*cur);
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
      }
    }
    return ret;
  }
  void reset();
  ObLatchStat *get_item(int32_t idx) const
  {
    return items_[idx];
  }
  ObLatchStat *get_or_create_item(int32_t idx)
  {
    if (OB_ISNULL(items_[idx])) {
      items_[idx] = create_item();
    }
    return items_[idx];
  }
  void accumulate_to(ObStatLatchArray &array)
  {
    for (int64_t i = 0; i < ObLatchIds::LATCH_END; ++i) {
      if (OB_ISNULL(items_[i])) {
      } else {
        array.get(i)->add(*items_[i]);
      }
    }
  }

private:
  ObLatchStat *create_item();
  void free_item(ObLatchStat *stat);
private:
  ObIAllocator *allocator_;
  ObLatchStat *items_[ObLatchIds::LATCH_END] = {NULL};
};

class ObWaitEventHistoryIter
{
public:
  ObWaitEventHistoryIter();
  virtual ~ObWaitEventHistoryIter();
  int init(ObWaitEventDesc *items, const int64_t start_pos, int64_t item_cnt);
  int get_next(ObWaitEventDesc *&item);
  void reset();
private:
  ObWaitEventDesc *items_;
  int64_t curr_;
  int64_t start_pos_;
  int64_t item_cnt_;
};

class ObWaitEventHistory
{
public:
  ObWaitEventHistory();
  virtual ~ObWaitEventHistory();
  int push(const int64_t event_no, const uint64_t timeout_ms, const uint64_t p1, const uint64_t p2, const uint64_t p3);
  int add(const ObWaitEventHistory &other);
  int get_iter(ObWaitEventHistoryIter &iter);
  int get_last_wait(ObWaitEventDesc *&item);
  int get_curr_wait(ObWaitEventDesc *&item);
  int get_accord_event(ObWaitEventDesc *&event_desc);
  int calc_wait_time(ObWaitEventDesc *&event_desc);
  void reset();
  int get_next_and_compare(int64_t &iter_1, int64_t &iter_2, int64_t &cnt, const ObWaitEventHistory &other, ObWaitEventDesc *tmp);
  int64_t curr_pos_;
  int64_t item_cnt_;
  int64_t nest_cnt_;
  int64_t current_wait_;
  ObWaitEventDesc items_[SESSION_WAIT_HISTORY_CNT];
};

class ObDiagnoseTenantInfo;
class ObDiagnoseSessionInfo
{
public:
  ObDiagnoseSessionInfo();
  virtual ~ObDiagnoseSessionInfo();
  int add(ObDiagnoseSessionInfo &other);
  void reset();
  int set_max_wait(ObWaitEventDesc *max_wait)
  {
    max_wait_ = max_wait;
    return OB_SUCCESS;
  }

  int set_total_wait(ObWaitEventStat *total_wait)
  {
    total_wait_ = total_wait;
    return OB_SUCCESS;
  }
  ObWaitEventDesc &get_curr_wait();
  int inc_stat(const int16_t stat_no);
  int update_stat(const int16_t stat_no, const int64_t delta);
  inline ObWaitEventHistory &get_event_history()  { return event_history_; }
  inline ObWaitEventStatArray &get_event_stats()  { return event_stats_; }
  inline ObStatEventAddStatArray &get_add_stat_stats()  { return stat_add_stats_; }
  inline void reset_max_wait() { max_wait_ = NULL; }
  inline void reset_total_wait() { total_wait_ = NULL; }
  inline ObWaitEventDesc *get_max_wait() { return max_wait_; }
  inline ObWaitEventStat *get_total_wait() { return total_wait_; }
  inline bool is_valid() const { return tenant_id_ < UINT32_MAX; }
  const ObWaitEventDesc &get_curr_wait() const
  {
    return curr_wait_;
  };
  void set_curr_wait(ObWaitEventDesc &wait)
  {
    curr_wait_ = wait;
  };
  int set_tenant_id(uint64_t tenant_id);
  inline uint64_t get_tenant_id() { return tenant_id_; }
  TO_STRING_EMPTY();
private:
  ObWaitEventDesc curr_wait_;
  ObWaitEventDesc *max_wait_;
  ObWaitEventStat *total_wait_;
  ObWaitEventHistory event_history_;
  ObWaitEventStatArray event_stats_;
  ObStatEventAddStatArray stat_add_stats_;
  uint64_t tenant_id_;
  DIRWLock lock_;
};

class ObDiagnoseTenantInfo final
{
public:
  ObDiagnoseTenantInfo(ObIAllocator *allocator = NULL);
  ~ObDiagnoseTenantInfo();
  void add(const ObDiagnoseTenantInfo &other);
  void add_wait_event(const ObDiagnoseTenantInfo &other);
  void add_stat_event(const ObDiagnoseTenantInfo &other);
  void add_latch_stat(const ObDiagnoseTenantInfo &other);
  void reset();
  int inc_stat(const int16_t stat_no);
  int update_stat(const int16_t stat_no, const int64_t delta);
  int set_stat(const int16_t stat_no, const int64_t value);
  int get_stat(const int16_t stat_no, int64_t &value);
  inline ObWaitEventStatArray &get_event_stats()  { return event_stats_; }
  inline ObStatEventAddStatArray &get_add_stat_stats()  { return stat_add_stats_; }
  inline ObStatEventSetStatArray &get_set_stat_stats()  { return stat_set_stats_; }
  inline ObLatchStatArray &get_latch_stats() { return latch_stats_; }
  TO_STRING_EMPTY();
private:
  ObWaitEventStatArray event_stats_;
  ObStatEventAddStatArray stat_add_stats_;
  ObStatEventSetStatArray stat_set_stats_;
  ObLatchStatArray latch_stats_;
};

class ObWaitEventGuard
{
public:
  explicit ObWaitEventGuard(
    const int64_t event_no,
    const uint64_t timeout_ms = 0,
    const int64_t p1 = 0,
    const int64_t p2 = 0,
    const int64_t p3 = 0,
    const bool is_atomic = false);
  ~ObWaitEventGuard();
private:
  int64_t event_no_;
  ObDiagnosticInfo *di_;
  bool is_atomic_;
  //Do you need statistics
  bool need_record_;
};

template<ObWaitEventIds::ObWaitEventIdEnum EVENT_ID>
class ObSleepEventGuard : public ObWaitEventGuard
{
public:
  ObSleepEventGuard(
      const int64_t sleep_us,
      const int64_t p2, //caller bt
      const uint64_t timeout_ms = 0
  ) : ObWaitEventGuard(EVENT_ID, timeout_ms, sleep_us, p2, 0)
  {
    lib::Thread::sleep_us_ = sleep_us;
  }
  ObSleepEventGuard(
      const int64_t sleep_us,
      const int64_t p1,
      const int64_t p2,
      const int64_t p3,
      const uint64_t timeout_ms = 0
  ) : ObWaitEventGuard(EVENT_ID, timeout_ms, p1, p2, p3)
  {
    lib::Thread::sleep_us_ = sleep_us;
  }
  ObSleepEventGuard(
      const int64_t event_no,
      const int64_t sleep_us,
      const int64_t p1,
      const int64_t p2,
      const int64_t p3,
      const uint64_t timeout_ms = 0
  ) : ObWaitEventGuard(event_no, timeout_ms, p1, p2, p3)
  {
    lib::Thread::sleep_us_ = sleep_us;
  }
  ~ObSleepEventGuard()
  {
    lib::Thread::sleep_us_ = 0;
  }
};

class ObMaxWaitGuard
{
public:
  explicit ObMaxWaitGuard(ObWaitEventDesc *max_wait);
  ~ObMaxWaitGuard();
  TO_STRING_KV(K_(need_record), K_(max_wait));
private:
  bool need_record_;
  ObWaitEventDesc *max_wait_;
  ObDiagnosticInfo *di_;
};

class ObTotalWaitGuard
{
public:
  explicit ObTotalWaitGuard(ObWaitEventStat *total_wait);
  ~ObTotalWaitGuard();
private:
  ObWaitEventStat *total_wait_;
  ObDiagnosticInfo *di_;
};

extern int64_t get_rel_offset(int64_t addr);
} /* namespace common */
} /* namespace oceanbase */

#define SLEEP(time)                                                                                         \
  do {                                                                                                      \
    oceanbase::common::ObSleepEventGuard<oceanbase::common::ObWaitEventIds::DEFAULT_SLEEP>                  \
        wait_guard(((int64_t)time) * 1000 * 1000, get_rel_offset((int64_t)__builtin_frame_address(0)));     \
    ::sleep(time);                                                                                          \
  } while (0)
#define USLEEP(time)                                                                                        \
  do {                                                                                                      \
    oceanbase::common::ObSleepEventGuard<oceanbase::common::ObWaitEventIds::DEFAULT_SLEEP>                  \
        wait_guard((int64_t)time, get_rel_offset((int64_t)__builtin_frame_address(0)));                     \
    ::usleep(time);                                                                                         \
  } while (0)

#define GLOBAL_EVENT_GET(stat_no)             \
  ({                                                \
      int64_t ret = 0;                              \
      ObStatEventAddStat *stat = tenant_info->get_add_stat_stats().get(stat_no); \
      if (NULL != stat) {         \
        ret = stat->stat_value_;   \
      }   \
      ret; \
   })

#endif /* OB_DIAGNOSE_INFO_H_ */
