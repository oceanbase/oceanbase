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

namespace oceanbase
{
namespace common
{
static const int16_t SESSION_WAIT_HISTORY_CNT = 10;
typedef ObStatArray<ObWaitEventStat, WAIT_EVENTS_TOTAL> ObWaitEventStatArray;
typedef ObStatArray<ObStatEventAddStat, ObStatEventIds::STAT_EVENT_ADD_END> ObStatEventAddStatArray;
typedef ObStatArray<ObStatEventSetStat, ObStatEventIds::STAT_EVENT_SET_END - ObStatEventIds::STAT_EVENT_ADD_END -1> ObStatEventSetStatArray;

struct ObLatchStat
{
  ObLatchStat();
  int add(const ObLatchStat &other);
  void reset();
  uint64_t addr_;
  uint64_t id_;
  uint64_t level_;
  uint64_t hash_;
  uint64_t gets_;
  uint64_t misses_;
  uint64_t sleeps_;
  uint64_t immediate_gets_;
  uint64_t immediate_misses_;
  uint64_t spin_gets_;
  uint64_t wait_time_;
};

struct ObLatchStatArray
{
public:
  ObLatchStatArray(ObIAllocator *allocator = NULL);
  ~ObLatchStatArray();
  int add(const ObLatchStatArray &other);
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
private:
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
  int notify_wait_begin(
      const int64_t event_no,
      const uint64_t timeout_ms = 0,
      const uint64_t p1 = 0,
      const uint64_t p2 = 0,
      const uint64_t p3 = 0,
      const bool is_atomic = false);
  int notify_wait_end(ObDiagnoseTenantInfo *tenant_info, const bool is_atomic = false);
  int set_max_wait(ObWaitEventDesc *max_wait);
  int set_total_wait(ObWaitEventStat *total_wait);
  ObWaitEventDesc &get_curr_wait();
  int inc_stat(const int16_t stat_no);
  int update_stat(const int16_t stat_no, const int64_t delta);
  static ObDiagnoseSessionInfo *get_local_diagnose_info();
  inline ObWaitEventHistory &get_event_history()  { return event_history_; }
  inline ObWaitEventStatArray &get_event_stats()  { return event_stats_; }
  inline ObStatEventAddStatArray &get_add_stat_stats()  { return stat_add_stats_; }
  inline void reset_max_wait() { max_wait_ = NULL; }
  inline void reset_total_wait() { total_wait_ = NULL; }
  inline ObWaitEventDesc *get_max_wait() { return max_wait_; }
  inline ObWaitEventStat *get_total_wait() { return total_wait_; }
  inline bool is_valid() const { return tenant_id_ < UINT32_MAX; }
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
  static ObDiagnoseTenantInfo *get_local_diagnose_info();
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
  uint64_t wait_begin_time_;
  uint64_t timeout_ms_;
  ObDiagnoseSessionInfo *di_;
  bool is_atomic_;
  //Do you need statistics
  bool need_record_;
};

class ObSleepEventGuard : public ObWaitEventGuard
{
public:
  explicit ObSleepEventGuard(
      const int64_t event_no,
      const uint64_t timeout_ms,
      const int64_t sleep_us
  ) : ObWaitEventGuard(event_no, timeout_ms, sleep_us, 0, 0, false)
  {
    lib::Thread::sleep_us_ = sleep_us;
  }
  explicit ObSleepEventGuard(
    const int64_t sleep_us = 0
  ) : ObWaitEventGuard(ObWaitEventIds::DEFAULT_SLEEP, 0, sleep_us, 0, 0, false)
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
  explicit ObMaxWaitGuard(ObWaitEventDesc *max_wait, ObDiagnoseSessionInfo *di = NULL);
  ~ObMaxWaitGuard();
private:
  ObWaitEventDesc *prev_wait_;
  ObDiagnoseSessionInfo *di_;
  //Do you need statistics
  bool need_record_;
};

class ObTotalWaitGuard
{
public:
  explicit ObTotalWaitGuard(ObWaitEventStat *total_wait, ObDiagnoseSessionInfo *di = NULL);
  ~ObTotalWaitGuard();
private:
  ObWaitEventStat *prev_wait_;
  ObDiagnoseSessionInfo *di_;
  //Do you need statistics
  bool need_record_;
};

} /* namespace common */
} /* namespace oceanbase */


#define EVENT_ADD(stat_no, value)                               \
  do {                                                          \
    if (oceanbase::lib::is_diagnose_info_enabled()) {              \
      if (oceanbase::common::OB_STAT_EVENTS[::oceanbase::common::ObStatEventIds::stat_no].summary_in_session_) {  \
        oceanbase::common::ObDiagnoseSessionInfo *session_info            \
        = oceanbase::common::ObDiagnoseSessionInfo::get_local_diagnose_info();   \
        if (NULL != session_info) {                                \
          session_info->update_stat(::oceanbase::common::ObStatEventIds::stat_no, value); \
        }                                                           \
      }                                      \
      oceanbase::common::ObDiagnoseTenantInfo *tenant_info            \
      = oceanbase::common::ObDiagnoseTenantInfo::get_local_diagnose_info();   \
      if (NULL != tenant_info) {                                \
        tenant_info->update_stat(::oceanbase::common::ObStatEventIds::stat_no, value);  \
      }                              \
    }                              \
  } while(0)

#define EVENT_TENANT_ADD(stat_no, value, tenant_id)    \
  oceanbase::common::ObTenantStatEstGuard tenant_guard(tenant_id); \
  EVENT_ADD(stat_no, value);

#define EVENT_INC(stat_no) EVENT_ADD(stat_no, 1)

#define EVENT_TENANT_INC(stat_no, tenant_id) EVENT_TENANT_ADD(stat_no, 1, tenant_id)

#define EVENT_DEC(stat_no) EVENT_ADD(stat_no, -1)

#define EVENT_SET(stat_no, value)                               \
  do {                                                          \
    if (oceanbase::lib::is_diagnose_info_enabled()) {              \
      oceanbase::common::ObDiagnoseTenantInfo *diagnose_info            \
      = oceanbase::common::ObDiagnoseTenantInfo::get_local_diagnose_info();   \
      if (NULL != diagnose_info) {                                \
        diagnose_info->set_stat(::oceanbase::common::ObStatEventIds::stat_no, value);  \
      }                                                           \
    }                                                           \
  } while(0)

#define WAIT_EVENT_GET(stat_no)                                      \
  ({                                                            \
    uint64_t ret = 0;                                        \
    if (oceanbase::lib::is_diagnose_info_enabled()) {              \
      oceanbase::common::ObDiagnoseSessionInfo *session_info            \
        = oceanbase::common::ObDiagnoseSessionInfo::get_local_diagnose_info();   \
      if (NULL != session_info) {                                \
        oceanbase::common::ObWaitEventStat *stat                  \
            = session_info->get_event_stats().get(                \
                ::oceanbase::common::ObWaitEventIds::stat_no);    \
        if (NULL != stat) {                                       \
          ret = stat->time_waited_;                           \
        }                       \
      }                                                           \
    }                                                           \
    ret;                                                        \
  })

#define EVENT_GET(stat_no, session_info)                                      \
  ({                                                            \
    int64_t ret = 0;                                            \
    if (oceanbase::lib::is_diagnose_info_enabled()) {              \
      if (OB_LIKELY(oceanbase::common::stat_no < oceanbase::common::ObStatEventIds::STAT_EVENT_ADD_END)) {    \
        oceanbase::common::ObStatEventAddStat *stat                  \
            = session_info->get_add_stat_stats().get(::oceanbase::common::stat_no);    \
        if (OB_LIKELY(NULL != stat)) {                                       \
          ret = stat->get_stat_value();                           \
        }                                                         \
      }                                                         \
    }                                                         \
    ret;                                                        \
  })

#define WAIT_BEGIN(stat_no, ...)                                \
  do {                                                          \
    if (oceanbase::lib::is_diagnose_info_enabled()) {              \
      oceanbase::common::ObDiagnoseSessionInfo *di                       \
      = oceanbase::common::ObDiagnoseSessionInfo::get_local_diagnose_info();   \
      if (di) {                                                   \
        di->notify_wait_begin(                                    \
            oceanbase::common::ObWaitEventIds::stat_no, ## __VA_ARGS__);             \
      }                                                           \
    }                                                           \
  } while (0)


#define WAIT_END(stat_no)                           \
  do {                                                          \
    if (oceanbase::lib::is_diagnose_info_enabled()) {              \
      oceanbase::common::ObDiagnoseSessionInfo *di                       \
      = oceanbase::common::ObDiagnoseSessionInfo::get_local_diagnose_info();   \
      oceanbase::common::ObDiagnoseTenantInfo *tenant_di                       \
      = oceanbase::common::ObDiagnoseTenantInfo::get_local_diagnose_info();   \
      if (NULL != di && NULL != tenant_di) {                                                   \
        di->notify_wait_end(tenant_di);                          \
      }                                                           \
    }                                                           \
  } while (0)

#define SLEEP(time)                           \
  do {                                                          \
    oceanbase::common::ObSleepEventGuard wait_guard(((int64_t)time) * 1000 * 1000);    \
    ::sleep(time);                                                      \
  } while (0)

#define USLEEP(time)                           \
  do {                                                          \
    oceanbase::common::ObSleepEventGuard wait_guard((int64_t)time);    \
    ::usleep(time);                                         \
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
