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

#define USING_LOG_PREFIX COMMON

#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/stat/ob_diagnostic_info_container.h"
#undef private
#undef protected

namespace oceanbase
{
namespace common
{

TEST(ObDiagnosticInfo, obj)
{
  int ret = OB_SUCCESS;
  ObWaitEventPool wait_event_pool(1, true, lib::is_mini_mode(), get_cpu_count());
  wait_event_pool.init();
  ObDiagnosticInfo test;
  ASSERT_EQ(test.init(1, 1, 1, wait_event_pool), OB_SUCCESS);

  ObWaitEventStat *cur = nullptr;
  for (int i = 1; i < ObWaitEventIds::WAIT_EVENT_DEF_END; i++) {
    ASSERT_EQ(test.get_event_stats().get(
                  static_cast<oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum>(i), cur),
        OB_ITEM_NOT_SETTED);
    ASSERT_EQ(nullptr, cur);
  }
  for (int i = 1; i < ObWaitEventIds::WAIT_EVENT_DEF_END; i++) {
    ASSERT_EQ(test.get_event_stats().get_and_set(
                  static_cast<oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum>(i), cur),
        OB_SUCCESS);
    ASSERT_NE(nullptr, cur);
    ObWaitEventStat *prev = cur;
    ASSERT_EQ(test.get_event_stats().get_and_set(
                  static_cast<oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum>(i), cur),
        OB_SUCCESS);
    ASSERT_EQ(cur, prev);
    ASSERT_EQ(test.get_event_stats().get(
                  static_cast<oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum>(i), cur),
        OB_SUCCESS);
    ASSERT_EQ(cur, prev);
    if (i <= WAIT_EVENT_LIST_THRESHOLD) {
      ASSERT_EQ(test.get_event_stats().rule_, ObWaitEventRule::LIST);
    } else {
      ASSERT_EQ(test.get_event_stats().rule_, ObWaitEventRule::ARRAY);
    }
    cur->total_timeouts_ = i;
    cur->max_wait_ = i;
    cur->total_waits_ = i;
    cur->total_waits_ = i;
    int cnt = 0;
    auto fn = [&cnt](oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum event_no,
                  const ObWaitEventStat &stat) {
      ++cnt;
      ASSERT_EQ(stat.total_timeouts_, event_no);
      ASSERT_EQ(stat.max_wait_, event_no);
      ASSERT_EQ(stat.total_waits_, event_no);
      ASSERT_EQ(stat.total_waits_, event_no);
    };
    test.get_event_stats().for_each(fn);
    ASSERT_EQ(cnt, i);
  }
  for (int i = 0; i < ObStatEventIds::STAT_EVENT_ADD_END; i++) {
    test.get_add_stat_stats().get(i)->add(i);
  }
}

TEST(ObDiagnosticInfo, summary)
{
  int64_t session_id = 0;
  ObWaitEventPool wait_event_pool(1, true, lib::is_mini_mode(), get_cpu_count());
  wait_event_pool.init();
  ObFixedClassAllocator<ObDiagnosticInfoCollector> di_collector_allocator(lib::ObMemAttr(1, "DICollector"), 1, 4);
  DiagnosticInfoValueAlloc<ObDiagnosticInfoCollector, ObDiagnosticKey> value_alloc(
            &di_collector_allocator);
  ObBaseDiagnosticInfoSummary sum(value_alloc);
  int64_t session_sum = 0, wait_time_sum = 0;
  ASSERT_EQ(sum.init(4), OB_SUCCESS);
  int slot_cnt = 0;
  for (int tenant_id = 1; tenant_id <= 20; tenant_id++) {
    for (int group_id = 1; group_id <= 20; group_id++) {
      ++slot_cnt;
      for (int i = 0; i < 10; i++) {
        ObDiagnosticInfo cur;
        ASSERT_EQ(cur.init(tenant_id, group_id, session_id++, wait_event_pool), OB_SUCCESS);

        for (int i = 1; i <= tenant_id; i++) {
          ObWaitEventStat *wait_event = nullptr;
          ASSERT_EQ(
              cur.get_event_stats().get_and_set(
                  static_cast<oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum>(i), wait_event),
              OB_SUCCESS);
          ASSERT_NE(nullptr, wait_event);
          if (i <= WAIT_EVENT_LIST_THRESHOLD) {
            ASSERT_EQ(cur.get_event_stats().rule_, ObWaitEventRule::LIST);
          } else {
            ASSERT_EQ(cur.get_event_stats().rule_, ObWaitEventRule::ARRAY);
          }
          wait_event->total_waits_ = session_id;
          wait_time_sum += session_id;
          ASSERT_EQ(
              cur.get_event_stats().get_and_set(
                  static_cast<oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum>(i), wait_event),
              OB_SUCCESS);
          ASSERT_EQ(wait_event->total_waits_, session_id);
        }
        for (int i = 0; i < tenant_id; i++) {
          cur.get_add_stat_stats().get(i)->add(session_id);
          session_sum += session_id;
        }
        ASSERT_EQ(sum.add_diagnostic_info(cur), OB_SUCCESS);
      }
    }
  }

  // verify summary info
  ObWaitEventStatArray events;
  ObStatEventAddStatArray stats;
  ObDiagnosticInfoCollector *cur = nullptr;
  ObBaseDiagnosticInfoSummary::SummaryMap::Iterator iter(sum.collectors_);
  int summary_collect_count = 0;
  while (OB_NOT_NULL(iter.next(cur))) {
    cur->get_all_add_stats(stats);
    cur->get_all_events(events);
    ++summary_collect_count;
  }
  ASSERT_EQ(summary_collect_count, slot_cnt);
  int64_t verified_event_sum = 0;
  int64_t verified_stat_sum = 0;
  for (int i = 1; i < WAIT_EVENTS_TOTAL; i++) {
    verified_event_sum += events.get(i)->total_waits_;
  }
  for (int i = 0; i < ObStatEventIds::STAT_EVENT_ADD_END; i++) {
    verified_stat_sum += stats.get(i)->get_stat_value();
  }
  ASSERT_EQ(verified_stat_sum, session_sum);
  ASSERT_EQ(verified_event_sum, wait_time_sum);
}

TEST(ObDiagnosticInfo, container)
{
  for (int cpu_cnt = 1; cpu_cnt < 128; cpu_cnt++) {
    ObDiagnosticInfoContainer container(cpu_cnt);
    ASSERT_EQ(container.init(cpu_cnt), OB_SUCCESS);
    ObArray<ObDiagnosticInfo *> arr;
    ObDiagnosticInfo *cur = nullptr;
    int64_t session_id = 0;
    int cnt = 0;
    for (int group_id = 0; group_id < 10; group_id++) {
      for (int i = 0; i < 20; i++) {
        ASSERT_EQ(
            OB_SUCCESS, container.acquire_diagnostic_info(cpu_cnt, group_id, session_id++, cur));
        ASSERT_EQ(arr.push_back(cur), OB_SUCCESS);
      }
    }
    ASSERT_EQ(session_id, arr.size());

    for (int group_id = 0; group_id < 10; group_id++) {
      for (int i = 0; i < 20; i++) {
        ASSERT_EQ(arr.pop_back(cur), OB_SUCCESS);
        ASSERT_EQ(OB_SUCCESS, container.return_diagnostic_info(cur));
      }
    }
    int remains = 0;
    auto fn = [&remains](const ObDiagnosticInfo &) {++remains;};
    ASSERT_EQ(remains, 0);
  }
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
