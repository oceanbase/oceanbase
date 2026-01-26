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

#ifndef OCEANBASE_SRC_SHARE_COMPACTION_OB_COMPACTION_RESOURCE_MANAGER_H_
#define OCEANBASE_SRC_SHARE_COMPACTION_OB_COMPACTION_RESOURCE_MANAGER_H_

#include "share/compaction/ob_schedule_daily_maintenance_window.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "rootserver/freeze/window/ob_window_compaction_helper.h"

namespace oceanbase
{
namespace share
{

/**
 *
 *  ┌─────────────────────────────────────────────────────────┐
 *  │  Normal Mode                                            │ ←────────────────────────────────────────────┐
 *  │  - job_config {"version": x, "thread_cnt": x}           │                                              |
 *  │  - No window compaction active                          │                                              |
 *  └──────────────────┬──────────────────────────────────────┘                                              |
 *                     │                                                                                     |
 *                     │ check_and_switch(to_window=true)                                                    |
 *                     │ Step 1: SUBMIT_PREV_INFO                                                            | check_and_switch(to_window=false)
 *                     │         (saves current plan & consumer_group in prev_plan & prev_group)             | → ROLLBACK_PREV_INFO
 *                     ↓                                                                                     |   (apply prev plan & consumer_group, clear prev_info)
 *  ┌────────────────────────────────────────────────────────────────────────────────────────┐               |
 *  │  Window Mode Phase1 (Prev_Submitted)                                                   │               |
 *  │  - job_config {"version": x, "thread_cnt": x, "prev_plan": "xxx", "prev_group": "xxx"} |---------------|
 *  └──────────────────┬─────────────────────────────────────────────────────────────────────┘               |
 *                     │ check_and_switch(to_window=true)                                                    |
 *                     │ Step 2: APPLY_WINDOW_PLAN (Optional)                                                |
 *                     │         (apply window_plan & window_consumer_group)                                 |
 *                     ↓                                                                                     |
 *  ┌────────────────────────────────────────────────────────────────────────────────────────┐               |
 *  │  Window Mode Phase2 (Plan_Switched)                                                    │               |
 *  │  - job_config {"version": x, "thread_cnt": x, "prev_plan": "xxx", "prev_group": "xxx"} │               |
 *  │  - Current plan is INNER_DAILY_WINDOW_PLAN                                             │---------------┘
 *  │  - Current consumer group is INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP                   │
 *  └────────────────────────────────────────────────────────────────────────────────────────┘
 */

class ObCompactionResourceManager final
{
public:
  enum ObFinishedStep {
    NONE = 0,
    SUBMIT_PREV_INFO = 1,
    APPLY_WINDOW_PLAN = 2,
    ROLLBACK_PREV_INFO = 3,
    FINISH_STEP_MAX
  };
public:
  ObCompactionResourceManager();
  ~ObCompactionResourceManager() = default;
  int check_and_switch(
      const bool to_window,
      const int64_t merge_start_time_us,
      rootserver::ObWindowResourceCache &resource_cache,
      bool &is_switched);
  int switch_to_normal_before_major_merge(
      const int64_t merge_start_time_us,
      rootserver::ObWindowResourceCache &resource_cache);
  int get_window_compaction_thread_cnt(int64_t &thread_cnt, bool &from_job_config);
public:
  static bool is_window_plan_(const ObString &plan) { return 0 == plan.case_compare(INNER_DAILY_WINDOW_PLAN_NAME); }
  static bool is_window_consumer_group_(const ObString &consumer_group) { return 0 == consumer_group.case_compare(INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP_NAME); }
private:
  bool could_skip_switch_(const bool to_window) const;
  void update_resource_cache_(const bool to_window);
  int check_window_plan_and_consumer_group_exist_();
  int check_window_resources_ready_(ObMySQLTransaction &trans);
  int get_current_resource_manager_plan_(ObSqlString &current_plan);
  int get_current_compaction_low_consumer_group_(
      ObMySQLTransaction &trans,
      ObSqlString &current_consumer_group);
  int switch_compaction_plan_(
      const ObString &current_plan,
      const bool to_window,
      const int64_t merge_start_time_us,
      rootserver::ObWindowResourceCache &resource_cache,
      bool &is_switched);
  int switch_compaction_plan_by_step_(
      const ObString &current_plan,
      const bool to_window,
      const int64_t merge_start_time_us,
      rootserver::ObWindowResourceCache &resource_cache,
      ObFinishedStep &finished_step);
  int submit_prev_info_(
      ObMySQLTransaction &trans,
      ObDailyWindowJobConfig &job_cfg,
      const ObString &current_plan,
      const ObString &current_consumer_group);
  int switch_plan_and_consumer_group_(
      ObMySQLTransaction &trans,
      ObDailyWindowJobConfig &job_cfg,
      const ObString &current_plan,
      const bool to_window);
  int switch_plan_(
      ObMySQLTransaction &trans,
      const ObDailyWindowJobConfig &job_cfg,
      const ObString &current_plan,
      const bool to_window);
  int switch_consumer_group_(
    ObMySQLTransaction &trans,
    const ObDailyWindowJobConfig &job_cfg,
    const bool to_window);
  int calculate_window_compaction_thread_cnt_(
      ObMySQLTransaction &trans,
      int64_t &thread_cnt);
private:
  static const int64_t ADD_RS_EVENT_INTERVAL = 10L * 60 * 1000 * 1000; // 10m
  static const ObString window_plan_;
  static const ObString window_consumer_group_;
  static const ObString attr_name_;
  static const ObString attr_value_;
private:
  uint64_t tenant_id_;
  ObArenaAllocator allocator_;
  share::ObResourceManagerProxy proxy_;
};

} // end of share
} // end of oceanbase

#endif