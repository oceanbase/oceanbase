//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_COMPACTION_TIMER_TASK_MGR_H_
#define OB_SHARE_COMPACTION_COMPACTION_TIMER_TASK_MGR_H_
#include "deps/oblib/src/lib/task/ob_timer.h"
namespace oceanbase
{
namespace compaction
{
#define DEFINE_TIMER_TASK(TaskName)                                            \
  class TaskName : public common::ObTimerTask {                                \
  public:                                                                      \
    TaskName() = default;                                                      \
    virtual ~TaskName() = default;                                             \
    virtual void runTimerTask() override;                                      \
  };
#define DEFINE_TIMER_TASK_WITHOUT_TIMEOUT_CHECK(TaskName)                      \
  class TaskName : public common::ObTimerTask {                                \
  public:                                                                      \
    TaskName() { disable_timeout_check(); }                                    \
    virtual ~TaskName() = default;                                             \
    virtual void runTimerTask() override;                                      \
  };

#define THREAD_OP(FUNC_NAME, tg_id)                                            \
  if (-1 != tg_id) {                                                           \
    FUNC_NAME(tg_id);                                                          \
  }
#define DESTROY_THREAD(tg_id) \
  THREAD_OP(TG_DESTROY, tg_id) \
  tg_id = -1;
#define STOP_THREAD(tg_id) THREAD_OP(TG_STOP, tg_id)
#define WAIT_THREAD(tg_id) THREAD_OP(TG_WAIT, tg_id)
struct ObCompactionTimerTask
{
  ObCompactionTimerTask() {}
  ~ObCompactionTimerTask() {}
  virtual void destroy() = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  static int restart_schedule_timer_task(
    const int64_t interval,
    const int64_t tg_id,
    common::ObTimerTask &timer_task);
};


} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_COMPACTION_TIMER_TASK_MGR_H_
