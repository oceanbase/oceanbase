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

#ifndef SRC_SHARE_SCHEDULER_OB_DAG_WORKER_H_
#define SRC_SHARE_SCHEDULER_OB_DAG_WORKER_H_

#include "lib/lock/ob_thread_cond.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase {
namespace share {
class ObITaskNew;

// thread worker
class ObDagWorkerNew : public lib::ThreadPool, public common::ObDLinkBase<ObDagWorkerNew> {
public:
  enum SwitchFlagType {
    SF_INIT = 0,
    SF_CONTINUE = 1,
    SF_SWITCH = 2,
    SF_PAUSE = 3,
  };

public:
  ObDagWorkerNew();
  virtual ~ObDagWorkerNew();
  int init();
  void reset();
  void stop_worker();
  void resume();
  void run1() override;
  static void yield(void* ptr);
  void set_task(ObITaskNew* task);
  ObITaskNew* get_task();
  int64_t get_switch_flag();
  void set_switch_flag(int64_t switch_flag);
  void notify();

private:
  int check_flag();
  int switch_run_permission();
  int pause_task();

private:
  static const uint32_t SLEEP_TIME_MS = 100;  // 100ms
private:
  bool is_inited_;
  int64_t switch_flag_;
  common::ObThreadCond cond_;
  ObITaskNew* task_;
};

}  // namespace share
}  // namespace oceanbase

#endif
