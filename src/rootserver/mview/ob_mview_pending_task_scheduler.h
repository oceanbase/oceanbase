/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/lock/ob_thread_cond.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewPendingTaskManager;
} // namespace rootserver
} // namespace oceanbase

namespace oceanbase
{
namespace rootserver
{

class ObMviewPendingTaskScheduler : public lib::TGRunnable
{
public:
  ObMviewPendingTaskScheduler();
  ~ObMviewPendingTaskScheduler();
  DISALLOW_COPY_AND_ASSIGN(ObMviewPendingTaskScheduler);

  int init(ObMViewPendingTaskManager &manager);
  int start();
  void stop();
  void wait();
  void destroy();
  void wakeup();
  void run1() override;

private:
  int process_one_task();
  int check_concurrent_limit(uint64_t tenant_id) const;
  void idle_wait();

private:
  int tg_id_;
  common::ObThreadCond idle_cond_;
  bool has_pending_work_;
  ObMViewPendingTaskManager *manager_;
  bool is_started_;
  bool is_inited_;
};

} // namespace rootserver
} // namespace oceanbase
