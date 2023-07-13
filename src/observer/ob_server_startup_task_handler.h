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

#ifndef OCEABASE_SERVER_OB_SERVER_STARTUP_TASK_HANDLER_H_
#define OCEABASE_SERVER_OB_SERVER_STARTUP_TASK_HANDLER_H_

#include "lib/ob_define.h"
#include "lib/thread/thread_mgr.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/allocator/ob_fifo_allocator.h"

namespace oceanbase
{
namespace observer
{
class ObServerStartupTask
{
public:
  ObServerStartupTask() {}
  virtual ~ObServerStartupTask() {}
  virtual int execute() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

// This handler is only used to process tasks during startup. it can speed up the startup process.
// If you have tasks that need to be processed in parallel, you can use this handler,
// but please note that this handler will be destroyed after obsever startup.
class ObServerStartupTaskHandler : public lib::TGTaskHandler
{
public:
  static const int64_t MAX_QUEUED_TASK_NUM;
  static const int64_t MAX_THREAD_NUM;
  ObServerStartupTaskHandler();
  ~ObServerStartupTaskHandler();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  void handle(void *task) override;
  ObIAllocator &get_task_allocator() { return task_allocator_; }
  int push_task(ObServerStartupTask *task);
  static int64_t get_thread_num() { return std::min(MAX_THREAD_NUM, common::get_cpu_count()); }
  static OB_INLINE ObServerStartupTaskHandler &get_instance();

private:
  bool is_inited_;
  int tg_id_;
  common::ObFIFOAllocator task_allocator_;
};

OB_INLINE ObServerStartupTaskHandler &ObServerStartupTaskHandler::get_instance()
{
  static ObServerStartupTaskHandler instance;
  return instance;
}

#define SERVER_STARTUP_TASK_HANDLER (::oceanbase::observer::ObServerStartupTaskHandler::get_instance())

} // observer
} // oceanbase

#endif
