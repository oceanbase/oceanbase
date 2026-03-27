/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_SERVER_OB_STARTUP_ACCEL_TASK_HANDLER_H_
#define OCEABASE_SERVER_OB_STARTUP_ACCEL_TASK_HANDLER_H_

#include "lib/ob_define.h"
#include "lib/thread/thread_mgr.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/allocator/ob_fifo_allocator.h"

namespace oceanbase
{
namespace observer
{
class ObStartupAccelTask
{
public:
  ObStartupAccelTask() {}
  virtual ~ObStartupAccelTask() {}
  virtual int execute() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

enum ObStartupAccelType
{
  SERVER_ACCEL = 1,
  TENANT_ACCEL = 2,
};

class ObStartupAccelTaskHandler : public lib::TGTaskHandler
{
public:
  static const int64_t MAX_QUEUED_TASK_NUM;
  static const int64_t MAX_THREAD_NUM;

  ObStartupAccelTaskHandler();
  ~ObStartupAccelTaskHandler();
  int init(ObStartupAccelType accel_type);
  int start();
  void stop();
  void wait();
  void destroy();
  void handle(void *task) override;
  ObIAllocator &get_task_allocator() { return task_allocator_; }
  int push_task(ObStartupAccelTask *task);
  int64_t get_thread_cnt();

private:
  bool is_inited_;
  ObStartupAccelType accel_type_;
  int tg_id_;
  common::ObFIFOAllocator task_allocator_;
};

} // observer
} // oceanbase

#endif
