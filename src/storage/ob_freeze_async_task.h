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

#ifndef OCEANBASE_STORAGE_FREEZE_ASYNC_WORKER_
#define OCEANBASE_STORAGE_FREEZE_ASYNC_WORKER_

#include "lib/task/ob_timer.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;

class ObFreezeAsyncWorker {
public:
  ObFreezeAsyncWorker() : inited_(false), async_task_(), timer_()
  {}
  ~ObFreezeAsyncWorker()
  {}

  int init(ObPartitionService* partition_service);
  int start();
  void wait();
  void stop();
  void before_minor_freeze();
  void after_minor_freeze();
  void destroy();

  ObFreezeAsyncWorker(const ObFreezeAsyncWorker&) = delete;
  ObFreezeAsyncWorker& operator=(const ObFreezeAsyncWorker&) = delete;

private:
  class ObFreezeAsyncTask : public common::ObTimerTask {
  public:
    ObFreezeAsyncTask()
        : inited_(false),
          partition_service_(NULL),
          in_freezing_(0),
          in_marking_dirty_(false),
          needed_round_after_freeze_(0),
          has_error_(false),
          all_cleared_(true),
          latch_()
    {}
    virtual ~ObFreezeAsyncTask()
    {}

    int init(ObPartitionService* partition_service);
    void before_minor_freeze();
    void after_minor_freeze();
    void before_marking_dirty_();
    void after_marking_dirty_();
    void fetch_task_with_lock_(int& in_freezing, int& needed_round);
    virtual void runTimerTask() override;

  private:
    bool inited_;
    ObPartitionService* partition_service_;
    int in_freezing_;
    bool in_marking_dirty_;
    int needed_round_after_freeze_;
    bool has_error_;
    bool all_cleared_;
    common::ObLatch latch_;
  };

  static const int64_t EXEC_INTERVAL = 1L * 1000L * 1000L;  // 1s

  bool inited_;
  ObFreezeAsyncTask async_task_;
  common::ObTimer timer_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_FREEZE_ASYNC_WORKER_ */
