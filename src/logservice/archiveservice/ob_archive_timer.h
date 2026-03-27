/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_TIMER_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_TIMER_H_

#include "lib/task/ob_timer.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_thread_pool.h"           // ObThreadPool

namespace oceanbase
{
namespace archive
{
class ObLSMetaRecorder;
class ObArchiveRoundMgr;
class ObArchiveTimer : public share::ObThreadPool
{
  static const int64_t THREAD_RUN_INTERVAL = 1000 * 1000L; // 1s
public:
  ObArchiveTimer();
  ~ObArchiveTimer();

  int init(const uint64_t tenant_id, ObLSMetaRecorder *recorder, ObArchiveRoundMgr *round_mgr);
  void destroy();
  int start();
  void wait();
  void stop();

private:
  void run1();
  void do_thread_task_();

private:
  class LSMetaRecordTask
  {
  public:
    explicit LSMetaRecordTask(ObArchiveTimer *timer);
    ~LSMetaRecordTask();

    void handle();
    ObArchiveTimer *timer_;
  };

  friend class LSMetaRecordTask;
private:
  bool inited_;
  uint64_t tenant_id_;
  LSMetaRecordTask record_task_;

  ObLSMetaRecorder *recorder_;
  ObArchiveRoundMgr *round_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveTimer);
};

} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_TIMER_H_ */
