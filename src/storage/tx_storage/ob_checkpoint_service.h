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

#ifndef OCEABASE_STORAGE_OB_CHECKPOINT_SERVICE_
#define OCEABASE_STORAGE_OB_CHECKPOINT_SERVICE_
#include "storage/tx_storage/ob_ls_freeze_thread.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{
class ObDataCheckpoint;

class ObCheckPointService
{
public:
  ObCheckPointService()
    : is_inited_(false),
      checkpoint_timer_(),
      traversal_flush_timer_(),
      check_clog_disk_usage_timer_(),
      checkpoint_task_(),
      traversal_flush_task_(),
      check_clog_disk_usage_task_(*this)
  {}

  static const int64_t NEED_FLUSH_CLOG_DISK_PERCENT = 30;
  static int mtl_init(ObCheckPointService *&m);
  int init(const int64_t tenant_id);
  int start();
  int stop();
  void wait();
  void destroy();
  // add ls checkpoint freeze task
  // @param [in] data_checkpoint, the data_checkpoint of this task.
  // @param [in] rec_scn, freeze all the memtable whose rec_scn is
  // smaller than this one.
  int add_ls_freeze_task(
      ObDataCheckpoint *data_checkpoint,
      share::SCN rec_scn);
private:
  bool is_inited_;

  // the thread which is used to deal with checkpoint task.
  ObLSFreezeThread freeze_thread_;

  bool get_disk_usage_threshold_(int64_t &threshold);
  bool cannot_recycle_log_over_threshold_(const int64_t threshold, const bool need_update_checkpoint_scn);
  int flush_if_need_(bool need_flush);
  // reduce the risk of clog full due to checkpoint long interval
  static int64_t CHECK_CLOG_USAGE_INTERVAL;
  static int64_t CHECKPOINT_INTERVAL;
  static int64_t TRAVERSAL_FLUSH_INTERVAL;
  class ObCheckpointTask : public common::ObTimerTask
  {
  public:
    ObCheckpointTask() {}
    virtual ~ObCheckpointTask() {}

    virtual void runTimerTask();
  };

  class ObTraversalFlushTask : public common::ObTimerTask
  {
  public:
    ObTraversalFlushTask() {}
    virtual ~ObTraversalFlushTask() {}
    virtual void runTimerTask();
  };

  class ObCheckClogDiskUsageTask : public common::ObTimerTask
  {
  public:
    ObCheckClogDiskUsageTask(ObCheckPointService &checkpoint_service)
      : checkpoint_service_(checkpoint_service)
    {}
    virtual ~ObCheckClogDiskUsageTask() {}
    virtual void runTimerTask();
    private:
      ObCheckPointService &checkpoint_service_;
  };

  common::ObTimer checkpoint_timer_;
  common::ObTimer traversal_flush_timer_;
  common::ObTimer check_clog_disk_usage_timer_;
  ObCheckpointTask checkpoint_task_;
  ObTraversalFlushTask traversal_flush_task_;
  ObCheckClogDiskUsageTask check_clog_disk_usage_task_;
};

} // checkpoint
} // storage
} // oceanbase

#endif
