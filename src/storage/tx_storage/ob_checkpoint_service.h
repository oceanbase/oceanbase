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
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace storage
{
class ObLS;

namespace checkpoint
{
class ObDataCheckpoint;

#ifdef OB_BUILD_SHARED_STORAGE

struct ScheLSUploadTask {
public:
  ScheLSUploadTask() : ls_handle_() {}
  ~ScheLSUploadTask() {
    ls_handle_.reset();
  }
  void run();

  storage::ObLSHandle &ls_handle() { return ls_handle_; }

private:
  storage::ObLSHandle ls_handle_;
};

class ObSSScheUploadThreadPool : public lib::TGTaskHandler {
public:
  static const int64_t SS_SCHE_UPLOAD_THREAD_NUM = 8;
  static const int64_t SS_SCHE_UPLOAD_MINI_MODE_THREAD_NUM = 4;
public:
  ObSSScheUploadThreadPool() : tg_id_(-1) {}
  virtual ~ObSSScheUploadThreadPool() {}
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void handle(void *task) override;

  int push_task(ScheLSUploadTask *task);
  int get_tg_id() { return tg_id_; }

private:
  int tg_id_;
};

#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
        check_clog_disk_usage_task_(*this),
        prev_ss_advance_ckpt_task_ts_(0),
        ss_sche_upload_thread_pool_(),
        ss_update_ckpt_scn_timer_(),
        ss_update_ckpt_lsn_timer_(),
        ss_advance_ckpt_timer_(),
        ss_schedule_upload_timer_(),
        ss_update_ckpt_scn_task_(),
        ss_update_ckpt_lsn_task_(),
        ss_advance_ckpt_task_(),
        ss_schedule_upload_task_(*this)
#else
        check_clog_disk_usage_task_(*this)
#endif
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
  // reduce the risk of clog full due to checkpoint long interval
  static int64_t CHECK_CLOG_USAGE_INTERVAL;
  static int64_t CHECKPOINT_INTERVAL;
  static int64_t TRAVERSAL_FLUSH_INTERVAL;
  class ObCheckpointTask : public common::ObTimerTask
  {
    virtual void runTimerTask();
  };

  class ObTraversalFlushTask : public common::ObTimerTask
  {
    virtual void runTimerTask();
  };

  class ObCheckClogDiskUsageTask : public common::ObTimerTask
  {
  public:
    ObCheckClogDiskUsageTask(ObCheckPointService &checkpoint_service) : checkpoint_service_(checkpoint_service) {}
    virtual void runTimerTask();

  private:
    ObCheckPointService &checkpoint_service_;
  };

private:
  int flush_to_recycle_clog_();
  int update_ss_checkpoint_scn_();
  int update_ss_checkpoint_lsn_();

private:
  bool is_inited_;

  // the thread which is used to deal with checkpoint task.
  ObLSFreezeThread freeze_thread_;
  common::ObTimer checkpoint_timer_;
  common::ObTimer traversal_flush_timer_;
  common::ObTimer check_clog_disk_usage_timer_;
  ObCheckpointTask checkpoint_task_;
  ObTraversalFlushTask traversal_flush_task_;
  ObCheckClogDiskUsageTask check_clog_disk_usage_task_;

#ifdef OB_BUILD_SHARED_STORAGE

public:
  static int64_t SS_UPDATE_CKPT_INTERVAL;
  static int64_t SS_TRY_ADVANCE_CKPT_INTERVAL;
  static int64_t SS_TRY_SCHEDULE_UPLOAD_INTERVAL;

public:
  void set_prev_ss_advance_ckpt_task_ts(int64_t rhs) { prev_ss_advance_ckpt_task_ts_ = rhs; }
  int64_t prev_ss_advance_ckpt_task_ts() { return prev_ss_advance_ckpt_task_ts_; }
  ObSSScheUploadThreadPool &ss_sche_upload_thread_pool() { return ss_sche_upload_thread_pool_; }

private:
  class ObSSUpdateCkptSCNTask : public common::ObTimerTask {
  public:
    virtual void runTimerTask() override;
  };

  class ObSSUpdateCkptLSNTask : public common::ObTimerTask {
  public:
    virtual void runTimerTask() override;
  };

  class ObSSAdvanceCkptTask : public common::ObTimerTask {
  public:
    virtual void runTimerTask() override;
  };

  struct ObSSScheduleIncUploadTask : public common::ObTimerTask {
  public:
    ObSSScheduleIncUploadTask(ObCheckPointService &host_ckpt_sv) : host_ckpt_sv_(host_ckpt_sv) {}
    virtual void runTimerTask() override;

  public:
    ObCheckPointService &host_ckpt_sv_;
  };

private:
  int64_t prev_ss_advance_ckpt_task_ts_;
  ObSSScheUploadThreadPool ss_sche_upload_thread_pool_;
  common::ObTimer ss_update_ckpt_scn_timer_;
  common::ObTimer ss_update_ckpt_lsn_timer_;
  common::ObTimer ss_advance_ckpt_timer_;
  common::ObTimer ss_schedule_upload_timer_;
  ObSSUpdateCkptSCNTask ss_update_ckpt_scn_task_;
  ObSSUpdateCkptLSNTask ss_update_ckpt_lsn_task_;
  ObSSAdvanceCkptTask ss_advance_ckpt_task_;
  ObSSScheduleIncUploadTask ss_schedule_upload_task_;
#endif  // OB_BUILD_SHARED_STORAGE
};

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase

#endif
