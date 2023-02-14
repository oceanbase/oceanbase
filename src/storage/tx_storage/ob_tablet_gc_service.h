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

#ifndef OCEABASE_STORAGE_OB_TABLET_GC_SERVICE_
#define OCEABASE_STORAGE_OB_TABLET_GC_SERVICE_
#include "storage/tx_storage/ob_ls_freeze_thread.h"
#include "lib/task/ob_timer.h"
#include "lib/lock/ob_rwlock.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
namespace checkpoint
{

class ObTabletGCHandler
{
public:
  ObTabletGCHandler()
    : ls_(NULL),
      tablet_persist_trigger_(0),
      update_enabled_(true),
      is_inited_(false)
  {}
  ~ObTabletGCHandler() { reset(); }
  void reset()
  {
    ls_ = NULL;
    tablet_persist_trigger_ = 0;
    is_inited_ = false;
  }
  int init(storage::ObLS *ls);

  uint8_t &get_tablet_persist_trigger() { return tablet_persist_trigger_; }
  static bool is_set_tablet_persist_trigger(uint8_t tablet_persist_trigger)
  { return 0 != (tablet_persist_trigger & 1); }
  static bool is_tablet_gc_trigger(uint8_t tablet_persist_trigger)
  { return 0 != (tablet_persist_trigger & 2); }
  void set_tablet_persist_trigger();
  void set_tablet_gc_trigger();
  uint8_t get_tablet_persist_trigger_and_reset();
  int get_unpersist_tablet_ids(common::ObTabletIDArray &unpersist_create_tablet_ids,
                               bool &need_retry,
                               bool only_deleted = false,
                               const int64_t checkpoint_ts = -1);
  int flush_unpersist_tablet_ids(const common::ObTabletIDArray &unpersist_tablet_ids,
                                 const int64_t checkpoint_ts);
  int gc_tablets(const common::ObTabletIDArray &tablet_ids);
  bool check_stop() { return ATOMIC_LOAD(&update_enabled_) == false; }
  int offline();
  void online();
  TO_STRING_KV(K_(tablet_persist_trigger), K_(is_inited));

private:
  static const int64_t FLUSH_CHECK_MAX_TIMES;
  static const int64_t FLUSH_CHECK_INTERVAL;
  static const int DISK_USED_PERCENT_TRIGGER_FOR_GC_TABLET;
  int freeze_unpersist_tablet_ids(const common::ObTabletIDArray &unpersist_tablet_ids);
  int wait_unpersist_tablet_ids_flushed(const common::ObTabletIDArray &unpersist_tablet_ids,
                                        const int64_t checkpoint_ts);
  bool is_finish() { obsys::ObWLockGuard lock(wait_lock_, false); return lock.acquired(); }
  void set_stop() { ATOMIC_STORE(&update_enabled_, false); }
  void set_start() { ATOMIC_STORE(&update_enabled_, true); }
  bool check_tablet_gc();

public:
  obsys::ObRWLock wait_lock_;

private:
  storage::ObLS *ls_;
  uint8_t tablet_persist_trigger_;
  bool update_enabled_;
  bool is_inited_;
};

class ObTabletGCService
{
public:
  ObTabletGCService()
    : is_inited_(false),
      timer_(),
      tablet_gc_task_(*this)
  {}

  static int mtl_init(ObTabletGCService *&m);
  int init();
  int start();
  int stop();
  void wait();
  void destroy();

private:
  bool is_inited_;

  static const int64_t GC_CHECK_INTERVAL;
  static const int64_t GLOBAL_GC_CHECK_INTERVAL_TIMES;

  class ObTabletGCTask : public common::ObTimerTask
  {
  public:
    ObTabletGCTask(ObTabletGCService &tablet_gc_service)
      : tablet_gc_service_(tablet_gc_service)
    {}
    virtual ~ObTabletGCTask() {}

    virtual void runTimerTask();
  private:
    ObTabletGCService &tablet_gc_service_;
  };
  common::ObTimer timer_;
  ObTabletGCTask tablet_gc_task_;
};

} // checkpoint
} // storage
} // oceanbase

#endif
