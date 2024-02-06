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
#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tx_storage/ob_empty_shell_task.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
namespace checkpoint
{
#define TABLET_PERSIST                  0x01  /* tablet persist trigger */
#define TABLET_GC                       0x02  /* tablet gc trigger */

enum TabletGCStatus
{
  NOT_NEED_GC = 0,
  NEED_GC_AFTER_MDS_PERSIST = 1,
  NEED_GC_IMMEDIATELY = 2
};

class ObTabletGCHandler
{
  friend class ObTabletGCService;
public:
  ObTabletGCHandler()
    : ls_(NULL),
      tablet_persist_trigger_(TABLET_PERSIST | TABLET_GC),
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
  { return 0 != (tablet_persist_trigger & TABLET_PERSIST); }
  static bool is_tablet_gc_trigger(uint8_t tablet_persist_trigger)
  { return 0 != (tablet_persist_trigger & TABLET_GC); }
  bool is_tablet_gc_trigger_and_reset();

  void set_tablet_persist_trigger();
  void set_tablet_gc_trigger();
  uint8_t get_tablet_persist_trigger_and_reset();
  int check_tablet_need_persist_(
      ObTabletHandle &tablet_handle,
      bool &need_persist,
      bool &need_retry,
      const share::SCN &decided_scn);
  int check_tablet_need_gc_(
      ObTabletHandle &tablet_handle,
      TabletGCStatus &need_gc,
      bool &need_retry,
      const share::SCN &decided_scn);
  int get_unpersist_tablet_ids(common::ObIArray<ObTabletHandle> &deleted_tablets,
      common::ObIArray<ObTabletHandle> &immediately_deleted_tablets,
      common::ObTabletIDArray &unpersist_tablet_ids,
      const bool only_persist,
      bool &need_retry,
      const share::SCN &decided_scn);
  int flush_unpersist_tablet_ids(const common::ObTabletIDArray &unpersist_tablet_ids,
                                 const share::SCN &decided_scn);
  int get_max_tablet_transfer_scn(const common::ObIArray<ObTabletHandle> &deleted_tablets, share::SCN &transfer_scn);
  int set_ls_transfer_scn(const common::ObIArray<ObTabletHandle> &deleted_tablets);
  int gc_tablets(const common::ObIArray<ObTabletHandle> &deleted_tablets);
  bool check_stop() { return ATOMIC_LOAD(&update_enabled_) == false; }
  int disable_gc();
  void enable_gc();
  int set_tablet_change_checkpoint_scn(const share::SCN &scn);
  int offline();
  void online();
  TO_STRING_KV(K_(tablet_persist_trigger), K_(is_inited));

private:
  static const int64_t FLUSH_CHECK_MAX_TIMES;
  static const int64_t FLUSH_CHECK_INTERVAL;
  int freeze_unpersist_tablet_ids(const common::ObTabletIDArray &unpersist_tablet_ids,
                                  const share::SCN &decided_scn);
  int wait_unpersist_tablet_ids_flushed(const common::ObTabletIDArray &unpersist_tablet_ids,
                                        const share::SCN &decided_scn);
  bool is_finish() { obsys::ObWLockGuard lock(wait_lock_, false); return lock.acquired(); }
  void set_stop() { ATOMIC_STORE(&update_enabled_, false); }
  void set_start() { ATOMIC_STORE(&update_enabled_, true); }

public:
  static const int64_t GC_LOCK_TIMEOUT = 100_ms; // 100ms
  obsys::ObRWLock wait_lock_;
  lib::ObMutex gc_lock_;

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
      timer_for_tablet_change_(),
      tablet_change_task_(*this),
      timer_for_tablet_shell_(),
      tablet_shell_task_(*this)
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
  static const int64_t GC_CHECK_DELETE_INTERVAL;
  static const int64_t GLOBAL_GC_CHECK_INTERVAL_TIMES;

  class ObTabletChangeTask : public common::ObTimerTask
  {
  public:
    ObTabletChangeTask(ObTabletGCService &tablet_gc_service)
      : tablet_gc_service_(tablet_gc_service)
    {}
    virtual ~ObTabletChangeTask() {}

    virtual void runTimerTask();
  private:
    ObTabletGCService &tablet_gc_service_;
  };

  common::ObTimer timer_for_tablet_change_;
  ObTabletChangeTask tablet_change_task_;

  common::ObTimer timer_for_tablet_shell_;
  ObEmptyShellTask tablet_shell_task_;
};

} // checkpoint
} // storage
} // oceanbase

#endif
