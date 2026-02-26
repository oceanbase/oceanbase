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
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/shared_storage/ob_private_block_gc_task.h"
#endif

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
    : wait_lock_(common::ObLatchIds::TABLET_GC_WAIT_LOCK),
      ls_(NULL),
      tablet_persist_trigger_(TABLET_PERSIST | TABLET_GC),
      update_enabled_(true),
      is_inited_(false)
  {}
  ~ObTabletGCHandler() {
    int ret = 0;
    STORAGE_LOG(WARN, "failed to alloc", KR(ret));
    reset();
  }
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
      const share::SCN &decided_scn,
      bool &need_persist,
      bool &need_retry,
      bool &no_need_wait_persist);

  int check_tablet_need_gc_(
      ObTabletHandle &tablet_handle,
      TabletGCStatus &need_gc,
      bool &need_retry,
      const share::SCN &decided_scn);
  static int check_tablet_from_aborted_tx(const ObTablet &tablet, TabletGCStatus &gc_status);
  int get_unpersist_tablet_ids(
      common::ObIArray<ObTabletHandle> &deleted_tablets,
      common::ObIArray<ObTabletHandle> &immediately_deleted_tablets,
      common::ObTabletIDArray &unpersist_tablet_ids,
      const share::SCN &decided_scn,
      const bool only_persist,
      bool &need_retry,
      bool &no_need_wait_persist);
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
  bool is_finish() { obsys::ObWLockGuard<> lock(wait_lock_, false); return lock.acquired(); }
  void set_stop() { ATOMIC_STORE(&update_enabled_, false); }
  void set_start() { ATOMIC_STORE(&update_enabled_, true); }

  int64_t get_gc_lock_abs_timeout() const;

public:
  static const int64_t GC_LOCK_TIMEOUT = 100_ms; // 100ms
  obsys::ObRWLock<> wait_lock_;

private:
  storage::ObLS *ls_;
  uint8_t tablet_persist_trigger_;
  bool update_enabled_;
  mutable common::RWLock gc_rwlock_;
  bool is_inited_;
};

class ObTabletGCService
{
public:
  ObTabletGCService()
    : is_inited_(false),
      timer_for_tablet_change_(),
      tablet_change_task_(*this),
#ifdef OB_BUILD_SHARED_STORAGE
      timer_for_private_block_gc_(),
      private_block_gc_task_(*this),
      private_tablet_gc_safe_time_(ObLSPrivateBlockGCHandler::DEFAULT_PRIVATE_TABLET_GC_SAFE_TIME),
      private_block_gc_thread_(),
#endif
      timer_for_tablet_shell_(),
      tablet_shell_task_(*this)
  {}

  static int mtl_init(ObTabletGCService *&m);
  int init();
  int start();
  int stop();
  void wait();
  void destroy();
#ifdef OB_BUILD_SHARED_STORAGE
  int64_t get_private_tablet_gc_safe_time()
  { return private_tablet_gc_safe_time_; }

  int report_failed_macro_ids(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    return private_block_gc_task_.report_failed_macro_ids(block_ids);
  }
  ObPrivateBlockGCThread* get_private_block_gc_thread()
  { return &private_block_gc_thread_; }
  void set_mtl_start_max_block_id(const uint64_t mtl_start_max_block_id)
  { private_block_gc_task_.set_mtl_start_max_block_id(mtl_start_max_block_id); }
  void set_observer_start_macro_block_id_trigger()
  { private_block_gc_task_.set_observer_start_macro_block_id_trigger(); }
  uint64_t get_mtl_start_max_block_id()
  { return private_block_gc_task_.get_mtl_start_max_block_id(); }
#endif

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

#ifdef OB_BUILD_SHARED_STORAGE
  common::ObTimer timer_for_private_block_gc_;
  ObPrivateBlockGCTask private_block_gc_task_;
  int64_t private_tablet_gc_safe_time_;
  ObPrivateBlockGCThread private_block_gc_thread_;
#endif

  common::ObTimer timer_for_tablet_shell_;
  ObEmptyShellTask tablet_shell_task_;
};

} // checkpoint
} // storage
} // oceanbase

#endif
