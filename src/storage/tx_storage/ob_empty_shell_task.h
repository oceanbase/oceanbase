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

#ifndef OCEABASE_STORAGE_OB_EMPTY_SHELL_TASK_
#define OCEABASE_STORAGE_OB_EMPTY_SHELL_TASK_

#include "lib/oblog/ob_log.h"
#include "lib/task/ob_timer.h"
#include "share/scn.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/ob_tenant_info_proxy.h"

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{

class ObTabletGCService;
class ObTabletEmptyShellHandler
{
public:
  friend class ObEmptyShellTask;
public:
  ObTabletEmptyShellHandler()
    : ls_(NULL),
      is_trigger_(true),
      stopped_(false),
      is_inited_(false)
  {}
  ~ObTabletEmptyShellHandler() { reset(); }
  void reset()
  {
    ls_ = NULL;
    is_inited_ = false;
  }
  int init(storage::ObLS *ls);

  bool check_stop() { return ATOMIC_LOAD(&stopped_) == true; }
  int offline();
  void online();
  bool get_empty_shell_trigger() const;
  void set_empty_shell_trigger(bool is_trigger);
  TO_STRING_KV(K_(is_inited), K_(is_trigger), K_(stopped));

private:
  void set_stop() { ATOMIC_STORE(&stopped_, true); }
  void set_start() { ATOMIC_STORE(&stopped_, false); }
  bool is_finish() { obsys::ObWLockGuard lock(wait_lock_, false); return lock.acquired(); }
  int check_tablet_deleted_(ObTablet *tablet, bool &is_deleted);
  int get_empty_shell_tablet_ids(common::ObTabletIDArray &empty_shell_tablet_ids, bool &need_retry);
  int update_tablets_to_empty_shell(ObLS *ls, const common::ObIArray<common::ObTabletID> &tablet_ids);
  // Conditions for a tablet status is transfer_out_deleted to become an empty shell in standby database(1 or 2 or 3 or 4):
  // 1. Tenant-level replayable scn is greater than the finish_scn of transfer_out_deleted tablet
  // 2. The node dest_ls where the tablet resides does not exist;
  // 3. The migration status of dest_ls is OB_MIGRATION_STATUS_MIGRATE;
  // 4. The replay decided scn of dest_ls is greater than the finish_scn of transfer_out_deleted tablet
  int check_tablet_empty_shell_(
      const ObTablet &tablet,
      const ObTabletCreateDeleteMdsUserData &user_data,
      bool &can, bool &need_retry);
  int check_can_become_empty_shell_(const ObTablet &tablet, bool &can, bool &need_retry);
  int check_transfer_out_deleted_tablet_(const ObTablet &tablet, const ObTabletCreateDeleteMdsUserData &user_data, bool &can, bool &need_retry);

public:
  obsys::ObRWLock wait_lock_;

private:
  storage::ObLS *ls_;
  bool is_trigger_;
  bool stopped_;
  bool is_inited_;
};


class ObEmptyShellTask : public common::ObTimerTask
{

public:
  static const int64_t GC_EMPTY_TABLET_SHELL_INTERVAL;
  static const int64_t GLOBAL_EMPTY_CHECK_INTERVAL_TIMES;
  ObEmptyShellTask(ObTabletGCService &tablet_gc_service)
    : tablet_gc_service_(tablet_gc_service)
  {}
  virtual ~ObEmptyShellTask() {}
  virtual void runTimerTask();

private:
  ObTabletGCService &tablet_gc_service_;
};

} // checkpoint
} // storage
} // oceanbase

#endif
