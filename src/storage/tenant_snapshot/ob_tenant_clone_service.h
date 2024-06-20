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

#ifndef OCEANBASE_STORAGE_OB_TENANT_CLONE_SERVICE_
#define OCEANBASE_STORAGE_OB_TENANT_CLONE_SERVICE_

#include "share/ob_rpc_struct.h"
#include "lib/lock/ob_rwlock.h"
#include "share/restore/ob_ls_restore_status.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_mgr.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_mgr.h"
#include "storage/slog_ckpt/ob_tenant_meta_snapshot_handler.h"
#include "share/restore/ob_tenant_clone_table_operator.h"
#include "observer/ob_startup_accel_task_handler.h"

namespace oceanbase
{
namespace storage
{

// When it was originally designed, ObTenantCloneService was an independent service.
// Later, in order to reduce the number of threads, the thread of ObTenantSnapshotService was
// used to drive ObTenantCloneService.
// Logically, ObTenantCloneService is located on the upper layer of ObTenantSnapshotService
class ObTenantCloneService final
{
public:
  ObTenantCloneService() : is_inited_(false),
                           is_started_(false),
                           meta_handler_(nullptr),
                           startup_accel_handler_(){}

  virtual ~ObTenantCloneService() { }

  int init(ObTenantMetaSnapshotHandler* meta_handler);
  int start();
  void stop();
  void wait();
  void destroy();
  void run();

  bool is_started() { return is_started_; }

  TO_STRING_KV(K(is_inited_), K(is_started_), KP(meta_handler_));
private:
  int wait_();
  int get_clone_job_(ObArray<ObCloneJob>& clone_jobs);
  int try_clone_(const ObCloneJob& job);
  void try_clone_one_ls_(const ObCloneJob& job, ObLS* ls);
  void drive_clone_ls_sm_(const ObCloneJob& job,
                          const share::ObLSRestoreStatus& restore_status,
                          ObLS* ls,
                          share::ObLSRestoreStatus& next_status,
                          bool& next_loop);
  int check_ls_status_valid_(const share::ObLSID& ls_id);
  void handle_clone_start_(const ObCloneJob& job,
                           ObLS* ls,
                           share::ObLSRestoreStatus& next_status,
                           bool& next_loop);
  void handle_copy_all_tablet_meta_(const ObCloneJob& job,
                                    ObLS* ls,
                                    share::ObLSRestoreStatus& next_status,
                                    bool& next_loop);
  void handle_copy_ls_meta_(const ObCloneJob& job,
                            ObLS* ls,
                            ObLSRestoreStatus& next_status,
                            bool& next_loop);
  void handle_clog_replay_(const ObCloneJob& job,
                           ObLS* ls,
                           share::ObLSRestoreStatus& next_status,
                           bool& next_loop);
  int advance_status_(ObLS* ls, const share::ObLSRestoreStatus &next_status);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantCloneService);

  bool is_inited_;
  bool is_started_;
  ObTenantMetaSnapshotHandler* meta_handler_;
  observer::ObStartupAccelTaskHandler startup_accel_handler_;
};

}
}

#endif
