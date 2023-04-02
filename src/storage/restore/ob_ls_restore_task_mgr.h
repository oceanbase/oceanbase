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

#ifndef OCEABASE_STORAGE_LS_RESTORE_TASK_MGR_H
#define OCEABASE_STORAGE_LS_RESTORE_TASK_MGR_H
#include "ob_ls_restore_args.h"
#include "share/restore/ob_ls_restore_status.h"

namespace oceanbase
{
namespace storage
{

class ObLSRestoreTaskMgr 
{
public:
  static const int64_t OB_RESTORE_MAX_DAG_NET_NUM = 5;
  static const int64_t OB_LS_RESTORE_MAX_TABLET_NUM = 1000000;
  static const int64_t OB_LS_RESOTRE_TABLET_DAG_NET_BATCH_NUM = 1024;

  using TaskMap = common::hash::ObHashMap<share::ObTaskId, ObSArray<ObTabletID>, common::hash::NoPthreadDefendMode>;
  using TabletSet = common::hash::ObHashSet<common::ObTabletID>;
public:
  ObLSRestoreTaskMgr();
  ~ObLSRestoreTaskMgr();
  int init();
  void destroy();
  
  int add_tablet_in_wait_set(const ObIArray<common::ObTabletID> &tablet_ids);
  int add_tablet_in_schedule_set(const ObIArray<common::ObTabletID> &tablet_ids);

  int schedule_tablet(const share::ObTaskId &task_id, const ObSArray<common::ObTabletID> &tablet_need_restore, bool &reach_dag_limit);
  int pop_need_restore_tablets(storage::ObLS &ls, ObIArray<common::ObTabletID> &need_restore_tablets);
  int pop_restored_tablets(storage::ObLS &ls, ObIArray<common::ObTabletID> &tablet_send_to_follower);
  int cancel_task();

  void reuse_set() { schedule_tablet_set_.reuse(); wait_tablet_set_.reuse(); }
  void reuse_wait_set() { wait_tablet_set_.reuse(); }
  bool is_restore_completed() { return 0 == tablet_map_.size() && 0 == wait_tablet_set_.size(); }
  bool has_no_task() { return 0 == tablet_map_.size(); }


private:
  int check_task_exist_(const share::ObTaskId &task_id, bool &is_exist);
  int check_tablet_deleted_or_restored_(storage::ObLS &ls, const common::ObTabletID &tablet_id, 
      bool &is_deleted, bool &is_restored);

private:
  bool is_inited_;
  lib::ObMutex mtx_;
  TaskMap tablet_map_;
  TabletSet schedule_tablet_set_;
  TabletSet wait_tablet_set_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreTaskMgr);
};

}
}

#endif
