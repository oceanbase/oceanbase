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

#ifndef SRC_SHARE_SCHEDULER_STAT_OB_SYS_TASK_STATUS_H_
#define SRC_SHARE_SCHEDULER_STAT_OB_SYS_TASK_STATUS_H_

#include "common/ob_simple_iterator.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace share
{
static const int64_t DEFAULT_SYS_TASK_STATUS_COUNT = 1024;

struct ObSysTaskStat;

typedef common::ObSimpleIterator<ObSysTaskStat,
    common::ObModIds::OB_SYS_TASK_STATUS,
    DEFAULT_SYS_TASK_STATUS_COUNT> ObSysStatMgrIter;

enum ObSysTaskType
{
  GROUP_MIGRATION_TASK = 0,
  MIGRATION_TASK,
  DDL_TASK,
  SSTABLE_MINI_MERGE_TASK,
  SPECIAL_TABLE_MERGE_TASK,
  SSTABLE_MINOR_MERGE_TASK,
  SSTABLE_MAJOR_MERGE_TASK,
  WRITE_CKPT_TASK,
  BACKUP_TASK,
  BACKUP_VALIDATION_TASK,
  BACKUP_BACKUPSET_TASK,
  BACKUP_ARCHIVELOG_TASK,
  DDL_KV_MERGE_TASK,
  COMPLEMENT_DATA_TASK,
  RESTORE_TASK,
  BACKUP_CLEAN_TASK,
  BACKFILL_TX_TASK,
  REMOVE_MEMBER_TASK,
  TRANSFER_TASK,
  MDS_MINI_MERGE_TASK,
  TABLE_API_TTL_TASK,
  TENANT_SNAPSHOT_CREATE_TASK,
  TENANT_SNAPSHOT_GC_TASK,
  BATCH_FREEZE_TABLET_TASK,
  MAX_SYS_TASK_TYPE
};

const char *sys_task_type_to_str(const ObSysTaskType &type);

struct ObSysTaskStat
{
  ObSysTaskStat();
  int64_t start_time_;
  ObTaskId task_id_;
  ObSysTaskType task_type_;
  common::ObAddr svr_ip_;
  int64_t tenant_id_;
  char comment_[common::OB_MAX_TASK_COMMENT_LENGTH];
  bool is_cancel_;

  TO_STRING_KV(K_(start_time), K_(task_id), K_(task_type), K_(svr_ip), K_(tenant_id), K_(is_cancel), K_(comment));
};

class ObSysTaskStatMgr
{
public:
  ObSysTaskStatMgr();
  virtual ~ObSysTaskStatMgr();

  static ObSysTaskStatMgr &get_instance();

  int add_task(ObSysTaskStat &status);
  int get_iter(ObSysStatMgrIter &iter);
  int del_task(const ObTaskId &task_id);
  int update_task(const ObTaskId &task_id, const char *msg, const int64_t msg_len);
  int set_self_addr(const common::ObAddr addr);
  int task_exist(const ObTaskId &task_id, bool &is_exist);
  int cancel_task(const ObTaskId &task_id);
  int is_task_cancel(const ObTaskId &task_id, bool &is_cancel);
  int generate_task_id(ObTaskId &task_id);
private:
  common::SpinRWLock lock_;
  common::ObArray<ObSysTaskStat> task_array_;
  common::ObAddr self_addr_;
  DISALLOW_COPY_AND_ASSIGN(ObSysTaskStatMgr);
};

}//share
}//oceanbase

#define SYS_TASK_STATUS_MGR (::oceanbase::share::ObSysTaskStatMgr::get_instance())

#endif /* SRC_SHARE_SCHEDULER_STAT_OB_SYS_TASK_STATUS_H_ */
