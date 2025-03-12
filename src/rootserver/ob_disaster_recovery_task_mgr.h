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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_MGR_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_MGR_H_

#include "lib/lock/ob_thread_cond.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "ob_disaster_recovery_task.h"
#include "ob_disaster_recovery_task_table_operator.h"

namespace oceanbase
{

namespace rootserver
{
class ObParallelMigrationMode
{
  OB_UNIS_VERSION(1);
public:
  enum ParallelMigrationMode
  {
    AUTO = 0,
    ON,
    OFF,
    MAX
  };
public:
  ObParallelMigrationMode() : mode_(MAX) {}
  ObParallelMigrationMode(ParallelMigrationMode mode) : mode_(mode) {}
  ObParallelMigrationMode &operator=(const ParallelMigrationMode mode) { mode_ = mode; return *this; }
  ObParallelMigrationMode &operator=(const ObParallelMigrationMode &other) { mode_ = other.mode_; return *this; }
  bool operator==(const ObParallelMigrationMode &other) const { return other.mode_ == mode_; }
  bool operator!=(const ObParallelMigrationMode &other) const { return other.mode_ != mode_; }
  void reset() { mode_ = MAX; }
  void assign(const ObParallelMigrationMode &other) { mode_ = other.mode_; }
  bool is_auto_mode() const { return AUTO == mode_; }
  bool is_on_mode() const { return ON == mode_; }
  bool is_off_mode() const { return OFF == mode_; }
  bool is_valid() const { return MAX != mode_; }
  const ParallelMigrationMode &get_mode() const { return mode_; }
  int parse_from_string(const ObString &mode);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  const char* get_mode_str() const;
private:
  ParallelMigrationMode mode_;
};

class ObDRTaskMgr
{
public:
  ObDRTaskMgr();
  virtual ~ObDRTaskMgr();
  void set_service_epoch(const int64_t service_epoch) { service_epoch_ = service_epoch; }

  // check the __all_ls_replica_task table for invalid tasks and clean them up
  // @params[in]  last_check_ts, timestamp of last check
  int try_clean_and_cancel_task(
      const uint64_t tenant_id);

  // check task in __all_ls_replica_task and schedule it
  // @params[in]  tenant_id, task of which tenent
  int try_pop_and_execute_task(
      const uint64_t tenant_id);

private:
  // check and execute task
  // @params[in]  task, task to execute
  int execute_task_(
      ObDRTask &task);

  // update task status from waiting to inprogress and set schedule_time
  // @params[in]  task, which task to update
  int update_task_schedule_status_(
      const ObDRTask &task);

  int check_and_set_parallel_migrate_task_(
      const uint64_t tenant_id);

  // do some check before execute dr task
  // @params[in]  task, target task to check
  // @params[out] ret_comment, if check failed, record ret_comment
  int check_befor_execute_dr_task_(
      const ObDRTask &task,
      ObDRTaskRetComment &ret_comment);

  // check the __all_ls_replica_task table for invalid tasks and clean them up
  // @params[in]  task_status, task status to check
  int check_clean_and_cancel_task_();

  // check if task need be cleaning
  // @params[in]  task, task to check
  // @params[out] need_cleanning, if task need be cleaning
  // @params[out] ret_comment, reason for cleaning
  int check_task_need_cleaning_(
      const ObDRTask &task,
      bool &need_cleanning,
      ObDRTaskRetComment &ret_comment);

  // check if migration task need be canceled
  // @params[in]  task, task to check
  // @params[out] need_cancel, if task need be canceled
  int check_need_cancel_migrate_task_(
      const ObDRTask &task,
      bool &need_cancel);

  // check if tenant has unit in target server
  // @params[in]  tenant_id, target tenant to check
  // @params[in]  server_addr, target server to check
  // @params[out] has_unit, if has unit
  int check_tenant_has_unit_in_server_(
      const uint64_t tenant_id,
      const common::ObAddr &server_addr,
      bool &has_unit);

private:
  int64_t service_epoch_;
  ObArray<ObDRTask*> dr_tasks_;
  common::ObArenaAllocator task_alloc_;
  ObLSReplicaTaskTableOperator table_operator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDRTaskMgr);
};

} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_MGR_H_
