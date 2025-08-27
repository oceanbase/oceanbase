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

#ifndef OCEABASE_STORAGE_LS_MIGRATION_HANDLER_
#define OCEABASE_STORAGE_LS_MIGRATION_HANDLER_

#include "share/ob_ls_id.h"
#include "common/ob_member.h"
#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "ob_storage_ha_struct.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "observer/ob_rpc_processor_simple.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace storage
{
class ObLSCompleteMigrationParam;

/*
 * Migration Handler State Machine:
 *
 *                                                              ret!=OB_SUCCESS
 *    ┌────────────────────────────────────────────────────────────────────────┐
 *    │                                                                        │
 * ┌──┴─┐  ┌──────────┐  ┌───────────────┐  ┌────────┐  ┌─────────────┐  ┌─────▼───────┐  ┌────────────────┐  ┌──────┐
 * │INIT├─►│PREPARE_LS├─►│WAIT_PREPARE_LS├─►│BUILD_LS├─►│WAIT_BUILD_LS├─►│ COMPLETE_LS ├─►│WAIT_COMPLETE_LS├─►│FINISH│
 * └────┘  └──────────┘  └──┬─┬──────▲───┘  └────────┘  └─┬─┬──────▲──┘  └─▲─┬──────▲──┘  └────┬──────▲────┘  └──────┘
 *                          │ │      │                    │ │      │       │ │      │          │      │
 *                          │ └─wait─┘                    │ └─wait─┘       │ └──────┘          └─wait─┘
 *                          └─────────────────────────────┴────────────────┘  ret!=OB_SUCCESS
 *                             ret!=OB_SUCCESS || result_!=OB_SUCCESS         && !is_complete_
 */
enum class ObLSMigrationHandlerStatus : int8_t // FARM COMPAT WHITELIST
{
  INIT = 0,
  PREPARE_LS = 1,
  WAIT_PREPARE_LS = 2,
  BUILD_LS = 3,
  WAIT_BUILD_LS = 4,
  COMPLETE_LS = 5,
  WAIT_COMPLETE_LS = 6,
  FINISH = 7,
  MAX_STATUS,
};

struct ObLSMigrationHandlerStatusHelper
{
public:
  static int check_can_change_status(
      const ObLSMigrationHandlerStatus &curr_status,
      const ObLSMigrationHandlerStatus &change_status,
      bool &can_change);
  static bool is_valid(const ObLSMigrationHandlerStatus &status);
  static int get_next_change_status(
      const ObLSMigrationHandlerStatus &curr_status,
      const int32_t result,
      ObLSMigrationHandlerStatus &next_status);
};

struct ObLSMigrationTask
{
  ObLSMigrationTask();
  virtual ~ObLSMigrationTask();
  void reset();
  bool is_valid() const;
  VIRTUAL_TO_STRING_KV(
      K_(arg),
      K_(task_id));

  ObMigrationOpArg arg_;
  share::ObTaskId task_id_;
};

class ObLSMigrationHandler : public ObIHAHandler
{
public:
  ObLSMigrationHandler();
  virtual ~ObLSMigrationHandler();
  int init(
      ObLS *ls,
      common::ObInOutBandwidthThrottle *bandwidth_throttle,
      obrpc::ObStorageRpcProxy *svr_rpc_proxy,
      storage::ObStorageRpc *storage_rpc,
      common::ObMySQLProxy *sql_proxy);
  int add_ls_migration_task(const share::ObTaskId &task_id, const ObMigrationOpArg &arg);
  virtual int process();
  int switch_next_stage(const int32_t result);
  int check_task_exist(const share::ObTaskId &task_id, bool &is_exist);
  void destroy();
  void stop();
  void wait(bool &wait_finished);
  int cancel_task(const share::ObTaskId &task_id, bool &is_exist);
  bool is_cancel() const;
  bool is_complete() const;
  bool is_dag_net_cleared() const;
  void set_dag_net_cleared();
  int set_result(const int32_t result);
private:
  void reuse_();
  void wakeup_();
  int get_ls_migration_handler_status_(ObLSMigrationHandlerStatus &status);
  int check_task_list_empty_(bool &is_empty);
  int change_status_(const ObLSMigrationHandlerStatus &new_status);
  int get_result_(int32_t &result);
  bool is_migration_failed_() const;
  int get_ls_migration_task_(ObLSMigrationTask &task);
  int check_task_exist_(bool &is_exist);
  int handle_failed_task_(
      const ObLSMigrationHandlerStatus &status,
      bool &need_generate_dag_net);
  int handle_current_task_(
      bool &need_wait,
      int32_t &task_result);
  // only use this function when task exist
  int cancel_current_task_();

  int do_init_status_();
  int do_prepare_ls_status_();
  int do_build_ls_status_();
  int do_complete_ls_status_();
  int do_finish_status_();
  int do_wait_status_();
  int generate_build_ls_dag_net_();
  int generate_prepare_ls_dag_net_();
  int generate_complete_ls_dag_net_();
  int report_result_();
  int report_meta_table_();
  int report_to_rs_();
  int check_can_skip_prepare_status_(bool &can_skip);
  int check_before_do_task_();
  int check_disk_space_(const ObMigrationOpArg &arg);
  int get_ls_required_size_(
      const ObMigrationOpArg &arg,
      int64_t &required_size);
  int inner_report_result_(const ObLSMigrationTask &task);
  int report_to_rebuild_service_();
  int get_ls_migration_task_with_nolock_(ObLSMigrationTask &task) const;
  int check_task_exist_with_nolock_(const share::ObTaskId &task_id, bool &is_exist) const;
  int switch_next_stage_with_nolock_(const int32_t result);
  int generate_build_tablet_dag_net_();
  int check_need_to_abort_(bool &need_to_abort);
  template<typename DagNetType>
  int schedule_dag_net_(const share::ObIDagInitParam *param, const bool check_cancel);
private:
  bool is_inited_;
  ObLS *ls_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *sql_proxy_;

  int64_t start_ts_;
  int64_t finish_ts_;
  ObSEArray<ObLSMigrationTask, 1> task_list_;
  common::SpinRWLock lock_;
  ObLSMigrationHandlerStatus status_;
  int32_t result_;
  bool is_stop_;
  bool is_cancel_;
  bool is_complete_; // true when ObLSCompleteMigrationDagNet has been generated
  bool is_dag_net_cleared_;

  DISALLOW_COPY_AND_ASSIGN(ObLSMigrationHandler);
};

template<typename DagNetType>
int ObLSMigrationHandler::schedule_dag_net_(
    const share::ObIDagInitParam *param,
    const bool check_cancel)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls migration handler is not inited", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "schedule dag net get invalid argument", K(ret), K(status_), KP(param));
  } else {
    const int32_t cancel_result = OB_CANCELED;
    if (check_cancel && is_cancel_) {
      STORAGE_LOG(INFO, "skip schedule dag net when canceled", K(ret), K(status_), KPC(ls_),
        K(cancel_result), K(check_cancel));
      if (OB_FAIL(switch_next_stage_with_nolock_(cancel_result))) {
        STORAGE_LOG(WARN, "failed to swicth next stage cancel", K(ret), K(status_));
      }
    } else {
      ObTenantDagScheduler *scheduler = nullptr;
      if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
      } else if (FALSE_IT(is_dag_net_cleared_ = false)) {
      } else if (OB_FAIL(scheduler->create_and_add_dag_net<DagNetType>(param))) {
        STORAGE_LOG(WARN, "failed to create and add migration dag net", K(ret), K(status_), KPC(ls_));
        is_dag_net_cleared_ = true;
      } else {
        STORAGE_LOG(INFO, "schedule dag net success", K(ret), K(status_), KPC(ls_));
      }
    }
  }
  return ret;
}

}
}
#endif
