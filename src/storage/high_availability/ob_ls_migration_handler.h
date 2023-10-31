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
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace storage
{

enum class ObLSMigrationHandlerStatus : int8_t
{
  INIT = 0,
  PREPARE_LS = 1,
  BUILD_LS = 2,
  COMPLETE_LS = 3,
  FINISH = 4,
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

private:
  void reuse_();
  void wakeup_();
  int get_ls_migration_handler_status_(ObLSMigrationHandlerStatus &status);
  int check_task_list_empty_(bool &is_empty);
  int change_status_(const ObLSMigrationHandlerStatus &new_status);
  int set_result_(const int32_t result);
  int get_result_(int32_t &result);
  bool is_migration_failed_() const;
  int get_ls_migration_task_(ObLSMigrationTask &task);
  int check_task_exist_(
      const ObLSMigrationHandlerStatus &status,
      bool &is_exist);

  int do_init_status_();
  int do_prepare_ls_status_();
  int do_build_ls_status_();
  int do_complete_ls_status_();
  int do_finish_status_();

  int generate_build_ls_dag_net_();
  int schedule_build_ls_dag_net_(
      const ObLSMigrationTask &task);
  int generate_prepare_ls_dag_net_();
  int schedule_prepare_ls_dag_net_(
      const ObLSMigrationTask &task);
  int generate_complete_ls_dag_net_();
  int schedule_complete_ls_dag_net_(
      const ObLSMigrationTask &task);
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
  int get_ls_info_(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      share::ObLSInfo &ls_info);

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
  DISALLOW_COPY_AND_ASSIGN(ObLSMigrationHandler);
};


}
}
#endif
