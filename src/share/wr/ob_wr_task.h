/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_TASK_H_
#define OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_TASK_H_

#include "deps/oblib/src/lib/task/ob_timer.h"
#include "share/wr/ob_wr_snapshot_rpc_processor.h"

namespace oceanbase
{
namespace share
{


enum class ObWrSnapshotStatus : int64_t
{
  SUCCESS = 0,
  PROCESSING,
  FAILED,
  DELETED,
};

enum class ObWrSnapshotFlag : int64_t
{
  SCHEDULE = 0,
  MANUAL,
  IMPORT,
};

class WorkloadRepositoryTask : public common::ObTimerTask
{
public:
  WorkloadRepositoryTask();
  virtual ~WorkloadRepositoryTask() {};
  DISABLE_COPY_ASSIGN(WorkloadRepositoryTask);
  int schedule_one_task(int64_t interval = 0);
  int modify_snapshot_interval(int64_t minutes);
  void cancel_current_task();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  int do_snapshot(const bool is_user_submit, obrpc::ObWrRpcProxy& wr_proxy, int64_t &snap_id);

  static int modify_tenant_snapshot_status_and_startup_time(
    int64_t snap_id, uint64_t tenant_id,
    int64_t cluster_id,
    const ObAddr &addr,
    int64_t startup_time,
    ObWrSnapshotStatus status);

  // do delete single tenant`s snapshot that have survived for more than the RETENTION timestamp
  //
  // @tenant_id [in] the id of tenant
  // @cluster_id [in] the id of clusted
  // @end_ts_of_retention [in] the end timestamp of retention
  // @task_timeout_ts [in] the timeout of delete task
  // @rpc_timeout [in] the timeout of rpc request
  // @wr_proxy [in] the wr rpc proxy
  // @return the error code.
  static int do_delete_single_tenant_snapshot(
      const uint64_t tenant_id,
      const int64_t cluster_id,
      const int64_t end_ts_of_retention,
      const int64_t task_timeout_ts,
      const int64_t rpc_timeout,
      const obrpc::ObWrRpcProxy &wr_proxy);

  // do delete single tenant`s snapshot that at the range specified by the user
  //
  // @tenant_id [in] the id of tenant
  // @cluster_id [in] the id of clusted
  // @low_snap_id [in] the left of range
  // @high_snap_id [in] the right of range
  // @task_timeout_ts [in] the timeout of delete task
  // @rpc_timeout [in] the timeout of rpc request
  // @wr_proxy [in] the wr rpc proxy
  // @return the error code.
  static int do_delete_single_tenant_snapshot(
      const uint64_t tenant_id,
      const int64_t cluster_id,
      const int64_t low_snap_id,
      const int64_t high_snap_id,
      const int64_t task_timeout_ts,
      const int64_t rpc_timeout,
      const obrpc::ObWrRpcProxy &wr_proxy);

  // Get the snapshot ids that have survived for more than the RETENTION timestamp
  //
  // @end_ts_of_retention [in] the end timestamp of retention
  // @tenant_id [in] the id of tenant
  // @cluster_id [in] the id of clusted
  // @server_addr [in] the ip and port of leader
  // @to_delete_snap_ids [out] the list of snapshot id that need to be deleted
  // @return the error code.
  static int fetch_to_delete_snap_id_list_by_time(const int64_t end_ts_of_retention,
                                           const uint64_t tenant_id,
                                           const int64_t cluster_id,
                                           const ObAddr &server_addr,
                                           common::ObIArray<int64_t> &to_delete_snap_ids);

  // Get the snapshot ids of the range specified by the user
  //
  // @low_snap_id [in] the left of range
  // @high_snap_id [in] the right of range
  // @tenant_id [in] the id of tenant
  // @cluster_id [in] the id of clusted
  // @server_addr [in] the ip and port of leader
  // @to_delete_snap_ids [out] the list of snapshot id that need to be deleted
  // @return the error code.
  static int fetch_to_delete_snap_id_list_by_range(
    const int64_t low_snap_id,
    const int64_t high_snap_id,
    const uint64_t tenant_id,
    const int64_t cluster_id,
    const ObAddr &server_addr,
    common::ObIArray<int64_t> &to_delete_snap_ids);

  // Modify the status of the snapshot to the deleted state
  //
  // @tenant_id [in] the id of tenant
  // @cluster_id [in] the id of clusted
  // @server_addr [in] the ip and port of leader
  // @to_delete_snap_ids [in] the list of snapshot id that need to be deleted
  // @return the error code.
  static int modify_snap_info_status_to_deleted(const uint64_t tenant_id,
                                         const int64_t cluster_id,
                                         const ObAddr &server_addr,
                                         const common::ObIArray<int64_t> &to_delete_snap_ids);

  // Check whether all tenants' snapshot task is finished
  //
  // @timeout_ts [in] the timestamp of timeout
  // @is_all_finished [out] all the snapshot statuses generated last time are finished
  // @return the error code.
  static int check_all_tenant_last_snapshot_task_finished(bool& is_all_finished);
  static int check_snapshot_task_finished_for_snap_id(int64_t snap_id, bool &is_finished);
  static int get_last_snap_id(int64_t &snap_id);
  static int mins_to_duration(const int64_t mins, char* string);
  static int update_wr_control(const char* time_num_col_name,
                               const int64_t mins,
                               const char* time_col_name,
                               const int64_t tenant_id);
  static int fetch_retention_usec_from_wr_control(int64_t &retention);
  static int fetch_interval_num_from_wr_control(int64_t &interval);
  static int update_snap_info_in_wr_control(const int64_t tenant_id, const int64_t snap_id,
                                      const int64_t end_interval_time);
  static int get_init_wr_control_sql_string(const int64_t tenant_id,
                                            ObSqlString &sql);
  bool is_running_task() const { return is_running_task_; };
  int64_t get_snapshot_interval() const { return snapshot_interval_;};
  static bool check_tenant_can_do_wr_task(uint64_t tenant_id);
  INHERIT_TO_STRING_KV("ObTimerTask", ObTimerTask,
                        K_(snapshot_interval), K_(tg_id), K_(timeout_ts), K_(is_running_task));
  static const int64_t DEFAULT_SNAPSHOT_RETENTION = 7L * 24 * 60 ; // 7 days
  static const int64_t ESTIMATE_PS_RESERVE_TIME = 100 * 1000;  // different server's clock skew.
  static const int64_t DEFAULT_SNAPSHOT_INTERVAL = 60;
  static const int64_t WR_MODIFY_BATCH_SIZE = 3;
  static const int64_t WR_FETCH_TO_DELETE_SNAP_MAX_NUM = 1024;
  static const int64_t OB_WR_DELETE_SNAPSHOT_FREQUENCY = 7;
  static const int64_t WR_MIN_SNAPSHOT_INTERVAL = 10 * 60 * 1000 * 1000L; // 10 min
  static const int64_t WR_USER_DEL_TASK_TIMEOUT = 10 * 60 * 1000 * 1000L; // 10 min
private:
  void runTimerTask() override;
  int do_delete_snapshot();
  static int get_next_snapshot_id(int64_t &snap_id);
  static int create_snapshot_id_sequence();
  static int fetch_snapshot_id_sequence_nextval(int64_t &snap_id);
  static int get_begin_interval_time(int64_t &begin_interval_time);
  static int get_ps_timeout_timestamp(int64_t &ps_timeout_timestamp);
  static int setup_tenant_snapshot_info(int64_t snap_id, uint64_t tenant_id,
                                        int64_t cluster_id, const ObAddr &addr,
                                        int64_t begin_interval_time,
                                        int64_t end_interval_time,
                                        ObWrSnapshotFlag snap_flag);
  static int get_last_snapshot_task_begin_ts(int64_t snap_id, int64_t &task_begin_ts);
  obrpc::ObWrRpcProxy wr_proxy_;
  int64_t snapshot_interval_;  // in minutes
  int tg_id_;
  int64_t timeout_ts_;
  bool is_running_task_;
  bool is_inited_;
};

}//end namespace share
}//end namespace oceanbase

#endif // OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_TASK_H_
