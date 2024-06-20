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

#ifndef OCEANBASE_ROOTSERVER_OB_INDEX_BUILD_TASK_H
#define OCEANBASE_ROOTSERVER_OB_INDEX_BUILD_TASK_H

#include "ob_ddl_tablet_scheduler.h"
#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObIndexSSTableBuildTask : public share::ObAsyncTask
{
public:
  ObIndexSSTableBuildTask(
      const int64_t task_id,
      const uint64_t tenant_id,
      const int64_t data_table_id,
      const int64_t dest_table_id,
      const int64_t schema_version,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int64_t consumer_group_id,
      const common::ObCurTraceId::TraceId &trace_id,
      const int64_t parallelism,
      const bool is_partitioned_local_index_task,
      ObRootService *root_service,
      const common::ObAddr &inner_sql_exec_addr)
      : task_id_(task_id), tenant_id_(tenant_id), data_table_id_(data_table_id), dest_table_id_(dest_table_id),
        schema_version_(schema_version), snapshot_version_(snapshot_version), execution_id_(execution_id),
        consumer_group_id_(consumer_group_id), trace_id_(trace_id), parallelism_(parallelism), is_partitioned_local_index_task_(is_partitioned_local_index_task),
        allocator_("IdxSSTBuildTask"), root_service_(root_service), inner_sql_exec_addr_(inner_sql_exec_addr)
  {
    set_retry_times(0);
  }

  int send_build_replica_sql() const;
  int set_nls_format(const ObString &nls_date_format,
                     const ObString &nls_timestamp_format,
                     const ObString &nls_timestamp_tz_format);
  int set_addition_info(const share::ObLSID &ls_id, const common::ObAddr &ls_leader_addr, const ObIArray<ObTabletID> &index_partition_ids);
  ObDDLTaskID get_ddl_task_id() { return ObDDLTaskID(tenant_id_, task_id_); }
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  void add_event_info(const int ret, const ObString &ddl_event_stmt);
  TO_STRING_KV(K_(data_table_id), K_(dest_table_id), K_(schema_version), K_(snapshot_version),
               K_(execution_id), K_(consumer_group_id), K_(trace_id), K_(parallelism), K_(is_partitioned_local_index_task),
               K_(addition_info), K_(nls_date_format), K_(nls_timestamp_format), K_(nls_timestamp_tz_format));
private:
  inline bool is_partitioned_local_index_task() const { return is_partitioned_local_index_task_ == true; }
private:
  int64_t task_id_;
  int64_t tenant_id_;
  int64_t data_table_id_;
  int64_t dest_table_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t execution_id_;
  int64_t consumer_group_id_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t parallelism_;
  bool is_partitioned_local_index_task_;
  common::ObArenaAllocator allocator_;
  ObString nls_date_format_;
  ObString nls_timestamp_format_;
  ObString nls_timestamp_tz_format_;
  ObRootService *root_service_;
  common::ObAddr inner_sql_exec_addr_;
  ObDDLTaskInfo addition_info_;

  DISALLOW_COPY_AND_ASSIGN(ObIndexSSTableBuildTask);
};
class ObIndexBuildTask : public ObDDLTask
{
public:
  static constexpr const char *MESSAGE_KEY = "check_unique_snapshot";
  ObIndexBuildTask();
  virtual ~ObIndexBuildTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const share::ObDDLType &ddl_type,
      const share::schema::ObTableSchema *data_table_schema,
      const share::schema::ObTableSchema *index_schema,
      const int64_t schema_version,
      const int64_t parallel,
      const int64_t consumer_group_id,
      const int32_t sub_task_trace_id,
      const obrpc::ObCreateIndexArg &create_index_arg,
      const share::ObDDLType task_type,
      const int64_t parent_task_id /* = 0 */,
      const uint64_t tenant_data_version,
      const int64_t task_status = share::ObDDLTaskStatus::PREPARE,
      const int64_t snapshot_version = 0);
  int init(const ObDDLTaskRecord &task_record);
  int update_column_checksum_calc_status(
      const common::ObTabletID &tablet_id,
      const int ret_code);
  int update_complete_sstable_job_status(
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int ret_code,
      const ObDDLTaskInfo &addition_info);
  virtual int process() override;
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  virtual int cleanup_impl() override;
  virtual bool is_valid() const override;
  virtual int collect_longops_stat(share::ObLongopsValue &value) override;
  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual bool support_longops_monitoring() const override { return true; }
  static int deep_copy_index_arg(common::ObIAllocator &allocator, const obrpc::ObCreateIndexArg &source_arg, obrpc::ObCreateIndexArg &dest_arg);
  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, K(index_table_id_),K(snapshot_held_), K(is_sstable_complete_task_submitted_),
      K(sstable_complete_ts_), K(check_unique_snapshot_), K_(redefinition_execution_id), K(create_index_arg_), K(target_cg_cnt_));
private:
  int prepare();
  int wait_trans_end();
  int wait_data_complement();
  int wait_local_index_data_complement();
  int create_schedule_queue();
  int verify_checksum();
  int enable_index();
  int clean_on_failed();
  int succ();
  int hold_snapshot(const int64_t snapshot);
  int release_snapshot(const int64_t snapshot);
  int update_index_status_in_schema(
      const share::schema::ObTableSchema &index_schema,
      const share::schema::ObIndexStatus new_status);
  int check_health();
  int reap_old_replica_build_task(bool &need_exec_new_inner_sql);
  int send_build_single_replica_request(const bool &is_partitioned_local_index_task,
                                        const int64_t &parallelism,
                                        const int64_t &execution_id,
                                        const share::ObLSID &ls_id,
                                        const common::ObAddr &leader_addr,
                                        const ObIArray<ObTabletID> &index_partition_ids);
  int wait_and_send_single_partition_replica_task(bool &state_finished);
  int check_build_single_replica(bool &is_end);
  int check_build_local_index_single_replica(bool &is_end);
  int check_need_verify_checksum(bool &need_verify);
  int check_need_acquire_lob_snapshot(const ObTableSchema *data_table_schema,
                                      const ObTableSchema *index_table_schema,
                                      bool &need_acquire);
  bool is_sstable_complete_task_submitted();
  int check_target_cg_cnt();
  int update_mlog_last_purge_scn();
  bool is_create_partitioned_local_index();
private:
  static const int64_t OB_INDEX_BUILD_TASK_VERSION = 1;
  using ObDDLTask::is_inited_;
  using ObDDLTask::task_status_;
  using ObDDLTask::tenant_id_;
  using ObDDLTask::object_id_;
  using ObDDLTask::schema_version_;
  using ObDDLTask::snapshot_version_;
  uint64_t &index_table_id_;
  bool is_unique_index_;
  bool is_global_index_;
  ObRootService *root_service_;
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  bool snapshot_held_;
  bool is_sstable_complete_task_submitted_;
  int64_t sstable_complete_request_time_;
  int64_t sstable_complete_ts_;
  int64_t check_unique_snapshot_;
  ObDDLWaitColumnChecksumCtx wait_column_checksum_ctx_;
  int64_t complete_sstable_job_ret_code_;
  int64_t redefinition_execution_id_;
  ObDDLTabletScheduler tablet_scheduler_;
  obrpc::ObCreateIndexArg create_index_arg_; // this is not a valid arg, only has nls formats for now
  int64_t target_cg_cnt_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_INDEX_BUILD_TASK_H