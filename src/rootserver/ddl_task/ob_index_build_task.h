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
      ObRootService *root_service,
      const common::ObAddr &inner_sql_exec_addr,
      const bool is_vector_index)
      : task_id_(task_id), tenant_id_(tenant_id), data_table_id_(data_table_id), dest_table_id_(dest_table_id),
        schema_version_(schema_version), snapshot_version_(snapshot_version), execution_id_(execution_id),
        consumer_group_id_(consumer_group_id), trace_id_(trace_id), parallelism_(parallelism), allocator_("IdxSSTBuildTask"),
        root_service_(root_service), inner_sql_exec_addr_(inner_sql_exec_addr), is_vector_index_(is_vector_index)
  {
    set_retry_times(0);
  }

  int send_build_replica_sql() const;
  int set_nls_format(const ObString &nls_date_format,
                     const ObString &nls_timestamp_format,
                     const ObString &nls_timestamp_tz_format);
  ObDDLTaskID get_ddl_task_id() { return ObDDLTaskID(tenant_id_, task_id_); }
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  void add_event_info(const int ret, const ObString &ddl_event_stmt);
  bool is_vector_index() const { return is_vector_index_; }
  void set_vector_index_using_type(share::schema::ObIndexUsingType vector_index_using_type) { vector_index_using_type_ = vector_index_using_type; }
  void set_vd_type(common::ObVectorDistanceType vd_type) { vd_type_ = vd_type; }
  common::ObVectorDistanceType get_vd_type() const { return vd_type_; }
  void set_vector_pq_seg(const int64_t vector_pq_seg) { vector_pq_seg_ = vector_pq_seg; }
  void set_vector_hnsw_m(const int64_t vector_hnsw_m) { vector_hnsw_m_ = vector_hnsw_m; }
  void set_vector_hnsw_ef_construction(const int64_t vector_hnsw_ef_construction) { vector_hnsw_ef_construction_ = vector_hnsw_ef_construction; }
  void set_container_table_id(const int64_t container_table_id) { container_table_id_ = container_table_id; }
  void set_second_container_table_id(const int64_t second_container_table_id) { second_container_table_id_ = second_container_table_id; }
  TO_STRING_KV(K_(data_table_id), K_(dest_table_id), K_(schema_version), K_(snapshot_version),
               K_(execution_id), K_(consumer_group_id), K_(trace_id), K_(parallelism), K_(nls_date_format),
               K_(nls_timestamp_format), K_(nls_timestamp_tz_format), K_(is_vector_index), K_(container_table_id),
               K_(second_container_table_id), K_(vector_index_using_type), K_(vd_type), K_(vector_pq_seg), K_(vector_hnsw_m), K_(vector_hnsw_ef_construction));
  inline bool is_using_ivf_index() const { return is_using_ivfpq_index() || is_using_ivfflat_index(); }
  inline bool is_using_hnsw_index() const { return USING_HNSW == vector_index_using_type_; }
  inline bool is_using_ivfflat_index() const { return USING_IVFFLAT == vector_index_using_type_; }
  inline bool is_using_ivfpq_index() const { return USING_IVFPQ == vector_index_using_type_; }

private:
  int inner_normal_process(
      const ObTableSchema &data_schema,
      const ObTableSchema &index_schema,
      const bool is_oracle_mode,
      const ObSqlString &sql_string);
  int inner_hnsw_process(
      ObSchemaGetterGuard &schema_guard,
      const ObTableSchema &data_schema,
      const ObTableSchema &index_schema);
  int inner_ivf_process(
      const ObTableSchema &data_schema,
      const ObTableSchema &index_schema,
      const bool is_oracle_mode);

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
  common::ObArenaAllocator allocator_;
  ObString nls_date_format_;
  ObString nls_timestamp_format_;
  ObString nls_timestamp_tz_format_;
  ObRootService *root_service_;
  common::ObAddr inner_sql_exec_addr_;
  bool is_vector_index_;
  int64_t container_table_id_; // for ivf indexs
  int64_t second_container_table_id_; // for ivfpq indexs
  share::schema::ObIndexUsingType vector_index_using_type_;
  common::ObVectorDistanceType vd_type_;
  int64_t vector_pq_seg_;
  int64_t vector_hnsw_m_;
  int64_t vector_hnsw_ef_construction_;

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
      const int64_t parent_task_id /* = 0 */,
      const uint64_t tenant_data_version,
      const ObTableSchema *container_schema = nullptr,
      const ObTableSchema *second_container_schema = nullptr,
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
  virtual int deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual bool support_longops_monitoring() const override { return true; }
  static int deep_copy_index_arg(common::ObIAllocator &allocator, const obrpc::ObCreateIndexArg &source_arg, obrpc::ObCreateIndexArg &dest_arg);
  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, K(index_table_id_),K(snapshot_held_), K(is_sstable_complete_task_submitted_),
      K(sstable_complete_ts_), K(check_unique_snapshot_), K_(redefinition_execution_id), K(create_index_arg_), K(target_cg_cnt_));
private:
  int prepare();
  int wait_trans_end();
  int wait_data_complement();
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
  int send_build_single_replica_request();
  int check_build_single_replica(bool &is_end);
  int check_need_verify_checksum(bool &need_verify);
  int check_need_acquire_lob_snapshot(const ObTableSchema *data_table_schema,
                                      const ObTableSchema *index_table_schema,
                                      bool &need_acquire);
  bool is_sstable_complete_task_submitted();
  int check_target_cg_cnt();
  int update_mlog_last_purge_scn();
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
  obrpc::ObCreateIndexArg create_index_arg_; // this is not a valid arg, only has nls formats for now
  int64_t target_cg_cnt_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_INDEX_BUILD_TASK_H

