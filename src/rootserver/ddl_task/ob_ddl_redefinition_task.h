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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_REDEFINITION_TASK_H
#define OCEANBASE_ROOTSERVER_OB_DDL_REDEFINITION_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"
#include "share/stat/ob_opt_stat_manager.h"

namespace oceanbase
{
namespace rootserver
{
class ObRootService;

class ObDDLRedefinitionSSTableBuildTask : public share::ObAsyncTask
{
public:
  ObDDLRedefinitionSSTableBuildTask(
      const int64_t task_id,
      const uint64_t tenant_id,
      const int64_t data_table_id,
      const int64_t dest_table_id,
      const int64_t schema_version,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int64_t consumer_group_id,
      const ObSQLMode &sql_mode,
      const common::ObCurTraceId::TraceId &trace_id,
      const int64_t parallelism,
      const bool use_heap_table_ddl_plan,
      ObRootService *root_service,
      const common::ObAddr &inner_sql_exec_addr);
  int init(
      const ObTableSchema &orig_table_schema,
      const AlterTableSchema &alter_table_schema,
      const ObTimeZoneInfoWrap &tz_info_wrap);
  ObDDLTaskID get_ddl_task_id() { return ObDDLTaskID(tenant_id_, task_id_); }
  virtual ~ObDDLRedefinitionSSTableBuildTask() = default;
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t data_table_id_;
  int64_t dest_table_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t execution_id_;
  int64_t consumer_group_id_;
  ObSQLMode sql_mode_;
  ObTimeZoneInfoWrap tz_info_wrap_;
  share::ObColumnNameMap col_name_map_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t parallelism_;
  bool use_heap_table_ddl_plan_;
  ObRootService *root_service_;
  common::ObAddr inner_sql_exec_addr_;
};

class ObSyncTabletAutoincSeqCtx final
{
public:
  ObSyncTabletAutoincSeqCtx();
  ~ObSyncTabletAutoincSeqCtx() {}
  int init(
      const uint64_t src_tenant_id,
      const uint64_t dst_tenant_id,
      int64_t src_table_id,
      int64_t dest_table_id);
  int sync();
  bool is_inited() const { return is_inited_; }
  TO_STRING_KV(K_(is_inited), K_(is_synced), K_(src_tenant_id), K_(dst_tenant_id), K_(orig_src_tablet_ids), K_(src_tablet_ids),
               K_(dest_tablet_ids), K_(autoinc_params));
private:
  int build_ls_to_tablet_map(
      share::ObLocationService *location_service,
      const uint64_t tenant_id,
      const common::ObIArray<share::ObMigrateTabletAutoincSeqParam> &tablet_ids,
      const int64_t timeout,
      const bool force_renew,
      const bool by_src_tablet,
      common::hash::ObHashMap<share::ObLSID, common::ObSEArray<share::ObMigrateTabletAutoincSeqParam, 1>> &map);
  template<typename P, typename A>
  int call_and_process_all_tablet_autoinc_seqs(P &proxy, A &arg, const bool is_get);
  bool is_error_need_retry(const int ret_code) const
  {
    return common::OB_TIMEOUT == ret_code || common::OB_TABLET_NOT_EXIST == ret_code || common::OB_NOT_MASTER == ret_code ||
           common::OB_EAGAIN == ret_code;
  }
private:
  static const int64_t MAP_BUCKET_NUM = 1024;
  bool is_inited_;
  bool is_synced_;
  bool need_renew_location_;
  uint64_t src_tenant_id_;
  uint64_t dst_tenant_id_;
  ObSEArray<ObTabletID, 1> orig_src_tablet_ids_;
  ObSEArray<ObTabletID, 1> src_tablet_ids_;
  ObSEArray<ObTabletID, 1> dest_tablet_ids_;
  ObSEArray<share::ObMigrateTabletAutoincSeqParam, 1> autoinc_params_;
};

class ObDDLRedefinitionTask : public ObDDLTask
{
public:
  explicit ObDDLRedefinitionTask(const share::ObDDLType task_type):
    ObDDLTask(task_type), wait_trans_ctx_(), sync_tablet_autoinc_seq_ctx_(),
    build_replica_request_time_(0), complete_sstable_job_ret_code_(INT64_MAX), alter_table_arg_(),
    dependent_task_result_map_(), snapshot_held_(false), has_synced_autoincrement_(false),
    has_synced_stats_info_(false), update_autoinc_job_ret_code_(INT64_MAX), update_autoinc_job_time_(0),
    check_table_empty_job_ret_code_(INT64_MAX), check_table_empty_job_time_(0),
    is_sstable_complete_task_submitted_(false), sstable_complete_request_time_(0), replica_builder_()
     {}
  virtual ~ObDDLRedefinitionTask() {}
  virtual int process() = 0;
  virtual int update_complete_sstable_job_status(
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int ret_code,
      const ObDDLTaskInfo &addition_info) = 0;
  int on_child_task_finish(
      const uint64_t child_task_key,
      const int ret_code);
  int notify_update_autoinc_finish(const uint64_t autoinc_val, const int ret_code);
  virtual void flt_set_task_span_tag() const = 0;
  virtual void flt_set_status_span_tag() const = 0;
  virtual int cleanup_impl() override;
  int reap_old_replica_build_task(bool &need_exec_new_inner_sql);
  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask,
      K(wait_trans_ctx_), K(sync_tablet_autoinc_seq_ctx_), K(build_replica_request_time_),
      K(complete_sstable_job_ret_code_), K(snapshot_held_), K(has_synced_autoincrement_),
      K(has_synced_stats_info_), K(update_autoinc_job_ret_code_), K(update_autoinc_job_time_),
      K(check_table_empty_job_ret_code_), K(check_table_empty_job_time_));
protected:
  int prepare(const share::ObDDLTaskStatus next_task_status);
  int check_table_empty(const share::ObDDLTaskStatus next_task_status);
  virtual int obtain_snapshot(const share::ObDDLTaskStatus next_task_status);
  virtual int wait_data_complement(const share::ObDDLTaskStatus next_task_status);
  int send_build_single_replica_request();
  int check_build_single_replica(bool &is_end);
  bool check_can_validate_column_checksum(
      const bool is_oracle_mode,
      const share::schema::ObColumnSchemaV2 &src_column_schema,
      const share::schema::ObColumnSchemaV2 &dest_column_schema);
  int get_validate_checksum_columns_id(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &dest_table_schema,
      common::hash::ObHashMap<uint64_t, uint64_t> &validate_checksum_column_ids);
  int check_data_dest_tables_columns_checksum(const int64_t execution_id);
  virtual int fail();
  virtual int success();
  int hold_snapshot(const int64_t snapshot_version);
  int release_snapshot(const int64_t snapshot_version);
  int add_constraint_ddl_task(const int64_t constraint_id);
  int add_fk_ddl_task(const int64_t fk_id);
  int sync_auto_increment_position();
  int modify_autoinc(const share::ObDDLTaskStatus next_task_status);
  int finish();
  int check_health();
  int check_update_autoinc_end(bool &is_end);
  int check_check_table_empty_end(bool &is_end);
  int sync_stats_info();
  int sync_stats_info_in_same_tenant(common::ObMySQLTransaction &trans,
                                     ObSchemaGetterGuard *src_tenant_schema_guard,
                                     const ObTableSchema &data_table_schema,
                                     const ObTableSchema &new_table_schema);
  int sync_stats_info_accross_tenant(common::ObMySQLTransaction &trans,
                                     ObSchemaGetterGuard *dst_tenant_schema_guard,
                                     const ObTableSchema &data_table_schema,
                                     const ObTableSchema &new_table_schema);
  // get source table and partition level stats.
  int get_src_part_stats(const ObTableSchema &data_table_schema,
                         ObIArray<ObOptTableStat> &part_stats);
  // get source table and partition level column stats.
  int get_src_column_stats(const ObTableSchema &data_table_schema,
                           ObIAllocator &allocator,
                           ObIArray<ObOptKeyColumnStat> &column_stats);
  int sync_part_stats_info_accross_tenant(common::ObMySQLTransaction &trans,
                                          const ObTableSchema &data_table_schema,
                                          const ObTableSchema &new_table_schema,
                                          const ObIArray<ObOptTableStat> &part_stats);
  int sync_column_stats_info_accross_tenant(common::ObMySQLTransaction &trans,
                                            ObSchemaGetterGuard *dst_tenant_schema_guard,
                                            const ObTableSchema &data_table_schema,
                                            const ObTableSchema &new_table_schema,
                                            const ObIArray<ObOptKeyColumnStat> &column_stats);

  int sync_table_level_stats_info(common::ObMySQLTransaction &trans,
                                  const ObTableSchema &data_table_schema,
                                  const bool need_sync_history = true);
  int sync_partition_level_stats_info(common::ObMySQLTransaction &trans,
                                      const ObTableSchema &data_table_schema,
                                      const ObTableSchema &new_table_schema,
                                      const bool need_sync_history = true);
  int sync_column_level_stats_info(common::ObMySQLTransaction &trans,
                                   const ObTableSchema &data_table_schema,
                                   const ObTableSchema &new_table_schema,
                                   ObSchemaGetterGuard &schema_guard,
                                   const bool need_sync_history = true);
  int sync_one_column_table_level_stats_info(common::ObMySQLTransaction &trans,
                                             const ObTableSchema &data_table_schema,
                                             const uint64_t old_col_id,
                                             const uint64_t new_col_id,
                                             const bool need_sync_history = true);
  int sync_one_column_partition_level_stats_info(common::ObMySQLTransaction &trans,
                                                 const ObTableSchema &data_table_schema,
                                                 const ObTableSchema &new_table_schema,
                                                 const uint64_t old_col_id,
                                                 const uint64_t new_col_id,
                                                 const bool need_sync_history = true);
  int generate_sync_partition_level_stats_sql(const char *table_name,
                                              const ObIArray<ObObjectID> &src_partition_ids,
                                              const ObIArray<ObObjectID> &dest_partition_ids,
                                              const int64_t batch_start,
                                              const int64_t batch_end,
                                              ObSqlString &sql_string);
  int generate_sync_column_partition_level_stats_sql(const char *table_name,
                                                     const ObIArray<ObObjectID> &src_partition_ids,
                                                     const ObIArray<ObObjectID> &dest_partition_ids,
                                                     const uint64_t old_col_id,
                                                     const uint64_t new_col_id,
                                                     const int64_t batch_start,
                                                     const int64_t batch_end,
                                                     ObSqlString &sql_string);

  bool check_need_sync_stats_history();
  bool check_need_sync_stats();
  int sync_tablet_autoinc_seq();
  int check_need_rebuild_constraint(const ObTableSchema &table_schema,
                                    ObIArray<uint64_t> &constraint_ids,
                                    bool &need_rebuild_constraint);
  int check_need_check_table_empty(bool &need_check_table_empty);
  int get_child_task_ids(char *buf, int64_t len);
  int get_estimated_timeout(const share::schema::ObTableSchema *dst_table_schema, int64_t &estimated_timeout);
  int get_orig_all_index_tablet_count(ObSchemaGetterGuard &schema_guard, int64_t &all_tablet_count);
  int64_t get_build_replica_request_time();
protected:
  static const int64_t MAP_BUCKET_NUM = 1024;
  struct DependTaskStatus final
  {
  public:
    DependTaskStatus()
      : ret_code_(INT64_MAX), task_id_(0)
    {}
    ~DependTaskStatus() = default;
    TO_STRING_KV(K_(task_id), K_(ret_code));
  public:
    int64_t ret_code_;
    int64_t task_id_;
  };
  static const int64_t OB_REDEFINITION_TASK_VERSION = 1L;
  static const int64_t MAX_DEPEND_OBJECT_COUNT = 100L;
  static const int64_t RETRY_INTERVAL = 1 * 1000 * 1000; // 1s
  static const int64_t RETRY_LIMIT = 100;   
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  ObSyncTabletAutoincSeqCtx sync_tablet_autoinc_seq_ctx_;
  int64_t build_replica_request_time_;
  int64_t complete_sstable_job_ret_code_;
  obrpc::ObAlterTableArg alter_table_arg_;
  common::hash::ObHashMap<uint64_t, DependTaskStatus> dependent_task_result_map_;
  bool snapshot_held_;
  bool has_synced_autoincrement_;
  bool has_synced_stats_info_;
  int64_t update_autoinc_job_ret_code_;
  int64_t update_autoinc_job_time_;
  int64_t check_table_empty_job_ret_code_;
  int64_t check_table_empty_job_time_;
  bool is_sstable_complete_task_submitted_;
  int64_t sstable_complete_request_time_;
  ObDDLSingleReplicaExecutor replica_builder_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_TABLE_REDEFINITION_TASK_H
