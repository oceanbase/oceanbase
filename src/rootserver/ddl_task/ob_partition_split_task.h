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

#ifndef OCEANBASE_ROOTSERVER_OB_PARTITION_SPLIT_TASK_H
#define OCEANBASE_ROOTSERVER_OB_PARTITION_SPLIT_TASK_H

#include "lib/net/ob_addr.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace rootserver
{

enum ObCheckProgressStatus
{
  NOT_STARTED = 0,
  ONGOING = 1,
  DONE,
};

enum ObPartitionSplitReplicaType
{
  DATA_TABLET_REPLICA = 0,
  LOCAL_INDEX_TABLET_REPLICA = 1,
  LOB_TABLET_REPLICA = 2
};

template <typename T>
class ObCheckProgressKey final
{
public:
  ObCheckProgressKey() = default;
  ObCheckProgressKey(
      const T &field,
      const ObTabletID &tablet_id);
  ~ObCheckProgressKey() = default;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash(); return OB_SUCCESS;
  }
  bool operator==(const ObCheckProgressKey &other) const;
  bool operator!=(const ObCheckProgressKey &other) const;
  bool is_valid() const
  {
    return tablet_id_.is_valid();
  }
  int assign(const ObCheckProgressKey&other);
  TO_STRING_KV(K(field_), K(tablet_id_));

public:
  T field_;
  ObTabletID tablet_id_;
};

class ObSplitFinishItem final
{
public:
  ObSplitFinishItem()
    : leader_addr_(), finish_arg_()
  {}
  ~ObSplitFinishItem() {}
  TO_STRING_KV(K(leader_addr_), K(finish_arg_));
public:
  ObAddr leader_addr_;
  obrpc::ObTabletSplitFinishArg finish_arg_;
};

// the process of partition split
class ObPartitionSplitTask final : public ObDDLTask
{
public:
  ObPartitionSplitTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t table_id,
      const int64_t schema_version,
      const int64_t parallelism,
      const obrpc::ObPartitionSplitArg &partition_split_arg,
      const int64_t tablet_size,
      const uint64_t tenant_data_version,
      const ObTableSchema *src_table_schema,
      const int64_t parent_task_id = 0,
      const int64_t task_status = share::ObDDLTaskStatus::PREPARE);
  int init(const ObDDLTaskRecord &task_record);
  virtual ~ObPartitionSplitTask();
  virtual int process() override;
  virtual int collect_longops_stat(share::ObLongopsValue &value) override;
  virtual bool support_longops_monitoring() const { return true; }
  int update_complete_sstable_job_status(
      const ObTabletID &tablet_id,
      const ObAddr &svr,
      const int64_t execution_id,
      const int ret_code,
      const ObDDLTaskInfo &addition_info);
  virtual int serialize_params_to_message(
      char *buf,
      const int64_t buf_size,
      int64_t &pos) const override;
  virtual int deserialize_params_from_message(
      const uint64_t tenant_id,
      const char *buf,
      const int64_t buf_size,
      int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual bool is_error_need_retry(const int ret_code) override
  {
    UNUSED(ret_code);
    // we should always retry on partition split task
    return true;
  }
  int get_src_tablet_ids(ObIArray<ObTabletID> &src_ids);
  int get_dest_tablet_ids(ObIArray<ObTabletID> &dest_ids);
  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask,
      K(partition_split_arg_), K(has_synced_stats_info_),
      K(replica_build_task_submit_), K(replica_build_request_time_),
      K(replica_build_ret_code_), K(all_src_tablet_ids_),
      K(data_tablet_compaction_scn_), K(index_tablet_compaction_scns_),
      K(lob_tablet_compaction_scns_), K(freeze_progress_status_inited_),
      K(compact_progress_status_inited_), K(write_split_log_status_inited_),
      K_(data_tablet_parallel_rowkey_list), K_(index_tablet_parallel_rowkey_list),
      K_(min_split_start_scn), K_(split_start_delayed));
protected:
  virtual void clear_old_status_context() override;
private:
  int prepare(const share::ObDDLTaskStatus next_task_status);
  int wait_freeze_end(const share::ObDDLTaskStatus next_task_status);
  int wait_compaction_end(const share::ObDDLTaskStatus next_task_status);
  int write_split_start_log(const share::ObDDLTaskStatus next_task_status);
  int wait_data_tablet_split_end(const share::ObDDLTaskStatus next_task_status);
  int wait_local_index_tablet_split_end(const share::ObDDLTaskStatus next_task_status);
  int wait_lob_tablet_split_end(const share::ObDDLTaskStatus next_task_status);
  int wait_trans_end(const share::ObDDLTaskStatus next_task_status);
  int delete_stat_info(common::ObMySQLTransaction &trans,
                       const char *table_name,
                       const uint64_t table_id,
                       const uint64_t src_part_id);
  int copy_and_delete_src_part_stat_info(const uint64_t table_id,
                                         const int64_t src_part_id,
                                         const ObIArray<uint64_t> &local_index_table_ids,
                                         const ObIArray<int64_t> &src_local_index_part_ids,
                                         const ObSArray<int64_t> &dest_part_ids,
                                         const ObSArray<ObSArray<int64_t>> &dest_local_index_part_ids);

  const char *get_table_schema(const char *table_name);
  int copy_stat_info(common::ObMySQLTransaction &trans,
                     const char *table_name,
                     const uint64_t table_id,
                     const uint64_t src_part_id,
                     const uint64_t dest_part_id);

  int copy_src_part_stat_info_to_dest(common::ObMySQLTransaction &trans,
                                      const uint64_t table_id,
                                      const int64_t src_part_id,
                                      const ObIArray<uint64_t> &local_index_table_ids,
                                      const ObIArray<int64_t> &src_local_index_part_ids,
                                      const ObSArray<int64_t> &dest_part_ids,
                                      const ObSArray<ObSArray<int64_t>> &dest_local_index_part_ids);
  int take_effect(const share::ObDDLTaskStatus next_task_status);
  int succ();
  int wait_recovery_task_finish(const share::ObDDLTaskStatus next_task_status);
  virtual int cleanup_impl() override;
  virtual int clean_splitted_tablet();
  int check_health();
  int setup_src_tablet_ids_array();
  int init_freeze_progress_map();
  int init_compaction_scn_map();
  int init_tablet_compaction_scn_array();
  int update_tablet_compaction_scn_array();
  int restore_compaction_scn_map();
  int serialize_compaction_scn_to_task_record();
  int init_data_complement_progress_map();
  int init_send_finish_map();
  int get_all_dest_tablet_ids(
    const ObTabletID &source_tablet_id,
    ObArray<ObTabletID> &dest_ids);
  int setup_split_finish_items(
    ObAddr &leader_addr,
    ObIArray<obrpc::ObTabletSplitArg> &split_info_array);
  int setup_lob_idxs_arr(ObSArray<uint64_t> &lob_col_idxs_);
  int check_freeze_progress(
      const ObSArray<ObTabletID> &tablet_ids,
      bool &is_end);
  int check_compaction_progress(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObIArray<ObAddr> &split_replica_addrs,
      bool &is_end);
  int send_split_request(
      const ObPartitionSplitReplicaType replica_type);
  int check_split_finished(bool &is_end);
  int check_local_index_checksum();
  int send_split_rpc(const bool is_split_start);
  bool check_need_sync_stats();
  int sync_stats_info();
  void reset_replica_build_stat();
  int init_sync_stats_info(const ObTableSchema* const table_schema,
                           ObSchemaGetterGuard &schema_guard,
                           int64_t &src_partition_id, /* OUTPUT */
                           ObSArray<int64_t> &src_local_index_partition_ids, /* OUTPUT */
                           ObSArray<int64_t> &dest_partition_ids, /* OUTPUT */
                           ObSArray<ObSArray<int64_t>> &dest_local_index_partition_ids /* OUTPUT */);
  int update_message_row_progress_(const oceanbase::share::ObDDLTaskStatus status,
                                   const bool task_submitted,
                                   int64_t &pos);
  int update_message_tablet_progress_(const oceanbase::share::ObDDLTaskStatus status,
                                      int64_t &pos);
  int get_waiting_tablet_ids_(const hash::ObHashMap<ObCheckProgressKey<common::ObAddr>, ObCheckProgressStatus> &tablet_hash,
                              ObIArray<uint64_t> &waiting_tablets /* OUT */);
  int get_waiting_tablet_ids_(const hash::ObHashMap<ObCheckProgressKey<uint64_t>, ObCheckProgressStatus> &tablet_hash,
                              ObIArray<uint64_t> &waiting_tablets /* OUT */);
  int batch_insert_reorganize_history();
  int check_can_reuse_macro_block(
      ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const ObIArray<uint64_t> &table_ids,
      ObSArray<bool> &can_reuse_macro_blocks);
  int check_src_tablet_exist(
      const uint64_t tenant_id,
      const int64_t table_id,
      const ObTabletID &src_tablet_id,
      bool &is_src_tablet_exist);
  int prepare_tablet_split_ranges_inner(
      ObSEArray<ObSEArray<blocksstable::ObDatumRowkey, 8>, 8> &parallel_datum_rowkey_list);
  int prepare_tablet_split_infos(
      const share::ObLSID &ls_id,
      const ObAddr &leader_addr,
      ObIArray<obrpc::ObTabletSplitArg> &split_info_array);
  int update_task_message();
  int register_split_info_mds(const share::ObDDLTaskStatus next_task_status);
  int prepare_tablet_split_ranges(const share::ObDDLTaskStatus next_task_status);
private:
  static const int64_t OB_PARTITION_SPLIT_TASK_VERSION = 1;
  using ObDDLTask::is_inited_;
  using ObDDLTask::task_status_;
  using ObDDLTask::snapshot_version_;
  using ObDDLTask::tenant_id_;
  using ObDDLTask::dst_tenant_id_;
  using ObDDLTask::object_id_;
  using ObDDLTask::schema_version_;
  using ObDDLTask::dst_schema_version_;
  using ObDDLTask::task_type_;
  ObRootService *root_service_;
  obrpc::ObPartitionSplitArg partition_split_arg_;
  bool has_synced_stats_info_;
  bool replica_build_task_submit_;
  int64_t replica_build_request_time_;
  int64_t replica_build_ret_code_;
  ObSArray<ObTabletID> all_src_tablet_ids_; // src data tablet, src local index tablet, src lob tablet
  int64_t data_tablet_compaction_scn_;
  ObSArray<int64_t> index_tablet_compaction_scns_;
  ObSArray<int64_t> lob_tablet_compaction_scns_;
  hash::ObHashMap<ObTabletID, int64_t> tablet_compaction_scn_map_;
  hash::ObHashMap<ObCheckProgressKey<uint64_t>, ObCheckProgressStatus> freeze_progress_map_;
  hash::ObHashMap<ObCheckProgressKey<common::ObAddr>, ObCheckProgressStatus> compaction_progress_map_;
  hash::ObHashMap<ObCheckProgressKey<uint64_t>, ObCheckProgressStatus> send_finish_map_;
  bool freeze_progress_status_inited_;
  bool compact_progress_status_inited_;
  bool write_split_log_status_inited_;
  ObDDLReplicaBuildExecutor replica_builder_;
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  int64_t tablet_size_;
  common::ObSEArray<blocksstable::ObDatumRowkey, 8> data_tablet_parallel_rowkey_list_; // data table
  common::ObSEArray<common::ObSEArray<blocksstable::ObDatumRowkey, 8>, 8> index_tablet_parallel_rowkey_list_; // index table.
  share::SCN min_split_start_scn_;
  bool split_start_delayed_;
  ObTableSchema src_table_schema_; // incomplete table schema, no partition schema included in it
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_PARTITION_SPLIT_TASK_H
