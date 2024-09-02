/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROOTSERVER_OB_DROP_VEC_INDEX_TASK_H
#define OCEANBASE_ROOTSERVER_OB_DROP_VEC_INDEX_TASK_H

#include "rootserver/ddl_task/ob_drop_index_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObDropVecIndexTask : public ObDDLTask
{
public:
  ObDropVecIndexTask();
  virtual ~ObDropVecIndexTask();

  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const uint64_t data_table_id,
      const share::ObDDLType ddl_type,
      const ObVecIndexDDLChildTaskInfo &rowkey_vid,
      const ObVecIndexDDLChildTaskInfo &vid_rowkey,
      const ObVecIndexDDLChildTaskInfo &domain_index,
      const ObVecIndexDDLChildTaskInfo &vec_delta_buffer,
      const ObVecIndexDDLChildTaskInfo &vec_index_snapshot_data,
      const int64_t schema_version,
      const int64_t consumer_group_id,
      const uint64_t tenant_data_version,
      const obrpc::ObDropIndexArg &drop_index_arg);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
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
  virtual int on_child_task_finish(const uint64_t child_task_key, const int ret_code) override { return OB_SUCCESS; }
  int update_drop_lob_meta_row_job_status(const common::ObTabletID &tablet_id,
                                        const int64_t snapshot_version,
                                        const int64_t execution_id,
                                        const int ret_code,
                                        const ObDDLTaskInfo &addition_info);

  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, K_(rowkey_vid), K_(vid_rowkey), K_(domain_index), K_(vec_index_id),
                      K_(vec_index_snapshot_data), K(wait_trans_ctx_), K(snapshot_held_));
private:
  static const int64_t OB_DROP_VEC_INDEX_TASK_VERSION = 1;
  int deep_copy_index_arg(common::ObIAllocator &allocator,
                          const obrpc::ObDropIndexArg &src_index_arg,
                          obrpc::ObDropIndexArg &dst_index_arg);
  int check_switch_succ();
  int prepare(const share::ObDDLTaskStatus &status);
  int check_and_wait_finish(const share::ObDDLTaskStatus &status);
  int release_snapshot(const int64_t snapshot_version);
  int wait_trans_end(const ObDDLTaskStatus next_task_status);
  int obtain_snapshot(const share::ObDDLTaskStatus next_task_status);
  int drop_aux_index_table(const share::ObDDLTaskStatus &status);
  int drop_lob_meta_row(const share::ObDDLTaskStatus next_task_status);
  int check_and_cancel_del_dag(bool &all_dag_exit);
  int exit_all_dags_and_clean();
  int finish();
  int check_drop_index_finish(
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t table_id,
      bool &has_finished);
  int wait_child_task_finish(
      const common::ObIArray<ObVecIndexDDLChildTaskInfo> &child_task_ids,
      bool &has_finished);
  int wait_none_share_index_child_task_finish(bool &has_finished);
  int wait_share_index_child_task_finish(bool &has_finished);
  int create_drop_index_task(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t index_tid,
      const common::ObString &index_name,
      int64_t &task_id,
      const bool is_domain_index = false);
  int create_drop_share_index_task();
  int update_task_message();
  int succ();
  int fail();
  int send_build_single_replica_request();
  int check_build_single_replica(bool &is_end);
  virtual int cleanup_impl() override;

private:
  ObRootService *root_service_;
  ObVecIndexDDLChildTaskInfo rowkey_vid_;
  ObVecIndexDDLChildTaskInfo vid_rowkey_;
  ObVecIndexDDLChildTaskInfo domain_index_;
  ObVecIndexDDLChildTaskInfo vec_index_id_;
  ObVecIndexDDLChildTaskInfo vec_index_snapshot_data_;
  obrpc::ObDropIndexArg drop_index_arg_;
  ObDDLSingleReplicaExecutor replica_builder_;
  common::hash::ObHashMap<common::ObTabletID, common::ObTabletID> check_dag_exit_tablets_map_; // for delete lob meta row data ddl only.
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  int64_t delte_lob_meta_request_time_;
  int64_t delte_lob_meta_job_ret_code_;
  int64_t check_dag_exit_retry_cnt_;
  bool del_lob_meta_row_task_submitted_;
  bool snapshot_held_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DROP_domain_INDEX_TASK_H
