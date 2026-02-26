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

#ifndef OCEANBASE_ROOTSERVER_OB_DROP_VEC_IVF_INDEX_TASK_H
#define OCEANBASE_ROOTSERVER_OB_DROP_VEC_IVF_INDEX_TASK_H

#include "rootserver/ddl_task/ob_drop_index_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObDropVecIVFIndexTask : public ObDDLTask
{
public:
  ObDropVecIVFIndexTask();
  virtual ~ObDropVecIVFIndexTask();

  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const uint64_t data_table_id,
      const share::ObDDLType task_type,
      const ObVecIndexDDLChildTaskInfo &centroid,       // flat/sq8/pq
      const ObVecIndexDDLChildTaskInfo &cid_vector,     // flat/sq8
      const ObVecIndexDDLChildTaskInfo &rowkey_cid,     // flat/sq8/pq
      const ObVecIndexDDLChildTaskInfo &sq_meta,        // sq8
      const ObVecIndexDDLChildTaskInfo &pq_centroid,    // pq
      const ObVecIndexDDLChildTaskInfo &pq_code,        // pq
      const int64_t schema_version,
      const int64_t consumer_group_id,
      const uint64_t tenant_data_version,
      const obrpc::ObDropIndexArg &drop_index_arg);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual int on_child_task_finish(const uint64_t child_task_key, const int ret_code) override { return OB_SUCCESS; }

  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, K_(centroid), K_(cid_vector), K_(rowkey_cid), K_(sq_meta), K_(pq_centroid), K_(pq_code));
private:
  static const int64_t OB_DROP_VEC_IVF_INDEX_TASK_VERSION = 1;
  int deep_copy_index_arg(common::ObIAllocator &allocator,
                          const obrpc::ObDropIndexArg &src_index_arg,
                          obrpc::ObDropIndexArg &dst_index_arg);
  int check_switch_succ();
  int prepare(const share::ObDDLTaskStatus &status);
  int drop_aux_index_table(const share::ObDDLTaskStatus &status);
  int wait_drop_task_finish(const share::ObDDLTaskStatus &status);
  int drop_aux_ivfflat_index_table(const share::ObDDLTaskStatus &status);
  int drop_aux_ivfsq8_index_table(const share::ObDDLTaskStatus &status);
  int drop_aux_ivfpq_index_table(const share::ObDDLTaskStatus &status);

  int check_drop_index_finish(
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t table_id,
      bool &has_finished);
  int wait_child_task_finish(
      const common::ObIArray<ObVecIndexDDLChildTaskInfo> &child_task_ids,
      bool &has_finished);

  int wait_ivfflat_index_child_task_finish(bool &has_finished);
  int wait_ivfsq8_index_child_task_finish(bool &has_finished);
  int wait_ivfpq_index_child_task_finish(bool &has_finished);

  int create_drop_index_task(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t index_tid,
      const common::ObString &index_name,
      int64_t &task_id,
      const bool is_domain_index = false);
  int update_task_message();
  int succ();
  int fail();
  virtual int cleanup_impl() override;
  virtual bool is_error_need_retry(const int ret_code) override
  {
    UNUSED(ret_code);
    // we should always retry on drop index task
    return task_status_ < share::ObDDLTaskStatus::DROP_AUX_INDEX_TABLE;
  }
private:
  ObRootService *root_service_;
  ObVecIndexDDLChildTaskInfo centroid_;
  ObVecIndexDDLChildTaskInfo cid_vector_;
  ObVecIndexDDLChildTaskInfo rowkey_cid_;
  ObVecIndexDDLChildTaskInfo sq_meta_;
  ObVecIndexDDLChildTaskInfo pq_centroid_;
  ObVecIndexDDLChildTaskInfo pq_code_;
  obrpc::ObDropIndexArg drop_index_arg_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DROP_domain_INDEX_TASK_H
