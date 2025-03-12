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

#ifndef OCEANBASE_ROOTSERVER_OB_VEC_IVF_INDEX_BUILD_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_VEC_IVF_INDEX_BUILD_TASK_H_

#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_domain_index_builder_util.h"

namespace oceanbase
{
namespace rootserver
{

class ObVecIVFIndexBuildTask : public ObDDLTask
{
public:
  ObVecIVFIndexBuildTask();
  virtual ~ObVecIVFIndexBuildTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const ObTableSchema *data_table_schema,
      const ObTableSchema *index_schema,
      const int64_t schema_version,
      const int64_t parallelism,
      const int64_t consumer_group_id,
      const share::ObDDLType task_type,
      const obrpc::ObCreateIndexArg &create_index_arg,
      const uint64_t tenant_data_version,
      const int64_t parent_task_id = 0,
      const int64_t task_status = share::ObDDLTaskStatus::PREPARE,
      const int64_t snapshot_version = 0);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual int cleanup_impl() override;
  virtual bool is_valid() const override;
  virtual int collect_longops_stat(share::ObLongopsValue &value) override;
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
  virtual bool support_longops_monitoring() const override { return true; }
  virtual int on_child_task_finish(
    const uint64_t child_task_key,
    const int ret_code) override;
  TO_STRING_KV(K(centroid_table_id_), K(cid_vector_table_id_), K(rowkey_cid_table_id_),
      K(sq_meta_table_id_), K(pq_centroid_table_id_), K(pq_code_table_id_), K(pq_rowkey_cid_table_id_),
      K(centroid_table_task_submitted_), K(cid_vector_table_task_submitted_), K(rowkey_cid_table_task_submitted_),
      K(sq_meta_table_task_submitted_), K(pq_centroid_table_task_submitted_), K(pq_code_table_task_submitted_),
      K(pq_rowkey_cid_table_task_submitted_),
      K(centroid_table_task_id_), K(cid_vector_table_task_id_), K(rowkey_cid_table_task_id_),
      K(sq_meta_table_task_id_), K(pq_centroid_table_task_id_), K(pq_code_table_task_id_),
      K(pq_rowkey_cid_table_task_id_),
      K(drop_index_task_id_), K(is_rebuild_index_),
      K(drop_index_task_submitted_), K(schema_version_), K(execution_id_),
      K(consumer_group_id_), K(trace_id_), K(parallelism_), K(create_index_arg_));

public:
  void set_centroid_table_id(const uint64_t id) { centroid_table_id_ = id; }
  void set_cid_vector_table_id(const uint64_t id) { cid_vector_table_id_ = id; }
  void set_rowkey_cid_table_id(const uint64_t id) { rowkey_cid_table_id_ = id; }
  void set_sq_meta_table_id(const uint64_t id) { sq_meta_table_id_ = id; }
  void set_pq_centroid_table_id(const uint64_t id) { pq_centroid_table_id_ = id; }
  void set_pq_code_table_id(const uint64_t id) { pq_code_table_id_ = id; }
  void set_pq_rowkey_cid_table_id(const uint64_t id) { pq_rowkey_cid_table_id_ = id; }

  void set_centroid_table_task_submitted(const bool status) { centroid_table_task_submitted_ = status; }
  void set_cid_vector_table_task_submitted(const bool status) { cid_vector_table_task_submitted_ = status; }
  void set_rowkey_cid_table_task_submitted(const bool status) { rowkey_cid_table_task_submitted_ = status; }
  void set_sq_meta_table_task_submitted(const bool status) { sq_meta_table_task_submitted_ = status; }
  void set_pq_centroid_table_task_submitted(const bool status) { pq_centroid_table_task_submitted_ = status; }
  void set_pq_code_table_task_submitted(const bool status) { pq_code_table_task_submitted_ = status; }
  void set_pq_rowkey_cid_table_task_submitted(const bool status) { pq_rowkey_cid_table_task_submitted_ = status; }

  void set_centroid_table_task_id(const uint64_t id) { centroid_table_task_id_ = id; }
  void set_cid_vector_table_task_id(const uint64_t id) { cid_vector_table_task_id_ = id; }
  void set_rowkey_cid_table_task_id(const uint64_t id) { rowkey_cid_table_task_id_ = id; }
  void set_sq_meta_table_task_id(const uint64_t id) { sq_meta_table_task_id_ = id; }
  void set_pq_centroid_table_task_id(const uint64_t id) { pq_centroid_table_task_id_ = id; }
  void set_pq_code_table_task_id(const uint64_t id) { pq_code_table_task_id_ = id; }
  void set_pq_rowkey_cid_table_task_id(const uint64_t id) { pq_rowkey_cid_table_task_id_ = id; }

  void set_drop_index_task_id(const uint64_t id) { pq_rowkey_cid_table_task_id_ = id; }
  void set_drop_index_task_submitted(const bool status) { drop_index_task_submitted_ = status; }
  int update_task_message(common::ObISQLClient &proxy);

private:
  int get_next_status(share::ObDDLTaskStatus &next_status);
  int prepare_aux_table(const ObIndexType index_type, bool &task_submitted, uint64_t &aux_table_id, int64_t &task_id);
  int construct_create_index_arg(const ObIndexType index_type, obrpc::ObCreateIndexArg &arg);

  int prepare_sq_meta_table();
  int prepare_centroid_table();
  int prepare_pq_centroid_table();
  int prepare_aux_index_tables();
  int prepare_aux_ivfflat_index_tables(bool &state_finished);
  int prepare_aux_ivfsq8_index_tables(bool &state_finished);
  int prepare_aux_ivfpq_index_tables(bool &state_finished);

  int check_ivfflat_aux_table_schema_exist(bool &is_all_exist);
  int check_ivfsq8_aux_table_schema_exist(bool &is_all_exist);
  int check_ivfpq_aux_table_schema_exist(bool &is_all_exist);

  int get_centroid_table_index_type(ObIndexType &index_type);

  int build_ivfflat_dependent_task_result_map();
  int build_ivfsq8_dependent_task_result_map();
  int build_ivfpq_dependent_task_result_map();

  int prepare();
  int wait_aux_table_complement();
  int validate_checksum();
  int clean_on_failed();
  int submit_drop_vec_index_task();
  int wait_drop_index_finish(bool &is_finish);
  int succ();
  int check_health();
  int check_aux_table_schemas_exist(bool &is_all_exist);
  int deep_copy_index_arg(
      common::ObIAllocator &allocator,
      const obrpc::ObCreateIndexArg &source_arg,
      obrpc::ObCreateIndexArg &dest_arg);
  int print_child_task_ids(char *buf, int64_t len);

private:
  struct ChangeTaskStatusFn final
  {
  public:
    ChangeTaskStatusFn(common::hash::ObHashMap<uint64_t, share::ObDomainDependTaskStatus> &dependent_task_result_map, const uint64_t tenant_id, ObRootService *root_service, int64_t &not_finished_cnt) :
      dependent_task_result_map_(dependent_task_result_map),
      rt_service_(root_service),
      dest_tenant_id_(tenant_id),
      not_finished_cnt_(not_finished_cnt)
    {}
  public:
    ~ChangeTaskStatusFn() = default;
    int operator() (common::hash::HashMapPair<uint64_t, share::ObDomainDependTaskStatus> &entry);
  public:
    common::hash::ObHashMap<uint64_t, share::ObDomainDependTaskStatus> &dependent_task_result_map_;
    ObRootService *rt_service_;
    uint64_t dest_tenant_id_;
    int64_t &not_finished_cnt_;
  };
  struct CheckTaskStatusFn final
  {
  public:
    CheckTaskStatusFn(common::hash::ObHashMap<uint64_t, share::ObDomainDependTaskStatus> &dependent_task_result_map,
                      int64_t &finished_task_cnt, bool &child_task_failed, bool &state_finished, const uint64_t tenant_id) :
      dependent_task_result_map_(dependent_task_result_map),
      finished_task_cnt_(finished_task_cnt),
      child_task_failed_(child_task_failed),
      state_finished_(state_finished),
      dest_tenant_id_(tenant_id)
    {}
  public:
    ~CheckTaskStatusFn() = default;
    int operator() (common::hash::HashMapPair<uint64_t, share::ObDomainDependTaskStatus> &entry);
  public:
    common::hash::ObHashMap<uint64_t, share::ObDomainDependTaskStatus> &dependent_task_result_map_;
    int64_t &finished_task_cnt_;
    bool &child_task_failed_;
    bool &state_finished_;
    uint64_t dest_tenant_id_;
  };
  static const int64_t OB_VEC_IVF_INDEX_BUILD_TASK_VERSION = 1;
  static const int64_t OB_VEC_IVF_MAX_BUILD_CHILD_TASK_NUM = 4;

  using ObDDLTask::tenant_id_;
  using ObDDLTask::task_id_;
  using ObDDLTask::schema_version_;
  using ObDDLTask::parallelism_;
  using ObDDLTask::consumer_group_id_;
  using ObDDLTask::parent_task_id_;
  using ObDDLTask::task_status_;
  using ObDDLTask::snapshot_version_;
  using ObDDLTask::object_id_;
  using ObDDLTask::target_object_id_;
  using ObDDLTask::is_inited_;
  uint64_t &index_table_id_;

  uint64_t centroid_table_id_;
  uint64_t cid_vector_table_id_;
  uint64_t rowkey_cid_table_id_;
  uint64_t sq_meta_table_id_;
  uint64_t pq_centroid_table_id_;
  uint64_t pq_code_table_id_;
  uint64_t pq_rowkey_cid_table_id_;

  bool centroid_table_task_submitted_;
  bool cid_vector_table_task_submitted_;
  bool rowkey_cid_table_task_submitted_;
  bool sq_meta_table_task_submitted_;
  bool pq_centroid_table_task_submitted_;
  bool pq_code_table_task_submitted_;
  bool pq_rowkey_cid_table_task_submitted_;

  int64_t centroid_table_task_id_;
  int64_t cid_vector_table_task_id_;
  int64_t rowkey_cid_table_task_id_;
  int64_t sq_meta_table_task_id_;
  int64_t pq_centroid_table_task_id_;
  int64_t pq_code_table_task_id_;
  int64_t pq_rowkey_cid_table_task_id_;

  bool drop_index_task_submitted_;
  int64_t drop_index_task_id_;
  bool is_rebuild_index_;
  ObRootService *root_service_;
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  obrpc::ObCreateIndexArg create_index_arg_;
  common::hash::ObHashMap<uint64_t, share::ObDomainDependTaskStatus> dependent_task_result_map_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* OCEANBASE_ROOTSERVER_OB_VEC_IVF_INDEX_BUILD_TASK_H_*/
