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

#ifndef OCEANBASE_ROOTSERVER_OB_FTS_INDEX_BUILD_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_FTS_INDEX_BUILD_TASK_H_

#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObFtsIndexBuildTask : public ObDDLTask
{
public:
  ObFtsIndexBuildTask();
  virtual ~ObFtsIndexBuildTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const ObTableSchema *data_table_schema,
      const ObTableSchema *index_schema,
      const int64_t schema_version,
      const int64_t parallelism,
      const int64_t consumer_group_id,
      const obrpc::ObCreateIndexArg &create_index_arg,
      const int64_t parent_task_id = 0,
      const int64_t task_status = share::ObDDLTaskStatus::PREPARE,
      const int64_t snapshot_version = 0);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
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
  virtual bool support_longops_monitoring() const override { return false; }
  virtual int on_child_task_finish(
    const uint64_t child_task_key,
    const int ret_code) override;
  TO_STRING_KV(K(index_table_id_), K(rowkey_doc_aux_table_id_),
      K(doc_rowkey_aux_table_id_), K(fts_index_aux_table_id_),
      K(fts_doc_word_aux_table_id_), K(rowkey_doc_schema_generated_),
      K(doc_rowkey_schema_generated_), K(fts_index_aux_schema_generated_),
      K(fts_doc_word_schema_generated_), K(rowkey_doc_task_submitted_),
      K(doc_rowkey_task_submitted_), K(fts_index_aux_task_submitted_),
      K(fts_doc_word_task_submitted_), K(drop_index_task_id_),
      K(drop_index_task_submitted_), K(schema_version_), K(execution_id_),
      K(consumer_group_id_), K(trace_id_), K(parallelism_), K(create_index_arg_));

private:
  int get_next_status(share::ObDDLTaskStatus &next_status);
  int prepare_rowkey_doc_table();
  int prepare_aux_index_tables();
  int construct_rowkey_doc_arg(obrpc::ObCreateIndexArg &arg);
  int construct_doc_rowkey_arg(obrpc::ObCreateIndexArg &arg);
  int construct_fts_index_aux_arg(obrpc::ObCreateIndexArg &arg);
  int construct_fts_doc_word_arg(obrpc::ObCreateIndexArg &arg);
  int record_index_table_id(
      const obrpc::ObCreateIndexArg *create_index_arg_,
      uint64_t &aux_table_id);
  int get_index_table_id(
      const obrpc::ObCreateIndexArg *create_index_arg,
      uint64_t &index_table_id);
  int prepare();
  int wait_aux_table_complement();
  int submit_build_aux_index_task(
      const obrpc::ObCreateIndexArg &create_index_arg,
      ObDDLTaskRecord &task_record,
      bool &task_submitted);
  int validate_checksum();
  int clean_on_failed();
  int submit_drop_fts_index_task();
  int wait_drop_index_finish(bool &is_finish);
  int succ();
  int update_index_status_in_schema(
      const ObTableSchema &index_schema,
      const ObIndexStatus new_status);
  int check_health();
  int check_aux_table_schemas_exist(bool &is_all_exist);
  int deep_copy_index_arg(
      common::ObIAllocator &allocator,
      const obrpc::ObCreateIndexArg &source_arg,
      obrpc::ObCreateIndexArg &dest_arg);

private:
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
  static const int64_t OB_FTS_INDEX_BUILD_TASK_VERSION = 1;
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
  uint64_t rowkey_doc_aux_table_id_;
  uint64_t doc_rowkey_aux_table_id_;
  uint64_t fts_index_aux_table_id_;
  uint64_t fts_doc_word_aux_table_id_;
  bool rowkey_doc_schema_generated_;
  bool doc_rowkey_schema_generated_;
  bool fts_index_aux_schema_generated_;
  bool fts_doc_word_schema_generated_;
  bool rowkey_doc_task_submitted_;
  bool doc_rowkey_task_submitted_;
  bool fts_index_aux_task_submitted_;
  bool fts_doc_word_task_submitted_;
  int64_t drop_index_task_id_;
  bool drop_index_task_submitted_;
  ObRootService *root_service_;
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  obrpc::ObCreateIndexArg create_index_arg_;
  common::hash::ObHashMap<uint64_t, DependTaskStatus> dependent_task_result_map_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* OCEANBASE_ROOTSERVER_OB_FTS_INDEX_BUILD_TASK_H_*/
