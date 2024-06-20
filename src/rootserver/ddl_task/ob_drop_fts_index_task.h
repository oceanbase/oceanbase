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

#ifndef OCEANBASE_ROOTSERVER_OB_DROP_FTS_INDEX_TASK_H
#define OCEANBASE_ROOTSERVER_OB_DROP_FTS_INDEX_TASK_H

#include "rootserver/ddl_task/ob_drop_index_task.h"

namespace oceanbase
{
namespace rootserver
{

/**
 * For fulltext search index, the drop fts index task creates other subtasks, and the execution
 * dependency directed graph between each task is as follows,
 *
 *             ---> ObDropIndexTask(fts index)--->       ---> ObDropIndexTask(rowkey doc) --->
 *            /                                   \     /                                     \
 *  ObDropFTSIndexTask(parent)           ObDropFTSIndexTask(parent)                ObDropFTSIndexTask(parent)
 *            \                                   /  |  \                                     /    |
 *             ---> ObDropIndexTask(doc word)---->   |   ---> ObDropIndexTask(doc rowkey) --->     |
 *                                                   |                                             |
 *                                                   --- non-last ---> SUCCESS (end) <--- last ----
 *
 */
class ObDropFTSIndexTask : public ObDDLTask
{
public:
  ObDropFTSIndexTask();
  virtual ~ObDropFTSIndexTask();

  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const uint64_t data_table_id,
      const share::ObDDLType ddl_type,
      const ObFTSDDLChildTaskInfo &rowkey_doc,
      const ObFTSDDLChildTaskInfo &doc_rowkey,
      const ObFTSDDLChildTaskInfo &domain_index,
      const ObFTSDDLChildTaskInfo &fts_doc_word,
      const int64_t schema_version,
      const int64_t consumer_group_id);
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

  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  virtual int on_child_task_finish(const uint64_t child_task_key, const int ret_code) override { return OB_SUCCESS; }

  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, K_(rowkey_doc), K_(doc_rowkey), K_(domain_index), K_(fts_doc_word));
private:
  static const int64_t OB_DROP_FTS_INDEX_TASK_VERSION = 1;
  int check_switch_succ();
  int prepare(const share::ObDDLTaskStatus &status);
  int check_and_wait_finish(const share::ObDDLTaskStatus &status);
  int check_drop_index_finish(
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t table_id,
      bool &has_finished);
  int wait_child_task_finish(
      const common::ObIArray<ObFTSDDLChildTaskInfo> &child_task_ids,
      bool &has_finished);
  int wait_fts_child_task_finish(bool &has_finished);
  int wait_doc_child_task_finish(bool &has_finished);
  int create_drop_index_task(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t index_tid,
      const common::ObString &index_name,
      int64_t &task_id);
  int create_drop_doc_rowkey_task();
  int succ();
  int fail();
  virtual int cleanup_impl() override;
  bool is_fts_task() const { return share::ObDDLType::DDL_DROP_FTS_INDEX == task_type_; }

private:
  ObRootService *root_service_;
  ObFTSDDLChildTaskInfo rowkey_doc_;
  ObFTSDDLChildTaskInfo doc_rowkey_;
  ObFTSDDLChildTaskInfo domain_index_;
  ObFTSDDLChildTaskInfo fts_doc_word_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DROP_domain_INDEX_TASK_H
