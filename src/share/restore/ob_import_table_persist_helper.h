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

#ifndef OCEANBASE_SHARE_IMPORT_TABLE_PERSIST_HELPER_H
#define OCEANBASE_SHARE_IMPORT_TABLE_PERSIST_HELPER_H

#include "lib/ob_define.h"
#include "share/ob_inner_table_operator.h"
#include "share/restore/ob_import_table_struct.h"

namespace oceanbase
{
namespace share
{

class ObImportTableJobPersistHelper final : public ObIExecTenantIdProvider
{
public:
  ObImportTableJobPersistHelper();
  virtual ~ObImportTableJobPersistHelper() {}
  int init(const uint64_t tenant_id);
  void reset() { is_inited_ = false; }
  uint64_t get_exec_tenant_id() const override { return gen_meta_tenant_id(tenant_id_); }
  int insert_import_table_job(common::ObISQLClient &proxy, const ObImportTableJob &job) const;

  int get_import_table_job(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id,
      ObImportTableJob &job) const;
  int get_all_import_table_jobs(common::ObISQLClient &proxy, common::ObIArray<ObImportTableJob> &jobs) const;
  int advance_status(common::ObISQLClient &proxy,
      const ObImportTableJob &job, const ObImportTableJobStatus &next_status) const;

  int report_import_job_statistics(common::ObISQLClient &proxy, const ObImportTableJob &job) const;
  int move_import_job_to_history(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id) const;
  int get_import_table_job_history_by_initiator(common::ObISQLClient &proxy,
      const uint64_t initiator_tenant_id, const uint64_t initiator_job_id, ObImportTableJob &job) const;
  int get_import_table_job_by_initiator(common::ObISQLClient &proxy,
      const uint64_t initiator_tenant_id, const uint64_t initiator_job_id, ObImportTableJob &job) const;
  int force_cancel_import_job(common::ObISQLClient &proxy) const;
  int report_statistics(common::ObISQLClient &proxy, const ObImportTableJob &job) const;
  TO_STRING_KV(K_(is_inited), K_(tenant_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObImportTableJobPersistHelper);
  bool is_inited_;
  uint64_t tenant_id_; // sys or user tenant id
  ObInnerTableOperator table_op_;
};

class ObImportTableTaskPersistHelper final : public ObIExecTenantIdProvider
{
public:
  ObImportTableTaskPersistHelper();
  virtual ~ObImportTableTaskPersistHelper() {}
  int init(const uint64_t tenant_id);
  void reset() { is_inited_ = false; }
  uint64_t get_exec_tenant_id() const override { return gen_meta_tenant_id(tenant_id_); }
  int insert_import_table_task(common::ObISQLClient &proxy, const ObImportTableTask &task) const;

  int get_all_import_table_tasks_by_initiator(common::ObISQLClient &proxy,
      const ObImportTableJob &job, common::ObIArray<ObImportTableTask> &tasks) const;
  int get_one_not_finish_task_by_initiator(common::ObISQLClient &proxy,
      const ObImportTableJob &job, bool &all_finish, ObImportTableTask &task) const;
  int get_recover_table_task(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t task_id,
      ObImportTableTask &task) const;

  int advance_status(common::ObISQLClient &proxy,
      const ObImportTableTask &task, const ObImportTableTaskStatus &next_status) const;

  int move_import_task_to_history(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id) const;
  int report_import_task_statistics(common::ObISQLClient &proxy, const ObImportTableTask &task) const;
  TO_STRING_KV(K_(is_inited), K_(tenant_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObImportTableTaskPersistHelper);
  bool is_inited_;
  uint64_t tenant_id_; // sys or user tenant id
  ObInnerTableOperator table_op_;
};

}
}

#endif