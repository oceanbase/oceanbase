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

#ifndef OCEANBASE_ROOTSERVER_IMPORT_TABLE_JOB_SCHEDULER_H
#define OCEANBASE_ROOTSERVER_IMPORT_TABLE_JOB_SCHEDULER_H

#include "share/restore/ob_import_table_struct.h"
#include "share/restore/ob_import_table_persist_helper.h"
namespace oceanbase
{
namespace obrpc
{
struct ObRecoverRestoreTableDDLArg;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace common
{
class ObMySQLProxy;
class ObString;
class ObMySQLTransaction;
class ObISQLClient;
}

namespace rootserver
{
class ObImportTableJobScheduler final
{
public:
  ObImportTableJobScheduler();
  ~ObImportTableJobScheduler() {}
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObMySQLProxy &sql_proxy);
  void do_work();

private:
  int check_compatible_() const;
  int process_(share::ObImportTableJob &job);
  int wait_src_tenant_schema_refreshed_(const uint64_t tenant_id);
  int gen_import_table_task_(share::ObImportTableJob &job);
  int deal_with_import_table_task_(share::ObImportTableJob &job);
  int process_import_table_task_(share::ObImportTableTask &task);
  int do_after_import_all_table_(share::ObImportTableJob &job);
  int update_statistic_(common::ObIArray<share::ObImportTableTask> &import_tasks, share::ObImportTableJob &job);
  int canceling_(share::ObImportTableJob &job);
  int finish_(const share::ObImportTableJob &job);
  int persist_import_table_task_(common::ObMySQLTransaction &trans, const share::ObImportTableTask &task);
  int get_import_table_tasks_(const share::ObImportTableJob &job, common::ObIArray<share::ObImportTableTask> &import_tasks);
  int reconstruct_ref_constraint_(share::ObImportTableJob &job);
  int check_import_ddl_task_exist_(const share::ObImportTableTask &task, bool &is_exist);
  void wakeup_();

  int advance_status_(common::ObISQLClient &sql_proxy, const share::ObImportTableJob &job, const share::ObImportTableJobStatus &next_status);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy                       *sql_proxy_;
  share::ObImportTableJobPersistHelper       job_helper_;
  share::ObImportTableTaskPersistHelper      task_helper_;
  DISALLOW_COPY_AND_ASSIGN(ObImportTableJobScheduler);
};

class ObImportTableTaskScheduler final
{
public:
  ObImportTableTaskScheduler()
    : is_inited_(false), schema_service_(nullptr), sql_proxy_(nullptr), import_task_(nullptr),
      helper_() {}
  virtual ~ObImportTableTaskScheduler() { reset(); }
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObMySQLProxy &sql_proxy,
      share::ObImportTableTask &task);
  void reset();
  int process();
  TO_STRING_KV(KPC_(import_task));
private:
  int init_();
  int doing_();
  int try_advance_status_(const int err_code);
  int construct_import_table_arg_(obrpc::ObRecoverRestoreTableDDLArg &arg);
  int construct_import_table_schema_(
      const share::schema::ObTableSchema &src_table_schema, share::schema::ObTableSchema &tartget_table_schema);
  int check_import_ddl_task_exist_(bool &is_exist);
  int statistics_import_results_();
  int gen_import_ddl_task_();
  int wait_import_ddl_task_finish_(bool &is_finish);
  void wakeup_();
protected:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy                       *sql_proxy_;
  share::ObImportTableTask                   *import_task_;
  share::ObImportTableTaskPersistHelper         helper_;
  DISALLOW_COPY_AND_ASSIGN(ObImportTableTaskScheduler);
};

}
}

#endif