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

#ifndef _OB_RESTORE_TABLE_OPERATOR_H
#define _OB_RESTORE_TABLE_OPERATOR_H 1

#include "lib/utility/ob_macro_utils.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/backup/ob_physical_restore_table_operator.h"
#include "rootserver/restore/ob_restore_info.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
class ObISQLClient;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common
namespace share {
class ObDMLSqlSplicer;
}

namespace rootserver {

class ObSchemaMapSerializer {
public:
  static int serialize(common::ObIAllocator& allocator, const common::ObIArray<share::ObSchemaIdPair>& id_pair,
      common::ObString& schema_map_str);

  static int deserialize(const common::ObString& schema_map_str, common::ObIArray<share::ObSchemaIdPair>& id_pair);
};

class ObRestoreTableOperator {
public:
  ObRestoreTableOperator();
  virtual ~ObRestoreTableOperator() = default;
  int init(common::ObISQLClient* sql_client);

  // create a new job with the specified properties
  int insert_job(const RestoreJob& job_info);
  // get jobs count, expect 1 or 0
  int get_job_count(int64_t& job_count);
  // get the only job
  int get_job(const int64_t job_id, RestoreJob& job_info);
  // get all job
  int get_jobs(common::ObIArray<RestoreJob>& jobs);
  // update job status
  int update_job_status(int64_t job_id, int64_t status);
  // create a new task with the specified properties
  int insert_task(const PartitionRestoreTask& task_info);
  // create batch new tasks with the specified properties
  int insert_task(const common::ObIArray<PartitionRestoreTask>& task_info);
  // get all task
  int get_tasks(common::ObIArray<PartitionRestoreTask>& tasks);
  // get specific job task
  int get_tasks(int64_t job_id, common::ObIArray<PartitionRestoreTask>& tasks);
  // update task status
  int update_task_status(int64_t tenant_id, int64_t table_id, int64_t part_id, int64_t status);
  // recycle task
  int recycle_job(int64_t job_id, int64_t status);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRestoreTableOperator);
  // function members
  int cons_job_info(const common::sqlclient::ObMySQLResult& res, RestoreJob& job_info);
  int cons_task_info(const common::sqlclient::ObMySQLResult& res, PartitionRestoreTask& task_info);
  int update_job(int64_t job_id, share::ObDMLSqlSplicer& dml);
  int update_task(int64_t tenant_id, int64_t table_id, int64_t part_id, share::ObDMLSqlSplicer& dml);
  int remove_job(int64_t job_id);
  int remove_tasks(int64_t job_id);
  int record_job_in_history(int64_t job_id);
  int fill_dml_splicer(share::ObDMLSqlSplicer& dml, const RestoreJob& job_info);

private:
  // data members
  bool inited_;
  common::ObISQLClient* sql_client_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_RESTORE_TABLE_OPERATOR_H */
