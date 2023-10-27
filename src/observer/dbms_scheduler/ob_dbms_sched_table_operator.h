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

#ifndef SRC_OBSERVER_DBMS_SCHED_TABLE_OPERATOR_H_
#define SRC_OBSERVER_DBMS_SCHED_TABLE_OPERATOR_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace common
{

class ObMySQLProxy;
class ObString;
class ObIAllocator;
class ObString;

namespace sqlclient
{
class ObMySQLResult;
}

}

namespace dbms_scheduler
{
class ObDBMSSchedJobInfo;
class ObDBMSSchedJobClassInfo;

class ObDBMSSchedTableOperator
{
public:
  ObDBMSSchedTableOperator() : sql_proxy_(NULL) {}
  virtual ~ObDBMSSchedTableOperator() {};

  int init(common::ObISQLClient *sql_proxy) { sql_proxy_ = sql_proxy; return common::OB_SUCCESS; }

  int update_for_start(
    uint64_t tenant_id, ObDBMSSchedJobInfo &job_info, bool update_nextdate = true);
  int update_for_end(
    uint64_t tenant_id, ObDBMSSchedJobInfo &job_info, int err, const common::ObString &errmsg);
  int update_nextdate(uint64_t tenant_id, ObDBMSSchedJobInfo &job_info);

  int get_dbms_sched_job_info(
    uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const common::ObString &job_name,
    common::ObIAllocator &allocator, ObDBMSSchedJobInfo &job_info);
  int get_dbms_sched_job_infos_in_tenant(
    uint64_t tenant_id, bool is_oracle_tenant,
    common::ObIAllocator &allocator, common::ObIArray<ObDBMSSchedJobInfo> &job_infos);

  int get_dbms_sched_job_class_info(
    uint64_t tenant_id, bool is_oracle_tenant, const common::ObString job_class_name,
    common::ObIAllocator &allocator, ObDBMSSchedJobClassInfo &job_class_info);

  int extract_info(
    common::sqlclient::ObMySQLResult &result, int64_t tenant_id, bool is_oracle_tenant,
    common::ObIAllocator &allocator, ObDBMSSchedJobInfo &job_info);
  int extract_job_class_info(
    sqlclient::ObMySQLResult &result, int64_t tenant_id, bool is_oracle_tenant,
    ObIAllocator &allocator, ObDBMSSchedJobClassInfo &job_class_info);

  int calc_execute_at(
    ObDBMSSchedJobInfo &job_info, int64_t &execute_at, int64_t &delay, bool ignore_nextdate = false);

  int check_job_can_running(int64_t tenant_id, bool &can_running);

  int check_job_timeout(ObDBMSSchedJobInfo &job_info);

  int check_auto_drop(ObDBMSSchedJobInfo &job_info);

  int register_default_job_class(uint64_t tenant_id);
  int purge_run_detail_histroy(uint64_t tenant_id);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDBMSSchedTableOperator);

private:
  common::ObISQLClient *sql_proxy_;
};

}
}

#endif /* SRC_OBSERVER_DBMS_SCHED_TABLE_OPERATOR_H_ */
