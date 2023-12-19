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

#ifndef _OB_PHYSICAL_RESTORE_TABLE_OPERATOR_H
#define _OB_PHYSICAL_RESTORE_TABLE_OPERATOR_H 

#include "lib/utility/ob_macro_utils.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/restore/ob_physical_restore_info.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
//TODO, modify table in trans
class ObPhysicalRestoreTableOperator
{
public:
  ObPhysicalRestoreTableOperator();
  virtual ~ObPhysicalRestoreTableOperator() = default;
  /*
   * @param[in] tenant_id of restore job, maybe sys or user tenant
   * @param[in] sql client
   */
  int init(common::ObISQLClient *sql_client, const uint64_t tenant_id, const int32_t group_id);
  /*
   * description: insert into __all_restore_job
   * @param[in] restore job
   */
  int insert_job(const ObPhysicalRestoreJob &job_info);
  /*
   * description: remove job from __all_restore_job
   * @param[in] restore job id
   */
  int remove_job(int64_t job_id);
  /*
   * description: update comment of __all_restore_job
   * @param[in] restore job id
   * @param[in] result of restore job
   * @param[in] PhysicalRestoreMod
   * @param[in] trace
   * @param[in] addr
   * */
  int update_job_error_info(
      int64_t job_id,
      int return_ret,
      PhysicalRestoreMod mod,
      const common::ObCurTraceId::TraceId &trace_id,
      const common::ObAddr &addr);
  /*
   * description: update job status of __all_restore_job
   * @param[in] restore job id
   * @param[in] restore status
   */
  int update_job_status(
    int64_t job_id, int64_t status);
  /*
   * description: update job operation of __all_restore_job, like tenant_id 
   * @param[in] restore job id
   * @param[in] operation_name
   * @param[in] operation_value
   */
  template<typename T>
  int update_restore_option(int64_t job_id,
                            const char *option_name,
                            const T &option_value);
  /*
   * description: get all jobs of __all_restore_job
   * @param[out] all jobs in __all_restore_job of the tenant
   * */
  int get_jobs(common::ObIArray<ObPhysicalRestoreJob> &jobs);
  /*
   * description: check job exist 
   * @param[in] tenant_id of restore job, maybe sys or user tenant
   * @param[in] job_id of restore job
   * @param[out] return true while job exist 
   * */
  int check_job_exist(const int64_t job_id, bool &exist);
  /*
   * description: get all job of __all_restore_job by job_id
   * @param[in] job_id
   * @param[out] retore job 
   * */
  int get_job(const int64_t job_id, ObPhysicalRestoreJob &job_info);
  /*
   * description: get all job of __all_restore_job by tenant_id
   * @param[in] restore_tenant_id(sys tenant has all user tenant restore job)
   * @param[out] retore job 
   * */
  int get_job_by_tenant_id(const uint64_t restore_tenant_id,
                           ObPhysicalRestoreJob &job_info);
  /*
   * description: get all job of __all_restore_job by tenant_name
   * @param[in] restore_tenant_name
   * @param[out] retore job 
   * */
  int get_job_by_tenant_name(const ObString &tenant_name,
                           ObPhysicalRestoreJob &job_info);
  int get_job_by_restore_tenant_name(const ObString &tenant_name,
                           ObPhysicalRestoreJob &job_info);

  /*
   * description: check all ls has restored to consistent_scn
   * @param[out] return true while restore has finished.
   * @param[out] return success or failed while is finished.
   * */
  int check_finish_restore_to_consistent_scn(
    bool &is_finished, bool &is_success);
public:
  static const char* get_physical_restore_mod_str(PhysicalRestoreMod mod);
  static const char* get_restore_status_str(PhysicalRestoreStatus status);
  static PhysicalRestoreStatus get_restore_status(const common::ObString &status_str);
  static uint64_t get_exec_tenant_id(const uint64_t tenant_id)
  {
    uint64_t exec_tenant_id = OB_INVALID_TENANT_ID;
    if (is_sys_tenant(tenant_id)) {
      exec_tenant_id = tenant_id;
    } else if (is_meta_tenant(tenant_id)) {
    } else {
      exec_tenant_id = gen_meta_tenant_id(tenant_id);
    }
    return exec_tenant_id;
  }

private:
 int get_restore_job_by_sql_(const uint64_t exec_tenant_id,
                             const ObSqlString &sql,
                             ObPhysicalRestoreJob &job_info);
 int fill_dml_splicer(share::ObDMLSqlSplicer &dml,
                      const ObPhysicalRestoreJob &job_info);
 static int retrieve_restore_option(common::sqlclient::ObMySQLResult &result,
                                    ObPhysicalRestoreJob &job);
 static int retrieve_int_value(common::sqlclient::ObMySQLResult &result,
                               int64_t &value);
 static int retrieve_uint_value(common::sqlclient::ObMySQLResult &result,
                                uint64_t &value);

 int update_rs_job_status(int64_t job_id, int64_t status);

private:
  bool inited_;
  common::ObISQLClient *sql_client_;
  uint64_t tenant_id_;
  int32_t group_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreTableOperator);
};

template<typename T>
int ObPhysicalRestoreTableOperator::update_restore_option(
    int64_t job_id,
    const char *option_name,
    const T &option_value)
{
  int ret = common::OB_SUCCESS;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "physical restore table operator not init", KR(ret));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id_);
    if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
      SHARE_LOG(WARN, "fail to add pk column", KR(ret), K(job_id));
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id_))) {
      SHARE_LOG(WARN, "fail to add pk column", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(dml.add_pk_column("name", option_name))) {
      SHARE_LOG(WARN, "fail to add pk column", KR(ret), K(option_name));
    } else if (OB_FAIL(dml.add_column("value", option_value))) {
      SHARE_LOG(WARN, "fail to add column", KR(ret), K(option_value));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_RESTORE_JOB_TNAME, sql))) {
      SHARE_LOG(WARN, "splice_insert_sql failed", KR(ret));
    } else if (OB_FAIL(sql_client_->write(exec_tenant_id, sql.ptr(), group_id_, affected_rows))) {
      SHARE_LOG(WARN, "execute sql failed", K(sql), KR(ret), K(exec_tenant_id));
    } else if (affected_rows <= 0) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "invalid affected rows", KR(ret), K(affected_rows));
    }
  }
  SHARE_LOG(INFO, "[RESTORE] update job restore option",
            KR(ret), K(job_id), K(option_name), K(option_value));
  return ret;
}


} // end namespace share
} // end namespace oceanbase


#endif /* _OB_PHYSICAL_RESTORE_TABLE_OPERATOR_H */
