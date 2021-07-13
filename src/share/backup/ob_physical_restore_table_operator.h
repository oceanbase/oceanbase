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
#define _OB_PHYSICAL_RESTORE_TABLE_OPERATOR_H 1

#include "lib/utility/ob_macro_utils.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/backup/ob_physical_restore_info.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common
namespace share {
class ObPhysicalRestoreTableOperator {
public:
  ObPhysicalRestoreTableOperator();
  virtual ~ObPhysicalRestoreTableOperator() = default;
  int init(common::ObISQLClient* sql_client);

  int insert_job(const ObPhysicalRestoreJob& job_info);
  int replace_job(const ObPhysicalRestoreJob& job_info);
  int get_jobs(common::ObIArray<ObPhysicalRestoreJob>& jobs);
  int get_job_count(int64_t& job_count);
  int check_job_exist(const int64_t job_id, bool& exist);
  int get_job(const int64_t job_id, ObPhysicalRestoreJob& job_info);
  int update_job_error_info(int64_t job_id, int return_ret, PhysicalRestoreMod mod,
      const common::ObCurTraceId::TraceId& trace_id, const common::ObAddr& addr);
  int update_job_status(int64_t job_id, int64_t status);
  template <typename T>
  int update_restore_option(int64_t job_id, const char* option_name, const T& option_value);
  int recycle_job(int64_t job_id, int64_t status);
  int get_restore_infos(common::ObIArray<ObPhysicalRestoreInfo>& infos);
  int get_job_by_tenant_id(const uint64_t tenant_id, ObPhysicalRestoreJob& job_info);
  int get_restore_info(const uint64_t tenant_id, share::ObPhysicalRestoreInfo& restore_info);

  int init_restore_progress(const share::ObPhysicalRestoreJob& job_info);
  int reset_restore_progress(const share::ObPhysicalRestoreJob& job_info);
  int update_restore_progress(
      const share::ObPhysicalRestoreJob& job_info, const share::ObRestoreProgressInfo& statistic);
  int get_restore_progress_statistic(
      const share::ObPhysicalRestoreJob& job_info, share::ObRestoreProgressInfo& statistic);

public:
  static const char* get_physical_restore_mod_str(PhysicalRestoreMod mod);
  static const char* get_restore_status_str(PhysicalRestoreStatus status);
  static PhysicalRestoreStatus get_restore_status(const common::ObString& status_str);

private:
  int fill_dml_splicer(share::ObDMLSqlSplicer& dml, const ObPhysicalRestoreJob& job_info);
  static int retrieve_restore_option(common::sqlclient::ObMySQLResult& result, ObPhysicalRestoreJob& job);
  static int retrieve_int_value(common::sqlclient::ObMySQLResult& result, int64_t& value);
  static int retrieve_uint_value(common::sqlclient::ObMySQLResult& result, uint64_t& value);

  int update_rs_job_status(int64_t job_id, int64_t status);
  int remove_job(int64_t job_id);
  int record_job_in_history(int64_t job_id);

private:
  bool inited_;
  common::ObISQLClient* sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreTableOperator);
};

template <typename T>
int ObPhysicalRestoreTableOperator::update_restore_option(
    int64_t job_id, const char* option_name, const T& option_value)
{
  int ret = common::OB_SUCCESS;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "physical restore table operator not init", KR(ret));
  } else {
    share::ObDMLSqlSplicer dml;
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
      SHARE_LOG(WARN, "fail to add pk column", KR(ret), K(job_id));
    } else if (OB_FAIL(dml.add_pk_column("name", option_name))) {
      SHARE_LOG(WARN, "fail to add pk column", KR(ret), K(option_name));
    } else if (OB_FAIL(dml.add_column("value", option_value))) {
      SHARE_LOG(WARN, "fail to add column", KR(ret), K(option_value));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_RESTORE_INFO_TNAME, sql))) {
      SHARE_LOG(WARN, "splice_insert_sql failed", KR(ret));
    } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
      SHARE_LOG(WARN, "execute sql failed", K(sql), KR(ret));
    } else if (affected_rows <= 0) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "invalid affected rows", KR(ret), K(affected_rows));
    }
  }
  SHARE_LOG(INFO, "[RESTORE] update job restore option", KR(ret), K(job_id), K(option_name), K(option_value));
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase

#endif /* _OB_PHYSICAL_RESTORE_TABLE_OPERATOR_H */
