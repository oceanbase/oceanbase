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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_AUDIT_MGMT_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_AUDIT_MGMT_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace common
{
  class ObISQLClient;
} // namespace common

namespace dbms_job
{
  class ObDBMSJobInfo;
} // namespace dbms_job

namespace pl
{

class ObDbmsAuditMgmt
{
public:
enum AuditTrailType {
  AUDIT_TRAIL_UNKNOWN = -1,
  AUDIT_TRAIL_AUD_STD = 1, // Standard database audit records in the SYS.AUD$ table
  AUDIT_TRAIL_FGA_STD = 2, // Standard database fine-grained auditing (FGA) records in the SYS.FGA_LOG$ table
  AUDIT_TRAIL_DB_STD = 3, // Both standard audit (SYS.AUD$) and FGA audit(SYS.FGA_LOG$) records
  AUDIT_TRAIL_OS = 4, // Operating system audit trail. This refers to the audit records stored in operating system files
  AUDIT_TRAIL_XML = 8, // XML audit trail. This refers to the audit records stored in XML files.
  AUDIT_TRAIL_FILES = 12, // Both operating system (OS) and XML audit trails
  AUDIT_TRAIL_ALL = 15,  // All audit trail types. This includes the standard database audit trail (SYS.AUD$ and SYS.FGA_LOG$ tables), operating system (OS) audit trail,and XML audit trail
};

enum PurgeJobStatus {
  PURGE_JOB_ENABLE = 31,
  PURGE_JOB_DISABLE = 32,
};

enum TrailParamType {
  PARAM_UNKNOWN = -1,
  PARAM_TRAIL_TYPE = 0,
  PARAM_PURGE_INTERVAL,
  PARAM_PURGE_INTERVAL_TYPE, // H: hours, M: minutes
  PARAM_PURGE_NAME,
  PARAM_LAST_ARCH_TS,
  PARAM_USE_LAST_ARCH_TS,
  PARAM_JOB_STATUS,
};

struct ObLastArchTsInfo {
public:
  ObLastArchTsInfo() :
  tenant_id_(0),
  audit_trail_type_(0),
  last_arch_ts_(0),
  flag_(0) {}
public:
  uint64_t tenant_id_;
  uint64_t audit_trail_type_;
  int64_t last_arch_ts_;
  uint64_t flag_;

  TO_STRING_KV(K_(tenant_id), K_(audit_trail_type), K_(last_arch_ts), K_(flag));
};

struct ObCleanJobInfo {
public:
  ObCleanJobInfo() :
  tenant_id_(0),
  job_name_(),
  job_id_(0),
  job_status_(0),
  audit_trail_type_(0),
  job_interval_(0),
  job_frequency_(),
  job_flags_(0),
  job_zone_() {}
public:
  uint64_t tenant_id_;
  common::ObString job_name_;
  uint64_t job_id_;
  uint64_t job_status_;
  uint64_t audit_trail_type_;
  uint64_t job_interval_; // time quantity, dimension is: minutes
  common::ObString job_frequency_;
  int64_t job_flags_;
  common::ObString job_zone_;

  TO_STRING_KV(K_(tenant_id), K_(job_name), K_(job_id), K_(job_status),
               K_(audit_trail_type), K_(job_interval),
               K_(job_frequency), K_(job_flags), K_(job_zone));
};

struct ObAuditParam {
public:
  ObAuditParam() :
   job_interval_(0),
   audit_trail_type_(0),
   job_status_(0),
   last_arch_ts_(0),
   job_flags_(0),
   use_last_arch_ts_(true),
   job_name_() {}

public:
  uint64_t job_interval_; // time quantity, dimension is: minutes
  uint64_t audit_trail_type_;
  uint64_t job_status_;
  uint64_t last_arch_ts_;
  int64_t job_flags_;
  bool use_last_arch_ts_;
  common::ObString job_name_;

  TO_STRING_KV(K_(job_status), K_(job_interval), K_(job_flags), K_(last_arch_ts),
               K_(audit_trail_type), K_(use_last_arch_ts), K_(job_name));
};

struct ParamHandlerPair {
  using ParamHandler = std::function<void(const ObString params)>;
  TrailParamType type_;
  ParamHandler handler_;
};

public:
  static int clean_audit_trail(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int clean_audit_trail_impl(ObAuditParam &param, ObIAllocator &allocator, ObISQLClient *sql_proxy, uint64_t tenant_id);
  static int clean_table_data_page_size(ObISQLClient *trans, const uint64_t tenant_id, const int64_t end_ts, uint64_t &offset, bool &is_finish);
  static int clean_table_data_page_size2(ObISQLClient *trans, const uint64_t tenant_id, const int64_t end_ts, uint64_t &offset, bool &is_finish);
  static int clean_table_data(ObISQLClient *trans, const uint64_t tenant_id, const int64_t timestamp);
  static int get_last_arch_ts(ObIAllocator &allocator, ObISQLClient *sql_proxy, const uint64_t tenant_id, AuditTrailType trail_type, int64_t &timestamp);
  static int extract_last_arch_ts_info(sqlclient::ObMySQLResult &result, ObIAllocator &allocator, ObLastArchTsInfo &ts_info);
  static bool is_valid_trail_type(const AuditTrailType trail_type);
  static bool is_clean_file_data(const AuditTrailType trail_type);
  static int clean_file_data(ObIAllocator &allocator, const char *dir_name, int64_t timestamp);
  static int del_audit_files(ObIArray<ObString> &del_file_names);
  static int traverse_audit_file_name(ObIAllocator &allocator,
                                      const char *dir_name,
                                      int64_t timestamp,
                                      ObIArray<ObString> &del_file_names);
  static int parse_file_name(ObIAllocator &allocator,
                             const char *dir_name,
                             const char *file_name,
                             const int64_t timestamp,
                             ObIArray<ObString> &del_file_names);
  static int check_and_get_file_name(const char *file_name, ObString &ts_part);
  static bool is_valid_time(const struct tm &tm);

  static int update_last_arch_ts_info(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int update_last_arch_ts_info_impl(common::ObISQLClient &trans, uint64_t tenant_id, int32_t trail_type, int64_t timestamp);
  static int update_clean_job_info(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int update_clean_job_info_mysql(common::ObISQLClient &trans, uint64_t tenant_id, ObAuditParam &audit_param, uint64_t job_id);
  static int delete_last_arch_ts_info(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int update_job_interval_oracle(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int set_purge_job_status_oracle(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int drop_purge_job_oracle(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  // mysql
  static int split_params(const ObString &params, ObIArray<ObString> &parray);
  static int handle_purge_job_info_mysql(ObSysVarSchema &var_schema, ObISQLClient &trans, uint64_t tenant_id);
  static int parse_audit_param_mysql(const ObString &param, const TrailParamType &param_type, ObAuditParam &audit_param);
  static int parse_mysql_job_param(const ObString &params, ObAuditParam &audit_param);
  static int mock_audit_purge_job(dbms_job::ObDBMSJobInfo &job_info, ObSqlString &sql);
  static int handle_audit_param_mysql(ObSysVariableSchema &var_schema, ObISQLClient &trans);
  static int get_job_id(ObISQLClient &trans, uint64_t tenant_id, uint64_t &job_id);
  static int get_cwd_str(char *cwd, uint64_t length);
  static int drop_purge_job_mysql(ObISQLClient &trans, uint64_t tenant_id, const ObString &job_name);
  static int get_job_id_from_name(ObISQLClient &trans, uint64_t tenant_id,
                                          const ObString &job_name, uint64_t &job_id);
  static int set_last_arch_ts_mysql(ObISQLClient &trans, uint64_t tenant_id, ObSysVarSchema &var_schema);
  static int set_purge_job_interval_mysql(ObISQLClient &trans, uint64_t tenant_id, ObSysVarSchema &var_schema);
  static int set_purge_job_status_mysql(ObISQLClient &trans, uint64_t tenant_id, ObSysVarSchema &var_schema);
  static int clear_last_arch_ts_mysql(ObISQLClient &trans, uint64_t tenant_id, ObSysVarSchema &var_schema);
  static int get_job_flag_from_id(ObISQLClient &trans, uint64_t tenant_id, uint64_t job_id, uint64_t &flag);
};

} // namespace pl
} // namespace oceanbase

#endif