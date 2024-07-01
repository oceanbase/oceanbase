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

#ifndef SRC_OBSERVER_DBMS_SCHED_JOB_UTILS_H_
#define SRC_OBSERVER_DBMS_SCHED_JOB_UTILS_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/container/ob_iarray.h"


#define DATA_VERSION_SUPPORT_JOB_CLASS(data_version) (data_version >= DATA_VERSION_4_3_2_0)
#define DATA_VERSION_SUPPORT_RUN_DETAIL_V2(data_version) ((MOCK_DATA_VERSION_4_2_4_0 <= data_version && DATA_VERSION_4_3_0_0 > data_version) || DATA_VERSION_4_3_2_0 <= data_version)

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
namespace sql
{
class ObExecEnv;
class ObSQLSessionInfo;
class ObFreeSessionCtx;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObUserInfo;
}
}

namespace dbms_scheduler
{

class ObDBMSSchedJobInfo
{
public:
  ObDBMSSchedJobInfo() :
    tenant_id_(common::OB_INVALID_ID),
    job_(common::OB_INVALID_ID),
    lowner_(),
    powner_(),
    cowner_(),
    last_modify_(0),
    last_date_(0),
    this_date_(0),
    next_date_(0),
    total_(0),
    interval_(),
    failures_(0),
    flag_(0),
    what_(),
    nlsenv_(),
    charenv_(),
    field1_(),
    scheduler_flags_(0),
    exec_env_(),
    job_name_(),
    job_style_(),
    program_name_(),
    job_type_(),
    job_action_(),
    number_of_argument_(0),
    start_date_(0),
    repeat_interval_(),
    end_date_(0),
    job_class_(),
    enabled_(false),
    auto_drop_(false),
    state_(),
    run_count_(0),
    retry_count_(0),
    last_run_duration_(0),
    max_run_duration_(0),
    comments_(),
    credential_name_(),
    destination_name_(),
    interval_ts_(),
    is_oracle_tenant_(true) {}

  TO_STRING_KV(K(tenant_id_),
               K(job_),
               K(job_name_),
               K(lowner_),
               K(powner_),
               K(cowner_),
               K(last_modify_),
               K(start_date_),
               K(last_date_),
               K(this_date_),
               K(next_date_),
               K(end_date_),
               K(total_),
               K(interval_),
               K(repeat_interval_),
               K(failures_),
               K(flag_),
               K(what_),
               K(nlsenv_),
               K(charenv_),
               K(field1_),
               K(scheduler_flags_),
               K(enabled_),
               K(auto_drop_),
               K(max_run_duration_),
               K(interval_ts_));

  bool valid()
  {
    return tenant_id_ != common::OB_INVALID_ID
            && job_ != common::OB_INVALID_ID
            && !exec_env_.empty();
  }

  uint64_t get_tenant_id() { return tenant_id_; }
  uint64_t get_job_id() { return job_; }
  uint64_t get_job_id_with_tenant() { return common::combine_two_ids(tenant_id_, job_); }
  int64_t  get_this_date() { return this_date_; }
  int64_t  get_next_date() { return next_date_; }
  int64_t  get_last_date() { return last_date_; }
  int64_t  get_last_modify() { return last_modify_; }
  int64_t  get_interval_ts() { return interval_ts_; }
  int64_t  get_max_run_duration() { return (max_run_duration_ == 0) ? 30 : max_run_duration_ ; } // 30s by default
  int64_t  get_start_date() { return start_date_; }
  int64_t  get_end_date() { return end_date_; }
  int64_t  get_auto_drop() { return auto_drop_; }

  bool is_broken() { return 0x1 == (flag_ & 0x1); }
  bool is_running(){ return this_date_ != 0; }
  bool is_disabled() { return 0x0 == (enabled_ & 0x1); }

  common::ObString &get_what() { return what_; }
  common::ObString &get_exec_env() { return exec_env_; }
  common::ObString &get_lowner() { return lowner_; }
  common::ObString &get_powner() { return powner_; }
  common::ObString &get_cowner() { return cowner_; }
  common::ObString &get_zone() { return field1_; }
  common::ObString &get_interval() { return interval_; }
  common::ObString &get_program_name() { return program_name_; }
  common::ObString &get_job_name() { return job_name_; }
  common::ObString &get_job_class() { return job_class_; }

  bool is_oracle_tenant() { return is_oracle_tenant_; }
  bool is_date_expression_job_class() const { return !!(scheduler_flags_ & JOB_SCHEDULER_FLAG_DATE_EXPRESSION_JOB_CLASS); }

  int deep_copy(common::ObIAllocator &allocator, const ObDBMSSchedJobInfo &other);

public:
  uint64_t tenant_id_;
  uint64_t job_;
  common::ObString lowner_;
  common::ObString powner_;
  common::ObString cowner_;
  int64_t last_modify_;
  int64_t last_date_;
  int64_t this_date_;
  int64_t next_date_;
  int64_t total_;
  common::ObString interval_;
  int64_t failures_;
  int64_t flag_;
  common::ObString what_;
  common::ObString nlsenv_;
  common::ObString charenv_;
  common::ObString field1_;
  int64_t scheduler_flags_;
  common::ObString exec_env_;
  common::ObString job_name_;
  common::ObString job_style_;
  common::ObString program_name_;
  common::ObString job_type_;
  common::ObString job_action_;
  uint64_t number_of_argument_;
  int64_t start_date_;
  common::ObString repeat_interval_;
  int64_t end_date_;
  common::ObString job_class_;
  bool enabled_;
  bool auto_drop_;
  common::ObString state_;
  int64_t  run_count_;
  int64_t retry_count_;
  int64_t last_run_duration_;
  int64_t max_run_duration_;
  common::ObString comments_;
  common::ObString credential_name_;
  common::ObString destination_name_;
  int64_t interval_ts_;
  bool is_oracle_tenant_;

public:
  static const int64_t JOB_SCHEDULER_FLAG_DATE_EXPRESSION_JOB_CLASS = 1;
};

class ObDBMSSchedJobClassInfo
{
public:
  ObDBMSSchedJobClassInfo() :
    tenant_id_(common::OB_INVALID_ID),
    job_class_name_(),
    resource_consumer_group_(),
    logging_level_(),
    log_history_(0),
    comments_(),
    is_oracle_tenant_(true) {}

  TO_STRING_KV(K(tenant_id_),
              K(job_class_name_),
              K(service_),
              K(resource_consumer_group_),
              K(logging_level_),
              K(log_history_),
              K(comments_));
  bool valid()
  {
    return tenant_id_ != common::OB_INVALID_ID
            && !job_class_name_.empty();
  }
  uint64_t get_tenant_id() { return tenant_id_; }
  uint64_t get_log_history() { return log_history_; }
  common::ObString &get_job_class_name() { return job_class_name_; }
  common::ObString &get_service() { return service_; }
  common::ObString &get_resource_consumer_group() { return resource_consumer_group_; }
  common::ObString &get_logging_level() { return logging_level_; }
  common::ObString &get_comments() { return comments_; }
  bool is_oracle_tenant() { return is_oracle_tenant_; }
  int deep_copy(common::ObIAllocator &allocator, const ObDBMSSchedJobClassInfo &other);
public:
  uint64_t tenant_id_;
  common::ObString job_class_name_;
  common::ObString service_;
  common::ObString resource_consumer_group_;
  common::ObString logging_level_;
  uint64_t log_history_;
  common::ObString comments_;
  bool is_oracle_tenant_;
};

class ObDBMSSchedJobUtils
{
public:
  static int disable_dbms_sched_job(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const common::ObString &job_name,
                                    const bool if_exists = false);
  static int remove_dbms_sched_job(common::ObISQLClient &sql_client,
                                  const uint64_t tenant_id,
                                  const common::ObString &job_name,
                                  const bool if_exists = false);
  static int create_dbms_sched_job(common::ObISQLClient &sql_client,
                                   const uint64_t tenant_id,
                                   const int64_t job_id,
                                   const ObDBMSSchedJobInfo &job_info);
  static int add_dbms_sched_job(common::ObISQLClient &sql_client,
                                const uint64_t tenant_id,
                                const int64_t job_id,
                                const ObDBMSSchedJobInfo &job_info);
  static int reserve_user_with_minimun_id(ObIArray<const ObUserInfo *> &user_infos);
};
}
}

#endif /* SRC_OBSERVER_DBMS_SCHED_JOB_UTILS_H_ */
