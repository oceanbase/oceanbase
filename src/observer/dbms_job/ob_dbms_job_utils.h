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

#ifndef SRC_OBSERVER_DBMS_JOB_UTILS_H_
#define SRC_OBSERVER_DBMS_JOB_UTILS_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/container/ob_iarray.h"
#include "lib/queue/ob_priority_queue.h"

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

namespace dbms_job
{

class ObDBMSJobInfo
{
public:
  ObDBMSJobInfo() :
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
    exec_env_() {}

  TO_STRING_KV(K(tenant_id_),
               K(job_),
               K(lowner_),
               K(powner_),
               K(cowner_),
               K(last_modify_),
               K(last_date_),
               K(this_date_),
               K(next_date_),
               K(total_),
               K(interval_),
               K(failures_),
               K(flag_),
               K(what_),
               K(nlsenv_),
               K(charenv_),
               K(field1_),
               K(scheduler_flags_));

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

  bool is_broken() { return 0x1 == (flag_ & 0x1); }
  bool is_mysql_audit_job() { return !!(flag_ & JOB_FLAG_MASK_MYSQL); }
  bool is_running(){ return this_date_ != 0; }
  bool is_broadcast() { return 0 == get_zone().case_compare(__ALL_SERVER_BC); }

  common::ObString &get_what() { return what_; }
  common::ObString &get_exec_env() { return exec_env_; }
  common::ObString &get_lowner() { return lowner_; }
  common::ObString &get_zone() { return field1_; }
  common::ObString &get_interval() { return interval_; }

  int deep_copy(common::ObIAllocator &allocator, const ObDBMSJobInfo &other);

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

public:
  static const int64_t JOB_FLAG_MASK_BROKEN = 1;
  static const int64_t JOB_FLAG_MASK_MYSQL = 2;
  static const char *__ALL_SERVER_BC; // broadcast
};

class ObDBMSJobUtils
{
public:
  ObDBMSJobUtils() : sql_proxy_(NULL) {}
  virtual ~ObDBMSJobUtils() {};

  int init(common::ObISQLClient *sql_proxy) { sql_proxy_ = sql_proxy; return common::OB_SUCCESS; }

  int update_for_start(
    uint64_t tenant_id, ObDBMSJobInfo &job_info, bool update_nextdate = true);
  int update_for_end(
    uint64_t tenant_id, ObDBMSJobInfo &job_info, int err, const common::ObString &errmsg);
  int update_nextdate(uint64_t tenant_id, ObDBMSJobInfo &job_info);

  int get_dbms_job_info(
    uint64_t tenant_id, uint64_t job_id,
    common::ObIAllocator &allocator, ObDBMSJobInfo &job_info);
  int get_dbms_job_infos_in_tenant(
    uint64_t tenant_id,
    common::ObIAllocator &allocator, common::ObIArray<ObDBMSJobInfo> &job_infos);

  int extract_info(
    common::sqlclient::ObMySQLResult &result,
    common::ObIAllocator &allocator, ObDBMSJobInfo &job_info);

  int calc_execute_at(
    ObDBMSJobInfo &job_info, int64_t &execute_at, int64_t &delay, bool ignore_nextdate = false);

  int check_job_can_running(int64_t tenant_id, bool &can_running);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDBMSJobUtils);

private:
  common::ObISQLClient *sql_proxy_;
};

using ObDBMSJobQueue = common::ObPriorityQueue<1>;
}
}

#endif /* SRC_OBSERVER_DBMS_JOB_UTILS_H_ */
