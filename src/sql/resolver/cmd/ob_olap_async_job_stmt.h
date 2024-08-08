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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_OLAP_ASYNC_JOB_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_OLAP_ASYNC_JOB_STMT_

#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "lib/string/ob_strings.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObOLAPAsyncSubmitJobStmt: public ObCMDStmt
{
public:
  explicit ObOLAPAsyncSubmitJobStmt(common::ObIAllocator *name_pool);
  ObOLAPAsyncSubmitJobStmt();
  virtual ~ObOLAPAsyncSubmitJobStmt() = default;

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_database_id(const uint64_t id) { database_id_ = id; }
  inline void set_user_id(const uint64_t id) { user_id_ = id; }
  inline void set_job_id(const int64_t id) { job_id_ = id; }
  inline void set_query_time_out_second(const uint64_t time) { query_time_out_second_ = time; }
  inline void set_job_name(const common::ObString &job_name) {  job_name_ = job_name; }
  inline void set_job_definer(const common::ObString &job_definer) {  job_definer_ = job_definer; }
  inline void set_job_database(const common::ObString &job_database) {  job_database_ = job_database; }
  inline void set_job_action(const common::ObString &job_action) {  job_action_ = job_action; }
  inline void set_exec_env(const common::ObString &exec_env) {  exec_env_ = exec_env; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_user_id() const { return user_id_; }
  inline int64_t get_job_id() const { return job_id_; }
  inline uint64_t get_query_time_out_second() const { return query_time_out_second_; }
  inline const common::ObString &get_job_name() const { return job_name_; }
  inline const common::ObString &get_job_definer() const { return job_definer_; }
  inline const common::ObString &get_job_action() const { return job_action_; }
  inline const common::ObString &get_exec_env() const { return exec_env_; }
  inline const common::ObString &get_job_database() const { return job_database_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t user_id_;
  int64_t job_id_;
  uint64_t query_time_out_second_;
  common::ObString job_name_;
  common::ObString job_definer_;
  common::ObString job_database_;
  common::ObString job_action_;
  common::ObString exec_env_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOLAPAsyncSubmitJobStmt);
};

class ObOLAPAsyncCancelJobStmt: public ObCMDStmt
{
public:
  explicit ObOLAPAsyncCancelJobStmt(common::ObIAllocator *name_pool);
  ObOLAPAsyncCancelJobStmt();
  virtual ~ObOLAPAsyncCancelJobStmt() = default;

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_user_id(const uint64_t id) { user_id_ = id; }
  inline void set_job_name(const common::ObString &job_name) {  job_name_ = job_name; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_user_id() const { return user_id_; }
  inline const common::ObString &get_job_name() const { return job_name_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString job_name_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOLAPAsyncCancelJobStmt);
};

} // end namespace sql
} // end namespace oceanbase
#endif //OCEANBASE_SQL_RESOLVER_CMD_OB_OLAP_ASYNC_JOB_STMT_