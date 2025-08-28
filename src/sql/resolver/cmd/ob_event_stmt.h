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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_EVENT_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_EVENT_STMT_

#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "lib/string/ob_strings.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

class ObEventInfo
{
public:
  enum class ObEventBoolType : int64_t
  {
    NOT_SET = 0,
    SET_TRUE,
    SET_FALSE
  };
  ObEventInfo() :
    tenant_id_(common::OB_INVALID_ID),
    user_id_(common::OB_INVALID_ID),
    database_id_(common::OB_INVALID_ID),
    start_time_(common::OB_INVALID_TIMESTAMP),
    end_time_(common::OB_INVALID_TIMESTAMP),
    max_run_duration_(0),
    if_exist_or_if_not_exist_(false),
    is_enable_(ObEventBoolType::NOT_SET),
    auto_drop_(ObEventBoolType::NOT_SET),
    event_definer_(),
    event_database_(),
    event_name_(),
    repeat_interval_(),
    event_body_(),
    event_comment_(),
    event_rename_() {}

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_database_id(const uint64_t id) { database_id_ = id; }
  inline void set_user_id(const uint64_t id) { user_id_ = id; }
  inline void set_event_definer(const char *event_definer) {  event_definer_ = event_definer; }
  inline void set_event_definer(const common::ObString &event_definer) {  event_definer_ = event_definer; }
  inline void set_event_name(const common::ObString &event_name) {  event_name_ = event_name; }
  inline void set_event_body(const common::ObString &event_body) {  event_body_ = event_body; }
  inline void set_event_comment(const common::ObString &event_comment) {  event_comment_ = event_comment; }
  inline void set_repeat_interval(const common::ObString &repeat_interval) {  repeat_interval_ = repeat_interval; }
  inline void set_is_enable(const ObEventBoolType is_enable) { is_enable_ = is_enable; }
  inline void set_auto_drop(const ObEventBoolType auto_drop) { auto_drop_ = auto_drop; }
  inline void set_start_time(const int64_t start_time) { start_time_ = start_time;  }
  inline void set_end_time(const int64_t end_time) { end_time_ = end_time; }
  inline void set_max_run_duration(const int64_t max_run_duration) { max_run_duration_ = max_run_duration; }
  inline void set_event_rename(const common::ObString &event_rename) {  event_rename_ = event_rename; }
  inline void set_event_database(const common::ObString &event_database) {  event_database_ = event_database; }
  inline void set_if_exist_or_if_not_exist(bool exist) { if_exist_or_if_not_exist_ = exist; }
  inline void set_exec_env(const common::ObString &exec_env) {  exec_env_ = exec_env; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_user_id() const { return user_id_; }
  inline const common::ObString &get_event_definer() const { return event_definer_; }
  inline const common::ObString &get_event_name() const { return event_name_; }
  inline const common::ObString &get_event_body() const { return event_body_; }
  inline const common::ObString &get_event_comment() const { return event_comment_; }
  inline bool get_if_exist_or_if_not_exist() { return if_exist_or_if_not_exist_; }
  inline ObEventBoolType get_is_enable() const { return is_enable_; }
  inline ObEventBoolType get_auto_drop() const { return auto_drop_; }
  inline const common::ObString &get_repeat_interval() const { return repeat_interval_; }
  inline int64_t get_start_time() const { return start_time_; }
  inline int64_t get_end_time() const { return end_time_; }
  inline int64_t get_max_run_duration() const { return max_run_duration_; }
  inline const common::ObString &get_event_rename() const { return event_rename_; }
  inline const common::ObString &get_event_database() const { return event_database_; }
  inline const common::ObString &get_exec_env() const { return exec_env_; }

  TO_STRING_KV(K(tenant_id_),
               K(user_id_),
               K(database_id_),
               K(start_time_),
               K(end_time_),
               K(max_run_duration_),
               K(if_exist_or_if_not_exist_),
               K(is_enable_),
               K(auto_drop_),
               K(event_definer_),
               K(event_database_),
               K(event_name_),
               K(repeat_interval_),
               K(event_body_),
               K(event_comment_),
               K(event_rename_));  

private:
  // data members
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t max_run_duration_;
  bool if_exist_or_if_not_exist_;
  ObEventBoolType is_enable_;
  ObEventBoolType auto_drop_;
  common::ObString event_definer_;
  common::ObString event_database_;
  common::ObString event_name_;
  common::ObString repeat_interval_;
  common::ObString event_body_;
  common::ObString event_comment_;
  common::ObString event_rename_;
  common::ObString exec_env_;
};

class ObCreateEventStmt: public ObCMDStmt
{
public:
  explicit ObCreateEventStmt(common::ObIAllocator *name_pool);
  ObCreateEventStmt();
  virtual ~ObCreateEventStmt() = default;

  ObEventInfo& get_event_info() { return event_info_; }
  const ObEventInfo& get_event_info() const { return event_info_; }

  DECLARE_VIRTUAL_TO_STRING;
private:
  ObEventInfo event_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateEventStmt);
};

class ObAlterEventStmt: public ObCMDStmt
{
public:
  explicit ObAlterEventStmt(common::ObIAllocator *name_pool);
  ObAlterEventStmt();
  virtual ~ObAlterEventStmt() = default;

  ObEventInfo& get_event_info() { return event_info_; }
  const ObEventInfo& get_event_info() const { return event_info_; }

  DECLARE_VIRTUAL_TO_STRING;
private:
  ObEventInfo event_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterEventStmt);
};

class ObDropEventStmt: public ObCMDStmt
{
public:
  explicit ObDropEventStmt(common::ObIAllocator *name_pool);
  ObDropEventStmt();
  virtual ~ObDropEventStmt() = default;

  ObEventInfo& get_event_info() { return event_info_; }
  const ObEventInfo& get_event_info() const { return event_info_; }

  DECLARE_VIRTUAL_TO_STRING;
private:
  ObEventInfo event_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDropEventStmt);
};
} // end namespace sql
} // end namespace oceanbase
#endif //OCEANBASE_SQL_RESOLVER_CMD_OB_ALTER_EVENT_STMT_
