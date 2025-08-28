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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_event_resolver.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObEventResolver::ObEventResolver(ObResolverParams &params)
    : ObCMDResolver(params)
{
}

int ObEventResolver::resolve(const ParseNode &parse_tree)
{

  int ret = OB_SUCCESS;
  stmt_type_ = parse_tree.type_;
  switch (stmt_type_) {
  case T_EVENT_JOB_CREATE: {
    ObCreateEventStmt *stmt = create_stmt<ObCreateEventStmt>();
    OV (OB_NOT_NULL(stmt), OB_ALLOCATE_MEMORY_FAILED);
    OZ (resolve_create_event_stmt(parse_tree, stmt->get_event_info()));
    break;
  }
  case T_EVENT_JOB_ALTER: {
    ObAlterEventStmt *stmt = create_stmt<ObAlterEventStmt>();
    OV (OB_NOT_NULL(stmt), OB_ALLOCATE_MEMORY_FAILED);
    OZ (resolve_alter_event_stmt(parse_tree, stmt->get_event_info()));
    break;
  }
  case T_EVENT_JOB_DROP: {
    ObDropEventStmt *stmt = create_stmt<ObDropEventStmt>();
    OV (OB_NOT_NULL(stmt), OB_ALLOCATE_MEMORY_FAILED);
    OZ (resolve_drop_event_stmt(parse_tree, stmt->get_event_info()));
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stmt type", K(ret), K(stmt_type_));
  }
  return ret;
}

int ObEventResolver::resolve_create_event_stmt(const ParseNode &parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;

  OV (T_EVENT_JOB_CREATE == parse_node.type_, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (8 == parse_node.num_child_, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OX (event_info.set_tenant_id(session_info_->get_effective_tenant_id()));
  
  OZ (resolve_event_definer(parse_node.children_[0], event_info));
  OZ (resolve_event_exist(parse_node.children_[1], event_info));
  OZ (resolve_event_name(parse_node.children_[2], event_info));
  OZ (resolve_event_schedule(parse_node.children_[3], event_info));
  OZ (resolve_event_preserve(parse_node.children_[4], event_info));
  OZ (resolve_event_enable(parse_node.children_[5], event_info));
  OZ (resolve_event_comment(parse_node.children_[6], event_info));
  OZ (resolve_event_body(parse_node.children_[7], event_info));

  OZ (get_event_exec_env(event_info));
  return ret;
}

int ObEventResolver::resolve_alter_event_stmt(const ParseNode &parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  OV (T_EVENT_JOB_ALTER == parse_node.type_, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (7 == parse_node.num_child_, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OX (event_info.set_tenant_id(session_info_->get_effective_tenant_id()));
  if (OB_NOT_NULL(parse_node.children_[0])) {
    OZ (resolve_event_definer(parse_node.children_[0], event_info));
  }
  OZ (resolve_event_name(parse_node.children_[1], event_info));
  OZ (resolve_schedule_and_comple(parse_node.children_[2], event_info));
  OZ (resolve_event_rename(parse_node.children_[3], event_info));
  OZ (resolve_event_enable(parse_node.children_[4], event_info));
  OZ (resolve_event_comment(parse_node.children_[5], event_info));
  if (OB_NOT_NULL(parse_node.children_[6])) {
    OZ (resolve_event_body(parse_node.children_[6], event_info));
  }
  return ret;
}

int ObEventResolver::resolve_drop_event_stmt(const ParseNode &parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  OV (T_EVENT_JOB_DROP == parse_node.type_, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (2 == parse_node.num_child_, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OX (event_info.set_tenant_id(session_info_->get_effective_tenant_id()));  
  OZ (resolve_event_exist(parse_node.children_[0], event_info));
  OZ (resolve_event_name(parse_node.children_[1], event_info));
  return ret;
}

int ObEventResolver::resolve_event_definer(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(schema_checker_));
  CK(OB_NOT_NULL(schema_checker_->get_schema_guard()));
  CK(OB_NOT_NULL(session_info_));
  CK(OB_NOT_NULL(allocator_));
  ObString user_name, host_name;
  ObString cur_user_name, cur_host_name;
  cur_user_name = session_info_->get_user_name();
  cur_host_name = session_info_->get_host_name();
  if (OB_NOT_NULL(parse_node)) {
    CK(T_USER_WITH_HOST_NAME == parse_node->type_);
    if (OB_SUCC(ret)) {
      const ParseNode *user_node = parse_node->children_[0];
      const ParseNode *host_node = parse_node->children_[1];

      if (OB_ISNULL(user_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user must be specified", K(ret));
      } else {
        // 需要检查当前用户是否有超级权限或者set user id的权限
        if (!session_info_->has_user_super_privilege()) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("no privilege", K(ret));
        } else {
          user_name.assign_ptr(user_node->str_value_, static_cast<int32_t>(user_node->str_len_));
          // 得区分current_user和“current_user”, 前者需要获取当前用户和host，后者是作为用户名存在
          if (0 == user_name.case_compare("current_user") && T_IDENT == user_node->type_) {
            user_name = cur_user_name;
            host_name = cur_host_name;
          } else if (OB_ISNULL(host_node)) {
            host_name.assign_ptr("%", 1);
          } else {
            host_name.assign_ptr(host_node->str_value_, static_cast<int32_t>(host_node->str_len_));
          }
        }
        if (OB_SUCC(ret)) {
          // 检查user@host是否在mysql.user表中
          const ObUserInfo* user_info = nullptr;
          if (OB_FAIL(schema_checker_->get_schema_guard()->get_user_info(session_info_->get_effective_tenant_id(),
                                                                         user_name,
                                                                         host_name,
                                                                         user_info))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get_user_info", K(ret));
          } else if (OB_ISNULL(user_info)) {
            ret = OB_ERR_USER_NOT_EXIST;
            LOG_WARN("set definer not exist", K(ret));
          } else {
            event_info.set_user_id(user_info->get_user_id());
          }
        }
      }
    }
  } else if (lib::is_mysql_mode()) {
    // 不指定definer时，默认为当前用户和host
    user_name = cur_user_name;
    host_name = cur_host_name;
    event_info.set_user_id(session_info_->get_user_id());
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    //user@host作为一个整体存储到priv_user字段
    char* tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_DEFINER_MAX_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc geo tree obj");
    } else {
      snprintf(tmp_buf, OB_EVENT_DEFINER_MAX_LEN, "%.*s@%.*s", user_name.length(), user_name.ptr(),
                                                    host_name.length(), host_name.ptr());
      ObString definer_user(tmp_buf);
      if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                *allocator_, session_info_->get_dtc_params(), definer_user))) {
        LOG_WARN("fail to convert charset", K(ret));
      } else {
        event_info.set_event_definer(definer_user);
      }
    }
  }

  return ret;  
}

int ObEventResolver::resolve_event_exist(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_node)) {
    event_info.set_if_exist_or_if_not_exist(false);
  } else if (T_IF_NOT_EXISTS == parse_node->type_ || T_IF_EXISTS == parse_node->type_) {
    event_info.set_if_exist_or_if_not_exist(true);
  }
  return ret;
}

int ObEventResolver::resolve_event_name(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  OV (OB_NOT_NULL(parse_node), OB_INVALID_ARGUMENT);
  OV (T_SP_NAME == parse_node->type_, OB_ERR_UNEXPECTED, parse_node->type_);
  OV (2 == parse_node->num_child_, OB_ERR_UNEXPECTED, parse_node->num_child_);
  if (OB_SUCC(ret)) {
    ObString db_name, event_name;
    if (OB_FAIL(ObResolverUtils::resolve_sp_name(*session_info_, *parse_node, db_name, event_name))) {
      LOG_WARN("get sp name failed", K(ret), KP(parse_node));
    } else if (0 != db_name.compare(session_info_->get_database_name())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create event in non-current database");
      LOG_WARN("not support create other database", K(ret), K(db_name), K(session_info_->get_database_name()));
    } else {
      event_info.set_event_database(db_name);
      event_info.set_database_id(session_info_->get_database_id());
      char *event_name_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_NAME_MAX_LEN));
      if (OB_ISNULL(event_name_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("faild to alloc memory for event name", K(ret));
      } else {
        memset(event_name_buf, 0, OB_EVENT_NAME_MAX_LEN);
        snprintf(event_name_buf, OB_EVENT_NAME_MAX_LEN, "%lu.%.*s", session_info_->get_database_id(), event_name.length(), event_name.ptr());
        event_info.set_event_name(event_name_buf);
      }
    }
  }
  return ret;
}

int ObEventResolver::resolve_event_schedule(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  if (OB_ISNULL(parse_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse schedule node is null", K(ret));
  } else if (1 == parse_node->value_) {
    int64_t start_time_us = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(get_event_time_node_value(parse_node->children_[0], start_time_us))){
      LOG_WARN("event get at time failed", K(ret), KP(parse_node));
    } else if (stmt_type_ == T_EVENT_JOB_ALTER && start_time_us < now) {
      ret = OB_ERR_EVENT_CANNOT_ALTER_IN_THE_PAST;
      LOG_WARN("start time invalid", K(ret), K(start_time_us), K(now));
    } else {
      event_info.set_start_time(start_time_us);
      event_info.set_end_time(OB_INVALID_TIMESTAMP);
      event_info.set_max_run_duration(0);
    }
  //repeat
  } else if (2 == parse_node->value_ && 3 == parse_node->num_child_) {
    const ParseNode *repeat_num_node = parse_node->children_[0];
    const ParseNode *repeat_type_node = parse_node->children_[1];
    const ParseNode *range_node = parse_node->children_[2];
    char *repeat_interval = static_cast<char*>(allocator_->alloc(OB_EVENT_REPEAT_MAX_LEN));
    if (OB_ISNULL(repeat_interval)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("faild to alloc memory", K(ret));
    } else {
      int64_t max_run_duration = 0;
      memset(repeat_interval, 0, OB_EVENT_REPEAT_MAX_LEN);
      if (OB_FAIL(get_repeat_interval(repeat_num_node, repeat_type_node, repeat_interval, max_run_duration))){
        LOG_WARN("event get repeat interval str failed", K(ret), KP(parse_node));
      } else {
        event_info.set_max_run_duration(max_run_duration);
        event_info.set_repeat_interval(repeat_interval);
        if (OB_ISNULL(range_node)) {
          event_info.set_start_time(now);
          event_info.set_end_time(OB_INVALID_TIMESTAMP);
        } else {
          const ParseNode *start_time_node = range_node->children_[0];
          const ParseNode *end_time_node = range_node->children_[1];
          if (OB_NOT_NULL(start_time_node)) {
            int64_t start_time_us = OB_INVALID_TIMESTAMP;
            if (OB_FAIL(get_event_time_node_value(start_time_node->children_[0], start_time_us))) {
              LOG_WARN("event get time us failed", K(ret), KP(start_time_node));
            } else if (stmt_type_ == T_EVENT_JOB_ALTER && start_time_us < now) {
              ret = OB_ERR_EVENT_CANNOT_ALTER_IN_THE_PAST;
              LOG_WARN("start time invalid", K(ret), K(start_time_us), K(now));
            } else {
              event_info.set_start_time(start_time_us);
            }
          } else {
            event_info.set_start_time(now);
          }
          if (OB_NOT_NULL(end_time_node)) {
            int64_t end_time_us = OB_INVALID_TIMESTAMP;
            if (OB_FAIL(get_event_time_node_value(end_time_node->children_[0], end_time_us))) {
              LOG_WARN("event get time str failed", K(ret), KP(end_time_node));
            } else {
              event_info.set_end_time(end_time_us);
            }          
          } else {
            event_info.set_end_time(OB_INVALID_TIMESTAMP);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_INVALID_TIMESTAMP != event_info.get_end_time() && event_info.get_end_time() < event_info.get_start_time()) {
    ret = OB_ERR_EVENT_ENDS_BEFORE_STARTS;
    LOG_WARN("ends before starts", K(ret), K(event_info.get_end_time()), K(event_info.get_start_time()));
  }
  return ret;
}

int ObEventResolver::resolve_event_preserve(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  //1 preserve 0 not preserve
  if (OB_ISNULL(parse_node)) {
    event_info.set_auto_drop(ObEventInfo::ObEventBoolType::NOT_SET);
  } else if (1 == parse_node->value_) {
    event_info.set_auto_drop(ObEventInfo::ObEventBoolType::SET_FALSE);
  } else {
    event_info.set_auto_drop(ObEventInfo::ObEventBoolType::SET_TRUE);
  }
  return ret;
}

int ObEventResolver::resolve_event_enable(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  //0 disable 1 enable
  if (OB_ISNULL(parse_node)) {
    event_info.set_is_enable(ObEventInfo::ObEventBoolType::NOT_SET);
  } else if (1 == parse_node->value_) {
    event_info.set_is_enable(ObEventInfo::ObEventBoolType::SET_TRUE);
  } else {
    event_info.set_is_enable(ObEventInfo::ObEventBoolType::SET_FALSE);
  }
  return ret;
}
int ObEventResolver::resolve_event_comment(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parse_node)) {
    if (OB_EVENT_COMMENT_MAX_LEN < parse_node->str_len_) {
      ret = OB_ERR_EVENT_DATA_TOO_LONG;
      ObString error_string("event comment");
      LOG_USER_ERROR(OB_ERR_EVENT_DATA_TOO_LONG, error_string.length(), error_string.ptr());
    } else {
      ObString event_comment;
      event_comment.assign_ptr(parse_node->str_value_, parse_node->str_len_);
      event_info.set_event_comment(event_comment);
    }
  }
  return ret;
}
int ObEventResolver::resolve_event_body(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  char *event_body_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_BODY_MAX_LEN));
  char *sql_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_SQL_MAX_LEN));
  if (OB_ISNULL(event_body_buf) || OB_ISNULL(sql_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    memset(event_body_buf, 0, OB_EVENT_BODY_MAX_LEN);
    int available_space = OB_EVENT_BODY_MAX_LEN;
    for (int sql_index = 0; sql_index <  parse_node->num_child_ && OB_SUCC(ret); sql_index++) {
      if (OB_EVENT_SQL_MAX_LEN - 1 < parse_node->children_[sql_index]->str_len_) {
        ret = OB_ERR_EVENT_DATA_TOO_LONG;
        ObString error_string("event body single SQL");
        LOG_USER_ERROR(OB_ERR_EVENT_DATA_TOO_LONG, error_string.length(), error_string.ptr());
        LOG_WARN("single sql too long", K(ret), K(parse_node->children_[sql_index]->str_len_), K(sql_index));
      } else {
        memset(sql_buf, 0, OB_EVENT_SQL_MAX_LEN);
        snprintf(sql_buf, OB_EVENT_SQL_MAX_LEN, "%.*s;", (int)parse_node->children_[sql_index]->str_len_, parse_node->children_[sql_index]->str_value_);
        if (parse_node->children_[sql_index]->str_len_ + 1 > available_space) {
          ret = OB_ERR_EVENT_DATA_TOO_LONG;
          ObString error_string("event body");
          LOG_USER_ERROR(OB_ERR_EVENT_DATA_TOO_LONG, error_string.length(), error_string.ptr());
          LOG_WARN("out of max length", K(ret), K(parse_node->children_[sql_index]->str_len_), K(available_space));
        } else {
          strncat(event_body_buf, sql_buf, available_space);
          available_space = available_space - (parse_node->children_[sql_index]->str_len_ + 1);
        }
      }
    }
    if (OB_SUCC(ret)) {
      event_info.set_event_body(event_body_buf); 
    }
  }
  return ret;
}

int ObEventResolver::resolve_schedule_and_comple(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_node)) { //not alter
    event_info.set_start_time(OB_INVALID_TIMESTAMP);
    event_info.set_auto_drop(ObEventInfo::ObEventBoolType::NOT_SET);
  } else {
    if (OB_ISNULL(parse_node->children_[0])) {
      event_info.set_start_time(OB_INVALID_TIMESTAMP);
    } else {
      OZ (resolve_event_schedule(parse_node->children_[0], event_info));
    }
    OZ(resolve_event_preserve(parse_node->children_[1], event_info));
  }
  return ret;
}

int ObEventResolver::resolve_event_rename(const ParseNode *parse_node, ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parse_node)) {
    char *event_name_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_NAME_MAX_LEN));
    if (OB_ISNULL(event_name_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("faild to alloc memory for event name", K(ret));
    } else {
      memset(event_name_buf, 0, OB_EVENT_NAME_MAX_LEN);
      const ParseNode *database_node = parse_node->children_[0];
      const ParseNode *rename_node = parse_node->children_[1];
      if (OB_NOT_NULL(database_node)) {
        ObString data_base(database_node->str_len_, database_node->str_value_);
        if (0 != data_base.compare(session_info_->get_database_name())) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter event name in non-current database");
          LOG_WARN("alter event rename dabase failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        snprintf(event_name_buf, OB_EVENT_NAME_MAX_LEN, "%lu.%s",session_info_->get_database_id(), rename_node->str_value_);
        event_info.set_event_rename(event_name_buf);  
        if (0 == event_info.get_event_name().compare(event_info.get_event_rename())) {
          ret = OB_ERR_EVENT_SAME_NAME;
        }
      }
    }
  }
  return ret;
}

int ObEventResolver::get_repeat_interval(const ParseNode *repeat_num_node, const ParseNode *repeat_type_node, char *repeat_interval_str, int64_t &max_run_duration) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(repeat_num_node) || OB_ISNULL(repeat_type_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("interval node is null",
             K(ret), KP(repeat_num_node), KP(repeat_type_node));
  } else {
    int64_t date_unit_interval = repeat_num_node->value_;
    const char *date_unit_type = repeat_type_node->str_value_;
    if (0 == date_unit_interval || OB_EVENT_INTERVAL_MAX_VALUE < date_unit_interval) {
      ret = OB_ERR_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG;
      LOG_WARN("date_unit_interval error",
             K(ret), K(date_unit_interval));      
    } else if (OB_ISNULL(date_unit_type)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("interval type is null",
             K(ret), KP(date_unit_type));
    } else {
      max_run_duration = 0;
      const char *date_unit_str = NULL;
      if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_YEAR)))) {
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_YEAR);
        max_run_duration = date_unit_interval * 365 * 24 * 60 * 60;
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_MONTH)))) {
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_MONTH);
        max_run_duration = date_unit_interval * 30 * 24 * 60 * 60;
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_DAY)))) {
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_DAY);
        max_run_duration = date_unit_interval * 24 * 60 * 60;
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_HOUR)))) {
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_HOUR);
        max_run_duration = date_unit_interval * 60 * 60;
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_MINUTE)))) {
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_MINUTE);
        max_run_duration = date_unit_interval * 60;
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_SECOND)))) {
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_SECOND);
        max_run_duration = date_unit_interval;
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(date_unit_str)) {
        snprintf(repeat_interval_str, OB_EVENT_REPEAT_MAX_LEN, "FREQ=%.*sLY; INTERVAL=%ld", (int)repeat_type_node->str_len_, date_unit_str, date_unit_interval);
      }
    }
  }
  return ret;
}

int ObEventResolver::get_event_exec_env(ObEventInfo &event_info)
{
  int ret = OB_SUCCESS;
  char *event_exec_buf = static_cast<char*>(allocator_->alloc(OB_MAX_PROC_ENV_LENGTH));
  if (OB_ISNULL(event_exec_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    memset(event_exec_buf, 0, OB_MAX_PROC_ENV_LENGTH);
    int64_t pos = 0;
    if (OB_FAIL(ObExecEnv::gen_exec_env(*session_info_, event_exec_buf, OB_MAX_PROC_ENV_LENGTH, pos))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate exec env failed", K(ret), K(session_info_));
    } else {
      ObString event_exec_env(pos, event_exec_buf);
      event_info.set_exec_env(event_exec_env); 
    }
  }
  return ret;
}

int ObEventResolver::get_event_time_node_value(const ParseNode *parse_node, int64_t &time_us)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_ISNULL(parse_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("time node is null", K(ret));
  } else if (T_VARCHAR == parse_node->type_) {
    if(OB_FAIL(sql.assign_fmt("select TIME_TO_USEC (\'%.*s\') as time",  /* 如果输入为纯字符串, SQL 解析会删去 ' 号 需补回去 */ 
                                                                        (int)parse_node->str_len_, 
                                                                        parse_node->str_value_))) {
     LOG_WARN("time node is not vaild", K(ret));
    }
  } else if (OB_FAIL(sql.assign_fmt("select TIME_TO_USEC (%.*s) as time", (int)parse_node->str_len_, parse_node->str_value_))) {
    LOG_WARN("time node is not vaild", K(ret));
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      time_us = OB_INVALID_TIMESTAMP;
      if (OB_FAIL(GCTX.sql_proxy_->read(result, session_info_->get_effective_tenant_id(), sql.ptr()))) {
        ret = OB_ERR_WRONG_VALUE;
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "AT", parse_node->str_value_);
        LOG_WARN("execute query failed", K(ret), K(sql), K(parse_node->type_));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_SUCC(ret) && OB_SUCC(res.next())) {
          EXTRACT_INT_FIELD_MYSQL(res, "time", time_us, int64_t);
          if (0 > time_us) { // 0 为 1970-01-01
            ret = OB_ERR_WRONG_VALUE;
            LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "AT", parse_node->str_value_);
          }
        }     
      }
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
