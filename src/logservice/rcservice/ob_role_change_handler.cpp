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

#include "ob_role_change_handler.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "share/ob_errno.h"
namespace oceanbase
{
using namespace common;
namespace logservice
{
void RCDiagnoseInfo::reset()
{
  id_ = -1;
  state_ = TakeOverState::INVALID_TAKE_OVER_STATE;
  log_type_ = ObLogBaseType::INVALID_LOG_BASE_TYPE;
}

ObRoleChangeHandler::ObRoleChangeHandler(): lock_(common::ObLatchIds::RCS_LOCK),
                                            sub_role_change_handler_arr_()
{
  reset();
}

ObRoleChangeHandler::~ObRoleChangeHandler()
{
  reset();
}

void ObRoleChangeHandler::reset()
{
  for (int i = 0; i < ObLogBaseType::MAX_LOG_BASE_TYPE; i++) {
    sub_role_change_handler_arr_[i] = NULL;
  }
}

int ObRoleChangeHandler::register_handler(const ObLogBaseType &type,
                                          ObIRoleChangeSubHandler *role_change_handler)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (false == is_valid_log_base_type(type)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    sub_role_change_handler_arr_[type] = role_change_handler;
    CLOG_LOG(INFO, "register_handler success", K(ret), K(type), KP(role_change_handler));
  }
  return ret;
}

void ObRoleChangeHandler::unregister_handler(const ObLogBaseType &type)
{
  ObSpinLockGuard guard(lock_);
  if (true == is_valid_log_base_type(type)) {
    sub_role_change_handler_arr_[type] = NULL;
    CLOG_LOG(INFO, "unregister_handler success", K(type));
  }
}

void ObRoleChangeHandler::switch_to_follower_forcedly()
{
  ObSpinLockGuard guard(lock_);
  for (int i = 0; i < ObLogBaseType::MAX_LOG_BASE_TYPE; i++) {
    ObIRoleChangeSubHandler *handler = sub_role_change_handler_arr_[i];
    char sub_role_change_handler_str[OB_LOG_BASE_TYPE_STR_MAX_LEN] = {'\0'};
    ObLogBaseType base_type = static_cast<ObLogBaseType>(i);
    bool has_defined_to_string = false;
    if (OB_SUCCESS == log_base_type_to_string(base_type, sub_role_change_handler_str,
          OB_LOG_BASE_TYPE_STR_MAX_LEN)) {
      has_defined_to_string = true;
    }
    if (NULL != handler) {
      handler->switch_to_follower_forcedly();
      CLOG_LOG(INFO, "leader to follower forcedly, current role change handler is",
          "cursor", i, "name", has_defined_to_string ? sub_role_change_handler_str : "hasn't define to string");
    }
  }
}

int ObRoleChangeHandler::switch_to_leader(RCDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  for (int i = 0; i < ObLogBaseType::MAX_LOG_BASE_TYPE && OB_SUCC(ret); i++) {
    ObIRoleChangeSubHandler *handler = sub_role_change_handler_arr_[i];
    char sub_role_change_handler_str[OB_LOG_BASE_TYPE_STR_MAX_LEN] = {'\0'};
    ObLogBaseType base_type = static_cast<ObLogBaseType>(i);
    diagnose_info.log_type_ = base_type;
    bool has_defined_to_string = false;
    if (OB_SUCCESS == log_base_type_to_string(base_type, sub_role_change_handler_str,
          OB_LOG_BASE_TYPE_STR_MAX_LEN)) {
      has_defined_to_string = true;
    }
    if (NULL == handler) {
    } else if (OB_FAIL(handler->switch_to_leader())) {
      CLOG_LOG(WARN, "switch_to_leader failed", K(ret), KP(handler), K(i),
          "cursor", i, "name", has_defined_to_string ? sub_role_change_handler_str : "hasn't define to string");
    } else {
      CLOG_LOG(INFO, "follower to leader, current role change handler is",
          "cursor", i, "name", has_defined_to_string ? sub_role_change_handler_str : "hasn't define to string");
    }
  }
  return ret;
}

int ObRoleChangeHandler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  int64_t cursor = 0;
  const int64_t count = ObLogBaseType::MAX_LOG_BASE_TYPE;
  while (cursor < count && OB_SUCC(ret)) {
    char sub_role_change_handler_str[OB_LOG_BASE_TYPE_STR_MAX_LEN] = {'\0'};
    ObLogBaseType base_type = static_cast<ObLogBaseType>(cursor);
    bool has_defined_to_string = false;
    if (OB_SUCCESS == log_base_type_to_string(base_type, sub_role_change_handler_str,
          OB_LOG_BASE_TYPE_STR_MAX_LEN)) {
      has_defined_to_string = true;
    }
    ObIRoleChangeSubHandler *handler = sub_role_change_handler_arr_[cursor];
    if (NULL == handler) {
      cursor++;
    } else if (OB_FAIL(handler->switch_to_follower_gracefully()) && OB_LS_NEED_REVOKE != ret) {
      CLOG_LOG(WARN, "switch_to_follower_gracefully failed, need resume other sub modules", K(ret),
          KP(handler), K(cursor),
          "cursor", cursor, "name", has_defined_to_string ? sub_role_change_handler_str : "hasn't define to string");
      // NB: resume_leader failed, need revoke leader.
    } else if (OB_LS_NEED_REVOKE == ret) {
      CLOG_LOG(WARN, "ObIRoleChangeSubHandler resume leader failed", K(ret), K(cursor));
    } else {
      CLOG_LOG(INFO, "leader to follower gracefully, current role change handler is",
          "cursor", cursor, "name", has_defined_to_string ? sub_role_change_handler_str : "hasn't define to string");
      cursor++;
    }
  }
  // if any sub role handler switch_to_follower_gracefully failed, and no need to revoke leader,
  // we should resume other sub role handler, meanwhile, we should overrite 'ret' only if
  // resume_leader_when_switch_failure_ failed.
  if (OB_FAIL(ret) && OB_LS_NEED_REVOKE != ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = resume_leader_when_switch_failure_(cursor))) {
      CLOG_LOG(WARN, "resume_leader_when_switch_failure_ failed", K(tmp_ret), K(cursor));
      ret = tmp_ret;
    } else {
      CLOG_LOG(WARN, "resume_leader_when_switch_failure_ success, no need to excut follower to leader gracefully",
          K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObRoleChangeHandler::resume_to_leader()
{
  int ret = OB_SUCCESS;
  int cursor = ObLogBaseType::MAX_LOG_BASE_TYPE;
  if (OB_FAIL(resume_leader_when_switch_failure_(cursor))) {
    CLOG_LOG(WARN, "resume_leader_when_switch_failure_ failed");
  } else {
    CLOG_LOG(INFO, "resume_to_leader success");
  }
  return ret;
}

int ObRoleChangeHandler::resume_leader_when_switch_failure_(const int64_t cursor)
{
  int ret = OB_SUCCESS;
  for (int64_t i = cursor - 1; i >= 0 && OB_SUCC(ret); i--) {
    ObIRoleChangeSubHandler *handler = sub_role_change_handler_arr_[i];
    ObLogBaseType base_type = static_cast<ObLogBaseType>(i);
    char sub_role_change_handler_str[OB_LOG_BASE_TYPE_STR_MAX_LEN] = {'\0'};
    bool has_defined_to_string = false;
    if (OB_SUCCESS == log_base_type_to_string(base_type, sub_role_change_handler_str,
          OB_LOG_BASE_TYPE_STR_MAX_LEN)) {
      has_defined_to_string = true;
    }
    if (NULL == handler){
      CLOG_LOG(INFO, "not register into role change service", K(ret), K(i));
    } else if (OB_FAIL(handler->resume_leader())) {
      CLOG_LOG(WARN, "resume_leader failed", K(ret), K(i), KP(handler),
          "cursor", i, "name", has_defined_to_string ? sub_role_change_handler_str : "hasn't define to string");
    } else {
      CLOG_LOG(INFO, "resume_leader success", K(ret), K(i), KP(handler),
          "cursor", i, "name", has_defined_to_string ? sub_role_change_handler_str : "hasn't define to string");
    }
  }
  if (OB_FAIL(ret)) {
    ret = OB_LS_NEED_REVOKE;
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
