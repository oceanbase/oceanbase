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

#ifndef OCEANBASE_LOGSERVICE_OB_ROLE_CHANGE_HANDLER_
#define OCEANBASE_LOGSERVICE_OB_ROLE_CHANGE_HANDLER_
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "share/ob_errno.h"
namespace oceanbase
{
namespace logservice
{
enum TakeOverState {
  INVALID_TAKE_OVER_STATE = 0,
  WAIT_REPLAY_DONE = 1,
  WAIT_RC_HANDLER_DONE = 2,
  TAKE_OVER_FINISH = 3,
  UNKNOWN_TAKE_OVER_STATE = 4,
  MAX_TAKE_OVER_STATE = 5
};

static inline
int takeover_state_to_string(const TakeOverState log_type,
                            char *str,
                            const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (log_type == INVALID_TAKE_OVER_STATE) {
    strncpy(str ,"INVALID_STATE", str_len);
  } else if (log_type == WAIT_REPLAY_DONE) {
    strncpy(str ,"WAIT_REPLAY_DONE", str_len);
  } else if (log_type == WAIT_RC_HANDLER_DONE) {
    strncpy(str ,"WAIT_RC_HANDLER_DONE", str_len);
  } else if (log_type == TAKE_OVER_FINISH) {
    strncpy(str ,"FINISH", str_len);
  } else if (log_type == UNKNOWN_TAKE_OVER_STATE) {
    strncpy(str ,"UNKNOWN", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

struct RCDiagnoseInfo {
  RCDiagnoseInfo() { reset(); }
  ~RCDiagnoseInfo() { reset(); }
  void reset();
  int64_t id_;
  TakeOverState state_;
  ObLogBaseType log_type_; //仅当处于WAIT_RC_HANDLER_DONE时才有意义
  TO_STRING_KV(K(state_),
               K(log_type_));
};

class ObRoleChangeHandler {
public:
  ObRoleChangeHandler();
  ~ObRoleChangeHandler();
  void reset();
  int register_handler(const ObLogBaseType &type, ObIRoleChangeSubHandler *role_change_hander);
  void unregister_handler(const ObLogBaseType &type);

  void switch_to_follower_forcedly();
  int switch_to_leader(RCDiagnoseInfo &diagnose_info);
  // @retval:
  // 1. OB_SUCCESS
  // 2. OB_LS_NEED_REVOKE, ObRoleChangeService need revoke this LS.
  // 3. OTHERS, switch_to_follower_gracefully failed, but no need to revoke leader.
  int switch_to_follower_gracefully();
  int resume_to_leader();

private:
  int resume_leader_when_switch_failure_(int64_t cursor);
private:
  ObSpinLock lock_;
  ObIRoleChangeSubHandler* sub_role_change_handler_arr_[ObLogBaseType::MAX_LOG_BASE_TYPE];
};
}
}
#endif
