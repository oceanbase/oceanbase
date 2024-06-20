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

#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/ob_sql_define.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/ls/ob_ls_get_mod.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
using namespace share::detector;

namespace transaction
{

SessionGuard::SessionGuard() : sess_ptr_(nullptr) {}

SessionGuard::~SessionGuard()
{
  revert_session_();
}

void SessionGuard::set_session(sql::ObSQLSessionInfo *session_ptr)
{
  revert_session_();
  sess_ptr_ = session_ptr;
}

bool SessionGuard::is_valid() const { return nullptr != sess_ptr_; }

sql::ObSQLSessionInfo *SessionGuard::operator->() const { return sess_ptr_; }

sql::ObSQLSessionInfo &SessionGuard::get_session() const { return *sess_ptr_; };

void SessionGuard::revert_session_()
{
  if (nullptr != sess_ptr_) {
    if (OB_ISNULL(GCTX.session_mgr_)) {
      DETECT_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "GCTX.session_mgr is NULL, session will leak", K_(sess_ptr));
    } else {
      GCTX.session_mgr_->revert_session(sess_ptr_);
    }
  }
  sess_ptr_ = nullptr;
}

#define CHECK_DEADLOCK_ENABLED() \
do {\
  if (OB_UNLIKELY(!ObDeadLockDetectorMgr::is_deadlock_enabled())) {\
    if (REACH_TIME_INTERVAL(1_s)) {\
      DETECT_LOG(INFO, "deadlock not enabled");\
    }\
    return common::OB_NOT_RUNNING;\
  }\
} while(0)

void ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(const char *src_ptr,
                                                                       const int64_t src_len,
                                                                       char *dest_ptr,// C-style str, contain '\0'
                                                                       const int64_t dest_len) {
  int64_t src_idx = 0;
  int64_t dest_idx = 0;
  if (dest_len > 0 && src_ptr && dest_ptr) {
    while (src_idx < src_len && dest_idx < dest_len - 3) {// remain 1 byte for '\0', reserve 2 bytes to translate '\''
      if (src_ptr[src_idx] == '\0') {
        break;
      } else if (src_ptr[src_idx] == '\'') {
        dest_ptr[dest_idx++] = '\\';
      }
      dest_ptr[dest_idx++] = src_ptr[src_idx++];
    }
    dest_ptr[dest_idx] = '\0';
  }
};

int ObTransDeadlockDetectorAdapter::get_session_info(const uint32_t session_id, SessionGuard &guard) {
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session_info = nullptr;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "GCTX.session_mgr is NULL",
                     KR(ret), K(session_id), KP(GCTX.session_mgr_), K(lbt()));
  } else if (OB_FAIL(GCTX.session_mgr_->get_session(session_id, session_info))) {
    DETECT_LOG(WARN, "get session info failed", KR(ret), K(session_id), K(lbt()));
  } else {
    guard.set_session(session_info);
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::get_session_info(const ObTxDesc &desc, SessionGuard &guard) {
  return get_session_info(desc.get_session_id(), guard);
}

int ObTransDeadlockDetectorAdapter::kill_tx(const uint32_t sess_id)
{
  int ret = OB_SUCCESS;
  memtable::ObLockWaitMgr *mgr = nullptr;
  SessionGuard session_guard;
  sql::ObSQLSessionInfo *session_info = nullptr;
  if (OB_UNLIKELY(sess_id == 0)) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument", K(ret), K(sess_id));
  } else if (OB_FAIL(get_session_info(sess_id, session_guard))) {
    DETECT_LOG(WARN, "get session guard failed", K(ret), K(sess_id));
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "session guard invalid", K(ret), K(sess_id));
  } else if (FALSE_IT(session_info = &session_guard.get_session())) {
  } else if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can't get session mgr", K(ret), K(sess_id), K(*session_info));
  } else if (OB_ISNULL(mgr = MTL(memtable::ObLockWaitMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can't get lock wait mgr", K(ret), K(sess_id), K(*session_info));
  } else if (OB_FAIL(GCTX.session_mgr_->kill_deadlock_tx(session_info))) {
    DETECT_LOG(WARN, "fail to kill transaction", K(ret), K(sess_id), K(*session_info));
  } else if (OB_FAIL(GCTX.session_mgr_->set_query_deadlocked(*session_info))) {
    DETECT_LOG(WARN, "set query dealocked failed", K(ret), K(sess_id), K(*session_info));
  } else {
    session_info->reset_tx_variable();
    mgr->notify_deadlocked_session(sess_id);
    DETECT_LOG(INFO, "set query dealocked success in mysql mode", K(ret), K(sess_id), K(*session_info));
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::kill_stmt(const uint32_t sess_id)
{
  int ret = OB_SUCCESS;
  memtable::ObLockWaitMgr *mgr = nullptr;
  SessionGuard session_guard;
  sql::ObSQLSessionInfo *session_info = nullptr;
  if (OB_UNLIKELY(sess_id == 0)) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument", K(ret), K(sess_id));
  } else if (OB_FAIL(get_session_info(sess_id, session_guard))) {
    DETECT_LOG(WARN, "get session guard failed", K(ret), K(sess_id));
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "session guard invalid", K(ret), K(sess_id));
  } else if (FALSE_IT(session_info = &session_guard.get_session())) {
  } else if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can't get session mgr", K(ret), K(sess_id), K(*session_info));
  } else if (OB_ISNULL(mgr = MTL(memtable::ObLockWaitMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can't get lock wait mgr", K(ret), K(sess_id), K(*session_info));
  } else if (OB_FAIL(GCTX.session_mgr_->set_query_deadlocked(*session_info))) {
    TRANS_LOG(WARN, "set query dealocked failed", K(ret), K(sess_id), K(*session_info));
  } else {
    mgr->notify_deadlocked_session(sess_id);
    TRANS_LOG(INFO, "set query dealocked success in oracle mode", K(ret), K(sess_id), K(*session_info));
  }
  return ret;
}

// Remote execution retries each time when the lock needed is held by others.
// Before each retry, the remote execution will rollback previous operations
// through end_stmt.
//
// So we register the lock dependency when the retry is needed. The conflicts
// are detected and passed through trans_result during remote execution. NB:
// what we need pay special attention to is how to handle different conflicts in
// different reties. If the deadlock detector just activate all previous
// conflicts and block on all new conflicts, the liveness may be broken because
// each retry will push the private label and cause the cycle to be unstable.
// Current solution is to use the primitive interface, replace_block_list,
// which keeps the unchanged conflicts, activates disappearred conflicts and
// blocks on new conflicts.
//
// The dependency is unregisterred when no conflicts appear in trans_result.

int ObTransOnDetectOperation::operator()(
    const common::ObIArray<share::detector::ObDetectorInnerReportInfo> &info,
    const int64_t self_idx) {
  CHECK_DEADLOCK_ENABLED();
  UNUSED(info);
  UNUSED(self_idx);
  SessionGuard session_guard;
  int ret = OB_SUCCESS;
  int step = 0;

  if (++step && OB_UNLIKELY(sess_id_ == 0 || !trans_id_.is_valid())) {
    ret = OB_NOT_INIT;
  } else if (++step && OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id_, session_guard))) {
  } else if (++step && !session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (++step && ObCompatibilityMode::MYSQL_MODE == session_guard->get_compatibility_mode()) {
    ret = ObTransDeadlockDetectorAdapter::kill_tx(sess_id_);
  } else if (++step && ObCompatibilityMode::ORACLE_MODE == session_guard->get_compatibility_mode()) {
    ret = ObTransDeadlockDetectorAdapter::kill_stmt(sess_id_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "unknown mode", KR(ret), K(step), K(session_guard.get_session()), K(*this));
  }

  if (!OB_SUCC(ret)) {
    DETECT_LOG(WARN, "execute on detect op failed", KR(ret), K(step), K(*this));
  }

  return ret;
}

/******************************[FOR REMOTE EXECUTION]******************************/

class RemoteDeadLockCollectCallBack {
public:
  RemoteDeadLockCollectCallBack(const uint32_t &sess_id) : sess_id_(sess_id) {}
  int operator()(ObDetectorUserReportInfo &info) {
    CHECK_DEADLOCK_ENABLED();
    int ret = OB_SUCCESS;
    constexpr int64_t trans_id_str_len = 128;
    constexpr int64_t current_sql_str_len = 256;
    char * buffer_trans_id = nullptr;
    char * buffer_current_sql = nullptr;
    int step = 0;
    if (OB_UNLIKELY(nullptr == (buffer_trans_id = (char*)ob_malloc(trans_id_str_len, "deadlockCB")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc memory failed", KR(ret));
    } else if (OB_UNLIKELY(nullptr == (buffer_current_sql = (char*)ob_malloc(current_sql_str_len, "deadlockCB")))) {
      ob_free(buffer_trans_id);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc memory failed", KR(ret));
    } else {
      SessionGuard session_guard;
      ObTransID self_trans_id;
      if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id_, session_guard))) {
        DETECT_LOG(WARN, "got session info is NULL", KR(ret), K(session_guard), K(sess_id_));
      } else if (!session_guard.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(session_guard->get_tx_desc())) {
        ret = OB_ERR_UNEXPECTED;
        DETECT_LOG(WARN, "desc on session is not valid", KR(ret));
      } else if (!(session_guard->get_tx_desc()->get_tx_id().is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        DETECT_LOG(WARN, "trans id on desc on session is not valid", KR(ret));
      } else {
        ObSharedGuard<char> temp_guard;
        const ObString &cur_query_str = session_guard->get_current_query_string();
        if (cur_query_str.empty()) {
          DETECT_LOG(WARN, "cur_query_str on session is empty", K(cur_query_str), K(session_guard.get_session()));
        } else {
          DETECT_LOG(WARN, "cur_query_str on session is not empty", K(cur_query_str), K(session_guard.get_session()));
        }
        ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(cur_query_str.ptr(),
                                                                          cur_query_str.length(),
                                                                          buffer_current_sql,
                                                                          current_sql_str_len);
        (void) session_guard->get_tx_desc()->get_tx_id().to_string(buffer_trans_id, trans_id_str_len);
        if (++step && OB_FAIL(temp_guard.assign((char*)"transaction", [](char*){}))) {
        } else if (++step && OB_FAIL(info.set_module_name(temp_guard))) {
        } else if (++step && OB_FAIL(temp_guard.assign((char*)"remote row", [](char*){}))) {
        } else if (++step && OB_FAIL(info.set_resource(temp_guard))) {
        } else if (++step && OB_FAIL(temp_guard.assign(buffer_trans_id, [](char *ptr){ ob_free(ptr); }))) {
        } else if (FALSE_IT(buffer_trans_id = nullptr)) {
        } else if (++step && OB_FAIL(info.set_visitor(temp_guard))) {
        } else if (++step && OB_FAIL(temp_guard.assign(buffer_current_sql, [](char *ptr){ ob_free(ptr); }))) {
        } else if (FALSE_IT(buffer_current_sql = nullptr)) {
        } else if (++step && OB_FAIL(info.set_extra_info("current sql", temp_guard))) {
        }
      }
      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(buffer_trans_id)) {
          ob_free(buffer_trans_id);
        }
        if (OB_NOT_NULL(buffer_current_sql)) {
          ob_free(buffer_current_sql);
        }
        DETECT_LOG(WARN, "get string failed in deadlock", KR(ret), K(step));
      }
    }
    return ret;
  }
private:
  const uint32_t sess_id_;
};

int ObTransDeadlockDetectorAdapter::gen_dependency_resource_array_(const ObIArray<ObTransIDAndAddr> &blocked_trans_ids_and_addrs,
                                                                  ObIArray<ObDependencyResource> &dependency_resources)
{
  int ret = OB_SUCCESS;
  UserBinaryKey binary_key;
  ObDependencyResource resource;
  for (int64_t idx = 0; idx < blocked_trans_ids_and_addrs.count() && OB_SUCC(ret); idx++) {
    if (!blocked_trans_ids_and_addrs.at(idx).is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG(ERROR, "invalid trans id and addr");
    } else if (OB_FAIL(binary_key.set_user_key(blocked_trans_ids_and_addrs.at(idx).tx_id_))) {
      DETECT_LOG(ERROR, "fail to create key");
    } else if (OB_FAIL(resource.set_args(blocked_trans_ids_and_addrs.at(idx).scheduler_addr_, binary_key))) {
      DETECT_LOG(ERROR, "fail to create resource");
    } else if (OB_FAIL(dependency_resources.push_back(resource))) {
      DETECT_LOG(ERROR, "fail to push resource");
    }
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::register_to_deadlock_detector_(const ObTransID self_tx_id,
                                                                   const uint32_t self_session_id,
                                                                   const ObIArray<ObTransIDAndAddr> &conflict_tx_ids,
                                                                   SessionGuard &session_guard)
{
  #define PRINT_WRAPPER KR(ret), K(self_tx_id), K(self_session_id), K(conflict_tx_ids), K(query_timeout), K(self_tx_scheduler)
  int ret = OB_SUCCESS;
  int64_t query_timeout = 0;
  ObAddr self_tx_scheduler;
  RemoteDeadLockCollectCallBack on_collect_op(self_session_id);
  ObTransOnDetectOperation on_detect_op(self_session_id, self_tx_id);
  ObSEArray<ObDependencyResource, DEFAULT_BLOCKED_TRANS_ID_COUNT> blocked_resources;
  if (OB_UNLIKELY(conflict_tx_ids.empty())) {
    DETECT_LOG(INFO, "conflict tx idx is empty", PRINT_WRAPPER);
  } else if (OB_FAIL(session_guard->get_query_timeout(query_timeout))) {
    DETECT_LOG(ERROR, "get query timeout ts failed", PRINT_WRAPPER);
  } else if (OB_ISNULL(session_guard->get_tx_desc())) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "tx desc on session is NULL", PRINT_WRAPPER);
  } else if (FALSE_IT(self_tx_scheduler = session_guard->get_tx_desc()->get_addr())) {
  } else if (OB_FAIL(gen_dependency_resource_array_(conflict_tx_ids, blocked_resources))) {
    DETECT_LOG(WARN, "fail to generate block resource", PRINT_WRAPPER);
  } else if (OB_ISNULL(MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "mtl deadlock detector mgr is null", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->register_key(self_tx_id,
                                                               on_detect_op,
                                                               on_collect_op,
                                                               ~session_guard->get_tx_desc()->get_active_ts(),
                                                               3_s,
                                                               10))) {
    DETECT_LOG(WARN, "fail to register deadlock", PRINT_WRAPPER);
  } else {
    MTL(ObDeadLockDetectorMgr*)->set_timeout(self_tx_id, query_timeout);
    if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_tx_id, blocked_resources))) {
      DETECT_LOG(WARN, "block on resource failed", PRINT_WRAPPER);
    } else if (self_tx_scheduler != GCTX.self_addr()) {
      if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->add_parent(self_tx_id, self_tx_scheduler, self_tx_id))) {
        DETECT_LOG(WARN, "scheduler is not self, set parent failed", PRINT_WRAPPER);
      } else {
        DETECT_LOG(INFO, "remote execution register to deadlock detector success, scheduler is not self", PRINT_WRAPPER);
      }
    } else {
      DETECT_LOG(INFO, "remote execution register to deadlock detector success, scheduler is self", PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::replace_conflict_trans_ids_(const ObTransID self_tx_id,
                                                                const ObIArray<ObTransIDAndAddr> &conflict_tx_ids,
                                                                SessionGuard &session_guard)
{
  #define PRINT_WRAPPER KR(ret), K(self_tx_id), K(conflict_tx_ids), K(current_blocked_resources)
  int ret = OB_SUCCESS;
  ObSEArray<ObDependencyResource, DEFAULT_BLOCKED_TRANS_ID_COUNT> blocked_resources;
  ObSEArray<ObDependencyResource, DEFAULT_BLOCKED_TRANS_ID_COUNT> current_blocked_resources;
  auto check_at_least_one_holder_same = [](ObSEArray<ObDependencyResource, DEFAULT_BLOCKED_TRANS_ID_COUNT> &l,
                                           ObSEArray<ObDependencyResource, DEFAULT_BLOCKED_TRANS_ID_COUNT> &r) -> bool {
    bool has_same_holder = false;
    for (int64_t idx1 = 0; idx1 < l.count() && !has_same_holder; ++idx1) {
      for (int64_t idx2 = 0; idx2 < r.count() && !has_same_holder; ++idx2) {
        if (l[idx1] == r[idx2]) {
          has_same_holder = true;
        }
      }
    }
    return has_same_holder;
  };
  if (OB_UNLIKELY(!conflict_tx_ids.empty())) {
    if (OB_ISNULL(MTL(ObDeadLockDetectorMgr*))) {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG(ERROR, "mtl deadlock detector mgr is null", PRINT_WRAPPER);
    } else if (OB_FAIL(gen_dependency_resource_array_(conflict_tx_ids, blocked_resources))) {
      DETECT_LOG(ERROR, "generate dependency array failed", PRINT_WRAPPER);
    } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->get_block_list(self_tx_id, current_blocked_resources))) {
      DETECT_LOG(WARN, "generate dependency array failed", PRINT_WRAPPER);
    } else if (check_at_least_one_holder_same(current_blocked_resources, blocked_resources)) {
      if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->replace_block_list(self_tx_id, blocked_resources))) {
        DETECT_LOG(WARN, "replace block list failed", PRINT_WRAPPER);
      }
      (void) MTL(ObDeadLockDetectorMgr*)->dec_count_down_allow_detect(self_tx_id);
    } else {
      unregister_from_deadlock_detector(self_tx_id,
                                        UnregisterPath::REPLACE_MEET_TOTAL_DIFFERENT_LIST);
      DETECT_LOG(WARN, "unregister detector cause meet total different block list", PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::register_or_replace_conflict_trans_ids(const ObTransID self_tx_id,
                                                                                            const uint32_t self_session_id,
                                                                                            const ObArray<ObTransIDAndAddr> &conflict_tx_ids)
{
  #define PRINT_WRAPPER KR(ret), K(self_tx_id), K(self_session_id), K(conflict_tx_ids)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  SessionGuard session_guard;
  bool is_detector_exist = false;
  if (self_session_id == 1) {
    DETECT_LOG(INFO, "inner session no need register to deadlock", PRINT_WRAPPER);
  } else if (self_session_id == 0) {
    DETECT_LOG(ERROR, "invalid session id", PRINT_WRAPPER);
  } else if (conflict_tx_ids.empty()) {
    DETECT_LOG(WARN, "empty conflict tx ids", PRINT_WRAPPER);
  } else if (OB_FAIL(get_session_info(self_session_id, session_guard))) {
    DETECT_LOG(ERROR, "fail to get session info", PRINT_WRAPPER);
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "fail to get session info", PRINT_WRAPPER);
  } else if (OB_ISNULL(MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "MTL ObDeadLockDetectorMgr is NULL", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(self_tx_id, is_detector_exist))) {
    DETECT_LOG(WARN, "fail to get detector exist status", PRINT_WRAPPER);
  } else if (!is_detector_exist) {
    if (OB_FAIL(register_to_deadlock_detector_(self_tx_id, self_session_id, conflict_tx_ids, session_guard))) {
      DETECT_LOG(WARN, "register new detector in remote execution failed", PRINT_WRAPPER);
    } else {
      DETECT_LOG(INFO, "register new detector in remote execution", PRINT_WRAPPER);
    }
  } else {
    if (OB_FAIL(replace_conflict_trans_ids_(self_tx_id, conflict_tx_ids, session_guard))) {
      DETECT_LOG(INFO, "replace block list in remote execution", PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::get_session_related_info_(const uint32_t sess_id,
                                                             int64_t &query_timeout)
{
  int ret = OB_SUCCESS;
  SessionGuard session_guard;
  if (OB_FAIL(get_session_info(sess_id, session_guard))) {
    DETECT_LOG(WARN, "get session failed", KR(ret), K(sess_id));
  } else if (!session_guard.is_valid()) {
    DETECT_LOG(WARN, "get session failed", K(sess_id), K(session_guard));
  } else if (OB_FAIL(session_guard.get_session().get_query_timeout(query_timeout))) {
    DETECT_LOG(WARN, "get query timeout failed", K(sess_id));
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::get_trans_start_time_and_scheduler_from_session(const uint32_t session_id,
                                                                                   int64_t &trans_start_time,
                                                                                   ObAddr &scheduler_addr)
{
  #define PRINT_WRAPPER KR(ret), K(trans_start_time)
  int ret = OB_SUCCESS;
  SessionGuard guard;
  if (OB_FAIL(get_session_info(session_id, guard))) {
    DETECT_LOG(ERROR, "get session failed", PRINT_WRAPPER);
  } else if (!guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "get session failed", PRINT_WRAPPER);
  } else if (OB_ISNULL(guard->get_tx_desc())) {
    ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(WARN, "desc on session is NULL", PRINT_WRAPPER);
  } else if (guard->get_tx_desc()->get_addr() != GCTX.self_addr()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "this not happened on scheduler", PRINT_WRAPPER, K(guard->get_tx_desc()->get_addr()));
  } else {
    trans_start_time = guard->get_curr_trans_start_time();
    scheduler_addr = guard->get_tx_desc()->get_addr();
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::get_trans_scheduler_info_on_participant(const ObTransID trans_id,
                                                                           const share::ObLSID ls_id, 
                                                                           ObAddr &scheduler_addr)
{
  #define PRINT_WRAPPER KR(ret), K(trans_id), K(ls_id), K(scheduler_addr)
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  if (OB_ISNULL(ls_service)) {
    ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(ERROR, "ls_service is NULL", PRINT_WRAPPER);
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DEADLOCK_MOD))) {
    DETECT_LOG(WARN, "fail to get ls", PRINT_WRAPPER);
  } else if (OB_FAIL(ls_handle.get_ls()->get_tx_scheduler(trans_id, scheduler_addr))) {
    DETECT_LOG(WARN, "fail to get tx scheduler", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::get_conflict_trans_scheduler(const ObTransID &self_trans_id,
                                                                 ObAddr &scheduler_addr)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_id), K(scheduler_addr)
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService*);
  ObLS *ls = nullptr;
  ObSharedGuard<ObLSIterator> iter;
  scheduler_addr.reset();
  if (OB_UNLIKELY(ls_service == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can not get ls service", PRINT_WRAPPER);
  } else if (OB_FAIL(ls_service->get_ls_iter(iter, storage::ObLSGetMod::DEADLOCK_MOD))) {
    DETECT_LOG(WARN, "fail to get ls iter", PRINT_WRAPPER);
  } else {
    do {
      if (OB_FAIL(iter->get_next(ls))) {
        DETECT_LOG(WARN, "get next iter failed", PRINT_WRAPPER);
      } else if (OB_FAIL(ls->get_tx_scheduler(self_trans_id, scheduler_addr))) {
        if (ret == OB_TRANS_CTX_NOT_EXIST) {
          ret = OB_SUCCESS;
          DETECT_LOG(TRACE, "ctx not exist on this logstream", K(ls->get_ls_id()), PRINT_WRAPPER);
        } else {
          DETECT_LOG(WARN, "get tx ctx failed", PRINT_WRAPPER);
        }
      } else {
        DETECT_LOG(TRACE, "ctx on this logstream", K(ls->get_ls_id()), PRINT_WRAPPER);
        break;// find ctx
      }
    } while (OB_SUCC(ret));
    if (OB_FAIL(ret) || !scheduler_addr.is_valid()) {
      if (OB_SUCC(ret)) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      DETECT_LOG(WARN, "can't find trans ctx from all ls", PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::create_detector_node_and_set_parent_if_needed_(CollectCallBack &on_collect_op,
                                                                                  const ObTransID &self_trans_id,
                                                                                  const uint32_t sess_id)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_id), K(sess_id), K(query_timeout), K(trans_begin_ts), K(scheduler_addr)
  int ret = OB_SUCCESS;
  int64_t query_timeout = 0;
  int64_t trans_begin_ts = 0;
  ObTransOnDetectOperation on_detect_op(sess_id, self_trans_id);
  ObAddr scheduler_addr;
  SessionGuard guard;
  if (OB_FAIL(get_session_related_info_(sess_id, query_timeout))) {
    DETECT_LOG(WARN, "fail to get session related info", PRINT_WRAPPER);
  } else if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id, guard))) {
    DETECT_LOG(WARN, "fail to get session related info", PRINT_WRAPPER);
  } else if (!guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "get session failed", PRINT_WRAPPER);
  } else if (OB_ISNULL(guard->get_tx_desc())) {
    ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(WARN, "tx desc is NULL", PRINT_WRAPPER);
  } else if (FALSE_IT(scheduler_addr = guard->get_tx_desc()->get_addr())) {
  } else if (FALSE_IT(trans_begin_ts = guard->get_tx_desc()->get_active_ts())) {
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->register_key(self_trans_id,
                                                               on_detect_op,
                                                               on_collect_op,
                                                               ~trans_begin_ts))) {
    DETECT_LOG(WARN, "fail to register key", PRINT_WRAPPER);
  } else {
    MTL(ObDeadLockDetectorMgr*)->set_timeout(self_trans_id, query_timeout);
    if (scheduler_addr != GCTX.self_addr()) {
      if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->add_parent(self_trans_id, scheduler_addr, self_trans_id))) {
        DETECT_LOG(WARN, "fail to get add parent", PRINT_WRAPPER);
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

/******************************BELOW INTERFACE CALL FROM OTHER FILES******************************/

// Call from SQL trans control, check if need register to deadlock or replace block list
// (depends on session status, is registered to deadlock or not)
// 
// @param [in] on_collect_op collect deadlock related info when deadlock detected.
// @param [in] func the block function to tell detector waiting for who.
// @param [in] self_trans_id who am i.
// @param [in] sess_id which session to kill if this node is killed.
// @return the error code.
int ObTransDeadlockDetectorAdapter::maintain_deadlock_info_when_end_stmt(sql::ObExecContext &exec_ctx,
                                                                         const bool is_rollback)
{
  #define PRINT_WRAPPER K(step), KR(ret), KR(exec_ctx.get_errcode()), KPC(session),\
                        KPC(desc), K(is_rollback), K(conflict_txs)
  int ret = OB_SUCCESS;
  int step = 0;
  CHECK_DEADLOCK_ENABLED();
  memtable::ObLockWaitMgr::Node *node = nullptr;
  ObSQLSessionInfo *session = nullptr;
  ObTxDesc *desc = nullptr;
  ObArray<ObTransIDAndAddr> conflict_txs;
  if (++step && OB_ISNULL(session = GET_MY_SESSION(exec_ctx))) {
    ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(ERROR, "session is NULL", PRINT_WRAPPER);
  } else if (++step && session->is_inner()) {
    DETECT_LOG(TRACE, "inner session no need register to deadlock", PRINT_WRAPPER);
  } else if (++step && OB_ISNULL(desc = session->get_tx_desc())) {
    ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(ERROR, "desc in session is NULL", PRINT_WRAPPER);
  } else if (++step && OB_FAIL(desc->fetch_conflict_txs(conflict_txs))) {
    DETECT_LOG(WARN, "fail to get conflict txs from desc", PRINT_WRAPPER);
  } else if (++step && !desc->is_valid()) {
    DETECT_LOG(INFO, "invalid tx desc no need register to deadlock", PRINT_WRAPPER);
  } else if (++step && is_rollback) {// statment is failed, maybe will try again, check if need register to deadlock detector
    if (++step && session->get_query_timeout_ts() < ObClockGenerator::getClock()) {
      unregister_from_deadlock_detector(desc->tid(), UnregisterPath::END_STMT_TIMEOUT);
      DETECT_LOG(INFO, "query timeout, no need register to deadlock", PRINT_WRAPPER);
    } else if (++step && conflict_txs.empty()) {
      unregister_from_deadlock_detector(desc->tid(), UnregisterPath::END_STMT_NO_CONFLICT);
      DETECT_LOG(INFO, "try unregister deadlock detecotr cause conflict array is empty", PRINT_WRAPPER);
    } else if (++step && exec_ctx.get_errcode() != OB_TRY_LOCK_ROW_CONFLICT) {
      unregister_from_deadlock_detector(desc->tid(), UnregisterPath::END_STMT_OTHER_ERR);
      DETECT_LOG(INFO, "try unregister deadlock detecotr cause meet non-lock error", PRINT_WRAPPER);
    } else if (++step && OB_FAIL(register_or_replace_conflict_trans_ids(desc->tid(), session->get_sessid(), conflict_txs))) {
      DETECT_LOG(WARN, "register or replace list failed", PRINT_WRAPPER);
    } else {
      // do nothing, register success or keep retrying
    }
  } else {// statment is done, will not try again, all related deadlock info should be resetted
    unregister_from_deadlock_detector(desc->tid(), UnregisterPath::END_STMT_DONE);
    DETECT_LOG(TRACE, "try unregister from deadlock detector", KR(ret), K(desc->tid()));
  }
  if (OB_NOT_NULL(desc)) {// whether registered or not, clean conflict info anyway
    desc->reset_conflict_txs();
  }
  int exec_ctx_err_code = exec_ctx.get_errcode();
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != exec_ctx_err_code) {
      if ((OB_ITER_END != exec_ctx_err_code)) {
        if (session->get_retry_info().get_retry_cnt() <= 1 ||// first time lock conflict or other error
            session->get_retry_info().get_retry_cnt() % 10 == 0) {// other wise, control log print frequency
          DETECT_LOG(INFO, "maintain deadlock info", PRINT_WRAPPER);
        }
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

// Call from LockWaitMgr, register local excution waiting for row
// 
// @param [in] on_collect_op collect deadlock related info when deadlock detected.
// @param [in] func the block function to tell detector waiting for who.
// @param [in] self_trans_id who am i.
// @param [in] sess_id which session to kill if this node is killed.
// @return the error code.
int ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_row(CollectCallBack &on_collect_op,
                                                                                       const BlockCallBack &func,
                                                                                       const ObTransID &self_trans_id,
                                                                                       const uint32_t sess_id)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_id), K(sess_id), K(exist)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  bool exist = false;
  if (sess_id == 0) {
    DETECT_LOG(ERROR, "invalid session id", PRINT_WRAPPER);
  } else if (nullptr == (MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "fail to get ObDeadLockDetectorMgr", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(self_trans_id, exist))) {
    DETECT_LOG(WARN, "fail to check detector exist", PRINT_WRAPPER);
  } else if (exist) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_id))) {
      DETECT_LOG(WARN, "fail to unregister key", K(tmp_ret), PRINT_WRAPPER);
    }
  }
  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "local execution register to deadlock detector waiting for row failed", PRINT_WRAPPER);
  } else if (OB_FAIL(create_detector_node_and_set_parent_if_needed_(on_collect_op, self_trans_id, sess_id))) {
    DETECT_LOG(WARN, "fail to create detector node", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_trans_id, func))) {
    DETECT_LOG(WARN, "fail to block on call back function", PRINT_WRAPPER);
  } else {
    DETECT_LOG(TRACE, "local execution register to deadlock detector waiting for row success", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

// Call from LockWaitMgr, register local excution waiting for trans
// 
// @param [in] on_collect_op collect deadlock related info when deadlock detected.
// @param [in] conflict_trans_id tell detector waiting for who.
// @param [in] self_trans_id who am i.
// @param [in] sess_id which session to kill if this node is killed.
// @return the error code.
int ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_trans(CollectCallBack &on_collect_op,
                                                                                         const ObTransID &conflict_trans_id,
                                                                                         const ObTransID &self_trans_id,
                                                                                         const uint32_t sess_id)
{
  #define PRINT_WRAPPER KR(ret), K(scheduler_addr), K(self_trans_id), K(sess_id), K(exist)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  ObAddr scheduler_addr;
  bool exist = false;
  if (nullptr == (MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "fail to get ObDeadLockDetectorMgr", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(self_trans_id, exist))) {
    DETECT_LOG(WARN, "fail to check detector exist", PRINT_WRAPPER);
  } else if (exist) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_id))) {
      DETECT_LOG(WARN, "fail to unregister key", K(tmp_ret), PRINT_WRAPPER);
    }
  }
  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "local execution register to deadlock detector waiting for row failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_conflict_trans_scheduler(conflict_trans_id, scheduler_addr))) {
    DETECT_LOG(WARN, "fail to get conflict trans scheduler addr", PRINT_WRAPPER);
  } else if (OB_FAIL(create_detector_node_and_set_parent_if_needed_(on_collect_op, self_trans_id, sess_id))) {
    DETECT_LOG(WARN, "fail to create detector node", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_trans_id, scheduler_addr, conflict_trans_id))) {
    DETECT_LOG(WARN, "fail to block on conflict trans", PRINT_WRAPPER);
  } else {
    DETECT_LOG(TRACE, "local execution register to deadlock detector waiting for trans success", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::change_detector_waiting_obj_from_row_to_trans(const ObTransID &self_trans_id,
                                                                                  const ObTransID &conflict_trans_id,
                                                                                  const ObAddr &scheduler_addr)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_id), K(scheduler_addr), K(conflict_trans_id)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  if (nullptr == (MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "fail to get ObDeadLockDetectorMgr", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->activate_all(self_trans_id))) {
    DETECT_LOG(WARN, "fail to activate all", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_trans_id, scheduler_addr, conflict_trans_id))) {
    DETECT_LOG(WARN, "fail to block on conflict trans", PRINT_WRAPPER);
  } else {
    DETECT_LOG(INFO, "change denpendency relationship from row to trnas", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

// Register autonomous trans dependency relationship, no need session id here, cause this trans should not be killed
// 
// @param [in] last_trans_id who is the trans before start autonomous trans.
// @param [in] now_trans_id who is the trans after start autonomous trans.
// @param [in] query_timeout from session, to tell detector how long it will live(avoid leak).
// @return void.
int ObTransDeadlockDetectorAdapter::autonomous_register_to_deadlock(const ObTransID last_trans_id,
                                                                   const ObTransID now_trans_id,
                                                                   const int64_t query_timeout)
{
  #define PRINT_WRAPPER KR(ret), K(last_trans_id), K(now_trans_id), K(query_timeout)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  if (OB_ISNULL(MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "tenant deadlock detector mgr is null", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->register_key(last_trans_id,
                                                  [](const common::ObIArray<ObDetectorInnerReportInfo> &,
                                                    const int64_t) { DETECT_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "should not kill inner node");
                                                                      return common::OB_ERR_UNEXPECTED; },
                                                  [last_trans_id](ObDetectorUserReportInfo& report_info) {
                                                    ObSharedGuard<char> ptr;
                                                    ptr.assign((char*)"detector", [](char*){});
                                                    report_info.set_module_name(ptr);
                                                    char *buffer = (char *)ob_malloc(sizeof(char) * 64, "DeadLockDA");
                                                    if (OB_NOT_NULL(buffer)) {
                                                      last_trans_id.to_string(buffer, 64);
                                                      buffer[63] = '\0';
                                                      ptr.assign((char*)buffer, [](char* p){ ob_free(p); });
                                                    } else {
                                                      DETECT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "alloc memory failed");
                                                      ptr.assign((char*)"inner visitor", [](char*){});
                                                    }
                                                    report_info.set_visitor(ptr);
                                                    ptr.assign((char*)"waiting for autonomous trans", [](char*){});
                                                    report_info.set_resource(ptr);
                                                    return common::OB_SUCCESS;
                                                  },
                                                  ObDetectorPriority(PRIORITY_RANGE::EXTREMELY_HIGH, 0)))) {
    DETECT_LOG(WARN, "register key failed", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(last_trans_id, now_trans_id))) {
    DETECT_LOG(WARN, "block resource failed", PRINT_WRAPPER);
  } else {
    MTL(ObDeadLockDetectorMgr*)->set_timeout(last_trans_id, query_timeout);
    DETECT_LOG(INFO, "register autonomous deadlock dependency success", PRINT_WRAPPER);
  }
  return ret;
}

// Call from ALL PATH, unregister detector, and mark the reason
// 
// @param [in] self_trans_id who am i.
// @param [in] path call from which code path.
// @return void.
void ObTransDeadlockDetectorAdapter::unregister_from_deadlock_detector(const ObTransID &self_trans_id,
                                                                       const UnregisterPath path)
{
  int ret = common::OB_SUCCESS;
  ObDeadLockDetectorMgr *mgr = nullptr;
  if (nullptr == (mgr = MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "fail to get ObDeadLockDetectorMgr", K(self_trans_id), K(to_string(path)));
  } else if (OB_FAIL(mgr->unregister_key(self_trans_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      DETECT_LOG(WARN, "unregister from deadlock detector failed", K(self_trans_id), K(to_string(path)));
    } else {
      ret = OB_SUCCESS;// it's ok if detector not exist
    }
  } else {
    DETECT_LOG(TRACE, "unregister from deadlock detector success", K(self_trans_id), K(to_string(path)));
  }
}

} // namespace transaction
} // namespace oceanbase
