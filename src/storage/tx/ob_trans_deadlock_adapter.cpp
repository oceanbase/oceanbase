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
#include "lib/list/ob_dlist.h"
#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "storage/lock_wait_mgr/ob_lock_wait_mgr.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/memtable/ob_row_conflict_info.h"
#include "lib/profile/ob_trace_id.h"
#include "storage/tx/deadlock_adapter/ob_remote_execution_deadlock_callback.h"
#include "storage/tx/deadlock_adapter/ob_trans_detector_key.h"

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

int ObTransDeadlockDetectorAdapter::get_session_info(const SessionIDPair sess_id_pair, SessionGuard &guard) {
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session_info = nullptr;
  if (sess_id_pair.get_valid_sess_id() == 0) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "try to get invalid session", K(sess_id_pair), KR(ret), KP(GCTX.session_mgr_), K(lbt()));
  } else if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "GCTX.session_mgr is NULL", KR(ret), KP(GCTX.session_mgr_), K(lbt()));
  } else if (OB_FAIL(GCTX.session_mgr_->get_session(sess_id_pair.get_valid_sess_id(), session_info))) {
    DETECT_LOG(WARN, "get assoc_sess_id info failed", KR(ret), K(sess_id_pair));
  }
  if (OB_SUCC(ret)) {
    guard.set_session(session_info);
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::get_session_info(const ObTxDesc &desc, SessionGuard &guard) {
  return get_session_info(SessionIDPair(desc.get_session_id(), desc.get_assoc_session_id()), guard);
}

int ObTransDeadlockDetectorAdapter::kill_tx(const SessionIDPair sess_id_pair)
{
  int ret = OB_SUCCESS;
  lockwaitmgr::ObLockWaitMgr *mgr = nullptr;
  SessionGuard session_guard;
  sql::ObSQLSessionInfo *session_info = nullptr;
  if (OB_UNLIKELY(sess_id_pair.get_valid_sess_id() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument", K(ret), K(sess_id_pair));
  } else if (OB_FAIL(get_session_info(sess_id_pair, session_guard))) {
    DETECT_LOG(WARN, "get session guard failed", K(ret), K(sess_id_pair));
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "session guard invalid", K(ret), K(sess_id_pair));
  } else if (FALSE_IT(session_info = &session_guard.get_session())) {
  } else if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can't get session mgr", K(ret), K(sess_id_pair), K(*session_info));
  } else if (OB_ISNULL(mgr = MTL(lockwaitmgr::ObLockWaitMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can't get lock wait mgr", K(ret), K(sess_id_pair), K(*session_info));
  } else if (OB_FAIL(GCTX.session_mgr_->set_query_deadlocked(*session_info))) {
    DETECT_LOG(WARN, "set query dealocked failed", K(ret), K(sess_id_pair), K(*session_info));
  } else if (OB_FAIL(GCTX.session_mgr_->kill_deadlock_tx(session_info))) {
    DETECT_LOG(WARN, "fail to kill transaction", K(ret), K(sess_id_pair), K(*session_info));
  } else {
    session_info->reset_tx_variable();
    mgr->notify_killed_session(sess_id_pair.get_valid_sess_id());
    DETECT_LOG(INFO, "set query dealocked success in mysql mode", K(ret), K(sess_id_pair), K(*session_info));
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::kill_stmt(const SessionIDPair sess_id_pair)
{
  int ret = OB_SUCCESS;
  lockwaitmgr::ObLockWaitMgr *mgr = nullptr;
  SessionGuard session_guard;
  sql::ObSQLSessionInfo *session_info = nullptr;
  if (OB_UNLIKELY(sess_id_pair.get_valid_sess_id() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument", K(ret), K(sess_id_pair));
  } else if (OB_FAIL(get_session_info(sess_id_pair, session_guard))) {
    DETECT_LOG(WARN, "get session guard failed", K(ret), K(sess_id_pair));
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "session guard invalid", K(ret), K(sess_id_pair));
  } else if (FALSE_IT(session_info = &session_guard.get_session())) {
  } else if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can't get session mgr", K(ret), K(sess_id_pair), K(*session_info));
  } else if (OB_ISNULL(mgr = MTL(lockwaitmgr::ObLockWaitMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "can't get lock wait mgr", K(ret), K(sess_id_pair), K(*session_info));
  } else if (OB_FAIL(GCTX.session_mgr_->set_query_deadlocked(*session_info))) {
    TRANS_LOG(WARN, "set query dealocked failed", K(ret), K(sess_id_pair), K(*session_info));
  } else {
    mgr->notify_killed_session(sess_id_pair.get_valid_sess_id());
    TRANS_LOG(INFO, "set query dealocked success in oracle mode", K(ret), K(sess_id_pair), K(*session_info));
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::gen_dependency_resource_array_(const ObIArray<storage::ObRowConflictInfo> &block_conflict_info_array,
                                                                   ObIArray<ObDependencyHolder> &dependency_resources,
                                                                   const ObDeadlockKeyType &detector_key_type)
{
  int ret = OB_SUCCESS;
  UserBinaryKey binary_key;
  ObDependencyHolder resource;
  for (int64_t idx = 0; idx < block_conflict_info_array.count() && OB_SUCC(ret); idx++) {
    if (!block_conflict_info_array.at(idx).is_valid()) {
      DETECT_LOG(WARN, "invalid conflict info", K(block_conflict_info_array.at(idx)));
    } else if (!is_need_wait_remote_lock()) {
      if (OB_FAIL(binary_key.set_user_key(block_conflict_info_array.at(idx).conflict_tx_id_))) {
        DETECT_LOG(ERROR, "fail to create key");
      }
    } else {
      ObTransDeadlockDetectorKey detector_key(detector_key_type, block_conflict_info_array.at(idx).conflict_tx_id_);
      if (OB_FAIL(binary_key.set_user_key(detector_key))) {
        DETECT_LOG(ERROR, "fail to create key");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(resource.set_args(block_conflict_info_array.at(idx).conflict_tx_scheduler_, binary_key))) {
      DETECT_LOG(ERROR, "fail to create resource");
    } else if (OB_FAIL(dependency_resources.push_back(resource))) {
      DETECT_LOG(ERROR, "fail to push resource");
    }
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::register_to_deadlock_detector_(const SessionIDPair sess_id_pair,
                                                                   const ObTransID self_tx_id,
                                                                   const ObIArray<storage::ObRowConflictInfo> &conflict_array,
                                                                   SessionGuard &session_guard,
                                                                   const uint32_t count_down_allow_detect,
                                                                   const uint64_t start_delay,
                                                                   const ObDeadlockKeyType &detector_key_type)
{
  #define PRINT_WRAPPER KR(ret), K(self_tx_id), K(sess_id_pair), K(conflict_array), K(query_timeout), K(self_tx_scheduler)
  int ret = OB_SUCCESS;
  int64_t query_timeout = 0;
  int64_t tx_active_ts = 0;
  ObAddr self_tx_scheduler;
  RemoteDeadLockCollectCallBack on_collect_op(sess_id_pair);
  ObTransOnDetectOperation on_detect_op(sess_id_pair, self_tx_id);
  ObTransDeadLockRemoteExecutionFillVirtualInfoOperation fill_virtual_info_callback;
  ObSEArray<ObDependencyHolder, DEFAULT_BLOCKED_TRANS_ID_COUNT> blocked_resources;
  if (OB_FAIL(fill_virtual_info_callback.init(conflict_array))) {
    DETECT_LOG(WARN, "fail to init fill_virtual_info_callback", PRINT_WRAPPER);
  } else if (OB_FAIL(on_collect_op.row_conflict_info_array_.assign(conflict_array))) {
    DETECT_LOG(WARN, "fail to copy conflict info array", PRINT_WRAPPER);
  } else if (OB_UNLIKELY(conflict_array.empty())) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(INFO, "conflict tx idx is empty", PRINT_WRAPPER);
  } else if (OB_FAIL(session_guard->get_query_timeout(query_timeout))) {
    DETECT_LOG(ERROR, "get query timeout ts failed", PRINT_WRAPPER);
  } else if (OB_ISNULL(session_guard->get_tx_desc())) {
    bool autocommit = false;
    if (OB_FAIL(session_guard->get_autocommit(autocommit))) {
      DETECT_LOG(WARN, "get autocommit on session fail", PRINT_WRAPPER);
    } else if (autocommit) {
      self_tx_scheduler = GCTX.self_addr();
      tx_active_ts = common::ObTimeUtil::current_time();
    } else {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG(ERROR, "tx desc on session is NULL", PRINT_WRAPPER);
    }
  } else {
    self_tx_scheduler = session_guard->get_tx_desc()->get_addr();
    tx_active_ts = session_guard->get_tx_desc()->get_active_ts();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_dependency_resource_array_(conflict_array, blocked_resources, detector_key_type))) {
    DETECT_LOG(WARN, "fail to generate block resource", PRINT_WRAPPER);
  } else if (OB_ISNULL(MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "mtl deadlock detector mgr is null", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->register_key(self_tx_id,
                                                               on_detect_op,
                                                               on_collect_op,
                                                               fill_virtual_info_callback,
                                                               tx_active_ts,
                                                               ~tx_active_ts,
                                                               start_delay,
                                                               count_down_allow_detect))) {
    DETECT_LOG(WARN, "fail to register deadlock", PRINT_WRAPPER);
  } else {
    MTL(ObDeadLockDetectorMgr*)->set_timeout(self_tx_id, query_timeout);
    if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_tx_id, blocked_resources))) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_tx_id))) {
        DETECT_LOG(WARN, "fail to unregister key", PRINT_WRAPPER, K(tmp_ret));
      }
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

static bool check_at_least_one_holder_same(ObSEArray<ObDependencyHolder, DEFAULT_BLOCKED_TRANS_ID_COUNT> &l,
                                           ObSEArray<ObDependencyHolder, DEFAULT_BLOCKED_TRANS_ID_COUNT> &r) {
  bool has_same_holder = false;
  for (int64_t idx1 = 0; idx1 < l.count() && !has_same_holder; ++idx1) {
    for (int64_t idx2 = 0; idx2 < r.count() && !has_same_holder; ++idx2) {
      if (l[idx1] == r[idx2]) {
        has_same_holder = true;
      }
    }
  }
  return has_same_holder;
}
int ObTransDeadlockDetectorAdapter::replace_conflict_trans_ids_(const SessionIDPair sess_id_pair,
                                                                const ObTransID self_tx_id,
                                                                const ObIArray<storage::ObRowConflictInfo> &conflict_array,
                                                                SessionGuard &session_guard)
{
  #define PRINT_WRAPPER KR(ret), K(self_tx_id), K(conflict_array), K(current_blocked_resources)
  int ret = OB_SUCCESS;
  ObSEArray<ObDependencyHolder, DEFAULT_BLOCKED_TRANS_ID_COUNT> blocked_resources;
  ObSEArray<ObDependencyHolder, DEFAULT_BLOCKED_TRANS_ID_COUNT> current_blocked_resources;
  RemoteDeadLockCollectCallBack on_collect_op(sess_id_pair);
  if (OB_FAIL(on_collect_op.row_conflict_info_array_.assign(conflict_array))) {
    DETECT_LOG(WARN, "fail to copy array", PRINT_WRAPPER);
  } else if (OB_UNLIKELY(!conflict_array.empty())) {
    if (OB_ISNULL(MTL(ObDeadLockDetectorMgr*))) {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG(ERROR, "mtl deadlock detector mgr is null", PRINT_WRAPPER);
    } else if (OB_FAIL(gen_dependency_resource_array_(conflict_array, blocked_resources, ObDeadlockKeyType::DEFAULT))) {
      DETECT_LOG(ERROR, "generate dependency array failed", PRINT_WRAPPER);
    } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->get_block_list(self_tx_id, current_blocked_resources))) {
      DETECT_LOG(WARN, "generate dependency array failed", PRINT_WRAPPER);
    } else if (check_at_least_one_holder_same(current_blocked_resources, blocked_resources)) {
      if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->replace_block_list(self_tx_id, blocked_resources))) {
        DETECT_LOG(WARN, "replace block list failed", PRINT_WRAPPER);
      } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->replace_collect_callback(self_tx_id, on_collect_op))) {
        DETECT_LOG(WARN, "remote execution block on new resource success", PRINT_WRAPPER);
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

int ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_remote_self_(const rpc::ObLockWaitNode& remote_node)
{
  constexpr int64_t MAX_BUFFER_SIZE = 512;
  int ret = OB_SUCCESS;
  ObTransID self_tx_id(remote_node.tx_id_);
  ObRowConflictInfo cflict_info;
  char buffer[MAX_BUFFER_SIZE] = {0};// if str length less than ObStringHolder::TINY_STR_SIZE, no heap memory llocated
  int64_t pos = 0;
  common::databuff_printf(buffer, MAX_BUFFER_SIZE, pos, "%s", remote_node.key_);// it'ok if buffer not enough
  ObString temp_ob_string(pos, buffer);
  ObStringHolder temp_string_holder;
  temp_string_holder.assign(temp_ob_string);
  cflict_info.init(remote_node.exec_addr_,
                    share::ObLSID(remote_node.ls_id_),
                    common::ObTabletID(remote_node.tablet_id_),
                    meta::ObMover<ObStringHolder>(temp_string_holder),
                    SessionIDPair(remote_node.sessid_, remote_node.assoc_sess_id_),
                    remote_node.exec_addr_, // conflict tx scheduler addr
                    remote_node.tx_id_, // conflict tx id
                    ObTxSEQ::cast_from_int(remote_node.holder_tx_hold_seq_value_),
                    remote_node.hash_,
                    remote_node.lock_seq_,
                    remote_node.lock_wait_expire_ts_,
                    remote_node.tx_id_,
                    remote_node.lock_mode_,
                    remote_node.last_compact_cnt_,
                    remote_node.total_update_cnt_,
                    remote_node.assoc_sess_id_,
                    remote_node.holder_sessid_,
                    0 /*holder_tx_start_time_ is for ASH diagnose info*/,
                    remote_node.client_sid_);
  ObArray<ObRowConflictInfo> cflict_info_array;
  if (OB_FAIL(cflict_info_array.push_back(ObRowConflictInfo()))) {
    TRANS_LOG(WARN, "fail to push back", K(cflict_info), K(remote_node));
  } else if (OB_FAIL(cflict_info_array.at(0).assign(meta::ObMover<ObRowConflictInfo>(cflict_info)))) {
    TRANS_LOG(WARN, "move operation failed, should not happen", K(cflict_info), K(remote_node));
  } else if (OB_FAIL(ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_conflict_trans_ids(
                                          ObTransID(remote_node.tx_id_),
                                          SessionIDPair(remote_node.get_sess_id(), remote_node.get_assoc_session_id()),
                                          cflict_info_array,
                                          0 /*count_down_allow_detect*/))) {
    TRANS_LOG(WARN, "register remote node wait self to deadlock fail", K(cflict_info), K(remote_node));
  } else {
    TRANS_LOG(INFO, "register remote node wait self to deadlock", K(cflict_info), K(remote_node));
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::register_or_replace_conflict_trans_ids(const ObTransID self_tx_id,
                                                                           const SessionIDPair sess_id_pair,
                                                                           const ObArray<storage::ObRowConflictInfo> &conflict_array,
                                                                           const uint32_t count_down_allow_detect)
{
  #define PRINT_WRAPPER KR(ret), K(self_tx_id), K(sess_id_pair), K(conflict_array)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  SessionGuard session_guard;
  bool is_detector_exist = false;
  if (sess_id_pair.get_valid_sess_id() == 1) {
    DETECT_LOG(INFO, "inner session no need register to deadlock", PRINT_WRAPPER);
  } else if (sess_id_pair.get_valid_sess_id() == 0) {
    DETECT_LOG(ERROR, "invalid session id", PRINT_WRAPPER);
  } else if (conflict_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "empty conflict tx ids", PRINT_WRAPPER);
  } else if (OB_FAIL(get_session_info(sess_id_pair, session_guard))) {
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
    if (OB_FAIL(register_to_deadlock_detector_(sess_id_pair, self_tx_id, conflict_array, session_guard, count_down_allow_detect, 3_s, ObDeadlockKeyType::DEFAULT))) {
      DETECT_LOG(WARN, "register new detector in remote execution failed", PRINT_WRAPPER);
    } else {
      DETECT_LOG(INFO, "register new detector in remote execution", PRINT_WRAPPER);
    }
  } else {
    if (OB_FAIL(replace_conflict_trans_ids_(sess_id_pair, self_tx_id, conflict_array, session_guard))) {
      DETECT_LOG(INFO, "replace block list in remote execution", PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_conflict_trans_ids(const ObTransID self_tx_id,
                                                                                 const SessionIDPair sess_id_pair,
                                                                                 const ObArray<storage::ObRowConflictInfo> &conflict_array,
                                                                                 const uint32_t count_down_allow_detect)
{
  #define PRINT_WRAPPER KR(ret), K(self_tx_id), K(sess_id_pair), K(conflict_array)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  SessionGuard session_guard;
  bool is_detector_exist = false;
  ObTransDeadlockDetectorKey detector_key(ObDeadlockKeyType::REMOTE_EXEC_SIDE, self_tx_id);
  if (sess_id_pair.get_valid_sess_id() == 1) {
    DETECT_LOG(INFO, "inner session no need register to deadlock", PRINT_WRAPPER);
  } else if (sess_id_pair.get_valid_sess_id() == 0) {
    DETECT_LOG(ERROR, "invalid session id", PRINT_WRAPPER);
  } else if (conflict_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "empty conflict tx ids", PRINT_WRAPPER);
  } else if (OB_FAIL(get_session_info(sess_id_pair, session_guard))) {
    DETECT_LOG(ERROR, "fail to get session info", PRINT_WRAPPER);
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "fail to get session info", PRINT_WRAPPER);
  } else if (OB_ISNULL(MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "MTL ObDeadLockDetectorMgr is NULL", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(detector_key, is_detector_exist))) {
    DETECT_LOG(WARN, "fail to get detector exist status", PRINT_WRAPPER);
  } else if (is_detector_exist) {
    unregister_from_deadlock_detector(detector_key,
                                      UnregisterPath::REPLACE_MEET_TOTAL_DIFFERENT_LIST);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(register_to_deadlock_detector_(sess_id_pair, self_tx_id, conflict_array, session_guard, count_down_allow_detect, 0, ObDeadlockKeyType::REMOTE_EXEC_SIDE))) {
    DETECT_LOG(WARN, "register new detector in remote execution failed", PRINT_WRAPPER);
  } else {
    DETECT_LOG(INFO, "register new detector in remote execution", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}


int ObTransDeadlockDetectorAdapter::get_session_related_info_(const SessionIDPair sess_id_pair,
                                                              int64_t &query_timeout)
{
  int ret = OB_SUCCESS;
  SessionGuard session_guard;
  if (OB_FAIL(get_session_info(sess_id_pair, session_guard))) {
    DETECT_LOG(WARN, "get session failed", KR(ret), K(sess_id_pair));
  } else if (!session_guard.is_valid()) {
    DETECT_LOG(WARN, "get session failed", K(sess_id_pair), K(session_guard));
  } else if (OB_FAIL(session_guard.get_session().get_query_timeout(query_timeout))) {
    DETECT_LOG(WARN, "get query timeout failed", K(sess_id_pair));
  }
  return ret;
}

int ObTransDeadlockDetectorAdapter::get_trans_start_time_and_scheduler_from_session(const SessionIDPair sess_id_pair,
                                                                                    int64_t &trans_start_time,
                                                                                    ObAddr &scheduler_addr)
{
  #define PRINT_WRAPPER KR(ret), K(trans_start_time), K(sess_id_pair)
  int ret = OB_SUCCESS;
  SessionGuard guard;
  if (OB_FAIL(get_session_info(sess_id_pair, guard))) {
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

int ObTransDeadlockDetectorAdapter::get_trans_info_on_participant(const ObTransID trans_id,
                                                                  const share::ObLSID ls_id,
                                                                  ObAddr &scheduler_addr,
                                                                  SessionIDPair &sess_id_pair)
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
  } else if (OB_FAIL(ls_handle.get_ls()->get_tx_scheduler_and_sess_id(trans_id,
                                                                      scheduler_addr,
                                                                      sess_id_pair.sess_id_,
                                                                      sess_id_pair.assoc_sess_id_))) {
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
      uint32_t sess_id;
      uint32_t assoc_sess_id;
      if (OB_FAIL(iter->get_next(ls))) {
        DETECT_LOG(WARN, "get next iter failed", PRINT_WRAPPER);
      } else if (OB_FAIL(ls->get_tx_scheduler_and_sess_id(self_trans_id, scheduler_addr, sess_id, assoc_sess_id))) {
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
                                                                                   const FillVirtualInfoCallBack &on_fill_op,
                                                                                   const ObTransID &self_trans_id,
                                                                                   const SessionIDPair sess_id_pair)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_id), K(sess_id_pair), K(query_timeout), KTIME(trans_begin_ts), K(scheduler_addr)
  int ret = OB_SUCCESS;
  int64_t query_timeout = 0;
  int64_t trans_begin_ts = 0;
  ObTransOnDetectOperation on_detect_op(sess_id_pair, self_trans_id);
  ObAddr scheduler_addr;
  SessionGuard guard;
  if (OB_FAIL(get_session_related_info_(sess_id_pair, query_timeout))) {
    DETECT_LOG(WARN, "fail to get session related info", PRINT_WRAPPER);
  } else if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id_pair, guard))) {
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
                                                               on_fill_op,
                                                               trans_begin_ts,
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

void ObTransDeadlockDetectorAdapter::check_if_needed_register_detector_when_end_stmt_(const bool is_rollback,
                                                                                      const int ret_code,
                                                                                      ObSQLSessionInfo &session,
                                                                                      ObTxDesc &desc,
                                                                                      ObArray<storage::ObRowConflictInfo> &conflict_info_array)
{
  #define PRINT_WRAPPER KR(ret), KR(ret_code), K(session), K(desc), K(is_rollback), K(conflict_info_array), \
          KTIME(query_timeout_ts), KTIME(current_ts), K(continuous_lock_conflict_cnt)
  int ret = OB_SUCCESS;
  int64_t query_timeout_ts = session.get_query_timeout_ts();
  int64_t current_ts = ObClockGenerator::getClock();
  int64_t continuous_lock_conflict_cnt = desc.inc_and_get_continuous_lock_conflict_cnt();
  if (ret_code == OB_TRY_LOCK_ROW_CONFLICT && // 1. must be 6005
      is_rollback == true && // 2. must rollback statment
      !conflict_info_array.empty() && // 3. must has conflict objects
      current_ts < query_timeout_ts && // 4. must not timeout
      continuous_lock_conflict_cnt > 10) {
      // 5. remote execution or local exection but retry more than 10 times for some unknown reasons
    if (OB_FAIL(register_or_replace_conflict_trans_ids(desc.tid(),
                                                       SessionIDPair(desc.get_session_id(), desc.get_assoc_session_id()),
                                                       conflict_info_array,
                                                       10 /*count_down_allow_detect*/))) {// after 3 seconds and 10 times replace with same conflict arrays(at least one item same), detection will REALLY BEGIN THEN
      DETECT_LOG(WARN, "register or replace list failed", PRINT_WRAPPER);
    } else {
      DETECT_LOG(INFO, "register deadlock detector waiting for trans", PRINT_WRAPPER);
    }
  } else {// conditions not all matchs
    if (!is_rollback) {// statment is done
      desc.reset_continuous_lock_conflict_cnt();
    }
    unregister_from_deadlock_detector(desc.tid(), UnregisterPath::END_STMT);
  }
  if (OB_UNLIKELY(continuous_lock_conflict_cnt % 100 == 0)) {
    DETECT_LOG(INFO, "this stmt retryed too much times already", PRINT_WRAPPER);
  }
  #undef PRINT_WRAPPER
}

int ObTransDeadlockDetectorAdapter::fetch_conflict_info_array(transaction::ObTxDesc &desc,
                                                              ObArray<storage::ObRowConflictInfo> &array)
{
  int ret = OB_SUCCESS;
  ObArray<ObTransIDAndAddr> temp_conflict_txs_array;// for compat reason
  ObArray<storage::ObRowConflictInfo> temp_conflict_info_array;
  (void) desc.fetch_conflict_info_array(temp_conflict_info_array);
  (void) desc.fetch_conflict_txs_array(temp_conflict_txs_array);
  if (detector::ObDeadLockDetectorMgr::is_new_deadlock_logic()) {
    for (int64_t idx = 0; idx < temp_conflict_info_array.count() && OB_SUCC(ret); ++idx) {
      if (OB_FAIL(array.push_back(ObRowConflictInfo()))) {
        DETECT_LOG(WARN, "fail to push back", KR(ret), K(idx));
      } else if (OB_FAIL(array.at(array.count() - 1).assign(meta::ObMover<ObRowConflictInfo>(temp_conflict_info_array[idx])))) {
        DETECT_LOG(WARN, "move operation failed, should not happen", KR(ret), K(idx));
      }
    }
  } else {
    // if deadlock module run in upgrade scenario, some process use old logic, just has conflict_txs_ structure
    ObStringHolder row_key_str;
    if (OB_FAIL(row_key_str.assign("fake row"))) {
      DETECT_LOG(WARN, "failed to generate fake row", KR(ret));
    } else {
      for (int64_t idx = 0; idx < temp_conflict_txs_array.count() && OB_SUCC(ret); ++idx) {
        if (OB_FAIL(array.push_back(ObRowConflictInfo()))) {
          DETECT_LOG(WARN, "fail to push back", KR(ret), K(idx));
        } else if (OB_FAIL(array.at(array.count() - 1).init(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888),
                                                            ObLSID(8888),
                                                            ObTabletID(8888),
                                                            meta::ObMover<ObStringHolder>(row_key_str),
                                                            transaction::SessionIDPair(888, 888),
                                                            temp_conflict_txs_array[idx].scheduler_addr_,
                                                            temp_conflict_txs_array[idx].tx_id_,
                                                            transaction::ObTxSEQ::mk_v0(8888888888),
                                                            array.at(array.count() - 1).conflict_hash_,
                                                            array.at(array.count() - 1).lock_seq_,
                                                            array.at(array.count() - 1).abs_timeout_,
                                                            array.at(array.count() - 1).self_tx_id_,
                                                            array.at(array.count() - 1).lock_mode_,
                                                            array.at(array.count() - 1).last_compact_cnt_,
                                                            array.at(array.count() - 1).total_update_cnt_,
                                                            array.at(array.count() - 1).assoc_sess_id_,
                                                            array.at(array.count() - 1).holder_sess_id_,
                                                            array.at(array.count() - 1).holder_tx_start_time_,
                                                            array.at(array.count() - 1).client_session_id_))) {
          DETECT_LOG(WARN, "move operation failed, should not happen", KR(ret), K(idx));
        }
      }
    }
  }
  return ret;
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
                        KPC(desc), K(is_rollback), K(conflict_info_array)
  int ret = OB_SUCCESS;
  int step = 0;
  CHECK_DEADLOCK_ENABLED();
  ObSQLSessionInfo *session = nullptr;
  ObTxDesc *desc = nullptr;
  int exec_ctx_err_code = exec_ctx.get_errcode();
  // 1. prepare session and desc
  ObArray<storage::ObRowConflictInfo> conflict_info_array;
  if (++step && OB_ISNULL(session = GET_MY_SESSION(exec_ctx))) {
    ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(ERROR, "session is NULL", PRINT_WRAPPER);
  } else if (++step && OB_ISNULL(desc = session->get_tx_desc())) {
    ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(ERROR, "desc in session is NULL", PRINT_WRAPPER);
  } else if (++step && !desc->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "desc in session is invalid", PRINT_WRAPPER);
  // 2. prepare conflict info array(fetch means reset data after this action done)
  } else if (++step && OB_FAIL(fetch_conflict_info_array(*desc, conflict_info_array))) {
    DETECT_LOG(WARN, "fail to get conflict txs from desc", PRINT_WRAPPER);
  // 3. see if needed register to deadlock detector here
  } else {
    check_if_needed_register_detector_when_end_stmt_(is_rollback,
                                                      exec_ctx.get_errcode(),
                                                      *session,
                                                      *desc,
                                                      conflict_info_array);
  }
  // if (session->get_retry_info().get_retry_cnt() <= 1 ||// first time lock conflict or other error
  //     session->get_retry_info().get_retry_cnt() % 30 == 0) {// other wise, control log print frequency
  //   DETECT_LOG(INFO, "maintain deadlock info", PRINT_WRAPPER);
  // }
  // if (OB_SUCC(ret)) {
  //   if (OB_SUCCESS != exec_ctx_err_code) {
  //     if ((OB_ITER_END != exec_ctx_err_code)) {
  //       if (session->get_retry_info().get_retry_cnt() <= 1 ||// first time lock conflict or other error
  //           session->get_retry_info().get_retry_cnt() % 10 == 0) {// other wise, control log print frequency
  //         DETECT_LOG(INFO, "maintain deadlock info", PRINT_WRAPPER);
  //       }
  //     }
  //   }
  // }
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
                                                                                       const FillVirtualInfoCallBack &on_fill_op,
                                                                                       const ObTransID &self_trans_id,
                                                                                       const SessionIDPair sess_id_pair)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_id), K(sess_id_pair), K(exist)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  bool exist = false;
  if (sess_id_pair.get_valid_sess_id() == 0) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "invalid session id", PRINT_WRAPPER);
  } else if (nullptr == (MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "fail to get ObDeadLockDetectorMgr", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(self_trans_id, exist))) {
    DETECT_LOG(WARN, "fail to check detector exist", PRINT_WRAPPER);
  } else if (exist) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_id))) {
      DETECT_LOG(WARN, "fail to unregister key", PRINT_WRAPPER, K(tmp_ret));
    }
  }
  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "local execution register to deadlock detector waiting for row failed", PRINT_WRAPPER);
  } else if (OB_FAIL(create_detector_node_and_set_parent_if_needed_(on_collect_op, on_fill_op, self_trans_id, sess_id_pair))) {
    DETECT_LOG(WARN, "fail to create detector node", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_trans_id, func))) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_id))) {
      DETECT_LOG(WARN, "fail to unregister key", PRINT_WRAPPER, K(tmp_ret));
    }
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
                                                                                         const FillVirtualInfoCallBack &on_fill_op,
                                                                                         const ObTransID &conflict_trans_id,
                                                                                         const ObTransID &self_trans_id,
                                                                                         const SessionIDPair sess_id_pair)
{
  #define PRINT_WRAPPER KR(ret), K(scheduler_addr), K(self_trans_id), K(sess_id_pair), K(exist)
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
      DETECT_LOG(WARN, "fail to unregister key", PRINT_WRAPPER, K(tmp_ret));
    }
  }
  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "local execution register to deadlock detector waiting for row failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_conflict_trans_scheduler(conflict_trans_id, scheduler_addr))) {
    DETECT_LOG(WARN, "fail to get conflict trans scheduler addr", PRINT_WRAPPER);
  } else if (OB_FAIL(create_detector_node_and_set_parent_if_needed_(on_collect_op, on_fill_op, self_trans_id, sess_id_pair))) {
    DETECT_LOG(WARN, "fail to create detector node", PRINT_WRAPPER);
  } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(self_trans_id, scheduler_addr, conflict_trans_id))) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_id))) {
      DETECT_LOG(WARN, "fail to unregister key", PRINT_WRAPPER, K(tmp_ret));
    }
    DETECT_LOG(WARN, "fail to block on conflict trans", PRINT_WRAPPER);
  } else {
    DETECT_LOG(TRACE, "local execution register to deadlock detector waiting for trans success", PRINT_WRAPPER);
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
int ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_row_without_session(CollectCallBack &on_collect_op,
                                                                                                       const FillVirtualInfoCallBack &on_fill_op,
                                                                                                       const BlockCallBack &func,
                                                                                                       const ObTransID &self_trans_id,
                                                                                                       const uint32_t sess_id,
                                                                                                       const int64_t trans_begin_ts,
                                                                                                       const int64_t query_timeout_us)
{
  #define PRINT_WRAPPER KR(ret), K(self_trans_id), K(exist)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  bool exist = false;
  if (nullptr == (MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "fail to get ObDeadLockDetectorMgr", PRINT_WRAPPER);
  } else if (!is_need_wait_remote_lock()) {
    if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(self_trans_id, exist))) {
      DETECT_LOG(WARN, "fail to check detector exist", PRINT_WRAPPER);
    } else if (exist) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_id))) {
        DETECT_LOG(WARN, "fail to unregister key", K(tmp_ret), PRINT_WRAPPER);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_detector_node_without_session_and_block_row_(on_collect_op,
                                                                on_fill_op,
                                                                self_trans_id,
                                                                sess_id,
                                                                trans_begin_ts,
                                                                query_timeout_us,
                                                                func))) {
      DETECT_LOG(WARN, "fail to create detector node and block row", PRINT_WRAPPER);
    }
  } else {
    ObTransDeadlockDetectorKey detector_key(ObDeadlockKeyType::REMOTE_EXEC_SIDE, self_trans_id);
    if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(detector_key, exist))) {
      DETECT_LOG(WARN, "fail to check detector exist", PRINT_WRAPPER);
    } else if (exist) {
      unregister_from_deadlock_detector(detector_key, UnregisterPath::LOCK_WAIT_MGR_REPOST);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_detector_node_without_session_and_block_row_(on_collect_op,
                                                                on_fill_op,
                                                                detector_key,
                                                                sess_id,
                                                                trans_begin_ts,
                                                                query_timeout_us,
                                                                func))) {
      DETECT_LOG(WARN, "fail to create detector node and block row", PRINT_WRAPPER);
    }
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
int ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_trans_without_session(CollectCallBack &on_collect_op,
                                                                                                         const FillVirtualInfoCallBack &on_fill_op,
                                                                                                         const ObTransID &conflict_trans_id,
                                                                                                         const ObTransID &self_trans_id,
                                                                                                         const uint32_t sess_id,
                                                                                                         const int64_t trans_begin_ts,
                                                                                                         const int64_t query_timeout_us)
{
  #define PRINT_WRAPPER KR(ret), K(scheduler_addr), K(self_trans_id), K(exist)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  ObAddr scheduler_addr;
  bool exist = false;
  if (nullptr == (MTL(ObDeadLockDetectorMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "fail to get ObDeadLockDetectorMgr", PRINT_WRAPPER);
  } else if (OB_FAIL(get_conflict_trans_scheduler(conflict_trans_id, scheduler_addr))) {
    DETECT_LOG(WARN, "fail to get conflict trans scheduler addr", PRINT_WRAPPER);
  } else if (!is_need_wait_remote_lock()) {
    if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(self_trans_id, exist))) {
      DETECT_LOG(WARN, "fail to check detector exist", PRINT_WRAPPER);
    } else if (exist) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_trans_id))) {
        DETECT_LOG(WARN, "fail to unregister key", K(tmp_ret), PRINT_WRAPPER);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_detector_node_without_session_and_block_trans_(on_collect_op,
                                                                      on_fill_op,
                                                                      self_trans_id,
                                                                      conflict_trans_id,
                                                                      sess_id,
                                                                      trans_begin_ts,
                                                                      query_timeout_us,
                                                                      scheduler_addr))) {
      DETECT_LOG(WARN, "fail to create detector node and block trans", PRINT_WRAPPER);
    }
  } else {
    ObTransDeadlockDetectorKey self_detector_key(ObDeadlockKeyType::REMOTE_EXEC_SIDE, self_trans_id);
    ObTransDeadlockDetectorKey conflict_detector_key(ObDeadlockKeyType::DEFAULT, conflict_trans_id);
    if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->check_detector_exist(self_detector_key, exist))) {
      DETECT_LOG(WARN, "fail to check detector exist", PRINT_WRAPPER);
    } else if (exist) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(self_detector_key))) {
        DETECT_LOG(WARN, "fail to unregister key", PRINT_WRAPPER, K(tmp_ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_detector_node_without_session_and_block_trans_(on_collect_op,
                                                                      on_fill_op,
                                                                      self_detector_key,
                                                                      conflict_detector_key,
                                                                      sess_id,
                                                                      trans_begin_ts,
                                                                      query_timeout_us,
                                                                      scheduler_addr))) {
      DETECT_LOG(WARN, "fail to create detector node and block trans", PRINT_WRAPPER);
    }
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
static int autonomous_detect_callback(const common::ObIArray<ObDetectorInnerReportInfo> &, const int64_t) {
  DETECT_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "should not kill inner node");
  return common::OB_ERR_UNEXPECTED;
}
struct AutoNomousCollectCallback {
  AutoNomousCollectCallback(const ObTransID last_trans_id)
  : last_trans_id_(last_trans_id) {}
  int operator()(const ObDependencyHolder &, ObDetectorUserReportInfo& report_info) {
    ObSharedGuard<char> ptr;
    ptr.assign((char*)"detector", DoNothingDeleter());
    report_info.set_module_name(ptr);
    char *buffer = (char *)share::mtl_malloc(sizeof(char) * 64, "DeadLockDA");
    if (OB_NOT_NULL(buffer)) {
      last_trans_id_.to_string(buffer, 64);
      buffer[63] = '\0';
      ptr.assign((char*)buffer, MtlDeleter());
    } else {
      DETECT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "alloc memory failed");
      ptr.assign((char*)"inner visitor", DoNothingDeleter());
    }
    report_info.set_visitor(ptr);
    ptr.assign((char*)"waiting for autonomous trans", DoNothingDeleter());
    report_info.set_resource(ptr);
    return common::OB_SUCCESS;
  }
private:
  ObTransID last_trans_id_;
};
int ObTransDeadlockDetectorAdapter::autonomous_register_to_deadlock(const ObTransID last_trans_id,
                                                                    const ObTransID now_trans_id,
                                                                    const int64_t last_trans_active_ts,
                                                                    const int64_t query_timeout)
{
  #define PRINT_WRAPPER KR(ret), K(last_trans_id), K(now_trans_id), K(query_timeout)
  CHECK_DEADLOCK_ENABLED();
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->register_key(last_trans_id,
                                                        autonomous_detect_callback,
                                                        AutoNomousCollectCallback(last_trans_id),
                                                        DummyFillCallBack(),
                                                        last_trans_active_ts,
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


} // namespace transaction
} // namespace oceanbase