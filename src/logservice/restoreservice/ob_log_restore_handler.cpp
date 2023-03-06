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

#include "ob_log_restore_handler.h"
#include "common/ob_member_list.h"
#include "common/ob_role.h"                  // ObRole
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/net/ob_addr.h"                 // ObAddr
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"         // K*
#include "lib/string/ob_string.h"            // ObString
#include "lib/time/ob_time_utility.h"        // ObTimeUtility
#include "lib/utility/ob_macro_utils.h"
#include "logservice/ob_log_service.h"       // ObLogService
#include "logservice/palf/palf_env.h"        // PalfEnv
#include "logservice/replayservice/ob_log_replay_service.h"
#include "ob_log_restore_rpc_define.h"       // RPC Request
#include "ob_remote_log_source.h"            // ObRemoteLogParent
#include "ob_remote_log_source_allocator.h"  // ObResSrcAlloctor
#include "ob_log_restore_define.h"           // ObRemoteFetchContext
#include "share/ob_define.h"
#include "share/restore/ob_log_archive_source.h"

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::share;
ObLogRestoreHandler::ObLogRestoreHandler() :
  parent_(NULL),
  context_(),
  is_in_stop_state_(true),
  is_inited_(false)
{}

ObLogRestoreHandler::~ObLogRestoreHandler()
{
  destroy();
}

int ObLogRestoreHandler::init(const int64_t id, PalfEnv *palf_env)
{
  int ret = OB_SUCCESS;
  palf::PalfHandle palf_handle;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (NULL == palf_env) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(palf_env));
  } else if (OB_FAIL(palf_env->open(id, palf_handle))) {
    CLOG_LOG(WARN, "get palf_handle failed", K(ret), K(id));
  } else {
    id_ = id;
    palf_handle_ = palf_handle;
    palf_env_ = palf_env;
    role_ = FOLLOWER;
    is_in_stop_state_ = false;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogRestoreHandler init success", K(id), K(role_), K(palf_handle));
  }

  if (OB_FAIL(ret)) {
    if (true == palf_handle.is_valid() && false == is_valid()) {
      palf_env_->close(palf_handle);
    }
  }
  return ret;
}

int ObLogRestoreHandler::stop()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_INIT) {
    is_in_stop_state_ = true;
    if (palf_handle_.is_valid()) {
      palf_env_->close(palf_handle_);
    }
  }
  CLOG_LOG(INFO, "ObLogRestoreHandler stop", K(ret), KPC(this));
  return ret;
}

void ObLogRestoreHandler::destroy()
{
  WLockGuard guard(lock_);
  if (IS_INIT) {
    is_in_stop_state_ = true;
    is_inited_ = false;
    if (palf_handle_.is_valid()) {
      palf_env_->close(palf_handle_);
    }
    palf_env_ = NULL;
    id_ = -1;
    proposal_id_ = 0;
    role_ = ObRole::INVALID_ROLE;
    context_.reset();
  }
  if (NULL != parent_) {
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
  }
}

void ObLogRestoreHandler::switch_role(const common::ObRole &role, const int64_t proposal_id)
{
  WLockGuard guard(lock_);
  role_ = role;
  proposal_id_ = proposal_id;
  if (! is_strong_leader(role_)) {
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
    context_.reset();
  }
}

bool ObLogRestoreHandler::is_valid() const
{
  return true == is_inited_
         && false == is_in_stop_state_
         && true == palf_handle_.is_valid()
         && NULL != palf_env_;
}

int ObLogRestoreHandler::get_upper_limit_ts(int64_t &ts) const
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_)) {
    ret = OB_EAGAIN;
  } else {
    parent_->get_upper_limit_ts(ts);
  }
  return ret;
}

int ObLogRestoreHandler::get_max_restore_log_ts(int64_t &ts) const
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_) || ! parent_->to_end()) {
    ret = OB_EAGAIN;
  } else {
    parent_->get_end_ts(ts);
  }
  return ret;
}

int ObLogRestoreHandler::add_source(share::DirArray &array, const int64_t end_log_ts)
{
  UNUSED(array);
  UNUSED(end_log_ts);
  /*
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(array.empty() || OB_INVALID_TIMESTAMP == end_log_ts)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(array), K(end_log_ts), KPC(this));
  } else if (FALSE_IT(alloc_source(share::ObLogArchiveSourceType::RAWPATH))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteRawPathParent *source = static_cast<ObRemoteRawPathParent *>(parent_);
    if (OB_FAIL(source->set(array, end_log_ts))) {
      CLOG_LOG(WARN, "ObRemoteRawPathParent set failed", K(ret), K(array), K(end_log_ts));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    }
  }
  */
  return OB_NOT_SUPPORTED;;
}

int ObLogRestoreHandler::add_source(logservice::DirArray &array, const int64_t end_log_ts)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
    /*
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
    ret = OB_NOT_MASTER;
    */
  } else if (OB_UNLIKELY(array.empty() || OB_INVALID_TIMESTAMP == end_log_ts)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(array), K(end_log_ts), KPC(this));
  } else if (FALSE_IT(alloc_source(share::ObLogArchiveSourceType::RAWPATH))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteRawPathParent *source = static_cast<ObRemoteRawPathParent *>(parent_);
    if (OB_FAIL(source->set(array, end_log_ts))) {
      CLOG_LOG(WARN, "ObRemoteRawPathParent set failed", K(ret), K(array), K(end_log_ts));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    }
  }
  return ret;;
}

int ObLogRestoreHandler::add_source(share::ObBackupDest &dest, const int64_t end_log_ts)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(! dest.is_valid() || OB_INVALID_TIMESTAMP == end_log_ts)) {
    ret = OB_INVALID_ARGUMENT;
     CLOG_LOG(WARN, "invalid argument", K(ret), K(end_log_ts), K(dest), KPC(this));
  } else if (FALSE_IT(alloc_source(share::ObLogArchiveSourceType::LOCATION))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteLocationParent *source = static_cast<ObRemoteLocationParent *>(parent_);
    if (OB_FAIL(source->set(dest, end_log_ts))) {
      CLOG_LOG(WARN, "ObRemoteLocationParent set failed", K(ret), K(end_log_ts), K(dest));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    }
  }
  return ret;
}

int ObLogRestoreHandler::add_source(const common::ObAddr &addr, const int64_t end_log_ts)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(!addr.is_valid() || OB_INVALID_TIMESTAMP == end_log_ts)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(end_log_ts), K(addr), KPC(this));
  } else if (FALSE_IT(alloc_source(ObLogArchiveSourceType::SERVICE))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteSerivceParent *source = static_cast<ObRemoteSerivceParent *>(parent_);
    if (OB_FAIL(source->set(addr, end_log_ts))) {
      CLOG_LOG(WARN, "ObRemoteSerivceParent set failed",
          K(ret), K(end_log_ts), K(addr), KPC(this));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    }
  }
  return ret;
}

int ObLogRestoreHandler::raw_write(const palf::LSN &lsn,
                                   const char *buf,
                                   const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t wait_times = 0;
  const bool need_nonblock = true;
  palf::PalfAppendOptions opts;
  opts.need_nonblock = need_nonblock;
  opts.need_check_proposal_id = true;
  while (wait_times < MAX_RAW_WRITE_RETRY_TIMES) {
    do {
      RLockGuard guard(lock_);
      if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
      } else if (is_in_stop_state_) {
        ret = OB_IN_STOP_STATE;
      } else if (LEADER != role_) {
        ret = OB_NOT_MASTER;
      } else if (OB_UNLIKELY(!lsn.is_valid() || NULL == buf || 0 >= buf_size)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        opts.proposal_id = proposal_id_;
        ret = palf_handle_.raw_write(opts, lsn, buf, buf_size);
      }
    } while (0);

    if (OB_EAGAIN == ret) {
      static const int64_t MAX_SLEEP_US = 100;
      ++wait_times;
      int64_t sleep_us = wait_times * 10;
      if (sleep_us > MAX_SLEEP_US) {
        sleep_us = MAX_SLEEP_US;
      }
      ob_usleep(sleep_us);
    } else {
      break;
    }
  }
  return ret;
}

void ObLogRestoreHandler::deep_copy_source(ObRemoteSourceGuard &source_guard)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  ObRemoteLogParent *source = NULL;
  if (OB_ISNULL(parent_)) {
    CLOG_LOG(WARN, "parent_ is NULL", K(ret));
  } else if (OB_ISNULL(source = ObResSrcAlloctor::alloc(parent_->get_source_type(), share::ObLSID(id_)))) {
  } else if (FALSE_IT(parent_->deep_copy_to(*source))) {
  } else if (FALSE_IT(source_guard.set_source(source))) {
  }
}

int ObLogRestoreHandler::schedule(const int64_t id,
    const int64_t proposal_id,
    const LSN &lsn,
    bool &scheduled)
{
  int ret = OB_SUCCESS;
  scheduled = false;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (id != id_ || proposal_id != proposal_id_) {
    // stale task
  } else if (OB_UNLIKELY(context_.issued_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    scheduled = true;
    context_.issued_ = true;
    context_.max_submit_lsn_ = lsn;
  }
  return ret;
}

int ObLogRestoreHandler::update_fetch_log_progress(const int64_t id,
    const int64_t proposal_id,
    const LSN &max_fetch_lsn,
    const int64_t max_submit_log_ts,
    const bool is_finished,
    const bool is_to_end,
    bool &is_stale)
{
  int ret = OB_SUCCESS;
  is_stale = false;
  WLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    is_stale = true;
    CLOG_LOG(INFO, "ls not leader, stale task, just skip it", K(id), K(role_));
  } else if (OB_UNLIKELY(id != id_ || proposal_id != proposal_id_)) {
    is_stale = true;
    context_.issued_ = false;
    CLOG_LOG(INFO, "stale task, just skip it", K(id), K(proposal_id), KPC(this));
  } else {
    context_.issued_ = ! is_finished && ! is_to_end;
    context_.max_fetch_lsn_ = max_fetch_lsn;
    context_.last_fetch_ts_ = ObTimeUtility::current_time();
    parent_->set_to_end(is_to_end, max_submit_log_ts);
  }
  return ret;
}

int ObLogRestoreHandler::need_schedule(bool &need_schedule,
    int64_t &proposal_id,
    ObRemoteFetchContext &context) const
{
  int ret = OB_SUCCESS;
  need_schedule = false;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(parent_)) {
    // do nothing
  } else if (OB_SUCCESS != context_.error_context_.ret_code_) {
    // error exist, no need schedule
  } else {
    need_schedule = is_strong_leader(role_) && ! context_.issued_ && ! parent_->to_end();
    proposal_id = proposal_id_;
    context = context_;
  }
  if (! need_schedule) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      CLOG_LOG(INFO, "restore not schedule", KPC(this));
    }
  }
  return ret;
}

bool ObLogRestoreHandler::need_update_source() const
{
  RLockGuard guard(lock_);
  return is_strong_leader(role_);
}

void ObLogRestoreHandler::mark_error(share::ObTaskId &trace_id, const int ret_code)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    CLOG_LOG(INFO, "not leader, no need record error", K(id_), K(ret_code));
  } else if (OB_SUCCESS == context_.error_context_.ret_code_) {
    context_.error_context_.ret_code_ = ret_code;
    context_.error_context_.trace_id_.set(trace_id);
  }
}

int ObLogRestoreHandler::get_restore_error(share::ObTaskId &trace_id, int &ret_code, bool &error_exist)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  error_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    // not leader, not report
  } else if (OB_SUCCESS == context_.error_context_.ret_code_) {
    // no error, not report
  } else {
    trace_id.set(context_.error_context_.trace_id_);
    ret_code = context_.error_context_.ret_code_;
    error_exist = true;
  }
  return ret;
}
int ObLogRestoreHandler::update_location_info(ObRemoteLogParent *source)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(source)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "source is NULL", K(ret), K(source));
  } else if (OB_ISNULL(parent_) || ! is_strong_leader(role_)) {
    // stale task, just skip
  } else if (source->get_source_type() != parent_->get_source_type()) {
    CLOG_LOG(INFO, "source type not consistent with parent, just skip", KPC(source), KPC(this));
  } else if (OB_FAIL(parent_->update_locate_info(*source))) {
    CLOG_LOG(WARN, "update location info failed", K(ret), KPC(source), KPC(this));
  }
  return ret;
}

void ObLogRestoreHandler::alloc_source(const ObLogArchiveSourceType &type)
{
  if (parent_ != NULL && parent_->get_source_type() != type) {
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
  }

  if (NULL == parent_) {
    parent_ = ObResSrcAlloctor::alloc(type, share::ObLSID(id_));
  }
}

int ObLogRestoreHandler::get_restore_sync_ts(const share::ObLSID &id, int64_t &log_ts)
{
  int ret = OB_SUCCESS;
  int64_t upper_limit_ts = OB_INVALID_TIMESTAMP;
  UNUSED(id);
  RLockGuard guard(lock_);
  if (OB_ISNULL(parent_)) {
    ret = OB_EAGAIN;
  } else {
    parent_->get_upper_limit_ts(upper_limit_ts);
    log_ts = std::max(log_ts, upper_limit_ts);
  }
  return ret;
}

int ObLogRestoreHandler::check_restore_done(bool &done)
{
  int ret = OB_SUCCESS;
  int64_t end_ts = OB_INVALID_TIMESTAMP;
  int64_t replay_ts = OB_INVALID_TIMESTAMP;
  done = false;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    ret = OB_NOT_MASTER;
  } else if (OB_ISNULL(parent_)) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "parent is NULL, need wait", K(ret));
  } else if (! parent_->to_end()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      CLOG_LOG(WARN, "log not restore to end, need wait", K(ret), KPC(this));
    }
  } else if (FALSE_IT(parent_->get_end_ts(end_ts))) {
  } else if (OB_FAIL(MTL(ObLogService*)->get_log_replay_service()->get_min_unreplayed_log_ts_ns(ObLSID(id_), replay_ts))) {
    CLOG_LOG(WARN, "get min unreplay log ts failed", K(ret), KPC(this));
  } else if (replay_ts > end_ts) {
    done = true;
    CLOG_LOG(INFO, "check restore done succ", KPC(this));
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
