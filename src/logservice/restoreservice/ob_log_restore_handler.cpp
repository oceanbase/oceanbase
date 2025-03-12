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
#include "logservice/ob_log_service.h"       // ObLogService
#include "ob_remote_log_source_allocator.h"  // ObResSrcAlloctor

namespace oceanbase
{
using namespace palf;
namespace logservice
{
using namespace oceanbase::share;
using namespace oceanbase::archive;

const char *restore_comment_str[static_cast<int>(RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS)] = {
  "Invalid restore status",
  " ",
  "There is a gap between the log source and standby",
  "Log conflicts, the standby with the same LSN is different from the log source when submit log",
  "Log conflicts, the standby with the same LSN is different from the log source when fetch log",
  "Log source can not be accessed, the replication account may be incorrect or the privelege is insufficient",
  "Log source is unreachable, the log source access point may be unavailable",
  "Fetch log time out",
  "Restore suspend, the log stream has synchronized to recovery until scn",
  "Standby binary version is lower than primary data version, standby need to upgrade",
  "Primary tenant has been dropped",
  "Waiting log stream created",
  "Query primary failed",
  "Restore handler has no leader",
  "Unexpected exceptions",
};

const char *restore_status_str[static_cast<int>(RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS)] = {
  "INVALID RESTORE STATUS",
  "NORMAL",
  "SOURCE HAS A GAP",
  "STANDBY LOG NOT MATCH",
  "STANDBY LOG NOT MATCH",
  "CHECK USER OR PASSWORD",
  "CHECK NETWORK",
  "FETCH LOG TIMEOUT",
  "RESTORE SUSPEND",
  "STANDBY NEED UPGRADE",
  "PRIMARY TENANT DROPPED",
  "WAITING LS CREATED",
  "QUERY PRIMARY FAILED",
  "RESTORE HANDLER HAS NO LEADER",
  "NOT AVAILABLE",
};

RestoreSyncStatus str_to_restore_sync_status(const ObString &str)
{
  RestoreSyncStatus ret_status = RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS;
  if (str.empty()) {
    ret_status = RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS;
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(restore_status_str); i++) {
      if (0 == str.case_compare(restore_status_str[i])) {
        ret_status = static_cast<RestoreSyncStatus>(i);
        break;
      }
    }
  }
  return ret_status;
}

ObLogRestoreHandler::ObLogRestoreHandler() :
  parent_(NULL),
  context_(),
  restore_context_()
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

// To support log restore in parallel, the FetchLogTask is introduced and each task covers a range of logs, as [Min_LSN, Max_LSN),
// which means logs wiil be pulled from the log restore source  and submitted to Palf with this task.
//
// Two scenarios should be handled:
// 1) Log restore is done within the leader of restore handler, which changes with the Palf
// 2) The restore end_scn maybe set to control the log pull and restore.
//
// For follower, log restore should terminate while all tasks should be freed and the restore context can be reset.
// The same as restore to_end.
//
// To generate nonoverlapping and continuous FetchLogTask, a max_submit_lsn_ in context is hold, which is the Max_LSN of the previous task
// and the Min_LSN of the current task.
//
// When role change of the restore handler, the context is reset but which is not reset when restore to_end is set.
//
// The role change is easy to distinguish with the proposal_id while restore_scn change is not,
// so reset restore context and advance issue_version, to reset start_fetch_log_lsn if restore_scn is advanced
// and free issued tasks before restore to_end(paralleled)
//
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

int ObLogRestoreHandler::get_role(common::ObRole &role, int64_t &proposal_id) const
{
  return ObLogHandlerBase::get_role(role, proposal_id);
}

bool ObLogRestoreHandler::is_valid() const
{
  return true == is_inited_
         && false == is_in_stop_state_
         && true == palf_handle_.is_valid()
         && NULL != palf_env_;
}

int ObLogRestoreHandler::get_upper_limit_scn(SCN &scn) const
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_)) {
    ret = OB_EAGAIN;
  } else {
    parent_->get_upper_limit_scn(scn);
  }
  return ret;
}

int ObLogRestoreHandler::get_max_restore_scn(SCN &scn) const
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_) || ! restore_to_end_unlock_()) {
    ret = OB_EAGAIN;
  } else {
    parent_->get_end_scn(scn);
  }
  return ret;
}

int ObLogRestoreHandler::add_source(logservice::DirArray &array, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(array.empty() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(array), K(end_scn), KPC(this));
  } else if (FALSE_IT(alloc_source(share::ObLogRestoreSourceType::RAWPATH))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteRawPathParent *source = static_cast<ObRemoteRawPathParent *>(parent_);
    const bool source_exist = source->is_valid();
    if (OB_FAIL(source->set(array, end_scn))) {
      CLOG_LOG(WARN, "ObRemoteRawPathParent set failed", K(ret), K(array), K(end_scn));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    } else if (! source_exist) {
      context_.set_issue_version();
    }
  }
  return ret;;
}

int ObLogRestoreHandler::add_source(share::ObBackupDest &dest, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(! dest.is_valid() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
     CLOG_LOG(WARN, "invalid argument", K(ret), K(end_scn), K(dest), KPC(this));
  } else if (FALSE_IT(alloc_source(share::ObLogRestoreSourceType::LOCATION))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteLocationParent *source = static_cast<ObRemoteLocationParent *>(parent_);
    const bool source_exist = source->is_valid();
    if (OB_FAIL(source->set(dest, end_scn))) {
      CLOG_LOG(WARN, "ObRemoteLocationParent set failed", K(ret), K(end_scn), K(dest));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    } else if (! source_exist) {
      context_.set_issue_version();
    }
  }
  return ret;
}

int ObLogRestoreHandler::add_source(const share::ObRestoreSourceServiceAttr &service_attr, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(!service_attr.is_valid() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(end_scn), K(service_attr), KPC(this));
  } else if (FALSE_IT(alloc_source(ObLogRestoreSourceType::SERVICE))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteSerivceParent *source = static_cast<ObRemoteSerivceParent *>(parent_);
    const bool source_exist = source->is_valid();
    if (OB_FAIL(source->set(service_attr, end_scn))) {
      CLOG_LOG(WARN, "ObRemoteSerivceParent set failed",
          K(ret), K(end_scn), K(service_attr), KPC(this));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    } else if (! source_exist) {
      context_.reset();
    }
  }
  return ret;
}

int ObLogRestoreHandler::clean_source()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (NULL == parent_) {
    // just skip
  } else {
    CLOG_LOG(INFO, "log_restore_source is empty, clean source", KPC(parent_));
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
    context_.reset();
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_SUBMIT_LOG_ERROR);
int ObLogRestoreHandler::raw_write(const int64_t proposal_id,
                                   const palf::LSN &lsn,
                                   const SCN &scn,
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
      ret = OB_SUCCESS;
      RLockGuard guard(lock_);
      if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
      } else if (is_in_stop_state_) {
        ret = OB_IN_STOP_STATE;
      } else if (LEADER != role_) {
        ret = OB_NOT_MASTER;
      } else if (OB_UNLIKELY(!lsn.is_valid()
            || NULL == buf
            || 0 >= buf_size
            || 0 >= proposal_id)) {
        ret = OB_INVALID_ARGUMENT;
        CLOG_LOG(WARN, "invalid argument", K(ret), K(proposal_id), K(lsn), K(buf), K(buf_size));
      } else if (proposal_id != proposal_id_) {
        ret = OB_NOT_MASTER;
        CLOG_LOG(INFO, "stale task, just skip", K(proposal_id), K(proposal_id_), K(lsn), K(id_));
      } else if (NULL == parent_ || restore_to_end_unlock_()) {
        ret = OB_RESTORE_LOG_TO_END;
        CLOG_LOG(INFO, "submit log to end, just skip", K(ret), K(lsn), KPC(this));
      } else {
        opts.proposal_id = proposal_id_;
        // errsim fake error
        if (ERRSIM_SUBMIT_LOG_ERROR) {
          ret = ERRSIM_SUBMIT_LOG_ERROR;
          CLOG_LOG(TRACE, "errsim submit log error");
        } else {
          ret = palf_handle_.raw_write(opts, lsn, buf, buf_size);
          if (OB_SUCC(ret)) {
            uint64_t tenant_id = palf_env_->get_palf_env_impl()->get_tenant_id();
            EVENT_TENANT_ADD(ObStatEventIds::RESTORE_WRITE_LOG_SIZE, buf_size, tenant_id);
          }
        }
      }
    } while (0);

    if (OB_EAGAIN == ret) {
      ++wait_times;
      int64_t sleep_us = wait_times * 10;
      if (sleep_us > MAX_RETRY_SLEEP_US) {
        sleep_us = MAX_RETRY_SLEEP_US;
      }
      ob_usleep(sleep_us);
    } else {
      // other ret code, end loop
      break;
    }
  }
  return ret;
}

int ObLogRestoreHandler::update_max_fetch_info(const int64_t proposal_id,
                                                  const palf::LSN &lsn,
                                                  const SCN &scn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_in_stop_state_) {
    ret = OB_IN_STOP_STATE;
  } else if (LEADER != role_) {
    ret = OB_NOT_MASTER;
  } else if (proposal_id != proposal_id_) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(INFO, "stale task, just skip", K(proposal_id), K(proposal_id_), K(lsn), K(id_));
  } else if (OB_UNLIKELY(!lsn.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid lsn or scn", K(proposal_id), K(lsn), K(scn), KPC(this));
  } else if (context_.max_fetch_lsn_.is_valid() && context_.max_fetch_lsn_ >= lsn) {
    // do nothing
  } else {
    context_.max_fetch_lsn_ = lsn;
    context_.max_fetch_scn_ = scn;
    context_.last_fetch_ts_ = ObTimeUtility::fast_current_time();
    if (parent_->set_to_end(scn)) {
      // To stop and clear all restore log tasks and restore context, reset context and advance issue version
      CLOG_LOG(INFO, "restore log to_end succ", KPC(this), KPC(parent_));
    }
  }
  return ret;
}

void ObLogRestoreHandler::deep_copy_source(ObRemoteSourceGuard &source_guard)
{
  RLockGuard guard(lock_);
  deep_copy_source_(source_guard);
}

int ObLogRestoreHandler::schedule(const int64_t id,
    const int64_t proposal_id,
    const int64_t version,
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
  } else if (id != id_ || proposal_id != proposal_id_ || version != context_.issue_version_) {
    // stale task
  } else {
    scheduled = true;
    context_.max_submit_lsn_ = lsn;
    context_.issue_task_num_++;
  }
  return ret;
}

int ObLogRestoreHandler::try_retire_task(ObFetchLogTask &task, bool &done)
{
  done = false;
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!task.is_valid() || task.id_.id() != id_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (! is_strong_leader(role_) || NULL == parent_ || is_service_log_source_type(parent_->get_source_type())) {
    done = true;
    CLOG_LOG(INFO, "ls not leader or invalid source, stale task, just skip it", K(task), K(role_), KPC(parent_));
  } else if (OB_UNLIKELY(task.proposal_id_ != proposal_id_
        || task.version_ != context_.issue_version_)) {
    done = true;
    CLOG_LOG(INFO, "stale task, just skip it", K(task), KPC(this));
  } else if (context_.max_fetch_lsn_.is_valid() && context_.max_fetch_lsn_ >= task.end_lsn_) {
    CLOG_LOG(INFO, "restore max_lsn bigger than task end_lsn, just skip it", K(task), KPC(this));
    done = true;
    context_.issue_task_num_--;
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
  } else {
    need_schedule = is_strong_leader(role_) && ! restore_to_end_unlock_();
    proposal_id = proposal_id_;
    context = context_;
  }
  if (! need_schedule) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      CLOG_LOG(INFO, "restore not schedule", KPC(this), KPC(parent_));
    }
  }
  return ret;
}

bool ObLogRestoreHandler::need_update_source() const
{
  RLockGuard guard(lock_);
  return is_strong_leader(role_);
}

void ObLogRestoreHandler::mark_error(share::ObTaskId &trace_id,
                                     const int ret_code,
                                     const palf::LSN &lsn,
                                     const ObLogRestoreErrorContext::ErrorType &error_type)
{
  int ret = OB_SUCCESS;
  palf::LSN end_lsn;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    CLOG_LOG(INFO, "not leader, no need record error", K(id_), K(ret_code));
  } else if (OB_FAIL(palf_handle_.get_end_lsn(end_lsn))) {
    CLOG_LOG(WARN, "get end_lsn failed", K(id_));
  } else if (end_lsn < lsn) {
    CLOG_LOG(WARN, "end_lsn smaller than error lsn, just skip", K(id_), K(end_lsn), K(lsn), KPC(parent_), KPC(this));
  } else if (OB_SUCCESS == context_.error_context_.ret_code_ || OB_TIMEOUT == context_.error_context_.ret_code_) {
    context_.error_context_.error_type_ = error_type;
    context_.error_context_.ret_code_ = ret_code;
    context_.error_context_.trace_id_.set(trace_id);
    context_.error_context_.err_lsn_ = lsn;
    if ((OB_TIMEOUT == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_TENANT_NOT_EXIST == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_TENANT_NOT_IN_SERVER == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_IN_STOP_STATE == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_SERVER_IS_INIT == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_ERR_OUT_OF_LOWER_BOUND == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_SIZE_OVERFLOW == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)) {
      CLOG_LOG(WARN, "fetch log failed in restore", KPC(parent_), KPC(this));
    } else if (OB_SUCCESS != ret_code) {
      CLOG_LOG(ERROR, "fatal error occur in restore", KPC(parent_), KPC(this));
    }
  }
}

int ObLogRestoreHandler::get_restore_error(share::ObTaskId &trace_id, int &ret_code, bool &error_exist)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (OB_FAIL(get_restore_error_unlock_(trace_id, ret_code, error_exist))) {
    CLOG_LOG(WARN, "fail to get restore_error");
  }
  return ret;
}

int ObLogRestoreHandler::get_restore_error_unlock_(share::ObTaskId &trace_id, int &ret_code, bool &error_exist)
{
  int ret = OB_SUCCESS;
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

void ObLogRestoreHandler::alloc_source(const ObLogRestoreSourceType &type)
{
  if (parent_ != NULL && parent_->get_source_type() != type) {
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
  }

  if (NULL == parent_) {
    parent_ = ObResSrcAlloctor::alloc(type, share::ObLSID(id_));
  }
}

int ObLogRestoreHandler::check_restore_done(const SCN &recovery_end_scn, bool &done)
{
  int ret = OB_SUCCESS;
  palf::PalfGroupBufferIterator iter;
  SCN end_scn;
  palf::LogGroupEntry entry;
  SCN entry_scn;
  palf::LSN end_lsn;
  int64_t id = 0;
  done = false;
  ObLogService *logservice = MTL(ObLogService*);
  {
    RLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
    } else if (OB_UNLIKELY(!recovery_end_scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", K(ret), K(recovery_end_scn));
    } else if (OB_ISNULL(logservice)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "unexpected error, ObLogService must not be NULL", K(ret), K(recovery_end_scn));
    } else if (restore_context_.seek_done_) {
      end_lsn = restore_context_.lsn_;
    } else if (OB_FAIL(palf_handle_.get_end_scn(end_scn))) {
      CLOG_LOG(WARN, "get end scn failed", K(ret), K_(id));
    } else if (end_scn < recovery_end_scn) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "log restore not finish", K(ret), K_(id), K(end_scn), K(recovery_end_scn));
    } else if (OB_FAIL(seek_log_iterator(ObLSID(id_), recovery_end_scn, iter))) {
      CLOG_LOG(WARN, "palf seek failed", K(ret), K_(id));
    } else if (OB_FAIL(iter.set_io_context(palf::LogIOContext(MTL_ID(), id_, palf::LogIOUser::RESTORE)))) {
      CLOG_LOG(WARN, "set_io_context failed", K(ret), K_(id));
    } else if (OB_FAIL(iter.next())) {
      CLOG_LOG(WARN, "next entry failed", K(ret));
    } else if (OB_FAIL(iter.get_entry(entry, end_lsn))) {
      CLOG_LOG(WARN, "gen entry failed", K(ret), K_(id), K(iter));
    } else if (entry.get_scn() == recovery_end_scn) {
      // if the max log scn equals to recovery_end_scn, the max log should be replayed
      // otherwise the max log should not be replayed
      end_lsn = end_lsn + entry.get_serialize_size();
    }
    id = ATOMIC_LOAD(&id_);
  }

  // update restore context
  {
    WLockGuard guard(lock_);
    if (OB_SUCC(ret) && ! restore_context_.seek_done_) {
      restore_context_.lsn_ = end_lsn;
      restore_context_.seek_done_ = true;
      CLOG_LOG(INFO, "update restore context", K(id), K_(restore_context));
    }
  }

  if (OB_SUCC(ret)) {
    ObLogReplayService *replayservice = logservice->get_log_replay_service();
    if (OB_ISNULL(replayservice)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "unexpected error, ObLogReplayService must not be NULL", K(ret), K(recovery_end_scn));
    } else if (OB_FAIL(replayservice->is_replay_done(ObLSID(id), end_lsn, done))) {
      CLOG_LOG(WARN, "is_replay_done failed", K(ret), K(id), K(end_lsn));
    } else if (! done) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "log restore finish, and log replay not finish",
          K(ret), K(id), K(end_lsn), K(end_scn), K(recovery_end_scn));
    } else {
      CLOG_LOG(TRACE, "check restore done succ", K(ret), K(id));
    }
  }
  return ret;
}

int ObLogRestoreHandler::check_restore_to_newest(share::SCN &end_scn, share::SCN &archive_scn)
{
  int ret = OB_SUCCESS;
  ObRemoteSourceGuard guard;
  ObRemoteLogParent *source = NULL;
  ObLogArchivePieceContext *piece_context = NULL;
  share::ObBackupDest *dest = NULL;
  palf::LSN end_lsn;
  end_scn = SCN::min_scn();
  archive_scn = SCN::min_scn();
  SCN restore_scn = SCN::min_scn();
  CLOG_LOG(INFO, "start check restore to newest", K(id_));
  {
    RLockGuard lock_guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
    } else if (! is_strong_leader(role_)) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN, "not leader", K(ret), KPC(this));
    } else if (OB_FAIL(palf_handle_.get_end_lsn(end_lsn))) {
      CLOG_LOG(WARN, "get end lsn failed", K(id_));
    } else if (OB_FAIL(palf_handle_.get_end_scn(end_scn))) {
      CLOG_LOG(WARN, "get end scn failed", K(id_));
    } else if (FALSE_IT(deep_copy_source_(guard))) {
    } else if (OB_ISNULL(source = guard.get_source())) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "invalid source", K(ret), KPC(this), KPC(source));
    } else if (! source->is_valid()) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "source is invalid", K(ret), KPC(this), KPC(source));
    }
  }

  if (OB_SUCC(ret) && NULL != source) {
    if (share::is_location_log_source_type(source->get_source_type())) {
      ObRemoteLocationParent *location_source = dynamic_cast<ObRemoteLocationParent *>(source);
      ret = check_restore_to_newest_from_archive_(*location_source, end_lsn, end_scn, archive_scn);
    } else if (share::is_service_log_source_type(source->get_source_type())) {
      ObRemoteSerivceParent *service_source = dynamic_cast<ObRemoteSerivceParent *>(source);
      share::ObRestoreSourceServiceAttr *service_attr = NULL;
      service_source->get(service_attr, restore_scn);
      ret = check_restore_to_newest_from_service_(*service_attr, end_scn, archive_scn);
    } else if (share::is_raw_path_log_source_type(source->get_source_type())) {
      ObRemoteRawPathParent *rawpath_source = dynamic_cast<ObRemoteRawPathParent *>(source);
      ObLogRawPathPieceContext *rawpath_ctx = NULL;
      rawpath_source->get(rawpath_ctx, restore_scn);
      ret = check_restore_to_newest_from_rawpath_(*rawpath_ctx, end_lsn, end_scn, archive_scn);
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObLogRestoreHandler::submit_sorted_task(ObFetchLogTask &task)
{
  WLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "restore_handler not master", K(ret), KPC(this));
  } else if (OB_UNLIKELY(! task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (OB_UNLIKELY(task.iter_.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "task iterator is empty", K(ret), K(task));
  } else if (OB_FAIL(context_.submit_array_.push_back(&task))) {
    CLOG_LOG(WARN, "push back failed", K(ret), K(task));
  } else {
    lib::ob_sort(context_.submit_array_.begin(), context_.submit_array_.end(), FetchLogTaskCompare());
  }
  return ret;
}

int ObLogRestoreHandler::get_next_sorted_task(ObFetchLogTask *&task)
{
  int ret = OB_SUCCESS;
  palf::LSN max_lsn;
  ObFetchLogTask *first = NULL;
  task = NULL;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    ret = OB_NOT_MASTER;
  } else if (context_.submit_array_.empty()) {
    // sorted_array is empty, do nothing
  } else if (FALSE_IT(first = context_.submit_array_.at(0))) {
    // get the first one
  } else if (OB_FAIL(palf_handle_.get_max_lsn(max_lsn))) {
    CLOG_LOG(WARN, "get max lsn failed", K(ret), K_(id));
  } else if (max_lsn < first->start_lsn_) {
    // check the first task if is in turn, skip it if not
    CLOG_LOG(TRACE, "task not in turn", KPC(first), K(max_lsn));
  } else if (context_.submit_array_.count() == 1) {
    // only one task in array, just pop it
    context_.submit_array_.pop_back();
    task = first;
  } else {
    // more than one task, replace first and end, and sort again
    ObFetchLogTask *tmp_task = NULL;
    context_.submit_array_.pop_back(tmp_task);
    context_.submit_array_.at(0) = tmp_task;
    lib::ob_sort(context_.submit_array_.begin(), context_.submit_array_.end(), FetchLogTaskCompare());
    task = first;
  }
  return ret;
}

int ObLogRestoreHandler::diagnose(RestoreDiagnoseInfo &diagnose_info) const
{
  int ret = OB_SUCCESS;
  diagnose_info.restore_context_info_.reset();
  diagnose_info.restore_context_info_.reset();
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char trace_id[MAX_TRACE_ID_LENGTH];
  const bool need_ignore_invalid = true;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (FALSE_IT(diagnose_info.restore_role_ = role_)) {
  } else if (FALSE_IT(diagnose_info.restore_proposal_id_ = proposal_id_)) {
  } else if (OB_FAIL(diagnose_info.restore_context_info_.append_fmt("issue_task_num:%ld; "
                                                                    "last_fetch_ts:%ld; "
                                                                    "max_submit_lsn:%ld; "
                                                                    "max_fetch_lsn:%ld; "
                                                                    "max_fetch_scn:%ld; ",
                                                                    context_.issue_task_num_,
                                                                    context_.last_fetch_ts_,
                                                                    context_.max_submit_lsn_.val_,
                                                                    context_.max_fetch_lsn_.val_,
                                                                    context_.max_fetch_scn_.convert_to_ts(need_ignore_invalid)))) {
    CLOG_LOG(WARN, "append restore_context_info failed", K(ret), K(context_));
  } else if (FALSE_IT(context_.error_context_.trace_id_.to_string(trace_id, sizeof(trace_id)))) {
  } else if (OB_FAIL(diagnose_info.restore_err_context_info_.append_fmt("ret_code:%d; "
                                                                        "trace_id:%s; ",
                                                                        context_.error_context_.ret_code_,
                                                                        trace_id))) {
    CLOG_LOG(WARN, "append restore_context_info failed", K(ret), K(context_));
  }
  return ret;
}

int ObLogRestoreHandler::refresh_error_context()
{
  int ret = OB_SUCCESS;
  palf::LSN end_lsn;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    CLOG_LOG(TRACE, "not leader, no need refresh error context", K(id_));
  } else if (OB_FAIL(palf_handle_.get_end_lsn(end_lsn))) {
    CLOG_LOG(WARN, "get end_lsn failed", K(id_));
  } else if (end_lsn > context_.error_context_.err_lsn_ && OB_SUCCESS != context_.error_context_.ret_code_) {
    context_.error_context_.reset();
    CLOG_LOG(INFO, "flush error context to success", K(id_), K(context_), K(end_lsn), KPC(parent_), KPC(this));
  }
  return ret;
}

bool ObLogRestoreHandler::restore_to_end() const
{
  RLockGuard guard(lock_);
  return restore_to_end_unlock_();
}
ERRSIM_POINT_DEF(ERRSIM_LS_STATE_NOT_MATCH);
int ObLogRestoreHandler::check_restore_to_newest_from_service_(
    const share::ObRestoreSourceServiceAttr &service_attr,
    const share::SCN &end_scn,
    share::SCN &archive_scn)
{
  int ret = OB_SUCCESS;
  bool offline_log_exist = false;
  palf::AccessMode access_mode;
  SMART_VAR(share::ObLogRestoreProxyUtil, proxy_util) {
    if (OB_FAIL(proxy_util.init_with_service_attr(MTL_ID(), &service_attr))) {
      CLOG_LOG(WARN, "proxy_util init failed", K(id_), K(service_attr));
    } else if (OB_FAIL(proxy_util.get_max_log_info(share::ObLSID(id_), access_mode, archive_scn))) {
      // OB_ENTRY_NOT_EXIST, ls not exist in gv$ob_log_stat, a) ls has no leader; b) access virtual table failed; c) ls gc
      if (OB_ENTRY_NOT_EXIST == ret) {
        // get ls from dba_ob_ls
        // 1. OB_LS_NOT_EXIST, ls gc in log restore source tenant, check if offline_log already transported successfully
        // 2. OB_SUCCESS, ls still exist, check in next turn
        // 3. other error code
        if (OB_FAIL(proxy_util.is_ls_existing(share::ObLSID(id_))) && OB_LS_NOT_EXIST != ret) {
          CLOG_LOG(WARN, "get_ls failed", K(id_));
        } else if (OB_SUCCESS == ret) {
          ret = OB_EAGAIN;
          CLOG_LOG(WARN, "get_ls succ, while get ls max_log info failed, just retry", K(id_));
        } else if (OB_FAIL(check_if_ls_gc_(offline_log_exist))) {
          CLOG_LOG(WARN, "check ls_gc failed", K(id_));
        } else if (offline_log_exist) {
          archive_scn = end_scn;
          CLOG_LOG(INFO, "check ls_gc succ, set archive_scn equals to end_scn", K(id_), K(end_scn), K(service_attr));
        } else {
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
          CLOG_LOG(ERROR, "ls not exist in primary while offline_log not exist in standby, check log gap",
              K(id_), K(end_scn), K(service_attr));
        }
      } else {
        CLOG_LOG(WARN, "get max_log info failed", K(id_));
      }
    } else if (!palf::is_valid_access_mode(access_mode) || palf::AccessMode::RAW_WRITE != access_mode) {
      ret = OB_SOURCE_LS_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "access_mode not match, check if ls gc in log restore source tenant", K(id_), K(access_mode));
      // rewrite ret code, retry next time
      ret = OB_EAGAIN;
    } else if (end_scn < archive_scn) {
      CLOG_LOG(INFO, "end_scn smaller than archive_scn", K(id_), K(archive_scn), K(end_scn));
    } else {
      CLOG_LOG(INFO, "check_restore_to_newest succ", K(id_), K(archive_scn), K(end_scn));
    }
  }
   if (OB_UNLIKELY(ERRSIM_LS_STATE_NOT_MATCH)) {
    ret = OB_SUCC(ret) ? OB_SOURCE_LS_STATE_NOT_MATCH : ret;
    CLOG_LOG(WARN, "ERRSIM_LS_STATE_NOT_MATCH is on", KR(ret));
   }
  // if connect to source tenant denied, rewrite ret_code
  if (-ER_ACCESS_DENIED_ERROR == ret) {
    ret = OB_PASSWORD_WRONG;
  }
  return ret;
}

int ObLogRestoreHandler::check_restore_to_newest_from_archive_(
    ObRemoteLocationParent &location_parent,
    const palf::LSN &end_lsn,
    const share::SCN &end_scn,
    share::SCN &archive_next_scn)
{
  int ret = OB_SUCCESS;
  ObLogArchivePieceContext *piece_context = NULL;
  share::ObBackupDest *dest = NULL;
  share::SCN restore_scn;
  if (OB_FAIL(get_next_log_after_end_lsn_(location_parent, end_lsn, end_scn, archive_next_scn))) {
    CLOG_LOG(WARN, "get max archive log failed", K(id_));
  } else if (end_scn < archive_next_scn) {
    CLOG_LOG(INFO, "end_scn smaller than archive_scn", K(id_), K(archive_next_scn), K(end_scn));
  } else {
    CLOG_LOG(INFO, "check_restore_to_newest succ", K(id_), K(archive_next_scn), K(end_scn));
  }
  return ret;
}

int ObLogRestoreHandler::get_next_log_after_end_lsn_(ObRemoteLocationParent &location_parent,
  const palf::LSN &end_lsn, const share::SCN &end_scn, share::SCN &archive_scn)
{
  int ret = OB_SUCCESS;

  class GetSourceFunctor {
  public:
    GetSourceFunctor(ObRemoteLocationParent &location_parent):
        location_parent_(location_parent) {}
    int operator()(const share::ObLSID &id, ObRemoteSourceGuard &guard)
    {
      int ret = OB_SUCCESS;
      ObRemoteLocationParent *location_parent = static_cast<ObRemoteLocationParent*>(ObResSrcAlloctor::alloc(share::ObLogRestoreSourceType::LOCATION, id));
      if (OB_ISNULL(location_parent)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(WARN, "failed to allocate location_parent", K(id));
      } else if (OB_FAIL(location_parent_.deep_copy_to(*location_parent))) {
        CLOG_LOG(WARN, "failed to deep copy to location_parent");
      } else if (OB_FAIL(guard.set_source(location_parent))) {
        CLOG_LOG(WARN, "failed to set location source");
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(location_parent)) {
        ObResSrcAlloctor::free(location_parent);
        location_parent = nullptr;
      }
      return ret;
    }
  private:
    ObRemoteLocationParent &location_parent_;
  };
  GetSourceFunctor get_source_func(location_parent);
  ObRemoteLogGroupEntryIterator remote_iter(get_source_func);
  LargeBufferPool tmp_buffer_pool;
  ObLogExternalStorageHandler tmp_handler;
  LogGroupEntry tmp_entry;
  LSN tmp_lsn;
  const char *tmp_buf = NULL;
  int64_t tmp_buf_len = 0;
  constexpr int64_t DEFAULT_BUF_SIZE = 64L * 1024 * 1024;

  if (OB_FAIL(tmp_buffer_pool.init("TmpLargePool", 1024L * 1024 * 1024))) {
    CLOG_LOG(WARN, "failed to init tmp_buffer_pool");
  } else if (OB_FAIL(tmp_handler.init())) {
    CLOG_LOG(WARN, "failed to init tmp_handler");
  } else if (OB_FAIL(tmp_handler.start(0))) {
    CLOG_LOG(WARN, "failed to start tmp_handler");
  } else if (OB_FAIL(remote_iter.init(MTL_ID(), ObLSID(id_), end_scn, end_lsn, palf::LSN(palf::LOG_MAX_LSN_VAL), &tmp_buffer_pool, &tmp_handler, DEFAULT_BUF_SIZE))) {
    CLOG_LOG(WARN, "failed to init remote_iter");
  } else if (OB_FAIL(remote_iter.next(tmp_entry, tmp_lsn, tmp_buf, tmp_buf_len))) {
    if (OB_ITER_END == ret) {
      archive_scn = end_scn;
      ret = OB_SUCCESS;
    } else if (OB_ARCHIVE_ROUND_NOT_CONTINUOUS == ret) {
      // return
      CLOG_LOG(WARN, "round not continuous, there could be newer archivelog");
    } else {
      CLOG_LOG(WARN, "failed to iterate remote_log");
    }
  } else {
    archive_scn = tmp_entry.get_scn();
  }
  remote_iter.reset();
  tmp_handler.stop();
  tmp_handler.wait();
  tmp_handler.destroy();
  tmp_buffer_pool.destroy();

  return ret;
}

int ObLogRestoreHandler::check_restore_to_newest_from_rawpath_(ObLogRawPathPieceContext &rawpath_ctx,
    const palf::LSN &end_lsn, const share::SCN &end_scn, share::SCN &archive_scn)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id(id_);

  if (! rawpath_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "rawpath ctx is invalid");
  } else {
    palf::LSN archive_lsn;
    if (OB_FAIL(rawpath_ctx.get_max_archive_log(archive_lsn, archive_scn))) {
      CLOG_LOG(WARN, "fail to get max archive log", K_(id));
    } else if (archive_lsn <= end_lsn && archive_scn == SCN::min_scn()) {
      archive_scn = end_scn;
      CLOG_LOG(INFO, "rewrite archive_scn while end_lsn equals to archive_lsn and archive_scn not got",
        K(id_), K(archive_lsn), K(archive_scn), K(end_lsn), K(end_scn));
    } else if (end_scn < archive_scn) {
      CLOG_LOG(INFO, "end_scn smaller than archive_scn", K_(id), K(end_scn));
    } else if (end_lsn < archive_lsn) {
      ret = OB_EAGAIN;
      CLOG_LOG(INFO, "end_lsn smaller than archive_lsn", K_(id), K(end_lsn));
    } else {
      CLOG_LOG(INFO, "check_restore_to_newest succ", K(id_), K(archive_scn), K(end_scn), K(end_lsn));
    }
  }
  return ret;
}

int ObLogRestoreHandler::check_if_ls_gc_(bool &done)
{
  int ret = OB_SUCCESS;
  share::SCN offline_scn;
  done = false;
  if (OB_FAIL(get_offline_scn_(offline_scn))) {
    CLOG_LOG(WARN, "get offline_scn failed", K(offline_scn), K(id_));
  } else if (offline_scn.is_valid()) {
    done = true;
    CLOG_LOG(INFO, "offline_scn is valid, ls gc", K(id_), K(offline_scn));
  } else if (OB_FAIL(check_offline_log_(done))) {
    CLOG_LOG(WARN, "check offline_log failed", K(id_), K(offline_scn));
  } else if (!done) {
    // if check offline_log failed, double check if offline_scn valid
    if (OB_FAIL(get_offline_scn_(offline_scn))) {
      CLOG_LOG(WARN, "get offline_scn failed", K(offline_scn), K(id_));
    } else if (offline_scn.is_valid()) {
      done = true;
      CLOG_LOG(INFO, "offline_scn is valid, ls gc", K(id_), K(offline_scn));
    }
  }
  return ret;
}

// function out of lock, use palf guard to open palf
int ObLogRestoreHandler::check_offline_log_(bool &done)
{
  int ret = OB_SUCCESS;
  share::SCN replayed_scn;
  PalfBufferIterator iter;
  done = false;
  ObLogService *logservice = MTL(ObLogService*);
  ObLogReplayService *replayservice = NULL;
  if (OB_ISNULL(logservice)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error, ObLogService must not be NULL");
  } else if (FALSE_IT(replayservice = logservice->get_log_replay_service())) {
  } else if (OB_ISNULL(replayservice)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error, ObLogReplayService must not be NULL");
  } else if (OB_FAIL(replayservice->get_max_replayed_scn(share::ObLSID(id_), replayed_scn))) {
    CLOG_LOG(WARN, "get replayed_lsn failed", K(id_));
  } else if (OB_FAIL(seek_log_iterator(ObLSID(id_), replayed_scn, iter))) {
    CLOG_LOG(WARN, "seek failed", K(id_));
  } else if (OB_FAIL(iter.set_io_context(palf::LogIOContext(MTL_ID(), id_, palf::LogIOUser::RESTORE)))) {
    CLOG_LOG(WARN, "set_io_context failed", K(id_));
  } else {
    palf::LogEntry entry;
    palf::LSN lsn;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next())) {
        CLOG_LOG(WARN, "next failed", K(id_), K(lsn));
      } else if (OB_FAIL(iter.get_entry(entry, lsn))) {
        CLOG_LOG(WARN, "get entry failed", K(id_), K(lsn), K(entry));
      } else {
        int64_t header_pos = 0;
        int64_t log_pos = 0;
        const char *log_buf = entry.get_data_buf();
        const int64_t log_length = entry.get_data_len();
        logservice::ObLogBaseHeader header;
        const int64_t header_size = header.get_serialize_size();
        if (OB_FAIL(header.deserialize(log_buf, header_size, header_pos))) {
          CLOG_LOG(ERROR, "ObLogBaseHeader deserialize failed", K(id_), K(lsn), K(entry));
        } else if (OB_UNLIKELY(!header.is_valid())) {
          ret = OB_INVALID_DATA;
          CLOG_LOG(ERROR, "log base header not valid", K(id_), K(lsn), K(entry), K(header));
        } else if (OB_UNLIKELY(header_pos >= log_length)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "unexpected log pos", K(id_), K(header_pos), K(log_length), K(entry));
        } else if (logservice::GC_LS_LOG_BASE_TYPE == header.get_log_type()) {
          logservice::ObGCLSLog gc_log;
          if (OB_FAIL(gc_log.deserialize(log_buf, log_length, log_pos))) {
            CLOG_LOG(ERROR, "gc_log deserialize failed", K(id_), K(log_pos), K(log_length), K(entry));
          } else if (logservice::ObGCLSLOGType::OFFLINE_LS == gc_log.get_log_type()) {
            done = true;
            CLOG_LOG(INFO, "offline_log exist", K(id_), K(gc_log), K(lsn), K(entry));
            break;
          }
        }
      }
    } // while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

bool ObLogRestoreHandler::restore_to_end_unlock_() const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  share::SCN scn;
  share::SCN recovery_end_scn;
  if (NULL == parent_) {
    bret = false;
  } else if (parent_->to_end()) {
    bret = true;
  } else if (OB_FAIL(palf_handle_.get_end_scn(scn))) {
    CLOG_LOG(WARN, "get end scn failed", K(id_));
  } else {
    parent_->get_upper_limit_scn(recovery_end_scn);
    bret = scn >= recovery_end_scn;
    CLOG_LOG(INFO, "check restore to end", K(recovery_end_scn), K(scn));
  }
  return bret;
}

int ObLogRestoreHandler::get_ls_restore_status_info(RestoreStatusInfo &restore_status_info)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  LSN lsn;
  share::SCN scn;
  share::ObTaskId trace_id;
  int ret_code = OB_SUCCESS;
  bool error_exist = false;
  bool is_leader = true;
  RestoreSyncStatus sync_status;
  ObRole palf_role = FOLLOWER;
  int64_t palf_proposal_id = -1;
  bool is_pending_state = true;

  if (OB_FAIL(palf_handle_.get_role(palf_role, palf_proposal_id, is_pending_state))) {
    CLOG_LOG(WARN, "fail to get palf role", K(ret), K_(id));
  } else if (LEADER != palf_role || true == is_pending_state) {
    CLOG_LOG(TRACE, "palf is not leader when get ls restore status info", K_(id), K(palf_role), K(is_pending_state));
  } else if (LEADER == palf_role && !is_strong_leader(role_)) {
    is_leader = false;
    CLOG_LOG(WARN, "restore handler not leader", K_(id), K(role_), K(palf_role));
    restore_status_info.ls_id_ = id_;
    restore_status_info.err_code_ = OB_NOT_MASTER;
    restore_status_info.sync_lsn_ = 0;
    restore_status_info.sync_scn_ = SCN::min_scn();
    if (OB_FAIL(get_restore_sync_status(restore_status_info.err_code_, context_.error_context_.error_type_, sync_status))) {
      CLOG_LOG(WARN, "fail to get restore sync status", K_(restore_status_info.err_code), K(sync_status));
    } else if (OB_FALSE_IT(restore_status_info.sync_status_ = sync_status)) { // set sync_status before get_restore_comment
    } else if (OB_FAIL(restore_status_info.get_restore_comment())) {
      CLOG_LOG(WARN, "fail to get restore comment", K(sync_status));
    } else {
      CLOG_LOG(TRACE, "success to get error code and message", K(restore_status_info));
    }
  } else if (OB_FAIL(palf_handle_.get_end_lsn(lsn))) {
    CLOG_LOG(WARN, "fail to get end lsn when get ls restore status info");
  } else if (OB_FAIL(palf_handle_.get_end_scn(scn))) {
    CLOG_LOG(WARN, "fail to get end scn");
  } else if (OB_FAIL(get_restore_error_unlock_(trace_id, ret_code, error_exist))) {
    CLOG_LOG(WARN, "fail to get restore error");
  } else if (error_exist) {
    CLOG_LOG(TRACE, "start to mark restore sync error", K(trace_id), K(ret_code), K(context_.error_context_.error_type_));
    if (OB_FAIL(get_restore_sync_status(ret_code, context_.error_context_.error_type_, sync_status))) {
      CLOG_LOG(WARN, "fail to get sync status", K(ret_code), K(context_.error_context_.error_type_), K(sync_status));
    } else {
      restore_status_info.sync_status_ = sync_status;
    }
  } else if (restore_to_end_unlock_()) {
    restore_status_info.sync_status_ = RestoreSyncStatus::RESTORE_SYNC_SUSPEND;
    CLOG_LOG(TRACE, "restore suspend", K(error_exist), K(restore_status_info.sync_status_));
  } else {
    restore_status_info.sync_status_ = RestoreSyncStatus::RESTORE_SYNC_NORMAL;
    CLOG_LOG(TRACE, "error is not exist, restore sync is normal", K(error_exist), K(restore_status_info.sync_status_));
  }
  if (is_leader && OB_SUCC(ret)) {
    restore_status_info.ls_id_ = id_;
    restore_status_info.err_code_ = ret_code;
    restore_status_info.sync_lsn_ = lsn.val_;
    restore_status_info.sync_scn_ = scn;
    if (OB_FAIL(restore_status_info.get_restore_comment())) {
      CLOG_LOG(WARN, "fail to get restore comment", K(sync_status));
    } else {
      CLOG_LOG(TRACE, "success to get error code and message", K(restore_status_info));
    }
  }
  return ret;
}

int ObLogRestoreHandler::get_restore_sync_status(int ret_code,
    const ObLogRestoreErrorContext::ErrorType error_type,
    RestoreSyncStatus &sync_status)
{
  int ret = OB_SUCCESS;

  // RESTORE_SYNC_RESTORE_HANDLER_HAS_NO_LEADER
  if (OB_NOT_MASTER == ret_code) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_RESTORE_HANDLER_HAS_NO_LEADER;
  }
  // RESTORE_SYNC_SOURCE_HAS_A_GAP
  else if ((OB_ERR_OUT_OF_LOWER_BOUND == ret_code
    || OB_ARCHIVE_ROUND_NOT_CONTINUOUS == ret_code
    || OB_ARCHIVE_LOG_RECYCLED == ret_code)
    && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_SOURCE_HAS_A_GAP;
  }
  // RESTORE_SYNC_SUBMIT_LOG_NOT_MATCH
  else if (OB_ERR_UNEXPECTED == ret_code && ObLogRestoreErrorContext::ErrorType::SUBMIT_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_SUBMIT_LOG_NOT_MATCH;
  }
  // RESTORE_SYNC_FETCH_LOG_NOT_MATCH
  else if ((OB_INVALID_DATA == ret_code || OB_CHECKSUM_ERROR == ret_code)
    && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_FETCH_LOG_NOT_MATCH;
  }
  // RESTORE_SYNC_CHECK_NETWORK
  else if (-ER_CONNECT_FAILED == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_CHECK_NETWORK;
  }
  // RESTORE_SYNC_CHECK_USER_OR_PASSWORD
  else if ((-ER_ACCESS_DENIED_ERROR == ret_code || -ER_TABLEACCESS_DENIED_ERROR == ret_code)
    && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_CHECK_USER_OR_PASSWORD;
  }
  // RESTORE_SYNC_FETCH_LOG_TIME_OUT
  else if (OB_TIMEOUT == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_FETCH_LOG_TIME_OUT;
  }
  // RESTORE_SYNC_STANDBY_NEED_UPGRADE
  else if (OB_ERR_RESTORE_STANDBY_VERSION_LAG == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_STANDBY_NEED_UPGRADE;
  }
  // RESTORE_SYNC_PRIMARY_IS_DROPPED
  else if (OB_ERR_RESTORE_PRIMARY_TENANT_DROPPED == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_PRIMARY_IS_DROPPED;
  }
  // RESTORE_SYNC_WAITING_LS_CREATED
  else if (OB_LS_NOT_EXIST == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_WAITING_LS_CREATED;
  }
  // RESTORE_SYNC_ACCESS_PRIMARY_FAILED
  else if (OB_SIZE_OVERFLOW == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_QUERY_PRIMARY_FAILED;
  }
  // RESTORE_SYNC_NOT_AVAILABLE
  else if (OB_SUCCESS != ret_code) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_NOT_AVAILABLE;
  }
  CLOG_LOG(TRACE, "get error code and message succ", K(sync_status));
  return ret;
}

int ObLogRestoreHandler::get_offline_scn_(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  storage::ObLSHandle handle;
  if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(share::ObLSID(id_), handle, ObLSGetMod::LOG_MOD))) {
    CLOG_LOG(WARN, "get ls failed", K(id_));
  } else if (OB_FAIL(handle.get_ls()->get_offline_scn(scn))) {
    CLOG_LOG(WARN, "get offline_scn failed", K(id_));
  }
  return ret;
}

void ObLogRestoreHandler::deep_copy_source_(ObRemoteSourceGuard &source_guard)
{
  int ret = OB_SUCCESS;
  ObRemoteLogParent *source = NULL;
  if (OB_ISNULL(parent_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "parent_ is NULL", K(ret));
  } else if (OB_ISNULL(source = ObResSrcAlloctor::alloc(parent_->get_source_type(), share::ObLSID(id_)))) {
  } else if (FALSE_IT(parent_->deep_copy_to(*source))) {
  } else if (FALSE_IT(source_guard.set_source(source))) {
  }
}

RestoreStatusInfo::RestoreStatusInfo()
  : ls_id_(share::ObLSID::INVALID_LS_ID),
    sync_lsn_(LOG_INVALID_LSN_VAL),
    sync_status_(RestoreSyncStatus::INVALID_RESTORE_SYNC_STATUS),
    err_code_(OB_SUCCESS)
{
  sync_scn_.reset();
  comment_.reset();
}

int RestoreStatusInfo::set(const share::ObLSID &ls_id,
           const palf::LSN &lsn, const share::SCN &scn, int err_code,
           const RestoreSyncStatus sync_status)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !lsn.is_valid() || !scn.is_valid() || OB_SUCCESS == err_code) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(ls_id), K(lsn), K(scn), K(err_code));
  } else {
    ls_id_ = ls_id.id();
    sync_scn_= scn;
    sync_lsn_ = lsn.val_;
    sync_status_ = sync_status;
    err_code_ = err_code;
    if (OB_FAIL(get_restore_comment())) {
      CLOG_LOG(WARN, "failed to assign comment", KR(ret), K(sync_status));
    }
  }
  return ret;
}

void RestoreStatusInfo::reset()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  sync_lsn_ = LOG_INVALID_LSN_VAL;
  sync_scn_.reset();
  sync_status_ = RestoreSyncStatus::INVALID_RESTORE_SYNC_STATUS;
  err_code_ = OB_SUCCESS;
  comment_.reset();
}

int RestoreStatusInfo::restore_sync_status_to_string(char *str_buf, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_RESTORE_STATUS_STR_LEN = 32;
  if (OB_ISNULL(str_buf)
    || str_len < MAX_RESTORE_STATUS_STR_LEN
    || sync_status_ <= RestoreSyncStatus::INVALID_RESTORE_SYNC_STATUS
    || sync_status_ >= RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid restore status", K(sync_status_));
  } else if (OB_FAIL(databuff_printf(str_buf, str_len, "%s", restore_status_str[int(sync_status_)]))) {
    CLOG_LOG(WARN, "databuff printf restore status str failed", K(sync_status_));
  }
  return ret;
}

int RestoreStatusInfo::get_restore_comment()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(comment_.assign_fmt("%s", restore_comment_str[int(sync_status_)]))) {
    CLOG_LOG(WARN, "fail to assign comment", K_(sync_status));
  } else {
    CLOG_LOG(TRACE, "success to get restore status comment", K_(sync_status), K_(comment));
  }
  return ret;
}

int RestoreStatusInfo::assign(const RestoreStatusInfo &other)
{
  int ret = OB_SUCCESS;
  ls_id_ = other.ls_id_;
  sync_lsn_ = other.sync_lsn_;
  sync_scn_ = other.sync_scn_;
  sync_status_ = other.sync_status_;
  err_code_ = other.err_code_;
  if (OB_FAIL(comment_.assign(other.comment_))) {
    CLOG_LOG(WARN, "fail to assign comment");
  }
  return ret;
}

bool RestoreStatusInfo::is_valid() const
{
  return ls_id_ != share::ObLSID::INVALID_LS_ID
      && sync_lsn_ != LOG_INVALID_LSN_VAL
      && sync_scn_.is_valid()
      && sync_status_ != RestoreSyncStatus::INVALID_RESTORE_SYNC_STATUS;
}
} // namespace logservice
} // namespace oceanbase
