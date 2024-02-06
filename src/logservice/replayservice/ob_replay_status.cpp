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

#include "ob_replay_status.h"
#include "ob_log_replay_service.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/palf/palf_env.h"
#include "lib/stat/ob_session_stat.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;
namespace logservice
{
//---------------ObReplayServiceTask---------------//
ObReplayServiceTask::ObReplayServiceTask()
  : lock_(common::ObLatchIds::REPLAY_STATUS_TASK_LOCK),
    type_(ObReplayServiceTaskType::INVALID_LOG_TASK),
    replay_status_(NULL),
    lease_()
{
  reset();
}

ObReplayServiceTask::~ObReplayServiceTask()
{
  destroy();
}

void ObReplayServiceTask::reset()
{
  //考虑到reuse需求, lease不能重置, 否则可能出现同一个任务被push多次
  enqueue_ts_ = 0;
  err_info_.reset();
}

void ObReplayServiceTask::destroy()
{
  reset();
  type_ = ObReplayServiceTaskType::INVALID_LOG_TASK;
  replay_status_ = NULL;
}

void ObReplayServiceTask::TaskErrInfo::reset()
{
  has_fatal_error_ = false;
  ret_code_ = common::OB_SUCCESS;
  fail_ts_ = 0;
  fail_cost_ = 0;
}

void ObReplayServiceTask::clear_err_info(const int64_t cur_ts)
{
  err_info_.has_fatal_error_ = false;
  err_info_.ret_code_ = common::OB_SUCCESS;
  if (0 != err_info_.fail_ts_) {
    err_info_.fail_cost_ += (cur_ts - err_info_.fail_ts_);
    err_info_.fail_ts_ = 0;
  }
}

void ObReplayServiceTask::set_simple_err_info(const int ret_code,
                                              const int64_t fail_ts)
{
  err_info_.ret_code_ = ret_code;
  if (0 == err_info_.fail_ts_) {
    err_info_.fail_ts_ = fail_ts;
  }
}
void ObReplayServiceTask::set_fatal_err_info(const int ret_code,
                                             const int64_t fail_ts)
{
  err_info_.has_fatal_error_ = true;
  err_info_.ret_code_ = ret_code;
  err_info_.fail_ts_ = fail_ts;
}

bool ObReplayServiceTask::need_replay_immediately() const
{
  return (OB_SUCCESS == err_info_.ret_code_);
}

//---------------ObReplayServiceSubmitTask---------------//
int ObReplayServiceSubmitTask::init(const palf::LSN &base_lsn,
                                    const SCN &base_scn,
                                    PalfHandle *palf_handle,
                                    ObReplayStatus *replay_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status) || OB_ISNULL(palf_handle)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(type_), K(ret), K(replay_status), K(palf_handle));
  } else if (OB_FAIL(palf_handle->seek(base_lsn, iterator_))) {
    CLOG_LOG(WARN, "seek iterator failed", KR(ret), K(type_), K(palf_handle), K(base_lsn));
  } else if (OB_UNLIKELY(!base_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "base_scn is invalid", K(type_), K(base_lsn), K(base_scn), KR(ret));
  } else {
    replay_status_ = replay_status;
    next_to_submit_lsn_ = base_lsn;
    next_to_submit_scn_.set_min();
    base_lsn_ = base_lsn;
    base_scn_ = base_scn;
    type_ = ObReplayServiceTaskType::SUBMIT_LOG_TASK;
    if (OB_SUCCESS != (tmp_ret = iterator_.next())) {
      // 在没有写入的情况下有可能已经到达边界
      CLOG_LOG(WARN, "iterator next failed", K(iterator_), K(tmp_ret));
    }
    CLOG_LOG(INFO, "submit log task init success", K(type_), K(next_to_submit_lsn_),
               K(next_to_submit_scn_), K(replay_status_), K(ret));
  }
  return ret;
}

void ObReplayServiceSubmitTask::reset()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  next_to_submit_lsn_.reset();
  next_to_submit_scn_.reset();
  base_lsn_.reset();
  base_scn_.reset();
  ObReplayServiceTask::reset();
}

void ObReplayServiceSubmitTask::destroy()
{
  reset();
  //iterator不支持reset语义,不能在后续可能重用的接口中调用destroy接口
  iterator_.destroy();
  ObReplayServiceTask::destroy();
}

int ObReplayServiceSubmitTask::get_next_to_submit_log_info(LSN &lsn, SCN &scn) const
{
  ObLockGuard<ObSpinLock> guard(lock_);
  return get_next_to_submit_log_info_(lsn, scn);
}

int ObReplayServiceSubmitTask::get_next_to_submit_log_info_(LSN &lsn, SCN &scn) const
{
  int ret = OB_SUCCESS;
  lsn.val_ = ATOMIC_LOAD(&next_to_submit_lsn_.val_);
  scn = next_to_submit_scn_.atomic_load();
  return ret;
}

int ObReplayServiceSubmitTask::get_base_lsn(LSN &lsn) const
{
  ObLockGuard<ObSpinLock> guard(lock_);
  return get_base_lsn_(lsn);
}

int ObReplayServiceSubmitTask::get_base_lsn_(LSN &lsn) const
{
  int ret = OB_SUCCESS;
  lsn = base_lsn_;
  return ret;
}

int ObReplayServiceSubmitTask::get_base_scn(SCN &scn) const
{
  ObLockGuard<ObSpinLock> guard(lock_);
  return get_base_scn_(scn);
}

int ObReplayServiceSubmitTask::get_base_scn_(SCN &scn) const
{
  int ret = OB_SUCCESS;
  scn = base_scn_;
  return ret;
}

bool ObReplayServiceSubmitTask::has_remained_submit_log(const SCN &replayable_point,
                                                        bool &iterate_end_by_replayable_point)
{
  // next接口只在submit任务中单线程调用,和reset接口通过replay status的大锁互斥
  // 故此处不需要submit task锁保护
  if (false == iterator_.is_valid()) {
    // maybe new logs is written after last check
    next_log(replayable_point, iterate_end_by_replayable_point);
  }
  return iterator_.is_valid();
}

//只有padding日志的场景才能调用此接口
int ObReplayServiceSubmitTask::update_next_to_submit_lsn(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_next_to_submit_lsn_(lsn))) {
    CLOG_LOG(WARN, "failed to update_next_to_submit_lsn", KR(ret), K(lsn), K(next_to_submit_lsn_));
  } else {
    //do nothing
  }
  return ret;
}

int ObReplayServiceSubmitTask::update_submit_log_meta_info(const LSN &lsn,
                                                           const SCN &scn)
{
  ObLockGuard<ObSpinLock> guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_next_to_submit_lsn_(lsn))
      || OB_FAIL(update_next_to_submit_scn_(SCN::scn_inc(scn)))) {
    CLOG_LOG(ERROR, "failed to update_submit_log_meta_info", KR(ret), K(lsn), K(scn),
             K(next_to_submit_lsn_), K(next_to_submit_scn_));
  } else {
    CLOG_LOG(TRACE, "update_submit_log_meta_info", KR(ret), K(lsn), K(scn),
             K(next_to_submit_lsn_), K(next_to_submit_scn_), K(iterator_));
  }
  return ret;
}

int ObReplayServiceSubmitTask::need_skip(const SCN &scn, bool &need_skip)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!base_scn_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    need_skip = scn < base_scn_;
  }
  return ret;
}


int ObReplayServiceSubmitTask::get_log(const char *&buffer, int64_t &nbytes, SCN &scn,
                                       palf::LSN &offset, bool &is_raw_write)
{
  return iterator_.get_entry(buffer, nbytes, scn, offset, is_raw_write);
}

int ObReplayServiceSubmitTask::next_log(const SCN &replayable_point,
                                        bool &iterate_end_by_replayable_point)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SCN next_min_scn;
  if (OB_SUCCESS != (tmp_ret = iterator_.next(replayable_point, next_min_scn,
                                              iterate_end_by_replayable_point))) {
    if (OB_ITER_END == tmp_ret) {
      if (next_min_scn == next_to_submit_scn_
          || !replayable_point.is_valid()) {
        // do nothing
      } else if (!next_min_scn.is_valid()) {
        // should only occurs when palf has no log
        CLOG_LOG(INFO, "next_min_scn is invalid", K(type_), K(replayable_point),
                 K(next_min_scn), K(next_to_submit_scn_), K(ret), K(iterator_));
      } else if (OB_UNLIKELY(next_min_scn < next_to_submit_scn_)) {
        ret = OB_ERR_UNEXPECTED;
        LSN unused_lsn;
        // updating next to submit log info is failed, set fatal error for replay status.
        replay_status_->set_err_info(unused_lsn, next_min_scn, ObLogBaseType::INVALID_LOG_BASE_TYPE,
                                     0, true, ObClockGenerator::getClock(), ret);
        CLOG_LOG(ERROR, "failed to update next_to_submit_scn_", K(type_), K(replayable_point),
                 K(next_min_scn), K(next_to_submit_scn_), K(ret), K(iterator_));
      } else {
        next_to_submit_scn_ = next_min_scn;
        CLOG_LOG(INFO, "update next_to_submit_scn_", K(type_), K(replayable_point),
                 K(next_min_scn), K(next_to_submit_scn_), K(ret), K(iterator_));
      }
    } else {
      // ignore other err ret of iterator
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObReplayServiceSubmitTask::reset_iterator(PalfHandle &palf_handle, const LSN &begin_lsn)
{
  int ret = OB_SUCCESS;
  next_to_submit_lsn_ = std::max(next_to_submit_lsn_, begin_lsn);
  if (OB_FAIL(palf_handle.seek(next_to_submit_lsn_, iterator_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "seek interator failed", K(type_), K(begin_lsn), K(ret));
  } else if (OB_FAIL(iterator_.next())) {
    CLOG_LOG(WARN, "iterator next failed", K(type_), K(begin_lsn), K(ret));
  }
  return ret;
}

int ObReplayServiceSubmitTask::update_next_to_submit_scn_(const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(scn <= next_to_submit_scn_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "update_next_to_submit_scn invalid argument", K(type_), K(scn),
               K(next_to_submit_scn_), K(ret));
  } else {
    next_to_submit_scn_ = scn;
  }
  return ret;
}

int ObReplayServiceSubmitTask::update_next_to_submit_lsn_(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(lsn <= next_to_submit_lsn_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "update_next_to_submit_lsn invalid argument", K(type_), K(lsn),
               K(next_to_submit_lsn_), K(ret));
  } else {
    next_to_submit_lsn_ = lsn;
  }
  return ret;
}

//---------------ObReplayServiceReplayTask---------------//
int ObReplayServiceReplayTask::init(ObReplayStatus *replay_status,
                                    const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status) || idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(type_), KP(replay_status), K(ret), K(idx));
  } else {
    replay_status_ = replay_status;
    idx_ = idx;
    type_ = ObReplayServiceTaskType::REPLAY_LOG_TASK;
    CLOG_LOG(INFO, "ObReplayServiceReplayTask init success", K(type_), K(replay_status_), K(idx_));
  }
  return ret;
}

void ObReplayServiceReplayTask::reset()
{
  //attention: type_ and replay_status_ can not be reset
  ObLink *top_item = NULL;
  if (NULL != replay_status_) {
    ObLockGuard<ObSpinLock> guard(lock_);
    while (NULL != (top_item = pop_()))
    {
      ObLogReplayTask *replay_task = static_cast<ObLogReplayTask *>(top_item);
      //此处一定只能让引用计数归零的任务释放log_buff
      if (replay_task->is_pre_barrier_) {
        ObLogReplayBuffer *replay_buf = static_cast<ObLogReplayBuffer *>(replay_task->log_buf_);
        if (NULL == replay_buf) {
          CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "replay_buf is NULL when reset", KPC(replay_task));
        } else if (0 == replay_buf->dec_replay_ref()) {
          replay_status_->free_replay_task_log_buf(replay_task);
          replay_status_->dec_pending_task(replay_task->log_size_);
        }
      } else {
        replay_status_->dec_pending_task(replay_task->log_size_);
      }
      replay_status_->free_replay_task(replay_task);
    };
  }
  idx_ = -1;
  ObReplayServiceTask::reset();
}

void ObReplayServiceReplayTask::destroy()
{
  reset();
  ObReplayServiceTask::destroy();
}

int64_t ObReplayServiceReplayTask::idx() const
{
  return idx_;
}

int ObReplayServiceReplayTask::get_min_unreplayed_log_info(LSN &lsn,
                                                           SCN &scn,
                                                           int64_t &replay_hint,
                                                           ObLogBaseType &log_type,
                                                           int64_t &first_handle_ts,
                                                           int64_t &replay_cost,
                                                           int64_t &retry_cost,
                                                           bool &is_queue_empty)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  ObLogReplayTask *replay_task = NULL;
  ObLink *top_item = top();
  if (NULL != top_item && NULL != (replay_task = static_cast<ObLogReplayTask *>(top_item))) {
    lsn = replay_task->lsn_;
    scn = replay_task->scn_;
    replay_hint = replay_task->replay_hint_;
    log_type = replay_task->log_type_;
    first_handle_ts = replay_task->first_handle_ts_;
    replay_cost = replay_task->replay_cost_;
    retry_cost = replay_task->retry_cost_;
    is_queue_empty = false;
  } else {
    is_queue_empty = true;
  }
  return ret;
}

ObLink *ObReplayServiceReplayTask::pop()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  return pop_();
}

void ObReplayServiceReplayTask::push(Link *p)
{
  need_batch_push_ = true;
  queue_.push(p);
}

bool ObReplayServiceReplayTask::need_batch_push()
{
  return need_batch_push_;
}

void ObReplayServiceReplayTask::set_batch_push_finish()
{
  need_batch_push_ = false;
}

//---------------ObLogReplayBuffer---------------//
void ObLogReplayBuffer::reset()
{
  ref_ = 0;
  log_buf_ = NULL;
}

int64_t ObLogReplayBuffer::dec_replay_ref()
{
  return ATOMIC_SAF(&ref_, 1);
}

void ObLogReplayBuffer::inc_replay_ref()
{
  ATOMIC_INC(&ref_);
}

int64_t ObLogReplayBuffer::get_replay_ref()
{
  return ATOMIC_LOAD(&ref_);
}

//---------------ObLogReplayTask---------------//
int ObLogReplayTask::init(void *log_buf)
{
  int ret = OB_SUCCESS;
  log_buf_ = log_buf;
  if (is_pre_barrier_) {
    ObLogReplayBuffer *replay_log_buffer = static_cast<ObLogReplayBuffer *>(log_buf);
    replay_log_buffer->ref_ = REPLAY_TASK_QUEUE_SIZE;
  }
  init_task_ts_ = ObTimeUtility::fast_current_time();
  CLOG_LOG(TRACE, "ObLogReplayTask init success", KPC(this));
  return ret;
}

void ObLogReplayTask::reset()
{
  ls_id_.reset();
  scn_.reset();
  lsn_.reset();
  log_size_ = 0;
  is_pre_barrier_ = false;
  is_post_barrier_ = false;
  replay_hint_ = 0;
  log_type_ = ObLogBaseType::INVALID_LOG_BASE_TYPE;
  is_raw_write_ = false;
  init_task_ts_ = common::OB_INVALID_TIMESTAMP;
  first_handle_ts_ = common::OB_INVALID_TIMESTAMP;
  print_error_ts_ = common::OB_INVALID_TIMESTAMP;
  replay_cost_ = common::OB_INVALID_TIMESTAMP;
  retry_cost_ = common::OB_INVALID_TIMESTAMP;
  log_buf_ = NULL;
}

bool ObLogReplayTask::is_valid()
{
  bool ret = false;
  ret = ls_id_.is_valid()
        && scn_.is_valid()
        && lsn_.is_valid()
        && log_size_ > 0
        && NULL != log_buf_;
  return ret;
}

void ObLogReplayTask::shallow_copy(const ObLogReplayTask &other)
{
  ls_id_ = other.ls_id_;
  scn_ = other.scn_;
  lsn_ = other.lsn_;
  log_size_ = other.log_size_;
  is_pre_barrier_ = other.is_pre_barrier_;
  is_post_barrier_ = other.is_post_barrier_;
  replay_hint_ = other.replay_hint_;
  log_type_ = other.log_type_;
  is_raw_write_ = other.is_raw_write_;
  log_buf_ = other.log_buf_;
  init_task_ts_ = other.init_task_ts_;
  first_handle_ts_ = other.init_task_ts_;
}

int64_t ObLogReplayTask::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  char log_base_type_str[logservice::OB_LOG_BASE_TYPE_STR_MAX_LEN] = {'\0'};
  (void) log_base_type_to_string(log_type_, log_base_type_str, logservice::OB_LOG_BASE_TYPE_STR_MAX_LEN);
  J_OBJ_START();
  J_KV(K(ls_id_),
         K_(log_type),
         "log_type", log_base_type_str,
         K(lsn_),
         K(scn_),
         K(is_pre_barrier_),
         K(is_post_barrier_),
         K(log_size_),
         K(replay_hint_),
         K(is_raw_write_),
         K(first_handle_ts_),
         K(replay_cost_),
         K(retry_cost_),
         KP(log_buf_));
  J_OBJ_END();
  return pos;
}

//---------------ObReplayFsCb---------------//
int ObReplayFsCb::update_end_lsn(int64_t id,
                                 const LSN &end_offset,
                                 const int64_t proposal_id)
{
  UNUSED(id);
  UNUSED(proposal_id);
  return replay_status_->update_end_offset(end_offset);
}

//---------------ObReplayStatus---------------//
ObReplayStatus::ObReplayStatus():
    is_inited_(false),
    is_enabled_(false),
    is_submit_blocked_(true),
    role_(FOLLOWER),
    ls_id_(),
    ref_cnt_(0),
    post_barrier_lsn_(),
    err_info_(),
    pending_task_count_(0),
    last_check_memstore_lsn_(),
    rwlock_(common::ObLatchIds::REPLAY_STATUS_LOCK),
    rolelock_(common::ObLatchIds::REPLAY_STATUS_LOCK),
    rp_sv_(NULL),
    submit_log_task_(),
    palf_env_(NULL),
    palf_handle_(),
    fs_cb_(),
    get_log_info_debug_time_(OB_INVALID_TIMESTAMP),
    try_wrlock_debug_time_(OB_INVALID_TIMESTAMP),
    check_enable_debug_time_(OB_INVALID_TIMESTAMP)
{
}

ObReplayStatus::~ObReplayStatus()
{
  destroy();
}

int ObReplayStatus::init(const share::ObLSID &id,
                         PalfEnv *palf_env,
                         ObLogReplayService *rp_sv)
{
  //TODO: use replica type init need_replay
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "replay status has already been inited", K(ret));
  } else if (!id.is_valid() || OB_ISNULL(palf_env) || OB_ISNULL(rp_sv)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id), K(rp_sv), KP(palf_env), K(ret));
  } else if (OB_FAIL(palf_env->open(id.id(), palf_handle_))) {
    CLOG_LOG(ERROR, "failed to open palf handle", K(palf_env), K(id));
  } else {
    ls_id_ = id;
    get_log_info_debug_time_ = OB_INVALID_TIMESTAMP;
    try_wrlock_debug_time_ = OB_INVALID_TIMESTAMP;
    check_enable_debug_time_ = OB_INVALID_TIMESTAMP;
    palf_env_ = palf_env;
    rp_sv_ = rp_sv;
    IGNORE_RETURN new (&fs_cb_) ObReplayFsCb(this);
    is_inited_ = true;
    if (OB_FAIL(palf_handle_.register_file_size_cb(&fs_cb_))) {
      CLOG_LOG(ERROR, "failed to register cb", K(ret));
    } else {
      CLOG_LOG(INFO, "replay status init success", K(ret), KPC(this));
    }
  }
  if (OB_FAIL(ret) && (OB_INIT_TWICE != ret))
  {
    destroy();
  }
  return ret;
}

void ObReplayStatus::destroy()
{
  int ret = OB_SUCCESS;
  //注意: 虽然replay status的引用计数已归0, 但此时fs_cb_依然可能访问replay status，需要先调用unregister_file_size_cb
  if (OB_FAIL(palf_handle_.unregister_file_size_cb())) {
    CLOG_LOG(ERROR, "failed to unregister cb", K(ret));
  }
  WLockGuard wlock_guard(rwlock_);
  CLOG_LOG(INFO, "destuct replay status", KPC(this));
  // 析构前必须是disable状态
  if (is_enabled_) {
    CLOG_LOG(ERROR, "is_enable when destucting", K(this));
  } else {
    is_inited_ = false;
    if (palf_handle_.is_valid()) {
      palf_env_->close(palf_handle_);
    }
    submit_log_task_.destroy();
    for (int64_t i = 0; i < REPLAY_TASK_QUEUE_SIZE; ++i) {
      task_queues_[i].destroy();
    }
    is_submit_blocked_ = true;
    role_ = FOLLOWER;
    ls_id_.reset();
    post_barrier_lsn_.reset();
    err_info_.reset();
    last_check_memstore_lsn_.reset();
    pending_task_count_ = 0;
    fs_cb_.destroy();
    get_log_info_debug_time_ = OB_INVALID_TIMESTAMP;
    try_wrlock_debug_time_ = OB_INVALID_TIMESTAMP;
    check_enable_debug_time_ = OB_INVALID_TIMESTAMP;
    palf_env_ = NULL;
    rp_sv_ = NULL;
  }
}

//不可重入,如果失败需要外部手动disable
int ObReplayStatus::enable(const LSN &base_lsn, const SCN &base_scn)
{
  int ret = OB_SUCCESS;
  if (is_enabled()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "replay status already enable", K(ret));
  } else {
    WLockGuard wlock_guard(rwlock_);
    if (OB_FAIL(enable_(base_lsn, base_scn))) {
      CLOG_LOG(WARN, "enable replay status failed", K(ret), K(base_lsn), K(base_scn), K(ls_id_));
    } else {
      CLOG_LOG(INFO, "enable replay status success", K(ret), K(base_lsn), K(base_scn), K(ls_id_));
    }
  }
  return ret;
}

// 提交当前的submit_log_task并注册回调
int ObReplayStatus::enable_(const LSN &base_lsn, const SCN &base_scn)
{
  int ret = OB_SUCCESS;
  // 处理submit_task需要先设置enable状态
  is_enabled_ = true;
  is_submit_blocked_ = false;
  if (0 != pending_task_count_) {
    //针对reuse场景的防御检查
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "remain pending task when enable replay status", K(ret), KPC(this));
  } else if (OB_FAIL(submit_log_task_.init(base_lsn, base_scn, &palf_handle_, this))) {
    CLOG_LOG(WARN, "failed to init submit_log_task", K(ret), K(&palf_handle_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < REPLAY_TASK_QUEUE_SIZE; ++i) {
      if (OB_FAIL(task_queues_[i].init(this, i))) {
        CLOG_LOG(WARN, "failed to init task_queue", K(ret));
      }
    }
    if (OB_SUCCESS == ret) {
      set_last_check_memstore_lsn(base_lsn);
      if (OB_FAIL(submit_task_to_replay_service_(submit_log_task_))) {
        CLOG_LOG(ERROR, "failed to submit submit_log_task to replay service", K(submit_log_task_),
                    KPC(this), K(ret));
      }
    }
  }
  if (OB_SUCCESS != ret) {
    disable_();
    is_submit_blocked_ = true;
  }
  return ret;
}

//可以重入
int ObReplayStatus::disable()
{
  int ret = OB_SUCCESS;
  if (!is_enabled()) {
    CLOG_LOG(INFO, "replay status already disable", K(ls_id_));
  } else {
    do {
      WLockGuard guard(rolelock_);
      is_submit_blocked_ = true;
    } while (0);
    int64_t abs_timeout_us = WRLOCK_TRY_THRESHOLD + ObTimeUtility::current_time();
    if (OB_SUCC(rwlock_.wrlock(abs_timeout_us))) {
      if (OB_FAIL(disable_())) {
        CLOG_LOG(WARN, "disable replay status failed", K(ls_id_));
      } else {
        CLOG_LOG(INFO, "disable replay status success", K(ls_id_));
      }
      rwlock_.unlock();
    } else {
      ret = OB_EAGAIN;
      if (palf_reach_time_interval(1000 * 1000, try_wrlock_debug_time_)) {
        CLOG_LOG(INFO, "try lock failed in disable", KPC(this), K(ret));
      }
    }
  }
  return ret;
}

int ObReplayStatus::disable_()
{
  int ret = OB_SUCCESS;
  is_enabled_ = false;
  submit_log_task_.reset();
  for (int64_t i = 0; i < REPLAY_TASK_QUEUE_SIZE; ++i) {
    task_queues_[i].reset();
  }
  err_info_.reset();
  last_check_memstore_lsn_.reset();
  get_log_info_debug_time_ = OB_INVALID_TIMESTAMP;
  ATOMIC_STORE(&post_barrier_lsn_.val_, LOG_INVALID_LSN_VAL);
  return ret;
}

bool ObReplayStatus::is_enabled() const
{
  RLockGuard rlock_guard(rwlock_);
  return is_replay_enabled_();
}

bool ObReplayStatus::is_enabled_without_lock() const
{
  return is_replay_enabled_();
}

void ObReplayStatus::block_submit()
{
  WLockGuard guard(rolelock_);
  is_submit_blocked_ = true;
  CLOG_LOG(INFO, "replay status block submit", KPC(this));
}

void ObReplayStatus::unblock_submit()
{
  int ret = OB_SUCCESS;
  do {
    WLockGuard guard(rolelock_);
    is_submit_blocked_ = false;
    CLOG_LOG(INFO, "replay status unblock submit", KPC(this));
  } while (0);

  RLockGuard rlock_guard(rwlock_);
  if (!is_enabled_) {
    // do nothing
  } else if (OB_FAIL(submit_task_to_replay_service_(submit_log_task_))) {
    CLOG_LOG(ERROR, "failed to submit submit_log_task to replay service", K(submit_log_task_),
             KPC(this), K(ret));
  }
}

bool ObReplayStatus::is_replay_enabled_() const
{
  return is_enabled_;
}

bool ObReplayStatus::need_submit_log() const
{
  RLockGuard guard(rolelock_);
  return (FOLLOWER == role_ && !is_submit_blocked_);
}

void ObReplayStatus::switch_to_leader()
{
  WLockGuard guard(rolelock_);
  role_ = LEADER;
  CLOG_LOG(INFO, "replay status switch_to_leader", KPC(this));
}

void ObReplayStatus::switch_to_follower(const palf::LSN &begin_lsn)
{
  int ret = OB_SUCCESS;
  // 1.switch role after reset iterator, or fscb may push submit task with
  //   old iterator, which will fetch logs smaller than max decided scn.
  // 2.submit task after switch role, or this task may be discarded if role
  //   still be leader, and no more submit task being submitted.
  do {
    WLockGuardWithRetryInterval wguard(rwlock_, WRLOCK_TRY_THRESHOLD, WRLOCK_RETRY_INTERVAL);
    if (!is_enabled_) {
      // do nothing
    } else {
      (void)submit_log_task_.reset_iterator(palf_handle_, begin_lsn);
    }
  } while (0);
  do {
    WLockGuard role_guard(rolelock_);
    role_ = FOLLOWER;
  } while (0);

  RLockGuard rguard(rwlock_);
  if (!is_enabled_) {
    // do nothing
  } else if (OB_FAIL(submit_task_to_replay_service_(submit_log_task_))) {
    CLOG_LOG(ERROR, "failed to submit submit_log_task to replay service", K(submit_log_task_),
                KPC(this), K(ret));
  } else {
    // success
  }
  CLOG_LOG(INFO, "replay status switch_to_follower", KPC(this), K(begin_lsn));
}

bool ObReplayStatus::has_remained_replay_task() const
{
  int64_t count = 0;
  bool bool_ret = (0 != pending_task_count_);

  if (pending_task_count_ > PENDING_COUNT_THRESHOLD && REACH_TIME_INTERVAL(1000 * 1000)) {
    CLOG_LOG_RET(WARN, OB_ERR_UNEXPECTED, "too many pending replay task", K(count), KPC(this));
  }
  return bool_ret;
}

int ObReplayStatus::is_replay_done(const LSN &end_lsn,
                                   bool &is_done)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "replay status has not been inited", K(ret));
  } else {
    RLockGuard rlock_guard(rwlock_);
    LSN min_unreplayed_lsn;
    if (!is_enabled_) {
      is_done = false;
      CLOG_LOG(INFO, "repaly is not enabled", K(end_lsn));
    } else if (OB_FAIL(get_min_unreplayed_lsn(min_unreplayed_lsn))) {
      CLOG_LOG(ERROR, "get_min_unreplayed_lsn failed", K(this), K(ret), K(min_unreplayed_lsn));
    } else if (!min_unreplayed_lsn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "min_unreplayed_lsn invalid", K(this), K(ret), K(end_lsn));
    } else {
      is_done = min_unreplayed_lsn >= end_lsn;
      //TODO: @keqing.llt 限流改为类内
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        if (is_done) {
          CLOG_LOG(INFO, "log stream finished replay", K(ls_id_), K(min_unreplayed_lsn), K(end_lsn));
        } else {
          CLOG_LOG(INFO, "log stream has not finished replay", K(ls_id_), K(min_unreplayed_lsn), K(end_lsn));
        }
      }
    }
  }
  return ret;
}

int ObReplayStatus::update_end_offset(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  //check when log slide out
  RLockGuard rlock_guard(rwlock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "replay status is not init", K(ls_id_), K(lsn), K(ret));
  } else if (!is_enabled_) {
    if (palf_reach_time_interval(100 * 1000, check_enable_debug_time_)) {
      CLOG_LOG(INFO, "replay status is not enabled", K(this), K(ret), K(lsn));
    }
  } else if (OB_UNLIKELY(!lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ls_id_), K(lsn), K(ret));
  } else if (!need_submit_log()) {
    // leader do nothing, keep submit_log_task recording last round status as follower
  } else if (OB_FAIL(submit_task_to_replay_service_(submit_log_task_))) {
    CLOG_LOG(ERROR, "failed to submit submit_log_task to replay Service", K(submit_log_task_),
             KPC(this), K(ret));
  }
  return ret;
}

int ObReplayStatus::get_ls_id(share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  if (!ls_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "ls_id_ is invalid", K(ret), K(ls_id_));
  } else {
    id = ls_id_;
  }
  return ret;
}

int ObReplayStatus::get_min_unreplayed_lsn(LSN &lsn)
{
  SCN unused_scn;
  int64_t unused_replay_hint = 0;
  ObLogBaseType unused_log_type = ObLogBaseType::INVALID_LOG_BASE_TYPE;
  int64_t unused_first_handle_ts = 0;
  int64_t unused_replay_cost = 0;
  int64_t unused_retry_cost = 0;
  return get_min_unreplayed_log_info(lsn, unused_scn, unused_replay_hint, unused_log_type,
                                     unused_first_handle_ts, unused_replay_cost, unused_retry_cost);
}

int ObReplayStatus::get_max_replayed_scn(SCN &scn)
{
  int ret = OB_SUCCESS;
  LSN unused_lsn;
  SCN min_unreplayed_scn;
  int64_t unused_replay_hint = 0;
  int64_t unused_first_handle_ts = 0;
  ObLogBaseType unused_log_type = ObLogBaseType::INVALID_LOG_BASE_TYPE;
  int64_t unused_replay_cost = 0;
  int64_t unused_retry_cost = 0;
  if (OB_FAIL(get_min_unreplayed_log_info(unused_lsn, min_unreplayed_scn, unused_replay_hint, unused_log_type,
                                          unused_first_handle_ts, unused_replay_cost, unused_retry_cost))) {
    CLOG_LOG(WARN, "get_min_unreplayed_log_info failed", K(ret), KPC(this));
  } else {
    scn = min_unreplayed_scn > SCN::base_scn() ? SCN::scn_dec(min_unreplayed_scn) : SCN::min_scn();
  }
  return ret;
}

int ObReplayStatus::get_min_unreplayed_log_info(LSN &lsn,
                                                SCN &scn,
                                                int64_t &replay_hint,
                                                ObLogBaseType &log_type,
                                                int64_t &first_handle_ts,
                                                int64_t &replay_cost,
                                                int64_t &retry_cost)
{
  int ret = OB_SUCCESS;
  SCN base_scn = SCN::min_scn();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay status is not inited", K(ret));
  } else if (!is_enabled_) {
    ret = OB_STATE_NOT_MATCH;
    if (palf_reach_time_interval(1 * 1000 * 1000, get_log_info_debug_time_)) {
      CLOG_LOG(WARN, "replay status is not enabled", K(ret), KPC(this));
    }
  } else if (OB_FAIL(submit_log_task_.get_next_to_submit_log_info(lsn, scn))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get_next_to_submit_scn failed", K(ret));
  } else if (OB_FAIL(submit_log_task_.get_base_scn(base_scn))) {
    CLOG_LOG(ERROR, "get_base_scn failed", K(ret));
  } else if (scn <= base_scn) {
    //拉到的日志尚未超过过滤点
    scn = base_scn;
    if (palf_reach_time_interval(5 * 1000 * 1000, get_log_info_debug_time_)) {
      CLOG_LOG(INFO, "get_min_unreplayed_log_info in skip state", K(lsn), K(scn), KPC(this));
    }
  } else {
    LSN queue_lsn;
    SCN queue_scn;
    bool is_queue_empty = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < REPLAY_TASK_QUEUE_SIZE; ++i) {
      if (OB_FAIL(task_queues_[i].get_min_unreplayed_log_info(queue_lsn, queue_scn, replay_hint, log_type,
                                                first_handle_ts, replay_cost, retry_cost, is_queue_empty))) {
        CLOG_LOG(ERROR, "task_queue get_min_unreplayed_log_info failed", K(ret), K(task_queues_[i]));
      } else if (!is_queue_empty
                && queue_lsn < lsn
                && queue_scn < scn) {
        lsn = queue_lsn;
        scn = queue_scn;
      }
    }
    if (palf_reach_time_interval(5 * 1000 * 1000, get_log_info_debug_time_)) {
      CLOG_LOG(INFO, "get_min_unreplayed_log_info", K(lsn), K(scn), KPC(this));
    }
  }
  if (OB_SUCC(ret) && !is_enabled_) {
    //double check
    ret = OB_STATE_NOT_MATCH;
    if (palf_reach_time_interval(1 * 1000 * 1000, get_log_info_debug_time_)) {
      CLOG_LOG(WARN, "replay status is not enabled", K(ret), KPC(this));
    }
  }
  if (palf_reach_time_interval(5 * 1000 * 1000, get_log_info_debug_time_)) {
    CLOG_LOG(INFO, "get_min_unreplayed_log_info", K(lsn), K(scn), KPC(this), K(ret));
  }
  return ret;
}

int ObReplayStatus::get_replay_process(int64_t &replayed_log_size,
                                       int64_t &unreplayed_log_size)
{
  int ret = OB_SUCCESS;
  LSN base_lsn;
  LSN min_unreplayed_lsn;
  LSN committed_end_lsn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "replay status is not inited", K(ret));
  } else if (!is_enabled_) {
    replayed_log_size = 0;
    unreplayed_log_size = 0;
    CLOG_LOG(INFO, "replay status is not enabled", KPC(this));
  } else if (OB_FAIL(submit_log_task_.get_base_lsn(base_lsn))) {
    CLOG_LOG(WARN, "get_base_lsn failed", K(ret), KPC(this));
  } else if (OB_FAIL(get_min_unreplayed_lsn(min_unreplayed_lsn))) {
    CLOG_LOG(WARN, "get_min_unreplayed_lsn failed", K(ret), KPC(this));
  } else if (!need_submit_log()) {
    replayed_log_size = min_unreplayed_lsn.val_ - base_lsn.val_;
    unreplayed_log_size = 0;
    CLOG_LOG(INFO, "replay status is not follower", K(min_unreplayed_lsn), K(base_lsn), KPC(this));
  } else if (OB_FAIL(palf_handle_.get_end_lsn(committed_end_lsn))) {
    CLOG_LOG(WARN, "get_end_lsn failed", K(ret), KPC(this));
  } else {
    replayed_log_size = min_unreplayed_lsn.val_ - base_lsn.val_;
    unreplayed_log_size = committed_end_lsn.val_ - min_unreplayed_lsn.val_;
    if (replayed_log_size < 0 || unreplayed_log_size < 0) {
      CLOG_LOG(WARN, "get_replay_process failed", K(committed_end_lsn), K(min_unreplayed_lsn), K(base_lsn), KPC(this));
    }
  }
  return ret;
}

int ObReplayStatus::push_log_replay_task(ObLogReplayTask &task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "replay service is NULL", K(task), K(ret));
  } else if (task.is_pre_barrier_) {
    //广播到所有队列, 分配多份内存时如果失败需要全部释放
    const int64_t task_size = sizeof(ObLogReplayTask);
    common::ObSEArray<ObLogReplayTask*, REPLAY_TASK_QUEUE_SIZE> broadcast_task_array;
    //入参任务本身占用一个槽位
    broadcast_task_array.push_back(&task);
    for (int64_t i = 1; OB_SUCC(ret) && i < REPLAY_TASK_QUEUE_SIZE; ++i) {
      void *task_buf = NULL;
      if (OB_UNLIKELY(NULL == (task_buf = rp_sv_->alloc_replay_task(task_size)))) {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          CLOG_LOG(WARN, "failed to alloc replay task buf when broadcast pre barrier", K(i), K(ret));
        }
      } else {
        ObLogReplayTask *replay_task = new (task_buf) ObLogReplayTask();
        replay_task->shallow_copy(task);
        if (OB_FAIL(broadcast_task_array.push_back(replay_task))) {
          free_replay_task(replay_task);
          CLOG_LOG(ERROR, "broadcast_task_array push back replay_task failed", K(task), K(i), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int index = 0;
      ObLogBaseType log_type = task.log_type_;
      share::ObLSID ls_id = task.ls_id_;
      palf::LSN lsn = task.lsn_;
      share::SCN scn = task.scn_;
      bool is_pre_barrier = task.is_pre_barrier_;
      bool is_post_barrier = task.is_post_barrier_;
      int64_t log_size = task.log_size_;
      for (index = 0; OB_SUCC(ret) && index < REPLAY_TASK_QUEUE_SIZE; ++index) {
        ObLogReplayTask *replay_task = broadcast_task_array[index];
        task_queues_[index].push(replay_task);
        //失败后整体重试会导致此任务引用计数错乱, 必须原地重试
        int retry_count = 0;
        while (OB_FAIL(submit_task_to_replay_service_(task_queues_[index]))) {
          //print interval 100ms
          if (0 == retry_count) {
            CLOG_LOG(ERROR, "failed to push replay task queue to replay service", KPC(replay_task),
                     K(ret), KPC(this), K(index));
          }
          retry_count = (retry_count + 1) % 1000;
          ob_usleep(100);
        }
        task_queues_[index].set_batch_push_finish();
      }
      CLOG_LOG(INFO, "submit pre barrier log success", K(log_type), K(ls_id), K(lsn), K(scn),
               K(is_pre_barrier), K(is_post_barrier), K(log_size));
    } else {
      for (int64_t i = 1; i < broadcast_task_array.count(); ++i) {
        free_replay_task(broadcast_task_array[i]);
      }
    }
  } else {
    const uint64_t queue_idx = calc_replay_queue_idx(task.replay_hint_);
    ObReplayServiceReplayTask &task_queue = task_queues_[queue_idx];
    task_queue.push(&task);
  }
  return ret;
}

//此接口不会失败
int ObReplayStatus::batch_push_all_task_queue()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < REPLAY_TASK_QUEUE_SIZE; ++i) {
    ObReplayServiceReplayTask &task_queue = task_queues_[i];
    if (!task_queue.need_batch_push()) {
      // do nothing
    } else if (OB_FAIL(submit_task_to_replay_service_(task_queue))) {
      CLOG_LOG(ERROR, "failed to push replay task queue to replay service", K(task_queue),
               K(ret), KPC(this));
    } else {
      task_queue.set_batch_push_finish();
      CLOG_LOG(TRACE, "push replay task queue to replay service", K(task_queue),
               K(ret), KPC(this));
    }
  }
  return ret;
}

void ObReplayStatus::inc_pending_task(const int64_t log_size)
{
  if (log_size < 0) {
    CLOG_LOG_RET(ERROR, OB_INVALID_ERROR, "task is invalid", K(log_size), KPC(this));
  } else if (OB_ISNULL(rp_sv_)) {
    CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "rp sv is NULL", K(log_size), KPC(this));
  } else {
    ATOMIC_INC(&pending_task_count_);
    rp_sv_->inc_pending_task_size(log_size);
  }
}

void ObReplayStatus::dec_pending_task(const int64_t log_size)
{
  if (log_size < 0) {
    CLOG_LOG_RET(ERROR, OB_INVALID_ERROR, "task is invalid", K(log_size), KPC(this));
  } else if (OB_ISNULL(rp_sv_)) {
    CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "rp sv is NULL", K(log_size), KPC(this));
  } else {
    ATOMIC_DEC(&pending_task_count_);
    rp_sv_->dec_pending_task_size(log_size);
  }
}

void ObReplayStatus::free_replay_task(ObLogReplayTask *task)
{
  rp_sv_->free_replay_task(task);
}

void ObReplayStatus::free_replay_task_log_buf(ObLogReplayTask *task)
{
  rp_sv_->free_replay_task_log_buf(task);
}

void ObReplayStatus::set_post_barrier_submitted(const palf::LSN &lsn)
{
  ATOMIC_STORE(&post_barrier_lsn_.val_, lsn.val_);
}

int ObReplayStatus::set_post_barrier_finished(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  offset_t post_barrier_lsn_val = ATOMIC_LOAD(&post_barrier_lsn_.val_);
  if (OB_UNLIKELY(post_barrier_lsn_val != lsn.val_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "post_barrier_lsn_ not match", K(post_barrier_lsn_val), K(lsn), K(ret));
  } else {
    ATOMIC_STORE(&post_barrier_lsn_.val_, LOG_INVALID_LSN_VAL);
  }
  return ret;
}

int ObReplayStatus::check_submit_barrier()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "replay status not inited", K(ret));
  } else {
    offset_t post_barrier_lsn_val = ATOMIC_LOAD(&post_barrier_lsn_.val_);
    // 如果当前已经有后向barrier在队列中, 则需要此后向barrier日志回放完才能提交新任务
    if (LOG_INVALID_LSN_VAL == post_barrier_lsn_val) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

//前向barrier日志只有引用计数减为0的线程需要回放
int ObReplayStatus::check_replay_barrier(ObLogReplayTask *replay_task,
                                         ObLogReplayBuffer *&replay_log_buf,
                                         bool &need_replay,
                                         const int64_t replay_queue_idx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "replay status not inited", K(ret));
  } else if (NULL == replay_task
            || replay_queue_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "check_replay_barrier invalid argument", KP(replay_task), K(replay_queue_idx));
  } else if (replay_task->is_pre_barrier_) {
    int64_t replay_hint = replay_task->replay_hint_;
    int64_t nv = -1;
    if (NULL == (replay_log_buf = static_cast<ObLogReplayBuffer *>(replay_task->log_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "pre barrier log buff is NULL", K(ret), KPC(replay_task));
    } else if (NULL == replay_log_buf->log_buf_) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "pre barrier log real log buff is NULL", K(ret), KPC(replay_task));
    } else if (replay_queue_idx == calc_replay_queue_idx(replay_hint)
               && 1 != replay_log_buf->get_replay_ref()) {
      ret = OB_EAGAIN;
      //某个事务内的前向barrier日志只能在此队列回放
      CLOG_LOG(TRACE, "skip dec pre barrier log ref", K(ret), K(replay_task), KPC(replay_task),
               K(nv), KPC(this));
    } else if ((0 == (nv = replay_log_buf->dec_replay_ref()))) {
      if (replay_queue_idx != calc_replay_queue_idx(replay_hint)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "pre barrier log need replay but replay_queue_idx not match", K(ret), K(replay_task),
                 KPC(replay_task), K(replay_queue_idx), KPC(this));
      } else {
        need_replay = true;
      }
    } else if (nv < 0) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "dec pre barrier log ref less than 0", K(ret), K(replay_task), KPC(replay_task),
               K(nv), KPC(this));
    } else {
      //skip
      need_replay = false;
      CLOG_LOG(TRACE, "dec pre barrier log ref, skip replay", K(ret), K(replay_task), KPC(replay_task),
               K(nv), KPC(this));
    }
  } else {
    need_replay = true;
  }

  return ret;
}

void ObReplayStatus::set_err_info(const palf::LSN &lsn,
                                  const SCN &scn,
                                  const ObLogBaseType &log_type,
                                  const int64_t replay_hint,
                                  const bool is_submit_err,
                                  const int64_t err_ts,
                                  const int err_ret)
{
  err_info_.lsn_ = lsn;
  err_info_.scn_ = scn;
  err_info_.log_type_ = log_type;
  err_info_.is_submit_err_ = is_submit_err;
  err_info_.err_ts_ = err_ts;
  err_info_.err_ret_ = err_ret;
}

bool ObReplayStatus::is_fatal_error(const int ret) const
{
  return (OB_SUCCESS != ret
          && OB_ALLOCATE_MEMORY_FAILED != ret
          && OB_EAGAIN != ret
          && OB_NOT_RUNNING != ret
          // for temporary positioning issue
          && OB_IO_ERROR != ret
          && OB_DISK_HUNG != ret);
}

int ObReplayStatus::submit_task_to_replay_service_(ObReplayServiceTask &task)
{
  int ret = OB_SUCCESS;
  if (task.acquire_lease()) {
    /* The thread that gets the lease is responsible for encapsulating the task  as a request
     * and placing it into replay Service*/
    inc_ref(); //Add the reference count first, if the task push fails, dec_ref() is required
    if (OB_FAIL(rp_sv_->submit_task(&task))) {
      CLOG_LOG(ERROR, "failed to submit task to replay service", KPC(this),
                 K(task), K(ret));
      dec_ref();
    }
  }
  return ret;
}

int ObReplayStatus::stat(LSReplayStat &stat) const
{
  int ret = OB_SUCCESS;
  RLockGuard rlock_guard(rwlock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    stat.ls_id_ = ls_id_.id();
    stat.role_ = role_;
    stat.enabled_ = is_enabled_;
    stat.pending_cnt_ = pending_task_count_;
    if (OB_FAIL(submit_log_task_.get_next_to_submit_log_info(stat.unsubmitted_lsn_,
                                                             stat.unsubmitted_scn_))) {
      CLOG_LOG(WARN, "get_next_to_submit_log_info failed", KPC(this), K(ret));
    } else if (OB_FAIL(palf_handle_.get_end_lsn(stat.end_lsn_))) {
      CLOG_LOG(WARN, "get_end_lsn from palf failed", KPC(this), K(ret));
    }
  }
  return ret;
}

int ObReplayStatus::diagnose(ReplayDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  RLockGuard rlock_guard(rwlock_);
  LSN min_unreplayed_lsn;
  SCN min_unreplayed_scn;
  int64_t replay_hint = 0;
  ObLogBaseType log_type = ObLogBaseType::INVALID_LOG_BASE_TYPE;
  char log_type_str[common::MAX_SERVICE_TYPE_BUF_LENGTH];
  int64_t first_handle_time = 0;
  int64_t replay_cost = 0;
  int64_t retry_cost = 0;
  int replay_ret = OB_SUCCESS;
  bool is_submit_err = false;
  diagnose_info.diagnose_str_.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_enabled_) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(get_min_unreplayed_log_info(min_unreplayed_lsn, min_unreplayed_scn, replay_hint,
                                                 log_type, first_handle_time, replay_cost, retry_cost))) {
    CLOG_LOG(WARN, "get_min_unreplayed_log_info failed", KPC(this), K(ret));
  } else if (FALSE_IT(diagnose_info.max_replayed_lsn_ = min_unreplayed_lsn) ||
             FALSE_IT(diagnose_info.max_replayed_scn_ = SCN::minus(min_unreplayed_scn, 1))) {
  } else if (OB_FAIL(log_base_type_to_string(log_type, log_type_str, common::MAX_SERVICE_TYPE_BUF_LENGTH))) {
    CLOG_LOG(WARN, "log_base_type_to_string failed", K(ret), K(log_type));
  } else if (OB_SUCCESS != err_info_.err_ret_) {
    // 发生过不可重试的错误, 此场景不需要诊断最小未回放位日志
    min_unreplayed_lsn = err_info_.lsn_;
    min_unreplayed_scn = err_info_.scn_;
    replay_hint = err_info_.replay_hint_;
    log_type = err_info_.log_type_;
    first_handle_time = err_info_.err_ts_;
    replay_cost = 0;
    retry_cost = 0;
    replay_ret = err_info_.err_ret_;
    is_submit_err = err_info_.is_submit_err_;
  } else if (0 < retry_cost || 0 < replay_cost) {
    replay_ret = OB_EAGAIN;
  }
  if (OB_SUCC(ret) || OB_STATE_NOT_MATCH == ret) {
    ret = OB_SUCCESS;
    if (OB_FAIL(diagnose_info.diagnose_str_.append_fmt("is_enabled:%s; "
                                                       "ret:%d; "
                                                       "min_unreplayed_lsn:%ld; "
                                                       "min_unreplayed_scn:%lu; "
                                                       "replay_hint:%ld; "
                                                       "log_type:%s; "
                                                       "replay_cost:%ld; "
                                                       "retry_cost:%ld; "
                                                       "first_handle_time:%ld;" ,
                                                       is_enabled_? "true" : "false",
                                                       replay_ret, min_unreplayed_lsn.val_,
                                                       min_unreplayed_scn.get_val_for_inner_table_field(), replay_hint,
                                                       is_submit_err ? "REPLAY_SUBMIT" : log_type_str,
                                                       replay_cost, retry_cost, first_handle_time))) {
      CLOG_LOG(WARN, "append diagnose str failed", K(ret), K(replay_ret), K(min_unreplayed_lsn), K(min_unreplayed_scn),
               K(replay_hint), K(is_submit_err), K(replay_cost), K(retry_cost), K(first_handle_time));
    }
  }
  return ret;
}

int ObReplayStatus::trigger_fetch_log()
{
  int ret = OB_SUCCESS;
  RLockGuard rlock_guard(rwlock_);
  if (is_enabled_) {
    if (OB_FAIL(submit_task_to_replay_service_(submit_log_task_))) {
      CLOG_LOG(ERROR, "failed to submit submit_log_task to replay service", K(submit_log_task_),
                  KPC(this), K(ret));
    }
  } else {
    // do nothing
  }
  return ret;
}
} // namespace logservice
}
