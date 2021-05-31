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

#include "ob_trans_dependency.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_ctx_mgr.h"

namespace oceanbase {

namespace transaction {
int ObPartTransCtxDependencyWrap::init(ObPartTransCtx* ctx)
{
  int ret = OB_SUCCESS;
  const int64_t RESERVE_NUM = 2;

  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "ctx is null", KR(ret));
  } else {
    part_ctx_ = ctx;
  }

  return ret;
}

void ObPartTransCtxDependencyWrap::reset()
{
  part_ctx_ = NULL;
  prev_trans_arr_.reset();
  cur_stmt_prev_trans_arr_.reset();
  next_trans_arr_.reset();
  prev_trans_commit_count_ = 0;
  is_already_callback_ = false;
  min_prev_trans_version_ = INT64_MAX;
}

void ObPartTransCtxDependencyWrap::destroy()
{
  reset();
}

void ObPartTransCtxDependencyWrap::reset_prev_trans_arr()
{
  SpinWLockGuard guard(prev_trans_lock_);
  prev_trans_arr_.reset();
  cur_stmt_prev_trans_arr_.reset();
}

void ObPartTransCtxDependencyWrap::reset_next_trans_arr()
{
  SpinWLockGuard guard(next_trans_lock_);
  next_trans_arr_.reset();
}

int ObPartTransCtxDependencyWrap::prev_trans_arr_assign(const ObElrTransInfoArray& info_arr)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(prev_trans_lock_);

  // merge
  if (cur_stmt_prev_trans_arr_.count() > 0 && OB_FAIL(merge_cur_stmt_prev_trans_item_())) {
    TRANS_LOG(WARN, "merge current stmt prev trans error", K(ret), K_(cur_stmt_prev_trans_arr));
  } else if (OB_FAIL(prev_trans_arr_.assign(info_arr))) {
    TRANS_LOG(WARN, "prev trans arr assign erro", KR(ret), K(info_arr));
  } else {
    int prev_trans_commit_count = 0;
    for (int64_t i = 0; i < prev_trans_arr_.count(); i++) {
      if (ObTransResultState::is_abort(prev_trans_arr_.at(i).get_result())) {
        prev_trans_commit_count = INT32_MIN;
        break;
      } else if (ObTransResultState::is_commit(prev_trans_arr_.at(i).get_result())) {
        prev_trans_commit_count++;
      } else {
        // do nothing
      }
    }
    ATOMIC_STORE(&prev_trans_commit_count_, prev_trans_commit_count);
  }

  return ret;
}

int ObPartTransCtxDependencyWrap::get_prev_trans_arr_result(int& state)
{
  SpinWLockGuard guard(prev_trans_lock_);
  return get_prev_trans_arr_result_(state);
}

int64_t ObPartTransCtxDependencyWrap::get_prev_trans_arr_count()
{
  SpinRLockGuard guard(prev_trans_lock_);
  return (prev_trans_arr_.count() + cur_stmt_prev_trans_arr_.count());
}

int64_t ObPartTransCtxDependencyWrap::get_next_trans_arr_count()
{
  SpinRLockGuard guard(next_trans_lock_);
  return next_trans_arr_.count();
}

int ObPartTransCtxDependencyWrap::callback_next_trans_arr(int state)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(part_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "part ctx is NULL", K(*part_ctx_));
  } else {
    SpinRLockGuard guard(next_trans_lock_);
    // Force callback next transactions, even if next transaction is already committed/rollbacked;
    // Because of 6005 scenario, the state of next transaction is temporarily;
    for (int64_t i = 0; OB_SUCC(ret) && i < next_trans_arr_.count(); i++) {
      ObElrTransInfo& trans_info = next_trans_arr_.at(i);
      if (OB_FAIL(part_ctx_->submit_elr_callback_task(
              ObTransRetryTaskType::CALLBACK_NEXT_TRANS_TASK, trans_info.get_trans_id(), state))) {
        TRANS_LOG(WARN, "submit elr callback next trans task error", KR(ret), K(trans_info), K(state));
      } else {
        TRANS_LOG(DEBUG, "submit elr callback next trans task succ", K(trans_info), K(state));
      }
    }
    ATOMIC_STORE(&is_already_callback_, true);
  }

  return ret;
}

// pay attentation to lock sequence :
// 1) next_trans_arr of prev transaction lock firstly
// 2) prev_trans_arr of current transaction lock secondly
int ObPartTransCtxDependencyWrap::register_dependency_item(const uint32_t ctx_id, ObTransCtx* prev_trans_ctx)
{
  int ret = OB_SUCCESS;
  int64_t MAX_PREV_OR_NEXT_TRANS_COUNT = GCONF._max_elr_dependent_trx_count;

  if (OB_ISNULL(part_ctx_) || OB_ISNULL(prev_trans_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "part or prev trans ctx is null", KR(ret), KP_(part_ctx), KP(prev_trans_ctx));
  } else {
    ObPartTransCtx* dependent_ctx = static_cast<ObPartTransCtx*>(prev_trans_ctx);
    ObPartTransCtxDependencyWrap& ctx_dependency_wrap = dependent_ctx->get_ctx_dependency_wrap();
    if (OB_UNLIKELY(dependent_ctx->get_ctx_id() != ctx_id)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected ctx descriptor", KR(ret), KP_(part_ctx), KP(prev_trans_ctx));
    } else {
      SpinWLockGuard prev_trans_guard(ctx_dependency_wrap.next_trans_lock_);
      SpinWLockGuard cur_trans_guard(prev_trans_lock_);
      // prev transaction already committed/rollbacked, no need to register dependency;
      if (ctx_dependency_wrap.is_already_callback()) {
        ret = OB_EAGAIN;
        TRANS_LOG(INFO, "register dependency item already callback", KR(ret));
      } else if (ctx_dependency_wrap.get_next_trans_arr_().count() >= MAX_PREV_OR_NEXT_TRANS_COUNT ||
                 (prev_trans_arr_.count() + cur_stmt_prev_trans_arr_.count()) >= MAX_PREV_OR_NEXT_TRANS_COUNT) {
        ret = OB_EAGAIN;
        TRANS_LOG(DEBUG,
            "register dependency item need retry",
            KR(ret),
            K(ctx_dependency_wrap.get_next_trans_arr_().count()),
            K(prev_trans_arr_.count()));
        // add self into next_trans_arr of prev transaction
      } else if (OB_FAIL(ctx_dependency_wrap.add_next_trans_item_(
                     part_ctx_->get_trans_id(), part_ctx_->get_ctx_id(), part_ctx_->get_commit_version()))) {
        TRANS_LOG(WARN,
            "add self into prev trans item error",
            KR(ret),
            "prev_dependency_wrap",
            ctx_dependency_wrap,
            "curr_dependency_wrap",
            *this,
            "prev_context",
            *dependent_ctx,
            "cur_context",
            *part_ctx_);
        // add prev transaction into prev_trans_arr of self
      } else if (OB_FAIL(add_cur_stmt_prev_trans_item_(
                     dependent_ctx->get_trans_id(), ctx_id, dependent_ctx->get_commit_version()))) {
        TRANS_LOG(WARN,
            "add prev trans item into self error",
            KR(ret),
            "prev_dependency_wrap",
            ctx_dependency_wrap,
            "curr_dependency_wrap",
            *this,
            "prev_context",
            *dependent_ctx,
            "cur_context",
            *part_ctx_);
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObPartTransCtxDependencyWrap::unregister_dependency_item(const uint32_t ctx_id, ObTransCtx* prev_trans_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(part_ctx_) || OB_ISNULL(prev_trans_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "part or prev trans ctx is null", KR(ret), KP_(part_ctx), KP(prev_trans_ctx));
  } else {
    ObPartTransCtx* dependent_ctx = static_cast<ObPartTransCtx*>(prev_trans_ctx);
    ObPartTransCtxDependencyWrap& ctx_dependency_wrap = dependent_ctx->get_ctx_dependency_wrap();
    SpinWLockGuard prev_trans_guard(ctx_dependency_wrap.next_trans_lock_);
    SpinWLockGuard cur_trans_guard(prev_trans_lock_);
    // prev transaction already committed/rollbacked, no need to register dependency;
    if (ctx_dependency_wrap.is_already_callback()) {
      // remove self from next_trans_arr of prev transactions
    } else if (OB_FAIL(ctx_dependency_wrap.remove_next_trans_item_(
                   part_ctx_->get_trans_id(), part_ctx_->get_ctx_id(), part_ctx_->get_commit_version()))) {
      TRANS_LOG(WARN,
          "remove self from prev trans item error",
          KR(ret),
          "prev_dependency_wrap",
          ctx_dependency_wrap,
          "curr_dependency_wrap",
          *this,
          "prev_context",
          *dependent_ctx,
          "cur_context",
          *part_ctx_);
      // remove dependent transaction from prev_trans_arr of self
    } else if (OB_FAIL(remove_prev_trans_item_(
                   dependent_ctx->get_trans_id(), ctx_id, dependent_ctx->get_commit_version()))) {
      TRANS_LOG(WARN,
          "remove prev trans item from self error",
          KR(ret),
          "prev_dependency_wrap",
          ctx_dependency_wrap,
          "curr_dependency_wrap",
          *this,
          "prev_context",
          *dependent_ctx,
          "cur_context",
          *part_ctx_);
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPartTransCtxDependencyWrap::remove_prev_trans_item(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard cur_trans_guard(prev_trans_lock_);

  if (cur_stmt_prev_trans_arr_.count() > 0 && OB_FAIL(merge_cur_stmt_prev_trans_item_())) {
    TRANS_LOG(WARN, "merge current stmt prev trans error", K(ret), K_(cur_stmt_prev_trans_arr));
  } else {
    for (int64_t i = 0; i < prev_trans_arr_.count(); i++) {
      if (trans_id == prev_trans_arr_.at(i).get_trans_id()) {
        if (OB_FAIL(prev_trans_arr_.remove(i))) {
          TRANS_LOG(WARN, "remove trans item error", K(ret), K(trans_id), K_(prev_trans_arr));
        } else {
          ATOMIC_FAA(&prev_trans_commit_count_, -1);
        }
        break;
      }
    }
  }
  return ret;
}

int ObPartTransCtxDependencyWrap::remove_prev_trans_item_(
    const ObTransID& trans_id, uint32_t ctx_id, int64_t commit_version)
{
  int ret = OB_SUCCESS;

  if (cur_stmt_prev_trans_arr_.count() > 0 && OB_FAIL(merge_cur_stmt_prev_trans_item_())) {
    TRANS_LOG(WARN, "merge current stmt prev trans error", K(ret), K_(cur_stmt_prev_trans_arr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < prev_trans_arr_.count(); i++) {
      if (trans_id == prev_trans_arr_.at(i).get_trans_id()) {
        if (OB_FAIL(prev_trans_arr_.remove(i))) {
          TRANS_LOG(WARN, "remove trans item error", KR(ret), K(trans_id), K(ctx_id), K(commit_version));
        }
      }
    }
  }
  return ret;
}

void ObPartTransCtxDependencyWrap::clear_cur_stmt_prev_trans_item()
{
  SpinWLockGuard cur_trans_guard(prev_trans_lock_);
  cur_stmt_prev_trans_arr_.reset();
}

int ObPartTransCtxDependencyWrap::merge_cur_stmt_prev_trans_arr()
{
  SpinWLockGuard cur_trans_guard(prev_trans_lock_);
  return merge_cur_stmt_prev_trans_item_();
}

// for performance
int ObPartTransCtxDependencyWrap::merge_cur_stmt_prev_trans_item_()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < cur_stmt_prev_trans_arr_.count(); i++) {
    int64_t j = 0;
    for (; j < prev_trans_arr_.count(); j++) {
      if (cur_stmt_prev_trans_arr_.at(i).get_trans_id() == prev_trans_arr_.at(j).get_trans_id()) {
        break;
      }
    }
    if (j == prev_trans_arr_.count()) {
      if (OB_FAIL(prev_trans_arr_.push_back(cur_stmt_prev_trans_arr_.at(i)))) {
        TRANS_LOG(WARN, "prev trans arr push back error", K_(cur_stmt_prev_trans_arr), K_(prev_trans_arr));
      }
    }
  }
  if (OB_SUCC(ret) && cur_stmt_prev_trans_arr_.count() > 0) {
    cur_stmt_prev_trans_arr_.reset();
  }
  return ret;
}

int ObPartTransCtxDependencyWrap::add_cur_stmt_prev_trans_item_(
    const ObTransID& trans_id, uint32_t ctx_id, int64_t commit_version)
{
  int ret = OB_SUCCESS;
  ObElrTransInfo info;
  bool hit = false;

  for (int64_t i = 0; i < cur_stmt_prev_trans_arr_.count(); i++) {
    if (trans_id == cur_stmt_prev_trans_arr_.at(i).get_trans_id()) {
      hit = true;
      break;
    }
  }
  if (!hit) {
    if (OB_FAIL(info.init(trans_id, ctx_id, commit_version))) {
      TRANS_LOG(WARN, "elr trans info init error", KR(ret), K(trans_id), K(commit_version), K(*this));
    } else if (OB_FAIL(cur_stmt_prev_trans_arr_.push_back(info))) {
      TRANS_LOG(WARN, "push back elr trans info error", KR(ret), K(info), K(*this));
    } else if (commit_version < min_prev_trans_version_) {
      // record minimum version
      min_prev_trans_version_ = commit_version;
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartTransCtxDependencyWrap::remove_next_trans_item_(
    const ObTransID& trans_id, uint32_t ctx_id, int64_t commit_version)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < next_trans_arr_.count(); i++) {
    if (trans_id == next_trans_arr_.at(i).get_trans_id()) {
      if (ctx_id != next_trans_arr_.at(i).get_ctx_id()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected prev trans item info", KR(ret), K(trans_id), K(ctx_id), K(commit_version));
      } else if (OB_FAIL(next_trans_arr_.remove(i))) {
        TRANS_LOG(WARN, "remove trans item error", KR(ret), K(trans_id), K(ctx_id), K(commit_version));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObPartTransCtxDependencyWrap::add_next_trans_item_(
    const ObTransID& trans_id, uint32_t ctx_id, int64_t commit_version)
{
  int ret = OB_SUCCESS;
  ObElrTransInfo info;
  bool hit = false;

  for (int64_t i = 0; i < next_trans_arr_.count(); i++) {
    if (trans_id == next_trans_arr_.at(i).get_trans_id()) {
      hit = true;
      break;
    }
  }
  if (!hit) {
    if (OB_FAIL(info.init(trans_id, ctx_id, commit_version))) {
      TRANS_LOG(WARN, "elr trans info init error", KR(ret), K(trans_id), K(commit_version), K(*this));
    } else if (OB_FAIL(next_trans_arr_.push_back(info))) {
      TRANS_LOG(WARN, "push back elr trans info error", KR(ret), K(info), K(*this));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartTransCtxDependencyWrap::get_prev_trans_arr_result_(int& state)
{
  int ret = OB_SUCCESS;

  if (cur_stmt_prev_trans_arr_.count() > 0 && OB_FAIL(merge_cur_stmt_prev_trans_item_())) {
    TRANS_LOG(WARN, "merge current stmt prev trans error", K(ret), K_(cur_stmt_prev_trans_arr));
  } else {
    int32_t tmp_prev_trans_commit_count = ATOMIC_LOAD(&prev_trans_commit_count_);
    if (tmp_prev_trans_commit_count < 0) {
      state = ObTransResultState::ABORT;
    } else if (prev_trans_arr_.count() == tmp_prev_trans_commit_count) {
      state = ObTransResultState::COMMIT;
    } else if (prev_trans_arr_.count() > tmp_prev_trans_commit_count) {
      if (1 == prev_trans_arr_.count()) {
        if (ObTransResultState::is_decided_state(prev_trans_arr_.at(0).get_result()) &&
            0 == tmp_prev_trans_commit_count) {
          TRANS_LOG(WARN, "unexpected prev trans state", K(prev_trans_arr_), K(tmp_prev_trans_commit_count), K(lbt()));
          state = prev_trans_arr_.at(0).get_result();
        } else {
          state = ObTransResultState::UNKNOWN;
        }
      } else {
        state = ObTransResultState::UNKNOWN;
      }
    } else if (0 == prev_trans_arr_.count()) {
      state = ObTransResultState::COMMIT;
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "prev_trans_commit_count unexpected",
          KR(ret),
          K(tmp_prev_trans_commit_count),
          K(prev_trans_arr_.count()),
          K(*part_ctx_));
    }
  }

  return ret;
}

int ObPartTransCtxDependencyWrap::mark_prev_trans_result(const ObTransID& trans_id, int state, int& result)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(prev_trans_lock_);
  int64_t i = 0;
  bool hit = false;

  if (cur_stmt_prev_trans_arr_.count() > 0 && OB_FAIL(merge_cur_stmt_prev_trans_item_())) {
    TRANS_LOG(WARN, "merge current stmt prev trans error", K(ret), K_(cur_stmt_prev_trans_arr));
  } else {
    for (; i < prev_trans_arr_.count(); i++) {
      if (trans_id == prev_trans_arr_.at(i).get_trans_id()) {
        hit = true;
        break;
      }
    }
    if (hit) {
      ObElrTransInfo& info = prev_trans_arr_.at(i);
      if (OB_FAIL(cas_prev_trans_info_state_(info, state))) {
        TRANS_LOG(WARN, "cas trans info state error", KR(ret), K(info), K(state));
      } else if (OB_FAIL(get_prev_trans_arr_result_(result))) {
        TRANS_LOG(WARN, "get prev trans arr result error", KR(ret), K(info), K(result));
      } else {
        // do nothing
      }
    } else {
      // maybe statement rollback scenario
      // participant context already release when prev transaction is callbacking;
      TRANS_LOG(DEBUG, "no need to callback current trans", KR(ret), K(trans_id), K(*part_ctx_));
    }
  }

  return ret;
}

// should not depndend prev transaction if prev_trans's commit_version + 400ms < current trans's commti version;
int ObPartTransCtxDependencyWrap::check_can_depend_prev_elr_trans(const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(prev_trans_lock_);

  // for performance, we caculate the newest min commit version every time
  // in order to reduce the num of prev transactions;
  if (min_prev_trans_version_ != INT64_MAX && commit_version >= MAX_ELR_TRANS_INTERVAL + min_prev_trans_version_) {
    if (prev_trans_commit_count_ < 0 || prev_trans_commit_count_ == prev_trans_arr_.count()) {
      // all prev transaction rollbacked or committed
      ret = OB_SUCCESS;
    } else if (prev_trans_commit_count_ > prev_trans_arr_.count()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected prev trans commit count", K(ret), K(prev_trans_commit_count_));
    } else {
      // caculate the minimum version of uncommitted transaction
      int64_t tmp_min_commit_version = INT64_MAX;
      for (int64_t i = 0; i < prev_trans_arr_.count(); ++i) {
        ObElrTransInfo& trans_info = prev_trans_arr_.at(i);
        if (!ObTransResultState::is_decided_state(trans_info.get_result())) {
          if (trans_info.get_commit_version() < tmp_min_commit_version) {
            tmp_min_commit_version = trans_info.get_commit_version();
          }
        }
      }

      if (INT64_MAX == tmp_min_commit_version || tmp_min_commit_version < min_prev_trans_version_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR,
            "unexpected min prev trans commit version",
            K(ret),
            K(min_prev_trans_version_),
            K(tmp_min_commit_version));
      } else {
        min_prev_trans_version_ = tmp_min_commit_version;
        if (commit_version >= MAX_ELR_TRANS_INTERVAL + min_prev_trans_version_) {
          ret = OB_EAGAIN;
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  } else {
    ret = OB_SUCCESS;
    // continue to write sp_elr_trans_commit log
  }

  return ret;
}

int ObPartTransCtxDependencyWrap::cas_next_trans_info_state_(ObElrTransInfo& info, int state)
{
  int ret = OB_SUCCESS;
  int old_state = info.get_result();

  if (ObTransResultState::is_decided_state(state)) {
    if (!ObTransResultState::is_decided_state(old_state) && info.cas_result(old_state, state)) {
      TRANS_LOG(INFO, "set next trans info success", K(info), K(state), K(*part_ctx_), K(lbt()));
    }
  }

  return ret;
}

int ObPartTransCtxDependencyWrap::cas_prev_trans_info_state_(ObElrTransInfo& info, int state)
{
  int ret = OB_SUCCESS;
  int old_state = info.get_result();

  if (ObTransResultState::is_decided_state(state)) {
    if (!ObTransResultState::is_decided_state(old_state) && info.cas_result(old_state, state)) {
      if (ObTransResultState::is_commit(state)) {
        ATOMIC_FAA(&prev_trans_commit_count_, 1);
      } else {
        ATOMIC_STORE(&prev_trans_commit_count_, INT32_MIN);
      }
      TRANS_LOG(DEBUG, "cas prev trans info state success", K(info), K(state), K(*part_ctx_), K(lbt()));
    }
  }

  return ret;
}

int ObPartTransCtxDependencyWrap::get_prev_trans_arr_guard(ObElrTransArrGuard& guard)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(part_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "part_ctx is NULL", KR(ret));
  } else if (OB_FAIL(guard.set_args(prev_trans_arr_, prev_trans_lock_))) {
    TRANS_LOG(WARN, "set guard args error", KR(ret), K(*part_ctx_));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxDependencyWrap::get_next_trans_arr_guard(ObElrTransArrGuard& guard)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(part_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "part_ctx is NULL", KR(ret));
  } else if (OB_FAIL(guard.set_args(next_trans_arr_, next_trans_lock_))) {
    TRANS_LOG(WARN, "set guard args error", KR(ret), K(*part_ctx_));
  } else {
    // do nothing
  }

  return ret;
}

ObElrTransArrGuard::~ObElrTransArrGuard()
{
  int ret = OB_SUCCESS;
  if (NULL != trans_lock_ && OB_FAIL(trans_lock_->unlock())) {
    TRANS_LOG(WARN, "fail to unlock", KR(ret));
  }
}

int ObElrTransArrGuard::set_args(ObElrTransInfoArray& trans_arr, common::SpinRWLock& trans_lock)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(trans_lock.rdlock())) {
    TRANS_LOG(WARN, "fail to rdlock", KR(ret));
  } else {
    trans_arr_ = &trans_arr;
    trans_lock_ = &trans_lock;
  }

  return ret;
}

ObElrTransInfo* ObElrTransArrIterator::next()
{
  SpinRLockGuard guard(trans_lock_);
  return (index_ < trans_arr_.count()) ? &trans_arr_.at(index_++) : NULL;
}

}  // namespace transaction
}  // namespace oceanbase
