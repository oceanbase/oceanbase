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

#include "ob_election_timer.h"
#include "ob_election.h"
#include "ob_election_async_log.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::election;

int ObElectionTask::init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTask inited twice", "election", *e_);
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(e) || OB_ISNULL(timer) || OB_ISNULL(rpc)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", "election", OB_P(e), KP(timer), KP(rpc));
    ret = OB_INVALID_ARGUMENT;
  } else {
    e_ = e;
    timer_ = timer;
    rpc_ = rpc;
    is_inited_ = true;
    ELECT_ASYNC_LOG(DEBUG, "ObElectionTask inited success");
  }

  return ret;
}

void ObElectionTask::reset()
{
  ObTimeWheelTask::reset();
  is_inited_ = false;
  is_running_ = false;
  e_ = NULL;
  timer_ = NULL;
  rpc_ = NULL;
  run_time_expect_ = 0;
  registered_ = false;
}

uint64_t ObElectionTask::hash() const
{
  uint64_t hv = 0;

  if (OB_ISNULL(e_)) {
    ELECT_ASYNC_LOG(WARN, "election is null", KP(e_));
  } else {
    hv = e_->hash();
  }

  return hv;
}

bool ObElectionTask::is_valid() const
{
  return (NULL != e_ && NULL != timer_ && NULL != rpc_);
}

int64_t ObElectionTask::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = ObTimeWheelTask::to_string(buf, buf_len);
  databuff_printf(buf,
      buf_len,
      pos,
      "[is_inited=%d, is_running=%d, election=%s, run_time_expect=%ld, registered=%d]",
      is_inited_,
      is_running_,
      to_cstring(*e_),
      run_time_expect_,
      registered_);
  return pos;
}

int ObElectionGT1Task::init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    if (NULL != e_) {
      ELECT_ASYNC_LOG(WARN, "ObElectionGT1Task inited twice", "election", *e_);
    }
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(e) || OB_ISNULL(timer) || OB_ISNULL(rpc)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", "election", OB_P(e), KP(timer), KP(rpc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObElectionTask::init(e, timer, rpc))) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTask inited error", K(ret), "election", *e);
  } else {
    is_inited_ = true;
    ELECT_ASYNC_LOG(DEBUG, "ObElectionGT1Task inited success");
  }

  return ret;
}

void ObElectionGT1Task::reset()
{
  ObElectionTask::reset();
  candidate_index_ = 0;
}

void ObElectionGT1Task::runTimerTask()
{
  if (OB_ISNULL(e_)) {
    ELECT_ASYNC_LOG(WARN, "election is NULL, unexpected error", KP(e_));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    e_->run_gt1_task(run_time_expect_);
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > 100 * 1000) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "run_gt1_task cost too much time", "time", end_time - start_time, KP(e_));
    }
    set_running(false);
  }
}

int64_t ObElectionGT1Task::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = ObElectionTask::to_string(buf, buf_len);
  databuff_printf(buf, buf_len, pos, "[task_type=%s, candidate_index=%ld]", "GT1", candidate_index_);
  return pos;
}

int ObElectionGT2Task::init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    if (NULL != e_) {
      ELECT_ASYNC_LOG(WARN, "ObElectionGT2Task inited twice", "election", *e_);
    }
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(e) || OB_ISNULL(timer) || OB_ISNULL(rpc)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", "election", OB_P(e), KP(timer), KP(rpc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObElectionTask::init(e, timer, rpc))) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTask inited error", K(ret), "election", *e_);
  } else {
    is_inited_ = true;
    ELECT_ASYNC_LOG(DEBUG, "ObElectionGT2Task inited success", "election", *e_);
  }

  return ret;
}

void ObElectionGT2Task::reset()
{
  ObElectionTask::reset();
}

void ObElectionGT2Task::runTimerTask()
{
  if (OB_ISNULL(e_)) {
    ELECT_ASYNC_LOG(WARN, "election is NULL, unexpected error", KP(e_));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    e_->run_gt2_task(run_time_expect_);
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > 100 * 1000) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "run_gt2_task cost too much time", "time", end_time - start_time, KP(e_));
    }
    set_running(false);
  }
}

int64_t ObElectionGT2Task::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = ObElectionTask::to_string(buf, buf_len);
  databuff_printf(buf, buf_len, pos, "[task_type=%s]", "GT2");
  return pos;
}

int ObElectionGT3Task::init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    if (NULL != e_) {
      ELECT_ASYNC_LOG(WARN, "ObElectionGT3Task inited twice", "election", *e_);
    }
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(e) || OB_ISNULL(timer) || OB_ISNULL(rpc)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", "election", OB_P(e), KP(timer), KP(rpc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObElectionTask::init(e, timer, rpc))) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTask inited error", K(ret), "election", *e_);
  } else {
    is_inited_ = true;
    ELECT_ASYNC_LOG(DEBUG, "ObElectionGT3Task inited success", "election", *e_);
  }

  return ret;
}

void ObElectionGT3Task::reset()
{
  ObElectionTask::reset();
}

void ObElectionGT3Task::runTimerTask()
{
  if (OB_ISNULL(e_)) {
    ELECT_ASYNC_LOG(WARN, "election is NULL, unexpected error", KP(e_));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    e_->run_gt3_task(run_time_expect_);
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > 100 * 1000) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "run_gt3_task cost too much time", "time", end_time - start_time, KP(e_));
    }
    set_running(false);
  }
}

int64_t ObElectionGT3Task::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = ObElectionTask::to_string(buf, buf_len);
  databuff_printf(buf, buf_len, pos, "[task_type=%s]", "GT3");
  return pos;
}

int ObElectionGT4Task::init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    if (NULL != e_) {
      ELECT_ASYNC_LOG(WARN, "ObElectionGT4Task inited twice", "election", *e_);
    }
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(e) || OB_ISNULL(timer) || OB_ISNULL(rpc)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", "election", OB_P(e), KP(timer), KP(rpc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObElectionTask::init(e, timer, rpc))) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTask inited error", K(ret), "election", *e_);
  } else {
    is_inited_ = true;
    ELECT_ASYNC_LOG(DEBUG, "ObElectionGT4Task inited success", "election", *e_);
  }

  return ret;
}

void ObElectionGT4Task::reset()
{
  ObElectionTask::reset();
}

void ObElectionGT4Task::runTimerTask()
{
  if (OB_ISNULL(e_)) {
    ELECT_ASYNC_LOG(WARN, "election is NULL, unexpected error", KP(e_));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    e_->run_gt4_task(run_time_expect_);
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > 100 * 1000) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "run_gt4_task cost too much time", "time", end_time - start_time, KP(e_));
    }
    set_running(false);
  }
}

int64_t ObElectionGT4Task::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = ObElectionTask::to_string(buf, buf_len);
  databuff_printf(buf, buf_len, pos, "[task_type=%s]", "GT4");
  return pos;
}

int ObElectionChangeLeaderTask::init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    if (NULL != e_) {
      ELECT_ASYNC_LOG(WARN, "ObElectionGT4Task inited twice", "election", *e_);
    }
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(e) || OB_ISNULL(timer) || OB_ISNULL(rpc)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", "election", OB_P(e), KP(timer), KP(rpc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObElectionTask::init(e, timer, rpc))) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTask inited error", K(ret), "election", *e_);
  } else {
    is_inited_ = true;
    ELECT_ASYNC_LOG(DEBUG, "ObElectionGT4Task inited success", "election", *e_);
  }

  return ret;
}

void ObElectionChangeLeaderTask::reset()
{
  ObElectionTask::reset();
}

void ObElectionChangeLeaderTask::runTimerTask()
{
  if (OB_ISNULL(e_)) {
    ELECT_ASYNC_LOG(WARN, "election is NULL, unexpected error", KP(e_));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    e_->run_change_leader_task(run_time_expect_);
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > 100 * 1000) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "run_change_leader_task cost too much time", "time", end_time - start_time, KP(e_));
    }
    set_running(false);
  }
}

int64_t ObElectionChangeLeaderTask::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = ObElectionTask::to_string(buf, buf_len);
  databuff_printf(buf, buf_len, pos, "[task_type=%s]", "ChangeLeaderTask");
  return pos;
}

// ObElectionTimer class implement
ObElectionTimer::ObElectionTimer() : is_inited_(false), is_running_(false), e_(NULL), tw_(NULL), rpc_(NULL)
{}

ObElectionTimer::~ObElectionTimer()
{
  destroy();
  ELECT_ASYNC_LOG(INFO, "ObElectionTimer deconstructed");
}

void ObElectionTimer::reset()
{
  is_inited_ = false;
  is_running_ = false;
  e_ = NULL;
  tw_ = NULL;
  rpc_ = NULL;
  gt1_.reset();
  gt2_.reset();
  gt3_.reset();
  gt4_.reset();
  change_leader_t_.reset();
}

int ObElectionTimer::init(ObIElectionTimerP* e, ObTimeWheel* tw, ObIElectionRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    if (NULL != e_) {
      ELECT_ASYNC_LOG(WARN, "ObElectionTimer has been inited", "election", *e_);
    }
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(e) || OB_ISNULL(tw) || OB_ISNULL(rpc)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", "election", OB_P(e), "timewheel", OB_P(tw), "rpc", OB_P(rpc));
  } else if (OB_FAIL(gt1_.init(e, this, rpc))) {
    ELECT_ASYNC_LOG(WARN, "init gt1 task error", K(ret), "election", *e);
  } else if (OB_FAIL(gt2_.init(e, this, rpc))) {
    ELECT_ASYNC_LOG(WARN, "init gt2 task error", K(ret), "election", *e);
  } else if (OB_FAIL(gt3_.init(e, this, rpc))) {
    ELECT_ASYNC_LOG(WARN, "init gt3 task error", K(ret), "election", *e);
  } else if (OB_FAIL(gt4_.init(e, this, rpc))) {
    ELECT_ASYNC_LOG(WARN, "init gt4 task error", K(ret), "election", *e);
  } else if (OB_FAIL(change_leader_t_.init(e, this, rpc))) {
    ELECT_ASYNC_LOG(WARN, "init change leader task error", K(ret), "election", *e);
  } else {
    e_ = e;
    tw_ = tw;
    rpc_ = rpc;
    is_inited_ = true;
    is_running_ = false;
    ELECT_ASYNC_LOG(INFO, "ObElectionTimer inited success", "election", *e_);
  }

  return ret;
}

void ObElectionTimer::destroy()
{
  int tmp_ret = OB_SUCCESS;

  if (is_inited_) {
    if (is_running_ && OB_SUCCESS != (tmp_ret = stop())) {
      ELECT_ASYNC_LOG(WARN, "ObElectionTimer stop error", K(tmp_ret), "election", *e_);
    } else {
      FORCE_ELECT_LOG(INFO, "ObElectionTimer destroyed", "election", *e_, K(lbt()));
      e_ = NULL;
      is_inited_ = false;
    }
  }
}

int ObElectionTimer::start(const int64_t start)
{
  int ret = OB_SUCCESS;
  int64_t delay = 0;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer is running", "election", *e_);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(register_gt_once_(gt1_, start, delay))) {
    ELECT_ASYNC_LOG(WARN, "register gt1 task error", K(ret), "election", *e_, K(start));
  } else {
    is_running_ = true;
    ELECT_ASYNC_LOG(INFO, "ObElectionTimer start success", "election", *e_, K(start), K(delay));
  }

  return ret;
}

int ObElectionTimer::try_stop()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer is not running", "election", *e_);
    // return OB_SUCCESS
  } else {
    int ret1 = unregister_gt_(gt1_);
    int ret2 = unregister_gt_(gt2_);
    int ret3 = unregister_gt_(gt3_);
    int ret4 = unregister_gt_(gt4_);
    int ret5 = unregister_gt_(change_leader_t_);

    if (OB_SUCC(ret1) && OB_SUCC(ret2) && OB_SUCC(ret3) && OB_SUCC(ret4) && OB_SUCC(ret5)) {
      is_running_ = false;
      ret = OB_SUCCESS;
      ELECT_ASYNC_LOG(INFO, "ObElectionTimer try_stop success", "election", *e_);
    } else {
      ret = OB_EAGAIN;
    }
  }

  return ret;
}

int ObElectionTimer::stop()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer is not running", "election", *e_);
    // return OB_SUCCESS
  } else {
    is_running_ = false;
    (void)unregister_gt_(gt1_);
    (void)unregister_gt_(gt2_);
    (void)unregister_gt_(gt3_);
    (void)unregister_gt_(gt4_);
    (void)unregister_gt_(change_leader_t_);
    ELECT_ASYNC_LOG(INFO, "ObElectionTimer stop success", "election", *e_);
  }

  return ret;
}

int ObElectionTimer::register_gt1_once(const int64_t run_time_expect, int64_t& delay)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer not inited");
    ret = OB_NOT_INIT;
  } else if (run_time_expect <= 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(run_time_expect), "election", *e_);
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer is not running", "election", *e_);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(register_gt_once_(gt1_, run_time_expect, delay))) {
    ELECT_ASYNC_LIMIT_LOG(WARN, "register gt1 task error", K(ret), "election", *e_, "start", run_time_expect);
  } else {
    // do nothing
  }

  return ret;
}

int ObElectionTimer::register_gt2_once(const int64_t run_time_expect, int64_t& delay)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer not inited");
    ret = OB_NOT_INIT;
  } else if (run_time_expect <= 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(run_time_expect), "election", *e_);
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer is not running", "election", *e_);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(register_gt_once_(gt2_, run_time_expect, delay))) {
    ELECT_ASYNC_LIMIT_LOG(WARN, "register gt2 task error", K(ret), "election", *e_, "start time", run_time_expect);
  } else {
    // do nothing
  }

  return ret;
}

int ObElectionTimer::register_gt3_once(const int64_t run_time_expect, int64_t& delay)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer not inited");
    ret = OB_NOT_INIT;
  } else if (run_time_expect <= 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(run_time_expect), "election", *e_);
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer is not running", "election", *e_);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(register_gt_once_(gt3_, run_time_expect, delay))) {
    ELECT_ASYNC_LOG(WARN, "register gt3 task error", K(ret), "election", *e_, "start time", run_time_expect);
  } else {
    // do nothing
  }

  return ret;
}

int ObElectionTimer::register_gt4_once(const int64_t run_time_expect, int64_t& delay)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer not inited");
    ret = OB_NOT_INIT;
  } else if (run_time_expect <= 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(run_time_expect), "election", *e_);
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer is not running", "election", *e_);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(register_gt_once_(gt4_, run_time_expect, delay))) {
    ELECT_ASYNC_LIMIT_LOG(WARN, "register gt4 task error", K(ret), "election", *e_, "start time", run_time_expect);
  }

  return ret;
}

int ObElectionTimer::register_change_leader_once(const int64_t run_time_expect, int64_t& delay)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer not inited");
    ret = OB_NOT_INIT;
  } else if (run_time_expect <= 0) {
    ELECT_ASYNC_LIMIT_LOG(WARN, "invalid argument", K(run_time_expect), "election", *e_);
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_running_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionTimer is not running", "election", *e_);
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(register_gt_once_(change_leader_t_, run_time_expect, delay))) {
      ELECT_ASYNC_LIMIT_LOG(
          WARN, "register change leader task error", K(ret), "election", *e_, "start time", run_time_expect);
    }
  }
  if (OB_SUCC(ret)) {
    // cencel registered gt4 task when register change leader task, avoid state resetted in gt4
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = unregister_gt_(gt4_))) {
      ELECT_ASYNC_LIMIT_LOG(
          WARN, "unregister gt4 task failed", K(tmp_ret), "election", *e_, "start time", run_time_expect);
    }
  }

  return ret;
}

template <typename GT>
int ObElectionTimer::register_gt_once_(GT& gt, const int64_t run_time_expect, int64_t& delay)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(e_) || OB_ISNULL(tw_)) {
    ELECT_ASYNC_LOG(WARN, "e_ or tw_ is NULL", KP(e_), KP(tw_));
    ret = OB_ERR_UNEXPECTED;
  } else if (run_time_expect <= 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(run_time_expect));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t cur_ts = e_->get_current_ts();
    // each partition actually delay more get_election_time_offset_() us time
    delay = run_time_expect - cur_ts;
    if (!gt.is_valid() || run_time_expect <= 0 || delay <= 0) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "invalid argument", K(gt), K(run_time_expect), K(delay), "election", *e_);
      ret = OB_INVALID_ARGUMENT;
    } else {
      if (gt.is_registered()) {
        (void)unregister_gt_(gt);
      }
      if (OB_FAIL(tw_->schedule(&gt, delay))) {
        ELECT_ASYNC_LOG(WARN, "fail to register gt timer task", K(ret), "election", *e_);
      } else {
        gt.set_run_time_expect(run_time_expect);
        gt.set_registered(true);
      }
    }
  }

  return ret;
}

template <typename GT>
int ObElectionTimer::unregister_gt_(GT& gt)
{
  int ret = OB_SUCCESS;

  if (gt.is_registered()) {
    if (OB_ISNULL(tw_)) {
      ELECT_ASYNC_LOG(ERROR, "tw_ is NULL", "election", *e_);
      ret = OB_ERR_UNEXPECTED;
    } else {
      (void)tw_->cancel(&gt);
      gt.set_registered(false);
    }
  }
  if (gt.is_running()) {
    ret = OB_EAGAIN;
  }

  return ret;
}
