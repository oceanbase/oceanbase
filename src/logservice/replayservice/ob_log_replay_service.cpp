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

#include "ob_log_replay_service.h"
#include "lib/atomic/ob_atomic.h"
#include "ob_replay_status.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_ls_adapter.h"
#include "logservice/palf/palf_env.h"
#include "share/scn.h"
#include "share/ob_thread_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf;
using namespace storage;
namespace logservice
{
//---------------ReplayProcessStat---------------//
ReplayProcessStat::ReplayProcessStat()
  : last_replayed_log_size_(-1),
    rp_sv_(NULL),
    tg_id_(-1),
    is_inited_(false)
{}

ReplayProcessStat::~ReplayProcessStat()
{
  last_replayed_log_size_ = -1;
  rp_sv_ = NULL;
  tg_id_ = -1;
  is_inited_ = false;
}

int ReplayProcessStat::init(ObLogReplayService *rp_sv)
{
  int ret = OB_SUCCESS;
  int tg_id = lib::TGDefIDs::ReplayProcessStat;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ReplayProcessStat init twice", K(ret));
  } else if (NULL == rp_sv) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "rp_sv is NULL", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_))) {
    CLOG_LOG(ERROR, "ReplayProcessStat create failed", K(ret));
  } else {
    last_replayed_log_size_ = -1;
    rp_sv_ = rp_sv;
    CLOG_LOG(INFO, "ReplayProcessStat init success", K(rp_sv_), K(tg_id_), K(tg_id));
    is_inited_ = true;
  }
  return ret;
}

int ReplayProcessStat::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TG_START(tg_id_))) {
    CLOG_LOG(WARN, "ReplayProcessStat TG_START failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, SCAN_TIMER_INTERVAL, true))) {
    CLOG_LOG(WARN, "ReplayProcessStat TG_SCHEDULE failed", K(ret));
  } else {
    CLOG_LOG(INFO, "ReplayProcessStat start success", K(tg_id_), K(rp_sv_));
  }
  return ret;
}

void ReplayProcessStat::stop()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
    CLOG_LOG(INFO, "ReplayProcessStat stop finished", K(tg_id_), K(rp_sv_));
  }
}

void ReplayProcessStat::wait()
{
  if (IS_INIT) {
    TG_WAIT(tg_id_);
    CLOG_LOG(INFO, "ReplayProcessStat wait finished", K(tg_id_), K(rp_sv_));
  }
}

void ReplayProcessStat::destroy()
{
  if (IS_INIT) {
    CLOG_LOG(INFO, "ReplayProcessStat destroy finished", K(tg_id_), K(rp_sv_));
    is_inited_ = false;
    if (-1 != tg_id_) {
      TG_DESTROY(tg_id_);
      tg_id_ = -1;
    }
    rp_sv_ = NULL;
    last_replayed_log_size_ = -1;
  }
}

void ReplayProcessStat::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t replayed_log_size = 0;
  int64_t unreplayed_log_size = 0;
  int64_t estimate_time = 0;
  if (NULL == rp_sv_) {
    CLOG_LOG(ERROR, "rp_sv_ is NULL, unexpected error");
  } else if (OB_FAIL(rp_sv_->stat_all_ls_replay_process(replayed_log_size, unreplayed_log_size))) {
    CLOG_LOG(WARN, "stat_all_ls_replay_process failed", K(ret));
  } else if (0 > replayed_log_size || 0 > unreplayed_log_size) {
    CLOG_LOG(WARN, "stat_all_ls_replay_process failed", K(ret));
  } else if (-1 == last_replayed_log_size_) {
    last_replayed_log_size_ = replayed_log_size;
    CLOG_LOG(TRACE, "initial last_replayed_log_size_", K(ret), K(last_replayed_log_size_));
  } else {
    int64_t round_cost_time = SCAN_TIMER_INTERVAL / 1000 / 1000; //second
    int64_t last_replayed_log_size_MB = last_replayed_log_size_ >> 20;
    int64_t replayed_log_size_MB = replayed_log_size >> 20;
    int64_t unreplayed_log_size_MB = unreplayed_log_size >> 20;
    int64_t round_replayed_log_size_MB = replayed_log_size_MB - last_replayed_log_size_MB;
    int64_t pending_replay_log_size_MB = rp_sv_->get_pending_task_size() >> 20;
    last_replayed_log_size_ = replayed_log_size;
    if (0 == unreplayed_log_size) {
      estimate_time = 0;
    } else if (0 != round_replayed_log_size_MB) {
      estimate_time = round_cost_time * unreplayed_log_size_MB / round_replayed_log_size_MB;
    } else {
      estimate_time = -1;
    }

    if (-1 == estimate_time) {
      CLOG_LOG(INFO, "dump tenant replay process", "tenant_id", MTL_ID(), "unreplayed_log_size(MB)", unreplayed_log_size_MB,
                "estimate_time(second)=INF, replayed_log_size(MB)", replayed_log_size_MB,
                "last_replayed_log_size(MB)", last_replayed_log_size_MB, "round_cost_time(second)", round_cost_time,
                "pending_replay_log_size(MB)", pending_replay_log_size_MB);
    } else {
      CLOG_LOG(INFO, "dump tenant replay process", "tenant_id", MTL_ID(), "unreplayed_log_size(MB)", unreplayed_log_size_MB,
                "estimate_time(second)", estimate_time, "replayed_log_size(MB)", replayed_log_size_MB,
                "last_replayed_log_size(MB)", last_replayed_log_size_MB, "round_cost_time(second)", round_cost_time,
                "pending_replay_log_size(MB)", pending_replay_log_size_MB);
    }
  }
}

//---------------ObLogReplayService---------------//
ObLogReplayService::ObLogReplayService()
    : is_inited_(false),
    is_running_(false),
    tg_id_(-1),
    replay_stat_(),
    ls_adapter_(NULL),
    palf_env_(NULL),
    allocator_(NULL),
    replayable_point_(),
    replay_status_map_(),
    pending_replay_log_size_(0),
    wait_cost_stat_("[REPLAY STAT REPLAY TASK IN QUEUE TIME]", PALF_STAT_PRINT_INTERVAL_US),
    replay_cost_stat_("[REPLAY STAT REPLAY TASK EXECUTE COST TIME]", PALF_STAT_PRINT_INTERVAL_US)
  {}

ObLogReplayService::~ObLogReplayService()
{
  destroy();
}

int ObLogReplayService::init(PalfEnv *palf_env,
                             ObLSAdapter *ls_adapter,
                             ObILogAllocator *allocator)
{
  int ret = OB_SUCCESS;
  const uint64_t MAP_TENANT_ID = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MAP_TENANT_ID));
  int64_t thread_quota = std::max(1L, static_cast<int64_t>(tenant_config.is_valid() ? tenant_config->cpu_quota_concurrency : 4));

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogReplayService init twice", K(ret));
  } else if (OB_ISNULL(palf_env_= palf_env)
             || OB_ISNULL(ls_adapter_ = ls_adapter)
             || OB_ISNULL(allocator_ = allocator)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(palf_env), KP(ls_adapter), KP(allocator));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReplayService, tg_id_))) {
    CLOG_LOG(WARN, "fail to create thread group", K(ret));
  } else if (OB_FAIL(MTL_REGISTER_THREAD_DYNAMIC(thread_quota, tg_id_))) {
    CLOG_LOG(WARN, "MTL_REGISTER_THREAD_DYNAMIC failed", K(ret), K(tg_id_));
  } else if (OB_FAIL(replay_status_map_.init("REPLAY_STATUS", MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "replay_status_map_ init error", K(ret));
  } else if (OB_FAIL(replay_stat_.init(this))) {
    CLOG_LOG(WARN, "replay_stat_ init error", K(ret));
  } else {
    replayable_point_ = SCN::min_scn();
    pending_replay_log_size_ = 0;
    is_inited_ = true;
  }
  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }

  if (OB_SUCC(ret)) {
    CLOG_LOG(INFO, "replay service init success", K(tg_id_));
  }
  return ret;
}

int ObLogReplayService::start()
{
  int ret = OB_SUCCESS;
  const ObAdaptiveStrategy adaptive_strategy(LEAST_THREAD_NUM,
                                             ESTIMATE_TS,
                                             EXPAND_RATE,
                                             SHRINK_RATE);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogReplayService not inited!!!", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    CLOG_LOG(ERROR, "start ObLogReplayService failed", K(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_STRATEGY(tg_id_, adaptive_strategy))) {
    CLOG_LOG(WARN, "set adaptive strategy failed", K(ret));
  } else {
    is_running_ = true;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = replay_stat_.start())) {
      //不影响回放线程工作
      CLOG_LOG(WARN, "replay_stat start failed", K(tmp_ret));
    }
    CLOG_LOG(INFO, "start ObLogReplayService success", K(ret), K(tg_id_));
  }
  return ret;
}

void ObLogReplayService::stop()
{
  CLOG_LOG(INFO, "replay service stop begin");
  replay_stat_.stop();
  is_running_ = false;
  CLOG_LOG(INFO, "replay service stop finish");
  return;
}

void ObLogReplayService::wait()
{
  CLOG_LOG(INFO, "replay service wait begin");
  replay_stat_.wait();
  int64_t num = 0;

 int ret = OB_SUCCESS;
  while (OB_SUCC(TG_GET_QUEUE_NUM(tg_id_, num)) && num > 0) {
    PAUSE();
  }
  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "ObLogReplayService failed to get queue number");
  }
  CLOG_LOG(INFO, "replay service SimpleQueue empty");
  TG_STOP(tg_id_);
  TG_WAIT(tg_id_);
  CLOG_LOG(INFO, "replay service SimpleQueue destroy finish");
  CLOG_LOG(INFO, "replay service wait finish");
  return;
}

void ObLogReplayService::destroy()
{
  (void)remove_all_ls_();
  is_inited_ = false;
  CLOG_LOG(INFO, "replay service destroy");
  if (-1 != tg_id_) {
    MTL_UNREGISTER_THREAD_DYNAMIC(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  replayable_point_.reset();
  replay_stat_.destroy();
  pending_replay_log_size_ = 0;
  allocator_ = NULL;
  ls_adapter_ = NULL;
  palf_env_ = NULL;
  replay_status_map_.destroy();
}

void ObLogReplayService::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObReplayServiceTask *task_to_handle = static_cast<ObReplayServiceTask *>(task);
  ObReplayStatus *replay_status = NULL;
  bool need_push_back = false;
  // 不需要重新推入线程池的任务必须归还replay status的引用计数
  if (OB_ISNULL(task_to_handle)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "task is null", K(ret));
    on_replay_error_();
  } else if (OB_ISNULL(replay_status = task_to_handle->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "replay status is NULL", K(ret), K(task_to_handle));
    on_replay_error_();
  } else if (OB_UNLIKELY(IS_NOT_INIT)) {
    ret = OB_NOT_INIT;
    revert_replay_status_(replay_status);
    task_to_handle = NULL;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(ERROR, "replay service is not inited", K(ret));
    }
  } else if (!is_running_) {
    CLOG_LOG(INFO, "replay service has been stopped, just ignore the task",
               K(is_running_), KPC(replay_status));
    revert_replay_status_(replay_status);
    task_to_handle = NULL;
  } else {
    bool is_timeslice_run_out = false;
    ObReplayServiceTaskType task_type = task_to_handle->get_type();
    // 此处检查is_enable不上锁, 依赖实际内部处理逻辑持锁判断
    // 支持reuse语义, 此任务不能直接丢弃, 需要走正常的push back流程推进lease
    if (!replay_status->is_enabled()) {
      CLOG_LOG(INFO, "replay status is disabled, just ignore the task", KPC(replay_status));
    } else if (OB_FAIL(pre_check_(*replay_status, *task_to_handle))) {
      //print log in pre_check_
    } else if (ObReplayServiceTaskType::REPLAY_LOG_TASK == task_type) {
      ObReplayServiceReplayTask *replay_task = static_cast<ObReplayServiceReplayTask *>(task_to_handle);
      ret = handle_replay_task_(replay_task, is_timeslice_run_out);
    } else if (ObReplayServiceTaskType::SUBMIT_LOG_TASK == task_type) {
      ObReplayServiceSubmitTask *submit_task = static_cast<ObReplayServiceSubmitTask *>(task_to_handle);
      ret = handle_submit_task_(submit_task, is_timeslice_run_out);
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "invalid task_type", K(ret), K(task_type), KPC(replay_status));
    }

    if (OB_FAIL(ret) || is_timeslice_run_out) {
      //replay failed or timeslice is run out or failed to get lock
      need_push_back = true;
    } else if (task_to_handle->revoke_lease()) {
      //success to set state to idle, no need to push back
      revert_replay_status_(replay_status);
    } else {
      need_push_back = true;
    }
  }

  if ((OB_FAIL(ret) || need_push_back) && NULL != task_to_handle) {
    //the ret is not OB_SUCCESS  or revoke fails, or the count of logs replayed this time reaches the threshold,
    //the task needs to be push back to the end of the queue
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = submit_task(task_to_handle))) {
      CLOG_LOG(ERROR, "push task back after handle failed", K(tmp_ret), KPC(task_to_handle), KPC(replay_status), K(ret));
      // simplethreadpool stop无锁, 并发下可能出现push失败
      // 失败时归还replay_status引用计数即可, 任务可以直接丢弃
      revert_replay_status_(replay_status);
    } else {
      //do nothing
    }
  }
}

int ObLogReplayService::add_ls(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObMemAttr attr(MTL_ID(), ObModIds::OB_LOG_REPLAY_STATUS);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (NULL == (replay_status = static_cast<ObReplayStatus*>(mtl_malloc(sizeof(ObReplayStatus), attr)))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to alloc replay status", K(ret), K(id));
  } else {
    new (replay_status) ObReplayStatus();
    if (OB_FAIL(replay_status->init(id, palf_env_, this))) {
      mtl_free(replay_status);
      replay_status = NULL;
      CLOG_LOG(WARN, "failed to init replay status", K(ret), K(id), K(palf_env_), K(this));
    } else {
      replay_status->inc_ref();
      if (OB_FAIL(replay_status_map_.insert(id, replay_status))) {
        // enable后已经开始回放,不能直接free
        CLOG_LOG(ERROR, "failed to insert log stream", K(ret), K(id), KPC(replay_status));
        revert_replay_status_(replay_status);
      } else {
        CLOG_LOG(INFO, "add_ls success", K(ret), K(id), KPC(replay_status));
      }
    }
  }
  return ret;
}

// 先从map中摘掉再尝试释放内存
int ObLogReplayService::remove_ls(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  RemoveReplayStatusFunctor functor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not inited", K(ret), K(id));
  } else if (OB_FAIL(replay_status_map_.erase_if(id, functor))) {
    CLOG_LOG(WARN, "failed to remove log stream", K(ret), K(id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else {
    CLOG_LOG(INFO, "replay service remove ls", K(ret), K(id));
  }
  return ret;
}

// base_lsn可以不和base_scn完全对应,以base_scn为基准过滤回放,
// 调用者需要注意log scn为base_scn的日志需要回放
int ObLogReplayService::enable(const share::ObLSID &id,
                               const LSN &base_lsn,
                               const SCN &base_scn)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else if (OB_FAIL(replay_status->enable(base_lsn, base_scn))) {
    CLOG_LOG(WARN, "replay status enable failed", K(ret), K(id), K(base_lsn), K(base_scn));
  }
  return ret;
}

int ObLogReplayService::disable(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else if (OB_FAIL(replay_status->disable())) {
    CLOG_LOG(WARN, "replay status disable failed", K(ret), K(id));
  }
  return ret;
}

int ObLogReplayService::is_enabled(const share::ObLSID &id, bool &is_enabled)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else {
    is_enabled = replay_status->is_enabled();
  }
  return ret;
}

int ObLogReplayService::block_submit_log(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else {
    replay_status->block_submit();
  }
  return ret;
}

int ObLogReplayService::unblock_submit_log(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else {
    replay_status->unblock_submit();
  }
  return ret;
}

int ObLogReplayService::switch_to_leader(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else {
    replay_status->switch_to_leader();
  }
  return ret;
}

int ObLogReplayService::switch_to_follower(const share::ObLSID &id,
                                           const palf::LSN &begin_lsn)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else {
    replay_status->switch_to_follower(begin_lsn);
  }
  return ret;
}

int ObLogReplayService::is_replay_done(const share::ObLSID &id,
                                       const LSN &end_lsn,
                                       bool &is_done)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else if (OB_FAIL(replay_status->is_replay_done(end_lsn, is_done))){
    CLOG_LOG(WARN, "check replay done failed", K(ret), K(id));
  } else {
    // do nothing
  }
  return ret;
}

//通用接口, 受控回放时最终返回值为受控回放点前的最后一条日志的log_ts
int ObLogReplayService::get_max_replayed_scn(const share::ObLSID &id, SCN &scn)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else if (OB_FAIL(replay_status->get_max_replayed_scn(scn))) {
    if (OB_STATE_NOT_MATCH != ret) {
      CLOG_LOG(WARN, "get_max_replayed_scn failed", K(ret), K(id));
    } else if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "get_max_replayed_scn failed, replay status is not enabled", K(ret), K(id));
    }
  }
  return ret;
}

int ObLogReplayService::submit_task(ObReplayServiceTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "task is NULL", K(ret));
  } else {
    task->set_enqueue_ts(ObTimeUtility::fast_current_time());
    while (OB_FAIL(TG_PUSH_TASK(tg_id_, task)) && OB_EAGAIN == ret) {
      //预期不应该失败
      ob_usleep(1000);
      CLOG_LOG(ERROR, "failed to push", K(ret));
    }
  }
  return ret;
}

void ObLogReplayService::free_replay_task(ObLogReplayTask *task)
{
  CLOG_LOG(TRACE, "free_replay_task", KPC(task), K(task));
  allocator_->free_replay_task(task);
  task = NULL;
}

void ObLogReplayService::free_replay_task_log_buf(ObLogReplayTask *task)
{
  CLOG_LOG(TRACE, "free_replay_task_log_buf", KPC(task), K(task));
  if (NULL != task && task->is_pre_barrier_) {
    allocator_->free_replay_log_buf(task->log_buf_);
    task->log_buf_ = NULL;
  }
}

int ObLogReplayService::update_replayable_point(const SCN &replayable_scn)
{
  int ret = OB_SUCCESS;
  FetchLogFunctor functor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else {
    if (replayable_scn > replayable_point_) {
      replayable_point_.atomic_set(replayable_scn) ;
    }
    if (OB_FAIL(replay_status_map_.for_each(functor))) {
      CLOG_LOG(WARN, "failed to trigger submit log task", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLogReplayService::get_replayable_point(SCN &replayable_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else {
    replayable_scn = replayable_point_.atomic_load();
  }
  return ret;
}

int ObLogReplayService::stat_for_each(const common::ObFunction<int (const ObReplayStatus &)> &func)
{
  auto stat_func = [&func](const ObLSID &id, ObReplayStatus *replay_status) -> bool {
    int ret = OB_SUCCESS;
    bool bret = true;
    if (OB_FAIL(func(*replay_status))) {
      bret = false;
      CLOG_LOG(WARN, "iter replay stat failed", K(ret));
    }
    return bret;
  };
  return replay_status_map_.for_each(stat_func);
}

int ObLogReplayService::stat_all_ls_replay_process(int64_t &replayed_log_size,
                                                   int64_t &unreplayed_log_size)
{
  int ret = OB_SUCCESS;
  StatReplayProcessFunctor functor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not inited", K(ret));
  } else if (OB_FAIL(replay_status_map_.for_each(functor))) {
    CLOG_LOG(WARN, "failed to remove log stream", K(ret));
  } else {
    replayed_log_size = functor.get_replayed_log_size();
    unreplayed_log_size = functor.get_unreplayed_log_size();
  }
  return ret;
}

void ObLogReplayService::inc_pending_task_size(const int64_t log_size)
{
  ATOMIC_AAF(&pending_replay_log_size_, log_size);
}

void ObLogReplayService::dec_pending_task_size(const int64_t log_size)
{
  int64_t nv = ATOMIC_SAF(&pending_replay_log_size_, log_size);
  if (nv < 0) {
    CLOG_LOG_RET(ERROR, OB_ERROR, "dec_pending_task_size less than 0", K(nv));
  }
}

int64_t ObLogReplayService::get_pending_task_size() const
{
  return ATOMIC_LOAD(&pending_replay_log_size_);
}

void *ObLogReplayService::alloc_replay_task(const int64_t size)
{
  return allocator_->alloc_replay_task(size);
}

int ObLogReplayService::get_replay_status_(const share::ObLSID &id,
                                           ObReplayStatusGuard &guard)
{
  int ret = OB_SUCCESS;
  GetReplayStatusFunctor functor(guard);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(replay_status_map_.operate(id, functor))) {
    CLOG_LOG(WARN, "replay service get replay status failed", K(ret), K(id));
  } else {
    CLOG_LOG(TRACE, "replay service get success", K(ret), K(id));
  }
  return ret;
}

bool ObLogReplayService::is_tenant_out_of_memory_() const
{
  bool bool_ret = true;
  int64_t pending_size = get_pending_task_size();
  bool is_pending_too_large = MTL(ObTenantFreezer *)->is_replay_pending_log_too_large(pending_size);
  bool_ret = (pending_size >= PENDING_TASK_MEMORY_LIMIT || is_pending_too_large);
  return bool_ret;
}

void ObLogReplayService::process_replay_ret_code_(const int ret_code,
                                                  ObReplayStatus &replay_status,
                                                  ObReplayServiceReplayTask &task_queue,
                                                  ObLogReplayTask &replay_task)
{
  if (OB_SUCCESS != ret_code) {
    int64_t cur_ts = ObTimeUtility::fast_current_time();
    if (replay_status.is_fatal_error(ret_code)) {
      replay_status.set_err_info(replay_task.lsn_, replay_task.scn_, replay_task.log_type_,
                                 replay_task.replay_hint_, false, cur_ts, ret_code);
      LOG_DBA_ERROR(OB_LOG_REPLAY_ERROR, "msg", "replay task encountered fatal error", "ret", ret_code,
                    K(replay_status), K(replay_task));
    } else {/*do nothing*/}

    if (OB_SUCCESS == task_queue.get_err_info_ret_code()) {
      task_queue.set_simple_err_info(cur_ts, ret_code);
    } else if (task_queue.get_err_info_ret_code() != ret_code) {
      //just override ret_code
      task_queue.override_err_info_ret_code(ret_code);
    } else {/*do nothing*/}
  }
}

int ObLogReplayService::pre_check_(ObReplayStatus &replay_status,
                                   ObReplayServiceTask &task)
{
  int ret = OB_SUCCESS;
  if (replay_status.try_rdlock()) {
    int64_t cur_time = ObTimeUtility::fast_current_time();
    int64_t enqueue_ts = task.get_enqueue_ts();
    // replay log task has encounted fatal error, just exists directly
    if (OB_UNLIKELY(replay_status.has_fatal_error() || task.has_fatal_error())) {
      //encounted fatal error last round,set OB_EAGAIN here
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        CLOG_LOG(ERROR, "ReplayService has encountered a fatal error", K(replay_status), K(task), K(ret));
      }
      //set egain just to push back into thread_pool
      ret = OB_EAGAIN;
      ob_usleep(1000); //1ms
    } else if (!task.need_replay_immediately()) {
      //避免重试过于频繁导致cpu跑满
      ob_usleep(10); //10us
    }
    // Check the waiting time of the task in the global queue
    if (OB_SUCC(ret) && (cur_time - enqueue_ts > TASK_QUEUE_WAIT_IN_GLOBAL_QUEUE_TIME_THRESHOLD)) {
      CLOG_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "task queue has waited too much time in global queue, please check single replay task error",
               K(replay_status), K(task), K(ret));
    }
    //unlock
    replay_status.unlock();
  } else {
    ret = OB_EAGAIN;
    //At this time, the write lock is added, and the partition may be cleaning up
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "try lock failed in pre_check", K(replay_status), K(task), K(ret));
    }
  }
  return ret;
}

int ObLogReplayService::do_replay_task_(ObLogReplayTask *replay_task,
                                        ObReplayStatus *replay_status,
                                        const int64_t replay_queue_idx)
{
  int ret = OB_SUCCESS;
  ObLS *ls;
  get_replay_queue_index() = replay_queue_idx;
  ObLogReplayBuffer *replay_log_buff = NULL;
  bool need_replay = false;
  replay_task->first_handle_ts_ = ObTimeUtility::fast_current_time();
  if (OB_ISNULL(replay_status) || OB_ISNULL(replay_task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", KPC(replay_task), KPC(replay_status), KR(ret));
  } else if (OB_ISNULL(ls_adapter_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(replay_status->check_replay_barrier(replay_task, replay_log_buff,
                                                         need_replay, replay_queue_idx))) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "wait for barrier", K(ret), KPC(replay_status), KPC(replay_task));
    }
  } else if (!need_replay) {
    //do nothing
  } else if (replay_task->is_pre_barrier_) {
    replay_task->log_buf_ = replay_log_buff->log_buf_;
    if (OB_FAIL(ls_adapter_->replay(replay_task))) {
      replay_log_buff->inc_replay_ref();
      replay_task->log_buf_ = replay_log_buff;
      CLOG_LOG(WARN, "ls do pre barrier replay failed", K(ret), K(replay_task), KPC(replay_task),
               KPC(replay_status));
    } else {
      //释放log_buf内存
      CLOG_LOG(INFO, "pre barrier log replay succ", KPC(replay_task), KPC(replay_status),
               K(replay_queue_idx), K(ret));
      replay_task->log_buf_ = replay_log_buff;
      free_replay_task_log_buf(replay_task);
      replay_status->dec_pending_task(replay_task->log_size_);
    }
  } else if (OB_FAIL(ls_adapter_->replay(replay_task))) {
    CLOG_LOG(WARN, "ls do replay failed", K(ret), KPC(replay_task));
  }
  if (OB_SUCC(ret) && need_replay) {
    if (replay_task->is_post_barrier_) {
      if (OB_FAIL(replay_status->set_post_barrier_finished(replay_task->lsn_))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected barrier_status", KPC(replay_task), KPC(replay_status), K(ret));
      } else {
        CLOG_LOG(INFO, "post barrier log replay succ", KPC(replay_task),
                 KPC(replay_status), K(ret));
      }
    }
    CLOG_LOG(TRACE, "do replay task", KPC(replay_task), KPC(replay_status));
  }
  get_replay_queue_index() = -1;
  get_replay_is_writing_throttling() = false;
  return ret;
}

void ObLogReplayService::revert_replay_status_(ObReplayStatus *replay_status)
{
  if (NULL != replay_status) {
    if (0 == replay_status->dec_ref()) {
      CLOG_LOG(INFO, "free replay status", KPC(replay_status));
      replay_status->~ObReplayStatus();
      mtl_free(replay_status);
      replay_status = NULL;
    }
  }
}

int ObLogReplayService::check_can_submit_log_replay_task_(ObLogReplayTask *replay_task,
                                                          ObReplayStatus *replay_status)
{
  int ret = OB_SUCCESS;

  bool is_wait_barrier = false;
  bool is_tenant_out_of_mem = false;
  if (NULL == replay_task || NULL == replay_status) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "check_can_submit_log_replay_task_ invalid argument", KPC(replay_status), KPC(replay_task));
  } else if (OB_FAIL(replay_status->check_submit_barrier())) {
    if (OB_EAGAIN != ret) {
      CLOG_LOG(ERROR, "failed to check_submit_barrier", K(ret), KPC(replay_status));
    } else {
      is_wait_barrier = true;
    }
  } else if (OB_UNLIKELY(is_tenant_out_of_memory_())) {
    ret = OB_EAGAIN;
    is_tenant_out_of_mem = true;
  }
  if (OB_EAGAIN == ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
    CLOG_LOG(INFO, "submit replay task need retry", K(ret), KPC(replay_status), KPC(replay_task),
             K(is_wait_barrier), K(is_tenant_out_of_mem), "pending_task_size", get_pending_task_size());
  }
  return ret;
}

// under replay status lock protection
int ObLogReplayService::fetch_pre_barrier_log_(ObReplayStatus &replay_status,
                                               ObReplayServiceSubmitTask *submit_task,
                                               ObLogReplayTask *&replay_task,
                                               const ObLogBaseHeader &header,
                                               const char *log_buf,
                                               const LSN &cur_lsn,
                                               const SCN &cur_log_submit_scn,
                                               const int64_t log_size,
                                               const bool is_raw_write)
{
  int ret = OB_SUCCESS;
  ObLSID id;
  if (OB_FAIL(replay_status.get_ls_id(id))) {
    CLOG_LOG(ERROR, "replay status get log stream id failed", KPC(submit_task),
             K(replay_status), KR(ret));
  } else {
    const int64_t share_log_size = sizeof(ObLogReplayBuffer) + log_size;
    const int64_t task_size = sizeof(ObLogReplayTask);
    char *share_log_buf = NULL;
    void *task_buf = NULL;
    ObMemAttr attr(MTL_ID(), ObModIds::OB_LOG_REPLAY_TASK);
    if (OB_UNLIKELY(NULL == (share_log_buf =static_cast<char *>(allocator_->alloc_replay_log_buf(share_log_size))))) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        CLOG_LOG(WARN, "failed to alloc log replay task buf", K(ret), K(id), K(cur_lsn));
      }
    } else if (OB_UNLIKELY(NULL == (task_buf = alloc_replay_task(task_size)))) {
      ret = OB_EAGAIN;
      allocator_->free_replay_log_buf(share_log_buf);
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        CLOG_LOG(WARN, "failed to alloc log replay task buf", K(ret), K(id), K(cur_lsn));
      }
    } else {
      replay_task = new (task_buf) ObLogReplayTask(id, header, cur_lsn, cur_log_submit_scn, log_size, is_raw_write);
      ObLogReplayBuffer *replay_log_buffer = new (share_log_buf) ObLogReplayBuffer();
      char *real_log_buf = share_log_buf + sizeof(ObLogReplayBuffer);
      replay_log_buffer->log_buf_ = real_log_buf;
      MEMCPY(real_log_buf, log_buf, log_size);
      if (OB_FAIL(replay_task->init(replay_log_buffer))) {
        allocator_->free_replay_log_buf(share_log_buf);
        CLOG_LOG(ERROR, "init replay task failed", K(ret), K(id), K(id), K(cur_lsn), K(cur_log_submit_scn),
                 K(log_size), KP(replay_log_buffer), K(header), K(is_raw_write));
      } else {
        CLOG_LOG(TRACE, "fetch_and_submit_pre_barrier_log_", K(ret), K(replay_status), K(replay_task),
                 KPC(replay_task), K(replay_log_buffer->log_buf_));
      }
    }
  }

  return ret;
}

// under replay status lock protection
int ObLogReplayService::fetch_and_submit_single_log_(ObReplayStatus &replay_status,
                                                     ObReplayServiceSubmitTask *submit_task,
                                                     LSN &cur_lsn,
                                                     SCN &cur_log_submit_scn,
                                                     int64_t &log_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const char *log_buf = NULL;
  bool is_raw_write = true;
  ObLSID id;
  bool need_skip = false;
  bool need_iterate_next_log = false;
  ObLogBaseHeader header;
  int64_t header_pos = 0;
  ObLogReplayTask *replay_task = NULL;
  const SCN &replayable_point = replayable_point_.atomic_load();
  if (OB_UNLIKELY(OB_ISNULL(submit_task))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "submit task is NULL when fetch log", K(id), K(replay_status), KPC(submit_task), K(ret));
  } else if (OB_FAIL(submit_task->get_log(log_buf, log_size, cur_log_submit_scn, cur_lsn, is_raw_write))) {
    CLOG_LOG(WARN, "submit task get log value failed", K(id), K(replay_status), K(submit_task));
  } else if (OB_FAIL(replay_status.get_ls_id(id))) {
    CLOG_LOG(ERROR, "replay status get log stream id failed", KPC(submit_task), K(replay_status), KR(ret));
  } else if (OB_FAIL(submit_task->need_skip(cur_log_submit_scn, need_skip))) {
    CLOG_LOG(INFO, "check need_skip failed", K(replay_status), K(cur_lsn), K(log_size), K(cur_log_submit_scn), KR(ret));
  } else if (need_skip) {
    need_iterate_next_log = true;
    CLOG_LOG(TRACE, "skip current log", K(replay_status), K(cur_lsn), K(log_size), K(cur_log_submit_scn));
  } else if (OB_FAIL(header.deserialize(log_buf, log_size, header_pos))) {
    CLOG_LOG(WARN, "basic header deserialize failed", K(ret), K(header_pos), K(id));
  } else if (ObLogBaseType::PADDING_LOG_BASE_TYPE == header.get_log_type()) {
    // For padding log entry, iterate next log directly.
    need_iterate_next_log = true;
    CLOG_LOG(INFO, "no need to replay padding log entry", KPC(submit_task), K(header));
  } else if (header.need_pre_replay_barrier()) {
    // 前向barrier日志的replay task和log buf需要分别分配内存
    if (OB_FAIL(fetch_pre_barrier_log_(replay_status,
                                       submit_task,
                                       replay_task,
                                       header,
                                       log_buf,
                                       cur_lsn,
                                       cur_log_submit_scn,
                                       log_size,
                                       is_raw_write))) {
      //print log inside
    }
  } else {
    // 非前向barrier日志的replay task分配整块内存
    const int64_t task_size = sizeof(ObLogReplayTask) + log_size;
    char *task_buf = NULL;
    ObMemAttr attr(MTL_ID(), ObModIds::OB_LOG_REPLAY_TASK);
    if (OB_UNLIKELY(NULL == (task_buf = static_cast<char *>(alloc_replay_task(task_size))))) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        CLOG_LOG(WARN, "failed to alloc log replay task buf", K(ret), K(id), K(cur_lsn));
      }
    } else {
      replay_task = new (task_buf) ObLogReplayTask(id, header, cur_lsn, cur_log_submit_scn, log_size, is_raw_write);
      char *task_log_buf = task_buf + sizeof(ObLogReplayTask);
      MEMCPY(task_log_buf, log_buf, log_size);
      if (OB_FAIL(replay_task->init(task_log_buf))) {
        // print log details inside
        CLOG_LOG(ERROR, "init replay task failed", K(ret), K(id));
      }
    }
  }
  if (OB_SUCC(ret) && NULL != replay_task) {
    if (OB_FAIL(check_can_submit_log_replay_task_(replay_task, &replay_status))) {
      // do nothing
    } else if (OB_FAIL(submit_log_replay_task_(*replay_task, replay_status))) {
      CLOG_LOG(WARN, "submit_log_replay_task_ failed", K(ret), K(replay_status), KPC(submit_task), K(cur_lsn));
    } else {
      need_iterate_next_log = true;
    }
  }
  if (OB_FAIL(ret) && NULL != replay_task) {
    if (OB_EAGAIN != ret) {
      replay_status.set_err_info(cur_lsn, cur_log_submit_scn, replay_task->log_type_, replay_task->replay_hint_,
                                 false, ObClockGenerator::getClock(), ret);
    }
    free_replay_task_log_buf(replay_task);
    free_replay_task(replay_task);
  } else if (OB_SUCC(ret) && need_iterate_next_log) {
    bool unused_iterate_end_by_replayable_point = false;
    // TODO by runlin: compatibility with LogEntryHeader
    if (OB_FAIL(submit_task->update_submit_log_meta_info(cur_lsn + log_size + sizeof(LogEntryHeader),
                                                         cur_log_submit_scn))) {
      // log info fallback
      CLOG_LOG(ERROR, "failed to update_submit_log_meta_info", KR(ret), K(cur_lsn),
               K(log_size), K(cur_log_submit_scn));
      replay_status.set_err_info(cur_lsn, cur_log_submit_scn, ObLogBaseType::INVALID_LOG_BASE_TYPE,
                                 0, true, ObClockGenerator::getClock(), ret);
    } else if (OB_FAIL(submit_task->next_log(replayable_point, unused_iterate_end_by_replayable_point))) {
      CLOG_LOG(ERROR, "failed to next_log", K(replay_status), K(cur_lsn), K(log_size), K(cur_log_submit_scn),
               K(replayable_point), K(ret));
    }
  }
  return ret;
}

int ObLogReplayService::handle_submit_task_(ObReplayServiceSubmitTask *submit_task,
                                            bool &is_timeslice_run_out)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_ISNULL(submit_task)) {
    ret = OB_ERR_UNEXPECTED;
    on_replay_error_();
    CLOG_LOG(ERROR, "submit_log_task is NULL", KPC(submit_task), KR(ret));
  } else if (OB_ISNULL(replay_status = submit_task->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    on_replay_error_();
    CLOG_LOG(ERROR, "replay status is NULL", KPC(submit_task), KPC(replay_status), KR(ret));
  } else if (replay_status->try_rdlock()) {
    const int64_t start_ts = ObClockGenerator::getClock();
    bool need_submit_log = true;
    int64_t count = 0;
    LSN last_batch_to_submit_lsn;
    bool iterate_end_by_replayable_point = false;
    while (OB_SUCC(ret) && need_submit_log && (!is_timeslice_run_out)) {
      int64_t log_size = 0;
      LSN to_submit_lsn;
      SCN to_submit_scn;
      if (!replay_status->is_enabled_without_lock() || !replay_status->need_submit_log()) {
        need_submit_log = false;
      } else {
        const SCN &replayable_point = replayable_point_.atomic_load();
        need_submit_log = submit_task->has_remained_submit_log(replayable_point,
                                                               iterate_end_by_replayable_point);
        if (!need_submit_log) {
        } else if (OB_SUCC(fetch_and_submit_single_log_(*replay_status, submit_task, to_submit_lsn,
                                                        to_submit_scn, log_size))) {
          count++;
          if (!last_batch_to_submit_lsn.is_valid()) {
            last_batch_to_submit_lsn = to_submit_lsn;
          } else if ((0 == (count & (BATCH_PUSH_REPLAY_TASK_COUNT_THRESOLD - 1)))
                     || ((to_submit_lsn - last_batch_to_submit_lsn) > BATCH_PUSH_REPLAY_TASK_SIZE_THRESOLD)) {
            if (OB_SUCCESS !=(tmp_ret = replay_status->batch_push_all_task_queue())) {
              CLOG_LOG(ERROR, "failed to batch_push_all_task_queue", KR(tmp_ret), KPC(replay_status));
            } else {
              last_batch_to_submit_lsn = to_submit_lsn;
            }
          }
          if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
            CLOG_LOG(INFO, "succ to submit log task to replay service", K(to_submit_lsn), K(to_submit_scn),
                       KPC(replay_status));
          }
        } else if (OB_EAGAIN == ret) {
          // do nothing
        } else {
          CLOG_LOG(WARN, "failed to fetch and submit single log", K(to_submit_lsn), KPC(replay_status), K(ret));
        }
      }
      if (OB_SUCCESS != ret) {
        submit_task->set_simple_err_info(ret, ObClockGenerator::getClock());
      } else {
        submit_task->clear_err_info(ObClockGenerator::getClock());
      }
      //To avoid a single task occupying too much thread time, set the upper limit of single
      //occupancy time to 10ms
      int64_t used_time = ObClockGenerator::getClock() - start_ts;
      if (OB_SUCC(ret) && used_time > MAX_SUBMIT_TIME_PER_ROUND) {
        is_timeslice_run_out = true;
      }
    // end while
    };
    if (OB_SUCCESS !=(tmp_ret = replay_status->batch_push_all_task_queue())) {
      CLOG_LOG(ERROR, "failed to batch_push_all_task_queue", KR(tmp_ret), KPC(replay_status));
    }
    replay_status->unlock();
  } else {
    //return OB_EAGAIN to avoid taking up worker threads
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(INFO, "try lock failed", "replay_status", *replay_status, K(ret));
    }
  }
  return ret;
}


int ObLogReplayService::handle_replay_task_(ObReplayServiceReplayTask *task_queue,
                                            bool &is_timeslice_run_out)
{
  int ret = OB_SUCCESS;
  bool is_queue_empty = false;
  ObReplayStatus *replay_status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_ISNULL(task_queue)) {
    ret = OB_ERR_UNEXPECTED;
    on_replay_error_();
    CLOG_LOG(ERROR, "replay task is NULL", KPC(task_queue), KR(ret));
  } else if (OB_ISNULL(replay_status = task_queue->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    on_replay_error_();
    CLOG_LOG(ERROR, "replay status is NULL", KPC(task_queue), KPC(replay_status), KR(ret));
  } else {
    int64_t start_ts = ObTimeUtility::fast_current_time();
    do {
      int64_t replay_task_used = 0;
      int64_t destroy_task_used = 0;
      ObLink *link = NULL;
      ObLink *link_to_destroy = NULL;
      ObLogReplayTask *replay_task = NULL;
      ObLogReplayTask *replay_task_to_destroy = NULL;
      if (replay_status->try_rdlock()) {
        if (!replay_status->is_enabled_without_lock()) {
          is_queue_empty = true;
        } else if (NULL == (link = task_queue->top())) {
          //queue is empty
          ret = OB_SUCCESS;
          is_queue_empty = true;
          task_queue->clear_err_info();
        } else if (OB_ISNULL(replay_task = static_cast<ObLogReplayTask *>(link))) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "replay_task is NULL", KPC(replay_status), K(ret));
        } else if (OB_FAIL(do_replay_task_(replay_task, replay_status, task_queue->idx()))) {
          (void)process_replay_ret_code_(ret, *replay_status, *task_queue, *replay_task);
        } else if (OB_FAIL(statistics_replay_cost_(replay_task->init_task_ts_, replay_task->first_handle_ts_))) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "do statistics replay cost failed", KPC(replay_task), K(ret));
        } else if (OB_ISNULL(link_to_destroy = task_queue->pop())) {
          CLOG_LOG(ERROR, "failed to pop task after replay", KPC(replay_task), K(ret));
          //It's impossible to get to this branch. Use on_replay_error to defend it.
          on_replay_error_(*replay_task, ret);
        } else if (OB_ISNULL(replay_task_to_destroy = static_cast<ObLogReplayTask *>(link_to_destroy))) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "replay_task_to_destroy is NULL when pop after replay", KPC(replay_task), K(ret));
          //It's impossible to get to this branch. Use on_replay_error to defend it.
          on_replay_error_(*replay_task, ret);
        } else {
          task_queue->clear_err_info();
          if (!replay_task->is_pre_barrier_) {
            //前向barrier日志执行回放的线程会提前释放内存
            replay_status->dec_pending_task(replay_task->log_size_);
          }
          free_replay_task(replay_task_to_destroy);
          //To avoid a single task occupies too long thread time, the upper limit of
          //single occupancy time is set to 10ms
          int64_t used_time = ObTimeUtility::fast_current_time() - start_ts;
          if (used_time > MAX_REPLAY_TIME_PER_ROUND) {
            is_timeslice_run_out = true;
          }
        }
        replay_status->unlock();
      } else {
        //write lock is locked, it may be that the parition is being cleaned up.
        //Just return OB_EAGAIN to aviod occuping worker threads
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          CLOG_LOG(INFO, "try lock failed", KPC(replay_status), K(ret));
        }
      }
    } while (OB_SUCC(ret) && (!is_queue_empty) && (!is_timeslice_run_out));
  }
  return ret;
}

int ObLogReplayService::submit_log_replay_task_(ObLogReplayTask &replay_task,
                                                ObReplayStatus &replay_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t replay_hint = 0;
  if (OB_UNLIKELY(!replay_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguemnts", K(replay_task), K(replay_status), K(ret));
  } else {
    // need reset info when push failed
    replay_status.inc_pending_task(replay_task.log_size_);
    if (replay_task.is_post_barrier_) {
      replay_status.set_post_barrier_submitted(replay_task.lsn_);
    }
    if (OB_SUCC(replay_status.push_log_replay_task(replay_task))) {
      CLOG_LOG(TRACE, "push_log_replay_task success", K(replay_status), K(ret));
    } else {
      // push error, dec pending count now
      if (replay_task.is_post_barrier_) {
        if (OB_SUCCESS != (tmp_ret = replay_status.set_post_barrier_finished(replay_task.lsn_))) {
          if (!replay_status.is_fatal_error(ret)) {
            ret = tmp_ret;
          }
          CLOG_LOG(ERROR, "revoke post barrier failed", K(replay_task), K(replay_status), K(ret), K(tmp_ret));
        }
      }
      replay_status.dec_pending_task(replay_task.log_size_);
    }
  }
  return ret;
}

void ObLogReplayService::statistics_submit_(const int64_t single_submit_task_used,
                                           const int64_t log_size,
                                           const int64_t log_count)
{
  RLOCAL(int64_t, SUBMIT_TASK_USED);
  RLOCAL(int64_t, SUBMIT_LOG_SIZE);
  RLOCAL(int64_t, SUBMIT_LOG_COUNT);
  RLOCAL(int64_t, TASK_COUNT);

  SUBMIT_TASK_USED += single_submit_task_used;
  SUBMIT_LOG_SIZE += log_size;
  SUBMIT_LOG_COUNT += log_count;
  TASK_COUNT++;

  if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    CLOG_LOG(INFO, "handle submit task statistics",
               "avg_submit_task_used", SUBMIT_TASK_USED / (TASK_COUNT + 1),
               "avg_submit_task_log_size", SUBMIT_LOG_SIZE / (TASK_COUNT + 1),
               "avg_submit_task_log_count", SUBMIT_LOG_COUNT / (TASK_COUNT + 1),
               "avg_log_size", SUBMIT_LOG_SIZE / (SUBMIT_LOG_COUNT + 1),
               K(*(&SUBMIT_LOG_SIZE)), K(*(&SUBMIT_LOG_COUNT)), K(*(&TASK_COUNT)));
    SUBMIT_TASK_USED= 0;
    SUBMIT_LOG_SIZE = 0;
    TASK_COUNT = 0;
  }
}

int ObLogReplayService::statistics_replay_cost_(const int64_t init_task_time,
                                                 const int64_t first_handle_time)
{
  int ret = OB_SUCCESS;
  int64_t handle_finish_time = ObTimeUtility::fast_current_time();
  int64_t wait_cost_time = first_handle_time - init_task_time;
  int64_t replay_cost_time = handle_finish_time - first_handle_time;
  wait_cost_stat_.stat(wait_cost_time);
  replay_cost_stat_.stat(replay_cost_time);
  return ret;
}

void ObLogReplayService::statistics_replay_(const int64_t single_replay_task_used,
                                          const int64_t single_destroy_task_used,
                                          const int64_t retry_count)
{
  RLOCAL(int64_t, REPLAY_TASK_USED);
  RLOCAL(int64_t, RETIRE_TASK_USED);
  RLOCAL(int64_t, RETRY_COUNT);
  RLOCAL(int64_t, TASK_COUNT);

  REPLAY_TASK_USED += single_replay_task_used;
  RETIRE_TASK_USED += single_destroy_task_used;
  RETRY_COUNT += retry_count;
  TASK_COUNT++;

  if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    CLOG_LOG(INFO, "handle replay task statistics",
               "avg_replay_task_used", REPLAY_TASK_USED / (TASK_COUNT + 1),
               "avg_retire_task_used", RETIRE_TASK_USED / (TASK_COUNT + 1),
               "avg_retry_count", RETRY_COUNT / (TASK_COUNT + 1),
               "task_count", *(&TASK_COUNT));
    REPLAY_TASK_USED = 0;
    RETIRE_TASK_USED = 0;
    RETRY_COUNT = 0;
    TASK_COUNT = 0;
  }
}

void ObLogReplayService::on_replay_error_(ObLogReplayTask &replay_task, int ret_code)
{
  const int64_t error_moment = ObTimeUtility::fast_current_time();
  while (is_inited_) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG_RET(ERROR, ret_code, "REPLAY_ERROR", K(ret_code), K(replay_task), K(error_moment));
    }
    ob_usleep(1 * 1000 * 1000); // sleep 1s
  }
}

void ObLogReplayService::on_replay_error_()
{
  const int64_t error_moment = ObTimeUtility::fast_current_time();
  while (is_inited_) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG_RET(ERROR, OB_ERROR, "REPLAY_ERROR", K(error_moment));
    }
    ob_usleep(1 * 1000 * 1000); // sleep 1s
  }
}

int ObLogReplayService::remove_all_ls_()
{
  int ret = OB_SUCCESS;
  RemoveReplayStatusFunctor functor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not inited", K(ret));
  } else if (OB_FAIL(replay_status_map_.for_each(functor))) {
    CLOG_LOG(WARN, "failed to remove log stream", K(ret));
  } else {
    CLOG_LOG(INFO, "replay service remove all ls", K(ret));
  }
  return ret;
}

int ObLogReplayService::diagnose(const share::ObLSID &id,
                                 ReplayDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  ObReplayStatus *replay_status = NULL;
  ObReplayStatusGuard guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "replay service not init", K(ret));
  } else if (OB_FAIL(get_replay_status_(id, guard))) {
    CLOG_LOG(WARN, "guard get replay status failed", K(ret), K(id));
  } else if (NULL == (replay_status = guard.get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is not exist", K(ret), K(id));
  } else if (OB_FAIL(replay_status->diagnose(diagnose_info))) {
    CLOG_LOG(WARN, "replay status enable failed", K(ret), K(id));
  }
  return ret;
}

bool ObLogReplayService::GetReplayStatusFunctor::operator()(const share::ObLSID &id,
                                                            ObReplayStatus *replay_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is NULL", K(id), KR(ret));
  } else {
    guard_.set_replay_status(replay_status);
  }
  ret_code_ = ret;
  return OB_SUCCESS == ret;
}

bool ObLogReplayService::RemoveReplayStatusFunctor::operator()(const share::ObLSID &id,
                                                               ObReplayStatus *replay_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is NULL", K(id), KR(ret));
  } else if (OB_FAIL(replay_status->disable())) {
    CLOG_LOG(WARN, "failed to disable replay status", K(ret), K(id), KPC(replay_status));
  }
  if (OB_SUCCESS == ret) {
    if (0 == replay_status->dec_ref()) {
      CLOG_LOG(INFO, "free replay status", KPC(replay_status));
      replay_status->~ObReplayStatus();
      mtl_free(replay_status);
    }
  }
  ret_code_ = ret;
  return OB_SUCCESS == ret;
}

bool ObLogReplayService::StatReplayProcessFunctor::operator()(const share::ObLSID &id,
                                                              ObReplayStatus *replay_status)
{
  int ret = OB_SUCCESS;
  int64_t replayed_log_size = 0;
  int64_t unreplayed_log_size = 0;
  if (OB_ISNULL(replay_status)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is NULL", K(id), KR(ret));
  } else if (OB_FAIL(replay_status->get_replay_process(replayed_log_size, unreplayed_log_size))){
    CLOG_LOG(WARN, "get_replay_process failed", K(id), KR(ret), KPC(replay_status));
  } else {
    replayed_log_size_ += replayed_log_size;
    unreplayed_log_size_ += unreplayed_log_size;
    CLOG_LOG(INFO, "get_replay_process success", K(id), K(replayed_log_size), K(unreplayed_log_size));
  }
  ret_code_ = ret;
  return true;
}

bool ObLogReplayService::FetchLogFunctor::operator()(const share::ObLSID &id,
                                                     ObReplayStatus *replay_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay status is NULL", K(id), KR(ret));
  } else if (OB_FAIL(replay_status->trigger_fetch_log())) {
    CLOG_LOG(WARN, "failed to trigger fetch log", K(ret), K(id), KPC(replay_status));
  }
  ret_code_ = ret;
  return OB_SUCCESS == ret;
}
} // namespace replayService
} // namespace oceanbase
