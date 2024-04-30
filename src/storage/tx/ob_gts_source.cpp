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

#include "ob_gts_define.h"
#include "ob_gts_source.h"
#include "ob_gts_rpc.h"
//#include "ob_ts_worker.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_service.h"
#include "ob_timestamp_access.h"
#include "ob_location_adapter.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace transaction
{
/////////////////////Implementation of ObGtsStatistics/////////////////////////
void ObGtsStatistics::reset()
{
  tenant_id_ = 0;
  last_stat_ts_ = 0;
  gts_rpc_cnt_ = 0;
  get_gts_cache_cnt_ = 0;
  get_gts_with_stc_cnt_ = 0;
  try_get_gts_cache_cnt_ = 0;
  try_get_gts_with_stc_cnt_ = 0;
  wait_gts_elapse_cnt_ = 0;
  try_wait_gts_elapse_cnt_ = 0;
}

int ObGtsStatistics::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  last_stat_ts_ = ObTimeUtility::current_time();
  tenant_id_ = tenant_id;

  return ret;
}

void ObGtsStatistics::statistics()
{
  const int64_t cur_ts = ObTimeUtility::current_time();
  const int64_t last_stat_ts = ATOMIC_LOAD(&last_stat_ts_);
  if (cur_ts - last_stat_ts >= STAT_INTERVAL) {
    if (ATOMIC_BCAS(&last_stat_ts_, last_stat_ts, cur_ts)) {
      TRANS_LOG(INFO, "gts statistics",
                      K_(tenant_id),
                      "gts_rpc_cnt", ATOMIC_LOAD(&gts_rpc_cnt_),
                      "get_gts_cache_cnt", ATOMIC_LOAD(&get_gts_cache_cnt_),
                      "get_gts_with_stc_cnt", ATOMIC_LOAD(&get_gts_with_stc_cnt_),
                      "try_get_gts_cache_cnt", ATOMIC_LOAD(&try_get_gts_cache_cnt_),
                      "try_get_gts_with_stc_cnt", ATOMIC_LOAD(&try_get_gts_with_stc_cnt_),
                      "wait_gts_elapse_cnt", ATOMIC_LOAD(&wait_gts_elapse_cnt_),
                      "try_wait_gts_elapse_cnt", ATOMIC_LOAD(&try_wait_gts_elapse_cnt_));
      ATOMIC_STORE(&gts_rpc_cnt_, 0);
      ATOMIC_STORE(&get_gts_cache_cnt_, 0);
      ATOMIC_STORE(&get_gts_with_stc_cnt_, 0);
      ATOMIC_STORE(&try_get_gts_cache_cnt_, 0);
      ATOMIC_STORE(&try_get_gts_with_stc_cnt_, 0);
      ATOMIC_STORE(&wait_gts_elapse_cnt_, 0);
      ATOMIC_STORE(&try_wait_gts_elapse_cnt_, 0);
    }
  }

}

////////////////////////Implementation of ObGtsSource///////////////////////////////////
void ObGtsSource::reset()
{
  is_inited_ = false;
  tenant_id_ = 0;
  gts_local_cache_.reset();
  server_.reset();
  gts_request_rpc_ = NULL;
  location_adapter_ = NULL;
  for (int64_t i = 0; i < TOTAL_GTS_QUEUE_COUNT; ++i) {
    queue_[i].reset();
  }
  gts_cache_leader_.reset();
}


int ObGtsSource::init(const uint64_t tenant_id, const ObAddr &server,
                      ObIGtsRequestRpc *gts_request_rpc, ObILocationAdapter *location_adapter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) ||
             OB_UNLIKELY(is_virtual_tenant_id(tenant_id)) ||
             OB_UNLIKELY(!server.is_valid()) ||
             OB_ISNULL(gts_request_rpc) ||
             OB_ISNULL(location_adapter)) {
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server),
        KP(gts_request_rpc), KP(location_adapter));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < TOTAL_GTS_QUEUE_COUNT; ++i) {
      if (i < GET_GTS_QUEUE_COUNT) {
        if (OB_FAIL(queue_[i].init(GET_GTS))) {
          TRANS_LOG(WARN, "gts queue init error", KR(ret));
        }
      } else if (i < GET_GTS_QUEUE_COUNT + WAIT_GTS_QUEUE_COUNT) {
        if (OB_FAIL(queue_[i].init(WAIT_GTS_ELAPSING))) {
          TRANS_LOG(WARN, "wait gts elapsing queue init error", KR(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpect error", KR(ret), K(i));
      }
    }
    if (OB_SUCCESS == ret) {
      tenant_id_ = tenant_id;
      server_ = server;
      gts_request_rpc_ = gts_request_rpc;
      location_adapter_ = location_adapter;
      gts_statistics_.init(tenant_id);
      is_inited_ = true;
      TRANS_LOG(INFO, "gts source init success", K(tenant_id), K(server), KP(this));
    }
  }

  return ret;
}

void ObGtsSource::destroy()
{
  if (is_inited_) {
    for (int64_t i = 0; i < TOTAL_GTS_QUEUE_COUNT; i++) {
      queue_[i].destroy();
    }
    is_inited_ = false;
  }
}

//Get the value of gts cache, no need to be up-to-date
int ObGtsSource::get_gts(ObTsCbTask *task, int64_t &gts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tmp_gts = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", KR(ret));
  } else if (OB_SUCCESS == (ret = gts_local_cache_.get_gts(tmp_gts))) {
    //Able to find a suitable gts value
    gts = tmp_gts;
  } else if (OB_EAGAIN != ret) {
    TRANS_LOG(WARN, "get gts error", KR(ret), KP(task));
  } else if (NULL == task) {
    // do nothing
  } else {
    //Generate the latest gts value of the task into the queue
    const int64_t queue_index = static_cast<int64_t>(task->hash() % GET_GTS_QUEUE_COUNT);
    ObGTSTaskQueue *queue = &(queue_[queue_index]);
    if (OB_SUCCESS != (tmp_ret = queue->push(task))) {
      //The number of queues is sufficient, so failure is not allowed
      TRANS_LOG(ERROR, "gts task push error", "ret", tmp_ret, KP(task));
      //overwrite retcode
      ret = tmp_ret;
    } else {
      const bool need_refresh_gts_location = false;
      if (OB_SUCCESS != (tmp_ret = refresh_gts_(need_refresh_gts_location))) {
        if (EXECUTE_COUNT_PER_SEC(16)) {
          TRANS_LOG(WARN, "refresh gts failed", K(tmp_ret));
        }
      }
    }
  }
  gts_statistics_.inc_get_gts_cache_cnt();

  return ret;
}

int ObGtsSource::get_gts(const MonotonicTs stc,
                         ObTsCbTask *task,
                         int64_t &gts,
                         MonotonicTs &receive_gts_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tmp_gts = 0;
  bool need_send_rpc = false;
  ObAddr leader;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(!stc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(stc), KP(task));
  } else if (OB_SUCCESS == (ret = gts_local_cache_.get_gts(stc,
                                                           tmp_gts,
                                                           receive_gts_ts,
                                                           need_send_rpc))) {
    //Able to find a suitable gts value
    gts = tmp_gts;
  } else if (OB_UNLIKELY(OB_EAGAIN != ret)) {
    TRANS_LOG(WARN, "get gts error", KR(ret), K(stc), KP(task));
  } else {
    TRANS_LOG(DEBUG, "query_gts", KR(ret), K(need_send_rpc), K(stc),
              K(gts_local_cache_.get_latest_srr()));
    // When getting gts, if the global timestamp service is locally, get gts directly
    if (OB_SUCCESS != (tmp_ret = get_gts_leader_(leader))) {
      TRANS_LOG(WARN, "get gts leader fail", K(tmp_ret), K_(tenant_id));
      (void)refresh_gts_location_();
    } else if (leader == server_) {
      MTL_SWITCH(tenant_id_) {
        ret = OB_EAGAIN;
        // When getting gts, if the global timestamp service is locally, get gts directly
        // Here the error code is overwritten by the result of the local call
        if (OB_SUCCESS != (tmp_ret = get_gts_from_local_timestamp_service_(leader, gts, receive_gts_ts))) {
          if (OB_EAGAIN != tmp_ret) {
            if (EXECUTE_COUNT_PER_SEC(16)) {
              TRANS_LOG(WARN, "get_gts_from_local_timestamp_service fail", K(leader), K_(server), K(tmp_ret));
            }
            if (OB_GTS_NOT_READY != tmp_ret) {
              refresh_gts_location();
            }
          }
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        ret = OB_EAGAIN;
        refresh_gts_location();
      }
    } else {
      // If not in local, refresh gts
      if (need_send_rpc) {
        if (OB_SUCCESS != (tmp_ret = query_gts_(leader))) {
          TRANS_LOG(WARN, "query gts fail", K(tmp_ret), K(leader));
        }
      }
      TRANS_LOG(DEBUG, "after query gts", KR(tmp_ret), K(leader), K(need_send_rpc));
    }
    // If ret is not OB_SUCCESS, it means that an asynchronous task needs to be added to wait for the subsequent gts value
    if (OB_FAIL(ret) && NULL != task) {
      // Generate a task to enter the queue, waiting for the latest gts value
      const int64_t queue_index = static_cast<int64_t>(task->hash() % GET_GTS_QUEUE_COUNT);
      ObGTSTaskQueue *queue = &(queue_[queue_index]);
      if (OB_SUCCESS != (tmp_ret = queue->push(task))) {
        //The number of queues is sufficient, so failure is not allowed
        TRANS_LOG(ERROR, "gts task push error", "ret", tmp_ret, KP(task));
        //overwrite retcode
        ret = tmp_ret;
      } else {
        if (EXECUTE_COUNT_PER_SEC(1)) {
          TRANS_LOG(INFO, "push gts task success", K(*task));
        }
      }
    }
  }

  gts_statistics_.inc_get_gts_with_stc_cnt();

  return ret;
}

// Get the timestamp from the local timestamp service
int ObGtsSource::get_gts_from_local_timestamp_service_(ObAddr &leader,
                                                       int64_t &gts,
                                                       MonotonicTs &receive_gts_ts)
{
  int ret = OB_SUCCESS;
  int64_t tmp_gts = 0;
  const MonotonicTs cur_ts = MonotonicTs::current_time();

  ObTimestampAccess *timestamp_access = MTL(ObTimestampAccess *);
  if (OB_ISNULL(timestamp_access)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "timestamp access is null", KR(ret), KP(timestamp_access), K_(tenant_id), K(leader));
  } else if (OB_FAIL(timestamp_access->get_number(tmp_gts))) {
    if (EXECUTE_COUNT_PER_SEC(100)) {
      TRANS_LOG(WARN, "global_timestamp_service get gts fail", K(leader), K(tmp_gts), KR(ret));
    }
    if (OB_NOT_MASTER == ret) {
        gts_cache_leader_.reset();
    }
  } else {
    if (gts_cache_leader_ != leader) {
      gts_cache_leader_ = leader;
    }
    if (OB_FAIL(gts_local_cache_.update_gts_and_check_barrier(cur_ts,
                                                              tmp_gts,
                                                              cur_ts))) {
      TRANS_LOG(WARN, "update gts fail", K(cur_ts), K(gts), K(receive_gts_ts), KR(ret));
    } else {
      // get gts success, need to overwrite the OB_EAGAIN error code to OB_SUCCESS
      gts = tmp_gts;
      receive_gts_ts = cur_ts;
    }
  }

  return ret;
}

int ObGtsSource::get_gts_from_local_timestamp_service_(ObAddr &leader,
                                                       int64_t &gts)
{
  MonotonicTs unused_receive_gts_ts;
  return get_gts_from_local_timestamp_service_(leader, gts, unused_receive_gts_ts);
}

int ObGtsSource::get_srr(MonotonicTs &srr)
{
  int ret = OB_SUCCESS;
  MonotonicTs tmp_srr;
  if ((tmp_srr = gts_local_cache_.get_srr()) < MonotonicTs(0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "gts local cache srr less than 0", KR(ret), K(tmp_srr));
  } else {
    srr = tmp_srr;
  }
  return ret;
}

int ObGtsSource::get_latest_srr(MonotonicTs &latest_srr)
{
  int ret = OB_SUCCESS;
  MonotonicTs tmp_latest_srr;
  if ((tmp_latest_srr = gts_local_cache_.get_latest_srr()) < MonotonicTs(0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "gts local cache latest_srr less than 0", KR(ret), K(tmp_latest_srr));
  } else {
    latest_srr = tmp_latest_srr;
  }
  return ret;
}

int64_t ObGtsSource::get_task_count() const
{
  int64_t task_count = 0;
  for (int64_t i = 0; i < TOTAL_GTS_QUEUE_COUNT; i++) {
    task_count += queue_[i].get_task_count();
  }
  return task_count;
}

int ObGtsSource::gts_callback_interrupted(const int errcode, const share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  int64_t task_count = 0;
  for (int64_t i = 0; i < TOTAL_GTS_QUEUE_COUNT; i++) {
    queue_[i].gts_callback_interrupted(errcode, ls_id);
    task_count += queue_[i].get_task_count();
  }
  if (OB_LS_OFFLINE != errcode) {
    // in this case, all callbacck tasks of this tenant need to be cleared.
    if (task_count > 0) {
      ret = OB_EAGAIN;
    }
  } else {
    // if OB_LS_OFFLINE, return OB_SUCCESS
  }
  return ret;
}

int ObGtsSource::wait_gts_elapse(const int64_t ts, ObTsCbTask *task, bool &need_wait)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(0 >= ts) || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(ts), KP(task));
  } else {
    int64_t gts = 0;
    bool tmp_need_wait = false;
    ObAddr leader;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(gts_local_cache_.get_gts(gts))) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        TRANS_LOG(WARN, "get gts failed", K(ret));
      } else {
        tmp_need_wait = true;
        // rewrite ret
        ret = OB_SUCCESS;
      }
    } else if (ts > gts) {
      tmp_need_wait = true;
    } else {
      tmp_need_wait = false;
    }
    if (OB_SUCCESS == ret && tmp_need_wait) {
      // When getting gts, if the global timestamp service is locally, get gts directly
      if (OB_SUCCESS != (tmp_ret = get_gts_leader_(leader))) {
        TRANS_LOG(WARN, "get gts leader fail", K(tmp_ret), K_(tenant_id));
        (void)refresh_gts_location_();
      } else if (leader == server_) {
        // When getting gts, if the global timestamp service is locally, get gts directly
        if (OB_SUCCESS != (tmp_ret = get_gts_from_local_timestamp_service_(leader, gts))) {
          if (OB_EAGAIN != tmp_ret && EXECUTE_COUNT_PER_SEC(100)) {
            TRANS_LOG(WARN, "get_gts_from_local_timestamp_service fail", K(leader), K_(server), K(tmp_ret));
          }
        } else if (ts <= gts) {
          tmp_need_wait = false;
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }
    }
    if (OB_SUCCESS == ret && tmp_need_wait) {
      const int64_t index = WAIT_GTS_QUEUE_START_INDEX + task->hash() % WAIT_GTS_QUEUE_COUNT;
      if (TOTAL_GTS_QUEUE_COUNT <= index) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "illegal gts queue index", KR(ret), K(index), KP(task));
      } else if (OB_FAIL(queue_[index].push(task))) {
        TRANS_LOG(ERROR, "wait queue push task failed", KR(ret), KP(task));
      } else {
        TRANS_LOG(INFO, "wait queue push task success", KP(task));
      }
      if (OB_SUCCESS == ret) {
        // ignore error code
        const bool need_refresh_gts_location = false;
        if (OB_SUCCESS != (tmp_ret = refresh_gts_(need_refresh_gts_location))) {
          if (EXECUTE_COUNT_PER_SEC(16)) {
            TRANS_LOG(WARN, "refresh gts failed", K(tmp_ret), K(need_refresh_gts_location));
          }
        }
      }
    }
    if (OB_SUCCESS == ret) {
      need_wait = tmp_need_wait;
    }
  }

  gts_statistics_.inc_wait_gts_elapse_cnt();

  return ret;
}

int ObGtsSource::wait_gts_elapse(const int64_t ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(0 >= ts)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(ts));
  } else {
    int64_t gts = 0;
    ObAddr leader;
    if (OB_FAIL(gts_local_cache_.get_gts(gts))) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        TRANS_LOG(WARN, "get gts failed", K(ret));
      }
    } else if (ts > gts) {
      ret = OB_EAGAIN;
    } else {
      //do nothing
    }
    // Local call optimization
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      // When getting gts, if the global timestamp service is locally, get gts directly
      if (OB_SUCCESS != (tmp_ret = get_gts_leader_(leader))) {
        TRANS_LOG(WARN, "get gts leader fail", K(tmp_ret), K_(tenant_id));
        (void)refresh_gts_location_();
      } else if (leader == server_) {
        // When getting gts, if the global timestamp service is locally, get gts directly
        if (OB_SUCCESS != (tmp_ret = get_gts_from_local_timestamp_service_(leader, gts))) {
          if (OB_EAGAIN != tmp_ret) {
            TRANS_LOG(WARN, "get_gts_from_local_timestamp_service fail", K(leader), K_(server), K(tmp_ret));
          }
        } else if (ts <= gts) {
          ret = OB_SUCCESS;
        } else {
          // do nothing
        }
      } else {
        // If the leader is not in local, gts needs to be refreshed
        if (OB_SUCCESS != (tmp_ret = query_gts_(leader))) {
          TRANS_LOG(WARN, "refresh gts failed", K(tmp_ret));
        }
      }
    }
  }
  gts_statistics_.inc_try_wait_gts_elapse_cnt();

  return ret;
}

int ObGtsSource::refresh_gts(const bool need_refresh)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else {
    ret = refresh_gts_(need_refresh);
  }
  statistics_();
  if (log_interval_.reach()) {
    TRANS_LOG(INFO, "refresh gts", KR(ret), K_(tenant_id), K(need_refresh), K_(gts_local_cache));
  }
  return ret;
}

int ObGtsSource::get_gts_leader_(ObAddr &leader)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_GET_GTS_LEADER) OB_SUCCESS;
  if (OB_LS_LOCATION_LEADER_NOT_EXIST == ret) {
    ObClockGenerator::msleep(100);
    return ret;
  }
#endif
  if (gts_cache_leader_.is_valid()) {
    leader = gts_cache_leader_;
  } else if (OB_FAIL(location_adapter_->nonblock_get_leader(cluster_id, tenant_id_, GTS_LS, leader))) {
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(WARN, "gts nonblock get leader failed", K(ret), K_(tenant_id), K(GTS_LS));
    }
  } else {
    gts_cache_leader_ = leader;
  }
  if (OB_SUCC(ret) && !leader.is_valid()) {
    ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(WARN, "gts cache leader is invalid", KR(ret), K(leader), K(*this));
    }
  }

  return ret;
}

int ObGtsSource::query_gts_(const ObAddr &leader)
{
  int ret = OB_SUCCESS;
  ObGtsRequest msg;
  const int64_t ts_range_size = 1;
  const MonotonicTs srr = MonotonicTs::current_time();
  if (OB_FAIL(gts_local_cache_.update_latest_srr(srr))) {
    TRANS_LOG(WARN, "update latest srr error", KR(ret), K_(tenant_id), K(srr));
  } else if (OB_FAIL(msg.init(tenant_id_, srr, ts_range_size, server_))) {
    TRANS_LOG(WARN, "msg init failed", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(gts_request_rpc_->post(tenant_id_, leader, msg))) {
    TRANS_LOG(WARN, "post gts request failed", KR(ret), K(leader), K(msg));
    (void)refresh_gts_location_();
  } else {
    gts_statistics_.inc_gts_rpc_cnt();
    TRANS_LOG(DEBUG, "post gts request success", K(srr), K_(gts_local_cache));
  }
  return ret;
}

int ObGtsSource::refresh_gts_location_()
{
  int ret = OB_SUCCESS;
  gts_cache_leader_.reset();
  if (refresh_location_interval_.reach()) {
    const int64_t cluster_id = GCONF.cluster_id;
    if (OB_FAIL(location_adapter_->nonblock_renew(cluster_id, tenant_id_, GTS_LS))) {
      TRANS_LOG(WARN, "gts nonblock renew error", KR(ret), K(GTS_LS));
    } else {
      TRANS_LOG(INFO, "gts nonblock renew success", K(ret), K_(tenant_id), K_(gts_local_cache));
    }
  }
  return ret;
}

int ObGtsSource::refresh_gts_(const bool need_refresh)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  bool need_refresh_gts_location = need_refresh;

  if (OB_FAIL(get_gts_leader_(leader))) {
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(WARN, "get gts leader failed", KR(ret), K_(tenant_id));
    }
    need_refresh_gts_location = true;
  } else {
    ret = query_gts_(leader);
  }
  if (need_refresh_gts_location) {
    (void)refresh_gts_location_();
  }

  return ret;
}

void ObGtsSource::statistics_()
{
  gts_statistics_.statistics();
}

int ObGtsSource::update_gts(const MonotonicTs srr,
                            const int64_t gts,
                            const MonotonicTs receive_gts_ts,
                            bool &update)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(0 >= gts) || OB_UNLIKELY(!receive_gts_ts.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts), K(receive_gts_ts));
  } else if (OB_FAIL(gts_local_cache_.update_gts(srr, gts, receive_gts_ts, update))) {
    TRANS_LOG(WARN, "gts local cache update error", KR(ret), K(srr), K(gts),
              K(receive_gts_ts), K(update));
  } else {
    TRANS_LOG(DEBUG, "gts local cache update success", K(srr), K(gts));
  }

  return ret;
}

int ObGtsSource::update_gts(const int64_t gts, bool &update)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(0 >= gts)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(gts));
  } else if (OB_FAIL(gts_local_cache_.update_gts(gts, update))) {
    TRANS_LOG(WARN, "gts local cache update error", KR(ret), K(gts), K(update));
  } else {
    TRANS_LOG(DEBUG, "gts local cache update success", K(gts));
  }

  return ret;
}

int ObGtsSource::handle_gts_result(const uint64_t tenant_id, const int64_t queue_index)
{
  int ret = OB_SUCCESS;
  MonotonicTs srr;
  int64_t gts = 0;
  MonotonicTs receive_gts_ts;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(gts_local_cache_.get_srr_and_gts_safe(srr, gts, receive_gts_ts))) {
    TRANS_LOG(WARN, "get srr and gts failed", KR(ret));
  } else {
    ObGTSTaskQueue *queue = &(queue_[queue_index]);
    if (OB_FAIL(queue->foreach_task(srr, gts, receive_gts_ts))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        if (gts_local_cache_.no_rpc_on_road()) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = refresh_gts_(false))) {
            TRANS_LOG(WARN, "refresh gts failed", K(tmp_ret));
          }
        }
      } else {
        TRANS_LOG(WARN, "iterate task failed", KR(ret), K(queue_index));
      }
    }
  }
  return ret;
}

int ObGtsSource::handle_gts_err_response(const ObGtsErrResponse &err_msg)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(!err_msg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(err_msg));
  } else {
    if (OB_NOT_MASTER == err_msg.get_status()) {
      gts_cache_leader_.reset();
      refresh_gts_location_();
    }
  }
  if (EXECUTE_COUNT_PER_SEC(16)) {
    TRANS_LOG(INFO, "handle gts err response", KR(ret), K(err_msg), K(*this));
  }

  return ret;
}

} // transaction
} // oceanbase
