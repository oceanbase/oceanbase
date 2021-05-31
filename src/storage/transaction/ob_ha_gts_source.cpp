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
#include "ob_ha_gts_source.h"
#include "ob_gts_rpc.h"
#include "ob_gts_worker.h"
#include "ob_gts_mgr.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_trans_part_ctx.h"
#include "ob_location_adapter.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace share;

namespace transaction {

////////////////////////ObGtsSource///////////////////////////////////
void ObHaGtsSource::reset()
{
  is_inited_ = false;
  tenant_id_ = 0;
  gts_local_cache_.reset();
  server_.reset();
  location_adapter_ = NULL;
  for (int64_t i = 0; i < TOTAL_GTS_QUEUE_COUNT; ++i) {
    queue_[i].reset();
  }
}

int ObHaGtsSource::init(const uint64_t tenant_id, const ObAddr& server, ObILocationAdapter* location_adapter)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || OB_ISNULL(location_adapter)) {
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), KP(location_adapter));
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
      location_adapter_ = location_adapter;
      gts_statistics_.init(tenant_id);
      is_inited_ = true;
    }
  }

  return ret;
}

void ObHaGtsSource::destroy()
{
  if (is_inited_) {
    for (int64_t i = 0; i < TOTAL_GTS_QUEUE_COUNT; i++) {
      queue_[i].destroy();
    }
    is_inited_ = false;
  }
}

int ObHaGtsSource::get_local_trans_version(ObTsCbTask* task, int64_t& gts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tmp_gts = 0;
  storage::ObPartitionService* ps = static_cast<storage::ObPartitionService*>(GCTX.par_ser_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (OB_SUCCESS == (ret = gts_local_cache_.get_local_trans_version(tmp_gts))) {
    // Able to find a suitable gts value
    gts = tmp_gts;
  } else if (OB_EAGAIN != ret) {
    TRANS_LOG(WARN, "get gts error", K(ret), KP(task));
  } else if (NULL == task) {
    // No need for asynchronous callbacks, report errors directly, and be handled by the caller
    TRANS_LOG(DEBUG, "no need to register callback task", KP(task));
  } else {
    // Generate the latest gts value for the task to enter the queue
    const int64_t queue_index = static_cast<int64_t>(task->hash() % GET_GTS_QUEUE_COUNT);
    ObGTSTaskQueue* queue = &(queue_[queue_index]);
    if (OB_SUCCESS != (tmp_ret = queue->push(task))) {
      // The number of queues is sufficient, so failure is not allowed
      TRANS_LOG(ERROR, "gts task push error", "ret", tmp_ret, KP(task));
      // overwrite retcode
      ret = tmp_ret;
    } else if (OB_SUCCESS != (tmp_ret = ps->trigger_gts())) {
      TRANS_LOG(WARN, "trigger_gts failed", K(tmp_ret));
    }
  }
  gts_statistics_.inc_get_gts_cache_cnt();

  return ret;
}

// Get the value of gts cache, no need to be up-to-date
int ObHaGtsSource::get_gts(ObTsCbTask* task, int64_t& gts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tmp_gts = 0;
  storage::ObPartitionService* ps = static_cast<storage::ObPartitionService*>(GCTX.par_ser_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", KR(ret));
  } else if (OB_SUCCESS == (ret = gts_local_cache_.get_gts(tmp_gts))) {
    // Able to find a suitable gts value
    gts = tmp_gts;
  } else if (OB_EAGAIN != ret) {
    TRANS_LOG(WARN, "get gts error", KR(ret), KP(task));
  } else if (NULL == task) {
    // do nothing
  } else {
    // Generate the latest gts value for the task to enter the queue
    const int64_t queue_index = static_cast<int64_t>(task->hash() % GET_GTS_QUEUE_COUNT);
    ObGTSTaskQueue* queue = &(queue_[queue_index]);
    if (OB_SUCCESS != (tmp_ret = queue->push(task))) {
      // The number of queues is sufficient, so failure is not allowed
      TRANS_LOG(ERROR, "gts task push error", "ret", tmp_ret, KP(task));
      // overwrite retcode
      ret = tmp_ret;
    } else if (OB_SUCCESS != (tmp_ret = ps->trigger_gts())) {
      TRANS_LOG(WARN, "trigger_gts failed", K(tmp_ret));
    }
  }
  gts_statistics_.inc_get_gts_cache_cnt();

  return ret;
}

int ObHaGtsSource::get_gts(const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tmp_gts = 0;
  bool need_send_rpc = false;
  storage::ObPartitionService* ps = static_cast<storage::ObPartitionService*>(GCTX.par_ser_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (!stc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(stc), KP(task));
  } else if (OB_SUCCESS == (ret = gts_local_cache_.get_gts(stc, tmp_gts, receive_gts_ts, need_send_rpc))) {
    // Able to find a suitable gts value
    gts = tmp_gts;
    if (OB_SUCCESS != (tmp_ret = ps->trigger_gts())) {
      TRANS_LOG(WARN, "trigger_gts failed", K(tmp_ret));
    }
  } else if (OB_EAGAIN != ret) {
    TRANS_LOG(WARN, "get gts error", KR(ret), K(stc), KP(task));
  } else if (NULL == task) {
    // No need for asynchronous callbacks, report errors directly, and be handled by the caller
    TRANS_LOG(DEBUG, "no need to register callback task", KP(task));
  } else {
    // If ret is not OB_SUCCESS,
    // it means that an asynchronous task needs to be added to wait for the subsequent gts value
    if (OB_FAIL(ret)) {
      // Generate the latest gts value for the task to enter the queue
      const int64_t queue_index = static_cast<int64_t>(task->hash() % GET_GTS_QUEUE_COUNT);
      ObGTSTaskQueue* queue = &(queue_[queue_index]);
      if (OB_SUCCESS != (tmp_ret = queue->push(task))) {
        // The number of queues is sufficient, so failure is not allowed
        TRANS_LOG(ERROR, "gts task push error", "ret", tmp_ret, KP(task));
        // overwrite retcode
        ret = tmp_ret;
      } else if (OB_SUCCESS != (tmp_ret = ps->trigger_gts())) {
        TRANS_LOG(WARN, "trigger_gts failed", K(tmp_ret));
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

int ObHaGtsSource::get_local_trans_version(
    const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tmp_gts = 0;
  bool need_send_rpc = false;
  storage::ObPartitionService* ps = static_cast<storage::ObPartitionService*>(GCTX.par_ser_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (stc.mts_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(stc), KP(task));
  } else if (OB_SUCCESS ==
             (ret = gts_local_cache_.get_local_trans_version(stc, tmp_gts, receive_gts_ts, need_send_rpc))) {
    // Able to find a suitable gts value
    gts = tmp_gts;
    if (OB_SUCCESS != (tmp_ret = ps->trigger_gts())) {
      TRANS_LOG(WARN, "trigger_gts failed", K(tmp_ret));
    }
  } else if (OB_EAGAIN != ret) {
    TRANS_LOG(WARN, "get gts error", K(ret), K(stc), KP(task));
  } else if (NULL == task) {
    // No need for asynchronous callbacks, report errors directly, and be handled by the caller
    TRANS_LOG(DEBUG, "no need to register callback task", KP(task));
  } else {
    // If ret is not OB_SUCCESS,
    // it means that an asynchronous task needs to be added to wait for the subsequent gts value
    if (OB_FAIL(ret)) {
      // Generate the latest gts value for the task to enter the queue
      const int64_t queue_index = static_cast<int64_t>(task->hash() % GET_GTS_QUEUE_COUNT);
      ObGTSTaskQueue* queue = &(queue_[queue_index]);
      if (OB_SUCCESS != (tmp_ret = queue->push(task))) {
        // The number of queues is sufficient, so failure is not allowed
        TRANS_LOG(ERROR, "gts task push error", "ret", tmp_ret, KP(task));
        // overwrite retcode
        ret = tmp_ret;
      } else if (OB_SUCCESS != (tmp_ret = ps->trigger_gts())) {
        TRANS_LOG(WARN, "trigger_gts failed", K(tmp_ret));
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

int ObHaGtsSource::get_srr(MonotonicTs& srr)
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

int ObHaGtsSource::get_latest_srr(MonotonicTs& latest_srr)
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

int64_t ObHaGtsSource::get_task_count() const
{
  int64_t task_count = 0;
  for (int64_t i = 0; i < TOTAL_GTS_QUEUE_COUNT; i++) {
    task_count += queue_[i].get_task_count();
  }
  return task_count;
}

int ObHaGtsSource::wait_gts_elapse(const int64_t ts, ObTsCbTask* task, bool& need_wait)
{
  int ret = OB_SUCCESS;
  storage::ObPartitionService* ps = static_cast<storage::ObPartitionService*>(GCTX.par_ser_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", KR(ret));
  } else if (0 >= ts || NULL == task) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(ts), KP(task));
  } else {
    int64_t gts = 0;
    bool tmp_need_wait = false;
    ObAddr leader;
    if (OB_FAIL(gts_local_cache_.get_gts(gts))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "get gts failed", KR(ret));
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
      const int64_t index = WAIT_GTS_QUEUE_START_INDEX + task->hash() % WAIT_GTS_QUEUE_COUNT;
      if (TOTAL_GTS_QUEUE_COUNT <= index) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "illegal gts queue index", KR(ret), K(index), KP(task));
      } else if (OB_FAIL(queue_[index].push(task))) {
        TRANS_LOG(ERROR, "wait queue push task failed", KR(ret), KP(task));
      } else if (OB_FAIL(ps->trigger_gts())) {
        TRANS_LOG(WARN, "trigger_gts failed", KR(ret));
      } else {
        TRANS_LOG(DEBUG, "wait queue push task success", KP(task));
      }
    }
    if (OB_SUCCESS == ret) {
      need_wait = tmp_need_wait;
    }
  }

  gts_statistics_.inc_wait_gts_elapse_cnt();

  return ret;
}

int ObHaGtsSource::wait_gts_elapse(const int64_t ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  storage::ObPartitionService* ps = static_cast<storage::ObPartitionService*>(GCTX.par_ser_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", KR(ret));
  } else if (0 >= ts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(ts));
  } else {
    int64_t gts = 0;
    if (OB_FAIL(gts_local_cache_.get_gts(gts))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "get gts failed", KR(ret));
      }
    } else if (ts > gts) {
      ret = OB_EAGAIN;
    } else {
      // do nothing
    }
    if (OB_EAGAIN == ret) {
      if (OB_SUCCESS != (tmp_ret = ps->trigger_gts())) {
        TRANS_LOG(WARN, "trigger_gts failed", K(tmp_ret));
      }
    }
  }
  gts_statistics_.inc_try_wait_gts_elapse_cnt();

  return ret;
}

int ObHaGtsSource::refresh_gts(const bool need_refresh)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else {
    ret = refresh_gts_(need_refresh);
  }
  statistics_();
  if (log_interval_.reach()) {
    TRANS_LOG(INFO, "refresh ha gts", KR(ret), K_(tenant_id), K(need_refresh), K_(gts_local_cache));
  }
  return ret;
}

int ObHaGtsSource::update_base_ts(const int64_t base_ts, const int64_t publish_version)
{
  int ret = OB_SUCCESS;
  if (0 > base_ts || 0 > publish_version) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(base_ts), K(publish_version));
  } else if (OB_FAIL(gts_local_cache_.update_base_ts(base_ts))) {
    TRANS_LOG(WARN, "update base ts failed", KR(ret), K(base_ts));
  } else {
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(
        WARN, "update base ts failed", KR(ret), K_(tenant_id), K(base_ts), K(publish_version), K_(gts_local_cache));
  } else {
    TRANS_LOG(INFO, "update base ts success", K_(tenant_id), K(base_ts), K(publish_version), K_(gts_local_cache));
  }
  return ret;
}

int ObHaGtsSource::get_base_ts(int64_t& base_ts, int64_t& publish_version)
{
  int ret = OB_SUCCESS;
  int64_t tmp_base_ts = 0;
  int64_t tmp_publish_version = 0;
  if (!is_inited_) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(gts_local_cache_.get_gts(tmp_base_ts))) {
  } else if (OB_FAIL(gts_local_cache_.get_gts(tmp_publish_version))) {
  } else {
    base_ts = tmp_base_ts;
    publish_version = tmp_publish_version;
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(WARN, "get base ts failed", KR(ret), K_(tenant_id));
  } else {
    TRANS_LOG(INFO, "get base ts success", K_(tenant_id), K(base_ts), K(publish_version));
  }
  return ret;
}

int ObHaGtsSource::update_publish_version(const int64_t publish_version)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else if (0 > publish_version) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(publish_version));
  } else {
    bool updated = false;
    if (OB_FAIL(gts_local_cache_.update_gts(publish_version, updated))) {
      TRANS_LOG(WARN, "update gts failed", KR(ret), K(publish_version));
    }
    //(void)verify_publish_version_(publish_version);
  }
  return ret;
}

int ObHaGtsSource::get_publish_version(int64_t& publish_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_gts(NULL, publish_version))) {
    if (OB_EAGAIN == ret) {
      // rewrite ret
      ret = OB_GTS_NOT_READY;
    }
  } else {
    // publish_version = publish_version + 10;
    // do nothing
  }
  return ret;
}

void ObHaGtsSource::statistics_()
{
  gts_statistics_.statistics();
}

int ObHaGtsSource::refresh_gts_(const bool need_refresh)
{
  UNUSED(need_refresh);
  int ret = OB_SUCCESS;
  storage::ObPartitionService* ps = static_cast<storage::ObPartitionService*>(GCTX.par_ser_);
  if (NULL == ps) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition service is NULL", KR(ret));
  } else if (OB_FAIL(ps->trigger_gts())) {
    TRANS_LOG(WARN, "trigger gts failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObHaGtsSource::update_gts(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts, bool& update)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else if (!srr.is_valid() || 0 >= gts || !receive_gts_ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts), K(receive_gts_ts));
  } else if (OB_FAIL(gts_local_cache_.update_gts(srr, gts, receive_gts_ts, update))) {
    TRANS_LOG(WARN, "gts local cache update error", KR(ret), K(srr), K(gts), K(update));
  } else {
    //(void)verify_publish_version_(gts);
    TRANS_LOG(DEBUG, "gts local cache update success", K(srr), K(gts));
  }

  return ret;
}

int ObHaGtsSource::update_gts(const int64_t gts, bool& update)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else if (0 >= gts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(gts));
  } else if (OB_FAIL(gts_local_cache_.update_gts(gts, update))) {
    TRANS_LOG(WARN, "gts local cache update error", KR(ret), K(gts), K(update));
  } else {
    //(void)verify_publish_version_(gts);
    TRANS_LOG(DEBUG, "gts local cache update success", K(gts));
  }

  return ret;
}

int ObHaGtsSource::update_local_trans_version(const int64_t version, bool& update)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "not inited");
    ret = OB_NOT_INIT;
  } else if (0 >= version) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(version));
  } else if (OB_FAIL(gts_local_cache_.update_local_trans_version(version, update))) {
    TRANS_LOG(WARN, "local trans version update error", K(ret), K(version), K(update));
  } else {
    //(void)verify_publish_version_(gts);
    TRANS_LOG(DEBUG, "local trans version update success", K(version));
  }

  return ret;
}

int ObHaGtsSource::handle_gts_result(const uint64_t tenant_id, const int64_t queue_index)
{
  int ret = OB_SUCCESS;
  MonotonicTs srr;
  int64_t gts = 0;
  int64_t local_trans_version = 0;
  MonotonicTs receive_gts_ts;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(gts_local_cache_.get_srr_and_gts_safe(srr, gts, local_trans_version, receive_gts_ts))) {
    TRANS_LOG(WARN, "get srr and gts failed", KR(ret));
  } else {
    ObGTSTaskQueue* queue = &(queue_[queue_index]);
    if (OB_FAIL(queue->foreach_task(srr, gts, local_trans_version, receive_gts_ts))) {
      TRANS_LOG(WARN, "iterate task failed", KR(ret), K(queue_index));
    }
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
