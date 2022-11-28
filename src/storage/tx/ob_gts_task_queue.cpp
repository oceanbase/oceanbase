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

#include "share/rc/ob_tenant_base.h"
#include "ob_gts_local_cache.h"
#include "ob_gts_task_queue.h"
#include "ob_ts_mgr.h"
#include "ob_trans_event.h"

namespace oceanbase
{

using namespace common;
using namespace share;

namespace transaction
{

int ObGTSTaskQueue::init(const ObGTSCacheTaskType &type)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "gts task queue init twice", KR(ret), K(type));
  } else if (GET_GTS != type && WAIT_GTS_ELAPSING != type) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid gts task type", KR(ret), K(type));
  } else {
    task_type_ = type;
    is_inited_ = true;
    TRANS_LOG(INFO, "gts task queue init success", KP(this), K(type));
  }
  return ret;
}

void ObGTSTaskQueue::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
  }
}

void ObGTSTaskQueue::reset()
{
  is_inited_ = false;
  task_type_ = INVALID_GTS_TASK_TYPE;
}

int ObGTSTaskQueue::foreach_task(const MonotonicTs srr,
                                 const int64_t gts,
                                 const MonotonicTs receive_gts_ts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (0 >= srr.mts_ || 0 >= gts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts));
  } else {
    int64_t last_tenant_id = OB_INVALID_TENANT_ID;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(ts_guard);
    while (OB_SUCCESS == ret) {
      common::ObLink *data = NULL;
      (void)queue_.pop(data);
      ObTsCbTask *task = static_cast<ObTsCbTask *>(data);
      if (NULL == task) {
        break;
      } else {
        const uint64_t tenant_id = task->get_tenant_id();
        const int64_t request_ts = 0/*task->get_request_ts()*/;
        if (tenant_id != last_tenant_id) {
          if (OB_FAIL(ts_guard.switch_to(tenant_id))) {
            TRANS_LOG(ERROR, "switch tenant failed", K(ret), K(tenant_id));
          } else {
            last_tenant_id = tenant_id;
          }
        }
        if (OB_SUCC(ret)) {
          SCN scn;
          scn.convert_for_gts(gts);
          if (GET_GTS == task_type_) {
            if (OB_FAIL(task->get_gts_callback(srr, scn, receive_gts_ts))) {
              if (OB_EAGAIN != ret) {
                TRANS_LOG(WARN, "get gts callback failed", KR(ret), K(srr), K(gts), KP(task));
              }
            } else {
              TRANS_LOG(DEBUG, "get gts callback success", K(srr), K(gts), KP(task));
            }
          } else if (WAIT_GTS_ELAPSING == task_type_) {
            if (OB_FAIL(task->gts_elapse_callback(srr, scn))) {
              if (OB_EAGAIN != ret) {
                TRANS_LOG(WARN, "gts elapse callback failed", KR(ret), K(srr), K(gts), KP(task));
              }
            } else {
              TRANS_LOG(DEBUG, "gts elapse callback success", K(srr), K(gts), KP(task));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "unknown gts task type", KR(ret), K_(task_type));
          }
          if (OB_EAGAIN == ret) {
            // rewrite ret
            ret = OB_SUCCESS;
            if (OB_FAIL(queue_.push(task))) {
              TRANS_LOG(ERROR, "push gts task failed", KR(ret), KP(task));
            } else {
              TRANS_LOG(DEBUG, "push back gts task", KP(task));
              break;
            }
          } else {
            if (GET_GTS == task_type_) {
              const int64_t total_used = ObTimeUtility::current_time() - request_ts;
              ObTransStatistic::get_instance().add_gts_acquire_total_time(tenant_id, total_used);
              ObTransStatistic::get_instance().add_gts_acquire_total_wait_count(tenant_id, 1);
            } else if (WAIT_GTS_ELAPSING == task_type_) {
              const int64_t total_used = ObTimeUtility::current_time() - request_ts;
              ObTransStatistic::get_instance().add_gts_wait_elapse_total_time(tenant_id, total_used);
              ObTransStatistic::get_instance().add_gts_wait_elapse_total_wait_count(tenant_id, 1);
            } else {
              // do nothing
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObGTSTaskQueue::push(ObTsCbTask *task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", KR(ret));
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(task));
  } else if (OB_FAIL(queue_.push(task))) {
    TRANS_LOG(ERROR, "push gts task failed", K(ret), KP(task));
  } else {
    TRANS_LOG(DEBUG, "push gts task success", KP(task));
  }
  return ret;
}

int ObGTSTaskQueue::gts_callback_interrupted(const int errcode)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t last_tenant_id = OB_INVALID_TENANT_ID;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(ts_guard);
    int64_t count = queue_.size();
    while (OB_SUCC(ret) && count > 0) {
      common::ObLink *data = NULL;
      (void)queue_.pop(data);
      count--;
      ObTsCbTask *task = static_cast<ObTsCbTask *>(data);
      if (NULL == task) {
        break;
      } else {
        const uint64_t tenant_id = task->get_tenant_id();
        if (tenant_id != last_tenant_id) {
          if (OB_FAIL(ts_guard.switch_to(tenant_id))) {
            TRANS_LOG(ERROR, "switch tenant failed", K(ret), K(tenant_id));
          } else {
            last_tenant_id = tenant_id;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(task->gts_callback_interrupted(errcode))) {
            if (OB_EAGAIN != ret) {
              TRANS_LOG(WARN, "gts callback interrupted fail", KR(ret), KP(task));
            } else {
              if (OB_FAIL(queue_.push(task))) {
                TRANS_LOG(ERROR, "push gts task failed", KR(ret), KP(task));
              } else {
                TRANS_LOG(DEBUG, "push back gts task", KP(task));
                break;
              }
            }
          } else {
            TRANS_LOG(DEBUG, "gts callback interrupted success", KP(task));
          }
        }
      }
    }
  }
  return ret;
}

} // transaction
} // oceanbase
