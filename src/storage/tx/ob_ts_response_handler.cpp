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

#define USING_LOG_PREFIX TRANSACTION 

#include "ob_ts_response_handler.h"
#include "ob_ts_mgr.h"

using namespace oceanbase::transaction;
using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::obrpc;

int64_t ObTsResponseTaskFactory::alloc_count_;
int64_t ObTsResponseTaskFactory::free_count_;

void ObTsResponseHandler::reset()
{
  task_ = NULL;
  ts_mgr_ = NULL;
}

int ObTsResponseHandler::init(observer::ObSrvTask *task, ObTsMgr *ts_mgr)
{
  int ret = OB_SUCCESS;

  if (NULL == task || NULL == ts_mgr) {
    ret = OB_INVALID_ARGUMENT;;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(task), KP(ts_mgr));
  } else {
    //Indicates that the task is a task generated internally by the Observer
    task_ = task;
    ts_mgr_ = ts_mgr;
  }

  return ret;
}

int ObTsResponseHandler::run()
{
  int ret = OB_SUCCESS;
  if (NULL == task_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "task is null, unexpected error", KR(ret), KP_(task));
  } else if (NULL == ts_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null, unexpected error", KR(ret), KP_(ts_mgr));
  } else {
    ObTsResponseTask *task = static_cast<ObTsResponseTask *>(task_);
    if (OB_FAIL(ts_mgr_->handle_gts_result(task->get_tenant_id(), task->get_arg1(), task->get_ts_type()))) {
      TRANS_LOG(WARN, "handle gts result failed", KR(ret), K(*task));
    }
    //op_reclaim_free(task);
    //task = NULL;
  }
  return ret;
}

void ObTsResponseTask::reset()
{
  tenant_id_ = 0;
  arg1_ = 0;
  handler_.reset();
  ts_type_ = TS_SOURCE_UNKNOWN;
}

int ObTsResponseTask::init(const uint64_t tenant_id,
                           const int64_t arg1,
                           ObTsMgr *ts_mgr,
                           int ts_type)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(tenant_id)
      || NULL == ts_mgr
      || !is_valid_ts_source(ts_type)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(arg1), KP(ts_mgr), K(ts_type));
  } else if (OB_FAIL(handler_.init(this, ts_mgr))) {
    TRANS_LOG(WARN, "ObTsResponseHandler init error", KR(ret));
  } else {
    //Different from the task of sql disconnection, it is used for memory release
    set_type(ObRequest::OB_TS_TASK);
    arg1_ = arg1;
    tenant_id_ = tenant_id;
    ts_type_ = ts_type;
  }

  return ret;
}

ObTsResponseTask *ObTsResponseTaskFactory::alloc()
{
  ObTsResponseTask *task = NULL;
  if (NULL != (task = op_reclaim_alloc(ObTsResponseTask))) {
    (void)ATOMIC_FAA(&alloc_count_, 1);
    alloc_count_++;
    if (REACH_TIME_INTERVAL(3 * 1000 * 1000)) {
      TRANS_LOG(INFO, "ts response task statistics", K_(alloc_count), K_(free_count));
    }
  }
  return task;
}

void ObTsResponseTaskFactory::free(ObTsResponseTask *task)
{
  if (NULL != task) {
    op_reclaim_free(task);
    task = NULL;
    (void)ATOMIC_FAA(&free_count_, 1);
  }
}

