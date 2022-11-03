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

#include "ob_ts_worker.h"
#include "ob_ts_response_handler.h"
#include "ob_ts_mgr.h"
#include "share/ob_thread_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
using namespace common;
using namespace omt;
using namespace observer;

namespace transaction
{
int ObTsWorker::init(ObTsMgr *ts_mgr, const bool use_local_worker)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", KR(ret));
  } else if (NULL == ts_mgr) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(ts_mgr));
  } else if (use_local_worker) {
    if (OB_FAIL(TG_CREATE(lib::TGDefIDs::TSWorker, tg_id_))) {
      TRANS_LOG(WARN, "ObTsWorker tg create failed", K(ret));
    } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
      TRANS_LOG(WARN, "simple thread pool init failed", K(ret));
    } else {
      TRANS_LOG(INFO, "ts worker thread pool init success");
    }
  } else {
    // do nothing
  }
  if (OB_SUCCESS == ret) {
    use_local_worker_ = use_local_worker;
    ts_mgr_ = ts_mgr;
    is_inited_ = true;
    TRANS_LOG(INFO, "ts worker init success", KP(this), KP(ts_mgr), K(use_local_worker));
  } else {
    TRANS_LOG(WARN, "ts worker init failed", KR(ret), KP(this), KP(ts_mgr), K(use_local_worker));
  }
  return ret;
}

void ObTsWorker::stop()
{
  TG_STOP(tg_id_);
}

void ObTsWorker::wait()
{
  TG_WAIT(tg_id_);
}

void ObTsWorker::destroy()
{
  stop();
  wait();
  TG_DESTROY(tg_id_);
}

int ObTsWorker::push_task(const uint64_t tenant_id, ObTsResponseTask *task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ts worker not init", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || NULL == task) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), KP(task));
  } else if (use_local_worker_) {
    if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
      TRANS_LOG(WARN, "push task to local worker failed", K(ret), K(tenant_id), KP(task));
    }
  } else {
    ObMultiTenant *omt = GCTX.omt_;
    if (NULL == omt) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected error, omt is null", KR(ret), KP(omt));
    } else if (OB_FAIL(omt->recv_request(tenant_id, *task))) {
      TRANS_LOG(WARN, "recv request failed", KR(ret), K(tenant_id), KP(task));
    } else {
      // do nothing
    }
  }
  return ret;
}

void ObTsWorker::handle(void *task)
{
  int ret = OB_SUCCESS;
  if (NULL != task) {
    ObTsResponseTask *ts_task = reinterpret_cast<ObTsResponseTask *>(task);
    if (NULL == ts_mgr_) {
      TRANS_LOG(WARN, "ts mgr is NULL", KP_(ts_mgr));
    } else if (OB_FAIL(ts_mgr_->handle_gts_result(ts_task->get_tenant_id(),
                                                  ts_task->get_arg1(),
                                                  ts_task->get_ts_type()))) {
      TRANS_LOG(WARN, "handle gts result failed", KR(ret), K(*ts_task));
    } else {
      TRANS_LOG(DEBUG, "handle gts result success", K(*ts_task));
    }
    ObTsResponseTaskFactory::free(ts_task);
    ts_task = NULL;
  }
}

} // transaction
} // oceanbase
