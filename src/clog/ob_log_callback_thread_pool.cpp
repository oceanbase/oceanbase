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

#include "ob_log_callback_thread_pool.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/utility.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase {
using namespace common;
namespace clog {

int ObLogCallbackThreadPool::init(int tg_id, ObLogCallbackHandler* handler, const common::ObAddr& self_addr)
{
  UNUSED(self_addr);
  int ret = OB_SUCCESS;
  const ObAdaptiveStrategy adaptive_strategy(LEAST_THREAD_NUM, ESTIMATE_TS, EXPAND_RATE, SHRINK_RATE);
  if (is_inited_) {
    CLOG_LOG(WARN, "ObLogCallbackThreadPool init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(handler) || !self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(OB_I(test_a) TG_SET_HANDLER_AND_START(tg_id, *this))) {
    CLOG_LOG(WARN, "ObSimpleThreadPool inited error", K(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_STRATEGY(tg_id, adaptive_strategy))) {
  } else {
    handler_ = handler;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

// void *ObLogCallbackThreadPool::on_begin()
//{
//  //bind_core();
//  return NULL;
//}

void ObLogCallbackThreadPool::destroy()
{
  TG_DESTROY(tg_id_);
  handler_ = NULL;
  is_inited_ = false;
}

void ObLogCallbackThreadPool::handle(void* task)
{
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    CLOG_LOG(WARN, "not init");
    tmp_ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    CLOG_LOG(WARN, "invalid argument", KP(task));
    tmp_ret = OB_INVALID_ARGUMENT;
  } else {
    handler_->handle(task);
  }
  UNUSED(tmp_ret);
}
}  // namespace clog
}  // namespace oceanbase
