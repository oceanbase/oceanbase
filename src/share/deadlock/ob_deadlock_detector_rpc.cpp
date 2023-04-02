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

#include "share/ob_occam_time_guard.h"
#include "ob_deadlock_detector_rpc.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_deadlock_detector_mgr.h"
#include "ob_deadlock_parameters.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_message.h"
#include "ob_deadlock_detector_common_define.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace detector;

namespace obrpc
{

using share::detector::ObDeadLockDetectorMgr;

int ObDetectorLCLMessageP::process()
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  ObDeadLockDetectorMgr *p_deadlock_detector_mgr = MTL(ObDeadLockDetectorMgr *);
  if (OB_ISNULL(p_deadlock_detector_mgr)) {
    DETECT_LOG(ERROR, "can not get ObDeadLockDetectorMgr", KP(p_deadlock_detector_mgr));
  } else if (OB_FAIL(p_deadlock_detector_mgr->process_lcl_message(arg_))) {
    DETECT_LOG(WARN, "process lcl message failed", KR(ret), KP(p_deadlock_detector_mgr));
  }
  
  result_ = Int64(ret);
  return ret;
}

int ObDeadLockCollectInfoMessageP::process()
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  ObDeadLockDetectorMgr *p_deadlock_detector_mgr = MTL(ObDeadLockDetectorMgr *);
  if (OB_ISNULL(p_deadlock_detector_mgr)) {
    DETECT_LOG(ERROR, "can not get ObDeadLockDetectorMgr", KP(p_deadlock_detector_mgr));
  } else if (OB_FAIL(p_deadlock_detector_mgr->process_collect_info_message(arg_))) {
    DETECT_LOG(WARN, "process collect info message failed",
               KR(ret), K(arg_));
  } else {
    // do nothing
  }
  
  result_ = Int64(ret);
  return ret;
}

int ObDeadLockNotifyParentMessageP::process()
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  DETECT_LOG(INFO, "receive notify parent msg", K(arg_));
  ObDeadLockDetectorMgr *p_deadlock_detector_mgr = MTL(ObDeadLockDetectorMgr *);
  if (OB_ISNULL(p_deadlock_detector_mgr)) {
    DETECT_LOG(ERROR, "can not get ObDeadLockDetectorMgr", KP(p_deadlock_detector_mgr));
  } else if (OB_FAIL(p_deadlock_detector_mgr->process_notify_parent_message(arg_))) {
    DETECT_LOG(WARN, "process notify parent message failed",
               KR(ret), K(arg_));
  } else {
    // do nothing
  }

  result_ = Int64(ret);
  return ret;
}

}// namespace obrpc

namespace share
{
namespace detector
{

int ObDeadLockDetectorRpc::init(obrpc::ObDetectorRpcProxy *proxy, const common::ObAddr &self)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    DETECT_LOG(WARN, "init twice", KR(ret));
  } else if (nullptr == proxy || false == self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "argument invalid", KR(ret), KP(proxy), K(self));
  } else {
    proxy_ = proxy;
    self_ = self;
    is_inited_ = true;
  }

  return ret;
}

void ObDeadLockDetectorRpc::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
  } else {
    DETECT_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "ObDeadLockDetectorRpc has been destroyed", K(lbt()));
  }
}

int ObDeadLockDetectorRpc::post_lcl_message(const ObAddr &dest_addr,
                                            const ObLCLMessage &msg)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  // DETECT_LOG(INFO, "post lcl msg", K(dest_addr), K(msg));
  if (false == is_inited_) {
    ret = OB_NOT_INIT;
    DETECT_LOG(WARN, "ObDeadLockDetectorRpc not inited", KR(ret));
  } else if (false == msg.is_valid() || false == dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument",
              KR(ret), K(dest_addr), K(msg));
  } else {
    if (OB_FAIL(proxy_->to(dest_addr)
                      .by(MTL_ID())
                      .timeout(OB_DETECTOR_RPC_TIMEOUT)
                      .post_lcl_message(msg, &lcl_msg_cb_))) {
      DETECT_LOG(WARN, "post label request failed",
                KR(ret), K(dest_addr), K(OB_DETECTOR_RPC_TIMEOUT), K(msg));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObDeadLockDetectorRpc::post_collect_info_message(const ObAddr &dest_addr,
                                                     const ObDeadLockCollectInfoMessage &msg)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  DETECT_LOG(INFO, "post collect info msg", K(dest_addr), K(msg));
  if (false == is_inited_) {
    ret = OB_NOT_INIT;
    DETECT_LOG(WARN, "ObDeadLockDetectorRpc not inited", KR(ret));
  } else if (false == msg.is_valid() || false == dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument",
              KR(ret), K(dest_addr), K(msg));
  } else {
    if (OB_FAIL(proxy_->to(dest_addr)
                      .by(MTL_ID())
                      .timeout(OB_DETECTOR_RPC_TIMEOUT)
                      .post_collect_info_message(msg, &collect_msg_cb_))) {
      DETECT_LOG(WARN, "post collect info msg failed",
                KR(ret), K(dest_addr), K(OB_DETECTOR_RPC_TIMEOUT), K(msg));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObDeadLockDetectorRpc::post_notify_parent_message(const ObAddr &dest_addr,
                                                      const ObDeadLockNotifyParentMessage &msg)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  DETECT_LOG(INFO, "post notify parent msg", K(dest_addr), K(msg));
  if (false == is_inited_) {
    ret = OB_NOT_INIT;
    DETECT_LOG(WARN, "ObDeadLockDetectorRpc not inited", KR(ret));
  } else if (false == msg.is_valid() || false == dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "invalid argument",
              KR(ret), K(dest_addr), K(msg));
  } else if (dest_addr == self_) {
    ObDeadLockDetectorMgr *p_deadlock_detector_mgr = MTL(ObDeadLockDetectorMgr *);
    if (OB_ISNULL(p_deadlock_detector_mgr)) {
      DETECT_LOG(ERROR, "can not get ObDeadLockDetectorMgr", KP(p_deadlock_detector_mgr));
    } else if(OB_FAIL(p_deadlock_detector_mgr->process_notify_parent_message(msg))) {
      DETECT_LOG(WARN, "process notify parent message failed", KR(ret), KP(p_deadlock_detector_mgr));
    }
  } else {
    if (OB_FAIL(proxy_->to(dest_addr)
                      .by(MTL_ID())
                      .timeout(OB_DETECTOR_RPC_TIMEOUT)
                      .post_notify_parent_message(msg, &notify_msg_cb_))) {
      DETECT_LOG(WARN, "post notify parent msg failed",
                KR(ret), K(dest_addr), K(OB_DETECTOR_RPC_TIMEOUT), K(msg));
    } else {
      // do nothing
    }
  }

  return ret;
}

}// namespace detector
}// namespace share
}// namespace oceanbase
