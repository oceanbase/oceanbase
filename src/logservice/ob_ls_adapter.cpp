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

#include "ob_ls_adapter.h"
#include "replayservice/ob_log_replay_service.h"
#include "replayservice/ob_replay_status.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
using namespace storage;
namespace logservice
{
ObLSAdapter::ObLSAdapter() :
    is_inited_(false),
    ls_service_(NULL)
  {}

ObLSAdapter::~ObLSAdapter()
{
  destroy();
}

int ObLSAdapter::init(ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLSAdapter init twice", K(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ls_service), K(ret));
  } else {
    ls_service_ = ls_service;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLSAdapter init success", K(ret), K(ls_service_));
  }
  return ret;
}

void ObLSAdapter::destroy()
{
  is_inited_ = false;
  ls_service_ = NULL;
}

int ObLSAdapter::replay(ObLogReplayTask *replay_task)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSHandle ls_handle;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLSAdapter not inited", K(ret));
  } else if (OB_FAIL(ls_service_->get_ls(replay_task->ls_id_, ls_handle, ObLSGetMod::ADAPTER_MOD))) {
    CLOG_LOG(ERROR, "get log stream failed", KPC(replay_task), K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, " log stream not exist", KPC(replay_task), K(ret));
  } else if (ObLogBaseType::PADDING_LOG_BASE_TYPE == replay_task->log_type_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "padding log entry can't be replayed, unexpected error", KPC(replay_task));
  } else if (OB_FAIL(ls->replay(replay_task->log_type_,
                                replay_task->get_replay_payload(),
                                replay_task->get_replay_payload_size(),
                                replay_task->lsn_,
                                replay_task->scn_))) {
    CLOG_LOG(WARN, "log stream do replay failed", K(ret), KPC(replay_task));
  }
  if (OB_EAGAIN == ret) {
    if (common::OB_INVALID_TIMESTAMP == replay_task->first_handle_ts_) {
      replay_task->first_handle_ts_ = start_ts;
      replay_task->print_error_ts_ = start_ts;
    } else {
      replay_task->retry_cost_ = start_ts - replay_task->first_handle_ts_;
      if ((start_ts - replay_task->print_error_ts_) > MAX_SINGLE_RETRY_WARNING_TIME_THRESOLD) {
        if (replay_task->retry_cost_ > 100 * 1000 *1000 && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          CLOG_LOG(ERROR, "single replay task retry cost too much time. replay may be delayed",
                   K(ret), KPC(replay_task));
        } else {
          CLOG_LOG(WARN, "single replay task retry cost too much time. replay may be delayed",
                   K(ret), KPC(replay_task));
        }
        replay_task->print_error_ts_ = start_ts;
      }
    }
  }
  replay_task->replay_cost_ = ObTimeUtility::fast_current_time() - start_ts;
  if (replay_task->replay_cost_ > MAX_SINGLE_REPLAY_WARNING_TIME_THRESOLD) {
    const bool is_meta_t = is_meta_tenant(MTL_ID());
    //meta tenant is in mini mode
    if (replay_task->replay_cost_ > MAX_SINGLE_REPLAY_ERROR_TIME_THRESOLD
        && !get_replay_is_writing_throttling()
        && (!lib::is_mini_mode() || is_meta_t)) {
      CLOG_LOG_RET(ERROR, OB_ERR_TOO_MUCH_TIME, "single replay task cost too much time. replay may be delayed", KPC(replay_task));
    } else {
      CLOG_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "single replay task cost too much time", KPC(replay_task));
    }
  }
  return ret;
}

int ObLSAdapter::wait_append_sync(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSHandle ls_handle;
  ObLogHandler *log_handler = NULL;
  if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::ADAPTER_MOD))) {
    CLOG_LOG(WARN, "get log stream failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "log stream not exist", K(ret), K(ls_id));
  } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_handler is NULL", K(ret), K(ls_id));
  } else {
    log_handler->wait_append_sync();
  }
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase
