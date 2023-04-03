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

#include "ob_replay_handler.h"
#include "logservice/ob_log_service.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
using namespace palf;
using namespace share;
namespace logservice
{
ObReplayHandler::ObReplayHandler(storage::ObLS *ls)
{
  reset();
  ls_ = ls;
}

ObReplayHandler::~ObReplayHandler()
{
  reset();
}

void ObReplayHandler::reset()
{
  for (int i = 0; i < ObLogBaseType::MAX_LOG_BASE_TYPE; i++) {
    handlers_[i] = NULL;
  }
  ls_ = NULL;
}

int ObReplayHandler::register_handler(const ObLogBaseType &type,
                                      ObIReplaySubHandler *handler)
{
  int ret = OB_SUCCESS;

  if (!is_valid_log_base_type(type) || NULL == handler) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(type), K(handler));
  } else {
    WLockGuard guard(lock_);
    handlers_[type] = handler;
  }

  return ret;
}

void ObReplayHandler::unregister_handler(const ObLogBaseType &type)
{
  int ret = OB_SUCCESS;

  if (!is_valid_log_base_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(type));
  } else {
    WLockGuard guard(lock_);
    handlers_[type] = NULL;
  }
}

int ObReplayHandler::replay(const ObLogBaseType &type,
                            const void *buffer,
                            const int64_t nbytes,
                            const palf::LSN &lsn,
                            const SCN &scn)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  if (!is_valid_log_base_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(type));
  } else if (OB_ISNULL(handlers_[type])) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid base_log_type", K(type));
  } else {
    ret = handlers_[type]->replay(buffer, nbytes, lsn, scn);
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase
