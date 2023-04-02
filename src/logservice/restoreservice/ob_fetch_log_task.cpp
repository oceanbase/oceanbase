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

#define USING_LOG_PREFIX CLOG
#include "ob_fetch_log_task.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/tx_storage/ob_ls_service.h"           // ObLSService
#include "storage/ls/ob_ls.h"                           // ObLS
#include "ob_log_restore_handler.h"                     // ObLogRestore
#include "ob_log_restore_allocator.h"                  // ObLogRestoreeHandler

namespace oceanbase
{
using namespace palf;
using namespace share;
namespace logservice
{
  auto get_source_func = [](const share::ObLSID &id, ObRemoteSourceGuard &source_guard) -> int {
    int ret = OB_SUCCESS;
    ObLSHandle handle;
    ObLS *ls = NULL;
    ObLogRestoreHandler *restore_handler = NULL;
    if (OB_FAIL(MTL(storage::ObLSService *)->get_ls(id, handle, ObLSGetMod::LOG_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls is NULL", K(ret), K(id), K(ls));
    } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("restore_handler is NULL", K(ret), K(id));
    } else {
      restore_handler->deep_copy_source(source_guard);
      LOG_TRACE("print get_source_", KPC(restore_handler), KPC(source_guard.get_source()));
    }
    return ret;
  };

  auto update_source_func = [](const share::ObLSID &id, ObRemoteLogParent *source) -> int {
    int ret = OB_SUCCESS;
    ObLSHandle handle;
    ObLS *ls = NULL;
    ObLogRestoreHandler *restore_handler = NULL;
    if (OB_ISNULL(source) || ! share::is_valid_log_source_type(source->get_source_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid source type", K(ret), KPC(source));
    } else if (OB_FAIL(MTL(storage::ObLSService *)->get_ls(id, handle, ObLSGetMod::LOG_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls is NULL", K(ret), K(id), K(ls));
    } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("restore_handler is NULL", K(ret), K(id));
    } else if (OB_FAIL(restore_handler->update_location_info(source))) {
      LOG_WARN("update locate info failed", K(ret), KPC(source), KPC(restore_handler));
    }
    return ret;
  };

  auto refresh_storage_info_func = [](share::ObBackupDest &dest) -> int {
    return OB_NOT_SUPPORTED;
  };

ObFetchLogTask::ObFetchLogTask(const share::ObLSID &id,
    const SCN &pre_scn,
    const palf::LSN &lsn,
    const int64_t size,
    const int64_t proposal_id,
    const int64_t version) :
  id_(id),
  proposal_id_(proposal_id),
  version_(version),
  start_lsn_(lsn),
  cur_lsn_(lsn),
  end_lsn_(lsn + size),
  max_fetch_scn_(),
  max_submit_scn_(),
  iter_(get_source_func, update_source_func, refresh_storage_info_func)
{
  pre_scn_ = pre_scn;
}

bool ObFetchLogTask::is_valid() const
{
  return id_.is_valid()
    && proposal_id_ > 0
    && version_ > 0
    && pre_scn_.is_valid()
    && start_lsn_.is_valid()
    && cur_lsn_.is_valid()
    && end_lsn_.is_valid()
    && end_lsn_ > start_lsn_;
}

void ObFetchLogTask::reset()
{
  id_.reset();
  proposal_id_ = 0;
  version_ = 0;
  pre_scn_.reset();
  start_lsn_.reset();
  cur_lsn_.reset();
  end_lsn_.reset();
  max_fetch_scn_.reset();
  max_submit_scn_.reset();
  iter_.reset();
}

} // namespace logservice
} // namespace oceanbase
