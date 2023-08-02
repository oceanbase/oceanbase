/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_restore_log_function.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_errno.h"
#include "share/ob_ls_id.h"     // ObLSID
#include <cstdint>
#include "storage/ls/ob_ls.h"    // ObLS
#include "ob_log_restore_handler.h"      // ObLogRestoreHandler
#include "storage/tx_storage/ob_ls_handle.h"     // ObLSHandle
#include "storage/tx_storage/ob_ls_service.h"    // ObLSService

namespace oceanbase
{
namespace logservice
{
#define GET_RESTORE_HANDLER_CTX(id)       \
  ObLS *ls = NULL;      \
  ObLSHandle ls_handle;         \
  ObLogRestoreHandler *restore_handler = NULL;       \
  if (OB_FAIL(ls_svr_->get_ls(id, ls_handle, ObLSGetMod::LOG_MOD))) {   \
    LOG_WARN("get ls failed", K(ret), K(id));     \
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {      \
    ret = OB_ERR_UNEXPECTED;       \
    LOG_INFO("get ls is NULL", K(ret), K(id));      \
  } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {     \
    ret = OB_ERR_UNEXPECTED;      \
    LOG_INFO("restore_handler is NULL", K(ret), K(id));   \
  }    \
  if (OB_SUCC(ret))

ObRestoreLogFunction::ObRestoreLogFunction()
{
  reset();
}

ObRestoreLogFunction::~ObRestoreLogFunction()
{
  reset();
}

void ObRestoreLogFunction::destroy()
{
  reset();
}

int ObRestoreLogFunction::init(const uint64_t tenant_id,
    storage::ObLSService *ls_svr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObRestoreLogFunction init twice", K(inited_));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID
        || NULL == ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_svr));
  } else {
    tenant_id_ = tenant_id;
    ls_svr_ = ls_svr;
    inited_ = true;
  }
  return ret;
}

void ObRestoreLogFunction::reset()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_svr_ = NULL;
}

int ObRestoreLogFunction::handle_group_entry(
    const uint64_t tenant_id,
    const share::ObLSID &id,
    const int64_t proposal_id,
    const palf::LSN &group_start_lsn,
    const palf::LogGroupEntry &group_entry,
    const char *buffer,
    void *ls_fetch_ctx,
    logfetcher::KickOutInfo &kick_out_info,
    logfetcher::TransStatInfo &tsi,
    volatile bool &stop_flag)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  const int64_t size = group_entry.get_serialize_size();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObRestoreLogFunction not initv", K(inited_));
  } else if (OB_UNLIKELY(!id.is_valid()
        || proposal_id <= 0
        || !group_start_lsn.is_valid()
        || !group_entry.check_integrity()
        || NULL == buffer)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id), K(proposal_id), K(group_start_lsn), K(group_entry), K(buffer));
  } else if (OB_FAIL(process_(id, proposal_id, group_start_lsn, group_entry.get_scn(),
          buffer, group_entry.get_serialize_size(), stop_flag))) {
    CLOG_LOG(WARN, "process failed", K(id), K(group_start_lsn), K(group_entry), K(buffer));
  }
  return ret;
}

int ObRestoreLogFunction::process_(const share::ObLSID &id,
    const int64_t proposal_id,
    const palf::LSN &lsn,
    const share::SCN &scn,
    const char *buf,
    const int64_t buf_size,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  do {
    GET_RESTORE_HANDLER_CTX(id) {
      if (OB_FAIL(restore_handler->raw_write(proposal_id, lsn, scn, buf, buf_size))) {
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          ret = OB_SUCCESS;
        } else if (OB_NOT_MASTER  == ret || OB_EAGAIN == ret || OB_RESTORE_LOG_TO_END == ret) {
          ret = OB_NEED_RETRY;
        } else {
          LOG_WARN("raw write failed", K(ret), K(id), K(lsn), K(buf), K(buf_size));
        }
      }
    } else {
      // ls not exist, just return need_retry
      if (OB_LS_NOT_EXIST == ret) {
        ret = OB_NEED_RETRY;
      }
    }
  } while (OB_LOG_OUTOF_DISK_SPACE == ret && ! stop_flag);
  return ret;
}
} // namespace logservice
} // namespace oceanbase
