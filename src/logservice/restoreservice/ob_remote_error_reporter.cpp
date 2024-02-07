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
#include "ob_remote_error_reporter.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_define.h"
#include "storage/ls/ob_ls.h"                   // ObLS
#include "storage/restore/ob_ls_restore_handler.h"
#include "storage/tx_storage/ob_ls_map.h"       // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h"   // ObLSService

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::share;
using namespace oceanbase::storage;
ObRemoteErrorReporter::ObRemoteErrorReporter() :
  inited_(false),
  tenant_id_(common::OB_INVALID_TENANT_ID),
  last_check_ts_(OB_INVALID_TIMESTAMP),
  ls_svr_(NULL)
{}

ObRemoteErrorReporter::~ObRemoteErrorReporter()
{
  destroy();
}

int ObRemoteErrorReporter::init(const uint64_t tenant_id, storage::ObLSService *ls_svr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRemoteErrorReporter init twice", K(ret), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_svr));
  } else {
    tenant_id_ = tenant_id;
    ls_svr_ = ls_svr;
    last_check_ts_ = common::ObTimeUtility::fast_current_time();
    inited_ = true;
  }
  return ret;
}

void ObRemoteErrorReporter::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  last_check_ts_ = OB_INVALID_TIMESTAMP;
  ls_svr_ = NULL;
}

int ObRemoteErrorReporter::report_error()
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  const int64_t cur_ts = common::ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRemoteErrorReporter not init", K(ret));
  } else if (cur_ts  - last_check_ts_ < CHECK_ERROR_INTERVAL) {
    // skip
  } else if (! is_user_tenant(tenant_id_)) {
    // not user tenant, just skip
  } else if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get ls iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("iter is NULL", K(ret), K(iter));
  } else {
    while (OB_SUCC(ret)) {
      ls = NULL;
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter ls get next failed", K(ret));
        } else {
          LOG_TRACE("iter to end", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is NULL", K(ret), K(ls));
      } else if (OB_FAIL(do_report_(*ls))) {
        LOG_WARN("do fetch log failed", K(ret), K(ls));
      }
    } // while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      last_check_ts_ = common::ObTimeUtility::fast_current_time();
    }
  }
  return ret;
}

int ObRemoteErrorReporter::do_report_(ObLS &ls)
{
  int ret = OB_SUCCESS;
  share::ObTaskId trace_id;
  int ret_code = OB_SUCCESS;
  bool error_exist = false;
  ObLogRestoreHandler *restore_handler = NULL;
  ObLSRestoreStatus restore_status;
  if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get restore_handler failed", K(ret), K(ls));
  } else if (OB_FAIL(restore_handler->get_restore_error(trace_id, ret_code, error_exist))) {
    LOG_WARN("get restore error failed", K(ret), K(ls));
  } else if (! error_exist) {
  } else if (OB_FAIL(ls.get_ls_meta().get_restore_status(restore_status))) {
    LOG_WARN("get restore status failed", K(ret), K(ls));
  } else if (!restore_status.is_none()) {
    ret = report_restore_error_(ls, trace_id, ret_code);
  } else {
    ret = report_standby_error_(ls, trace_id, ret_code);
  }
  return ret;
}

int ObRemoteErrorReporter::report_restore_error_(ObLS &ls, share::ObTaskId &trace_id, const int ret_code)
{
  int ret = OB_SUCCESS;
  ObLSRestoreHandler *ls_restore_handler = NULL;
  if (OB_ISNULL(ls_restore_handler = ls.get_ls_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get ls_restore_handler failed", K(ret), K(ls));
  } else if (OB_FAIL(ls_restore_handler->record_clog_failed_info(trace_id, ls.get_ls_id(), ret_code))) {
    LOG_WARN("record clog failed info failed", K(ret), K(ls));
  } else {
    LOG_INFO("report log restore error succ", K(ls));
  }
  return ret;
}

int ObRemoteErrorReporter::report_standby_error_(ObLS &ls, share::ObTaskId &trace_id, const int ret_code)
{
  UNUSED(ls);
  UNUSED(trace_id);
  UNUSED(ret_code);
  return OB_NOT_SUPPORTED;
}
}
}
