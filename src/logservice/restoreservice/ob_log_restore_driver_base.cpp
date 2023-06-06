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
#include "ob_log_restore_driver_base.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "storage/tx_storage/ob_ls_map.h"       // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h"   // ObLSService
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace logservice
{
ObLogRestoreDriverBase::ObLogRestoreDriverBase() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  ls_svr_(NULL),
  log_service_(NULL)
{}

ObLogRestoreDriverBase::~ObLogRestoreDriverBase()
{
  destroy();
}

int ObLogRestoreDriverBase::init(const uint64_t tenant_id,
    ObLSService *ls_svr,
    ObLogService *log_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("restore driver already init", K(inited_));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID
        || NULL == ls_svr
        || NULL == log_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ls_svr), K(log_service));
  } else {
    tenant_id_ = tenant_id;
    ls_svr_ = ls_svr;
    log_service_ = log_service;
    inited_ = true;
  }
  return ret;
}

void ObLogRestoreDriverBase::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_svr_ = NULL;
  log_service_ = NULL;
}

int ObLogRestoreDriverBase::do_schedule()
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogRestoreArchiveDriver not init", K(ret));
  } else if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get log stream iter failed", K(ret));
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
      } else if (OB_FAIL(do_fetch_log_(*ls))) {
        LOG_WARN("do fetch log failed", K(ret), K(ls));
      }
    } // while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogRestoreDriverBase::set_global_recovery_scn(const share::SCN &recovery_scn)
{
  int ret = OB_SUCCESS;
  global_recovery_scn_ = recovery_scn;
  return ret;
}

int ObLogRestoreDriverBase::check_replica_status_(storage::ObLS &ls, bool &can_fetch_log)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus restore_status;
  if (OB_FAIL(ls.get_restore_status(restore_status))) {
    LOG_WARN("get restore status failed", K(ret), K(ls));
  } else {
    can_fetch_log = restore_status.can_restore_log();
  }
  return ret;
}

// Restore log need be under control, otherwise log disk may be full as single ls restore log too fast
// NB: Logs can be replayed only if its scn not bigger than replayable_point
int ObLogRestoreDriverBase::get_upper_resotore_scn(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  SCN replayable_point;
  if (OB_FAIL(log_service_->get_replayable_point(replayable_point))) {
    ARCHIVE_LOG(WARN, "get replayable point failed", K(ret));
  } else {
    share::SCN advance_scn = share::SCN::plus(replayable_point, FETCH_LOG_AHEAD_THRESHOLD_NS);
    scn = global_recovery_scn_ <= advance_scn ? global_recovery_scn_ : advance_scn;
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
